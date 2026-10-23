"""
NFS pool of libcephfs clients (--clients_per_pool) scenario driver.

Each suite YAML entry runs one scenario (config["scenario"]). Soft-skips
with exit 0 when create -h lacks --clients_per_pool. OC combo scenarios
also soft-skip when live Ceph < 20.2.2.
"""

import json
from time import sleep

from nfs_operations import (
    Enable_nfs_coredump,
    init_cluster_health_check,
    log_cluster_health_and_check_crashes,
    mount_retry,
    wait_for_nfs_cluster_backend_endpoint,
    wait_for_nfs_ganesha_daemons,
)

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from tests.nfs.nfs_oc_pec import (
    _node_by_hostname,
    apply_nfs_object_cache,
    assert_pool_users_absent,
    ceph_users_object_name,
    create_nfs_with_clients_per_pool,
    ensure_nfs_admin_socket,
    list_cephfs_asoks,
    skip_oc_pec_unless_supported,
    skip_pool_unless_supported,
    verify_ceph_users,
    verify_nfs_object_cache,
    verify_pec_exports,
    verify_pool,
)
from utility.log import Log

log = Log(__name__)

DEFAULT_CLUSTER = "nfspool"
DEFAULT_K = 3
# Default / export count when suite omits exports=; always at least K.
DEFAULT_EXPORTS = 6
DEFAULT_SIZE = "200MiB"
DEFAULT_DIRTY = "100MiB"
_OC_COMBOS = frozenset({"pool_with_oc", "pool_with_oc_pec"})


def _pool_export_count(config, k, key="exports"):
    """Return / export count: suite value (default 6), never below K."""
    requested = int(config.get(key, DEFAULT_EXPORTS))
    n = max(requested, int(k))
    if n != requested:
        log.info("Raising %s %s -> %s (min clients_per_pool=%s)", key, requested, n, k)
    return n


def _do_mount(client, mount_point, version, port, server, export):
    client.exec_command(sudo=True, cmd=f"mkdir -p {mount_point}")
    mount_retry(client, mount_point, version, port, server, export)
    log.info("Mounted %s:%s at %s", server, export, mount_point)


def _do_unmount(client, mount_point):
    Unmount(client).unmount(mount_point)
    client.exec_command(
        sudo=True,
        cmd=f"timeout 120 rm -rf -- {mount_point}",
        check_ec=False,
        timeout=180,
    )
    sleep(2)


def _write_with_integrity(client, mount_point, filename="testfile", size_mb=4):
    data_path = f"{mount_point}/{filename}"
    md5_path = f"{mount_point}/{filename}.md5"
    client.exec_command(
        sudo=True,
        cmd=(
            f"dd if=/dev/urandom of={data_path} bs=1M count={size_mb} "
            f"conv=fsync status=none"
        ),
        timeout=120,
    )
    client.exec_command(sudo=True, cmd=f"md5sum {data_path} > {md5_path}", timeout=60)
    return f"{filename}.md5"


def _verify_integrity(client, mount_point, md5_filename="testfile.md5"):
    out, _ = client.exec_command(
        sudo=True, cmd=f"md5sum -c {mount_point}/{md5_filename}", timeout=60
    )
    if "OK" not in out:
        raise OperationFailedError(f"Integrity check failed: {out}")


def _io_unmount_remount(client, mount_point, version, port, server, export):
    md5 = _write_with_integrity(client, mount_point)
    _do_unmount(client, mount_point)
    _do_mount(client, mount_point, version, port, server, export)
    _verify_integrity(client, mount_point, md5)


def _nodes_for_hosts(ceph_cluster, hosts):
    wanted = hosts if isinstance(hosts, list) else [hosts]
    nodes = []
    for host in wanted:
        node = _node_by_hostname(ceph_cluster, host)
        if node not in nodes:
            nodes.append(node)
    return nodes


def _nfs_hostnames(ceph_cluster, count=1):
    nfs_nodes = (
        ceph_cluster.get_nodes("nfs")
        or ceph_cluster.get_nodes(role="installer")
        or ceph_cluster.get_nodes("mds")
    )
    if not nfs_nodes:
        raise ConfigError("No NFS nodes")
    names = list(dict.fromkeys(n.hostname for n in nfs_nodes))
    if len(names) < count:
        raise ConfigError(f"Need {count} NFS hosts, got {len(names)}: {names}")
    return names[:count]


def _setup_nodes(ceph_cluster, config):
    clients = ceph_cluster.get_nodes("client")
    if not clients:
        raise ConfigError("No client nodes")
    installers = ceph_cluster.get_nodes(role="installer") or []
    installer = installers[0] if installers else clients[0]
    return clients[0], _nfs_hostnames(ceph_cluster, 1)[0], installer


def _server_ip(ceph_cluster, hostname):
    return _node_by_hostname(ceph_cluster, hostname).ip_address


def _create_pool_cluster(
    ceph_cluster, client, installer, nfs_name, nfs_server, port, clients_per_pool=None
):
    Ceph(client).mgr.module.enable(module="nfs", force=True)
    sleep(2)
    cluster_nodes = _nodes_for_hosts(ceph_cluster, nfs_server)
    if clients_per_pool is None:
        kwargs = {}
        if port and str(port) != "2049":
            kwargs["port"] = port
        Ceph(client).nfs.cluster.create(name=nfs_name, nfs_server=nfs_server, **kwargs)
    else:
        create_nfs_with_clients_per_pool(
            client, nfs_name, nfs_server, port, clients_per_pool
        )
    if cluster_nodes:
        Enable_nfs_coredump(cluster_nodes)
    wait_for_nfs_cluster_backend_endpoint(client, installer, nfs_name, timeout=180)


def _create_export(client, nfs_name, export_path, cmount_path=False):
    """Create cephfs export; optional PEC via ``cmount_path`` (fail if still /)."""
    kwargs = {}
    if cmount_path:
        kwargs["cmount_path"] = True
    Ceph(client).nfs.export.create(
        fs_name="cephfs",
        nfs_name=nfs_name,
        nfs_export=export_path,
        fs="cephfs",
        **kwargs,
    )
    sleep(2)
    if cmount_path:
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph nfs export info {nfs_name} {export_path} -f json",
        )
        info = json.loads(out)
        fsal = info.get("fsal") or {}
        got = fsal.get("cmount_path") or info.get("cmount_path") or "/"
        if got == "/":
            raise OperationFailedError(
                f"export {export_path} created without cmount_path "
                f"(got {got!r}); CLI wrapper must pass --cmount_path"
            )


def _delete_cluster(client, nfs_name):
    try:
        client.exec_command(
            sudo=True, cmd=f"ceph nfs cluster rm {nfs_name}", timeout=60
        )
    except Exception as err:
        log.warning("cluster rm %s: %s", nfs_name, err)
    for _ in range(12):
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph orch ls --service-name nfs.{nfs_name}",
            check_ec=False,
        )
        text = str(out)
        if "No services reported" in text or f"nfs.{nfs_name}" not in text:
            return
        sleep(5)


def _delete_exports_and_subvols(client, nfs_name, export_paths):
    for export_path in export_paths:
        try:
            Ceph(client).nfs.export.delete(nfs_name, export_path)
        except Exception as err:
            log.warning("export delete %s: %s", export_path, err)
        subvol_name = export_path.replace("/", "")
        if not subvol_name:
            continue
        client.exec_command(
            sudo=True,
            cmd=(
                f"ceph fs subvolume rm cephfs {subvol_name} "
                "--group_name ganeshagroup --force"
            ),
            check_ec=False,
        )


def _full_cleanup(client, nfs_name, export_paths, mount_points):
    if isinstance(mount_points, str):
        mount_points = [mount_points]
    for mount_point in mount_points:
        try:
            client.exec_command(
                sudo=True,
                cmd=f"timeout 60 umount -l {mount_point}",
                check_ec=False,
                timeout=90,
            )
            client.exec_command(
                sudo=True,
                cmd=f"timeout 120 rm -rf -- {mount_point}",
                check_ec=False,
                timeout=180,
            )
        except Exception:
            pass
    _delete_exports_and_subvols(client, nfs_name, export_paths)
    _delete_cluster(client, nfs_name)


def _mount_all(client, ceph_cluster, server, port, exports, mounts):
    server_ip = _server_ip(ceph_cluster, server)
    for export_path, mount_point in zip(exports, mounts):
        _do_mount(client, mount_point, "4.2", port, server_ip, export_path)
        client.exec_command(sudo=True, cmd=f"ls {mount_point}", timeout=30)
    return server_ip


def _remount_all(client, ceph_cluster, server, port, exports, mounts):
    for mount_point in mounts:
        try:
            _do_unmount(client, mount_point)
        except Exception:
            pass
    return _mount_all(client, ceph_cluster, server, port, exports, mounts)


def _export_ids(client, nfs_name, exports):
    table = []
    for export_path in exports:
        info = Ceph(client).nfs.export.info(nfs_name, export_path)
        if isinstance(info, str):
            info = json.loads(info)
        eid = int(info.get("export_id") or info.get("id") or 0)
        table.append((export_path, eid))
    log.info("export_id table: %s", table)
    return table


def scenario_pool_off_control(ceph_cluster, config):
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}off")
    port = str(config.get("port", "2060"))
    n = int(config.get("exports", 3))
    exports = [f"/pool_off_{i}" for i in range(1, n + 1)]
    mounts = [f"/mnt/pool_off_{i}" for i in range(1, n + 1)]
    try:
        _create_pool_cluster(
            ceph_cluster,
            client,
            installer,
            nfs_name,
            server,
            port,
            clients_per_pool=None,
        )
        for export_path in exports:
            _create_export(client, nfs_name, export_path, cmount_path=False)
        sleep(5)
        _mount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(
            ceph_cluster,
            client,
            nfs_name,
            expect_clients=1,
            check_conf=False,
            check_ceph_users=False,
        )
        _io_unmount_remount(
            client, mounts[0], "4.2", port, _server_ip(ceph_cluster, server), exports[0]
        )
    finally:
        _full_cleanup(client, nfs_name, exports, mounts)


def scenario_pool_ceph_users_object(ceph_cluster, config):
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}users")
    port = str(config.get("port", "2061"))
    k = int(config.get("clients_per_pool", DEFAULT_K))
    n = _pool_export_count(config, k)
    exports = [f"/pool_users_{i}" for i in range(1, n + 1)]
    mounts = [f"/mnt/pool_users_{i}" for i in range(1, n + 1)]
    try:
        _create_pool_cluster(
            ceph_cluster, client, installer, nfs_name, server, port, clients_per_pool=k
        )
        assert_pool_users_absent(client, nfs_name)
        log.info("CFG-07 OK: no CEPH_USERS content and no pool.* auth yet")
        _create_export(client, nfs_name, exports[0], cmount_path=False)
        sleep(5)
        verify_ceph_users(client, nfs_name, expect_k=k)
        log.info("CEPH_USERS form OK after first / export")
        for export_path in exports[1:]:
            _create_export(client, nfs_name, export_path, cmount_path=False)
        sleep(5)
        _mount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(
            ceph_cluster,
            client,
            nfs_name,
            expect_clients=k,
            check_conf=True,
            check_ceph_users=True,
        )
    finally:
        _full_cleanup(client, nfs_name, exports, mounts)


def scenario_pool_k3_proof(ceph_cluster, config):
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}k3")
    port = str(config.get("port", "2062"))
    k = int(config.get("clients_per_pool", DEFAULT_K))
    n = _pool_export_count(config, k)
    exports = [f"/pool_k3_{i}" for i in range(1, n + 1)]
    mounts = [f"/mnt/pool_k3_{i}" for i in range(1, n + 1)]
    try:
        _create_pool_cluster(
            ceph_cluster, client, installer, nfs_name, server, port, clients_per_pool=k
        )
        for export_path in exports:
            _create_export(client, nfs_name, export_path, cmount_path=False)
        sleep(5)
        server_ip = _mount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(ceph_cluster, client, nfs_name, expect_clients=k)
        verify_nfs_object_cache(ceph_cluster, client, nfs_name, expect_on=False)
        table = _export_ids(client, nfs_name, exports)
        # ASN smoke: IO on export i and matching i+K (same residue)
        idx = 0
        mate = idx + k
        if mate < len(exports):
            for i in (idx, mate):
                client.exec_command(
                    sudo=True,
                    cmd=(
                        f"dd if=/dev/urandom of={mounts[i]}/asn.bin "
                        f"bs=1M count=8 conv=fsync status=none"
                    ),
                    timeout=120,
                )
            log.info(
                "ASN smoke wrote exports %s (id=%s) and %s (id=%s) residue %% %s",
                exports[idx],
                table[idx][1],
                exports[mate],
                table[mate][1],
                k,
            )
        asoks = list_cephfs_asoks(ceph_cluster, client, nfs_name)
        if len(asoks) < k:
            raise OperationFailedError(f"asoks dropped after ASN IO: {asoks}")
        _io_unmount_remount(client, mounts[0], "4.2", port, server_ip, exports[0])
    finally:
        _full_cleanup(client, nfs_name, exports, mounts)


def scenario_pool_invalid_k(ceph_cluster, config):
    client, server, _installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}badk")
    port = str(config.get("port", "2063"))
    # CFG-06: 0 / -1 / abc hit different validators; keep all three.
    bad_values = config.get("bad_values") or [0, -1, "abc"]
    Ceph(client).mgr.module.enable(module="nfs", force=True)
    sleep(2)
    try:
        for bad in bad_values:
            cmd = (
                f"ceph nfs cluster create {nfs_name} '{server}' "
                f"--port {port} --clients_per_pool {bad}"
            )
            out, err = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
            text = f"{out or ''}{err or ''}"
            ls_out, _ = client.exec_command(
                sudo=True, cmd="ceph nfs cluster ls", check_ec=False
            )
            if nfs_name in str(ls_out):
                _delete_cluster(client, nfs_name)
                raise OperationFailedError(
                    f"invalid clients_per_pool={bad!r} created cluster: {text}"
                )
            log.info("invalid clients_per_pool=%r rejected OK: %s", bad, text[:200])
    finally:
        _delete_cluster(client, nfs_name)


def scenario_pool_immutable_k(ceph_cluster, config):
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}imm")
    port = str(config.get("port", "2064"))
    k = int(config.get("clients_per_pool", DEFAULT_K))
    n = _pool_export_count(config, k)
    exports = [f"/pool_imm_{i}" for i in range(1, n + 1)]
    mounts = [f"/mnt/pool_imm_{i}" for i in range(1, n + 1)]
    try:
        _create_pool_cluster(
            ceph_cluster, client, installer, nfs_name, server, port, clients_per_pool=k
        )
        for export_path in exports:
            _create_export(client, nfs_name, export_path, cmount_path=False)
        sleep(5)
        _mount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(ceph_cluster, client, nfs_name, expect_clients=k)
        # Attempt in-place K change via orch spec merge
        out, err = client.exec_command(
            sudo=True,
            cmd=(f"ceph orch ls --service-name nfs.{nfs_name} --export --format json"),
            check_ec=False,
        )
        specs = json.loads(out) if out and str(out).strip().startswith("[") else []
        if not specs:
            raise OperationFailedError(
                f"immutable_k: no orch export for nfs.{nfs_name}"
            )
        spec = specs[0]
        spec.setdefault("spec", {})
        spec["spec"]["clients_per_pool"] = k + 1
        spec.pop("status", None)
        spec.pop("events", None)
        remote = "/tmp/pool_imm_spec.json"
        rf = client.remote_file(sudo=True, file_name=remote, file_mode="w")
        rf.write(json.dumps(spec))
        rf.flush()
        apply_out, apply_err = client.exec_command(
            sudo=True,
            cmd=f"ceph orch apply -i {remote}",
            check_ec=False,
        )
        log.info(
            "orch apply clients_per_pool=%s: %s %s",
            k + 1,
            apply_out,
            apply_err,
        )
        sleep(10)
        verify_pool(ceph_cluster, client, nfs_name, expect_clients=k)
    finally:
        _full_cleanup(client, nfs_name, exports, mounts)


def scenario_pool_ceph_users_fallback(ceph_cluster, config):
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}fb")
    port = str(config.get("port", "2065"))
    k = int(config.get("clients_per_pool", DEFAULT_K))
    n = _pool_export_count(config, k)
    exports = [f"/pool_fb_{i}" for i in range(1, n + 1)]
    mounts = [f"/mnt/pool_fb_{i}" for i in range(1, n + 1)]
    try:
        _create_pool_cluster(
            ceph_cluster, client, installer, nfs_name, server, port, clients_per_pool=k
        )
        for export_path in exports:
            _create_export(client, nfs_name, export_path, cmount_path=False)
        sleep(5)
        _mount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(ceph_cluster, client, nfs_name, expect_clients=k)
        obj = ceph_users_object_name(nfs_name)
        client.exec_command(
            sudo=True,
            cmd=f"echo -n '' | rados -N {nfs_name} -p .nfs put {obj} -",
            check_ec=False,
        )
        client.exec_command(
            sudo=True, cmd=f"ceph orch restart nfs.{nfs_name}", check_ec=False
        )
        wait_for_nfs_ganesha_daemons(installer, timeout=180, nfs_name=nfs_name)
        sleep(10)
        _remount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(
            ceph_cluster,
            client,
            nfs_name,
            expect_clients=1,
            check_conf=False,
            check_ceph_users=False,
        )
    finally:
        _full_cleanup(client, nfs_name, exports, mounts)


def scenario_pool_daemon_restart(ceph_cluster, config):
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}rst")
    port = str(config.get("port", "2066"))
    k = int(config.get("clients_per_pool", DEFAULT_K))
    n = _pool_export_count(config, k)
    exports = [f"/pool_rst_{i}" for i in range(1, n + 1)]
    mounts = [f"/mnt/pool_rst_{i}" for i in range(1, n + 1)]
    try:
        _create_pool_cluster(
            ceph_cluster, client, installer, nfs_name, server, port, clients_per_pool=k
        )
        for export_path in exports:
            _create_export(client, nfs_name, export_path, cmount_path=False)
        sleep(5)
        _mount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(ceph_cluster, client, nfs_name, expect_clients=k)
        client.exec_command(sudo=True, cmd=f"ceph orch restart nfs.{nfs_name}")
        wait_for_nfs_ganesha_daemons(installer, timeout=180, nfs_name=nfs_name)
        sleep(10)
        _remount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(ceph_cluster, client, nfs_name, expect_clients=k)
    finally:
        _full_cleanup(client, nfs_name, exports, mounts)


def _pool_pec_topology(ceph_cluster, config, nfs_name, port, k, pool_n, pec_n):
    """Create K=pool + pec_n same-FS PEC exports (IBMCEPH-20715: expect K+pec asoks)."""
    client, server, installer = _setup_nodes(ceph_cluster, config)
    pool_exports = [f"/pool_mix_p{i}" for i in range(1, pool_n + 1)]
    pec_exports = [f"/pool_mix_e{i}" for i in range(1, pec_n + 1)]
    pool_mounts = [f"/mnt/pool_mix_p{i}" for i in range(1, pool_n + 1)]
    pec_mounts = [f"/mnt/pool_mix_e{i}" for i in range(1, pec_n + 1)]
    all_exports = pool_exports + pec_exports
    all_mounts = pool_mounts + pec_mounts
    _create_pool_cluster(
        ceph_cluster, client, installer, nfs_name, server, port, clients_per_pool=k
    )
    for export_path in pool_exports:
        _create_export(client, nfs_name, export_path, cmount_path=False)
    for export_path in pec_exports:
        _create_export(client, nfs_name, export_path, cmount_path=True)
    sleep(5)
    server_ip = _mount_all(client, ceph_cluster, server, port, all_exports, all_mounts)
    return (
        client,
        server,
        installer,
        server_ip,
        pool_exports,
        pec_exports,
        pool_mounts,
        pec_mounts,
        all_exports,
        all_mounts,
    )


def _verify_pool_pec_mix(ceph_cluster, client, nfs_name, k, pec_n):
    """PEC export ids distinct; CEPH_USERS stays pool-scoped length K; asoks >= K+pec."""
    verify_pec_exports(client, nfs_name, expect_clients=pec_n)
    # CEPH_USERS is pool-only (length K); PEC identity is via export FSAL user_id.
    verify_ceph_users(client, nfs_name, expect_k=k)
    verify_pool(
        ceph_cluster,
        client,
        nfs_name,
        expect_clients=k,
        min_asoks=k + pec_n,
        check_ceph_users=False,
    )


def scenario_pool_with_oc(ceph_cluster, config):
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}oc")
    port = str(config.get("port", "2067"))
    k = int(config.get("clients_per_pool", DEFAULT_K))
    n = _pool_export_count(config, k)
    size = config.get("oc_size", DEFAULT_SIZE)
    dirty = config.get("oc_max_dirty", DEFAULT_DIRTY)
    exports = [f"/pool_oc_{i}" for i in range(1, n + 1)]
    mounts = [f"/mnt/pool_oc_{i}" for i in range(1, n + 1)]
    try:
        _create_pool_cluster(
            ceph_cluster, client, installer, nfs_name, server, port, clients_per_pool=k
        )
        for export_path in exports:
            _create_export(client, nfs_name, export_path, cmount_path=False)
        sleep(5)
        server_ip = _mount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(ceph_cluster, client, nfs_name, expect_clients=k)
        verify_nfs_object_cache(ceph_cluster, client, nfs_name, expect_on=False)
        apply_nfs_object_cache(
            installer, nfs_name, enable=True, size=size, max_dirty=dirty
        )
        server_ip = _remount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(ceph_cluster, client, nfs_name, expect_clients=k)
        verify_nfs_object_cache(
            ceph_cluster, client, nfs_name, expect_on=True, size=size, max_dirty=dirty
        )
        _io_unmount_remount(client, mounts[0], "4.2", port, server_ip, exports[0])
    finally:
        _full_cleanup(client, nfs_name, exports, mounts)


def scenario_pool_with_pec(ceph_cluster, config):
    """Pool K + same-FS --cmount_path PEC: expect K+pec asoks (IBMCEPH-20715)."""
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}pec")
    port = str(config.get("port", "2068"))
    k = int(config.get("clients_per_pool", DEFAULT_K))
    pool_n = _pool_export_count(config, k, key="pool_exports")
    pec_n = int(config.get("pec_exports", 2))
    all_exports, all_mounts = [], []
    try:
        (
            client,
            _server,
            _installer,
            server_ip,
            pool_exports,
            _pec_exports,
            pool_mounts,
            pec_mounts,
            all_exports,
            all_mounts,
        ) = _pool_pec_topology(ceph_cluster, config, nfs_name, port, k, pool_n, pec_n)
        _verify_pool_pec_mix(ceph_cluster, client, nfs_name, k, pec_n)
        client.exec_command(
            sudo=True,
            cmd=(
                f"dd if=/dev/urandom of={pool_mounts[0]}/p.bin "
                f"bs=1M count=4 conv=fsync status=none"
            ),
            timeout=120,
        )
        client.exec_command(
            sudo=True,
            cmd=(
                f"dd if=/dev/urandom of={pec_mounts[0]}/e.bin "
                f"bs=1M count=4 conv=fsync status=none"
            ),
            timeout=120,
        )
        _verify_pool_pec_mix(ceph_cluster, client, nfs_name, k, pec_n)
        _io_unmount_remount(
            client, pool_mounts[0], "4.2", port, server_ip, pool_exports[0]
        )
    finally:
        _full_cleanup(client, nfs_name, all_exports, all_mounts)


def scenario_pool_with_oc_pec(ceph_cluster, config):
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}all")
    port = str(config.get("port", "2069"))
    k = int(config.get("clients_per_pool", DEFAULT_K))
    pool_n = _pool_export_count(config, k, key="pool_exports")
    pec_n = int(config.get("pec_exports", 2))
    size = config.get("oc_size", DEFAULT_SIZE)
    dirty = config.get("oc_max_dirty", DEFAULT_DIRTY)
    all_exports, all_mounts = [], []
    try:
        (
            client,
            server,
            installer,
            server_ip,
            pool_exports,
            pec_exports,
            pool_mounts,
            pec_mounts,
            all_exports,
            all_mounts,
        ) = _pool_pec_topology(ceph_cluster, config, nfs_name, port, k, pool_n, pec_n)
        _verify_pool_pec_mix(ceph_cluster, client, nfs_name, k, pec_n)
        apply_nfs_object_cache(
            installer, nfs_name, enable=True, size=size, max_dirty=dirty
        )
        server_ip = _remount_all(
            client, ceph_cluster, server, port, all_exports, all_mounts
        )
        _verify_pool_pec_mix(ceph_cluster, client, nfs_name, k, pec_n)
        verify_nfs_object_cache(
            ceph_cluster, client, nfs_name, expect_on=True, size=size, max_dirty=dirty
        )
        _io_unmount_remount(
            client, pool_mounts[0], "4.2", port, server_ip, pool_exports[0]
        )
        _io_unmount_remount(
            client, pec_mounts[0], "4.2", port, server_ip, pec_exports[0]
        )
    finally:
        _full_cleanup(client, nfs_name, all_exports, all_mounts)


SCENARIO_MAP = {
    "pool_off_control": scenario_pool_off_control,
    "pool_ceph_users_object": scenario_pool_ceph_users_object,
    "pool_k3_proof": scenario_pool_k3_proof,
    "pool_invalid_k": scenario_pool_invalid_k,
    "pool_immutable_k": scenario_pool_immutable_k,
    "pool_ceph_users_fallback": scenario_pool_ceph_users_fallback,
    "pool_daemon_restart": scenario_pool_daemon_restart,
    "pool_with_oc": scenario_pool_with_oc,
    "pool_with_pec": scenario_pool_with_pec,
    "pool_with_oc_pec": scenario_pool_with_oc_pec,
}


def run(ceph_cluster, **kw):
    config = kw.get("config") or {}
    if skip_pool_unless_supported(ceph_cluster):
        log.info("TEST SKIPPED - pool requires --clients_per_pool on create -h")
        return 0
    scenario_key = config.get("scenario")
    if not scenario_key:
        raise ConfigError(f"config['scenario'] required: {list(SCENARIO_MAP)}")
    scenario_fn = SCENARIO_MAP.get(scenario_key)
    if not scenario_fn:
        raise ConfigError(f"Unknown scenario {scenario_key!r}")
    if scenario_key in _OC_COMBOS and skip_oc_pec_unless_supported(ceph_cluster):
        log.info(
            "TEST SKIPPED - scenario=%s needs OC (live Ceph < 20.2.2)",
            scenario_key,
        )
        return 0

    client, _, _installer = _setup_nodes(ceph_cluster, config)
    rados_obj, start_time = init_cluster_health_check(ceph_cluster, config)
    rc = 0
    with ensure_nfs_admin_socket(client):
        try:
            scenario_fn(ceph_cluster, config)
        except Exception as err:
            log.error("Scenario FAILED: %s -- %s", scenario_key, err, exc_info=True)
            rc = 1
        if log_cluster_health_and_check_crashes(rados_obj, start_time):
            rc = 1
        if rc == 0:
            log.info(
                "TEST PASSED - scenario=%s pool clients workflow completed",
                scenario_key,
            )
    return rc
