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
    SoftSkip,
    _load_nfs_orch_spec,
    _node_by_hostname,
    apply_clients_per_pool,
    apply_nfs_object_cache,
    assert_cluster_info_clients_per_pool,
    assert_export_client_pool,
    assert_pool_users_absent,
    ceph_users_object_name,
    create_nfs_with_clients_per_pool,
    delete_nfs_cluster,
    ensure_nfs_admin_socket,
    full_nfs_cleanup,
    list_cephfs_asoks,
    patch_nfs_orch_spec,
    pool_enhancement_supported,
    rm_nfs_mount_point,
    run_pool_rotate_key,
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
# IBM Ceph 10.0 / Umbrella (Ceph >= 21.0.0) only
_ENHANCEMENT_SCENARIOS = frozenset(
    {
        "pool_raise_k",
        "pool_enable_brownfield",
        "pool_rotate_key",
        "nfs_spec_log_to_file",
    }
)
# Pre-fix orch rejection text for in-place clients_per_pool (raise / brownfield).
_CPP_IMMUTABLE_NEEDLES = (
    "only when creating",
    "can only be set",
    "only be set when creating",
    "immutable",
)


def _softskip_or_fail_cpp(rc, combined, soft_msg, hard_msg):
    """SoftSkip known immutable-cpp rejection; else OperationFailedError."""
    if rc == 0:
        return
    low = combined.lower()
    if any(s in low for s in _CPP_IMMUTABLE_NEEDLES):
        raise SoftSkip(f"{soft_msg}: {combined[:300]}")
    raise OperationFailedError(f"{hard_msg}: {combined[:400]}")


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
    rm_nfs_mount_point(client, mount_point)
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
    # Drop leftovers from aborted runs so brownfield baseline is clean.
    delete_nfs_cluster(client, nfs_name)
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
        full_nfs_cleanup(client, nfs_name, exports, mounts)


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
        full_nfs_cleanup(client, nfs_name, exports, mounts)


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
        # IBMCEPH-19519 eng scope (10.0+ only)
        if pool_enhancement_supported(ceph_cluster):
            assert_cluster_info_clients_per_pool(client, nfs_name, k)
            assert_export_client_pool(client, nfs_name, exports[0], expect_users=k)
        _io_unmount_remount(client, mounts[0], "4.2", port, server_ip, exports[0])
    finally:
        full_nfs_cleanup(client, nfs_name, exports, mounts)


def _assert_cpp_create_rejected(client, nfs_name, server, port, bad):
    """Create with bad clients_per_pool must fail and leave no cluster."""
    cmd = (
        f"ceph nfs cluster create {nfs_name} '{server}' "
        f"--port {port} --clients_per_pool {bad}"
    )
    out, err = client.exec_command(sudo=True, cmd=cmd, check_ec=False, timeout=120)
    text = f"{out or ''}{err or ''}"
    rc = getattr(client, "exit_status", 0)
    ls_out, _ = client.exec_command(
        sudo=True, cmd="ceph nfs cluster ls", check_ec=False, timeout=60
    )
    if nfs_name in str(ls_out):
        delete_nfs_cluster(client, nfs_name)
        raise OperationFailedError(
            f"invalid clients_per_pool={bad!r} created cluster: {text}"
        )
    if rc == 0:
        raise OperationFailedError(
            f"invalid clients_per_pool={bad!r} exited 0 "
            f"without listing cluster: {text}"
        )
    log.info(
        "invalid clients_per_pool=%r rejected OK rc=%s: %s",
        bad,
        rc,
        text[:200],
    )


def scenario_pool_invalid_k(ceph_cluster, config):
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}badk")
    port = str(config.get("port", "2063"))
    # CFG-06: 0 / -1 / abc hit different validators; keep all three.
    bad_values = config.get("bad_values") or [0, -1, "abc"]
    Ceph(client).mgr.module.enable(module="nfs", force=True)
    sleep(2)
    try:
        for bad in bad_values:
            _assert_cpp_create_rejected(client, nfs_name, server, port, bad)
        # IBMCEPH-19517 — K ceiling (10.0+ only)
        if pool_enhancement_supported(ceph_cluster):
            ceil_name = f"{nfs_name}ceil"
            _assert_cpp_create_rejected(
                client, ceil_name, server, str(int(port) + 100), 65
            )
            k64 = f"{nfs_name}k64"
            k64_port = str(int(port) + 101)
            try:
                _create_pool_cluster(
                    ceph_cluster,
                    client,
                    installer,
                    k64,
                    server,
                    k64_port,
                    clients_per_pool=64,
                )
                _create_export(client, k64, "/pool_k64_1", cmount_path=False)
                sleep(5)
                assert_cluster_info_clients_per_pool(client, k64, 64)
                mount = "/mnt/pool_k64_1"
                server_ip = _server_ip(ceph_cluster, server)
                _do_mount(client, mount, "4.2", k64_port, server_ip, "/pool_k64_1")
                client.exec_command(
                    sudo=True,
                    cmd=f"dd if=/dev/urandom of={mount}/k64.bin bs=1M count=2 "
                    f"conv=fsync status=none",
                    timeout=60,
                )
            finally:
                full_nfs_cleanup(client, k64, ["/pool_k64_1"], ["/mnt/pool_k64_1"])
    finally:
        delete_nfs_cluster(client, nfs_name)


def scenario_pool_raise_k(ceph_cluster, config):
    """IBMCEPH-19485: raise clients_per_pool in place; reject decrease."""
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}raise")
    port = str(config.get("port", "2064"))
    k = int(config.get("clients_per_pool", DEFAULT_K))
    raised = int(config.get("raised_k", k + 2))
    n = _pool_export_count(config, raised)
    exports = [f"/pool_raise_{i}" for i in range(1, n + 1)]
    mounts = [f"/mnt/pool_raise_{i}" for i in range(1, n + 1)]
    try:
        _create_pool_cluster(
            ceph_cluster, client, installer, nfs_name, server, port, clients_per_pool=k
        )
        for export_path in exports:
            _create_export(client, nfs_name, export_path, cmount_path=False)
        sleep(5)
        server_ip = _mount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(ceph_cluster, client, nfs_name, expect_clients=k)
        rc, combined = apply_clients_per_pool(
            client, installer, nfs_name, raised, restart=True
        )
        # Pre-19485 builds reject in-place raise; soft-skip those only.
        _softskip_or_fail_cpp(
            rc,
            combined,
            "raise clients_per_pool rejected (pre-19485?)",
            f"raise clients_per_pool to {raised} failed",
        )
        # Remount so raised-K clients open before asok count assert
        server_ip = _remount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(ceph_cluster, client, nfs_name, expect_clients=raised)
        _io_unmount_remount(client, mounts[0], "4.2", port, server_ip, exports[0])
        # Decrease must fail
        dec_rc, dec_out = apply_clients_per_pool(
            client, installer, nfs_name, max(2, k - 1), restart=False
        )
        if dec_rc == 0:
            raise OperationFailedError(
                f"decrease clients_per_pool unexpectedly succeeded: {dec_out[:300]}"
            )
        assert_cluster_info_clients_per_pool(client, nfs_name, raised)
    finally:
        full_nfs_cleanup(client, nfs_name, exports, mounts)


def scenario_pool_enable_brownfield(ceph_cluster, config):
    """IBMCEPH-19484: enable clients_per_pool on existing non-pool cluster."""
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}bf")
    port = str(config.get("port", "2070"))
    k = int(config.get("clients_per_pool", DEFAULT_K))
    n = _pool_export_count(config, k)
    exports = [f"/pool_bf_{i}" for i in range(1, n + 1)]
    mounts = [f"/mnt/pool_bf_{i}" for i in range(1, n + 1)]
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
        assert_pool_users_absent(client, nfs_name)
        for export_path in exports:
            _create_export(client, nfs_name, export_path, cmount_path=False)
        sleep(5)
        server_ip = _mount_all(client, ceph_cluster, server, port, exports, mounts)
        asoks = list_cephfs_asoks(ceph_cluster, client, nfs_name)
        if len(asoks) != 1:
            raise OperationFailedError(
                f"brownfield baseline expected 1 asok, got {asoks}"
            )
        rc, combined = apply_clients_per_pool(
            client, installer, nfs_name, k, restart=True
        )
        # Soft-skip only known pre-19484 rejection text; fail anything else.
        _softskip_or_fail_cpp(
            rc,
            combined,
            "brownfield enable clients_per_pool rejected (pre-19484)",
            f"brownfield enable clients_per_pool={k} failed",
        )
        assert_export_client_pool(
            client, nfs_name, exports[0], expect_users=k, timeout=90
        )
        # Remount so pool clients open before asok count assert
        server_ip = _remount_all(client, ceph_cluster, server, port, exports, mounts)
        verify_pool(ceph_cluster, client, nfs_name, expect_clients=k)
        assert_cluster_info_clients_per_pool(client, nfs_name, k)
        _io_unmount_remount(client, mounts[0], "4.2", port, server_ip, exports[0])
    finally:
        full_nfs_cleanup(client, nfs_name, exports, mounts)


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
        full_nfs_cleanup(client, nfs_name, exports, mounts)


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
        full_nfs_cleanup(client, nfs_name, exports, mounts)


def _pool_mix_paths(pool_n, pec_n):
    """Export/mount path lists for pool+PEC mix scenarios (built before try)."""
    pool_exports = [f"/pool_mix_p{i}" for i in range(1, pool_n + 1)]
    pec_exports = [f"/pool_mix_e{i}" for i in range(1, pec_n + 1)]
    pool_mounts = [f"/mnt/pool_mix_p{i}" for i in range(1, pool_n + 1)]
    pec_mounts = [f"/mnt/pool_mix_e{i}" for i in range(1, pec_n + 1)]
    return (
        pool_exports,
        pec_exports,
        pool_mounts,
        pec_mounts,
        pool_exports + pec_exports,
        pool_mounts + pec_mounts,
    )


def _pool_pec_topology(
    ceph_cluster,
    client,
    installer,
    server,
    nfs_name,
    port,
    k,
    pool_exports,
    pec_exports,
    pool_mounts,
    pec_mounts,
):
    """Create K=pool + same-FS PEC exports (IBMCEPH-20715: expect K+pec asoks)."""
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
    return _mount_all(client, ceph_cluster, server, port, all_exports, all_mounts)


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
        full_nfs_cleanup(client, nfs_name, exports, mounts)


def scenario_pool_with_pec(ceph_cluster, config):
    """Pool K + same-FS --cmount_path PEC: expect K+pec asoks (IBMCEPH-20715)."""
    # Bind client + paths before try so finally cleanup runs if topology fails.
    client, _server, _installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}pec")
    port = str(config.get("port", "2068"))
    k = int(config.get("clients_per_pool", DEFAULT_K))
    pool_n = _pool_export_count(config, k, key="pool_exports")
    pec_n = int(config.get("pec_exports", 2))
    (
        pool_exports,
        pec_exports,
        pool_mounts,
        pec_mounts,
        all_exports,
        all_mounts,
    ) = _pool_mix_paths(pool_n, pec_n)
    try:
        server_ip = _pool_pec_topology(
            ceph_cluster,
            client,
            _installer,
            _server,
            nfs_name,
            port,
            k,
            pool_exports,
            pec_exports,
            pool_mounts,
            pec_mounts,
        )
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
        # IBMCEPH-19519: / export has client_pool; PEC omits it (10.0+)
        if pool_enhancement_supported(ceph_cluster):
            assert_export_client_pool(client, nfs_name, pool_exports[0], expect_users=k)
            assert_export_client_pool(
                client, nfs_name, pec_exports[0], expect_users=None
            )
        _io_unmount_remount(
            client, pool_mounts[0], "4.2", port, server_ip, pool_exports[0]
        )
    finally:
        full_nfs_cleanup(client, nfs_name, all_exports, all_mounts)


def scenario_pool_with_oc_pec(ceph_cluster, config):
    # Bind client + paths before try so finally cleanup runs if topology fails.
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}all")
    port = str(config.get("port", "2069"))
    k = int(config.get("clients_per_pool", DEFAULT_K))
    pool_n = _pool_export_count(config, k, key="pool_exports")
    pec_n = int(config.get("pec_exports", 2))
    size = config.get("oc_size", DEFAULT_SIZE)
    dirty = config.get("oc_max_dirty", DEFAULT_DIRTY)
    (
        pool_exports,
        pec_exports,
        pool_mounts,
        pec_mounts,
        all_exports,
        all_mounts,
    ) = _pool_mix_paths(pool_n, pec_n)
    try:
        server_ip = _pool_pec_topology(
            ceph_cluster,
            client,
            installer,
            server,
            nfs_name,
            port,
            k,
            pool_exports,
            pec_exports,
            pool_mounts,
            pec_mounts,
        )
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
        full_nfs_cleanup(client, nfs_name, all_exports, all_mounts)


def scenario_nfs_spec_log_to_file(ceph_cluster, config):
    """IBMCEPH-13289: NFSServiceSpec log_to_file=true writes daemon logs to file."""
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", f"{DEFAULT_CLUSTER}logf")
    port = str(config.get("port", "2072"))
    export = "/pool_logf_1"
    mount = "/mnt/pool_logf_1"
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
        rc, combined = patch_nfs_orch_spec(
            client,
            installer,
            nfs_name,
            {"log_to_file": True},
            timeout=300,
            restart=True,
        )
        if rc != 0:
            # Soft-skip only unknown-field style rejection; fail anything else.
            if "log_to_file" in combined and (
                "unknown" in combined.lower() or "unexpected" in combined.lower()
            ):
                raise SoftSkip(
                    f"NFS spec log_to_file not supported (pre-13289?): "
                    f"{combined[:300]}"
                )
            raise OperationFailedError(
                f"orch apply log_to_file=true failed rc={rc}: {combined[:400]}"
            )
        applied = _load_nfs_orch_spec(client, nfs_name)
        if applied.get("spec", {}).get("log_to_file") is not True:
            raise OperationFailedError(
                f"nfs.{nfs_name} orch spec log_to_file not True after apply: "
                f"{applied.get('spec')}"
            )
        _create_export(client, nfs_name, export, cmount_path=False)
        server_ip = _server_ip(ceph_cluster, server)
        _do_mount(client, mount, "4.2", port, server_ip, export)
        client.exec_command(
            sudo=True,
            cmd=f"echo logf > {mount}/.probe && sync",
            timeout=30,
        )
        nfs_node = _node_by_hostname(ceph_cluster, server)
        fsid_out, _ = client.exec_command(
            sudo=True, cmd="ceph fsid", check_ec=False, timeout=30
        )
        fsid = (fsid_out or "").strip()
        # Only this cluster's daemon dir; require non-empty files (not *ganesha*
        # leftovers / empty dirs from global log_to_file).
        logs_out, _ = nfs_node.exec_command(
            sudo=True,
            cmd=(
                f"find /var/log/ceph/{fsid} -maxdepth 2 -type f -size +0 "
                f"-path '*/nfs.{nfs_name}*' 2>/dev/null | head -20"
            ),
            check_ec=False,
            timeout=60,
        )
        if not (logs_out or "").strip():
            raise OperationFailedError(
                f"nfs.{nfs_name} log_to_file=true but no non-empty log files under "
                f"/var/log/ceph/{fsid}/nfs.{nfs_name}*"
            )
        log.info("nfs.%s log_to_file evidence:\n%s", nfs_name, logs_out[:500])
    finally:
        full_nfs_cleanup(client, nfs_name, [export], [mount])


SCENARIO_MAP = {
    "pool_off_control": scenario_pool_off_control,
    "pool_ceph_users_object": scenario_pool_ceph_users_object,
    "pool_k3_proof": scenario_pool_k3_proof,
    "pool_invalid_k": scenario_pool_invalid_k,
    "pool_raise_k": scenario_pool_raise_k,
    "pool_enable_brownfield": scenario_pool_enable_brownfield,
    "pool_ceph_users_fallback": scenario_pool_ceph_users_fallback,
    "pool_daemon_restart": scenario_pool_daemon_restart,
    "pool_with_oc": scenario_pool_with_oc,
    "pool_with_pec": scenario_pool_with_pec,
    "pool_with_oc_pec": scenario_pool_with_oc_pec,
    "pool_rotate_key": run_pool_rotate_key,
    "nfs_spec_log_to_file": scenario_nfs_spec_log_to_file,
}


def run(ceph_cluster, **kw):
    config = kw.get("config") or {}
    scenario_key = config.get("scenario")
    # 13289 does not require --clients_per_pool
    needs_pool_flag = scenario_key != "nfs_spec_log_to_file"
    if needs_pool_flag and skip_pool_unless_supported(ceph_cluster):
        log.info("TEST SKIPPED - pool requires --clients_per_pool on create -h")
        return 0
    if not scenario_key:
        raise ConfigError(f"config['scenario'] required: {list(SCENARIO_MAP)}")
    scenario_fn = SCENARIO_MAP.get(scenario_key)
    if not scenario_fn:
        raise ConfigError(f"Unknown scenario {scenario_key!r}")
    if scenario_key in _ENHANCEMENT_SCENARIOS and not pool_enhancement_supported(
        ceph_cluster
    ):
        log.info(
            "TEST SKIPPED - scenario=%s requires Ceph >= 21.0.0 (IBM Ceph 10.0)",
            scenario_key,
        )
        return 0
    if scenario_key in _OC_COMBOS and skip_oc_pec_unless_supported(ceph_cluster):
        log.info(
            "TEST SKIPPED - scenario=%s needs OC (live Ceph < 20.2.2)",
            scenario_key,
        )
        return 0

    client, _, _installer = _setup_nodes(ceph_cluster, config)
    rados_obj, start_time = init_cluster_health_check(ceph_cluster, config)
    rc = 0
    skipped = False
    with ensure_nfs_admin_socket(client):
        try:
            scenario_fn(ceph_cluster, config)
        except SoftSkip as err:
            log.info("TEST SKIPPED - scenario=%s -- %s", scenario_key, err)
            skipped = True
        except Exception as err:
            log.error("Scenario FAILED: %s -- %s", scenario_key, err, exc_info=True)
            rc = 1
        # SoftSkip still runs crash check — skip must not hide a crash.
        if log_cluster_health_and_check_crashes(rados_obj, start_time):
            rc = 1
        if rc == 0 and not skipped:
            log.info(
                "TEST PASSED - scenario=%s pool clients workflow completed",
                scenario_key,
            )
    return rc
