"""
NFS Object Cache + Per-Export Client -- basic workflow scenarios.

Each suite YAML entry runs one scenario (config["scenario"]). Live Ceph
must be >= 20.2.2 or the test skips with exit 0.

OC is asserted in ganesha.conf (CEPH {}). PEC is N CephFS clients
(asoks + distinct user_id + cmount_path).
"""

import json
from time import sleep

from nfs_operations import (
    Enable_nfs_coredump,
    init_cluster_health_check,
    log_cluster_health_and_check_crashes,
    mount_retry,
    wait_for_nfs_cluster_backend_endpoint,
)

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from tests.nfs.nfs_oc_pec import (
    _node_by_hostname,
    apply_nfs_object_cache,
    deploy_nfs_with_object_cache,
    ensure_nfs_admin_socket,
    skip_oc_pec_unless_supported,
    verify_nfs_object_cache,
    verify_pec,
)
from utility.log import Log

log = Log(__name__)

DEFAULT_CLUSTER = "ocpec"
DEFAULT_PORT = "2050"
DEFAULT_SIZE = "200MiB"
DEFAULT_DIRTY = "100MiB"


def _do_mount(client, mount_point, version, port, server, export):
    client.exec_command(sudo=True, cmd=f"mkdir -p {mount_point}")
    mount_retry(client, mount_point, version, port, server, export)
    log.info("Mounted %s:%s at %s", server, export, mount_point)


def _do_unmount(client, mount_point):
    Unmount(client).unmount(mount_point)
    _rm_mount_point(client, mount_point)
    sleep(2)


def _rm_mount_point(client, mount_point):
    """Remove mount dir; bound so a stuck NFS leftover cannot hang forever."""
    client.exec_command(
        sudo=True,
        cmd=f"timeout 120 rm -rf -- {mount_point}",
        check_ec=False,
        timeout=180,
    )


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
    md5_path = f"{mount_point}/{md5_filename}"
    out, _ = client.exec_command(sudo=True, cmd=f"md5sum -c {md5_path}", timeout=60)
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


def _create_cluster(
    ceph_cluster,
    client,
    nfs_name,
    nfs_server,
    port,
    installer=None,
    enable_oc=False,
    oc_size=DEFAULT_SIZE,
    oc_max_dirty=DEFAULT_DIRTY,
):
    """Create an NFS cluster. enable_oc=True deploys OC in the initial orch spec.

    CLI create-time OC is IBMCEPH-18328; until then enable_oc uses spec apply.
    """
    Ceph(client).mgr.module.enable(module="nfs", force=True)
    sleep(2)
    if installer is None:
        installers = ceph_cluster.get_nodes(role="installer") or []
        installer = installers[0] if installers else client
    cluster_nodes = _nodes_for_hosts(ceph_cluster, nfs_server)
    if enable_oc:
        deploy_nfs_with_object_cache(
            installer,
            nfs_name,
            nfs_server,
            port,
            size=oc_size,
            max_dirty=oc_max_dirty,
            nfs_nodes=cluster_nodes or None,
        )
    else:
        kwargs = {}
        if port and str(port) != "2049":
            kwargs["port"] = port
        Ceph(client).nfs.cluster.create(name=nfs_name, nfs_server=nfs_server, **kwargs)
        if cluster_nodes:
            Enable_nfs_coredump(cluster_nodes)
    wait_for_nfs_cluster_backend_endpoint(client, installer, nfs_name, timeout=180)


def _create_export(client, nfs_name, export_path, cmount_path=False):
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
    log.warning("nfs.%s still listed after cluster rm", nfs_name)


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
            _rm_mount_point(client, mount_point)
        except Exception:
            pass
    _delete_exports_and_subvols(client, nfs_name, export_paths)
    _delete_cluster(client, nfs_name)


def _server_ip(ceph_cluster, hostname):
    return _node_by_hostname(ceph_cluster, hostname).ip_address


def _setup_nodes(ceph_cluster, config):
    clients = ceph_cluster.get_nodes("client")
    if not clients:
        raise ConfigError("No client nodes")
    installers = ceph_cluster.get_nodes(role="installer") or []
    installer = installers[0] if installers else clients[0]
    return clients[0], _nfs_hostnames(ceph_cluster, 1)[0], installer


def _export_info(client, nfs_name, export_path):
    info = Ceph(client).nfs.export.info(nfs_name, export_path)
    if isinstance(info, str):
        info = json.loads(info)
    if not isinstance(info, dict):
        raise OperationFailedError(f"export info not a dict: {info!r}")
    return info


def _fsal_cmount_and_user(info):
    fsal = info.get("fsal") or {}
    cmount = fsal.get("cmount_path") or info.get("cmount_path")
    user_id = fsal.get("user_id")
    return fsal, cmount, user_id, info.get("path")


def _apply_export_json(client, nfs_name, payload, remote_path="/tmp/ocpec_export.json"):
    body = json.dumps(payload)
    remote = client.remote_file(sudo=True, file_name=remote_path, file_mode="w")
    remote.write(body)
    remote.flush()
    out = Ceph(client).nfs.export.apply(nfs_name, remote_path)
    log.info("nfs export apply: %s", out)
    return out


def scenario_shared_to_pec_via_apply(ceph_cluster, config):
    """Shared export (cmount_path=/) to PEC via ceph nfs export apply -i.

    nfs export update has no --cmount_path; apply is the supported convert path.
    """
    client, server, _installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", DEFAULT_CLUSTER)
    port = str(config.get("port", DEFAULT_PORT))
    export_path = "/ocpec_shared_to_pec"
    mount_point = "/mnt/ocpec_shared_to_pec"
    try:
        _create_cluster(ceph_cluster, client, nfs_name, server, port)
        _create_export(client, nfs_name, export_path)
        info = _export_info(client, nfs_name, export_path)
        fsal, cmount, old_user, path = _fsal_cmount_and_user(info)
        if cmount != "/":
            raise OperationFailedError(
                f"expected shared cmount_path='/', got {cmount!r} info={info}"
            )
        if not old_user:
            raise OperationFailedError(f"shared export missing user_id: {info}")
        if not path or path == "/":
            raise OperationFailedError(
                f"shared export path must be a subvol, got {path!r}"
            )
        log.info(
            "Shared export path=%s user_id=%s export_id=%s",
            path,
            old_user,
            info.get("export_id"),
        )

        server_ip = _server_ip(ceph_cluster, server)
        _do_mount(client, mount_point, "4.2", port, server_ip, export_path)
        md5 = _write_with_integrity(client, mount_point)

        payload = dict(info)
        payload_fsal = dict(fsal)
        payload_fsal["cmount_path"] = path
        payload_fsal.pop("user_id", None)
        payload["fsal"] = payload_fsal
        if "cmount_path" in payload:
            payload["cmount_path"] = path
        apply_out = _apply_export_json(client, nfs_name, payload)
        if (
            "updated" not in str(apply_out).lower()
            and "created" not in str(apply_out).lower()
        ):
            raise OperationFailedError(
                f"nfs export apply did not update {export_path}: {apply_out}"
            )
        sleep(5)

        after = _export_info(client, nfs_name, export_path)
        _, new_cmount, new_user, new_path = _fsal_cmount_and_user(after)
        if new_cmount != new_path or new_cmount == "/":
            raise OperationFailedError(
                f"PEC convert failed cmount_path={new_cmount!r} path={new_path!r}"
            )
        if not new_user or new_user == old_user:
            raise OperationFailedError(
                f"expected new user_id after apply, old={old_user!r} new={new_user!r}"
            )
        auth, _ = client.exec_command(
            sudo=True, cmd=f"ceph auth get client.{new_user}", timeout=60
        )
        if path not in str(auth):
            raise OperationFailedError(
                f"new CephX client.{new_user} MDS cap missing path {path}: {auth}"
            )
        old_auth, old_err = client.exec_command(
            sudo=True,
            cmd=f"ceph auth get client.{old_user}",
            timeout=60,
            check_ec=False,
        )
        if f"[client.{old_user}]" not in f"{old_auth}{old_err}":
            raise OperationFailedError(
                f"old CephX client.{old_user} missing after PEC apply "
                f"(must not auth rm): {old_auth}{old_err}"
            )
        # Remount so Ganesha opens the new PEC CephFS client before asok check.
        _do_unmount(client, mount_point)
        _do_mount(client, mount_point, "4.2", port, server_ip, export_path)
        client.exec_command(sudo=True, cmd=f"ls {mount_point}", timeout=30)
        verify_pec(ceph_cluster, client, nfs_name, expect_clients=1)
        _verify_integrity(client, mount_point, md5)
    finally:
        _full_cleanup(client, nfs_name, [export_path], [mount_point])


def _export_and_io(
    client, ceph_cluster, server, nfs_name, export_path, mount_point, port
):
    # TBD: config["nfs_version"] for 3 / 4.0 / 4.1 / 4.2 (one YAML entry, not a
    # full scenario matrix). v3 needs --enable-nfsv3 on create + open_mandatory_v3_ports.
    _create_export(client, nfs_name, export_path)
    server_ip = _server_ip(ceph_cluster, server)
    _do_mount(client, mount_point, "4.2", port, server_ip, export_path)
    _io_unmount_remount(client, mount_point, "4.2", port, server_ip, export_path)


def scenario_oc_on(ceph_cluster, config):
    """Both OC enable paths on two NFS clusters (two unique nfs hosts).

    Same-host two clusters need distinct monitoring_port; this scenario
    does not set that, so it requires two hosts.

    1. nfs cluster create (OC off) then orch spec apply.
    2. First orch apply already has enable_client_object_cache.
    """
    client, _, installer = _setup_nodes(ceph_cluster, config)
    host_reapply, host_boot = _nfs_hostnames(ceph_cluster, 2)
    size = config.get("oc_size", DEFAULT_SIZE)
    dirty = config.get("oc_max_dirty", DEFAULT_DIRTY)
    name_reapply = config.get("nfs_name", DEFAULT_CLUSTER)
    name_boot = config.get("nfs_name_oc", name_reapply + "oc")
    port_reapply = str(config.get("port", DEFAULT_PORT))
    port_boot = str(config.get("port_oc", "2051"))
    export_reapply = "/ocpec_oc_reapply"
    export_boot = "/ocpec_oc_boot"
    mount_reapply = "/mnt/ocpec_oc_reapply"
    mount_boot = "/mnt/ocpec_oc_boot"
    try:
        _create_cluster(ceph_cluster, client, name_reapply, host_reapply, port_reapply)
        verify_nfs_object_cache(ceph_cluster, client, name_reapply, expect_on=False)
        apply_nfs_object_cache(
            installer, name_reapply, enable=True, size=size, max_dirty=dirty
        )
        verify_nfs_object_cache(
            ceph_cluster,
            client,
            name_reapply,
            expect_on=True,
            size=size,
            max_dirty=dirty,
        )

        _create_cluster(
            ceph_cluster,
            client,
            name_boot,
            host_boot,
            port_boot,
            installer=installer,
            enable_oc=True,
            oc_size=size,
            oc_max_dirty=dirty,
        )
        verify_nfs_object_cache(
            ceph_cluster, client, name_boot, expect_on=True, size=size, max_dirty=dirty
        )

        _export_and_io(
            client,
            ceph_cluster,
            host_reapply,
            name_reapply,
            export_reapply,
            mount_reapply,
            port_reapply,
        )
        _export_and_io(
            client,
            ceph_cluster,
            host_boot,
            name_boot,
            export_boot,
            mount_boot,
            port_boot,
        )
    finally:
        _full_cleanup(client, name_boot, [export_boot], [mount_boot])
        _full_cleanup(client, name_reapply, [export_reapply], [mount_reapply])


def scenario_enable_oc_on_existing(ceph_cluster, config):
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", DEFAULT_CLUSTER)
    port = str(config.get("port", DEFAULT_PORT))
    size = config.get("oc_size", DEFAULT_SIZE)
    dirty = config.get("oc_max_dirty", DEFAULT_DIRTY)
    export_path = "/ocpec_oc_existing"
    mount_point = "/mnt/ocpec_oc_existing"
    try:
        _create_cluster(ceph_cluster, client, nfs_name, server, port)
        _create_export(client, nfs_name, export_path)
        verify_nfs_object_cache(ceph_cluster, client, nfs_name, expect_on=False)
        apply_nfs_object_cache(
            installer, nfs_name, enable=True, size=size, max_dirty=dirty
        )
        verify_nfs_object_cache(
            ceph_cluster, client, nfs_name, expect_on=True, size=size, max_dirty=dirty
        )
        _do_mount(
            client,
            mount_point,
            "4.2",
            port,
            _server_ip(ceph_cluster, server),
            export_path,
        )
        _io_unmount_remount(
            client,
            mount_point,
            "4.2",
            port,
            _server_ip(ceph_cluster, server),
            export_path,
        )
    finally:
        _full_cleanup(client, nfs_name, [export_path], [mount_point])


def scenario_pec_cmount_path(ceph_cluster, config):
    client, server, _installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", DEFAULT_CLUSTER)
    port = str(config.get("port", DEFAULT_PORT))
    n_exports = int(config.get("exports", 3))
    exports = [f"/ocpec_pec_{i}" for i in range(1, n_exports + 1)]
    mounts = [f"/mnt/ocpec_pec_{i}" for i in range(1, n_exports + 1)]
    try:
        _create_cluster(ceph_cluster, client, nfs_name, server, port)
        for export_path in exports:
            _create_export(client, nfs_name, export_path, cmount_path=True)
        sleep(5)
        # Mount first: CephFS asoks appear after Ganesha opens clients.
        server_ip = _server_ip(ceph_cluster, server)
        for export_path, mount_point in zip(exports, mounts):
            _do_mount(client, mount_point, "4.2", port, server_ip, export_path)
            # Force Ganesha to open the CephFS client (asok appears on use).
            client.exec_command(sudo=True, cmd=f"ls {mount_point}", timeout=30)
        verify_pec(ceph_cluster, client, nfs_name, expect_clients=n_exports)
        for export_path, mount_point in zip(exports, mounts):
            _io_unmount_remount(
                client, mount_point, "4.2", port, server_ip, export_path
            )
    finally:
        _full_cleanup(client, nfs_name, exports, mounts)


def scenario_pec_then_enable_oc(ceph_cluster, config):
    client, server, installer = _setup_nodes(ceph_cluster, config)
    nfs_name = config.get("nfs_name", DEFAULT_CLUSTER)
    port = str(config.get("port", DEFAULT_PORT))
    size = config.get("oc_size", DEFAULT_SIZE)
    dirty = config.get("oc_max_dirty", DEFAULT_DIRTY)
    n_exports = int(config.get("exports", 3))
    exports = [f"/ocpec_both_{i}" for i in range(1, n_exports + 1)]
    mounts = [f"/mnt/ocpec_both_{i}" for i in range(1, n_exports + 1)]
    try:
        _create_cluster(ceph_cluster, client, nfs_name, server, port)
        for export_path in exports:
            _create_export(client, nfs_name, export_path, cmount_path=True)
        sleep(5)
        server_ip = _server_ip(ceph_cluster, server)
        for export_path, mount_point in zip(exports, mounts):
            _do_mount(client, mount_point, "4.2", port, server_ip, export_path)
            # Force Ganesha to open the CephFS client (asok appears on use).
            client.exec_command(sudo=True, cmd=f"ls {mount_point}", timeout=30)
        verify_pec(ceph_cluster, client, nfs_name, expect_clients=n_exports)
        verify_nfs_object_cache(ceph_cluster, client, nfs_name, expect_on=False)
        apply_nfs_object_cache(
            installer, nfs_name, enable=True, size=size, max_dirty=dirty
        )
        verify_nfs_object_cache(
            ceph_cluster, client, nfs_name, expect_on=True, size=size, max_dirty=dirty
        )
        # OC apply redeploys Ganesha; remount to reopen PEC CephFS clients.
        for mount_point in mounts:
            _do_unmount(client, mount_point)
        for export_path, mount_point in zip(exports, mounts):
            _do_mount(client, mount_point, "4.2", port, server_ip, export_path)
            client.exec_command(sudo=True, cmd=f"ls {mount_point}", timeout=30)
        verify_pec(ceph_cluster, client, nfs_name, expect_clients=n_exports)
        for export_path, mount_point in zip(exports, mounts):
            _io_unmount_remount(
                client, mount_point, "4.2", port, server_ip, export_path
            )
    finally:
        _full_cleanup(client, nfs_name, exports, mounts)


SCENARIO_MAP = {
    "oc_on": scenario_oc_on,
    "enable_oc_on_existing": scenario_enable_oc_on_existing,
    "pec_cmount_path": scenario_pec_cmount_path,
    "pec_then_enable_oc": scenario_pec_then_enable_oc,
    "shared_to_pec_via_apply": scenario_shared_to_pec_via_apply,
}


def run(ceph_cluster, **kw):
    config = kw.get("config") or {}
    if skip_oc_pec_unless_supported(ceph_cluster):
        log.info("TEST SKIPPED - OC/PEC requires live Ceph >= 20.2.2")
        return 0
    scenario_key = config.get("scenario")
    if not scenario_key:
        raise ConfigError(f"config['scenario'] required: {list(SCENARIO_MAP)}")
    scenario_fn = SCENARIO_MAP.get(scenario_key)
    if not scenario_fn:
        raise ConfigError(f"Unknown scenario {scenario_key!r}")

    # PEC asok listing needs client.nfs sockets under /run/ceph.
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
                "TEST PASSED - scenario=%s OC/PEC workflow completed",
                scenario_key,
            )
    return rc
