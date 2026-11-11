"""
IBMCEPH-11530 — disable / re-enable NFS Virtual Server without deleting the cluster.

Covers TC-VS8 (disable), TC-VS9 (IO after disable), TC-VS10 (re-enable + second
route), and TC-VS11 (default cluster renders Virtual_Server = false).

Toggle path (developer-confirmed, 9.2):
  ceph orch ls --export → edit enable_virtual_server → ceph orch apply -i
  → ceph orch daemon reconfig  (mandatory; field not in dependency tracker)
"""

from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from tests.nfs.regional_mount import virtual_server as vs
from utility.log import Log

log = Log(__name__)

DEFAULT_CLUSTER_ID = "cephfs-nfs-default"


def _assert_spec_enabled(client, nfs_name, expected):
    value = vs.get_enable_virtual_server_from_spec(client, nfs_name)
    expected_bool = bool(expected)
    is_true = value is True or str(value).lower() == "true"
    if expected_bool and not is_true:
        raise OperationFailedError(
            f"nfs.{nfs_name} enable_virtual_server={value!r}, expected true"
        )
    if not expected_bool and is_true:
        raise OperationFailedError(
            f"nfs.{nfs_name} enable_virtual_server={value!r}, expected not true"
        )
    log.info("nfs.%s enable_virtual_server=%r (expected true=%s)", nfs_name, value, expected_bool)


def _mount_io_unmount(route, export_name, mount_path, version, port, sudo):
    vs.mount_nfs_for_route(route, export_name, mount_path, version, port, sudo=sudo)
    vs.run_nfs_io_for_route(route, mount_path, sudo=sudo)
    vs.unmount_nfs_route(route["client"], mount_path)


def _wait_for_nfs_daemons(client, nfs_name, timeout=180):
    from time import sleep, time

    end = time() + timeout
    while time() < end:
        daemons = vs.list_running_nfs_daemons(client, nfs_name)
        if daemons:
            return daemons
        sleep(10)
    raise OperationFailedError(
        f"Timed out waiting for running nfs.{nfs_name} daemon(s)"
    )


def _run_default_cluster_check(client, nfs_nodes, nfs_server, config):
    """TC-VS11: cluster create without --enable-virtual-server → Virtual_Server = false."""
    default_name = config.get("default_nfs_name", DEFAULT_CLUSTER_ID)
    log.info(
        "TC-VS11: creating default NFS cluster %s (no virtual-server flag)",
        default_name,
    )
    try:
        Ceph(client).nfs.cluster.create(
            name=default_name,
            nfs_server=nfs_server,
            enable_virtual_server=False,
        )
        _wait_for_nfs_daemons(client, default_name)
        flag = vs.get_enable_virtual_server_from_spec(client, default_name)
        if flag is True or str(flag).lower() == "true":
            raise OperationFailedError(
                f"Default cluster {default_name} unexpectedly has "
                f"enable_virtual_server={flag!r}"
            )
        vs.assert_virtual_server_conf(nfs_nodes, default_name, expected_enabled=False)
        log.info("TC-VS11 passed: Virtual_Server = false on default cluster")
    finally:
        try:
            Ceph(client).nfs.cluster.delete(default_name)
        except Exception as exc:
            log.warning("TC-VS11 cleanup delete %s: %s", default_name, exc)


def run(ceph_cluster, **kw):
    """
    Scenario 6 — IBMCEPH-11530 virtual-server disable / re-enable toggle.

    1. Create VS cluster, assert Virtual_Server = true (VS1/VS2 setup).
    2. Disable via orch apply + reconfig; assert false + cluster/export kept (VS8).
    3. Mount + IO while disabled (VS9). Extra NICs may still answer — not a fail.
    4. Re-enable; assert true; mount + IO on two routes (VS10).
    5. Optional: create a default (non-VS) cluster and assert Virtual_Server = false (VS11).
    """
    config = kw.get("config") or {}
    clients = vs.get_clients(ceph_cluster, {**config, "clients": 1})
    client = clients[0]
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    nfs_node = nfs_nodes[0]
    nfs_server = vs.get_nfs_server_hostname(ceph_cluster)
    names = vs.default_nfs_names()
    version = config.get("nfs_version", "4.2")
    port = config.get("port", "2049")
    export_name = vs.export_name_for_index(names["nfs_export"], 0, single_export=True)
    sudo = vs.io_sudo_from_config(config)
    run_vs11 = config.get("run_vs11", True)

    try:
        routes = vs.discover_and_verify_routes(nfs_node, client, config)
        if len(routes) < 2:
            raise OperationFailedError(
                f"Need at least 2 routes for TC-VS10; discovered {len(routes)}"
            )

        # TC-VS11 first — avoids port 2049 clash with the VS cluster on the same host.
        if run_vs11:
            _run_default_cluster_check(client, nfs_nodes, nfs_server, config)
        else:
            log.info("TC-VS11 skipped (run_vs11=false)")

        vs.setup_virtual_server_exports(
            ceph_cluster,
            clients,
            nfs_server,
            config,
            single_export=True,
        )
        # VS11 deletes the prior NFS service; wait for the VS daemon before conf asserts.
        _wait_for_nfs_daemons(client, names["nfs_name"])

        # --- Setup gate: VS1 / VS2 ---
        _assert_spec_enabled(client, names["nfs_name"], True)
        vs.assert_virtual_server_conf(nfs_nodes, names["nfs_name"], True)
        log.info("Setup OK: enable_virtual_server true, Virtual_Server = true")

        # --- TC-VS8: disable ---
        log.info("TC-VS8: disabling virtual server via orch apply + reconfig")
        vs.set_virtual_server_enabled(client, names["nfs_name"], False)
        _assert_spec_enabled(client, names["nfs_name"], False)
        vs.assert_virtual_server_conf(nfs_nodes, names["nfs_name"], False)
        vs.assert_cluster_and_export_preserved(client, names["nfs_name"], export_name)
        log.info("TC-VS8 passed: Virtual_Server = false; cluster and export preserved")

        # --- TC-VS9: data path while disabled ---
        log.info("TC-VS9: mount + IO while Virtual_Server = false")
        route0 = routes[0]
        mount0 = vs.mount_path_for_route(
            names["nfs_mount"], f"toggle_off_{route0['route_id']}"
        )
        _mount_io_unmount(route0, export_name, mount0, version, port, sudo)
        daemons = vs.list_running_nfs_daemons(client, names["nfs_name"])
        if not daemons:
            raise OperationFailedError("No running NFS daemons after disable (TC-VS9)")
        log.info(
            "TC-VS9 passed: IO OK while disabled; %d daemon(s) running", len(daemons)
        )

        # --- TC-VS10: re-enable + two routes ---
        log.info("TC-VS10: re-enabling virtual server")
        vs.set_virtual_server_enabled(client, names["nfs_name"], True)
        _assert_spec_enabled(client, names["nfs_name"], True)
        vs.assert_virtual_server_conf(nfs_nodes, names["nfs_name"], True)
        vs.assert_cluster_and_export_preserved(client, names["nfs_name"], export_name)

        for route in routes[:2]:
            mount_path = vs.mount_path_for_route(
                names["nfs_mount"], f"toggle_on_{route['route_id']}"
            )
            _mount_io_unmount(route, export_name, mount_path, version, port, sudo)
        log.info("TC-VS10 passed: Virtual_Server = true; IO on two routes")

        log.info(
            "Scenario 6 passed: IBMCEPH-11530 disable/re-enable toggle "
            "(VS8–VS10%s)",
            ", VS11" if run_vs11 else "",
        )
        return 0
    except Exception as exc:
        log.error("Scenario 6 (virtual-server toggle) failed: %s", exc)
        return 1
    finally:
        vs.cleanup_virtual_server(
            clients,
            names["nfs_name"],
            names["nfs_export"],
            nfs_nodes=nfs_nodes,
            fs_name=names["fs_name"],
        )
