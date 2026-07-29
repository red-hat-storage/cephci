"""Scenario 5: fence one export; other exports remain accessible on all routes."""

from tests.nfs.regional_mount import virtual_server as vs
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Regional File Mount / Virtual Server — scenario 5.

    Fence export_0 to one server/client pair; export_1 must mount from all clients/routes.
    """
    config = kw.get("config") or {}
    clients = vs.get_clients(ceph_cluster, config)
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    nfs_node = nfs_nodes[0]
    nfs_server = vs.get_nfs_server_hostname(ceph_cluster)
    names = vs.default_nfs_names()
    version = config.get("nfs_version", "4.2")
    port = config.get("port", "2049")
    export_num = max(2, int(config.get("export_num", 2)))
    export_names = vs.export_names_for_count(export_num, names["nfs_export"])
    fenced_export = export_names[0]
    open_export = export_names[1]
    sudo = vs.io_sudo_from_config(config)

    try:
        routes = vs.discover_and_verify_routes(nfs_node, clients, config)

        vs.setup_virtual_server_exports(
            ceph_cluster,
            clients,
            nfs_server,
            config,
            export_num=export_num,
        )

        fence_route = routes[0]
        vs.apply_export_fencing_for_route(
            clients[0], names["nfs_name"], fenced_export, fence_route
        )

        mount_ok = vs.mount_path_for_route(names["nfs_mount"], "fenced_ok")
        vs.mount_nfs_for_route(
            fence_route, fenced_export, mount_ok, version, port, sudo=sudo
        )
        vs.run_nfs_io_for_route(fence_route, mount_ok, sudo=sudo)
        vs.unmount_nfs_route(fence_route["client"], mount_ok)

        for route in routes:
            if (
                route["server_ip"] == fence_route["server_ip"]
                and route["client_ip"] == fence_route["client_ip"]
            ):
                continue
            mount_bad = vs.mount_path_for_route(
                names["nfs_mount"], f"fenced_deny_{route['route_id']}"
            )
            vs.assert_mount_fails_for_route(
                route, fenced_export, mount_bad, version, port
            )

        for route in routes:
            mount_open = vs.mount_path_for_route(
                names["nfs_mount"], f"open_{route['route_id']}"
            )
            vs.mount_nfs_for_route(
                route, open_export, mount_open, version, port, sudo=sudo
            )
            vs.run_nfs_io_for_route(route, mount_open, sudo=sudo)
            vs.unmount_nfs_route(route["client"], mount_open)

        log.info(
            "Scenario 5 passed: fenced export restricted; open export multi-route OK"
        )
        return 0
    except Exception as exc:
        log.error("Scenario 5 failed: %s", exc)
        return 1
    finally:
        try:
            log.info("Starting cleanup of virtual server resources")
            vs.cleanup_virtual_server(
                clients,
                names["nfs_name"],
                names["nfs_export"],
                export_names=export_names,
                nfs_nodes=nfs_nodes,
                fs_name=names["fs_name"],
            )
            log.info("Cleanup completed successfully")
        except Exception as cleanup_exc:
            log.error("Cleanup failed: %s", cleanup_exc)
            log.error("Manual cleanup may be required")
