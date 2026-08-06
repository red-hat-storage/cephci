"""Scenario 3: multiple exports, multiple clients, each via a different route."""

from tests.nfs.regional_mount import virtual_server as vs
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Regional File Mount / Virtual Server — scenario 3.

    Multiple exports; each client mounts its export over a distinct route.
    """
    config = kw.get("config") or {}
    clients = vs.get_clients(ceph_cluster, config)
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    nfs_node = nfs_nodes[0]
    nfs_server = vs.get_nfs_server_hostname(ceph_cluster)
    names = vs.default_nfs_names()
    version = config.get("nfs_version", "4.2")
    port = config.get("port", "2049")
    export_num = int(config.get("export_num", len(clients)))
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
        export_names = vs.export_names_for_count(export_num, names["nfs_export"])

        if len(routes) != len(export_names):
            log.warning(
                f"Route count ({len(routes)}) != export count ({len(export_names)}). "
                f"Will process {min(len(routes), len(export_names))} mounts."
            )

        for route, export_name in zip(routes, export_names):
            mount_path = vs.mount_path_for_route(names["nfs_mount"], route["route_id"])
            log.info(f"Mounting export {export_name} on route {route['route_id']}")
            vs.mount_nfs_for_route(
                route, export_name, mount_path, version, port, sudo=sudo
            )
            vs.run_nfs_io_for_route(route, mount_path, sudo=sudo)

        log.info("Scenario 3 passed: multi-export multi-route IO succeeded")
        return 0
    except Exception as exc:
        log.error("Scenario 3 failed: %s", exc)
        return 1
    finally:
        vs.cleanup_virtual_server(
            clients,
            names["nfs_name"],
            names["nfs_export"],
            export_names=vs.export_names_for_count(
                int(config.get("export_num", len(clients))),
                names["nfs_export"],
            ),
            nfs_nodes=nfs_nodes,
            fs_name=names["fs_name"],
        )
