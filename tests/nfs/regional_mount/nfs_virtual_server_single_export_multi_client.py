"""Scenario 2: single export, multiple clients each using a different network route."""

from tests.nfs.regional_mount import virtual_server as vs
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Regional File Mount / Virtual Server — scenario 2.

    One shared export; each client mounts via a distinct route and runs IO.
    """
    config = kw.get("config") or {}
    clients = vs.get_clients(ceph_cluster, config)
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    nfs_node = nfs_nodes[0]
    nfs_server = vs.get_nfs_server_hostname(ceph_cluster)
    names = vs.default_nfs_names()
    version = config.get("nfs_version", "4.2")
    port = config.get("port", "2049")
    export_name = vs.export_name_for_index(names["nfs_export"], 0, single_export=True)
    sudo = vs.io_sudo_from_config(config)

    try:
        routes = vs.discover_and_verify_routes(nfs_node, clients, config)
        vs.setup_virtual_server_exports(
            ceph_cluster,
            clients,
            nfs_server,
            config,
            single_export=True,
        )

        for route in routes:
            mount_path = vs.mount_path_for_route(names["nfs_mount"], route["route_id"])
            vs.mount_nfs_for_route(
                route, export_name, mount_path, version, port, sudo=sudo
            )
            vs.run_nfs_io_for_route(route, mount_path, sudo=sudo)

        log.info("Scenario 2 passed: all clients mounted via distinct routes")
        return 0
    except Exception as exc:
        log.error("Scenario 2 failed: %s", exc)
        return 1
    finally:
        vs.cleanup_virtual_server(
            clients,
            names["nfs_name"],
            names["nfs_export"],
            nfs_nodes=nfs_nodes,
            fs_name=names["fs_name"],
        )
