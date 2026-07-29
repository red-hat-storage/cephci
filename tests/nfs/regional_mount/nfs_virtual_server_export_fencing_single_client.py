"""Scenario 4: export fencing — only the configured server/client IP pair may mount."""

from tests.nfs.regional_mount import virtual_server as vs
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Regional File Mount / Virtual Server — scenario 4.

    Apply Server_Addrs and CLIENT fencing; verify allowed route mounts and others fail.
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

    try:
        routes = vs.discover_and_verify_routes(nfs_node, client, config)
        vs.setup_virtual_server_exports(
            ceph_cluster,
            clients,
            nfs_server,
            config,
            single_export=True,
        )

        allowed = routes[0]
        vs.apply_export_fencing_for_route(
            client, names["nfs_name"], export_name, allowed
        )

        mount_ok = vs.mount_path_for_route(names["nfs_mount"], "allowed")
        vs.mount_nfs_for_route(allowed, export_name, mount_ok, version, port, sudo=sudo)
        vs.run_nfs_io_for_route(allowed, mount_ok, sudo=sudo)
        vs.unmount_nfs_route(client, mount_ok)

        for route in routes[1:]:
            mount_bad = vs.mount_path_for_route(
                names["nfs_mount"], f"deny_{route['route_id']}"
            )
            vs.assert_mount_fails_for_route(
                route, export_name, mount_bad, version, port
            )

        log.info("Scenario 4 passed: fencing enforced on single client")
        return 0
    except Exception as exc:
        log.error("Scenario 4 failed: %s", exc)
        return 1
    finally:
        try:
            log.info("Starting cleanup of virtual server resources")
            vs.cleanup_virtual_server(
                clients,
                names["nfs_name"],
                names["nfs_export"],
                nfs_nodes=nfs_nodes,
                fs_name=names["fs_name"],
            )
            log.info("Cleanup completed successfully")
        except Exception as cleanup_exc:
            log.error(f"Cleanup failed: {cleanup_exc}")
            log.error("Manual cleanup may be required")
            # Don't raise - we want to preserve original exception if any
