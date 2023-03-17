from json import loads

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from utility.log import Log

log = Log(__name__)

DEFAULT_CEPH_DIR = "/etc/ceph/"
DEFAULT_CONF_PATH = "/etc/ceph/ceph.conf"


class ClusterViewError(Exception):
    pass


def verify_mon_service(host):
    """
    Verify MON service is up
    Args:
        host(ceph): Installer host
    """
    timeout, interval = 30, 5
    for w in WaitUntil(timeout=timeout, interval=interval):
        configs = loads(CephAdm(host).ceph.orch.ls(service_type="mon", format="json"))
        running = configs[-1].get("status", {}).get("running")
        size = configs[-1].get("status", {}).get("size")
        if running == size:
            log.info("All MON services are up and running")
            return True
    if w.expired:
        raise ClusterViewError(
            f"MON service failed: expected: {size}, running: {running}"
        )


def verify_mon_ips(host):
    """
    Verify MON IPs updated in ceph.conf
    Args:
        host(ceph): Installer host
    """
    # Get updated MON nodes
    configs = loads(CephAdm(host).ceph.orch.ps(service_name="mon", format="json"))
    mon_nodes = [config.get("daemon_id") for config in configs]

    # Get MON node IPs
    configs = loads(CephAdm(host).ceph.orch.host.ls(format="json"))
    mon_ips = [c.get("addr") for c in configs if c.get("hostname") in mon_nodes]

    # Validate MON IPs updated in ceph.conf
    mon_ips_conf = host.remote_file(file_name=DEFAULT_CONF_PATH, file_mode="r")
    mon_ips_conf = mon_ips_conf.readlines()[-1]
    for mon_ip in mon_ips:
        if mon_ip not in mon_ips_conf:
            raise ClusterViewError("MON IPs not updated in ceph.conf")


def run(ceph_cluster, **kwargs):
    """Verify admin node has updated view of cluster
    Args:
        ceph_cluster(ceph.ceph.Ceph): CephNode or list of CephNode object
    """

    # Get installer node
    install_node = ceph_cluster.get_nodes(role="installer")[0]

    # List all nodes from cluster
    all_nodes = loads(CephAdm(install_node).ceph.orch.host.ls(format="json-pretty"))
    all_nodes = [c.get("hostname") for c in all_nodes]

    # List dedicated MON nodes
    mon_nodes = loads(
        CephAdm(install_node).ceph.orch.host.ls(label="mon", format="json-pretty")
    )
    mon_nodes = [c.get("hostname") for c in mon_nodes]

    # Get non MON nodes
    non_mon_nodes = set(all_nodes) - set(mon_nodes)
    if len(non_mon_nodes) == 0:
        raise ClusterViewError("There are no additional nodes present in cluster")

    # Apply MON placement
    conf = {"pos_args": [], "placement": len(all_nodes)}
    out = CephAdm(install_node).ceph.orch.apply(service_name="mon", **conf)
    if "Scheduled mon update" not in out:
        raise ClusterViewError("Apply MON placement failed")

    # Verify MON services are up and running
    verify_mon_service(install_node)

    # Check Ceph health status
    out = CephAdm(install_node).ceph.status()

    # Check if ceph.conf is present
    dir_out = install_node.get_dir_list(dir_path=DEFAULT_CEPH_DIR)
    if "ceph.conf" not in dir_out:
        raise ClusterViewError(f"Ceph.conf not found in {DEFAULT_CEPH_DIR}")

    # Verify MON IPs update in ceph.conf
    verify_mon_ips(install_node)

    # Apply MON on dedicated MON nodes
    conf = {
        "pos_args": [],
        "placement": f"\"{' '.join([str(node) for node in mon_nodes])}\"",
    }
    out = CephAdm(install_node).ceph.orch.apply(service_name="mon", **conf)
    if "Scheduled mon update" not in out:
        raise ClusterViewError("Apply mon placement failed")

    # Verify MON services are up and running
    verify_mon_service(install_node)

    # Verify MON IPs update in ceph.conf
    verify_mon_ips(install_node)

    return 0
