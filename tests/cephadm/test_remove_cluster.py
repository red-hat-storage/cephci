from ceph.ceph import CommandFailed
from cli.cephadm.cephadm import CephAdm
from cli.cephadm.exceptions import CephadmOpsExecutionError
from cli.utilities.utils import get_disk_list, get_running_containers
from utility.log import Log

log = Log(__name__)

CEPH_CONFIG_DIR = "/etc/ceph"
CEPH_LIB_DIR = "/var/lib/ceph"
CEPHADM_LIB_DIR = "/var/lib/cephadm"


def validate_ceph_config_dir(nodes, dir_path, roles=[], ignore_entries=[]):
    """Validate ceph config directory

    Args:
        nodes (Ceph): List of Ceph objects
        roles (list|str): Ceph object role
        dir_path (str): Directory to be validated
        ignore_entries (list|str): Ignore directory entry
    """
    if isinstance(roles, str):
        roles = [str(roles)]

    if isinstance(ignore_entries, str):
        ignore_entries = [str(ignore_entries)]

    for node in nodes:
        if node.role not in roles:
            continue

        dir_list = node.get_dir_list(sudo=True, dir_path=CEPH_CONFIG_DIR)
        if dir_list or dir_list not in [ignore_entries, None]:
            raise CephadmOpsExecutionError(
                f"Failed to clean ceph directory '{dir_path}' on node '{node.ip_address}': {dir_list} "
            )

        log.info(
            f"Ceph directory '{dir_path}' is cleaned as expected on node '{node.ip_address}'"
        )


def validate_running_ceph_containers(nodes, roles=[]):
    """Validate containers running on nodes.

    Args:
        nodes (Ceph): List of Ceph objects
        roles (list|str): Ceph object role
    """
    if isinstance(roles, str):
        roles = [str(roles)]

    for node in nodes:
        if node.role not in roles:
            continue

        containers_list = get_running_containers(node=node, expr="name=ceph-*")
        if containers_list:
            raise CephadmOpsExecutionError(
                f"Failed to clean ceph containers on node '{node.ip_address}': {containers_list}"
            )
        log.info(f"Ceph containers are cleaned as expected on node '{node.ip_address}'")


def validate_ceph_disks(nodes):
    """Validate disks used by ceph cluster

    Args:
        nodes (Ceph): List of Ceph objects
    """
    for node in nodes:
        try:
            disks_list = get_disk_list(node=node, expr="ceph-")
            raise CephadmOpsExecutionError(
                f"Failed to clean ceph disks on node '{node.ip_address}: {disks_list}'"
            )
        except CommandFailed:
            log.info(f"Ceph disks are cleaned as expected on node '{node.ip_address}'")


def run(ceph_cluster, **kw):
    """Cephadm remove cluster"""
    nodes = ceph_cluster.get_nodes()
    installer = ceph_cluster.get_ceph_object("installer")

    fsid = CephAdm(installer).ceph.fsid()
    if not fsid:
        raise CephadmOpsExecutionError("Failed to get cluster FSID")

    if CephAdm(installer).ceph.mgr.module_disable("cephadm"):
        raise CephadmOpsExecutionError("Failed to disable cephadm module")

    for node in nodes:
        if not node.role == "osd":
            continue

        if CephAdm(node).rm_cluster(fsid):
            raise CephadmOpsExecutionError(
                f"Failed to execute rm-cluster on node '{node.ip_address}'"
            )
    log.info("Executed 'rm-cluster' command sucessfully on all cluster nodes")

    validate_ceph_config_dir(
        nodes=nodes, dir_path=CEPH_CONFIG_DIR, roles=["mon", "osd"]
    )
    validate_ceph_config_dir(nodes=nodes, dir_path=CEPH_LIB_DIR, roles=["mon", "osd"])
    validate_ceph_config_dir(
        nodes=nodes, dir_path=CEPHADM_LIB_DIR, ignore_entries=[".ssh"]
    )

    validate_running_ceph_containers(nodes=nodes, roles=["mon", "osd"])

    validate_ceph_disks(nodes=nodes)

    if CephAdm(installer).ceph.status():
        raise CephadmOpsExecutionError("Failed to clean ceph cluster")

    return 0
