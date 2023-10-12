from ceph.ceph import CommandFailed
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.utils import get_disk_list, get_running_containers
from utility.log import Log

log = Log(__name__)

CEPH_CONFIG_DIR = "/etc/ceph"
CEPH_LIB_DIR = "/var/lib/ceph"
CEPHADM_LIB_DIR = "/var/lib/cephadm"
CEPH_LOG_DIR = "/var/log/ceph"


def validate_ceph_config_dir(nodes, dir_path, roles=[], ignore=[]):
    """Validate ceph config directory.

    Args:
        nodes (Ceph): list of Ceph objects
        roles (list|str): ceph object role
        dir_path (str): directory to be validated
        ignore (list|str): ignore directory entry
    """
    if isinstance(roles, str):
        roles = [str(roles)]

    if isinstance(ignore, str):
        ignore = [str(ignore)]

    for node in nodes:
        if node.role not in roles:
            continue

        _dirs = node.get_dir_list(sudo=True, dir_path=CEPH_CONFIG_DIR)
        if _dirs or _dirs not in [ignore, None]:
            raise OperationFailedError(
                f"Failed to clean ceph directory '{dir_path}' on node '{node.ip_address}' -\n{_dirs} "
            )

        log.info(
            f"Ceph directory '{dir_path}' is cleaned as expected on node '{node.ip_address}'"
        )


def validate_running_ceph_containers(nodes, roles=[]):
    """Validate containers running on nodes.

    Args:
        nodes (Ceph): list of Ceph objects
        roles (list|str): ceph object role
    """
    if isinstance(roles, str):
        roles = [str(roles)]

    for node in nodes:
        if node.role not in roles:
            continue

        _ctrs, _ = get_running_containers(sudo=True, node=node, expr="name=ceph-*")
        if _ctrs:
            raise OperationFailedError(
                f"Failed to clean ceph containers on node '{node.ip_address}' -\n{_ctrs}"
            )
        log.info(f"Ceph containers are cleaned as expected on node '{node.ip_address}'")


def validate_ceph_disks(nodes):
    """Validate disks used by ceph cluster

    Args:
        nodes (Ceph): list of Ceph objects
    """
    for node in nodes:
        try:
            _disks, _ = get_disk_list(sudo=True, node=node, expr="ceph-")
            raise OperationFailedError(
                f"Failed to clean ceph disks on node '{node.ip_address} -\n{_disks}'"
            )
        except CommandFailed:
            log.info(f"Ceph disks are cleaned as expected on node '{node.ip_address}'")


def run(ceph_cluster, **kw):
    """Cephadm remove cluster"""
    validation = bool(kw.get("config", {}).get("validation", True))
    nodes = ceph_cluster.get_nodes()
    installer = ceph_cluster.get_ceph_object("installer")

    # Get cluster FSID
    fsid = CephAdm(installer).ceph.fsid()
    if not fsid:
        raise OperationFailedError("Failed to get cluster FSID")

    # Disable cephadm module
    error, _ = CephAdm(installer).ceph.mgr.module(action="disable", module="cephadm")
    if error:
        raise OperationFailedError("Failed to disable cephadm module")

    # Execute rm-cluster on all nodes
    log.info("Executed 'rm-cluster' command sucessfully on all cluster nodes")
    for node in nodes:
        if not node.role == "osd":
            continue

        if CephAdm(node).rm_cluster(fsid):
            raise OperationFailedError(
                f"Failed to execute rm-cluster on node '{node.ip_address}'"
            )

    # Check if validation set to `False`
    if not validation:
        log.info("Skipping validation ...")
        return 0

    # Validate ceph config directory, disks and containers
    validate_ceph_config_dir(
        nodes=nodes, dir_path=CEPH_CONFIG_DIR, roles=["mon", "osd"]
    )
    validate_ceph_config_dir(nodes=nodes, dir_path=CEPH_LIB_DIR, roles=["mon", "osd"])
    validate_ceph_config_dir(nodes=nodes, dir_path=CEPHADM_LIB_DIR, ignore=[".ssh"])

    validate_ceph_config_dir(nodes=nodes, dir_path=CEPH_LOG_DIR, roles=["mon", "osd"])

    validate_running_ceph_containers(nodes=nodes, roles=["mon", "osd"])

    validate_ceph_disks(nodes=nodes)

    # Check ceph status to check if it cleaned cluster
    if CephAdm(installer).ceph.status():
        raise OperationFailedError("Failed to clean ceph cluster")

    return 0
