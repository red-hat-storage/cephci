from ceph.ceph_admin import CephAdmin
from utility.log import Log

log = Log(__name__)


class AdminLableValidationError(Exception):
    pass


def _add_host_label(instance):
    """
    Method add multiple labels to cephadmin node
    Args:
        instance: cluster instance
    Returns:
        True|False
    """

    node = instance.cluster.get_nodes()[0].hostname
    node_ip = instance.cluster.get_nodes()[0].ip_address
    cmd = (
        f"cephadm shell ceph orch host add {node} {node_ip} node-exporter "
        "crash osd installer grafana prometheus alertmanager"
    )
    result = instance.installer.exec_command(cmd, sudo=True)
    if result:
        log.info("Multiple labels added to node")
        return 0
    raise AdminLableValidationError("Failed to add multiple label to node")


def run(ceph_cluster, **kw):
    """
    Verify adding admin node again with different labels.

    Args:
        ceph_cluster: Ceph cluster object
        **kw :     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).
    """

    config = kw.get("config")

    # Checking files in path /etc/ceph before adding multiple lable
    instance = CephAdmin(cluster=ceph_cluster, **config)
    cmd = "ls /etc/ceph"
    out, _ = instance.installer.exec_command(cmd, sudo=True)
    files_before = out.split("\n")
    log.info(
        f"File exist in path /etc/ceph/ before adding multiple lables '{files_before}'"
    )

    # Adding multiple roles in same admin node
    _add_host_label(instance)

    # Checking cluster health and files in path /etc/ceph after multiple lable
    health, _ = instance.installer.exec_command(
        cmd="cephadm shell ceph -s | grep health:", sudo=True
    )
    log.info("Cluster health {}".format(health))
    ceph_dir = "/etc/ceph"
    out, _ = instance.installer.exec_command(cmd="ls {ceph_dir}", sudo=True)
    files_after = out.split("\n")
    log.info(
        f"File exist in path '{ceph_dir}' after adding multiple lables '{files_after}'"
    )

    if "HEALTH_OK" not in health and files_before != files_after:
        raise AdminLableValidationError("Test case failed, Admin node removed")
    log.info("Cluster health is OK and files are same")
    return 0
