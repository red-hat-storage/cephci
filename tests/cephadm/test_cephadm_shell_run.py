from ceph.ceph_admin import CephAdmin
from utility.log import Log

log = Log(__name__)


def getting_non_mon_node(cluster_instance):
    """
    Method return non mon instance node
    Args:
        node: cluster instance
    Returns:
        non mon instance
    """

    all_nodes = cluster_instance.cluster.get_nodes()
    mon_nodes = cluster_instance.cluster.get_nodes(role="mon")
    non_mon_nodes = []
    for node in all_nodes:
        if node not in mon_nodes:
            non_mon_nodes.append(node)
    return non_mon_nodes[0]


def create_mon_daemon_file(node):
    """
    Method create mon dir in /var/lib/ceph and create a daemon specific folder/file, like ceph-doc or ceph-test
    Args:
        node: node object
    Returns:
        boolean
    """

    try:
        out, _ = node.exec_command(
            cmd="mkdir -p /var/lib/ceph && touch /var/lib/ceph/ceph-test", sudo=True
        )
        return 0
    except Exception as e:
        log.error(e)
        return 1


def run(ceph_cluster, **kw):
    """Verification cephadm shell when running from a non-monitor node
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    Returns:
        0 - if test case pass
        1 - it test case fails
    Test case covered : CEPH-83575009
    Test Case Flow:
    1. Deploy a 4.x cluster
    2. Upgrade the cluster to 5.x
    3. Create a mon dir in /var/lib/ceph and create a daemon specific folder/file, like ceph-doc or ceph-test
    4. Run cephadm shell
    """

    try:
        config = kw.get("config")

        # Getting non mon node
        log.info("Getting non mon node")
        instance = CephAdmin(cluster=ceph_cluster, **config)
        non_mon_node = getting_non_mon_node(instance)
        log.info("Successfully selected non mon node")

        # Creating a daemon specfic folder
        log.info("Creating a mon dir and daemon specfic file")
        create_mon_daemon_file(non_mon_node)
        log.info("Successfully created a mon dir and daemon specfic file")

        # Running cephadm shell
        log.info("Running cephadm shell")
        out, _ = non_mon_node.exec_command(cmd="cephadm -v shell ceph -s", sudo=True)
        log.info("cephadm shell successfully running from a non-monitor node")
        return 0

    except Exception as e:
        log.error(e)
        return 1
