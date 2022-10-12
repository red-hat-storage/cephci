from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Verify if the cephadm shell tries to infer config from
    a dummy mon created.
    Verifies bz:2080242

    Args:
        ceph_cluster: Ceph cluster object
        **kw :     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).
    """

    config = kw.get("config")

    # Creating a dummy mon on a non monitor node
    for node in ceph_cluster.get_nodes():
        if node.role not in ["mon", "client"]:
            # Create a dummy mon
            hostname = node.hostname
            dummy_mon = f"sudo touch /var/lib/ceph/mon/ceph-{hostname}"
            _, err = node.exec_command(cmd=f"{dummy_mon}")
            if err:
                log.error("Failed to create dummy mon")
                return 1

            # Try cephadm shell command to make sure the above dummy mon doesn't interfere
            # Checking cluster health
            status = ceph_cluster.check_health(
                rhbuild=config.get("rhbuild"), client=node
            )
            if status:
                log.error(
                    "Unexpected!! Cephadm shell is trying to access config from the dummy mon"
                )
                return 1
            log.info(
                "Expected. Cephadm shell doesn't tries to infer the config from the dummy mon"
            )
            break

    return 0
