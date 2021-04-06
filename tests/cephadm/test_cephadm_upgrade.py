"""
Test module that verifies the Upgrade of Ceph Storage via the cephadm CLI.

"""
import logging

from ceph.ceph_admin.orch import Orch

LOG = logging.getLogger()


def run(ceph_cluster, **kwargs) -> int:
    """
    Upgrade the cluster to latest version and verify the status

    Args:
        ceph_cluster: Ceph cluster object
        kwargs:     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:
        - test:
            name: Upgrade cluster
            desc: Upgrade to latest version
            config:
                command: start
                service: upgrade
                base_cmd_args:
                    verbose: true

    Since image are part of main config, no need of any args here.
    """
    LOG.info("Upgrade Ceph cluster...")
    config = kwargs["config"]
    orch = Orch(cluster=ceph_cluster, **config)
    try:
        # Set repo to newer RPMs
        orch.set_tool_repo()

        # Install cephadm
        orch.install()

        # Check service versions vs available and target containers
        orch.upgrade_check(image=config.get("container_image"))

        # Start Upgrade
        config.update({"args": {"image": "latest"}})
        orch.start_upgrade(config)

        # Monitor upgrade status, till completion
        orch.monitor_upgrade_status()

        if config.get("verify_cluster_health"):
            orch.cluster.check_health(
                rhbuild=config.get("rhbuild"), client=orch.installer
            )

    except BaseException as be:  # noqa
        LOG.error(be, exc_info=True)
        return 1
    finally:
        # Get cluster state
        orch.get_cluster_state(
            [
                "ceph status",
                "ceph versions",
                "ceph orch ps -f yaml",
                "ceph orch ls -f yaml",
                "ceph orch upgrade status",
            ]
        )
    return 0
