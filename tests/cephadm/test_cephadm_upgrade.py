"""
Test module that verifies the Upgrade of Ceph Storage via the cephadm CLI.

"""
from ceph.ceph_admin.orch import Orch
from ceph.rados.rados_bench import RadosBench
from utility.log import Log

log = Log(__name__)


class UpgradeFailure(Exception):
    pass


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
    log.info("Upgrade Ceph cluster...")
    config = kwargs["config"]
    config["overrides"] = kwargs.get("test_data", {}).get("custom-config")
    orch = Orch(cluster=ceph_cluster, **config)

    client = ceph_cluster.get_nodes(role="client")[0]
    clients = ceph_cluster.get_nodes(role="client")
    executor = None

    # ToDo: Switch between the supported IO providers
    if config.get("benchmark"):
        executor = RadosBench(mon_node=client, clients=clients)

    try:
        # Initiate thread pool to run rados bench
        if executor:
            executor.run(config=config["benchmark"])

        # Set repo to newer RPMs
        orch.set_tool_repo()

        # Install cephadm
        orch.install()

        # Check service versions vs available and target containers
        orch.upgrade_check(image=config.get("container_image"))

        # work around for upgrading from 5.1 and 5.2 to 5.1 and 5.2 latest
        installer = ceph_cluster.get_nodes(role="installer")[0]
        base_cmd = "sudo cephadm shell -- ceph"
        ceph_version, err = installer.exec_command(cmd=f"{base_cmd} version")
        if ceph_version.startswith("ceph version 16.2."):
            installer.exec_command(
                cmd=f"{base_cmd} config set mgr mgr/cephadm/no_five_one_rgw true --force"
            )
            installer.exec_command(cmd=f"{base_cmd} orch upgrade stop")

        # Start Upgrade
        config.update({"args": {"image": "latest"}})
        orch.start_upgrade(config)

        # Monitor upgrade status, till completion
        orch.monitor_upgrade_status()

        if config.get("verify_cluster_health"):
            if orch.cluster.check_health(
                rhbuild=config.get("rhbuild"), client=orch.installer
            ):
                raise UpgradeFailure("Cluster is in HEALTH_ERR state")
    except BaseException as be:  # noqa
        log.error(be, exc_info=True)
        return 1
    finally:
        if executor:
            executor.teardown()

        # Get cluster state
        orch.get_cluster_state(
            [
                "ceph status",
                "ceph versions",
                "ceph orch ps -f yaml",
                "ceph orch ls -f yaml",
                "ceph orch upgrade status",
                "ceph mgr dump",  # https://bugzilla.redhat.com/show_bug.cgi?id=2033165#c2
                "ceph mon stat",
            ]
        )

    return 0
