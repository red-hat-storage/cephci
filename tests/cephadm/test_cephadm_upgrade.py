"""
Test module that verifies the Upgrade of Ceph Storage via the cephadm CLI.

"""

from ceph.ceph_admin.orch import Orch
from ceph.rados.rados_bench import RadosBench
from ceph.utils import is_legacy_container_present, remove_repos
from cephci.utils.build_info import CephTestManifest
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
    config["overrides"] = kwargs.get("test_data", {}).get("custom_config_dict")
    verify_image = bool(config.get("verify_cluster_health", False))

    ctm: CephTestManifest = config["manifest"]

    # Check if test requires overrides
    if config.get("args", {}).get("rhcs-version", None):

        _ceph_version = config["args"]["rhcs-version"]
        _target_release = config["args"].get("release", "released")
        _platform = config["args"].get("platform", config["platform"])
        ctm = CephTestManifest(
            product=config["product"],
            release=_ceph_version,
            build_type=_target_release,
            platform=_platform,
        )

        _rhcs_version = config.get("args").get("rhcs-version", None)
        _rhcs_release = config.get("args").get("release", None)

        log.debug("Build details fetched from manifest:")
        log.debug("  Base URL     : %s", ctm.repository)
        log.debug("  Registry     : %s", ctm.ceph_image_dtr)
        log.debug("  Image Name   : %s", ctm.ceph_image_path)
        log.debug("  Image Tag    : %s", ctm.ceph_image_tag)
        log.debug("  Custom Images: %s", ctm.custom_images)
        # The cluster object is configured so that the values are persistent till
        # an upgrade occurs. This enables us to execute the test in the right
        # context.
        config["base_url"] = ctm.repository
        config["container_image"] = ctm.ceph_image
        config["ceph_docker_registry"] = ctm.ceph_image_dtr
        config["ceph_docker_image"] = ctm.ceph_image_path
        config["ceph_docker_image_tag"] = ctm.ceph_image_tag
        ceph_cluster.rhcs_version = ctm.release
        config["rhbuild"] = f"{ctm.release}-{ctm.platform}"
        config["args"]["rhcs-version"] = _rhcs_version
        config["args"]["release"] = _rhcs_release
        config["args"]["image"] = config["container_image"]

    # initiate a new object with updated config
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

        # Remove existing repos
        for node in ceph_cluster.get_nodes():
            remove_repos(ceph_node=node)

        # Set repo to newer RPMs
        orch.set_tool_repo()

        # Update cephadm rpms
        orch.install(**{"upgrade": True})

        # Check service versions vs available and target containers
        orch.upgrade_check(image=config.get("container_image"))

        # work around for upgrading from 5.1 and 5.2 to 5.1 and 5.2 latest
        installer = ceph_cluster.get_nodes(role="installer")[0]
        base_cmd = "sudo cephadm shell -- ceph"
        ceph_version, _ = installer.exec_command(cmd=f"{base_cmd} version")
        if ceph_version.startswith("ceph version 16.2."):
            installer.exec_command(
                cmd=f"{base_cmd} config set mgr mgr/cephadm/no_five_one_rgw true --force"
            )
            installer.exec_command(cmd=f"{base_cmd} orch upgrade stop")

        # Start Upgrade
        if not config.get("args", {}).get("rhcs-version", None):
            config.update({"args": {"image": "latest"}})

        orch.start_upgrade(config)

        # Monitor upgrade status, till completion
        orch.monitor_upgrade_status()

        if verify_image:
            # BZ: 2077843
            if "docker.io" in orch.upgrade_status():
                raise UpgradeFailure("docker.io appended to the image.")

        if config.get("verify_cephadm_containers") and is_legacy_container_present(
            ceph_cluster
        ):
            log.info(
                "Checking cluster status to ensure that the legacy services are not being inferred"
            )
            if orch.cluster.check_health(
                rhbuild=config.get("rhbuild"), client=orch.installer
            ):
                raise UpgradeFailure("Cluster is in HEALTH_ERR state after upgrade")

        if config.get("verify_cluster_health"):
            if orch.cluster.check_health(
                rhbuild=config.get("rhbuild"), client=orch.installer
            ):
                raise UpgradeFailure("Cluster is in HEALTH_ERR state")

        ceph_cluster.rhcs_version = config.get("rhbuild")
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
