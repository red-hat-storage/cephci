"""
Test module for performing rack-based OSD upgrades.

This test upgrades OSDs in a rack-based manner, upgrading one rack at a time
to minimize impact on cluster availability and data redundancy.
"""

from ceph.ceph_admin.orch import Orch
from ceph.rados.rados_bench import RadosBench
from ceph.utils import remove_repos
from cephci.utils.build_info import CephTestManifest
from utility.log import Log

log = Log(__name__)


class RackBasedUpgradeFailure(Exception):
    """Exception raised when rack-based upgrade fails."""
    pass


def run(ceph_cluster, **kwargs) -> int:
    """
    Perform rack-based OSD upgrade.

    This test upgrades OSDs rack by rack, ensuring high availability during
    the upgrade process. Each rack is upgraded sequentially, and cluster
    health is verified between rack upgrades.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:
        - test:
            name: Rack-based OSD upgrade
            desc: Upgrade OSDs rack by rack to latest version
            module: test_rack_based_upgrade.py
            config:
                command: start
                service: upgrade
                verify_cluster_health: true
                base_cmd_args:
                    verbose: true
                args:
                    image: "latest"
    """
    log.info("Starting rack-based OSD upgrade...")

    config = kwargs["config"]
    config["overrides"] = kwargs.get("test_data", {}).get("custom_config_dict")
    verify_health = bool(config.get("verify_cluster_health", True))

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

    # Initiate a new object with updated config
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
        # orch.upgrade_check(image=config.get("container_image"))

        # Get all OSD hosts and detect racks from CRUSH map
        log.info("Detecting OSD hosts and rack assignments from CRUSH map...")
        osd_hosts = ceph_cluster.get_nodes(role="osd")
        if not osd_hosts:
            log.error("No OSD hosts found in the cluster")
            return 1

        log.info(f"Found {len(osd_hosts)} OSD hosts")

        # Get CRUSH tree to detect rack assignments
        from json import loads
        out, _ = orch.shell(args=["ceph", "osd", "tree", "-f", "json"])
        crush_tree = loads(out)

        # Build rack distribution from CRUSH map
        rack_distribution = {}
        host_to_rack = {}

        # Parse CRUSH tree to find rack->host mappings
        for node in crush_tree.get("nodes", []):
            if node.get("type") == "rack":
                rack_name = node.get("name")
                if rack_name and rack_name not in rack_distribution:
                    rack_distribution[rack_name] = []

        # Find hosts under each rack
        for node in crush_tree.get("nodes", []):
            if node.get("type") == "host":
                host_name = node.get("name")
                # Find parent rack by traversing up the tree
                for rack_node in crush_tree.get("nodes", []):
                    if rack_node.get("type") == "rack" and node.get("id") in rack_node.get("children", []):
                        rack_name = rack_node.get("name")
                        if rack_name in rack_distribution:
                            rack_distribution[rack_name].append(host_name)
                            host_to_rack[host_name] = rack_name
                        break

        # Log detected rack assignments
        for host in osd_hosts:
            rack_name = host_to_rack.get(host.hostname, None)
            log.info(f"  - {host.hostname} (rack: {rack_name if rack_name else 'not specified'})")

        if not rack_distribution or all(len(hosts) == 0 for hosts in rack_distribution.values()):
            log.error("No rack assignments found in CRUSH map")
            return 1

        rack_names = sorted(rack_distribution.keys())
        log.info(f"Detected racks for upgrade: {rack_names}")

        # Log rack distribution
        for rack_name, hosts in rack_distribution.items():
            log.info(f"Rack {rack_name}: {len(hosts)} hosts - {', '.join(hosts)}")

        # Start Upgrade - set default image if rhcs-version not provided
        if not config.get("args", {}).get("rhcs-version", None):
            config.update({"args": {"image": "latest"}})

        # Upgrade OSDs rack by rack
        for rack_name in rack_names:
            log.info(f"\n{'='*80}")
            log.info(f"Upgrading OSDs in Rack: {rack_name}")
            log.info(f"Hosts in rack: {', '.join(rack_distribution[rack_name])}")
            log.info(f"{'='*80}\n")

            # Prepare config for this rack's upgrade
            rack_config = config.copy()
            rack_config["args"] = config.get("args", {}).copy()
            rack_config["args"]["daemon_types"] = "osd"
            rack_config["args"]["crush_bucket_type"] = "rack"
            rack_config["args"]["crush_bucket_name"] = rack_name

            # Start upgrade for this rack
            log.info(f"Starting OSD upgrade for rack {rack_name}...")
            orch.start_upgrade(rack_config)

            # Monitor upgrade status for this rack
            log.info(f"Monitoring upgrade progress for rack {rack_name}...")
            orch.monitor_upgrade_status()

            log.info(f"✓ Rack {rack_name} OSD upgrade completed")

            # Verify cluster health after rack upgrade
            if verify_health:
                log.info(f"Verifying cluster health after rack {rack_name} upgrade...")
                if orch.cluster.check_health(
                    rhbuild=config.get("rhbuild"), client=orch.installer
                ):
                    raise RackBasedUpgradeFailure(
                        f"Cluster is in HEALTH_ERR state after rack {rack_name} upgrade"
                    )
                log.info(f"✓ Cluster health verified after rack {rack_name} upgrade")

        # Final cluster health verification
        if verify_health:
            log.info("Performing final cluster health check...")
            if orch.cluster.check_health(
                rhbuild=config.get("rhbuild"), client=orch.installer
            ):
                raise RackBasedUpgradeFailure("Cluster is in HEALTH_ERR state after upgrade")
            log.info("✓ Final cluster health check passed")

        ceph_cluster.rhcs_version = config.get("rhbuild")

        log.info("\n" + "="*80)
        log.info("✓ Rack-based OSD upgrade completed successfully!")
        log.info("="*80)

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
                "ceph osd tree",
                "ceph mgr dump",
                "ceph mon stat",
            ]
        )

    return 0
