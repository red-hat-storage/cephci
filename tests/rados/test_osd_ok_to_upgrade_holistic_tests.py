import json
import time
from collections import defaultdict
from typing import List

from ceph.ceph import CephNode
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.test_deploy_stretch_cluster_baremetal import move_crush_item
from utility.log import Log
from utility.utils import get_ceph_version_from_cluster

log = Log(__name__)

POLL_INTERVAL = 30
WAIT_TIMEOUT = 600
UPGRADE_TIMEOUT = 3600


def run(ceph_cluster, **kw):
    """
    OSD upgrade scenarios: downgrade all OSDs to older image, then run selective
    'ceph orch upgrade start' (by --hosts or --topological-labels), wait, and
    validate that only the expected OSDs are upgraded.

    Test scenarios (config.scenarios):
    - hosts_HOST1: Upgrade with --hosts HOST1; only OSDs on that host upgraded.
    - topological_host: Upgrade with --topological-labels host=HOST1; only that host's OSDs.
    - topological_chassis: Upgrade with --topological-labels chassis=chassis0; only chassis0 OSDs.
    - topological_rack: Upgrade with --topological-labels rack=rack0; only rack0 OSDs.
    - combo_host_chassis_rack: Upgrade with host=HOST1,chassis=chassis0,rack=rack0; only host1 OSDs (most specific).
    - combo_chassis_rack: Upgrade with chassis=chassis0,rack=rack0; all chassis0 OSDs.
    - combo_chassis1_chassis2_rack1: Upgrade with two chassis in one rack; OSDs in both chassis upgraded.
    - combo_chassis3_rack0_no_match: Negative: chassis=chassis3,rack=rack0; chassis3 not in rack0, no OSDs upgraded.
    """
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    scenarios = config.get("scenarios", [])
    upgrade_timeout = config.get("upgrade_timeout", UPGRADE_TIMEOUT)
    downgrade_timeout = config.get("downgrade_timeout", WAIT_TIMEOUT)
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    crush_hierarchy_cleanup = config.get("crush_hierarchy_cleanup", False)
    crush_hierarchy_setup = config.get("crush_hierarchy_setup", False)
    number_of_racks = config.get("number_of_racks", 3)
    number_of_chassis_per_rack = config.get("number_of_chassis_per_rack", 1)

    # OSDs are still in 9.0 stable version
    old_image = _get_daemon_container_images(rados_obj, daemon="osd")[0]

    # as mgr,mon,crash are already upgraded to latest version
    new_image = _get_daemon_container_images(rados_obj, daemon="mgr")[0]

    if not scenarios:
        log.error("config.scenarios cannot be empty")
        return 1

    try:
        osd_hosts: List[CephNode] = ceph_cluster.get_nodes(role="osd")
        # total chassis
        nchassis = number_of_chassis_per_rack * number_of_racks
        osd_hostnames = list()

        if crush_hierarchy_setup:
            log.info("============================================")
            log.info("Crush hierarchy setup")
            log.info("============================================")

            for index, host in enumerate(osd_hosts):
                osd_hostnames.append(host.hostname)
                chassis = f"chassis{index % nchassis}"
                move_crush_item(
                    node=cephadm, crush_obj=host.hostname, name="chassis", value=chassis
                )

            for index in range(0, number_of_chassis_per_rack * number_of_racks):
                chassis = f"chassis{index}"
                rack = f"rack{index % number_of_racks}"
                move_crush_item(
                    node=cephadm, crush_obj=chassis, name="rack", value=rack
                )

            for index in range(0, number_of_racks):
                rack = f"rack{index}"
                move_crush_item(
                    node=cephadm, crush_obj=rack, name="root", value="default"
                )

            for index, host in enumerate(osd_hosts):
                rack = f"rack{index % number_of_racks}"
                chassis = f"chassis{index % nchassis}"
                cmd = (
                    f"ceph orch host set-topological-labels {host.hostname} "
                    f"rack={rack},chassis={chassis},host={host.hostname}"
                )
                rados_obj.client.exec_command(cmd=cmd)
                log.info(
                    "Set topological labels on %s: rack=%s, chassis=%s",
                    host.hostname,
                    rack,
                    chassis,
                )

        host1 = osd_hostnames[0]

        log.info("============================================")
        rados_obj.client.exec_command(
            cmd="ceph orch host ls -f yaml", pretty_print=True
        )
        log.info("-" * 80)
        rados_obj.client.exec_command(cmd="ceph osd tree", pretty_print=True)
        log.info("============================================")

        for name in scenarios:
            log.info("============================================")
            log.info("Scenario: %s", name)
            log.info("============================================")

            # Remove the container_image which would be added to osds.
            # as a part of ceph orch upgrade --topological-labels command
            out = rados_obj.run_ceph_command(cmd="ceph config dump")
            for config in out:
                if config["name"] == "container_image":
                    mon_obj.remove_config(
                        section=config["section"], name="container_image"
                    )

            # 2) Downgrade all OSDs to deployed version
            rados_obj.run_ceph_command(
                cmd=f"ceph config set osd container_image {old_image}",
                client_exec=True,
            )
            rados_obj.client.exec_command(
                cmd="ceph orch redeploy osd.osds",
            )
            time.sleep(20)
            _wait_osds_single_version(
                rados_obj, downgrade_timeout, old_image, service_obj
            )

            # 3) Build upgrade cmd and expected OSDs for this scenario
            hosts_arg = None
            labels_arg = None
            if name == "hosts_HOST1":
                hosts_arg = [host1]
                expected_osds = rados_obj.collect_osd_daemon_ids(host1)
            elif name == "topological_host":
                labels_arg = f"host={host1}"
                expected_osds = rados_obj.collect_osd_daemon_ids(host1)
            elif name == "topological_chassis":
                labels_arg = "chassis=chassis0"
                expected_osds = rados_obj.collect_osd_daemon_ids("chassis0")
            elif name == "topological_rack":
                labels_arg = "rack=rack0"
                expected_osds = rados_obj.collect_osd_daemon_ids("rack0")
            elif name == "combo_host_chassis_rack":
                labels_arg = f"host={host1},chassis=chassis0,rack=rack0"
                expected_osds = rados_obj.collect_osd_daemon_ids(host1)
            elif name == "combo_chassis_rack":
                labels_arg = "chassis=chassis0,rack=rack0"
                expected_osds = rados_obj.collect_osd_daemon_ids("chassis0")
            elif name == "combo_chassis0_chassis1_rack1":
                # Negative scenario , failure expected
                labels_arg = "chassis=chassis0,chassis=chassis1,rack=rack1"
                try:
                    cmd = f"ceph orch upgrade start --image {new_image} --daemon-types osd"
                    if labels_arg:
                        cmd += " --topological-labels " + labels_arg
                    rados_obj.client.exec_command(cmd=cmd)
                except Exception as e:
                    log.info(f"Exception raised : {str(e)}")
                    if "Found duplicate key chassis" in str(e):
                        log.info(f"Expected failure: {str(e)}")
                        continue
                    else:
                        log.error(f"Unexpected exception raised: {str(e)}")
                        return 1

            # 4) Run upgrade
            cmd = f"ceph orch upgrade start --image {new_image} --daemon-types osd"
            if hosts_arg:
                cmd += " --hosts " + ",".join(hosts_arg)
            if labels_arg:
                cmd += " --topological-labels " + labels_arg
            rados_obj.client.exec_command(cmd=cmd)

            # 5) Wait and validate
            _wait_upgrade_done(rados_obj, upgrade_timeout)
            validate_upgraded_osds(rados_obj, expected_osds, new_image)
            ceph_version = get_ceph_version_from_cluster(
                ceph_cluster.get_nodes(role="client")[0]
            )
            rados_obj.client.exec_command(
                cmd=f"ceph orch ps | grep {ceph_version}", pretty_print=True
            )
            log.info("Scenario %s passed", name)

    except Exception as e:
        log.error("Execution failed: %s", e)
        log.exception(e)
        # rados_obj.log_cluster_health()
        return 1
    finally:
        log.info("Finally block")
        out = rados_obj.run_ceph_command(cmd="ceph config dump")
        for config in out:
            if config["name"] == "container_image":
                mon_obj.remove_config(section=config["section"], name="container_image")

        if crush_hierarchy_cleanup:
            log.info("============================================")
            log.info("Crush hierarchy cleanup")
            log.info("============================================")
            for osd_hostname in osd_hostnames:
                move_crush_item(
                    node=cephadm, crush_obj=osd_hostname, name="root", value="default"
                )

            for index in range(0, number_of_chassis_per_rack * number_of_racks):
                chassis = f"chassis{index}"
                rados_obj.run_ceph_command(cmd=f"ceph osd crush rm {chassis}")

            for index in range(0, number_of_racks):
                rack = f"rack{index}"
                rados_obj.run_ceph_command(cmd=f"ceph osd crush rm {rack}")
    return 0


def validate_upgraded_osds(
    rados_obj: RadosOrchestrator, expected_osds: List[int], ceph_version: str
):
    """Assert that exactly expected_osds have the given ceph version in orch ps."""
    version_map = _get_version_to_osds(rados_obj)
    actual_osds = version_map.get(ceph_version, [])
    expected_osds = sorted(expected_osds)
    actual_osds.sort()
    if actual_osds != expected_osds:
        log.error(
            f"Version {ceph_version}: expected OSDs {expected_osds}, got {actual_osds}"
        )
        raise ValueError(
            f"Version {ceph_version}: expected OSDs {expected_osds}, got {actual_osds}"
        )
    log.info("============================================")
    log.info(
        f"\nVersion {ceph_version}\n expected OSDs {expected_osds}\n actual {actual_osds}"
    )
    log.info("============================================")


def _get_version_to_osds(rados_obj: RadosOrchestrator) -> dict:
    """Return dict: version string -> list of OSD ids (int)."""
    out = rados_obj.run_ceph_command(
        cmd="ceph orch ps --daemon-type osd", client_exec=True
    )
    m = defaultdict(list)
    for d in out:
        image_name = d.get("container_image_name")
        if not image_name:
            continue
        oid = d.get("daemon_id")
        if oid is not None:
            m[image_name].append(int(oid))
    return dict(m)


def _wait_upgrade_done(rados_obj: RadosOrchestrator, timeout: int):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            rados_obj.run_ceph_command(cmd="ceph orch upgrade status", client_exec=True)
            log.debug(
                f"Upgrade still in progress. Sleeping for {POLL_INTERVAL}s before retrying"
            )
            time.sleep(POLL_INTERVAL)
        except json.decoder.JSONDecodeError:
            log.debug("Upgrade completed")
            return
    raise TimeoutError("Upgrade did not complete in %ss" % timeout)


def _get_daemon_container_images(
    rados_obj: RadosOrchestrator, daemon: str
) -> List[str]:
    """Return container_image_name for each OSD from ceph orch ps --daemon-type osd."""
    out = rados_obj.run_ceph_command(
        cmd=f"ceph orch ps --daemon-type {daemon} --refresh", client_exec=True
    )
    if isinstance(out, dict) and "daemons" in out:
        out = out["daemons"]
    if not isinstance(out, list):
        return []
    images = []
    for d in out:
        name = d.get("container_image_name")
        if name:
            images.append(name)
    return images


def _wait_osds_single_version(
    rados_obj: RadosOrchestrator,
    timeout: int,
    expected_image: str,
    service_obj: ServiceabilityMethods,
):
    """Wait until all OSDs in orch ps have container_image_name matching expected_image."""
    deadline = time.time() + timeout
    total_osds = service_obj.get_osd_count()
    while time.time() < deadline:
        images = _get_daemon_container_images(rados_obj, daemon="osd")
        if total_osds != len(images):
            log.debug(f"Not all OSDs are up and running. Sleeping for {POLL_INTERVAL}s")
            log.debug(f"total_osds : {total_osds}")
            log.debug(f"total_images: {len(images)}")
            time.sleep(POLL_INTERVAL)
            continue
        if len(set(images)) != 1:
            log.debug(
                f"There are more than 1 image in OSDs. Sleeping for {POLL_INTERVAL}s"
            )
            log.info(f"Images of osd:{images}")
            time.sleep(POLL_INTERVAL)
            continue
        log.debug(f"Actual image: {list(set(images))[0]}")
        log.debug(f"Expected image: {expected_image}")
        if list(set(images))[0].strip() == expected_image.strip():
            log.info("All %d OSDs at image %s", len(images), expected_image)
            return
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(
        "OSDs did not reach image %s within %ss (got %s)"
        % (
            expected_image,
            timeout,
            _get_daemon_container_images(rados_obj, daemon="osd"),
        )
    )
