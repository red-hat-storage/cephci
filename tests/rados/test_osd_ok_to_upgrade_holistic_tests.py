import json
import re
import time
from collections import defaultdict
from typing import List, Optional, Tuple

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from ceph.utils import get_node_by_id
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

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
    - hosts_HOST1_services_osd_osds: Upgrade with --hosts HOST1 --services osd.osds
      (override service name with config osd_service_name, default osd.osds); only that
      host's OSDs for the given OSD service should upgrade.
    - hosts_rack1_rack2: Upgrade with --hosts H1,H2 where H1 is the first host under
      CRUSH rack1 and H2 is the first host under rack2
    - hosts_HOST1_limit: Upgrade with --hosts HOST1 --limit N
    - topological_host: Upgrade with --topological-labels host=HOST1; only that host's OSDs.
    - topological_chassis: Upgrade with --topological-labels chassis=chassis0; only chassis0 OSDs.
    - topological_rack: Upgrade with --topological-labels rack=rack0; only rack0 OSDs.
    - combo_host_chassis_rack: Upgrade with host=HOST1,chassis=chassis0,rack=rack0; only host1 OSDs (most specific).
    - combo_chassis_rack: Upgrade with chassis=chassis0,rack=rack0; all chassis0 OSDs.
    - combo_chassis1_chassis2_rack1: Upgrade with two chassis in one rack; OSDs in both chassis upgraded.
    - combo_chassis3_rack1_no_match: Negative: chassis=chassis3,rack=rack0; chassis3 not in rack0, no OSDs upgraded.
    - hosts_invalid_host: Negative: --hosts with a non-existent host (default invalid_host;
      override with config invalid_upgrade_host). Expects upgrade start to fail.
    - topological_labels_rack_nonexistent: Negative: --topological-labels rack=does_not_exist
      (override rack value with config nonexistent_rack_label). Expects upgrade start to fail.

    Optional mid-upgrade checks (config):
    - test_stop_start: When progress reaches at least half of daemons (from orch upgrade
      status JSON "progress"), run ceph orch upgrade stop then re-run the same upgrade start
      command built for the scenario.
    - test_pause_resume: At the same half threshold, run ceph orch upgrade pause then
      ceph orch upgrade resume.
    """
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    scenarios = config.get("scenarios", [])
    upgrade_timeout = config.get("upgrade_timeout", 3600)
    downgrade_timeout = config.get("downgrade_timeout", 600)
    test_stop_start = config.get("test_stop_start", False)
    test_pause_resume = config.get("test_pause_resume", False)
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    mgr_config_method = MgrWorkflows(node=cephadm)
    mon_config_method = MonConfigMethods(rados_obj=rados_obj)
    host_without_osd = config.get("host_without_osd", "node15")
    re_pool_name = "re_pool"
    # ceph version check
    rhbuild = config.get("rhbuild")
    version = rhbuild.split("-")[0]
    if float(version) < 9.1:
        log.error(
            "This test module is a 9.1 feature. Skipping test in ceph versions less than 9.1."
        )
        return 0

    # OSDs are still in 8.1 stable version
    old_image = get_daemon_container_images(rados_obj, daemon="osd")[0]

    # as mgr,mon,crash are already upgraded to latest version 9.1
    new_image = get_daemon_container_images(rados_obj, daemon="mgr")[0]

    if not scenarios:
        log.error("config.scenarios cannot be empty")
        return 1

    try:
        osd_hostnames: List[str] = rados_obj.get_osd_hosts()
        active_mgr = mgr_config_method.get_active_mgr()
        active_mgr_node = ceph_cluster.get_node_by_hostname(active_mgr.split(".")[0])
        fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
        assert mon_config_method.set_config(
            section="mgr", name="debug_mgr", value="20/20"
        )

        host1 = osd_hostnames[0]

        log.info("============================================")
        rados_obj.client.exec_command(
            cmd="ceph orch host ls -f yaml", pretty_print=True
        )
        log.info("-" * 80)
        rados_obj.client.exec_command(cmd="ceph osd tree", pretty_print=True)
        log.info("============================================")

        # Create replicated pool
        assert rados_obj.create_pool(
            pool_name=re_pool_name, app_name="rados"
        ), f"Failed to create replicated pool {re_pool_name}"

        for scenario in scenarios:
            log.info("==============================================================")
            log.info("Scenario: %s", scenario)
            log.info("==============================================================")

            active_mgr_host_obj = rados_obj.get_host_object(active_mgr.split(".")[0])
            rados_obj.rotate_logs([active_mgr_host_obj])

            # Remove the container_image which would be added to osds.
            # as a part of ceph orch upgrade --topological-labels command
            log.info("=================Removing container_image===================")
            out = rados_obj.run_ceph_command(cmd="ceph config dump")
            for cfg_entry in out:
                if cfg_entry["name"] == "container_image":
                    if cfg_entry["section"] == "osd":
                        continue
                    mon_obj.remove_config(
                        section=cfg_entry["section"], name="container_image"
                    )

            # 2) Downgrade all OSDs to deployed version
            log.info("===========Downgrading OSDs to deployed version===============")
            mon_obj.set_config(section="osd", name="container_image", value=old_image)
            rados_obj.client.exec_command(
                cmd="ceph orch redeploy osd.osds",
            )
            time.sleep(20)

            # Wait for all OSDs to be in a single ceph image and the old ceph image
            _wait_osds_single_version(
                rados_obj, downgrade_timeout, old_image, service_obj
            )

            log.info(
                "=================OSD version before scenario start==================="
            )
            rados_obj.client.exec_command(
                cmd="ceph orch ps | grep osd", pretty_print=True
            )

            # 3) Build upgrade cmd and expected OSDs for this scenario
            log.info(
                "========Building upgrade cmd and expected OSDs for this scenario========"
            )
            hosts_arg = None  # --hosts
            labels_arg = None  # --topological-labels
            limit_arg = None  # --limit
            validate_with_limit = False
            host_osds_for_limit = None  # --hosts --limit
            services_arg = None  # --services
            daemon_types_arg = None  # --daemon-types
            bucket_type = None  # --bucket-type
            bucket_name = None  # --bucket-name
            expected_osds = []

            if scenario == "host_without_osd":
                host_wihtout_osd_obj = get_node_by_id(ceph_cluster, host_without_osd)
                hosts_arg = [host_wihtout_osd_obj.hostname]
                expected_osds = []
                daemon_types_arg = "osd"
            elif scenario == "hosts_HOST1":
                hosts_arg = [host1]
                expected_osds = rados_obj.collect_osd_daemon_ids(host1)
                daemon_types_arg = "osd"
            elif scenario == "hosts_HOST1_services_osd_osds":
                hosts_arg = [host1]
                services_arg = config.get("osd_service_name", "osd.osds")
                expected_osds = rados_obj.collect_osd_daemon_ids(host1)
            elif scenario == "hosts_rack1_rack2":
                rack1_osds = rados_obj.collect_osd_daemon_ids("rack1")
                out = rados_obj.run_ceph_command(
                    cmd=f"ceph osd find {rack1_osds[0]}",
                    client_exec=True,
                )
                host_from_rack1 = out["host"]

                rack2_osds = rados_obj.collect_osd_daemon_ids("rack2")
                out = rados_obj.run_ceph_command(
                    cmd=f"ceph osd find {rack2_osds[0]}",
                    client_exec=True,
                )
                host_from_rack2 = out["host"]

                hosts_arg = [host_from_rack1, host_from_rack2]
                expected_osds_host1 = rados_obj.collect_osd_daemon_ids(host_from_rack1)
                expected_osds_host2 = rados_obj.collect_osd_daemon_ids(host_from_rack2)
                expected_osds = sorted(
                    set(expected_osds_host1) | set(expected_osds_host2)
                )
                daemon_types_arg = "osd"
            elif scenario == "hosts_HOST1_limit":
                hosts_arg = [host1]
                limit_arg = config.get("limit", 3)
                host_osds_for_limit = rados_obj.collect_osd_daemon_ids(host1)
                expected_osds = None
                validate_with_limit = True
                daemon_types_arg = "osd"
            elif scenario == "topological_host":
                labels_arg = f"host={host1}"
                expected_osds = rados_obj.collect_osd_daemon_ids(host1)
                daemon_types_arg = "osd"
            elif scenario == "topological_chassis":
                labels_arg = "chassis=chassis1"
                expected_osds = rados_obj.collect_osd_daemon_ids("chassis1")
                daemon_types_arg = "osd"
            elif scenario == "topological_rack":
                labels_arg = "rack=rack1"
                expected_osds = rados_obj.collect_osd_daemon_ids("rack1")
                daemon_types_arg = "osd"
            elif scenario == "combo_host_chassis_rack":
                labels_arg = f"host={host1},chassis=chassis1,rack=rack1"
                expected_osds = rados_obj.collect_osd_daemon_ids(host1)
                daemon_types_arg = "osd"
            elif scenario == "combo_chassis_rack":
                labels_arg = "chassis=chassis1,rack=rack1"
                expected_osds = rados_obj.collect_osd_daemon_ids("chassis1")
                daemon_types_arg = "osd"
            elif scenario == "combo_chassis1_chassis2_rack1":
                # Negative scenario , failure expected
                labels_arg = "chassis=chassis1,chassis=chassis2,rack=rack1"
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

                log.error("Unexpected exception raised")
                return 1
            elif scenario == "combo_chassis3_rack1_no_match":
                # Negative scenario , failure expected
                labels_arg = "chassis=chassis3,rack=rack1"
                cmd = f"ceph orch upgrade start --image {new_image} --daemon-types osd"
                cmd += " --topological-labels " + labels_arg

                # exit status of command is not 1. echo $? will return 0.
                out = list(rados_obj.client.exec_command(cmd=cmd))[1]
                log.info(f"Output: {out}")

                if "did not match any hosts" in out:
                    log.info(f"Expected failure: {out}")
                    continue
                log.error("Unexpected exception raised")
                log.error(f"Output: {out}")
                return 1
            elif scenario == "hosts_invalid_host":
                bad_host = "invalidhost"
                cmd = (
                    f"ceph orch upgrade start --image {new_image} "
                    f"--daemon-types osd --hosts {bad_host}"
                )
                rados_obj.client.exec_command(cmd=cmd)
                # exit status of command is not 1. echo $? will return 0.
                out = list(rados_obj.client.exec_command(cmd=cmd))[1]
                log.info(f"Output: {out}")

                if "did not match any hosts" in out:
                    log.info(f"Expected failure: {out}")
                    continue
                log.error("Unexpected exception raised")
                log.error(f"Output: {out}")
                return 1
            elif scenario == "topological_labels_rack_nonexistent":
                rack_val = "doesnotexist"
                labels_neg = f"rack={rack_val}"
                cmd = (
                    f"ceph orch upgrade start --image {new_image} "
                    f"--daemon-types osd --topological-labels {labels_neg}"
                )
                rados_obj.client.exec_command(cmd=cmd)
                # exit status of command is not 1. echo $? will return 0.
                out = list(rados_obj.client.exec_command(cmd=cmd))[1]
                log.info(f"Output: {out}")

                if "did not match any hosts" in out:
                    log.info(f"Expected failure: {out}")
                    continue
                log.error("Unexpected exception raised")
                log.error(f"Output: {out}")
                return 1
            elif scenario == "full_cluster_upgrade":
                expected_osds = rados_obj.collect_osd_daemon_ids("default")
                daemon_types_arg = "osd"
            elif scenario == "bucket_type_bucket_name_rack":
                bucket_type = "rack"
                bucket_name = "rack1"
                expected_osds = rados_obj.collect_osd_daemon_ids("rack1")
                daemon_types_arg = "osd"
            elif scenario == "bucket_type_bucket_name_chassis":
                bucket_type = "chassis"
                bucket_name = "chassis1"
                expected_osds = rados_obj.collect_osd_daemon_ids("chassis1")
                daemon_types_arg = "osd"
            elif scenario == "bucket_type_bucket_name_rack_host_arg":
                bucket_type = "rack"
                bucket_name = "rack1"
                daemon_types_arg = "osd"
                hosts_arg = [host1]
                expected_osds = rados_obj.collect_osd_daemon_ids(host1)
            elif scenario == "bucket_type_bucket_name_chassis_host_arg":
                bucket_type = "chassis"
                bucket_name = "chassis1"
                daemon_types_arg = "osd"
                hosts_arg = [host1]
                expected_osds = rados_obj.collect_osd_daemon_ids(host1)
            else:
                log.error(f"Invalid scenario: {scenario}")
                return 1

            # 4) Run upgrade
            log.info(
                "=================================Running upgrade==========================="
            )
            cmd = f"ceph orch upgrade start --image {new_image}"
            if daemon_types_arg:
                cmd += f" --daemon-types {daemon_types_arg}"
            if hosts_arg:
                cmd += " --hosts " + ",".join(hosts_arg)
            if labels_arg:
                cmd += " --topological-labels " + labels_arg
            if limit_arg:
                cmd += f" --limit {limit_arg}"
            if services_arg:
                cmd += f" --services {services_arg}"
            if bucket_type and bucket_name:
                cmd += f" --bucket-type {bucket_type} --bucket-name {bucket_name}"
            rados_obj.client.exec_command(cmd=cmd)

            # 5) Wait and validate
            log.info(
                "====Waiting for upgrade to complete and validate right OSDs are upgraded===="
            )
            _wait_upgrade_done(
                rados_obj,
                upgrade_timeout,
                upgrade_start_cmd=cmd,
                test_stop_start=test_stop_start,
                test_pause_resume=test_pause_resume,
            )
            log.info("=================OSD version after upgrade===================")
            rados_obj.client.exec_command(
                cmd="ceph orch ps | grep osd", pretty_print=True
            )

            if validate_with_limit:
                validate_upgraded_osds_with_limit(
                    rados_obj, host_osds_for_limit, limit_arg, new_image
                )
            else:
                validate_upgraded_osds(rados_obj, expected_osds, new_image)

            # 6) Validate log lines
            if bucket_type is None and bucket_name is None:
                log.info(
                    "=================ok-to-stop log lines after upgrade==================="
                )
                lines = active_mgr_node.exec_command(
                    sudo=True,
                    cmd=f"grep ok-to-stop /var/log/ceph/{fsid}/ceph-mgr.{active_mgr}.log",
                )
                ok_to_stop_lines = lines[0].split("\n")[:-1]

                log.info("---start of ok-to-stop log lines---")
                for line in ok_to_stop_lines:
                    log.info(line)
                log.info("---end of ok-to-stop log lines---")

                assert (
                    len(ok_to_stop_lines) > 0
                ), "ERROR: ok-to-stop command is not used"

                log.info(
                    "ok-to-stop command will be used for all workflows which does not include"
                    " –-bucket_type=<chassis/rack> --bucket_name=<rackOne>"
                )
            else:

                # 7) Validate ok-to-upgrade log lines
                log.info(
                    "=================ok-to-upgrade log lines after upgrade==================="
                )
                lines = active_mgr_node.exec_command(
                    sudo=True,
                    cmd=f"grep ok-to-upgrade /var/log/ceph/{fsid}/ceph-mgr.{active_mgr}.log",
                )
                ok_to_upgrade_lines = lines[0].split("\n")[:-1]

                log.info("---start of ok-to-upgrade log lines---")
                for line in ok_to_upgrade_lines:
                    log.info(line)
                log.info("---end of ok-to-upgrade log lines---")

                log.info(
                    "ok-to-upgrade is used only for scenarios that include "
                    "--bucket_type=<chassis/rack> --bucket_name=<rackOne>"
                )

                assert (
                    len(ok_to_upgrade_lines) > 0
                ), "ERROR: ok-to-upgrade command is not used"

            log.info("==============================================================")
            log.info("Scenario %s passed", scenario)
            log.info("==============================================================")

    except Exception as e:
        log.error("Execution failed: %s", e)
        log.exception(e)
        # rados_obj.log_cluster_health()
        return 1
    finally:
        log.info("Finally block")
        # Delete pools created for IO during upgrade
        rados_obj.delete_pool(pool=re_pool_name)
        log.info("Deleted pool: %s", re_pool_name)

        # Resetting the OSD state as to before the test.
        log.info(
            "================= FINALLY BLOCK: Removing container_image==================="
        )
        out = rados_obj.run_ceph_command(cmd="ceph config dump")
        for cfg_entry in out:
            if cfg_entry["name"] == "container_image":
                if cfg_entry["section"] == "osd":
                    continue
                mon_obj.remove_config(
                    section=cfg_entry["section"], name="container_image"
                )
        log.info(
            "===========FINALLY BLOCK: Downgrading OSDs to deployed version==============="
        )
        mon_obj.set_config(section="osd", name="container_image", value=old_image)
        rados_obj.client.exec_command(
            cmd="ceph orch redeploy osd.osds",
        )
        time.sleep(20)
        _wait_osds_single_version(rados_obj, downgrade_timeout, old_image, service_obj)

    return 0


def validate_upgraded_osds(
    rados_obj: RadosOrchestrator,
    expected_osds_to_be_upgraded: List[int],
    ceph_version: str,
):
    """Assert that exactly expected_osds have the given ceph version in orch ps."""
    version_map = _get_version_to_osds(rados_obj)
    log.info(f"version_map: {version_map}")
    """
    version_map = {
        "19.2.1" : [1, 2, 3],
        "19.2.2" : [4, 5, 6],
    }
    """
    actual_osds_upgraded = version_map.get(ceph_version, [])
    expected_osds_to_be_upgraded.sort()
    actual_osds_upgraded.sort()
    if actual_osds_upgraded != expected_osds_to_be_upgraded:
        raise ValueError(
            f"Version {ceph_version}: expected OSDs {expected_osds_to_be_upgraded}, got {actual_osds_upgraded}"
        )
    log.info("============================================")
    log.info(
        f"\nVersion {ceph_version}\n expected OSDs {expected_osds_to_be_upgraded}\n actual {actual_osds_upgraded}"
    )
    log.info("============================================")


def validate_upgraded_osds_with_limit(
    rados_obj: RadosOrchestrator,
    host_osds: List[int],
    limit: int,
    ceph_image: str,
):
    """Assert that exactly min(limit, len(host_osds)) OSDs are upgraded and all are on the host."""
    version_map = _get_version_to_osds(rados_obj)
    osds_upgraded_to_new_version = version_map.get(ceph_image, [])
    expected_count = min(limit, len(host_osds))
    host_osds_set = set(host_osds)

    if len(osds_upgraded_to_new_version) != expected_count:
        log.error(
            f"Image {ceph_image}: expected {expected_count} OSDs upgraded: {osds_upgraded_to_new_version}"
        )
        raise ValueError(
            f"Image {ceph_image}: expected {expected_count} OSDs upgraded: {osds_upgraded_to_new_version}"
        )

    osds_upgraded_but_not_on_host = [
        o for o in osds_upgraded_to_new_version if o not in host_osds_set
    ]
    if osds_upgraded_but_not_on_host:
        log.error(
            f"Image {ceph_image}: OSDs {osds_upgraded_but_not_on_host} upgraded"
            f" but not on host (host OSDs: {host_osds})"
        )
        raise ValueError(
            f"Image {ceph_image}: OSDs {osds_upgraded_but_not_on_host} not on specified host"
        )
    log.info("============================================")
    log.info(
        f"\nVersion {ceph_image}\n expected count {expected_count} (limit={limit})\n"
        f" actual OSDs {sorted(osds_upgraded_to_new_version)}"
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


def _parse_upgrade_daemon_progress(
    progress: Optional[str],
) -> Optional[Tuple[int, int]]:
    """Parse 'N/M daemons upgraded' from orch upgrade status progress field."""
    if not progress:
        return None
    m = re.search(r"(\d+)/(\d+)\s+daemons upgraded", progress)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _wait_upgrade_done(
    rados_obj: RadosOrchestrator,
    timeout: int,
    upgrade_start_cmd: Optional[str] = None,
    test_stop_start: bool = False,
    test_pause_resume: bool = False,
):
    """
    Poll until ceph orch upgrade finishes. Optionally, once at least half of the
    daemons in status progress are upgraded, run:
    - test_stop_start: ceph orch upgrade stop, then re-run upgrade_start_cmd
    - test_pause_resume: ceph orch upgrade pause, then ceph orch upgrade resume

    upgrade_start_cmd must be set when test_stop_start is True (same string as
    ceph orch upgrade start ... used to begin this upgrade).
    """
    if test_stop_start and not (upgrade_start_cmd and upgrade_start_cmd.strip()):
        raise ValueError("test_stop_start requires a non-empty upgrade_start_cmd")

    deadline = time.time() + timeout
    mid_stop_start_done = False
    mid_pause_resume_done = False

    while time.time() < deadline:
        try:
            status = rados_obj.run_ceph_command(
                cmd="ceph orch upgrade status", client_exec=True
            )
        except json.decoder.JSONDecodeError:
            log.debug("Upgrade completed (upgrade status not JSON)")
            return

        if not status.get("in_progress", False):
            log.debug("Upgrade completed (in_progress is false)")
            return

        parsed = _parse_upgrade_daemon_progress(status.get("progress"))
        if parsed is not None:
            done, total = parsed
            half_threshold = (total + 1) // 2
            if total > 0 and done >= half_threshold:
                if test_stop_start and not mid_stop_start_done:
                    log.info(
                        "Mid-upgrade (%d/%d >= half): ceph orch upgrade stop then "
                        "re-start upgrade",
                        done,
                        total,
                    )
                    rados_obj.client.exec_command(cmd="ceph orch upgrade stop")
                    rados_obj.client.exec_command(cmd=upgrade_start_cmd)
                    mid_stop_start_done = True
                if test_pause_resume and not mid_pause_resume_done:
                    log.info(
                        "Mid-upgrade (%d/%d >= half): ceph orch upgrade pause then resume",
                        done,
                        total,
                    )
                    rados_obj.client.exec_command(cmd="ceph orch upgrade pause")
                    rados_obj.client.exec_command(cmd="ceph orch upgrade resume")
                    mid_pause_resume_done = True

        log.debug(
            "Upgrade still in progress. Sleeping for %ss before retrying",
            POLL_INTERVAL,
        )
        time.sleep(POLL_INTERVAL)

    raise TimeoutError("Upgrade did not complete in %ss" % timeout)


def get_daemon_container_images(rados_obj: RadosOrchestrator, daemon: str) -> List[str]:
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
        images = get_daemon_container_images(rados_obj, daemon="osd")
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
            get_daemon_container_images(rados_obj, daemon="osd"),
        )
    )
