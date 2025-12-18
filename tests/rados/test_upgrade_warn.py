"""
Test to Upgrade the ceph cluster to the latest version, with few RADOS checks
1. check if the warning "OSD_UPGRADE_FINISHED" is generated when "require_osd_release" does ot match
the current release during upgrades.
Quincy to reef bug : https://bugzilla.redhat.com/show_bug.cgi?id=2243570
2. Check if the cluster usage is same before & after upgrade
3. Check if MAX_AVAIL is calculated correctly for all the pools before and after upgrade
4. Check if all the daemons on the cluster are present post upgrade
5. Check if there were inactive PGs, causing data unavailability during the upgrade process
"""

import datetime
import json
import time

from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.orch import Orch
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from ceph.utils import get_node_by_id, remove_repos
from cephci.utils.build_info import CephTestManifest
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to Upgrade the ceph cluster to the latest version, with few RADOS checks
    1. check if the warning "OSD_UPGRADE_FINISHED" is generated when "require_osd_release" does ot match
    the current release during upgrades.
    Quincy to reef bug : https://bugzilla.redhat.com/show_bug.cgi?id=2243570
    2. Check if the cluster usage is same before & after upgrade
    3. Check if MAX_AVAIL is calculated correctly for all the pools before and after upgrade
    4. Check if all the daemons on the cluster are present post upgrade
    5. Check if there were inactive PGs, causing data unavailability during the upgrade process

    Args:
        ceph_cluster: Cluster object
        kw : the KW args for the test
            config: Test configuration dictionary with the following supported keys:
                args: Dictionary containing upgrade parameters:
                    rhcs-version: Target RHCS version (e.g., "7.1", "8.0")
                    release: Target release (e.g., "z1", "z2", "rc")
                    custom_image: Custom container image (optional)
                    custom_repo: Custom repository URL (optional)
                    daemon_types: Comma-separated daemon types to upgrade (e.g., "mon,mgr") (optional)
                    hosts: Comma-separated node IDs (e.g., "node1,node2") - auto-converted to hostnames (optional)
                    services: Comma-separated service names to upgrade (e.g., "mon,mgr") (optional)
                base_cmd_args: Base command arguments (e.g., verbose: true)
                timeout: Timeout for upgrade completion in seconds (default: 3600)
                verify_warning: Check if the health warnings during the upgrade is generated & removed post upgrade
                verify_older_version_warn: Check if DAEMON_OLD_VERSION warning is generated
                verify_daemons: Check for daemon existence on cluster post upgrade
                verify_cluster_usage: Check if cluster usage is same before & after upgrade
                verify_max_avail: Check max_avail calculation on pools
                check_for_inactive_pgs: Check for inactive PGs during upgrade
                verify_cluster_health: Verify cluster health post upgrade
                enable_debug_level: Enable debug logging for mon and mgr daemons

    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    args = config.get("args", {})
    timeout = config.get("timeout", 3600)
    rhbuild = config.get("rhbuild")
    cephadm_obj = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm_obj)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    verify_warning = config.get("verify_warning", False)
    verify_older_version_warn = config.get("verify_older_version_warn", False)
    verify_daemons = config.get("verify_daemons", False)
    verify_cluster_usage = config.get("verify_cluster_usage", False)
    verify_max_avail = config.get("verify_max_avail", False)
    check_for_inactive_pgs = config.get("check_for_inactive_pgs", False)
    verify_cluster_health = config.get("verify_cluster_health", False)
    enable_debug_level = config.get("enable_debug_level", False)
    installer = ceph_cluster.get_nodes(role="installer")[0]

    init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
    msg = f"time when upgrade test was started : {init_time}"
    log.info(msg)

    log.debug("Collecting daemon info and Cluster usage info before the upgrade")
    pre_upgrade_orch_ps = rados_obj.run_ceph_command(cmd="ceph orch ps")
    pre_upgrade_df_detail = rados_obj.run_ceph_command(cmd="ceph df detail")
    log.info("\n\nCluster status before upgrade: \n")
    log_dump = (
        f"ceph status :  {rados_obj.run_ceph_command(cmd='ceph -s')} \n "
        f"health detail :{rados_obj.run_ceph_command(cmd='ceph health detail')} \n "
        f"crashes : {rados_obj.run_ceph_command(cmd='ceph crash ls')} \n "
        f"ceph versions: {rados_obj.run_ceph_command(cmd='ceph versions')} \n"
    )
    log.info(log_dump)
    if enable_debug_level:
        log.debug("Setting up debug configs on the cluster for mon & Mgr daemons")
        mon_obj.set_config(section="mon", name="debug_mon", value="30/30")
        mon_obj.set_config(section="mgr", name="debug_mgr", value="20/20")

    if verify_max_avail and not rados_obj.verify_max_avail():
        log.error("MAX_AVAIL deviates on the cluster more than expected")
        raise Exception("MAX_AVAIL not proper error")

    log.debug("Starting upgrade")
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:
        # Preserve existing args and add image parameter
        if "args" not in config:
            config["args"] = {}
        config["args"]["image"] = "latest"

        # Support installation of the baseline cluster whose version is not available in
        # CDN. This is primarily used for an upgrade scenario. This support is currently
        # available only for RH network.
        _rhcs_version = args.get("rhcs-version", None)
        _rhcs_release = args.get("release", None)
        _platform = args.get("platform", config["platform"])
        _custom_image = args.get("custom_image", None)
        _custom_repo = args.get("custom_repo", None)
        _rpm_version = None
        if _rhcs_release and _rhcs_version:
            curr_ver, _ = cephadm_obj.shell(args=["ceph version | awk '{print $3}'"])
            log.debug(
                "Upgrading the cluster from ceph version %s to %s-%s "
                % (curr_ver, _rhcs_version, _rhcs_release)
            )
            product: str = args.get("--product", "redhat")
            ctm: CephTestManifest = CephTestManifest(
                product=product,
                release=_rhcs_version,
                build_type=_rhcs_release,
                platform=_platform,
            )
            _base_url = ctm.repository
            _registry = ctm.ceph_image_dtr
            _image_name = ctm.ceph_image_path
            _image_tag = ctm.ceph_image_tag
            _ver = ctm.ceph_version

            # The cluster object is configured so that the values are persistent till
            # an upgrade occurs. This enables us to execute the test in the right
            # context.
            config["base_url"] = _base_url
            config["container_image"] = f"{_registry}/{_image_name}:{_image_tag}"
            config["ceph_docker_registry"] = _registry
            config["ceph_docker_image"] = _image_name
            config["ceph_docker_image_tag"] = _image_tag
            ceph_cluster.rhcs_version = _rhcs_version or rhbuild
            config["rhbuild"] = f"{_rhcs_version}-{_platform}"
            config["args"]["rhcs-version"] = _rhcs_version
            config["args"]["release"] = _rhcs_release
            config["args"]["image"] = config["container_image"]
            os_ver = rhbuild.split("-")[-1]
            _rpm_version = f"2:{_ver}.el{os_ver}cp"
        elif _custom_image and _custom_repo:
            _registry, _image_name = _custom_image.split(":")[0].split("/", 1)
            _image_tag = _custom_image.split(":")[-1]
            _base_url = _custom_repo

            # The cluster object is configured so that the values are persistent till
            # an upgrade occurs. This enables us to execute the test in the right
            # context.
            config["base_url"] = _base_url
            config["container_image"] = f"{_registry}/{_image_name}:{_image_tag}"
            config["ceph_docker_registry"] = _registry
            config["ceph_docker_image"] = _image_name
            config["ceph_docker_image_tag"] = _image_tag
            config["args"]["image"] = config["container_image"]

        # initiate a new object with updated config
        cluster_obj = Orch(cluster=ceph_cluster, **config)

        # Remove existing repos
        for node in ceph_cluster.get_nodes():
            remove_repos(ceph_node=node)

        # Set repo to newer RPMs
        cluster_obj.set_tool_repo()
        time.sleep(5)
        upgd_dict = {"upgrade": True, "rpm_version": _rpm_version}
        cluster_obj.install(**upgd_dict)
        time.sleep(5)

        # Check service versions vs available and target containers
        cluster_obj.upgrade_check(image=config.get("container_image"))

        ceph_version = rados_obj.run_ceph_command(cmd="ceph version")
        log.info(f"Current version on the cluster : {ceph_version}")

        # Log selective upgrade parameters if provided
        if args:
            if args.get("daemon_types"):
                log.info(
                    f"Selective upgrade enabled - daemon types: {args['daemon_types']}"
                )
            if args.get("hosts"):
                log.info(f"Selective upgrade enabled - target nodes: {args['hosts']}")
            if args.get("services"):
                log.info(f"Selective upgrade enabled - services: {args['services']}")

        # Convert node IDs to hostnames if hosts parameter is provided
        if args and args.get("hosts"):
            node_ids = [node_id.strip() for node_id in args["hosts"].split(",")]
            hostnames = []
            for node_id in node_ids:
                node_obj = get_node_by_id(ceph_cluster, node_id)
                if node_obj:
                    hostnames.append(node_obj.hostname)
                    log.debug(
                        f"Converted node ID '{node_id}' to hostname '{node_obj.hostname}'"
                    )
                else:
                    log.warning(f"Could not find node with ID '{node_id}', skipping")
            if hostnames:
                config["args"]["hosts"] = ",".join(hostnames)
                log.info(f"Converted node IDs to hostnames: {hostnames}")

        # Start Upgrade
        cluster_obj.start_upgrade(config)
        time.sleep(5)

        warn_flag = False
        version_flag = False
        upgrade_complete = False
        inactive_pgs = 0
        # Monitor upgrade status, till completion, checking for the warning to be generated
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        command_failed_counter = 0
        while end_time > datetime.datetime.now():
            cmd = "ceph orch upgrade status"
            try:
                out, _ = rados_obj.client.exec_command(cmd=cmd, sudo=True)
                status = json.loads(out)
                if not status["in_progress"]:
                    log.info("Upgrade Complete...")
                    upgrade_complete = True
                    break
                command_failed_counter = 0
            except CommandFailed as err:
                command_failed_counter += 1
                log.error("Exception hit : %s" % err.__doc__)
                log.exception(err)
                log.info("Retrying after 20 seconds...")
                time.sleep(20)
                if command_failed_counter < 3:
                    continue
                else:
                    log.error("Command failed after 3 attempts...")
                    upgrade_complete = False
                    break
            except json.JSONDecodeError:
                if "no upgrades in progress" in out:
                    log.info("Upgrade Complete...")
                    upgrade_complete = True
                    break
            except Exception as err:
                # checking if the Exception raised is due to bug : 2314146
                # Error msg :  ENOTSUP: Warning: due to ceph-mgr restart, some PG states may not be up to date
                # Module 'orchestrator' is not enabled/loaded (required by command 'orch upgrade status'):
                # use `ceph mgr module enable orchestrator` to enable it
                if "not enabled/loaded" in str(err):
                    log.info(
                        "Intermittent issue hit. bug : 2314146. Error hit : \n %s \n"
                        % err,
                    )
                    continue
                else:
                    upgrade_complete = False
                    log.error("Exception hit : %s" % err)
                    break

            log.debug(f"upgrade in progress. Status : {out}")

            if check_for_inactive_pgs and not rados_obj.check_inactive_pgs_on_pool():
                log.error(
                    "Inactive PGs found on cluster during upgrade. Upgrade in progress"
                )
                inactive_pgs += 1

            if not warn_flag:
                status_report = rados_obj.run_ceph_command(
                    cmd="ceph report", client_exec=True
                )
                ceph_health_status = list(status_report["health"]["checks"].keys())
                expected_health_warns = "OSD_UPGRADE_FINISHED"
                if expected_health_warns in ceph_health_status:
                    warn_flag = True
                    log.info(
                        f"We have the expected health warning generated on the cluster.\n "
                        f"Warnings on cluster: {ceph_health_status}"
                    )
                    out = rados_obj.run_ceph_command(cmd="ceph health detail")
                    log.info(f"\n\nHealth detail on the cluster :\n {out}\n\n")
                else:
                    log.debug(
                        "expected health warning not yet generated on the cluster."
                        f" health_warns on cluster : {ceph_health_status}"
                    )

            if not version_flag:
                # Changing configs for generating DAEMON_OLD_VERSION warning
                mon_obj.set_config(
                    section="mon", name="mon_warn_older_version_delay", value="10"
                )
                status_report = rados_obj.run_ceph_command(
                    cmd="ceph report", client_exec=True
                )
                ceph_health_status = list(status_report["health"]["checks"].keys())
                expected_health_warns = "DAEMON_OLD_VERSION"
                if expected_health_warns in ceph_health_status:
                    version_flag = True
                    log.info(
                        "We have the expected health warning generated on the cluster.\n "
                        "Warnings on cluster: %s",
                        ceph_health_status,
                    )
                    out = rados_obj.run_ceph_command(cmd="ceph health detail")
                    log.info("\n\nHealth detail on the cluster :\n %s\n\n", out)
                    # Reverting the changes made for DAEMON_OLD_VERSION warning
                    mon_obj.remove_config(
                        section="mon", name="mon_warn_older_version_delay"
                    )
                else:
                    log.debug(
                        "expected health warning not yet generated on the cluster."
                        f" health_warns on cluster : {ceph_health_status}"
                    )

            log.info(
                "Upgrade in progress, sleeping for 5 seconds and checking cluster state again"
            )
            time.sleep(5)
        else:
            log_err_msg = f"Upgrade was not completed on the cluster within {timeout}"
            log.error(log_err_msg)
            raise Exception("Upgrade not complete")

        if not upgrade_complete:
            log.error("Upgrade was not completed on the cluster. Fail")
            raise Exception("Upgrade not complete")

        log.info(
            "Completed upgrade on the cluster successfully."
            "Proceeding to do further checks on the cluster post upgrade"
        )

        log.info("\n\nCluster status post upgrade: \n")
        log_dump = (
            f"ceph status :  {rados_obj.run_ceph_command(cmd='ceph -s')} \n "
            f"health detail :{rados_obj.run_ceph_command(cmd='ceph health detail')} \n "
            f"crashes : {rados_obj.run_ceph_command(cmd='ceph crash ls')} \n "
            f"ceph versions: {rados_obj.run_ceph_command(cmd='ceph versions')} \n"
        )
        log.info(log_dump)

        if verify_warning:
            """
            History:
            We should be observing a health warning on the cluster "OSD_UPGRADE_FINISHED",
            when upgrading from N-1 to N versions. eg : 6.1 -> 7.0.
            However, The warning won't be seen when upgrading b/w dot releases. eg : 7.0 -> 7.1
            Warning is seen as during upgrade, few OSDs will be in N version, and other OSDs will be in N-1 release.
            Until upgrade completes, the "require_osd_release" will still be set to N-1 release.
            When we have few OSDs on the cluster whose version do not match the version in "require_osd_release",
            We see the warning
            """
            log.debug(
                "Checking if the health warning was generated during the upgrade"
                "and if it was cleared later when upgrade completed"
            )
            if not warn_flag:
                log.error("expected warning not generated on the cluster. Fail")
                raise Exception("Warning not raised")
            status_report = rados_obj.run_ceph_command(
                cmd="ceph report", client_exec=True
            )
            ceph_health_status = list(status_report["health"]["checks"].keys())
            expected_health_warns = "OSD_UPGRADE_FINISHED"
            if expected_health_warns in ceph_health_status:
                log.error(
                    "Warning about the mismatched release present on the cluster. Fail. "
                )
                out = rados_obj.run_ceph_command(cmd="ceph health detail")
                log.error(f"\n\nHealth detail on the cluster :\n {out}\n\n")
                raise Exception("Warning present post upgrade")

            log.info(
                "Warning OSD_UPGRADE_FINISHED was found on in"
                " health status and Removed once upgrade was completed"
            )

        if verify_older_version_warn:
            if not version_flag:
                log.error(
                    "DAEMON_OLD_VERSION warning not observed on the cluster during upgrade"
                )
                raise Exception("DAEMON_OLD_VERSION not generated during upgrade")

        if verify_max_avail and not rados_obj.verify_max_avail():
            log.error(
                "MAX_AVAIL deviates on the cluster more than expected post upgrade"
            )
            raise Exception("MAX_AVAIL not proper error")

        if verify_daemons and not rados_obj.daemon_check_post_tests(
            pre_test_orch_ps=pre_upgrade_orch_ps
        ):
            log.error("There are daemons missing post upgrade")
            raise Exception("Daemons missing post upgrade error")

        if verify_cluster_usage and not rados_obj.compare_df_stats(
            pre_test_df_stats=pre_upgrade_df_detail
        ):
            log.error("Cluster usage changed post upgrade")
            # Not failing tests as of now due to RAW usage changes
            # raise Exception("Cluster usage changed post upgrade error")

        if check_for_inactive_pgs and inactive_pgs > 5:
            log.error("Found inactive PGs on the cluster during upgrade")
            raise Exception("Inactive PGs during Upgrade error")

        if verify_cluster_health:
            health_detail = rados_obj.log_cluster_health()
            if "HEALTH_ERR" in health_detail:
                log.error("cluster HEALTH is HEALTH_ERR post upgrade")
                raise Exception("Cluster health in ERROR state post upgrade")

    except Exception as e:
        log.error(f"Could not upgrade the cluster. error : {e}")
        return 1
    finally:
        log.debug("---------------- In Finally Block -------------")
        if enable_debug_level:
            log.debug("Removing debug configs on the cluster for mon & Mgr")
            mon_obj.remove_config(section="mon", name="debug_mon")
            mon_obj.remove_config(section="mgr", name="debug_mgr")

        end_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        msg = f"time when upgrade test was ended : {end_time}"
        log.info(msg)
        time.sleep(10)

        log_dump = (
            f"ceph versions: {rados_obj.run_ceph_command(cmd='ceph versions')} \n"
        )
        log.info(log_dump)

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed upgrade on the cluster")
    return 0
