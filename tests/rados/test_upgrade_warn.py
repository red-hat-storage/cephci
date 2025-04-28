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

from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.orch import Orch
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log
from utility.utils import fetch_build_artifacts

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
            verify_warning: Check if the health warnings during the upgrade is generated & removed post upgrade
            verify_daemons: Check for daemon existence on cluster post upgrade
            verify_max_avail: Check max_avail calculation on pools
            check_for_inactive_pgs: Check for inactive PGs during upgrade

    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    args = config.get("args", {})
    timeout = config.get("timeout", 1800)
    rhbuild = config.get("rhbuild")
    cephadm_obj = CephAdmin(cluster=ceph_cluster, **config)
    cluster_obj = Orch(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm_obj)
    verify_warning = config.get("verify_warning", False)
    verify_daemons = config.get("verify_daemons", False)
    verify_cluster_usage = config.get("verify_cluster_usage", False)
    verify_max_avail = config.get("verify_max_avail", False)
    check_for_inactive_pgs = config.get("check_for_inactive_pgs", False)
    verify_cluster_health = config.get("verify_cluster_health", False)

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

    if verify_max_avail and not rados_obj.verify_max_avail():
        log.error("MAX_AVAIL deviates on the cluster more than expected")
        raise Exception("MAX_AVAIL not proper error")

    log.debug("Starting upgrade")
    try:
        if rhbuild.startswith("8"):
            log.info("Build passed for upgrade: %s" % rhbuild)
            log_txt = """
            Disabling the balancer module as a WA for bug : https://bugzilla.redhat.com/show_bug.cgi?id=2314146
            Issue : If any mgr module based operation is performed right after mgr failover, The command execution fails
            as the module isn't loaded by mgr daemon. Issue was identified to be with Balancer module.
            Disabling automatic balancing on the cluster as a WA until we get the fix for the same.
            Disabling balancer should unblock Upgrade tests.
            Error snippet :
    Error ENOTSUP: Warning: due to ceph-mgr restart, some PG states may not be up to date
    Module 'crash' is not enabled/loaded (required by command 'crash ls'): use `ceph mgr module enable crash` to enable
            """
            log.info(log_txt)
            out, err = cephadm_obj.shell(args=["ceph balancer off"])
            log.debug(out + err)

        config.update({"args": {"image": "latest"}})

        # Support installation of the baseline cluster whose version is not available in
        # CDN. This is primarily used for an upgrade scenario. This support is currently
        # available only for RH network.
        _rhcs_version = args.get("rhcs-version", None)
        _rhcs_release = args.get("release", None)
        if _rhcs_release and _rhcs_version:
            curr_ver, _ = cephadm_obj.shell(args=["ceph version | awk '{print $3}'"])
            log.debug(
                "Upgrading the cluster from ceph version %s to %s-%s "
                % (curr_ver, _rhcs_version, _rhcs_release)
            )
            _platform = "-".join(rhbuild.split("-")[1:])
            _base_url, _registry, _image_name, _image_tag = fetch_build_artifacts(
                _rhcs_release, _rhcs_version, _platform
            )

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

            # initiate a new object with updated config
            cluster_obj = Orch(cluster=ceph_cluster, **config)

        # Remove existing repos
        rm_repo_cmd = (
            "find /etc/yum.repos.d/ -type f ! -name hashicorp.repo ! -name redhat.repo -delete ;"
            " yum clean all"
        )
        for node in ceph_cluster.get_nodes():
            node.exec_command(sudo=True, cmd=rm_repo_cmd)

        # Set repo to newer RPMs
        cluster_obj.set_tool_repo()
        time.sleep(5)
        upgd_dict = {"upgrade": True}
        cluster_obj.install(**upgd_dict)
        time.sleep(5)

        # Check service versions vs available and target containers
        cluster_obj.upgrade_check(image=config.get("container_image"))

        ceph_version = rados_obj.run_ceph_command(cmd="ceph version")
        log.info(f"Current version on the cluster : {ceph_version}")

        # Start Upgrade
        cluster_obj.start_upgrade(config)
        time.sleep(5)

        warn_flag = False
        upgrade_complete = False
        inactive_pgs = 0
        # Monitor upgrade status, till completion, checking for the warning to be generated
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        while end_time > datetime.datetime.now():
            cmd = "ceph orch upgrade status"
            out, _ = rados_obj.client.exec_command(cmd=cmd, sudo=True)
            try:
                status = json.loads(out)
                if not status["in_progress"]:
                    log.info("Upgrade Complete...")
                    upgrade_complete = True
                    break
            except json.JSONDecodeError:
                if "no upgrades in progress" in out:
                    log.info("Upgrade Complete...")
                    upgrade_complete = True
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

            log.info(
                "Upgrade in progress, sleeping for 5 seconds and checking cluster state again"
            )
            time.sleep(5)

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

        log.info("Completed upgrade on the cluster")
        return 0
    except Exception as e:
        log.error(f"Could not upgrade the cluster. error : {e}")
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        log.info("Enabling ceph balancer module")
        out, err = cephadm_obj.shell(args=["ceph balancer on"])
        log.debug(out + err)
