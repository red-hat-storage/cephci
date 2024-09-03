"""
This test module is used to deploy a stretch cluster on the given set of nodes with location attributes added
during deployment via spec file.

Note that all the daemons need to be co-located on the cluster. Esp mon + OSD daemons for the data sites
Issue:
    There won't be any clear site mentioned for the mons daemons to be added, if they are in the data sites,
     and do not contain OSDs. Currently, there is no intelligence to smartly add the mon crush locations.
     ( if none already provided with mon deployments, or otherwise specified )

Example of the cluster taken from the MDR deployment guide for ODF.
ref: https://access.redhat.com/documentation/en-us/red_hat_openshift_data_foundation/4.12/html/
configuring_openshift_data_foundation_disaster_recovery_for_openshift_workloads/metro-dr-solution#hardware_requirements

"""

import random
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonElectionStrategies
from tests.rados.stretch_cluster import (
    setup_crush_rule,
    setup_crush_rule_with_no_affinity,
    wait_for_clean_pg_sets,
)
from tests.rados.test_stretch_site_down import (
    get_stretch_site_hosts,
    stretch_enabled_checks,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    enables connectivity mode and deploys stretch cluster with arbiter mon node
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """

    log.info("Deploying stretch cluster with arbiter mon node")
    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonElectionStrategies(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    stretch_rule_name = config.get("stretch_rule_name", "stretch_rule")
    no_affinity_crush_rule = config.get("no_affinity", False)
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "arbiter")
    stretch_pre_deploy_negative_workflows = config.get("negative_scenarios", False)
    stretch_bucket = config.get("stretch_bucket", "datacenter")

    log.debug("Running pre-checks to deploy Stretch mode on the cluster")

    try:
        # Check-1 : Only 2 data sites permitted on cluster
        # getting the CRUSH buckets added into the cluster
        cmd = "ceph osd tree"
        buckets = rados_obj.run_ceph_command(cmd)
        dc_buckets = [d for d in buckets["nodes"] if d.get("type") == "datacenter"]
        if len(dc_buckets) > 2:
            log.error(
                f"There can only be two data sites in the stretch mode. Buckets present in cluster: {dc_buckets}"
            )
            raise Exception("Stretch mode cannot be enabled")
        dc_1 = dc_buckets.pop()
        dc_2 = dc_buckets.pop()

        # Check-2: 5 mons should be present on the cluster
        # Getting the mon daemons configured on the cluster
        cmd = "ceph mon dump"
        out = rados_obj.run_ceph_command(cmd)
        mons_dict = out["mons"]
        log.debug(f"Mons present on the cluster : {[mon for mon in mons_dict]}")
        if len(mons_dict) != 5:
            log.error(
                f"Stretch Cluster needs 5 mons to be present on the cluster. 2 for each DC & 1 for arbiter node"
                f"Currently configured on cluster : {len(mons_dict)}"
            )
            raise Exception("Stretch mode cannot be enabled")

        # Check-3: Both sites should be of equal weights
        log.debug(
            "Checking if thw weights of two DC's are same."
            " Currently, Stretch mode requires the site weights to be same."
        )
        cmd = "ceph osd tree"
        tree_op, _ = client_node.exec_command(cmd=cmd, sudo=True)
        datacenters = re.findall(r"\n(-\d+)\s+([\d.]+)\s+datacenter\s+(\w+)", tree_op)
        weights = [(weight, name) for _, weight, name in datacenters]
        for dc in weights:
            log.debug(f"Datacenter: {dc[0]}, Weight: {dc[1]}")
        if weights[0][0] != weights[1][0]:
            log.error(
                "The two site weights are not same. Currently, Stretch mode requires the site weights to be same."
            )
            raise Exception("Stretch mode cannot be enabled")

        # Check-4: Only replicated pools are allowed on the cluster with default rule
        # Finding any stray EC pools that might have been left on cluster
        pool_dump = rados_obj.run_ceph_command(cmd="ceph osd dump")
        for entry in pool_dump["pools"]:
            if entry["type"] != 1 and entry["crush_rule"] != 0:
                log.error(
                    f"A non-replicated pool found : {entry['pool_name']}, With non-default Crush Rule"
                    "Currently Stretch mode supports only Replicated pools."
                )
                raise Exception("Stretch mode cannot be enabled")

        log.info(
            "Completed Pre-Checks on the cluster, able to deploy Stretch mode on the cluster"
        )

        # Setting up CRUSH rules on the cluster
        if no_affinity_crush_rule:
            # Setting up CRUSH rules on the cluster without read affinity
            if not setup_crush_rule_with_no_affinity(
                node=client_node,
                rule_name=stretch_rule_name,
            ):
                log.error("Failed to Add crush rules in the crush map")
                raise Exception("Stretch mode deployment Failed")
        else:
            if not setup_crush_rule(
                node=client_node,
                rule_name=stretch_rule_name,
                site1=dc_1["name"],
                site2=dc_2["name"],
            ):
                log.error("Failed to Add crush rules in the crush map")
                raise Exception("Stretch mode deployment Failed")

        # Sleeping for 5 sec for the crush rules to be active
        time.sleep(5)

        # Waiting for Cluster to be active + Clean post Crush changes.
        wait_for_clean_pg_sets(rados_obj)

        # Setting up the election strategy on the cluster to connectivity
        if not mon_obj.set_election_strategy(mode="connectivity"):
            log.error("could not set election strategy to connectivity mode")
            raise Exception("Stretch mode deployment Failed")

        # Sleeping for 2 seconds after strategy update.
        time.sleep(2)

        # Checking updated election strategy in mon map
        strategy = mon_obj.get_election_strategy()
        if strategy != 3:
            log.error(
                f"cluster created election strategy other than connectivity, i.e {strategy}"
            )
            raise Exception("Stretch mode deployment Failed")
        log.info("Enabled connectivity mode on the cluster")

        def set_mon_crush_location(dc) -> bool:
            """
            Method to set the CRUSH location for the mon daemons
            Args:
                dc: datacenter bucket dump collected from ceph osd crush tree
            Returns:
                (bool) True -> Pass | False -> Fail
            """
            log.debug(f"setting up mon daemons from site : {dc['name']} ")
            for crush_id in dc["children"]:
                for entry in buckets["nodes"]:
                    if entry.get("id") == crush_id:
                        if entry["name"] in [mon["name"] for mon in mons_dict]:
                            cmd = f"ceph mon set_location {entry['name']} datacenter={dc['name']}"
                            try:
                                rados_obj.run_ceph_command(cmd)
                            except Exception:
                                log.error(
                                    f"Failed to set location on mon : {entry['name']}"
                                )
                                return False
            log.debug(
                "Iterated through all the children of the passed bucket and set the location attributes"
            )
            return True

        mon_dump_cmd = "ceph mon dump"
        loc_absent = False
        mons = rados_obj.run_ceph_command(mon_dump_cmd)
        for mon in mons["mons"]:
            if mon["crush_location"] == "{}":
                log.debug(f"Mon location attributes not present on mon : {mon}")
                loc_absent = True

        if loc_absent:
            log.debug(
                "Crush locations not added during bootstrap,"
                "Manually adding the crush locations on the Mon daemons"
            )
            # Setting up the mon locations on the cluster
            if not (set_mon_crush_location(dc=dc_1) & set_mon_crush_location(dc=dc_2)):
                log.error("Failed to set Crush locations to the Data Site mon daemons")
                raise Exception("Stretch mode deployment Failed")

            tiebreaker_mon = None
            mons = rados_obj.run_ceph_command(mon_dump_cmd)
            for mon in mons["mons"]:
                if (
                    mon["crush_location"]
                    == "{datacenter=" + f"{tiebreaker_mon_site_name}" + "}"
                ):
                    tiebreaker_mon = mon["name"]

            if not tiebreaker_mon:
                # Directly selecting tiebreaker mon as we have confirmed only 1 mon exists without location
                tiebreaker_mon = [
                    entry["name"]
                    for entry in mons["mons"]
                    if entry.get("crush_location") == "{}"
                ][0]
                # Setting up CRUSH location on the final Arbiter mon
                arbiter_cmd = f"ceph mon set_location {tiebreaker_mon} datacenter={tiebreaker_mon_site_name}"
            try:
                rados_obj.run_ceph_command(arbiter_cmd)
            except Exception:
                log.error(f"Failed to set location on mon : {entry['name']}")
                raise Exception("Stretch mode deployment Failed")

            log.debug("Completed setting up crush locations on the mon daemons")
        else:
            log.debug(
                "Mon locations present on the cluster with bootstrap. "
                "Manually setting the crush locations not required"
            )
        log.info(
            "Verified that CRUSH location attributes Set-up on all the mon daemons"
        )
        try:
            mons = rados_obj.run_ceph_command(mon_dump_cmd)
            for mon in mons["mons"]:
                if (
                    mon["crush_location"]
                    == "{datacenter=" + f"{tiebreaker_mon_site_name}" + "}"
                ):
                    tiebreaker_mon = mon["name"]
        except Exception as err:
            log.error(
                f"Failed to find mon with tiebreaker site : {tiebreaker_mon_site_name} \n"
                f"Error : {err}"
            )
            raise Exception("Tiebreaker mon not set")

        osd_tree_cmd = "ceph osd tree"
        buckets = rados_obj.run_ceph_command(osd_tree_cmd)
        dc_buckets = [d for d in buckets["nodes"] if d.get("type") == stretch_bucket]
        dc_1 = dc_buckets.pop()
        dc_1_name = dc_1["name"]
        dc_2 = dc_buckets.pop()
        dc_2_name = dc_2["name"]
        all_hosts = get_stretch_site_hosts(
            rados_obj=rados_obj, tiebreaker_mon_site_name=tiebreaker_mon_site_name
        )
        dc_1_hosts = all_hosts.dc_1_hosts
        dc_2_hosts = all_hosts.dc_2_hosts
        tiebreaker_hosts = all_hosts.tiebreaker_hosts

        log.debug(f"Hosts present in Datacenter : {dc_1_name} : {dc_1_hosts}")
        log.debug(f"Hosts present in Datacenter : {dc_2_name} : {dc_2_hosts}")
        log.debug(
            f"Hosts present in Datacenter : {tiebreaker_mon_site_name} : {tiebreaker_hosts}"
        )

        # Enabling Stretch mode on the cluster
        # Enabling the stretch cluster mode
        log.info(f"tiebreaker node provided: {tiebreaker_mon}")
        stretch_enable_cmd = f"ceph mon enable_stretch_mode {tiebreaker_mon} {stretch_rule_name} datacenter"

        if stretch_pre_deploy_negative_workflows:
            log.info("Performing Pre deployment -ve scenarios in stretch mode")
            # Stretch mode enablement should not be possible if there is EC pool.

            log.info(
                "Scenario 1: EC pool. With EC pool, stretch mode not to be deployed"
            )
            pool_name = "ec-stretch-pool"
            if not rados_obj.create_erasure_pool(
                name="stretch-ec", pool_name=pool_name
            ):
                log.error("UnAble to create EC pool Fail")
                raise Exception("EC pool not created error")
            try:
                cephadm.shell([stretch_enable_cmd])
                if stretch_mode_status(rados_obj=rados_obj):
                    log.error(
                        "The cluster has stretch mode enabled with EC pool...Exiting..."
                    )
                    return 1
            except Exception as err:
                log.info(
                    f"Unable to deploy stretch mode on cluster.. Scenario pass.. \nErr hit: {err} "
                )
            rados_obj.delete_pool(pool=pool_name)

            log.info("Scenario 2: non-default rule pool")
            pool_name = "non-stretch-rule-pool"
            if not rados_obj.create_pool(
                pool_name=pool_name, crush_rule="replicated_rule", size=2
            ):
                log.error(f"Failed to create pool : {pool_name}")
                raise Exception(
                    "Regular pool with non-default crush rule not created. Error.."
                )
            if stretch_mode_status(rados_obj=rados_obj):
                log.error(
                    "The cluster has stretch mode enabled with non-default crush rule pule...Exiting..."
                )
                return 1
            try:
                cephadm.shell([stretch_enable_cmd])
            except Exception as err:
                log.info(
                    f"Unable to deploy stretch mode on cluster.. Scenario pass.. \nErr hit: {err} "
                )
            rados_obj.delete_pool(pool=pool_name)

            log.info("Scenario 3: more than 2 DC's")
            # Trying to Add another DC in stretch mode. stretch mode deployment should Fail
            bucket_name = "test-bkt"
            stretch_bucket = "datacenter"
            cmd1 = f"ceph osd crush add-bucket {bucket_name} {stretch_bucket}"
            rados_obj.run_ceph_command(cmd=cmd1)
            cmd2 = f"ceph osd crush move {bucket_name} root=default"
            rados_obj.run_ceph_command(cmd=cmd2)
            time.sleep(10)
            try:
                cephadm.shell([stretch_enable_cmd])
                if stretch_mode_status(rados_obj=rados_obj):
                    log.error(
                        "The cluster has stretch mode enabled with more than 2 DCs...Exiting..."
                    )
                    return 1
            except Exception as err:
                log.info(
                    f"Unable to deploy stretch mode on cluster when there are more than 2 DCs.."
                    f" Scenario pass.. \n Err hit: {err} "
                )
            cmd3 = f"ceph osd crush rm {bucket_name}"
            rados_obj.run_ceph_command(cmd=cmd3)

            log.info("Scenario 4: Uneven site weights")
            # Making the site weights unequal. Stretch mode should not be enabled
            bucket_name = random.choice(dc_2_hosts)
            log.debug(
                f"Chose host : {dc_2_hosts} from {dc_2_name} to move to trigger weight imbalance"
            )
            cmd1 = f"ceph osd crush move {bucket_name} {stretch_bucket}={dc_1_name}"
            rados_obj.run_ceph_command(cmd=cmd1)
            log.info(f"bucket {bucket_name} moved to the other DC {dc_1_name}")
            time.sleep(10)
            cmd2 = f"ceph osd crush move {bucket_name} {stretch_bucket}={dc_2_name}"
            try:
                cephadm.shell([stretch_enable_cmd])
                if stretch_mode_status(rados_obj=rados_obj):
                    log.error(
                        "The cluster has stretch mode enabled with uneven site weights...Exiting..."
                    )
                    return 1
            except Exception as err:
                log.info(
                    f"Unable to deploy stretch mode on cluster when the site weights are uneven.."
                    f" Scenario pass.. Err hit: {err} "
                )
            rados_obj.run_ceph_command(cmd=cmd2)

            log.info("Completed all the pre-deployment -ve scenarios on stretch mode")
        try:
            log.debug("Executing command to enable stretch mode")
            cephadm.shell([stretch_enable_cmd])
        except Exception as err:
            log.error(
                f"Error while enabling stretch rule on the datacenter. Command : {cmd}"
            )
            log.error(err)
            raise Exception("Stretch mode deployment Failed")
        time.sleep(5)

        if not stretch_enabled_checks(rados_obj=rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        # Checking the stretch mode status
        stretch_details = rados_obj.get_stretch_mode_dump()
        if not stretch_details["stretch_mode_enabled"]:
            log.error(
                f"Stretch mode not enabled on the cluster. Details : {stretch_details}"
            )
            raise Exception("Stretch mode Post deployment tests Failed")

        # wait for PG's to settle down with new crush rules after deployment of stretch mode
        if not wait_for_clean_pg_sets(rados_obj):
            log.error(
                "Cluster did not reach active + Clean state post deployment of stretch mode"
            )
            raise Exception("Stretch mode Post deployment tests Failed")

        # Checking if the pools have been updated/ can be created with the new crush rules
        pool_name = "test_crush_pool"
        if not rados_obj.create_pool(pool_name="test_crush_pool"):
            log.error(
                f"Failed to create pool: {pool_name} post stretch mode deployment"
            )
            raise Exception("Stretch mode Post deployment tests Failed")

        acting_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
        if len(acting_set) != 4:
            log.error(
                f"There are {len(acting_set)} OSD's in PG. OSDs: {acting_set}. Stretch cluster requires 4"
            )
            raise Exception("Stretch mode Post deployment tests Failed")
        log.info(
            f"Acting set : {acting_set} Consists of 4 OSD's per PG. test pool : {pool_name}"
        )
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # removal of rados pool
        log.debug(f"Deleting the pool created ; {pool_name}")
        rados_obj.delete_pool(pool=pool_name)

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Stretch mode deployed successfully")
    return 0


def stretch_mode_status(rados_obj) -> bool:
    """
    Returns the status of stretch mode on cluster.

    Args:
        rados_obj: rados object for command execution

    Returns:
        if stretch mode is enabled on cluster -> True,
        If stretch mode is not enabled on cluster -> False
    """
    log.debug("Running checks to see if stretch mode is deployed on the cluster")
    stretch_details = rados_obj.get_stretch_mode_dump()
    return stretch_details["stretch_mode_enabled"]
