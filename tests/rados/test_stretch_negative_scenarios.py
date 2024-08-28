"""
This test module is used to test the -ve scenarios on a stretch enabled cluster

includes:

1. Try creation of EC pool. Should Fail.
2. Try changing the CRUSH rule to a non default rule. Should Fail.
3. Try addition of a new DC in stretch mode. Health warning generated.
4. Try making the Datacenter weights uneven. Health warning generated

"""

import random
import time
from collections import namedtuple

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from tests.rados.test_stretch_site_down import (
    get_stretch_site_hosts,
    stretch_enabled_checks,
)
from utility.log import Log

log = Log(__name__)

Hosts = namedtuple("Hosts", ["dc_1_hosts", "dc_2_hosts", "tiebreaker_hosts"])


def run(ceph_cluster, **kw):
    """
    performs post stretch mode deployment negative tests in stretch mode
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """

    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    rhbuild = config.get("rhbuild")
    pool_name = config.get("pool_name", "test_stretch_io")
    mon_obj = MonitorWorkflows(node=cephadm)
    stretch_bucket = config.get("stretch_bucket", "datacenter")
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "arbiter")

    try:
        if not stretch_enabled_checks(rados_obj=rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info(
            "Starting tests performing -ve scenarios in Stretch mode Pre-checks Passed"
        )
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

        log.debug(f"Hosts present in {stretch_bucket} : {dc_1_name} : {dc_1_hosts}")
        log.debug(f"Hosts present in {stretch_bucket} : {dc_2_name} : {dc_2_hosts}")
        log.debug(
            f"Hosts present in {stretch_bucket} : {tiebreaker_mon_site_name} : {tiebreaker_hosts}"
        )

        # Creating test pool to check if regular pool creation works
        log.info("Scenario 1: Create regular replicated pools. Should be successful")
        try:
            if not rados_obj.create_pool(pool_name=pool_name):
                log.error(f"Failed to create pool : {pool_name}")
                raise Exception("Test execution failed")
        except Exception as err:
            log.error(
                f"Unable to create regular pool on Stretch mode. SHould be possible.Err: {err} "
            )
            raise Exception("Pool not created error")
        # Sleeping for 5 seconds for pool to be populated in the cluster
        time.sleep(5)

        # Trying to create an EC pool. Should not be possible in stretch mode
        log.info(
            "Scenario 2: Create an EC pool. Should not be possible in stretch mode"
        )
        try:
            pool_name = "ec-stretch-pool"
            if rados_obj.create_erasure_pool(name="stretch-ec", pool_name=pool_name):
                log.error("Able to create EC pool in Stretch mode. Fail")
                return 1
            log.info("Could not create a EC pool")
        except Exception as err:
            log.info(
                f"Unable to create EC pool on Stretch mode.. Scenario pass.. Err hit: {err} "
            )
        log.debug("Completed test for ec pool")
        rados_obj.delete_pool(pool=pool_name)

        # Creating a regular pool with updated crush rules currently works. Commenting return 1 line.
        # Bz : https://bugzilla.redhat.com/show_bug.cgi?id=2267850
        log.info(
            "Scenario 3: Create an RE pool with non stretch mode CRUSH rule."
            " Should not be possible in stretch mode. Currently blocked with bug 2267850"
        )
        try:
            pool_name = "non-stretch-rule-pool"
            if rados_obj.create_pool(
                pool_name=pool_name, crush_rule="replicated_rule", size=2
            ):
                log.error(f"We are able to create non-default rule pool : {pool_name}")
                # return 1
            else:
                log.info("non default stretch rule Pool could not be created. Pass")
        except Exception as err:
            log.error(
                f"UnAble to create regular pool on Stretch mode. Should Not be possible. test Pass. Err message {err} "
            )
        log.debug("Completed test for non-default rule pool")
        rados_obj.delete_pool(pool=pool_name)

        log.info(
            "Scenario 4: having more than 3 DCs in stretch mode. Should not be possible in stretch mode"
        )
        # Trying to Add another DC in stretch mode. We should see health warning generated.
        bucket_name = "test-bkt"
        warning = "INCORRECT_NUM_BUCKETS_STRETCH_MODE"
        cmd1 = f"ceph osd crush add-bucket {bucket_name} {stretch_bucket}"
        rados_obj.run_ceph_command(cmd=cmd1)
        cmd2 = f"ceph osd crush move {bucket_name} root=default"
        rados_obj.run_ceph_command(cmd=cmd2)
        cmd3 = f"ceph osd crush rm {bucket_name}"
        log.info(
            f"New bucket {bucket_name} of type {stretch_bucket} created and moved under root"
            f"Checking for health warnings generated. sleeping for 10 seconds"
        )
        time.sleep(10)
        if not rados_obj.check_health_warning(warning=warning):
            log.error(f"Warning : {warning} not generated on the cluster")
            if float(rhbuild.split("-")[0]) >= 7.1:
                log.info("THe warning should be generated on the cluster.")
                rados_obj.run_ceph_command(cmd=cmd3)
                raise Exception("Stretch Warning not generated error")
            else:
                log.info(f"Warning not expected in RHCS : {rhbuild}")
        else:
            log.info("Warning generated on the cluster.")
        rados_obj.run_ceph_command(cmd=cmd3)
        log.debug("Completed scenario of 2+ DCs")

        log.info(
            "Scenario 5: having uneven site weights. Should not be possible in stretch mode"
        )
        # Making the site weights unequal. We should see health warning generated.
        bucket_name = random.choice(dc_2_hosts)
        log.debug(
            f"Chose host : {bucket_name} from {dc_2_name} to move to trigger weight imbalance"
        )
        warning = "UNEVEN_WEIGHTS_STRETCH_MODE"
        cmd1 = f"ceph osd crush move {bucket_name} {stretch_bucket}={dc_1_name}"
        rados_obj.run_ceph_command(cmd=cmd1)
        log.info(
            f"bucket {bucket_name} moved to {stretch_bucket}={dc_1_name}"
            f"Checking for health warnings generated. sleeping for 10 seconds"
        )
        time.sleep(10)
        cmd2 = f"ceph osd crush move {bucket_name} {stretch_bucket}={dc_2_name}"
        if not rados_obj.check_health_warning(warning=warning):
            log.error(f"Warning : {warning} not generated on the cluster")
            if float(rhbuild.split("-")[0]) >= 7.1:
                log.info("THe warning should be generated on the cluster.")
                rados_obj.run_ceph_command(cmd=cmd2)
                raise Exception("Stretch Warning not generated error")
            else:
                log.info(f"Warning not expected in RHCS : {rhbuild}")
        else:
            log.info("Warning generated on the cluster")
        rados_obj.run_ceph_command(cmd=cmd2)
        log.debug("Completed scenario of uneven OSD weight")

        log.info(
            "Scenario 6: Adding mon daemons without location attributes. Should not be possible in stretch mode"
        )
        # Trying to add a mon without location attributes. should fail
        """
        Warning seen on cluster:
        # ceph mon add ceph-pdhiran-w420vn-node5 10.0.208.191
        Error EINVAL: We are in stretch mode and new monitors must have a location,
        but could not parse your input location to anything real; [] turned into an empty map!
        RFEs Filed for the scenario:
        1. https://bugzilla.redhat.com/show_bug.cgi?id=2280926
        2. https://bugzilla.redhat.com/show_bug.cgi?id=2281638
        """
        mon_nodes = ceph_cluster.get_nodes(role="mon")
        cluster_nodes = ceph_cluster.get_nodes()

        # Get items from cluster_nodes which are not present in mon_nodes
        non_mon_nodes = [node for node in cluster_nodes if node not in mon_nodes]
        log.debug(f"Hosts on cluster without mon daemon are : {non_mon_nodes}")
        if len(non_mon_nodes) >= 1:
            new_addition_host = non_mon_nodes[0]
            flag = False
            # Setting the mon service as unmanaged
            if not mon_obj.set_mon_service_managed_type(unmanaged=True):
                log.error("Could not set the mon service to unmanaged")
                raise Exception("mon service not unmanaged error")

            try:
                # Adding the mon to the cluster without crush locations. -ve scenario
                mon_obj.add_mon_service(
                    host=new_addition_host,
                    add_label=False,
                )
                flag = True
            except Exception as err:
                log.info(
                    f"Could not add mon without location attributes."
                    f"Error message displayed : {err}"
                )
            if flag:
                log.error(
                    "Command execution passed for mon deployment in stretch mode without location "
                )

                # Checking if the new mon added is part of the quorum
                quorum = mon_obj.get_mon_quorum_hosts()
                if new_addition_host.hostname in quorum:
                    log.error(
                        f"selected host : {new_addition_host.hostname} mon is present as part of Quorum post addition,"
                        f"in -ve scenario"
                    )
                    raise Exception(
                        "Mon in quorum error post addition without location. -ve scenario"
                    )
            log.info(
                "-ve Test Pass: Add Mon daemon without location attributes into cluster. Mon could not be added"
            )
            # Setting the mon service as unmanaged
            if not mon_obj.set_mon_service_managed_type(unmanaged=False):
                log.error("Could not set the mon service to unmanaged")
                raise Exception("mon service not unmanaged error")
        else:
            log.info("No host found without mon daemon to test the -ve scenario")

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # removal of rados pools
        rados_obj.rados_pool_cleanup()
        mon_obj.set_mon_service_managed_type(unmanaged=False)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    log.info("All the tests completed on the cluster, Pass!!!")
    return 0
