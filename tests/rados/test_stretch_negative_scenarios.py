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
    stretch_bucket = config.get("stretch_bucket", "datacenter")
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "arbiter")

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
    rados_obj.detete_pool(pool=pool_name)

    # Creating a regular pool with updated crush rules currently works. Commenting return 1 line.
    # Bz : https://bugzilla.redhat.com/show_bug.cgi?id=2267850
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
    rados_obj.detete_pool(pool=pool_name)

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
        if rhbuild.split("-")[0] in ["7.1"]:
            log.info("THe warning should be generated on the cluster.")
            rados_obj.run_ceph_command(cmd=cmd3)
            raise Exception("Stretch Warning not generated error")
        else:
            log.info(f"Warning not expected in RHCS : {rhbuild}")
    else:
        log.info("Warning generated on the cluster.")
    rados_obj.run_ceph_command(cmd=cmd3)
    log.debug("Completed scenario of 2+ DCs")

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
        if rhbuild.split("-")[0] in ["7.1"]:
            log.info("THe warning should be generated on the cluster.")
            rados_obj.run_ceph_command(cmd=cmd2)
            raise Exception("Stretch Warning not generated error")
        else:
            log.info(f"Warning not expected in RHCS : {rhbuild}")
    else:
        log.info("Warning generated on the cluster")
    rados_obj.run_ceph_command(cmd=cmd2)
    log.debug("Completed scenario of uneven OSD weight")

    log.info("All the tests completed on the cluster, Pass!!!")
    return 0
