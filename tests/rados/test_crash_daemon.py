"""
Module to verify health warning generated after crashing a ceph daemon
"""
import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83573855
    Test to verify health warning generated after inducing a daemon crash
    using crash mgr module
    1. Choose an OSD daemon and a Mon daemon at random
    2. Ensure cluster is in good state
    3. Use crash common module to crash both the chosen daemons
    4. Remove any existing or newly generated crash
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)

    log.info("Test to verify crash warning upon inducing a crash for OSD and MON")

    try:
        # 1. Crash an OSD daemon and inject crash manually
        out, _ = cephadm.shell(args=["ceph osd ls"])
        osd_list = out.strip().split("\n")
        osd_id = random.choice(osd_list)
        log.debug(f"Chosen OSD to crash: OSD.{osd_id}")
        # ensure chosen daemon is not already in crash state
        health_detail, _ = cephadm.shell(["ceph health detail"])
        assert f"osd.{osd_id} crashed" not in health_detail
        assert rados_obj.crash_ceph_daemon(daemon="osd", id=osd_id, manual_inject=True)

        # 2. Crash a MON daemon and let the crash show up automatically
        mon_list = rados_obj.run_ceph_command("ceph status")["quorum_names"]
        mon_id = mon_list[0]
        log.debug(f"Chosen MON to crash: mon.{mon_id}")
        # ensure chosen daemon is not already in crash state
        health_detail, _ = cephadm.shell(["ceph health detail"])
        assert f"mon.{mon_id} crashed" not in health_detail
        assert rados_obj.crash_ceph_daemon(daemon="mon", id=mon_id)

        # archive all existing crashes
        crash_id_list = [
            x["crash_id"] for x in rados_obj.run_ceph_command("ceph crash ls-new")
        ]
        for crash_id in crash_id_list:
            # log the ceph crash info before archiving
            log.debug(f"Printing crash id: {crash_id} info")
            crash_info, _ = cephadm.shell([f"ceph crash info {crash_id}"])
            log.debug(crash_info)
            cephadm.shell([f"ceph crash archive {crash_id}"])
        time.sleep(5)
        if len(rados_obj.run_ceph_command("ceph crash ls-new")) != 0:
            log.error("Crash archive failed, crash ls-new still shows crashes")
            raise AssertionError(
                "Crash archive failed, crash ls-new still shows crashes"
            )
        log.info("Archiving of all all existing crashes complete")

        # logging ceph crash stat
        out, _ = cephadm.shell(["ceph crash stat"])
        log.info(out)

        # verification of crash related config parameters
        # https://docs.ceph.com/en/latest/mgr/crash/#options
        assert mon_obj.set_config(
            section="mgr", name="mgr/crash/warn_recent_interval", value=1
        )
        assert mon_obj.set_config(
            section="mgr", name="mgr/crash/retain_interval", value=2
        )
    except AssertionError as AE:
        log.error(f"Verification failed with exception: {AE.__doc__}")
        log.exception(AE)
        return 1
    finally:
        log.info("\n\n ********* Executing finally block ******** \n\n")
        time.sleep(30)
        # fetch crash list
        crash_ls_json = rados_obj.run_ceph_command("ceph crash ls")
        # clear all the crashes
        if crash_ls_json:
            for crash in crash_ls_json:
                cephadm.shell([f"ceph crash rm {crash['crash_id']}"])
            rados_obj.change_osd_state(action="restart", target=osd_id)
        cephadm.shell(["ceph orch restart mgr"])
        time.sleep(10)

        mon_obj.remove_config(section="mgr", name="mgr/crash/warn_recent_interval")
        mon_obj.remove_config(section="mgr", name="mgr/crash/retain_interval")

    log.info(
        "Completed verification of heath warnings when a crash is artificially induced for a daemon"
    )
    return 0
