import json
import math
import random

from ceph.ceph_admin import CephAdmin
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83590892
    OSD superblock corruption leads to OSD failure mostly during reboots
     when a Customer is upgrading, in order to verify the recovery,
     Bluestore superblock or Bluestore Bdev label present at
     offset 0 will be smashed using dd command:
    Purpose of this test is to verify the redundance of Bluestore superblock
     and verify recovery with ceph-bluestore-tool
    1. Choose an OSD at random
    2. Check the offsets at which replicas of superblock will be available
    3. Check default value of bluestore_bdev_label_multi and bluestore_bdev_label_require_all parameters
    4. Retrieve chosen's OSD's node and device using ceph-volume lvm list
    5. Corrupt OSD superblock first 4KB block using dd command
        # dd if=/dev/zero of=<lvm path> bs=512 count=8
    6. Restart OSD for which superblock was corrupted
    7. Use fsck-repair from ceph-bluestore-tool to repair the corrupted bluestore bdev label
    8. Ensure OSD restart is succesful and no crashes are reported on the cluster

    # Test is only meant to be run on Squid and above builds
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    bluestore_obj = BluestoreToolWorkflows(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)

    log.info(
        "Running test case to verify Bluestore Superblock "
        "redundancy and consquently OSDs' resiliency during corruption"
    )

    try:
        # check the default value of new config parameters
        new_configs = {
            "bluestore_bdev_label_multi": True,
            "bluestore_bdev_label_require_all": True,
            "bluestore_bdev_label_multi_upgrade": False,
        }

        for config in new_configs.keys():
            out = mon_obj.get_config(section="osd", param=config)
            if out != new_configs[config]:
                log.error(f"Value of {config} is not as expected")
                log.error(f"Expected value: {new_configs[config]}")
                log.error(f"Actual value: {out}")
            log.info(f"Value of {config} is {out}")

        log.info("New config parameter verification is complete")

        # determine the number of replicas that would ideally exist on an OSD
        # pick an OSD at random from list of active OSDs
        osd_list = rados_obj.get_active_osd_list()
        osd_id = random.choice(osd_list)

        # determine the OSD size is GB
        osd_size_kb = rados_obj.get_osd_df_stats(filter=f"osd.{osd_id}")["nodes"][0][
            "kb"
        ]
        osd_size_gb = math.floor(int(osd_size_kb) / (1 << 20))
        log.info(f"Size of chosen OSD.{osd_id} is: {osd_size_gb} G")

        # a primitive way to determining how many replicas should exist
        # can be improved upon later
        replicas = [0, 1, 10, 100, 1000, 11000]
        for item in replicas:
            if min(item, osd_size_gb) == osd_size_gb:
                replica_count = replicas.index(item)
                break

        log.info(f"Number of Superblock replicas expected: {replica_count}")

        # corrupting the superblock at offset 0
        # determine the OSD node for chosen osd
        osd_node = rados_obj.fetch_host_node(
            daemon_type="osd", daemon_id=f"osd.{osd_id}"
        )

        # determine osd's block device path
        _cmd = f"cephadm shell -- ceph-volume lvm list {osd_id} --format json"
        out, _ = osd_node.exec_command(cmd=_cmd, sudo=True)
        lvm_out = json.loads(out)
        for entry in lvm_out[f"{osd_id}"]:
            if entry["type"] == "block":
                block_device = entry["lv_path"]
                break

        if not block_device:
            log.error(f"block device for OSD {osd_id} not found")
            raise Exception(f"block device for OSD {osd_id} not found")

        log.info(f"OSD block device path for chosen OSD.{osd_id}: {block_device}")

        # corrupting the block device by running dd command
        # intention if to corrupt first 4KB of the block device at offset 0
        _cmd = f"dd if=/dev/zero of={block_device} bs=512 count=8"
        out, err = osd_node.exec_command(cmd=_cmd, sudo=True)
        if "4096 bytes" not in out or "4096 bytes" not in err:
            log.error("block device corruption failed, try manual execution")
            raise Exception("block device corruption failed, try manual execution")

        # restarting the OSD for which block device was corrupted
        # expected to fail because superblock at offset 0 has been deleted

        # stopping the osd, should be successful
        if not rados_obj.change_osd_state(action="stop"):
            log.error(
                f"Failed to stop the desired OSD - osd.{osd_id} on {osd_node.hostname}"
            )
            raise Exception(
                f"Failed to stop the desired OSD - osd.{osd_id} on {osd_node.hostname}"
            )

        # starting the osd, should fail
        if rados_obj.change_osd_state(action="start", timeout=100):
            log.error(
                f"OSD restart should not have been possible for"
                f" osd.{osd_id} on {osd_node.hostname}"
            )
            raise Exception(
                f"OSD restart expected to fail for osd.{osd_id} on {osd_node.hostname}"
            )

        # now that OSD has been corrupted, fix the superblock by running
        # fsck repair using ceph-bluestore-tool
        out = bluestore_obj.run_consistency_check(osd_id=osd_id, deep=True)
        log.info(f"Output of fsck execution: {out}")

        out = bluestore_obj.repair(osd_id=osd_id)
        log.info(f"Output of repair command execution: {out}")

        # once bluestore superblock has been repaired, start the failed OSD again
        if not rados_obj.change_osd_state(action="start"):
            log.error(f"osd.{osd_id} should have started on {osd_node.hostname}")
            raise Exception(f"osd.{osd_id} should have started on {osd_node.hostname}")

        # to-do: include the crash detection module in finally block

        log.info(
            "Bluestore superblock corruption and recovery has been verified"
            f"successfully for OSD osd.{osd_id} on {osd_node.hostname}"
        )
    except Exception as e:
        log.error(f"Execution failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # Delete the created osd pool
        rados_obj.rados_pool_cleanup()

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
