"""
Module to check the addition of  DB/wal devices to an existing OSD
"""

import math
import random
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    cephadm = CephAdmin(cluster=ceph_cluster)
    rados_object = RadosOrchestrator(node=cephadm)
    try:
        available_device_list = rados_object.get_orch_device_list()
        number_available_ssd = count_available_SSD(available_device_list)
        if number_available_ssd == 0:
            log.error("The SSD/NVME devices are not exist to perform the tests")
            return 1
        log.info(f"The total SSDs exist in the cluster are-{number_available_ssd}")
        available_ssd_path = get_ssd_path(available_device_list)
        log.info(f"The available SSD ins the cluster are-{available_ssd_path}")

        ceph_nodes = kw.get("ceph_nodes")
        osd_nodes = []
        for node in ceph_nodes:
            if node.role == "osd" and node.hostname in available_ssd_path:
                osd_nodes.append(node)
        test_count = 0

        for osd_node in osd_nodes:
            ssd_paths = available_ssd_path[osd_node.hostname]
            for path in ssd_paths:
                # Creating Volume group
                cmd_vgcreate = f"vgcreate LVMTest {path}"
                log.info(
                    f"Creating volume group in {osd_node.hostname} osd node at {path}"
                )
                vg_out, _ = osd_node.exec_command(
                    sudo=True, cmd=cmd_vgcreate, check_ec=False
                )
                log.info(f"The volume creation output is {vg_out}")
                if "successfully created" not in vg_out:
                    log.info(
                        f"Volume group is not created in the {osd_node.hostname}.Skipping the further tests."
                    )
                    continue
                log.info(
                    f"Volume group is created at {osd_node.hostname} osd node at {path}"
                )
                osds_id_in_node = rados_object.collect_osd_daemon_ids(osd_node)
                test_osd = random.choice(osds_id_in_node)
                log.info(f"Performing the configurations on {test_osd}")

                osd_size = get_osd_size(rados_object, test_osd)
                log.info(f"The {test_osd} total size is- {osd_size}")
                lv_size = math.floor((math.floor(osd_size * 0.04)) / (1024 * 1024))
                log.info(f"Creating the logical volume size with {lv_size} GB")
                # Create logical volume
                log.info("Creating logical volumes")
                cmd_lvcreate_base = f"lvcreate -l {lv_size} -n"
                if "nvme" in path:
                    cmd_lvcreate = f"{cmd_lvcreate_base} new_wal LVMTest -y"
                else:
                    cmd_lvcreate = f"{cmd_lvcreate_base} new_db LVMTest -y"
                lv_out, _ = osd_node.exec_command(
                    sudo=True, cmd=cmd_lvcreate, check_ec=False
                )
                log.info(f"Logical volumes are created in the {osd_node.hostname}")
                if "created" not in lv_out:
                    log.info(
                        f"Logical volume  is not created in the {osd_node.hostname}.Skipping the further tests."
                    )
                    continue
                # If in all nodes VG or lv creation fails then test_count is 0 and making the tes case fail.
                test_count += 1

                # Get the OSD_ID
                osd_uuid = rados_object.get_osd_uuid(test_osd)
                log.info(f" The osd-{test_osd} fsid is - {osd_uuid}")
                # stop test osd-
                if not rados_object.change_osd_state(action="stop", target=test_osd):
                    log.error(f"Unable to stop the OSD : {test_osd}")
                    raise Exception("Execution error")
                # Get the LVM list before the configuration
                before_configure_lvm = rados_object.get_ceph_volume_lvm_list(
                    osd_node, test_osd
                )
                log.info(
                    f" ceph volume lvm list for the osd{test_osd} before configuration is {before_configure_lvm}"
                )
                cmd_base = f"cephadm shell --name osd.{test_osd} --"
                if "nvme" in path:
                    device_flag = "wal"
                    cmd_add_db_wal = (
                        f"{cmd_base} ceph-volume lvm new-wal --osd-id {test_osd} --osd-fsid {osd_uuid} "
                        f"--target LVMTest/new_wal"
                    )
                    log.info(f"Configuring the new wal for the {test_osd} osd ")
                else:
                    device_flag = "db"
                    cmd_add_db_wal = (
                        f"{cmd_base} ceph-volume lvm new-db --osd-id {test_osd} --osd-fsid {osd_uuid} "
                        f"--target LVMTest/new_db"
                    )
                    log.info(f"Configuring the new db for the {test_osd} osd ")
                conf_output, _ = osd_node.exec_command(sudo=True, cmd=cmd_add_db_wal)

                log.info(f"The configuration out out is- {conf_output}")

                after_configure_lvm = rados_object.get_ceph_volume_lvm_list(
                    osd_node, test_osd
                )
                log.info(
                    f" ceph volume lvm list for the osd{test_osd} after configuration is {after_configure_lvm}"
                )
                # start test osd
                if not rados_object.change_osd_state(action="start", target=test_osd):
                    log.error(f"Unable to stop the OSD : {test_osd}")
                    raise Exception("Execution error")
                tc_verification_result = result_verification(
                    before_configure_lvm, after_configure_lvm, test_osd, device_flag
                )
                if tc_verification_result is False:
                    log.error(
                        f"{device_flag} configuration failed on {test_osd} in the {osd_node.hostname}"
                    )
                    return 1
        if test_count == 0:
            log.error(" Configuration failed on all the nodes")
            return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # log cluster health
        rados_object.log_cluster_health()
        # check for crashes after test execution
        if rados_object.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def count_available_SSD(total_list):
    """
    Method return the number of available SSD's in the cluster
    Args:
        total_list: Total device list in the json format
    Returns:Return the number of SSD's in the cluster

    """
    count = 0
    for device_list in total_list:
        for devices in device_list["devices"]:
            if devices["available"] is True and devices["human_readable_type"] == "ssd":
                count += 1
    return count


def get_ssd_path(total_list):
    """
    Method return the available SSD paths in the cluster
    Args:
        total_list: Total device list in the json format
    Returns: Method returns a dictionary with node name as key and SSD path list as values

    """
    ssd_paths = {}
    path = []
    for device_list in total_list:
        path.clear()
        for devices in device_list["devices"]:
            if devices["available"] and devices["human_readable_type"] == "ssd":
                path.append(devices["path"])
        values = path
        if values:
            ssd_paths[device_list["addr"]] = values
    return ssd_paths


def result_verification(before_configuration, after_configuration, osd_id, device_type):
    """
    Method performs the verification from the ceph volume list of before and after configuration
    Args:
        before_configuration: Before configuration ceph volume lvm list data
        after_configuration:  After configuration ceph volume lvm list data
        osd_id: osd id
        device_type: type of device for example wal/db

    Returns: True if verification pass
             False if verification fail

    """
    # Check1: Checking the number of devices defore and after configuration
    before_device_count = len(before_configuration[str(osd_id)])
    after_device_count = len(after_configuration[str(osd_id)])
    log.info(
        f"The device count before and after the configuration is {before_device_count} and {after_device_count}"
    )
    if after_device_count <= before_device_count:
        log.error("The Wal/DB is not configured")
        return False
    # Check2: Checking new db/wal is configured to the existing OSD
    before_conf_devices = []
    for device in before_configuration[str(osd_id)]:
        before_conf_devices.append(device["type"])
    after_conf_devices = []
    after_conf_paths = []
    for device in after_configuration[str(osd_id)]:
        after_conf_devices.append(device["type"])
        after_conf_paths.append(device["path"])
    before_conf_devices = set(before_conf_devices)
    after_conf_devices = set(after_conf_devices)
    result = after_conf_devices - before_conf_devices
    new_device = ", ".join(result)
    if new_device != device_type:
        log.error(f"The new {device_type} is not configured on {osd_id}")
        return False
    # Check3: Checking path for db/wal is added to the existing OSD
    if new_device == "wal":
        new_path = "/dev/LVMTest/new_wal"
    else:
        new_path = "/dev/LVMTest/new_db"
    if new_path not in after_conf_paths:
        log.error(f"The new device is not configures at path {new_path}")
        return False
    log.info(f"The new device is configures at path {new_path}")
    return True


def get_osd_size(node_object, osd_id):
    """
    Method returns the size of the OSD
    Args:
        node_object: node object for calling the method
        osd_id: osd id

    Returns: The total size of the osd

    """
    total_size_osd = node_object.get_osd_df_stats(
        tree=False, filter_by="name", filter=f"osd.{osd_id}"
    )["summary"]["total_kb"]
    return total_size_osd
