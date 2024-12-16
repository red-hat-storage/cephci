"""
Test Module to perform specific functionalities of ceph-volume.
 - ceph-volume lvm list [OSD_ID | DEVICE_PATH | VOLUME_GROUP/LOGICAL_VOLUME]
 - ceph-volume lvm zap [--destroy] [--osd-id OSD_ID | --osd-fsid OSD_FSID | DEVICE_PATH ]
 - ceph-volume --help
"""

import json
import random

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.cephvolume_workflows import CephVolumeWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from cli.utilities.operations import wait_for_osd_daemon_state
from tests.rados.rados_test_util import get_device_path
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to perform +ve workflows for the ceph-volume utility
    Returns:
        1 -> Fail, 0 -> Pass

    Zap Steps:
    1.  Deploy a Ceph cluster
    2.  Fetch all active OSDs and select random OSD from the list to zap
    3.  Set OSD service to unmanaged
    4.  Remove selected OSD from cluster without passing --zap flag to
         test ceph-volume utility
    5.  Zap using ceph-volume utility with/without --destroy flag
    6.  Validate OSD details are removed from ceph-volume lvm list
    7.  If --destroy flag is not passed, Validate file system is wiped from OSD device
    8.  If --destroy flag is passed, Validate LVs, VGs and PVs are
        removed from the host
    9.  If --destroy flag is not passed, Validate LVs, VGs and PVs are
        not removed from the host
    10. Set OSD service back to managed
    11. If --destroy flag is not passed, Execute zap with --destroy flag
        to clear LVs, VGs and PVs related to OSD device
    12. Wait until removed OSD is added back, since OSD service is set to managed
    13. Perform steps#2 through steps#12 for different options of zap command
    """
    log.info(run.__doc__)
    config = kw["config"]

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    volumeobject = CephVolumeWorkflows(node=cephadm)

    try:

        if config.get("zap_with_destroy_flag") or config.get(
            "zap_without_destroy_flag"
        ):

            options_list = [
                ["device"],
                ["osd_id"],
                ["osd_id", "osd_fsid"],
                ["logical_volume"],
            ]

            # Add --destroy option to each options_list.
            # example: ["device", "--destroy"], ["osd_id", "--destroy"], ["osd_id", "osd_fsid", "--destroy"]
            if config.get("zap_with_destroy_flag"):
                options_list = [option + ["--destroy"] for option in options_list]

            for option in options_list:

                if (
                    config.get("zap_without_destroy_flag")
                    and len(option) == 1
                    and option[0] == "device"
                ):
                    log.debug(
                        "Issue with `ceph-volume lvm zap device` execution\n"
                        "BZ 2329904\n"
                        "continuing with next option"
                    )
                    continue

                log.info(
                    f"\n -------------------------------------------"
                    f"\n Option selected: {option}"
                    f"\n -------------------------------------------"
                )

                log.info(
                    "fetching active OSDs and selecting random OSD for 'ceph-volume lvm zap' test\n"
                )

                osd_list = rados_obj.get_osd_list(status="up")
                osd_id = random.choice(osd_list)

                log.info(
                    f"Active OSDs in the cluster: {osd_list}\n"
                    f"OSD {osd_id} selected at random for zap test\n"
                    f"Proceeding to fetch Host, osd_fsid and device path for OSD {osd_id}\n"
                )

                osd_host = rados_obj.fetch_host_node(
                    daemon_type="osd", daemon_id=osd_id
                )
                osd_fsid = rados_obj.get_osd_uuid(osd_id)
                dev_path = get_device_path(osd_host, osd_id)
                osd_lv_name = volumeobject.get_osd_lvm_resource_name(
                    osd_host=osd_host, osd_id=osd_id, resource="lv"
                )
                osd_vg_name = volumeobject.get_osd_lvm_resource_name(
                    osd_host=osd_host, osd_id=osd_id, resource="vg"
                )

                log.info(
                    f"Successfully fetched Host, osd fsid and device path for OSD {osd_id}\n"
                    f"OSD ID: {osd_id}\n"
                    f"Host: {osd_host.hostname}\n"
                    f"OSD fsid: {osd_fsid}\n"
                    f"device path: {dev_path}\n"
                )

                # Execute ceph-volume --help
                log.info(
                    "\n ---------------------------------"
                    "\n Running help for Ceph Volume"
                    "\n ---------------------------------"
                )
                out = volumeobject.help(osd_host)
                log.info(out)

                # ceph-volume lvm zap [ --destroy | device path | --osd-id osd id | --osd-fsid osd fsid ]
                log.info(
                    f"\n -------------------------------------------"
                    f"\n Zapping device on host {osd_host.hostname} with command: "
                    f"ceph-volume lvm zap {' '.join(option)}\n"
                    f"\n -------------------------------------------"
                )

                log.info("Setting OSD service to unmanaged")
                utils.set_osd_devices_unmanaged(
                    ceph_cluster=ceph_cluster, osd_id=osd_id, unmanaged=True
                )

                log.info(
                    "Successfully set OSD service to unmanaged\n"
                    f"proceeding to remove OSD [id:{osd_id}, fsid:{osd_fsid}"
                    f"device path:{dev_path}] on host {osd_host.hostname}"
                )

                utils.osd_remove(ceph_cluster, osd_id=osd_id)

                log.info(
                    f"Removed OSD {osd_id} on host {osd_host}"
                    f"Proceeding to zap volume on host {osd_host}"
                )

                _ = volumeobject.lvm_zap(
                    device=dev_path if "device" in option else None,
                    destroy=True if "--destroy" in option else False,
                    osd_id=osd_id if "osd_id" in option else None,
                    osd_fsid=osd_fsid if "osd_fsid" in option else None,
                    logical_volume=(
                        f"{osd_vg_name}/{osd_lv_name}"
                        if "logical_volume" in option
                        else None
                    ),
                    host=osd_host,
                )

                log.info(
                    f"Zapped device {dev_path} using `ceph-volume lvm zap`\n"
                    f"Proceeding to check OSD {osd_id} removed from `ceph-volume lvm list`"
                )

                if volumeobject.osd_id_or_device_exists_in_lvm_list(
                    osd_host=osd_host,
                    osd_id=osd_id,
                    device_path=dev_path,
                ):
                    log.error(
                        f"zapping OSD device failed [ device_path: {dev_path} OSD ID: {osd_id} OSD FSID: {osd_fsid} ]\n"
                        f"`ceph-volume lvm list` output still contains OSD ID {osd_id} and device path {dev_path}\n"
                    )
                    raise Exception(
                        f"`ceph-volume lvm list` output still contains OSD ID {osd_id} and device path {dev_path}\n"
                    )

                log.info(
                    f"OSD {osd_id} removed from `ceph-volume lvm list` on host {osd_host.hostname}\n"
                )

                if config.get("zap_without_destroy_flag"):

                    log.info(
                        f"Proceeding to check if file system is removed from OSD {osd_id}"
                    )

                    if volumeobject.filesystem_exists_in_logical_volume(
                        osd_host=osd_host,
                        osd_id=osd_id,
                        osd_lv_name=osd_lv_name,
                        osd_vg_name=osd_vg_name,
                    ):
                        raise AssertionError(
                            f"zapping device {dev_path} failed\n"
                            f"`ceph-volume lvm zap` did not wipe filesystem on device {dev_path}\n"
                        )

                    log.info(
                        f"File system successfully wiped from OSD {osd_id} device {dev_path}\n"
                    )

                log.info(
                    f"Proceeding to check if LVs/VGs/PVs for OSD {osd_id} on host {osd_host.hostname}"
                )

                pv_names = get_logical_volume_resources(
                    osd_host=osd_host, resource="pv"
                )
                vg_names = get_logical_volume_resources(
                    osd_host=osd_host, resource="vg"
                )
                lv_names = get_logical_volume_resources(
                    osd_host=osd_host, resource="lv"
                )

                if config.get("zap_with_destroy_flag"):
                    log.info(
                        "Destroy flag (--destroy) passed during test execution\n"
                        f"Proceeding to check if LVs/VGs/PVs are removed for OSD {osd_id}"
                        f" ( OSD device path {dev_path} ) on host {osd_host.hostname}"
                    )

                    if (
                        (dev_path in pv_names)
                        or (osd_lv_name in lv_names)
                        or (osd_vg_name in vg_names)
                    ):
                        log.error(
                            f"LVs/VGs/PVs not cleared for OSD {osd_id} {osd_fsid} {dev_path}\n"
                            f"OSD LV name: {osd_lv_name} and OSD VG name: {osd_vg_name}\n"
                            f"PVs on host {osd_host.hostname} are {pv_names}"
                            f"VGs on host {osd_host.hostname} are {vg_names}"
                            f"LVs on host {osd_host.hostname} are {lv_names}"
                        )
                        raise

                    log.info(
                        f"LVs/VGs/PVs are cleared for OSD {osd_id} on host {osd_host.hostname}\n"
                        f"OSD LV name: {osd_lv_name} and OSD VG name: {osd_vg_name}\n"
                        f"PVs on host {osd_host.hostname} are {pv_names}"
                        f"VGs on host {osd_host.hostname} are {vg_names}"
                        f"LVs on host {osd_host.hostname} are {lv_names}"
                    )

                if config.get("zap_without_destroy_flag"):
                    # If --destroy flag is not passed with ceph-volume lvm zap command
                    # logical volumes, volume groups and physical volumes should not be wiped.
                    log.info(
                        f"Destroy flag (--destroy) not passed during test execution\n"
                        f"Proceeding to check LVs/VGs/PVs are intact for"
                        f" OSD {osd_id} ( OSD device path {dev_path} ) on host {osd_host.hostname}"
                    )
                    if (
                        (dev_path not in pv_names)
                        or (osd_lv_name not in lv_names)
                        or (osd_vg_name not in vg_names)
                    ):
                        log.error(
                            f"LVs/VGs/PVs are wiped without --destory flag for OSD {osd_id} {osd_fsid} {dev_path}\n"
                            f"OSD LV name: {osd_lv_name} and OSD VG name: {osd_vg_name}\n"
                            f"PVs on host {osd_host.hostname} are {pv_names}\n"
                            f"VGs on host {osd_host.hostname} are {vg_names}\n"
                            f"LVs on host {osd_host.hostname} are {lv_names}\n"
                        )
                        raise
                    log.info(
                        f"LVs/VGs/PVs are intact for OSD {osd_id} on host {osd_host.hostname}\n"
                        f"OSD LV name: {osd_lv_name} and OSD VG name: {osd_vg_name}\n"
                        f"PVs on host {osd_host.hostname} are {pv_names}\n"
                        f"VGs on host {osd_host.hostname} are {vg_names}\n"
                        f"LVs on host {osd_host.hostname} are {lv_names}\n"
                    )

                log.info(
                    "\n ---------------------------------"
                    f"\nSuccessfully zapped OSD device {osd_id} with path {dev_path} using command:"
                    f"\nceph-volume lvm zap {' '.join(option)}"
                    "\n ---------------------------------"
                )
                log.info(
                    f" OSD_ID:{osd_id} OSD_FSID:{osd_fsid} on host:{osd_host.hostname}"
                    f"\nAdding back zapped OSD {osd_id}\n"
                    f"Retrieving list of active OSDs"
                )

                cluster_osds = rados_obj.get_osd_list(status="up")

                log.info("Setting OSD service to managed")
                if len(cluster_osds):
                    utils.set_osd_devices_unmanaged(
                        ceph_cluster, cluster_osds[0], unmanaged=False
                    )

                log.info(
                    "Successfully set osd service to managed\n"
                    f"Waiting until removed osd {osd_id} on host {osd_host.hostname} is added back"
                )

                if config.get("zap_without_destroy_flag"):
                    log.info("Zapping device with --destroy flag to wipe LVs/VGs/PVs")
                    _ = volumeobject.lvm_zap(
                        device=dev_path,
                        destroy=True,
                        osd_id=None,
                        osd_fsid=None,
                        host=osd_host,
                    )

                wait_for_osd_daemon_state(osd_host, osd_id, "up")

                log.info(
                    f"Successfully added back osd {osd_id} to cluster\n"
                    f"Successfully removed osd {osd_id}, zapped device {dev_path}"
                    f" associated with osd [osd_id: {osd_id}, osd_fsid: {osd_fsid}]"
                    f" and added back the removed OSD"
                )

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )

        utils.set_osd_devices_unmanaged(ceph_cluster, cluster_osds[0], unmanaged=False)

        # log cluster health
        rados_obj.log_cluster_health()

        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed verification of ceph-volume utility commands.")
    return 0


def get_logical_volume_resources(osd_host, resource) -> list:
    """Function to retrieve logical volume manager resource
    such as logical volumes, volume groups and physical volumes.

    Args:
        host: CephNode object
        resource: lv/vg/pv

    Returns:
        If lv is passed as resource -> returns list of logical volumes on osd_host
        If vg is passed as resource -> returns list of volume groups on osd_host
        If pv is passed as resource -> returns list of physical volumes on osd_host

    Usage:
        list of logical volumes on osd_host : get_logical_volume_resources(osd_host, "lv")
        list of volume groups on osd_host : get_logical_volume_resources(osd_host, "vg")
        list of physical volumes on osd_host : get_logical_volume_resources(osd_host, "pv")
    """
    cmd = f"{resource}s -o {resource}_name --reportformat json"

    out, _ = osd_host.exec_command(sudo=True, cmd=cmd)

    try:
        out = json.loads(out)
    except json.JSONDecodeError as e:
        log.error(f"Error decoding `{cmd}` output {e}")
        raise Exception(
            f"Error Deserialising output of `{cmd}` into python object: {e}"
        )

    _names_list = [
        resource_detail[f"{resource}_name"]
        for resource_detail in out["report"][0][resource]
    ]
    log.info(f"{resource}s on host {osd_host.hostname} are {_names_list}")

    return _names_list
