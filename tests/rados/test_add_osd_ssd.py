"""
This file contains verification of the configuration of an OSD.This contain-
1. Get the available ssd paths
2. Configure the OSD if it is available
3. Check that the OSD is configured
"""

import datetime
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.utils import get_node_by_id
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    osd_falg_status = 0

    try:
        for device_list in config["add"]:
            new_node_name = device_list["config"]["node_name"]
            new_host = get_node_by_id(ceph_cluster, new_node_name)
            host_name = new_host.hostname
            path_list = get_available_ssd(rados_object, host_name)
            if not path_list:
                log.info(f"The device paths are empty to add on the {host_name}")
                continue
            for dev_path in path_list:
                out, err = cephadm.shell(
                    [f"ceph orch daemon add osd {host_name}:{dev_path}"]
                )
                log.info(out)
                # Check that OSD is configured or not
                end_time = datetime.datetime.now() + datetime.timedelta(seconds=600)
                while end_time > datetime.datetime.now():
                    volume_status = is_ssd_available(rados_object, host_name, dev_path)
                    if volume_status is False:
                        log.info(f"OSD is configured on the {dev_path} at {host_name}")
                        osd_falg_status = osd_falg_status + 1
                        break
                    time.sleep(30)
                    log.info("Waiting for 30 seconds to check the OSD configuration")

            if osd_falg_status == 0:
                log.error("Any of the devices are not configured as OSDs")
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


def is_ssd_available(rados_object, node, path):
    """
    Method returns the True or False based of device path availability.
        Args:
            rados_object: RadosOrchestrator class object
            node: node name
            path : device path
                   Example: /dev/sdb
        Returns: Return the True or False
    """
    available_device_list = rados_object.get_orch_device_list(node)
    for path_list in available_device_list[0]["devices"]:
        if path_list["path"] == path:
            if path_list["available"] is False:
                log.info(f"The path {path} at {node} is not available to configure OSD")
                return False
            return True
    log.error(f"The path {path} is not available in the node-{node}")
    return False


def get_available_ssd(rados_object, node):
    """
    Method returns the available SSD list in the provided node.
        Args:
             rados_object: RadosOrchestrator class object
             node: node name

         Returns: Returns the available device path list of the node.

    """
    ssd_paths = []
    available_device_list = rados_object.get_orch_device_list(node)
    for path_list in available_device_list[0]["devices"]:
        if path_list["human_readable_type"] == "hdd":
            continue
        if path_list["human_readable_type"] == "ssd" and path_list["available"] is True:
            ssd_paths.append(path_list["path"])
    return ssd_paths
