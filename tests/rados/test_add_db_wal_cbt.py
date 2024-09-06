"""
This file contains verification of adding db and wal to an OSD.This contains-
1. Get the available osds
2. Get available SSD and NVME paths
3. Add the DB and wal to the OSD
4. Check the devices are not added or not at "ceph orch device" and "osd metadata"
"""

import datetime
import math
import random
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    ceph_nodes = kw.get("ceph_nodes")
    rados_object = RadosOrchestrator(node=cephadm)
    bluestore_obj = BluestoreToolWorkflows(node=cephadm)
    client = ceph_cluster.get_nodes(role="client")[0]
    osd_flag_status = 0

    try:
        # Get the OSD list
        for node in ceph_nodes:
            if node.role == "osd":
                host_name = node.hostname
                ssd_path_list = rados_object.get_available_devices(
                    node_name=host_name, device_type="ssd"
                )
                if not ssd_path_list:
                    log.info(f"The device paths are empty to add on the {host_name}")
                    continue
                osds_id_in_node = rados_object.collect_osd_daemon_ids(node)
                log.info(f"The osd's  configured on the HDD are-{osds_id_in_node}")
                test_osd = random.choice(osds_id_in_node)
                log.info(f"Performing the configurations on {test_osd}")
                osd_size = rados_object.get_osd_df_stats(
                    tree=False, filter_by="name", filter=f"osd.{test_osd}"
                )["summary"]["total_kb"]
                log.info(f"The OSD size in KB is - {osd_size}")
                db_size = math.floor(osd_size * 1000 * 0.04)
                log.info(f"The DB size in bytes are -{db_size}")
                nvme_flag = False
                ssd_flag = False

                for dev_path in ssd_path_list:
                    if "nvme" in dev_path and not nvme_flag:
                        bluestore_obj.add_wal_device(test_osd, dev_path)
                        # Check 1: Checking the path at orch device ls output
                        osd_config_result = check_osd_config(
                            rados_object, host_name, dev_path
                        )

                        # Cehck2: Checking device in the OSD metadata
                        osd_metadata_result = check_osd_metadata(
                            ceph_cluster, client, test_osd, dev_path
                        )
                        if not osd_config_result and not osd_metadata_result:
                            log.error(
                                f"The NVME-{dev_path} is not added on osd -{test_osd} in the {host_name} node"
                            )
                            continue
                        log.info(
                            f"The NVME-{dev_path} is added on osd -{test_osd} in the {host_name} node"
                        )
                        nvme_flag = True
                    elif "nvme" not in dev_path and not ssd_flag:
                        bluestore_obj.add_db_device(test_osd, dev_path, db_size)
                        osd_config_result = check_osd_config(
                            rados_object, host_name, dev_path
                        )
                        osd_metadata_result = check_osd_metadata(
                            ceph_cluster, client, test_osd, dev_path
                        )
                        if not osd_config_result and not osd_metadata_result:
                            log.error(
                                f"The SSD -{dev_path} is not added on osd -{test_osd} in the {host_name} node"
                            )
                            continue
                        log.info(
                            f"The SSD -{dev_path} is added on osd -{test_osd} in the {host_name} node"
                        )
                        ssd_flag = True
                osd_flag_status = osd_flag_status + 1

        if osd_flag_status == 0:
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


def check_osd_config(node_object, host_name, dev_path):
    """
    Method  used to check the device path is available or not  in orch device list
    Args:
          node_object: node object for calling the method
          host_name: host name in the cluster where the device need to check
          dev_path: device path
          Example: /dev/sdb
    Returns : True/False
    """
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=600)
    while end_time > datetime.datetime.now():
        ssd_path_list = node_object.get_available_devices(
            node_name=host_name, device_type="ssd"
        )
        if dev_path not in ssd_path_list:
            log.info(f"OSD is configured on the {dev_path} at {host_name}")
            return True
        time.sleep(30)
        log.info("Waiting for 30 seconds to check the OSD configuration")
    log.error(f" The device {dev_path} is not added to the OSD")
    return False


def check_osd_metadata(ceph_cluster, client_object, osd_id, dev_path):
    """
    Method  used to check the device path is available or not in the OSD metadata
    Args:
          ceph_cluster: ceph cluster object
          client_object: client node object for calling the method
          osd_id: osd id
          dev_path: device path
          Example: /dev/sdb

    Returns : True/False

    """
    osd_metadata = ceph_cluster.get_osd_metadata(
        osd_id=int(osd_id), client=client_object
    )
    log.debug(osd_metadata)
    if "nvme" in dev_path:
        actual_device = osd_metadata["bluefs_wal_devices"]
    else:
        actual_device = osd_metadata["bluefs_db_devices"]
    if actual_device not in dev_path:
        return False
    return True
