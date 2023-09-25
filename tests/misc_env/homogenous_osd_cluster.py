"""
This module contains workflow for deploying OSD homogenously on specific devices
Sample test script
    - test:
      abort-on-fail: true
      desc: deploy osd on specific devices
      module: homogenous_osd_cluster.py
      name: OSD homogenous deploy on specific devices
"""
import json
import traceback
from typing import List

from ceph.ceph import Ceph, CephNode, CommandFailed
from ceph.parallel import parallel
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def generate_osd_list(ceph_cluster: Ceph):
    """
    Generates OSD list from conf file
    Args:
        ceph_cluster (class): Ceph cluster info from conf file
    Returns:
        None
    """
    installer = ceph_cluster.get_ceph_object("installer")
    ceph_osds = ceph_cluster.get_ceph_objects("osd")
    osd_nodes = set()
    for osd in ceph_osds:
        osd_nodes.add(osd.node.vmshortname)
    osd_node_list = list(osd_nodes)
    log.info(osd_node_list)
    out, rc = installer.exec_command(
        cmd=f"sudo cephadm shell -- ceph orch device ls --hostname= {' '.join(osd_node_list)} --format json"
    )
    grouped_data, desired_size, min_disk_count = get_osd_size_count(json.loads(out))
    with parallel() as p:
        for node, value in grouped_data.items():
            log.info(f"node:{node}")
            log.info(f"disk list : {value[desired_size][:min_disk_count]}")
            p.spawn(deploy_osd, installer, node, value[desired_size][:min_disk_count])


def get_osd_size_count(data):
    grouped_data = {}
    for entry in data:
        hostname = entry["addr"]
        devices = entry["devices"]
        if hostname not in grouped_data:
            grouped_data[hostname] = {}
        for device in devices:
            size = device["sys_api"]["human_readable_size"]
            path = device["path"]
            if size not in grouped_data[hostname]:
                grouped_data[hostname][size] = []
            if device["available"]:
                grouped_data[hostname][size].append(path)
    a = grouped_data
    size_counts = {}
    size_suffix = {}
    # Loop through the data to count devices for each size
    for host, sizes_and_devices in a.items():
        for size_str, devices in sizes_and_devices.items():
            size_value = float(size_str.split()[0])
            size_counts[size_value] = size_counts.get(size_value, 0) + len(devices)
            size_suffix[size_value] = size_str.split()[1]
    # Find the size with the maximum device count
    max_size_value = max(size_counts, key=size_counts.get)
    # Convert the maximum available size to a string with the appropriate unit
    desired_size = f"{max_size_value} {size_suffix[max_size_value]}"
    # Get the maximum device count for the desired size
    max_device_count = size_counts[max_size_value]
    # Initialize a variable to keep track of the minimum node count
    min_disk_count = float("inf")
    # Loop through the data to find the minimum node count for the desired size
    for host, sizes_and_devices in a.items():
        if desired_size in sizes_and_devices:
            size_device_count = len(sizes_and_devices[desired_size])
            min_disk_count = min(min_disk_count, size_device_count)
    # Print the desired size, the maximum number of devices, and the maximum number of nodes for that size
    log.info(
        f"The desired size based on max available devices across all nodes is: {desired_size}"
    )
    log.info(f"The maximum number of devices for {desired_size} is: {max_device_count}")
    if min_disk_count == float("inf"):
        log.info("No nodes have the desired size.")
    else:
        log.info(
            f"Maximum disks of size {desired_size} that can be applied across all nodes: {min_disk_count}"
        )
    return grouped_data, desired_size, min_disk_count


def deploy_osd(node: CephNode, osn, disk_list: List) -> None:
    """
    Deploy OSD on specific device
    Args:
        node:   Installer node
        osn:    OSD node name
        osd_node : node_ip of node where osd is to be deployed
        disk_list:   List of devices to deploy OSD
    Returns:
        None
    Raises:
        CommandFailed
    """
    disk_list_updated = []
    for i in range(len(disk_list)):
        try:
            print(f"cephadm shell -- ceph orch daemon add osd {osn}:{disk_list[i]}")
            node.exec_command(
                sudo=True,
                cmd=f"cephadm shell -- ceph orch daemon add osd {osn}:{disk_list[i]}",
            )
            disk_list_updated.append(disk_list[i])
        except CommandFailed:
            log.info(f"Could not deploy OSD on {disk_list[i]}")
    expected_osd_count = len(disk_list_updated)
    validate_osd(node, osn, expected_osd_count)


@retry(AssertionError, tries=15, delay=10)
def validate_osd(node: CephNode, osn, expected_osds):
    out, _ = node.exec_command(
        sudo=True,
        cmd=f"cephadm shell -- ceph orch ps {osn} --daemon_type=osd --format json-pretty",
    )
    out = json.loads(out)
    log.info(f"{out}")
    count = len(out)
    log.info(f"osd count :{count},expected_osds:{expected_osds}")
    if count < expected_osds:
        log.info(f"{osn} {count}/{expected_osds} osd daemon(s) up...")
        raise AssertionError(f"{osn} {count}/{expected_osds} osd daemon(s) up...")


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Entry point to this module that configures OSDs.
    """
    log.info("Configuring OSDs on given device")
    try:
        generate_osd_list(ceph_cluster)
    except BaseException as be:  # noqa
        log.error(be)
        log.error(traceback.format_exc())
        return 1
    log.info("Successfully deployed OSDs on specific devices.")
    return 0
