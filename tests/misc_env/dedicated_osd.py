"""
This module contains the workflows for deploying OSD uisng spec file

Sample test script

    - test:
      abort-on-fail: true
      clusters:
        ceph-pri:
          config:
            verify_cluster_health: true # Dummy parameter
      desc: configure osds
      module: configure_dedicated_osd.py
      name: configure osds
"""
import json
import time
from typing import List

from jinja2 import Template

from ceph.ceph import Ceph, CephNode
from utility.log import Log

LOG = Log(__name__)

OSD_CONF = """
---
service_type: osd
service_id: defaultDG
service_name: osd.defaultDG
placement:
  hosts:
  - hostname
spec:
  data_devices:
    paths:
{% for item in data %}
    - {{item}}
{%- endfor %}
  db_devices:
    rotational: false
  filter_logic: AND
  objectstore: bluestore

"""


def generate_osd_list(ceph_cluster: Ceph):
    """
    Generates OSD list from conf file

    Args:
        ceph_cluster (class): Ceph cluster info from conf file
    Returns:
        None
    """
    client = ceph_cluster.get_ceph_object("installer")
    ceph_osds = ceph_cluster.get_ceph_objects("osd")
    osd_nodes = set()
    disk_list = set()
    for osd in ceph_osds:
        osd_nodes.add(osd.node.vmshortname)
    osd_node_list = list(osd_nodes)
    LOG.info(osd_node_list)
    for osn in osd_node_list:
        for osd in ceph_osds:
            if osd.node.vmshortname == osn:
                for i in osd.node.vm_node.volumes:
                    disk_list.add(i)
        osd_disk_list = list(disk_list)
        LOG.info(osd_disk_list)
        LOG.info(len(osd_disk_list))
        dump_osd_data(client, osn, osd_disk_list)
        disk_list.clear()
        osd_disk_list.clear()


def dump_osd_data(node: CephNode, osn, disk_list: List) -> None:
    """
    Writes the osd spec file on installer node.

    Args:
        node:   Installer node
        osn:    OSD node name
        disk_list:   List of devices present on OSD node
    Returns:
        None
    Raises:
        CommandFailed
    """
    LOG.info("Generating the OSD.spec file.")
    tmp_osd = OSD_CONF
    tmp_osd = tmp_osd.replace("hostname", osn)
    templ = Template(tmp_osd)
    conf = templ.render(data=disk_list)
    node.exec_command(cmd=f"sudo touch /root/{osn}_spec.yaml")
    node.exec_command(cmd=f"sudo chmod 666 /root/{osn}_spec.yaml")
    conf_file = node.remote_file(
        file_name=f"/root/{osn}_spec.yaml", file_mode="w", sudo=True
    )
    conf_file.write(conf)
    LOG.info(conf)
    conf_file.flush()
    node.exec_command(cmd=f"sudo ceph orch apply -i /root/{osn}_spec.yaml")
    time.sleep(60)
    expected_osd_count = len(disk_list)
    out, _ = node.exec_command(cmd="sudo ceph osd tree --format json-pretty")
    oks = json.loads(out)
    for i in range(len(oks["nodes"])):
        if oks["nodes"][i]["name"] == osn:
            retries = 0
            while len(oks["nodes"][i]["children"]) < expected_osd_count:
                time.sleep(10)
                out, _ = node.exec_command(
                    cmd="sudo ceph osd tree --format json-pretty"
                )
                oks = json.loads(out)
                retries += 1
                if retries == 100:
                    raise AssertionError("Failed to add OSDs")
            break


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Entry point to this module that configures OSDs.
    """
    LOG.info("Configuring OSDs on given device")
    try:
        generate_osd_list(ceph_cluster)
    except BaseException as be:  # noqa
        LOG.error(be)
        return 1

    LOG.info("Successfully Applied OSD spec files!!!")
    return 0
