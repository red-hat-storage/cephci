"""
This test module is used to deploy a stretch cluster on the given set of nodes.

Actions Performed::

    1. Disables the automatic crush map update
    2. Adds the stretch rule into crush map.
    3. Moves OSD's and Mon's to respective crush locations
    4. Enables stretch rule and deploys stretch mode for RHCS cluster
    5. Check the acting set in PG for 4 OSD's. 2 from each site.

Example::

        config:
            stretch_rule_name: "stretch_rule"       # Name of the crush rule with which stretched mode would be deployed
            site1:
              name: "DC1"                           # Name of the datacenter-1 to be added in crush map
              hosts: ["<host1-shortname>", ... ]    # List of hostnames present in datacenter-1
            site2:
              name: "DC2"                           # Name of the datacenter-2 to be added in crush map
              hosts: ["<host3-shortname>", ... ]    # List of hostnames present in datacenter-2
            site3:
              name: "DC3"                           # Name of the Arbiter location to be added in crush map
              hosts: ["<host5-shortname>"]          # List of hostname present in Arbiter
"""

import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonElectionStrategies
from tests.rados.stretch_cluster import setup_crush_rule, wait_for_clean_pg_sets
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    enables connectivity mode and deploys stretch cluster with arbiter mon node
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """

    log.info("Deploying stretch cluster with arbiter mon node")
    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonElectionStrategies(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]

    site1_name = config["site1"]["name"]
    site2_name = config["site2"]["name"]

    # disabling automatic crush update
    cmd = "ceph config set osd osd_crush_update_on_start false"
    cephadm.shell([cmd])

    # Sleeping for 2 seconds after map update.
    time.sleep(2)

    # Setting the election strategy to connectivity mode
    if not mon_obj.set_election_strategy(mode="connectivity"):
        log.error("could not set election strategy to connectivity mode")
        return 1

    # Sleeping for 2 seconds after strategy update.
    time.sleep(2)

    # Checking updated election strategy in mon map
    strategy = mon_obj.get_election_strategy()
    if strategy != 3:
        log.error(
            f"cluster created election strategy other than connectivity, i.e {strategy}"
        )
        return 1
    log.info("Enabled connectivity mode on the cluster")

    # Creating new datacenter crush objects and moving under root/default
    for name in [site1_name, site2_name]:
        cmd = f"ceph osd crush add-bucket {name} datacenter"
        rados_obj.run_ceph_command(cmd)
        time.sleep(2)
        move_crush_item(cephadm, crush_obj=name, name="root", value="default")
        time.sleep(2)

    # Moving all the OSD and Mon daemons into respective sites
    sites = ["site1", "site2", "site3"]
    mon_hosts = mon_obj.get_mon_quorum().keys()
    osd_hosts = mon_obj.get_osd_hosts()
    log.debug(f"Mon hosts defined: {mon_hosts}")
    log.debug(f"OSD hosts defined: {osd_hosts}")

    def search(hosts, pattern):
        for daemon in hosts:
            if re.search(daemon, pattern):
                return daemon

    def mon_set_location(pattern, crush_name):
        daemon = search(mon_hosts, pattern)
        if daemon:
            _cmd = f"ceph mon set_location {daemon} datacenter={crush_name}"
            cephadm.shell([_cmd])
            log.info(
                f"Set location for mon.{daemon} onto site {crush_name}\n"
                "sleeping for 5 seconds"
            )
            time.sleep(5)

    def osd_set_location(pattern, crush_name):
        daemon = search(osd_hosts, pattern)
        if daemon:
            move_crush_item(
                node=cephadm,
                crush_obj=daemon,
                name="datacenter",
                value=crush_name,
            )
            log.info(
                f"Set location for OSD {daemon} onto site {crush_name}\n"
                "sleeping for 5 seconds"
            )
            time.sleep(5)

    for site in sites:
        # Collecting hosts from each site and setting locations accordingly
        site_details = config[site]
        _crush_name = site_details["name"]

        for item in site_details["hosts"]:
            mon_set_location(item, _crush_name)
            osd_set_location(item, _crush_name)

    log.info("Moved all the hosts into respective sites")

    stretch_rule_name = config.get("stretch_rule_name", "stretch_rule")
    if not setup_crush_rule(
        node=client_node,
        rule_name=stretch_rule_name,
        site1=site1_name,
        site2=site2_name,
    ):
        log.error("Failed to Add crush rules in the crush map")
        return 1

    # Sleeping for 5 sec for the strategy to be active
    time.sleep(5)

    # Enabling the stretch cluster mode
    tiebreaker_node = search(mon_hosts, config["site3"]["hosts"][0])
    log.info(f"tiebreaker node provided: {tiebreaker_node}")
    cmd = (
        f"ceph mon enable_stretch_mode {tiebreaker_node} {stretch_rule_name} datacenter"
    )
    try:
        cephadm.shell([cmd])
    except Exception as err:
        log.error(
            f"Error while enabling stretch rule on the datacenter. Command : {cmd}"
        )
        log.error(err)
        return 1
    time.sleep(2)

    # wait for PG's to settle down with new crush rules after deployment of stretch mode
    wait_for_clean_pg_sets(rados_obj)

    # Checking if the pools have been updated with the new crush rules
    acting_set = rados_obj.get_pg_acting_set()
    if len(acting_set) != 4:
        log.error(
            f"There are {len(acting_set)} OSD's in PG. OSDs: {acting_set}. Stretch cluster requires 4"
        )
        return 1
    log.info(f"Acting set : {acting_set} Consists of 4 OSD's per PG")
    log.info("Stretch rule with arbiter monitor node set up successfully")
    return 0


def move_crush_item(node: CephAdmin, crush_obj: str, name: str, value: str) -> None:
    """
    Moves the specified crush object to the given location, provided by name/value
    Args:
        node: node where the commands need to be executed
        crush_obj: Name of the CRUSH object to be moved
        name: New CRUSH object type
        value: New CRUSH object location

    Returns: None
    """
    cmd = f"ceph osd crush move {crush_obj} {name}={value}"
    try:
        node.shell([cmd])
        time.sleep(2)
    except Exception as err:
        log.error(err)
