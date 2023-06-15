"""
This test module is used to test side down scenarios with recovery in the stretch environment

"""

import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.utils import host_restart, host_shutdown
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    performs site down scenarios in stretch mode
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """

    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "test_stretch_io")
    osp_cred = config.get("osp_cred")
    shutdown_site = config.get("shutdown_site", "DC1")
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "Arbiter")

    log.debug(
        "Running checks to see if stretch mode is already deployed on the cluster"
    )
    stretch_details = rados_obj.get_stretch_mode_dump()
    if not stretch_details["stretch_mode_enabled"]:
        log.error(
            f"Stretch mode not enabled on the cluster. Details : {stretch_details}"
        )
        raise Exception("Stretch mode deployment Failed on the provided cluster")

    log.debug(
        "Running sanity check on the cluster before starting the tests to make sure health is ok"
    )
    if not rados_obj.run_pool_sanity_check():
        log.error("Checks failed on the provided cluster. Health not OK. Exiting...")
        raise Exception("Sanity checks failed on the Stretch cluster provided")

    log.info(
        f"Starting tests performing site down of {shutdown_site}. Pre-checks Passed"
    )

    # getting the CRUSH buckets added into the cluster via osd tree
    osd_tree_cmd = "ceph osd tree"
    buckets = rados_obj.run_ceph_command(osd_tree_cmd)
    dc_buckets = [d for d in buckets["nodes"] if d.get("type") == "datacenter"]
    dc_1 = dc_buckets.pop()
    dc_1_name = dc_1["name"]
    dc_2 = dc_buckets.pop()
    dc_2_name = dc_2["name"]
    dc_1_hosts = []
    dc_2_hosts = []

    # Fetching the hosts of the two DCs
    for crush_id in dc_1["children"]:
        for entry in buckets["nodes"]:
            if entry.get("id") == crush_id:
                dc_1_hosts.append(entry.get("name"))
    for crush_id in dc_2["children"]:
        for entry in buckets["nodes"]:
            if entry.get("id") == crush_id:
                dc_2_hosts.append(entry.get("name"))

    # Fetching the Mon daemon placement in each CRUSH locations
    def get_mon_from_dc(site_name) -> list:
        """
        Returns the list of dictionaries that are part of the site_name passed

        Args:
            site_name: Name of the site, whose mons have to be fetched

        Return:
            List of dictionaries that are present in a particular site
            eg:
            [
                {'rank': 1, 'name': 'ceph-pdhiran-snap-0by4ho-node6', 'crush_location': '{datacenter=DC1}'},
                {'rank': 2, 'name': 'ceph-pdhiran-snap-0by4ho-node3', 'crush_location': '{datacenter=DC1}'},
            ]
        """
        mons = rados_obj.run_ceph_command("ceph mon dump")
        site_mons = [
            d
            for d in mons["mons"]
            if d.get("crush_location") == "{datacenter=" + site_name + "}"
        ]
        return site_mons

    # Checking if the site passed to shut down is present in the Cluster CRUSH
    if shutdown_site not in [tiebreaker_mon_site_name, dc_1_name, dc_2_name]:
        log.error(
            f"Passed site : {shutdown_site} not part of crush locations on cluster.\n"
            f"locations present on cluster : {[tiebreaker_mon_site_name, dc_1_name, dc_2_name]}"
        )
        raise Exception("Test execution failed")

    # Getting the mon hosts for both the DCs from the mon dump
    dc_1_mons = set(entry.get("name") for entry in get_mon_from_dc(site_name=dc_1_name))
    dc_2_mons = set(entry.get("name") for entry in get_mon_from_dc(site_name=dc_2_name))

    # Combining each DCs OSD & MON hosts to get list of all the hosts in that site
    dc_1_hosts.extend(dc_1_mons)
    dc_2_hosts.extend(dc_2_mons)

    # Using sets as the OSD & MON hosts could be overlapping
    dc_1_hosts = list(set(dc_1_hosts))
    dc_2_hosts = list(set(dc_2_hosts))
    tiebreaker_hosts = list(
        set(
            entry.get("name")
            for entry in get_mon_from_dc(site_name=tiebreaker_mon_site_name)
        )
    )

    log.info(
        f"Hosts present in Datacenter : {dc_1_name} : {[entry for entry in dc_1_hosts]}"
    )
    log.info(
        f"Hosts present in Datacenter : {dc_2_name} : {[entry for entry in dc_2_hosts]}"
    )
    log.info(
        f"Hosts present in Datacenter : {tiebreaker_mon_site_name} : {[entry for entry in tiebreaker_hosts]}"
    )

    # Creating test pool to check the effect of Site down on the Pool IO
    if not rados_obj.create_pool(pool_name=pool_name):
        log.error(f"Failed to create pool : {pool_name}")
        raise Exception("Test execution failed")

    # Sleeping for 10 seconds for pool to be populated in the cluster
    time.sleep(10)

    # Collecting the init no of objects on the pool, before site down
    pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
    init_objects = pool_stat["stats"]["objects"]

    # Checking which DC to be turned off, It would be either data site or Arbiter site
    # Proceeding to Shut down either one of the Data DCs if crush name sent is either DC1 or DC2.
    if shutdown_site in [dc_1_name, dc_2_name]:
        log.debug(f"Proceeding to shutdown one of the data site {dc_1_name}")
        for host in dc_1_hosts:
            log.debug(f"Proceeding to shutdown host {host}")
            if not host_shutdown(gyaml=osp_cred, name=host):
                log.error(f"Failed to shutdown host : {host}")
                raise Exception("Test execution Failed")

        log.info(f"Completed shutdown of all the hosts in data site {dc_1_name}.")

        # sleeping for 10 seconds for the DC to be identified as down and proceeding to next checks
        time.sleep(10)

        # Checking the health status of the cluster and the active alerts for site down
        # These should be generated on the cluster
        status_report = rados_obj.run_ceph_command(cmd="ceph report", client_exec=True)
        ceph_health_status = list(status_report["health"]["checks"].keys())
        expected_health_warns = (
            "OSD_HOST_DOWN",
            "OSD_DOWN",
            "OSD_DATACENTER_DOWN",
            "MON_DOWN",
            "DEGRADED_STRETCH_MODE",
        )
        if not all(elem in ceph_health_status for elem in expected_health_warns):
            log.error(
                f"We do not have the expected health warnings generated on the cluster.\n"
                f" Warns on cluster : {ceph_health_status}\n"
                f"Expected Warnings : {expected_health_warns}\n"
            )

        log.info(
            f"The expected health warnings are generated on the cluster. Warnings : {ceph_health_status}"
        )

        # Checking is the cluster is marked degraed and operating in degraded mode post data site down
        stretch_details = rados_obj.get_stretch_mode_dump()
        if not stretch_details["degraded_stretch_mode"]:
            log.error(
                f"Stretch Cluster is not marked as degraded even though we have DC down : {stretch_details}"
            )
            raise Exception("Stretch mode degraded test Failed on the provided cluster")

        log.info(
            f"Cluster is marked degraded post DC Failure {stretch_details},"
            f"Proceeding to try writes into cluster"
        )

    else:
        log.info("Shutting down arbiter mon site")
        for host in tiebreaker_hosts:
            log.debug(f"Proceeding to shutdown host {host}")
            if not host_shutdown(gyaml=osp_cred, name=host):
                log.error(f"Failed to shutdown host : {host}")
                raise Exception("Test execution Failed")
        time.sleep(20)

        # Installer node will be down at this point. all operations need to be done at client nodes
        status_report = rados_obj.run_ceph_command(cmd="ceph report", client_exec=True)
        ceph_health_status = list(status_report["health"]["checks"].keys())
        expected_health_warns = ("MON_DOWN",)
        if not all(elem in ceph_health_status for elem in expected_health_warns):
            log.error(
                f"We do not have the expected health warnings generated on the cluster.\n"
                f" Warns on cluster : {ceph_health_status}\n"
                f"Expected Warnings : {expected_health_warns}\n"
            )

        log.info(
            f"The expected health warnings are generated on the cluster. Warnings : {ceph_health_status}"
        )
        log.info(
            f"Completed shutdown of hosts in site {shutdown_site}. Host names :{tiebreaker_hosts}. Proceeding to write"
        )

    # perform rados put to check if write ops is possible
    pool_obj.do_rados_put(client=client_node, pool=pool_name, nobj=200, timeout=50)

    log.debug("sleeping for 20 seconds for the objects to be displayed in ceph df")
    time.sleep(20)

    # Getting the number of objects post write, to check if writes were successful
    pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
    log.debug(pool_stat)

    # Objects should be more than the initial no of objects
    if pool_stat["stats"]["objects"] <= init_objects:
        log.error(
            "Write ops should be possible, number of objects in the pool has not changed"
        )
        raise Exception(f"Pool {pool_name} has {pool_stat['stats']['objects']} objs")
    log.info(
        f"Successfully wrote {pool_stat['stats']['objects']} on pool {pool_name} in degraded mode\n"
        f"Proceeding to bring up the nodes and recover the cluster from degraded mode"
    )

    # Starting to restart the down hosts.
    if shutdown_site in [dc_1_name, dc_2_name]:
        log.debug(f"Proceeding to reboot data site {dc_1_name}")
        for host in dc_1_hosts:
            log.debug(f"Proceeding to reboot host {host}")
            if not host_restart(gyaml=osp_cred, name=host):
                log.error(f"Failed to restart host : {host}")
                raise Exception("Test execution Failed")
        log.info(
            f"Completed restart of all the hosts in site {dc_1_name}. Host names : {dc_1_hosts}"
        )

    else:
        log.info("Restarting arbiter mon site")
        for host in tiebreaker_hosts:
            log.debug(f"Proceeding to Restart host {host}")
            if not host_restart(gyaml=osp_cred, name=host):
                log.error(f"Failed to Restart host : {host}")
                raise Exception("Test execution Failed")
        time.sleep(20)
        log.info(
            f"Completed Restart of all the hosts in site {shutdown_site}. Host names : {tiebreaker_hosts}"
        )

    log.info("Proceeding to do checks post Stretch mode site down scenarios")

    if not post_site_down_checks(rados_obj=rados_obj):
        log.error(f"Checks failed post Site {shutdown_site} Down and Up scenarios")
        raise Exception("Post execution checks failed on the Stretch cluster")

    if not rados_obj.run_pool_sanity_check():
        log.error(f"Checks failed post Site {shutdown_site} Down and Up scenarios")
        raise Exception("Post execution checks failed on the Stretch cluster")

    if config.get("delete_pool"):
        rados_obj.detete_pool(pool=pool_name)

    log.info("All the tests completed on the cluster, Pass!!!")
    return 0


def post_site_down_checks(rados_obj) -> bool:
    """
    method that checks the state of the cluster post site down scenarios.
    1. Checks the health of the cluster untill active + clean
    2. Get the stretch staus and checks if health is ok

    Args:
        rados_obj: Rados object for performing cmd executions
    Returns:
        Pass: True, Fail : False

    """

    # Sleeping for 20 seconds for the hosts to be added back to cluster
    time.sleep(20)

    # Waiting for Cluster to be active + Clean post boot of all hosts
    if not wait_for_clean_pg_sets(rados_obj):
        log.error(
            "Cluster did not reach active + Clean state post restart of failed DC"
        )
        return False

    # Checking health of stretch mode on the cluster. Should not be degraded any longer
    stretch_details = rados_obj.get_stretch_mode_dump()
    if stretch_details["degraded_stretch_mode"]:
        log.error(
            f"Stretch Cluster is marked as degraded even though all sites are up and healthy : {stretch_details}"
        )
        return False
    log.info("Degraded Stretch Cluster mode exited post active + Clean. Pass")
    return True
