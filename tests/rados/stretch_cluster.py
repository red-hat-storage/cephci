import datetime
import logging
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.parallel import parallel
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonElectionStrategies
from tests.rados.mute_alerts import get_alerts
from tests.rados.test_9281 import do_rados_get, do_rados_put

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    enables connectivity mode and deploys stretch cluster with arbiter mon node
    Actions Performed:
    1. Disables the automatic crush map update
    2. Collects the OSD daemons in the cluster and split them into 2 sites.
    3. If add capacity is selected, only half of the OSD's will be added to various sites initially.
    4. Adds the stretch rule into crush map.
    5. Adding monitors into the 2 sites.
    6. Create a replicated pool and deploy stretch mode.
    7. Create a test pool, write some data and perform add capacity. ( add osd nodes into two sites )
    8. Check for the bump in election epochs throughout.
    9. Check the acting set in PG for 4 OSD's. 2 from each site.
    Verifies bugs:
    [1]. https://bugzilla.redhat.com/show_bug.cgi?id=1937088
    [2]. https://bugzilla.redhat.com/show_bug.cgi?id=1952763
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
    tiebreaker_node = ceph_cluster.get_nodes(role="installer")[0]

    if not client_node and not tiebreaker_node:
        log.error(
            "Admin client and tie breaker node not configured, Cannot modify crush rules for stretch cluster"
        )
        return 1
    mon_state = get_mon_details(node=cephadm)
    if len(list(mon_state["monitors"])) < 5:
        log.error(
            f"Minimum of 5 Mon daemons needed to deploy a stretch cluster, found : {len(mon_state['monitors'])}"
        )
        return 1
    osd_details = get_osd_details(node=cephadm)
    if len(osd_details.keys()) < 4:
        log.error(
            f"Minimum of 4 osd daemons needed to deploy a stretch cluster, found : {len(osd_details.keys())}"
        )
        return 1

    if config.get("verify_forced_recovery"):
        log.info("Verifying forced recovery and healthy in stretch environment")

        pool_name = "stretch_pool_recovery"
        if not rados_obj.create_pool(pool_name=pool_name, pg_num=16):
            log.error("Failed to create the replicated Pool")
            return 1

        # getting the acting set for the created pool
        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)

        log.info(
            f"Killing 2 OSD's from acting set : {acting_pg_set} to verify recovery"
        )
        stop_osds = [acting_pg_set.pop() for _ in range(2)]
        for osd_id in stop_osds:
            if not rados_obj.change_osd_state(action="stop", target=osd_id):
                log.error(f"Unable to stop the OSD : {osd_id}")
                return 1

        # Sleeping for 25 seconds ( "osd_heartbeat_grace": "20" ) for osd's to be marked down
        time.sleep(25)

        log.info("Stopped 2 OSD's from acting set, starting to wait for recovery")
        rados_obj.change_recover_threads(config=config, action="set")

        if not rados_obj.bench_write(pool_name=pool_name, **config):
            log.error("Failed to write objects into the Pool")
            return 1

        log.debug("Triggering forced recovery in stretch mode")
        cmd = "ceph osd force_recovery_stretch_mode --yes-i-really-mean-it"
        rados_obj.run_ceph_command(cmd)
        log.info("Triggered the recovery in stretch mode")

        log.debug("Starting the stopped OSD's")
        for osd_id in stop_osds:
            if not rados_obj.change_osd_state(action="restart", target=osd_id):
                log.error(f"Unable to restart the OSD : {osd_id}")
                return 1

        # there was data written into pool when the OSD's were down.
        # Verifying if data is recovered and placed into the OSD's after bringing them back
        res = wait_for_clean_pg_sets(rados_obj)
        if not res:
            log.error("PG's in cluster are not active + Clean ")
            return 1

        log.debug("Forcing the stretch cluster into healthy mode")
        cmd = "ceph osd force_healthy_stretch_mode --yes-i-really-mean-it"
        rados_obj.run_ceph_command(cmd)

        log.info("Cluster has successfully recovered and is in healthy state")
        return 0

    # Finding and Deleting any stray EC pools that might have been left on cluster
    pool_dump = rados_obj.run_ceph_command(cmd="ceph osd dump")
    for entry in pool_dump["pools"]:
        if entry["type"] != 1 and entry["crush_rule"] != 0:
            log.info(
                f"A non-replicated pool found : {entry['pool_name']}, proceeding to delete pool"
            )
            if not rados_obj.detete_pool(pool=entry["pool_name"]):
                log.error(f"the pool {entry['pool_name']} could not be deleted")
                return 1
        log.debug("No pools other than replicated found on cluster")

    # disabling automatic crush update
    cmd = "ceph config set osd osd_crush_update_on_start false"
    cephadm.shell([cmd])

    site1 = config.get("site1", "site1")
    site2 = config.get("site2", "site2")

    # Collecting osd details and split them into Sita A and Site B
    sorted_osds = sort_osd_sites(all_osd_details=osd_details)
    site_a_osds = sorted_osds[0]
    site_b_osds = sorted_osds[1]
    if config.get("perform_add_capacity"):
        site_a_osds = sorted_osds[0][: (len(sorted_osds[0]) // 2)]
        site_b_osds = sorted_osds[1][: (len(sorted_osds[1]) // 2)]

    if not set_osd_sites(
        node=cephadm,
        osds=site_a_osds,
        site=site1,
        all_osd_details=osd_details,
    ):
        log.error("Failed to move the OSD's into sites")
        return 1

    if not set_osd_sites(
        node=cephadm,
        osds=site_b_osds,
        site=site2,
        all_osd_details=osd_details,
    ):
        log.error("Failed to move the OSD's into sites")
        return 1

    # collecting mon map to be compared after strtech cluster deployment
    stretch_rule_name = "stretch_rule"
    if not setup_crush_rule(
        node=client_node, rule_name=stretch_rule_name, site1=site1, site2=site2
    ):
        log.error("Failed to Add crush rules in the crush map")
        return 1

    # Setting the election strategy to connectivity mode
    if not mon_obj.set_election_strategy(mode="connectivity"):
        log.error("could not set election strategy to connectivity mode")
        return 1

    # Sleeping for 5 sec for the strategy to be active
    time.sleep(5)
    init_mon_state = get_mon_details(node=cephadm)

    # Checking if mon elections happened after changing election strategy
    if mon_state["epoch"] > init_mon_state["epoch"]:
        log.error("Election epoch not bumped up after setting the connectivity mode.")
        return 1

    # Checking updated election strategy in mon map
    strategy = mon_obj.get_election_strategy()
    if strategy != 3:
        log.error(
            f"cluster created election strategy other than connectivity, i.e {strategy}"
        )
        return 1
    log.info("Enabled connectivity mode on the cluster")

    log.info(f"selecting mon : {tiebreaker_node} as tie breaker monitor on site 3")
    if not set_mon_sites(
        node=cephadm, tiebreaker_node=tiebreaker_node, site1=site1, site2=site2
    ):
        log.error("Failed to ad monitors into respective sites")
        return 1

    # All the existing pools should be automatically changed with stretch rule. Creating a test pool
    pool_name = "test_pool_1"
    if not rados_obj.create_pool(pool_name=pool_name, pg_num=16):
        log.error("Failed to create the replicated Pool")
        return 1

    log.info("Monitors added to respective sites. enabling stretch rule")
    cmd = f"/bin/ceph mon enable_stretch_mode {tiebreaker_node.hostname} {stretch_rule_name} datacenter"
    try:
        cephadm.shell([cmd])
    except Exception as err:
        log.error(
            f"Error while enabling stretch rule on the datacenter. Command : {cmd}"
        )
        log.error(err)
        return 1

    if get_mon_details(node=cephadm)["epoch"] < init_mon_state["epoch"]:
        log.error("Election epoch not bumped up after Enabling strech mode")
        return 1

    # Increasing backfill/rebalance threads so that cluster will re-balance it faster
    rados_obj.change_recover_threads(config=config, action="set")

    # wait for active + clean after deployment of stretch mode
    # checking the state after deployment coz of BZ : https://bugzilla.redhat.com/show_bug.cgi?id=2025800
    res = wait_for_clean_pg_sets(rados_obj)
    if not res:
        status_report = rados_obj.run_ceph_command(cmd="ceph report")
        # Proceeding to check if all PG's are in active + clean
        for entry in status_report["num_pg_by_state"]:
            rec = ("remapped", "peering")
            if any(key in rec for key in entry["state"].split("+")):
                log.error(
                    "PG's in cluster are stuck in remapped+peering after stretch deployment."
                )
                return 1

    if config.get("perform_add_capacity"):
        pool_name = "test_stretch_pool"
        if not rados_obj.create_pool(
            pool_name=pool_name,
            crush_rule=stretch_rule_name,
        ):
            log.error("Failed to create the replicated Pool")
            return 1
        do_rados_put(mon=client_node, pool=pool_name, nobj=100)

        log.info("Performing add Capacity after the deployment of stretch cluster")
        site_a_osds = [osd for osd in sorted_osds[0] if osd not in site_a_osds]
        site_b_osds = [osd for osd in sorted_osds[1] if osd not in site_b_osds]

        if not set_osd_sites(
            node=cephadm,
            osds=site_a_osds,
            site=site1,
            all_osd_details=osd_details,
        ):
            log.error("Failed to move the OSD's into sites")
            return 1
        if not set_osd_sites(
            node=cephadm,
            osds=site_b_osds,
            site=site2,
            all_osd_details=osd_details,
        ):
            log.error("Failed to move the OSD's into sites")
            return 1

        flag = wait_for_clean_pg_sets(rados_obj)
        if not flag:
            log.error(
                "The cluster did not reach active + Clean state after add capacity"
            )
            return 1

        with parallel() as p:
            p.spawn(do_rados_get, client_node, pool_name, 10)
            for res in p:
                log.info(res)
        log.info("Successfully completed Add Capacity scenario")

    rados_obj.change_recover_threads(config=config, action="rm")

    # Checking if the pools have been updated with the new crush rules
    acting_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
    if len(acting_set) != 4:
        log.error(
            f"There are {len(acting_set)} OSD's in PG. OSDs: {acting_set}. Stretch cluster requires 4"
        )
        return 1
    log.info(f"Acting set : {acting_set} Consists of 4 OSD's per PG")
    log.info("Stretch rule with arbiter monitor node set up successfully")
    return 0


def wait_for_clean_pg_sets(rados_obj: RadosOrchestrator) -> bool:
    """
    Waiting for up to 2.5 hours for the PG's to enter active + Clean state after stretch changes
    Automation for bug : [1] & [2]
    Args:
        rados_obj: RadosOrchestrator object to run commands

    Returns:  True -> pass, False -> fail

    """
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=9000)
    while end_time > datetime.datetime.now():
        flag = True
        status_report = rados_obj.run_ceph_command(cmd="ceph report")

        # Proceeding to check if all PG's are in active + clean
        for entry in status_report["num_pg_by_state"]:
            rec = (
                "remapped",
                "backfilling",
                "degraded",
                "incomplete",
                "peering",
                "recovering",
                "recovery_wait",
                "undersized",
                "backfilling_wait",
            )
            if any(key in rec for key in entry["state"].split("+")):
                flag = False

        if flag:
            log.info("The recovery and back-filling of the OSD is completed")
            return True
        log.info(
            f"Waiting for active + clean. Active aletrs: {status_report['health']['checks'].keys()},"
            f"PG States : {status_report['num_pg_by_state']}"
            f" checking status again in 2 minutes"
        )
        time.sleep(120)

    log.error("The cluster did not reach active + Clean state")
    return False


def setup_crush_rule(node, rule_name: str, site1: str, site2: str) -> bool:
    """
    Adds the crush rule required for stretch cluster into crush map
    Args:
        node: ceph client node where the commands need to be executed
        rule_name: Name of the crush rule to add
        site1: Name the 1st site
        site2: Name of the 2nd site
    Returns: True -> pass, False -> fail
    """
    rule = rule_name
    rules = f"""id 111
type replicated
min_size 1
max_size 10
step take {site1}
step chooseleaf firstn 2 type host
step emit
step take {site2}
step chooseleaf firstn 2 type host
step emit"""
    if not add_crush_rules(node=node, rule_name=rule, rules=rules):
        log.error("Failed to add the new crush rule")
        return False
    return True


def add_crush_rules(node, rule_name: str, rules: str) -> bool:
    """
    Adds the given crush rules into the crush map
    Args:
        node: ceph client node where the commands need to be executed
        rule_name: Name of the crush rule to add
        rules: The rules for crush
    Returns: True -> pass, False -> fail
    """
    try:
        # Getting the crush map
        cmd = "/bin/ceph osd getcrushmap > /tmp/crush.map.bin"
        node.exec_command(cmd=cmd)

        # changing it to text for editing
        cmd = "/bin/crushtool -d /tmp/crush.map.bin -o /tmp/crush.map.txt"
        node.exec_command(cmd=cmd)

        # Adding the crush rules into the file
        cmd = f"""cat <<EOF >> /tmp/crush.map.txt
rule {rule_name} {"{"}
{rules}
{"}"}
EOF"""
        node.exec_command(cmd=cmd)

        # Changing back the text file into bin
        cmd = "/bin/crushtool -c /tmp/crush.map.txt -o /tmp/crush2.map.bin"
        node.exec_command(cmd=cmd)

        # Setting the new crush map
        cmd = "/bin/ceph osd setcrushmap -i /tmp/crush2.map.bin"
        node.exec_command(cmd=cmd)

        log.info(f"Crush rule : {rule_name} added successfully")
        return True
    except Exception as err:
        log.error("Failed to set the crush rules")
        log.error(err)
        return False


def sort_osd_sites(all_osd_details: dict) -> tuple:
    """
    Sorts the OSD's present such that the weights on two sites remains the same
    Args:
        all_osd_details: dictionary of OSD's containing the details
            eg : {'2': {'weight': 0.01459, 'state': 'up', 'name': 'osd.2'},
                '7': {'weight': 0.01459, 'state': 'up', 'name': 'osd.7'}}
    Returns: Tuple of lists, containing the OSD list for the 2 sites
        eg : ([1, 2, 3, 4, 5], [6, 7, 8, 9, 0])
    """
    site_a_osds = []
    site_b_osds = []
    osd_list = [x for x in all_osd_details.keys()]

    # distributing the OSD's into two sites such that both sites have equal weight
    while len(osd_list) > 1:
        site_a_osd = osd_list.pop()
        if not all_osd_details[site_a_osd]["state"] == "up":
            log.error(f"OSD : {site_a_osd} is not up")
            continue
        flag = 0
        for osd in osd_list:
            if all_osd_details[osd]["state"] == "up":
                if (
                    all_osd_details[site_a_osd]["weight"]
                    == all_osd_details[osd]["weight"]
                ):
                    osd_list.remove(osd)
                    site_a_osds.append(site_a_osd)
                    site_b_osds.append(osd)
                    flag = 1
                    break
            else:
                log.error(f"OSD : {osd} is not up")
                osd_list.remove(osd)
        if not flag:
            log.error(f"no peer OSD for: {site_a_osd} found")
    log.info(
        f"Proposed Site-A OSD's : {site_a_osds}\nProposed Site-B OSD's : {site_b_osds}"
    )
    return site_a_osds, site_b_osds


def set_osd_sites(
    node: CephAdmin, osds: list, site: str, all_osd_details: dict
) -> bool:
    """
    Collects all the details about the OSD's present on the cluster and distrubutes them among the two sites
    Args:
        node: Cephadm node where the commands need to be executed
        osds: list of OSD's to be added to the given site
        site: the name of the site.
        all_osd_details: dictionary of OSD's containing the details
            eg : {'2': {'weight': 0.01459, 'state': 'up', 'name': 'osd.2'},
                '7': {'weight': 0.01459, 'state': 'up', 'name': 'osd.7'}}
    Returns: True -> pass, False -> fail
    """
    # adding the identified OSD's into the respective sites
    sites = set()
    sites.add(site)
    if len(sites) > 2:
        log.error("There can only be 2 Sites with stretch cluster at present")
        return False
    try:
        for osd in osds:
            cmd = f"ceph osd crush move {all_osd_details[osd]['name']} host=host-{site}-{osd} datacenter={site}"
            node.shell([cmd])
            # sleeping for 20 seconds for osd to be moved
            time.sleep(20)
    except Exception:
        log.error("Failed to move the OSD's into Site A and Site B")
        return False

    cmd = "ceph osd tree"
    log.info(node.shell([cmd]))
    return True


def get_osd_details(node: CephAdmin) -> dict:
    """
    collects details such as weight and state of all OSD's on the cluster
    Args:
        node: Cephadm node where the commands need to be executed
    Returns: Dict -> pass, False -> fail
            dict eg : {'2': {'weight': 0.01459, 'state': 'up', 'name': 'osd.2'},
                        '7': {'weight': 0.01459, 'state': 'up', 'name': 'osd.7'}}
    """
    # Collecting all the OSD details
    cmd = "ceph osd tree"
    out, err = node.shell([cmd])
    log.info(out)
    regex = r"(\d{1,})\s+[\w]*\s+([.\d]*)\s+(osd.\d{1,})\s+(\w*)"
    osd_dict = {}
    if re.search(regex, out):
        osds = re.findall(regex, out)
        for osd in osds:
            osd_dict[osd[0]] = {
                "weight": float(osd[1]),
                "state": osd[3],
                "name": osd[2],
            }
    else:
        log.error("No osd's were found on the system")
    return osd_dict


def get_mon_details(node: CephAdmin) -> dict:
    """
    Collects the mon map details like election epoch, election strategy, active mons and fsid
    Args:
        node: Cephadm node where the commands need to be executed
    Returns: Dict -> pass, False -> fail
            dict eg : { 'epoch': '6', 'fsid': '00206990-70fb-11eb-a425-f0d4e2ebeb54', 'election_strategy': '1',
            'monitors': ['mon.dell-r640-016.dsal.lab.eng.tlv2.redhat.com', 'mon.dell-r640-019'] }
    """
    cmd = "ceph mon dump"
    mon_details = {}
    out, err = node.shell([cmd])
    log.info(out)
    regex_details = (
        r"\s*epoch\s+(\d{1,})\s+fsid\s+([\w-]*)[\w\W]*election_strategy:\s+(\d{1})"
    )
    regex_mon = r"\d{1}\:\s+[\[\]\w\:\./,]*\s+mon\.([\w\-_\.]*)"
    details = re.search(regex_details, out).groups()
    mon_details["epoch"] = int(details[0])
    mon_details["fsid"] = details[1]
    mon_details["election_strategy"] = int(details[2])
    mon_details["monitors"] = re.findall(regex_mon, out)
    return mon_details


def set_mon_sites(node: CephAdmin, tiebreaker_node, site1: str, site2: str) -> bool:
    """
    Adds the mon daemons into the two sites with arbiter node at site 3 as a tie breaker
    Args:
        node: Cephadm node where the commands need to be executed
        tiebreaker_node: name of the monitor to be added as tie breaker( site 3 )
        site1: Name the 1st site
        site2: Name of the 2nd site
    Returns: True -> pass, False -> fail
    """
    # Collecting the mon details
    mon_state = get_mon_details(node=node)
    monitors = list(mon_state["monitors"])
    monitors.remove(tiebreaker_node.hostname)
    commands = [
        f"/bin/ceph mon set_location {tiebreaker_node.hostname} datacenter=arbiter",
        f"/bin/ceph mon set_location {monitors[0]} datacenter={site1}",
        f"/bin/ceph mon set_location {monitors[1]} datacenter={site1}",
        f"/bin/ceph mon set_location {monitors[2]} datacenter={site2}",
        f"/bin/ceph mon set_location {monitors[3]} datacenter={site2}",
    ]
    for cmd in commands:
        try:
            node.shell([cmd])
        except Exception as err:
            log.error(err)
            return False
        # Sleeping till mon restarts with new site info and rejoin the mon quorum
        if not wait_for_alert(node=node, alert="MON_DOWN", duration=180):
            log.error("mon down after adding to site after waiting 180 seconds")
            return False
    log.info("Added all the mon nodes into respective sites")
    return True


def wait_for_alert(node: CephAdmin, alert: str, duration: int) -> bool:
    """
    This method checks for a particular alert on the cluster and waits until it's cleared.
    Args:
        node: Cephadm node where the commands need to be executed
        alert: name of the alert to wait until cleared
        duration: time duration for the wait
    Returns: True -> pass ( Alert cleared within given time)
            False -> fail ( Alert was not cleared within given time)
    """
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=duration)
    while end_time > datetime.datetime.now():
        time.sleep(5)
        alerts = get_alerts(node=node)
        log.info(alerts)
        if alert not in alerts["active_alerts"]:
            return True
    print(
        f"The alert {alert} still active on cluster after timeout of {duration} seconds"
    )
    return False
