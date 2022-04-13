import json
import random
import re
import time

from ceph.rados_utils import RadosHelper
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    BZ https://bugzilla.redhat.com/show_bug.cgi?id=1829646 :

    1. Check the mon memory usage to store the osd map in the DB and the OSD map epoch time.
    2. Bring down ODS(s) and check the memory usage.
    3. The OSD map should be trimmed even when the OSD is down.
    4. The DB size should be reduced by removing the old mappings once the new mappings are added.
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster

    Note: the test cannot be run on a cluster that is just created as the DB size increases with
          new mappings for around next hour or so.. Please run the test on cluster who's age is at least 1.5 hours.
          By 1.5 hours, the MonDB will be updated with the mappings.
    """
    log.info("Running bz-1829646")
    log.info(run.__doc__)
    ceph_nodes = kw.get("ceph_nodes")
    mons = []
    osds = []
    osd_list_dict = dict()
    config = kw.get("config")

    # The OSD back filling/Recovery can take up a lot of time...
    # time the method waits for the recovery to complete
    time_limit_for_recovery = 60 * 60 * 4
    # time the method waits for the trimming on monDB to be completed once the recovery is done
    mon_db_trim_time = 60 * 15
    # time interval at which the status of the cluster will be checked regularly for recovery completion
    recovery_wait_time = 60 * 3
    # number of OSD's to be brought down during test execution
    osd_down_no = config.get("osd_count", 1)
    # selection for node or daemon to be brought down
    osd_node_bring_down = config.get("non_scale_setup", True)

    for node in ceph_nodes:
        if node.role == "mon":
            mons.append(node)
        if node.role == "osd":
            osds.append(node)

    controller = mons[0]
    log.info(f"choosing mon {controller.hostname} as Control monitor")
    helper = RadosHelper(controller, config, log)

    # collecting the osd daemons present on the OSD node
    for node in osds:
        osd_list_dict[node] = helper.collect_osd_daemon_ids(
            mon_node=controller, osd_node=node
        )

    # collecting the initial size of the Mon DB and the OSD map epoch times
    mon_db_initial_size = get_mon_db_size(mon_node=controller)
    osd_map_initial_epoch_times = get_status_from_ceph_report(
        mon_node=controller, operation="osdmap"
    )
    log.info(f"Size of the MonDB before bringing down OSD's is {mon_db_initial_size}")
    log.info(f"the first and last commits to DB : {osd_map_initial_epoch_times}")

    # stopping the OSD daemon in one of the OSD nodes.
    # Randomly selecting a OSD node and a OSD daemon from that node
    osd_down_dictionary = {}
    random_osd_nodes = random.sample(osds, osd_down_no)
    if osd_node_bring_down:
        for node in random_osd_nodes:
            log.info(f"Randomly selected node : {node.hostname} ")
            change_osd_daemon_status(osd_node=node, task="stop")
    else:
        for node in random_osd_nodes:
            random_osd_daemon = random.choice(osd_list_dict[node])
            log.info(
                f"Randomly selected node : {node.hostname} "
                f"from which OSD ID :{random_osd_daemon} will be stopped"
            )
            change_osd_daemon_status(
                osd_node=node, osd_number=random_osd_daemon, task="stop"
            )
            osd_down_dictionary[node] = random_osd_daemon

    print(
        "sleeping for 2 minutes so that OSD down is recorded and recovery process is started"
    )
    time.sleep(120)

    recovery_start_time = time.time()
    mon_db_size_list = []
    while time_limit_for_recovery:
        # collecting the health status to check the status about the recovery process
        ceph_health_status = get_status_from_ceph_report(
            mon_node=controller, operation="health"
        )
        recovery_tuple = ("OSD_DOWN", "PG_AVAILABILITY", "PG_DEGRADED")
        mon_db_size_list.append(get_mon_db_size(mon_node=controller))
        if not any(
            key in ceph_health_status["checks"].keys() for key in recovery_tuple
        ):
            log.info("The recovery and back-filling of the OSD is completed")
            log.info(
                f"Sleeping {mon_db_trim_time / 60} minutes after the recovery for trimming of the MonDB to complete"
            )
            time.sleep(mon_db_trim_time)
            mon_db_size_list.append(get_mon_db_size(mon_node=controller))
            time.sleep(mon_db_trim_time)
            break
        time_limit_for_recovery -= recovery_wait_time
        log.info(
            f"The recovery and back-filling of the OSD is not completed / In-progress \n"
            f"Time elapsed since recovery start : {(time.time() - recovery_start_time) / 60} Minutes\n"
            f"checking the status of cluster recovery again in {recovery_wait_time / 60} minutes\n"
            f"Time remaining for process completion : {time_limit_for_recovery / 60} minutes"
        )
        time.sleep(recovery_wait_time)

    # collecting the final size of the Mon DB and the OSD map epoch times
    mon_db_final_size = get_mon_db_size(mon_node=controller)
    log.info(
        f"the size of the cluster DB after the OSD recovery : {mon_db_final_size} "
    )
    osd_map_final_epoch_times = get_status_from_ceph_report(
        mon_node=controller, operation="osdmap"
    )
    max_mon_db_size_reached = max(mon_db_size_list)
    log.info(
        f"the Maximum size of the cluster DB during the OSD recovery : {max_mon_db_size_reached} "
    )
    log.debug(
        f"the first and last commits to DB after recovery: {osd_map_initial_epoch_times}"
    )

    # starting the stopped OSD
    if osd_node_bring_down:
        for node in random_osd_nodes:
            log.info(f"starting the OSD node {node.hostname}")
            change_osd_daemon_status(osd_node=node, task="start")
            time.sleep(5)
    else:
        for node in osd_down_dictionary.keys():
            log.info(
                f"starting the OSD ID : {osd_down_dictionary[node]} on node {node.hostname}"
            )
            change_osd_daemon_status(
                osd_node=node, osd_number=osd_down_dictionary[node], task="start"
            )
            time.sleep(5)

    flag_db_size = 0
    # checking the monDB size and the OSD map trimmings
    max_size_increase = abs(max_mon_db_size_reached - mon_db_initial_size)
    final_size_change = abs(mon_db_final_size - mon_db_initial_size)
    if max_size_increase > final_size_change:
        log.info(
            f"The monDB map was trimmed by : {abs(max_size_increase - final_size_change)}"
        )
    else:
        log.error(
            f"The monDB was not trimmed. The size is equal or more :{abs(max_size_increase - final_size_change)}"
        )
        flag_db_size = 1

    # checking the OSD map, if the old mappings were updated.
    initial_epoch_time_difference = (
        osd_map_initial_epoch_times["osdmap_last_committed"]
        - osd_map_initial_epoch_times["osdmap_first_committed"]
    )
    log.debug(
        f"The initial difference in the osd maps is : {initial_epoch_time_difference}"
    )
    final_epoch_time_difference = (
        osd_map_final_epoch_times["osdmap_last_committed"]
        - osd_map_final_epoch_times["osdmap_first_committed"]
    )
    log.debug(
        f"The Final difference in the osd maps is : {final_epoch_time_difference}"
    )

    flag_osd_map = 1 if final_epoch_time_difference > 800 else 0

    if flag_osd_map == 0 and flag_db_size == 0:
        return 0
    return 1


def get_mon_db_size(mon_node):
    """
    Executes du -sch to get the size of the mon DB present at /var/lib/ceph/mon/ceph-$(hostname -s)/store.db
    :param mon_node: name of the monitor node (ceph.ceph.CephNode): ceph node
    :return: the DB size in int
    """

    cmd = f"sudo du -ch /var/lib/ceph/mon/ceph-{mon_node.hostname}"
    log.info(
        f"Collecting the size of the DB on node: {mon_node.hostname} by executing the command : {cmd}"
    )
    output, err = mon_node.exec_command(cmd=cmd)
    regex = r"\s*([\d]*)[M|G]\s+[\w\W]*store.db"
    match = re.search(regex, output)
    size = match.groups()[0] if match else exit("could not collect the size of DB")
    log.debug(f"the size of the cluster DB is {int(size)}")
    return int(size)


def get_status_from_ceph_report(mon_node, operation=None):
    """
    Executes command ceph report to fetch the status of the ceph cluster and collects
    :param mon_node: name of the monitor node (ceph.ceph.CephNode): ceph node
    :param operation: the type of info to be collected from the report.
            operations can be : "osdmap" -> collects "osdmap_first_committed" and "osdmap_last_committed": data
                                "health" -> collects the health status
                                If no operation is sent, returns the entire report of the cluster
    :return: dictionary with the requested values
    """

    cmd = r"sudo ceph report -f=json-pretty"
    log.info(
        f"Collecting the status of the cluster from node: {mon_node.hostname} by executing the command : {cmd}"
    )
    output, err = mon_node.exec_command(cmd=cmd)
    status_json = json.loads(output)
    if operation.lower() == "osdmap":
        status_dict = {
            "osdmap_first_committed": status_json["osdmap_first_committed"],
            "osdmap_last_committed": status_json["osdmap_last_committed"],
        }
        log.debug(f"the OSD maps collected are : {str(status_dict)} ")
    elif operation.lower() == "health":
        status_dict = status_json["health"]
        log.debug(f"the health status of the cluster is : {str(status_dict)} ")
    else:
        status_dict = status_json
        log.debug(f"Cluster report : {str(status_dict)} ")
    return status_dict


def change_osd_daemon_status(osd_node, task, osd_number=None):
    """
    The method is used start/stop the given OSD daemon on the given node
    :param osd_node: name of the OSD node (ceph.ceph.CephNode): ceph node
    :param osd_number: ID of the ceph daemon to be stopped/started. If no ID is provided, all the oSD's on the node
                        are stopped.
    :param task: operation to be performed. either 'start' or 'stop'
    :return: None
    """

    log.info(f"inside method to {task} the OSD(s) on the node : {osd_node.hostname}")
    if osd_number:
        cmd = f"sudo systemctl {task} ceph-osd@{osd_number}"
        log.debug(
            f"{task}ing OSD daemon ID {osd_number} on node {osd_node.hostname} by executing the command : {cmd}"
        )
    else:
        cmd = f"sudo systemctl {task} ceph-osd.target"
        log.debug(
            f"{task}ing all OSD daemons on node {osd_node.hostname} by executing the command : {cmd}"
        )
    osd_node.exec_command(cmd=cmd)
    return None
