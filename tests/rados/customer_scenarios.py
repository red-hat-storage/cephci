"""
This test module executes automated customer scenarios.
Currently automated scenarios:
1. BZ-1905339 -> Verification of Mon DB trimming regularly.
    Actions Performed :
        1. Check the mon memory usage Overtime and verify mon-db trimming.
        2. Check the OSD map entries and verify osd-map trimming.
        2. Bring down ODS(s) and check the memory usage.
        3. Trigger PG balancing by re-weighting based on utilization
        4. Deleting pools with objects to increase number of operations on OSD's
        5. Increase the back-fill and re-balance threads to reduce available bandwidth for client IO
        6. Check for the slow_ops reported and check historical ops for completion by reducing OSD compliant time.
        7. Clean-up all the config changes made.
    Note: The test cannot be run on a newly deployed cluster as the DB size increases with
          new mappings for around next hour or so.. The suite `Sanity_rados` is part of the workflow for this test.
          The workflow makes sure that there have been enough operations on monitor for this test to provide results.
"""
import datetime
import json
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Prepares the cluster & runs rados Customer Scenarios.
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
        kw: Args that need to be passed to the test for initialization
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    if config.get("mondb_trim_config"):
        db_config = config.get("mondb_trim_config")
        try:
            verify_mon_db_trim(ceph_cluster=ceph_cluster, node=cephadm, **db_config)
            log.info("Mon DB is getting trimmed regularly")
        except (TestCaseFailureException, TestBedSetupFailure):
            log.error("Failed to verify mon db trimming")
            return 1

    log.info("Completed running the customer Scenario(s)")
    return 0


def verify_mon_db_trim(ceph_cluster, node: CephAdmin, **kwargs):
    """
    The Mon DB size should be reduced by removing the old mappings regularly. To verify this behaviour,
    Creating various scenarios where the DB would be updated with new mappings and verify it DB is getting trimmed.
    Verifies BZ:
    https://bugzilla.redhat.com/show_bug.cgi?id=1905339
    https://bugzilla.redhat.com/show_bug.cgi?id=1829646
    https://bugzilla.redhat.com/show_bug.cgi?id=1972281
    https://bugzilla.redhat.com/show_bug.cgi?id=1943357
    https://bugzilla.redhat.com/show_bug.cgi?id=1766702
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
        node: Cephadm node where the commands need to be executed
        kwargs: Any other KV pairs that need to be passed for testing
    Returns: Exception hit for failure conditions.
    """

    # Creating rados object to run rados commands
    rados_obj = RadosOrchestrator(node=node)
    mon_nodes = ceph_cluster.get_nodes(role="mon")
    osd_nodes = ceph_cluster.get_nodes(role="osd")
    client_node = ceph_cluster.get_nodes(role="client")[0]
    installer_node = ceph_cluster.get_nodes(role="installer")[0]
    daemon_info = rados_obj.run_ceph_command(cmd="ceph orch ps")
    mon_daemons = [entry for entry in daemon_info if entry["daemon_type"] == "mon"]

    # Duration for which we will sleep after the mon DB changes are made and mon would have begun trimming old mappings
    mon_db_trim_wait_dur = 1200

    # List to capture the mon db size throughout the duration of the test to check the variations in DB size
    mon_db_size_list = list()
    mon_db_size_list.append(get_mondb_size(mon_nodes[0], mon_daemons))

    # Collecting first and last commits to osdmap
    status = rados_obj.run_ceph_command(cmd="ceph report")
    init_commmits = {
        "osdmap_first_committed": float(status["osdmap_first_committed"]),
        "osdmap_last_committed": float(status["osdmap_last_committed"]),
    }

    # creating scenarios where the mon db would be updated with new info
    change_config_for_slow_ops(rados_obj=rados_obj, action="set", **kwargs)
    mon_db_size_list.append(get_mondb_size(mon_nodes[0], mon_daemons))

    # Starting read and write on by creating a test pool .
    pool_name = "test_pool_ops"
    if not rados_obj.create_pool(pool_name=pool_name):
        error = "failed to create pool to run IO"
        raise TestCaseFailureException(error)
    cmd = f"rados --no-log-to-stderr -b 1024 -p {pool_name} bench 400 write --no-cleanup &"
    client_node.exec_command(sudo=True, cmd=cmd)

    mon_db_size_list.append(get_mondb_size(mon_nodes[0], mon_daemons))

    # deleting a previously created pool to increase OSD operations and map changes
    # Pool created as part of suite set-up workflow.
    rados_obj.detete_pool(pool="delete_pool")

    # Proceeding to reboot 1 OSD from each host to trigger rebalance & Backfill
    cluster_fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    daemon_info = rados_obj.run_ceph_command(cmd="ceph orch ps")
    osd_daemons = [entry for entry in daemon_info if entry["daemon_type"] == "osd"]
    for onode in osd_nodes:
        for osd in osd_daemons:
            if re.search(osd["hostname"], onode.hostname):
                # Not using the container ID's provided in ceph orch ps command.
                # Bug : https://bugzilla.redhat.com/show_bug.cgi?id=1943494
                # cmd = f"podman restart {osd['container_id']}"
                cmd = f"systemctl restart ceph-{cluster_fsid}@osd.{osd['daemon_id']}.service"
                log.info(
                    f"rebooting osd-{osd['daemon_id']} on host {osd['hostname']}. Command {cmd}"
                )
                onode.exec_command(sudo=True, cmd=cmd)
                # Sleeping for 5 seconds for status to be updated
                time.sleep(5)
                break

    # Re-weighting the OSd's based on usage to trigger rebalance
    # todo: Verify re-balancing process on OSD's ( PG movement across cluster)
    # todo: Add re-balancing based on crush item provided
    # BZ: https://bugzilla.redhat.com/show_bug.cgi?id=1766702
    rados_obj.reweight_crush_items()

    """
    Waiting for 2 hours for cluster to get to active + clean state.
    Rationale: during cluster activities like backfill, rebalance, osd map change cause Mon DB to be updated.
    Hence, we can wait till mon DB updates are completed, after which DB size should be reduced,
    by trimming the old mappings once the new mappings are added.
    If cluster healthy state is reached within 2 hours, we exit the loop earlier, without waiting for stipulated time,
    But if cluster is still performing operations for long time,
     we would need at-least some data to make sure DB is not just increasing.
      ( DB is expected to increase when operations are in progress.
       Old mappings are removed when operations/ new updates are completed. )
    """
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=7200)
    while end_time > datetime.datetime.now():
        status_report = rados_obj.run_ceph_command(cmd="ceph report")
        ceph_health_status = status_report["health"]
        recovery_tuple = ("OSD_DOWN", "PG_AVAILABILITY", "PG_DEGRADED", "SLOW_OPS")
        daemon_info = rados_obj.run_ceph_command(cmd="ceph orch ps")
        mon_daemons = [entry for entry in daemon_info if entry["daemon_type"] == "mon"]
        mon_db_size_list.append(get_mondb_size(mon_nodes[0], mon_daemons))

        # Checking for any health warnings that increase db size
        flag = (
            True
            if not any(
                key in ceph_health_status["checks"].keys() for key in recovery_tuple
            )
            else False
        )

        # Proceeding to check if all PG's are in active + clean
        if flag:
            for entry in status_report["num_pg_by_state"]:
                rec = ("remapped", "backfilling", "degraded")
                if any(key in rec for key in entry["state"].split("+")):
                    flag = False

        if flag:
            log.info(
                f"The recovery and back-filling of the OSD is completed"
                f"Sleeping for {mon_db_trim_wait_dur} Seconds after clean for mon to remove old mappings"
            )
            time.sleep(mon_db_trim_wait_dur / 2)
            mon_db_size_list.append(get_mondb_size(mon_nodes[0], mon_daemons))
            time.sleep(mon_db_trim_wait_dur)
            break
        log.info(
            f"Waiting for active + clean. Active aletrs: {ceph_health_status['checks'].keys()},"
            f" checking status again in 1 minute"
        )
        time.sleep(60)

    # Checking if any slow operations are reported and waiting for 'dur' for the slow_ops to be cleared
    end_time = datetime.datetime.now() + datetime.timedelta(
        seconds=mon_db_trim_wait_dur
    )
    while end_time > datetime.datetime.now():
        if not get_slow_ops_data(node=node, installer=installer_node, action="current"):
            log.info("Operations in progress, checking again in 30 seconds")
            time.sleep(30)
            continue
        # Logging all the historic operations for reference / future enhancement
        get_slow_ops_data(node=node, installer=installer_node, action="historic")
        break

    # collecting the final size of the Mon DB and the OSD map epoch times
    daemon_info = rados_obj.run_ceph_command(cmd="ceph orch ps")
    mon_daemons = [entry for entry in daemon_info if entry["daemon_type"] == "mon"]
    final_db_size = get_mondb_size(mon_nodes[0], mon_daemons)
    final_status = rados_obj.run_ceph_command(cmd="ceph report")
    final_commmits = {
        "osdmap_first_committed": float(final_status["osdmap_first_committed"]),
        "osdmap_last_committed": float(final_status["osdmap_last_committed"]),
    }

    mon_db_max_size = max(mon_db_size_list)
    # Getting the trend of mon DB size when the operations were running on cluster.
    mon_db_size_size_change = list()
    for i in range(len(mon_db_size_list) - 1):
        mon_db_size_size_change.append(mon_db_size_list[i + 1] - mon_db_size_list[i])

    # Reverting the config changes made for generation slow_ops
    change_config_for_slow_ops(rados_obj=rados_obj, action="rm", **kwargs)

    # Checking the final results
    if True not in list(map(lambda x: x <= 0, mon_db_size_size_change)):
        error = f"The mon DB is only increasing since the test begun. DB sizes {mon_db_size_list}"
        raise TestCaseFailureException(error)

    if not final_db_size <= mon_db_max_size:
        error = (
            f"The mon DB size after cluster clean is higher than when operations were being performed.\n"
            f"max size during operations : {mon_db_max_size} , final DB size after clean {final_db_size}"
        )
        log.error(error)
        raise TestCaseFailureException()

    # Initial update of OSD maps can be the same at the beginning and end of test
    if (
        final_commmits["osdmap_first_committed"]
        < init_commmits["osdmap_first_committed"]
    ):
        error = (
            f"The OSD map has not been updated of first commits\n"
            f"The commits are initial commits : {init_commmits}, final commits : {final_commmits}"
        )
        log.error(error)
        raise TestCaseFailureException()

    # Final updates need to be more than initial updates as there are OSD map changes during the duration of test
    if (
        final_commmits["osdmap_last_committed"]
        <= init_commmits["osdmap_last_committed"]
    ):
        error = (
            f"The OSD map has not been updated of last commits\n"
            f"The commits are initial commits : {init_commmits}, final commits : {final_commmits}"
        )
        log.error(error)
        raise TestCaseFailureException()

    # The number of OSD mappings present on cluster should not exceed 800 in total
    # https://tracker.ceph.com/issues/37875#note-1
    if (
        final_commmits["osdmap_last_committed"]
        - final_commmits["osdmap_first_committed"]
    ) > 800:
        error = (
            f"There are still too many old commits in Mon DB. OSD map not trimmed as per needed\n"
            f"The commits are initial commits : {init_commmits}, final commits : {final_commmits}"
        )
        log.error(error)
        raise TestCaseFailureException()

    # Checking the paxos trimming sizes
    if (
        int(final_status["paxos"]["last_committed"])
        - int(final_status["paxos"]["first_committed"])
    ) > 1000:
        error = (
            f"There are still too many old commits in Mon DB.\n"
            f"The commits are initial commits : {final_status['paxos']['first_committed']},"
            f" final commits : {final_status['paxos']['last_committed']}"
        )
        log.error(error)
        raise TestCaseFailureException()

    log.info("mon DB was trimmed successfully")


def get_mondb_size(mon_node, mon_daemons) -> int:
    """
    Executes du -sch to get the size of the mon DB present at /var/lib/ceph/mon/ceph-$(hostname -s)/store.db
    Args:
       mon_node: Node where the mon daemon is running
       mon_daemons: Details of all mon daemons running on the cluster
    Returns: Returns a dictionary whose keys are the daemons and the values are a named tuple, containing the info.
    """
    daemon = [
        mon for mon in mon_daemons if re.search(mon["hostname"], mon_node.hostname)
    ][0]
    cmd = f"podman exec {daemon['container_id']} du -ch /var/lib/ceph/mon/"
    log.info(
        f"Collecting the size of the DB on node: {mon_node.hostname} by executing the command : {cmd}"
    )
    output, err = mon_node.exec_command(sudo=True, cmd=cmd)
    regex = r"\s*([\d]*)[M|G]\s+[\w\W]*store.db"
    match = re.search(regex, output)
    size = match.groups()[0] if match else 0
    log.debug(f"the size of the cluster DB is {int(size)}")
    return int(size)


def change_config_for_slow_ops(rados_obj: RadosOrchestrator, action: str, **kwargs):
    """
    Changes few config Values on ceph cluster to intentionally increase changes of hitting slow_ops on
    the cluster network.
    Actions performed and rationale:
    * paxos_service_trim_min & paxos_service_trim_max set as mentioned in
    bz : https://bugzilla.redhat.com/show_bug.cgi?id=1943357#c0
    * osd_op_complaint_time -> reducing the time threshold by which OSD should respond to requests
    * osd_max_backfills & osd_recovery_max_active -> Incrasing the number of threads for recovery &
    backfill as to reduce n/w bandwidth for client IO operations
    Args:
        rados_obj: Rados object for command execution
        action: weather to set the Config or to remove it from cluster
                Values : "set" -> to set the config values
                         "rm" -> to remove the config changes made
        kwargs: Any other optional args that need to be passed
    Returns: Exception in case of failures
    """
    value_map = {
        "paxos_service_trim_min": kwargs.get("paxos_service_trim_min", 10),
        "paxos_service_trim_max": kwargs.get("paxos_service_trim_max", 100),
        "osd_op_complaint_time": kwargs.get("osd_op_complaint_time", 0.000001),
        "osd_max_backfills": kwargs.get("osd_max_backfills", 8),
        "osd_recovery_max_active": kwargs.get("osd_recovery_max_active", 10),
    }
    cmd_map = {
        "paxos_service_trim_min": f"ceph config {action} mon paxos_service_trim_min",
        "paxos_service_trim_max": f"ceph config {action} mon paxos_service_trim_max",
        "osd_op_complaint_time": f"ceph config {action} osd osd_op_complaint_time",
        "osd_max_backfills": f"ceph config {action} osd osd_max_backfills",
        "osd_recovery_max_active": f"ceph config {action} osd osd_recovery_max_active",
    }

    # Removing the config values set when action is to remove
    if action == "rm":
        for cmd in cmd_map.keys():
            rados_obj.node.shell([cmd_map[cmd]])
        return

    # Adding the config values
    for val in cmd_map.keys():
        cmd = f"{cmd_map[val]} {value_map[val]}"
        rados_obj.node.shell([cmd])

    # Verifying the values set in the config
    config_dump = rados_obj.run_ceph_command(cmd="ceph config dump")
    for val in cmd_map.keys():
        for conf in config_dump:
            if conf["name"] == val:
                if float(conf["value"]) != float(value_map[val]):
                    error = f"Values do not match for config {conf['name']}"
                    raise TestBedSetupFailure(error)


def get_slow_ops_data(node: CephAdmin, installer, action) -> bool:
    """
    Checks the operations running on the cluster
    Args:
        node: node: Cephadm node where the commands need to be executed
        installer: Name of the node where cephadm shell / Mon daemon are collocated
        action: Specifies weather to check for current operations or historic operations
                Values : "current" -> Checks for operations that are on going in cluster.
                         "historic" -> Operations that are completed and marked done by Monitor
    Returns: False if there are any running ops on the cluster, else True
    """

    # checking if any ops are currently present
    if action == "current":
        cmd = f" ceph daemon mon.{installer.hostname} ops -f json"
        out, err = node.shell([cmd])
        status = json.loads(out)
        log.info(status)
        if status["num_ops"] >= 1:
            log.error(
                f"There are operations on going on the cluster. Number : {status['num_ops']}"
            )
            for op in status["ops"]:
                log.error(
                    f"{op['description']} generated : {(op['type_data']['info'])}"
                )
            return False

    # Checking all the ops reports historically
    elif action == "historic":
        cmd = f"ceph daemon mon.{installer.hostname} dump_historic_ops -f json"
        out, err = node.shell([cmd])
        details = json.loads(out)
        size = details["size"]
        if size < 1:
            log.error("No slow operations generated on the cluster")
            return True

        total_dur = details["duration"]
        ops = details["ops"]
        log.info(
            f"No of slow_ops recorded : {size} for a total duration of {total_dur}\n"
            f"Slow ops generated for below items : \n"
        )
        for op in ops:
            log.info(f"{op['description']} generated : {(op['type_data']['info'])}")

    return True


class TestCaseFailureException(Exception):
    """
    Exception is raised when there is a test case failure
    """

    def __init__(self, message="Test case Failed"):
        self.message = f"Test case failed. Error : {message}"
        super().__init__(self.message)


class TestBedSetupFailure(Exception):
    """
    Exception is raised when there is failure in environment set-up for the scenario
    """

    def __init__(self, message="Failed to set-up ENV for test execution"):
        self.message = f"Test bed setup failed. Error : {message}"
        super().__init__(self.message)
