import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonElectionStrategies
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Changes b/w various election strategies and observes mon quorum behaviour
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonElectionStrategies(rados_obj=rados_obj)
    installer_node = ceph_cluster.get_nodes(role="installer")[0]

    # Collecting the number of mons in the quorum before the test
    mon_init_count = len(mon_obj.get_mon_quorum().keys())

    if config.get("stretch_mode"):
        try:
            log.info(
                "Verifying ceph mon election strategies in Stretch mode enabled stretch cluster"
            )

            # Verifying the default mode of election strategy is connectivity.
            default_strategy = mon_obj.get_election_strategy()
            if default_strategy != 3:
                log.error(
                    f"Stretch cluster election strategy was not 'connectivity', i.e {default_strategy}"
                )
                return 1
            log.info(
                f"Default election strategy for Mon verified successfully: {default_strategy}"
            )

            # Try changing the election strategy mode to classic or disallow, should fail
            # RFE raised to restrict user from changing Mon election strategy in Stretch mode
            # https://bugzilla.redhat.com/show_bug.cgi?id=2264124
            # Changing strategy to 1. i.e. classic mode
            """
            log.info("Trying to change mon election strategy to 1. i.e. classic mode")
            if mon_obj.set_election_strategy(mode="classic"):
                log.error(
                    "Able to set election strategy to classic mode in stretch mode, should not have worked"
                )
                return 1

            # Changing strategy to 2. i.e. disallowed mode
            log.info("Trying to change mon election strategy to 2. i.e. disallow mode")
            if mon_obj.set_election_strategy(mode="disallow"):
                log.error(
                    "Able to set election strategy to disallow mode in stretch mode, should not have worked"
                )
                return 1
            """

            # Checking connectivity scores of all the mons
            cmd = f"ceph daemon mon.{installer_node.hostname} connection scores dump"
            mon_conn_score = rados_obj.run_ceph_command(cmd=cmd)
            log.info(
                f"Connectivity score of all daemons in the cluster: \n {mon_conn_score}"
            )

            # reset the connectivity scores of all the mons
            log.info("Reset the connectivity scores of all the mons")
            cmd = f"ceph daemon mon.{installer_node.hostname} connection scores reset"
            cephadm.shell(args=[cmd])

            # Print the connectivity scores of all the mons post reset
            cmd = f"ceph daemon mon.{installer_node.hostname} connection scores dump"
            mon_conn_score = rados_obj.run_ceph_command(cmd=cmd)
            log.info(
                f"Connectivity score of all daemons post reset: \n {mon_conn_score}"
            )

            if not check_mon_status(rados_obj, mon_obj):
                log.error("All Mon daemons are not in running state")
                return 1
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("Completed all mon election test scenarios valid for stretch mode")
        return 0

    else:
        try:
            # By default, the election strategy is classic. Verifying that
            default_strategy = mon_obj.get_election_strategy()
            if default_strategy != 1:
                log.error(
                    f"cluster created election strategy other than classic, i.e {default_strategy}"
                )
                return 1

            # Changing strategy to 2. i.e. disallowed mode
            if not mon_obj.set_election_strategy(mode="disallow"):
                log.error("could not set election strategy to disallow mode")
                return 1

            # sleeping for 2 seconds for new elections to be triggered and new leader to be elected
            time.sleep(2)

            log.info(
                "Election strategy set to disallow mode. adding leader mon to disallowed list"
            )
            # Checking if new leader will be chosen if leader is added to disallowed list
            old_leader = mon_obj.get_mon_quorum_leader()
            if not mon_obj.set_disallow_mon(mon=old_leader):
                log.error(f"could not add Mon {old_leader} to the disallowed list")
                return 1

            # sleeping for 2 seconds for new elections to be triggered and new leader to be elected
            time.sleep(2)

            current_leader = mon_obj.get_mon_quorum_leader()
            if re.search(current_leader, old_leader):
                log.error(
                    f"The mon: {old_leader} added to disallow list is still leader"
                )
                return 1

            # removing the mon from the disallowed list
            if not mon_obj.remove_disallow_mon(mon=old_leader):
                log.error(f"could not remove mon: {old_leader} from disallowed list")
                return 1

            # sleeping for 2 seconds for new elections to be triggered and new leader to be elected
            time.sleep(2)

            # Changing strategy to 3. i.e Connectivity mode.
            if not mon_obj.set_election_strategy(mode="connectivity"):
                log.error("could not set election strategy to connectivity mode")
                return 1

            # Checking connectivity scores of all the mons
            cmd = f"ceph daemon mon.{installer_node.hostname} connection scores dump"
            mon_conn_score = rados_obj.run_ceph_command(cmd=cmd)
            log.info(
                f"Connectivity score of all daemons in the cluster: \n {mon_conn_score}"
            )

            # Changing strategy to default
            if not mon_obj.set_election_strategy(mode="classic"):
                log.error("could not set election strategy to classic mode")
                return 1

            # sleeping for 5 seconds for new elections to be triggered and new leader to be elected
            time.sleep(5)

            # todo: add other tests to ascertain the health of mon daemons in quorum
            # [12-Feb-2024] added ^
            if not check_mon_status(rados_obj, mon_obj):
                log.error("All Mon daemons are not in running state")
                return 1

            # Collecting the number of mons in the quorum after the test
            mon_final_count = len(mon_obj.get_mon_quorum().keys())
            if mon_init_count < mon_final_count:
                log.error(
                    "There are less mons in the quorum at the end than there before"
                )
                return 1
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # Changing strategy to default
            if not mon_obj.set_election_strategy(mode="classic"):
                log.error("could not set election strategy to classic mode")
                return 1
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("Completed all mon election test cases")
        return 0


def check_mon_status(rados_obj, mon_obj):
    """Method to verify all Mon daemons are in running state
    Args:
        rados_obj: CephAdmin object
        mon_obj: MonElectionStrategies class object
    Returns:
        True -> pass | False -> fail
    """
    # check running status of each mon daemon
    mon_list = mon_obj.get_mon_quorum().keys()
    log.debug(f"List of Monitors in the cluster: {mon_list}")
    for mon in mon_list:
        status, desc = rados_obj.get_daemon_status(daemon_type="mon", daemon_id=mon)
        if status != 1 or desc != "running":
            log.error(f"{mon} is not in running state: {status} | {desc}")
            return False
        log.info(f"Mon {mon} is in healthy state: {status} | {desc}")
    return True
