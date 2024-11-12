"""
Module to verify connection scores of monitor dameons are valid
at different scenarios
"""

import json
import random

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from tests.rados.monitor_configurations import MonElectionStrategies
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83602911
    Covers:
        - BZ-1892173
        - BZ-2174954
        - BZ-2151501
        - BZ-2315595
        - BZ-2326179
    Module to verify connection scores of monitor
    dameons are valid at different scenarios
    Returns:
        1 -> Fail, 0 -> Pass
    #steps
    1. Deploy a Ceph cluster
    2. Validate correct number of mons in connection score,
       correct number of peers for each mon, each mon present in quorum
       and valid connection score ( score between 0 - 1 )
    3. Change election strategy to disallow mode and perform
       validations mentioned in step(2)
    4. Change election strategy to connectivity mode and perform
       validations mentioned in step(2)
    5. Add new host as mon and perform validations
       mentioned in step(2)
    6. Remove mon added in step(5) in the cluster
       and perform validation mentioned in step(2)
    7. Restart monitor service using systemctl
       and perform validations mentioned in step(2)
    8. Stop monitor service using systemctl
       and perform validations mentioned in step(2)
    9. Start monitor service using systemctl
       and validations mentioned in step(2)
    """
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_election_obj = MonElectionStrategies(rados_obj=rados_obj)
    mon_workflow_obj = MonitorWorkflows(node=cephadm)

    try:
        # Collect mon details such as name, rank and number of mons
        mon_nodes = ceph_cluster.get_nodes(role="mon")
        osd_nodes = ceph_cluster.get_nodes(role="osd")
        mon_nums = len(mon_nodes)

        # Election strategy is classic by default, connection score validation
        if not mon_workflow_obj.connection_score_checks(
            mon_nodes, rados_obj, mon_nums, mon_election_obj
        ):
            raise Exception("Connection score checks failed")

        # Changing strategy to 2. i.e. disallowed mode
        # and perform connection score validation
        if not mon_election_obj.set_election_strategy(mode="disallow"):
            log.error("could not set election strategy to disallow mode")
            raise Exception("Changing election strategy failed")

        if not mon_workflow_obj.connection_score_checks(
            mon_nodes, rados_obj, mon_nums, mon_election_obj
        ):
            raise Exception("Connection score checks failed")

        # Changing strategy to 3. i.e. connectivity mode
        # and perform connection score validation
        if not mon_election_obj.set_election_strategy(mode="connectivity"):
            log.error("could not set election strategy to connectivity mode")
            raise Exception("Changing election strategy failed")

        if not mon_workflow_obj.connection_score_checks(
            mon_nodes, rados_obj, mon_nums, mon_election_obj
        ):
            raise Exception("Connection score checks failed")

        # Adding new host as mon and perform connection
        # score validation
        new_mon_host = random.choice(osd_nodes)

        if not mon_workflow_obj.add_mon_service(host=new_mon_host):
            log.error(f"Could not add mon service on host {new_mon_host.hostname}")
            raise Exception("mon service not added error")

        if not mon_workflow_obj.connection_score_checks(
            mon_nodes, rados_obj, mon_nums, mon_election_obj
        ):
            pass
            # Adding mon service to cluster causes test case failure - active bug BZ-2326179
            # raise Exception("Connection score checks failed")

        # Remove added mon service
        # and perform connection score validation
        if not mon_workflow_obj.set_mon_service_managed_type(unmanaged=True):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not unmanaged error")

        if not mon_workflow_obj.remove_mon_service(host=new_mon_host.hostname):
            log.error(f"Could not remove mon on host {new_mon_host.hostname}")
            raise Exception("mon service not removed error")

        if not mon_workflow_obj.connection_score_checks(
            mon_nodes, rados_obj, mon_nums - 1, mon_election_obj
        ):
            raise Exception("Connection score checks failed")

        # Restart monitor service and perform connection
        # score validation
        mon_host = random.choice(mon_nodes)

        if not rados_obj.change_daemon_systemctl_state(
            action="restart", daemon_type="mon", daemon_id=mon_host.hostname
        ):
            log.error(f"Failed to restart mon on host {mon_host.hostname}")
            raise Exception("Mon restart failure error")

        if not mon_workflow_obj.connection_score_checks(
            mon_nodes, rados_obj, mon_nums, mon_election_obj
        ):
            raise Exception("Connection score checks failed")

        # Stop monitor service and perform connection score validation
        if not rados_obj.change_daemon_systemctl_state(
            action="stop", daemon_type="mon", daemon_id=mon_host.hostname
        ):
            log.error(f"Failed to stop mon on host {mon_host.hostname}")
            raise Exception("Mon stop failure error")

        if not mon_workflow_obj.connection_score_checks(
            mon_nodes, rados_obj, mon_nums, mon_election_obj
        ):
            raise Exception("Connection score checks failed")

        # Start stopped monitor service and perform connection score validation
        if not rados_obj.change_daemon_systemctl_state(
            action="start", daemon_type="mon", daemon_id=mon_host.hostname
        ):
            log.error(f"Failed to start mon on host {mon_host.hostname}")
            raise Exception("Mon start failure error")

        if not mon_workflow_obj.connection_score_checks(
            mon_nodes, rados_obj, mon_nums, mon_election_obj
        ):
            raise Exception("Connection score checks failed")

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # Setting the mon service as managed by cephadm
        if not mon_workflow_obj.set_mon_service_managed_type(unmanaged=False):
            log.error("Could not set the mon service to managed")
            return 1

        # log cluster health
        rados_obj.log_cluster_health()

        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info(
        f"Successfully verified connection scores at different scenarios\
         later reverted cluster to original state"
    )
    return 0
