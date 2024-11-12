"""
Module to verify connection scores of monitor dameons are valid
at different scenarios
"""

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

        for mon_election_strategy in ["classic", "disallow", "connectivity"]:
            log.debug(f"Setting mon election strategy to {mon_election_strategy} mode")
            log.debug(
                f"Proceeding to test all scenarios with mon election strategy {mon_election_strategy}"
            )

            # Changing mon election strategy
            # and perform connection score validation
            if not mon_election_obj.set_election_strategy(mode=mon_election_strategy):
                log.error(
                    f"could not set election strategy to {mon_election_strategy} mode"
                )
                raise Exception("Changing election strategy failed")

            if not mon_workflow_obj.connection_score_checks():
                raise Exception("Connection score checks failed")

            log.debug(
                f"mon connection scores verified after changing election strategy to {mon_election_strategy} mode"
            )

            # Adding new host as mon and perform connection
            # score validation
            new_mon_host = random.choice(osd_nodes)

            log.debug(
                f"Picked random osd host {new_mon_host.hostname} to be added as mon host"
            )
            log.debug(f"Proceeding to add {new_mon_host.hostname} as mon service")

            if not mon_workflow_obj.set_mon_service_managed_type(unmanaged=True):
                log.error("Could not set the mon service to unmanaged")
                raise Exception("mon service not unmanaged error")

            log.debug("Successfully set mon service as unmanaged")

            if not mon_workflow_obj.add_mon_service(host=new_mon_host):
                log.error(f"Could not add mon service on host {new_mon_host.hostname}")
                raise Exception("mon service not added error")

            log.debug(
                f"Successfully added new mon.{new_mon_host.hostname} as mon service"
            )
            log.debug("Proceeding to check mon connection scores after adding new mon")

            if not mon_workflow_obj.connection_score_checks():
                log.debug(
                    "mon connection score checks skipped after adding mon service - active bug BZ-2151501"
                )
                # Adding mon service to cluster causes test case failure - active bug BZ-2151501
                # raise Exception("Connection score checks failed")

            log.debug(
                f"Mon connection scores verified after adding new mon host {new_mon_host.hostname}"
            )
            log.debug(f"Proceeding to remove newly added mon {new_mon_host.hostname}")

            # Remove added mon service
            # and perform connection score validation
            if not mon_workflow_obj.remove_mon_service(host=new_mon_host.hostname):
                log.error(f"Could not remove mon on host {new_mon_host.hostname}")
                raise Exception("mon service not removed error")

            log.debug(f"Successfully removed mon.{new_mon_host.hostname} service")
            log.debug(
                "Proceeding to check mon connection scores after removing newly added mon"
            )

            if not mon_workflow_obj.connection_score_checks():
                raise Exception("Connection score checks failed")

            log.debug(
                f"Mon connection scores verified after removing mon host {new_mon_host.hostname}"
            )

            # Restart monitor service and perform connection
            # score validation
            mon_host = random.choice(mon_nodes)

            log.debug(
                f"Picked random mon host {mon_host.hostname} to restart, stop and start mon service"
            )
            log.debug(f"Proceeding to restart mon service {mon_host.hostname}")

            if not rados_obj.change_daemon_systemctl_state(
                action="restart", daemon_type="mon", daemon_id=mon_host.hostname
            ):
                log.error(f"Failed to restart mon daemon on host {mon_host.hostname}")
                raise Exception("Mon restart failure error")

            log.debug(f"Successfully restarted mon.{mon_host.hostname} service")
            log.debug(
                "Proceeding to check mon connection scores after restarting mon service"
            )

            if not mon_workflow_obj.connection_score_checks():
                raise Exception("Connection score checks failed")

            log.debug(
                f"Mon connection scores verified after restarting mon service on host {mon_host.hostname}"
            )
            log.debug(f"Proceeding to stop mon service on host {mon_host.hostname}")

            # Stop monitor service and perform connection score validation
            if not rados_obj.change_daemon_systemctl_state(
                action="stop", daemon_type="mon", daemon_id=mon_host.hostname
            ):
                log.error(f"Failed to stop mon daemon on host {mon_host.hostname}")
                raise Exception("Mon stop failure error")

            log.debug(f"Successfully stopped mon.{mon_host.hostname} service")
            log.debug(
                "Proceeding to check mon connection scores after stopping mon service"
            )

            if not mon_workflow_obj.connection_score_checks():
                raise Exception("Connection score checks failed")

            log.debug(
                f"Mon connection scores verified after stopping mon service on host {mon_host.hostname}"
            )
            log.debug(f"Proceeding to start mon service on host {mon_host.hostname}")

            # Start stopped monitor service and perform connection score validation
            if not rados_obj.change_daemon_systemctl_state(
                action="start", daemon_type="mon", daemon_id=mon_host.hostname
            ):
                log.error(f"Failed to start mon daemon on host {mon_host.hostname}")
                raise Exception("Mon start failure error")

            log.debug(f"Successfully started mon.{mon_host.hostname} service")
            log.debug(
                "Proceeding to check mon connection scores after starting mon service"
            )

            if not mon_workflow_obj.connection_score_checks():
                raise Exception("Connection score checks failed")

            log.debug(
                f"Mon connection scores verified after starting mon service on host {mon_host.hostname}"
            )
            log.debug(
                f"Successfully verified mon connection scores with mon election strategy set as {mon_election_strategy}"
            )

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
        "Successfully verified connection scores at different scenarios\
        later reverted cluster to original state"
    )
    return 0
