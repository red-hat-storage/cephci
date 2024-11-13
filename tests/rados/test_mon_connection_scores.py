""" 
Module to verify connection scores of monitor dameons are valid at different scenarios
"""
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonElectionStrategies
from ceph.rados.monitor_workflows import MonitorWorkflows
from utility.log import Log
import json
import random

log = Log(__name__)

def run(ceph_cluster, **kw):
    """
    Module to verify connection scores of monitor dameons are valid at different scenarios
    Returns:
        1 -> Fail, 0 -> Pass
    #steps
    1. Change election strategy to disallow and perform connection score validation
    2. Change election strategy to connectivity and perform connection score validation
    3. Remove random monitor service and perform connection score validation
    4. Add monitor service and perform connection score validation
    5. Restart monitor service using systemctl and perform connection score validation
    6. Stop monitor service using systemctl and perform connection score validation
    7. Start monitor service using systemctl and perform connection score validation
    """
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_election_obj = MonElectionStrategies(rados_obj=rados_obj)
    mon_workflow_obj =  MonitorWorkflows(node=cephadm)

    try: 
        # Collect mon details such as name, rank and number of mons
        mon_nodes = ceph_cluster.get_nodes(role="mon")
        mon_nums = len(mon_nodes)

        # Election strategy is classic by default, connection score validation
        if not connection_score_checks(mon_nodes, rados_obj, mon_nums, mon_election_obj): 
            raise Exception("Connection score checks failed")        

        # Changing strategy to 2. i.e. disallowed mode and perform connection score validation
        if not mon_election_obj.set_election_strategy(mode="disallow"):
            log.error("could not set election strategy to disallow mode")
            raise Exception("Changing election strategy failed")
        
        if not connection_score_checks(mon_nodes, rados_obj, mon_nums, mon_election_obj): 
            raise Exception("Connection score checks failed")


        # Changing strategy to 3. i.e. connectivity mode and perform connection score validation
        if not mon_election_obj.set_election_strategy(mode="connectivity"):
            log.error("could not set election strategy to connectivity mode")
            raise Exception("Changing election strategy failed")
        
        if not connection_score_checks(mon_nodes, rados_obj,  mon_nums, mon_election_obj): 
            raise Exception("Connection score checks failed")
    

        # Select random mon service to be removed and perform connection score validation
        mon_host = random.choice(mon_nodes)

        if not mon_workflow_obj.set_mon_service_managed_type(unmanaged=True):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not unmanaged error")

        if not mon_workflow_obj.remove_mon_service(host=mon_host.hostname):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not removed error")

        if not connection_score_checks(mon_nodes, rados_obj,  mon_nums-1, mon_election_obj):  
            raise Exception("Connection score checks failed")


        # Adding monitor service and perform connection score validation
        if not mon_workflow_obj.add_mon_service(host=mon_host):
            log.error("Could not add mon service")
            raise Exception("mon service not added error")

        if not connection_score_checks(mon_nodes, rados_obj,  mon_nums, mon_election_obj): 
            raise Exception("Connection score checks failed")

    
        # Restart monitor service and perform connection score validation
        if not rados_obj.change_daemon_systemctl_state(
            action="restart", daemon_type="mon", daemon_id=mon_host.hostname
        ):
            log.error(f"Failed to restart mon on host {mon_host.hostname}")
            raise Exception("Mon restart failure error")
        
        if not connection_score_checks(mon_nodes, rados_obj,  mon_nums, mon_election_obj): 
            raise Exception("Connection score checks failed")


        # Stop monitor service and perform connection score validation
        if not rados_obj.change_daemon_systemctl_state(
            action="stop", daemon_type="mon", daemon_id=mon_host.hostname
        ):
            log.error(f"Failed to stop mon on host {mon_host.hostname}")
            raise Exception("Mon stop failure error")
        
        if not connection_score_checks(mon_nodes, rados_obj,  mon_nums, mon_election_obj): 
            raise Exception("Connection score checks failed")


        # Start stopped monitor service and perform connection score validation
        if not rados_obj.change_daemon_systemctl_state(
            action="start", daemon_type="mon", daemon_id=mon_host.hostname
        ):
            log.error(f"Failed to start mon on host {mon_host.hostname}")
            raise Exception("Mon start failure error")
        
        if not connection_score_checks(mon_nodes, rados_obj,  mon_nums, mon_election_obj): 
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
            f"Successfully removed and added back the mon on host : {mon_host.hostname}"
            )
    return 0

def connection_score_checks(mon_nodes, rados_obj, num_mons, mon_election_obj) -> bool:

    mon_quorum = mon_election_obj.get_mon_quorum()

    for node in mon_nodes:
        
        if not rados_obj.check_daemon_exists_on_host(host=node.hostname, daemon_type="mon"):
            continue

        mon_status, mon_status_desc = rados_obj.get_daemon_status(
                daemon_type="mon", daemon_id=node.hostname
            )
        
        if mon_status != 1 or mon_status_desc != "running":
            continue

        cmd = f"cephadm shell ceph daemon mon.{node.hostname} connection scores dump"
        out,err = node.exec_command(sudo=True, cmd=cmd)
        mon_conn_score = json.loads(out)
        log.info(
            f"Connectivity score of all daemons in the cluster: \n {mon_conn_score}"
        )

        reports = mon_conn_score["reports"]
        if len(reports) != num_mons:
            log.error("Invalid number of mons in connection scores dump")
            return False 

        for report in reports:
            current_mon_rank = report["rank"]
            peer_mons_rank = [ peer_mon_rank for peer_mon_rank in mon_quorum.values() if peer_mon_rank != current_mon_rank ]
            peer_scores = report["peer_scores"]

            log.info(f"currrent mon: {current_mon_rank} peer mons: {peer_mons_rank}")

            if len(peer_scores) != len(peer_mons_rank):
                log.error("Invalid number of peers in connection scores dump")
                return False

            for peer_score_detail in peer_scores:
                log.info(f"peer mon rank: {peer_score_detail['peer_rank']} peer mon score:{peer_score_detail['peer_score']}")
                if peer_score_detail["peer_rank"] not in peer_mons_rank:
                    log.error("peer mon score missing in connection scores dump")
                    return False
                
                if peer_score_detail["peer_score"] < 0 and peer_score_detail["peer_score"] > 1:
                    log.error("Invalid peer_score value in connection scores dump")
                    return False
                
    return True