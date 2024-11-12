""" 
TODO: Add details
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
    Module to test connection scores of monitor
    Returns:
        1 -> Fail, 0 -> Pass
    """
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_election_obj = MonElectionStrategies(rados_obj=rados_obj)
    mon_workflow_obj =  MonitorWorkflows(node=cephadm)

    try: 
        # Collect mon details such as name, rank, number of mons ( no function to fetch number of mons in quorum )
        mon_quorum = mon_election_obj.get_mon_quorum()
        mon_nodes = ceph_cluster.get_nodes(role="mon")
        mon_nums = len(mon_nodes)-1

        # Perform conn score checks
        if not connection_score_checks(mon_nodes, rados_obj, mon_quorum, mon_nums): 
            raise Exception("Connection score checks failed")
        
         # Changing strategy to 2. i.e. disallowed mode
        if not mon_election_obj.set_election_strategy(mode="disallow"):
            log.error("could not set election strategy to disallow mode")
            raise Exception("Changing election strategy failed")
        
            
       # Perform conn score checks
        if not connection_score_checks(mon_nodes, rados_obj, mon_quorum, mon_nums): 
            raise Exception("Connection score checks failed")

        # Setting up the election strategy on the cluster to connectivity
        if not mon_election_obj.set_election_strategy(mode="connectivity"):
            log.error("could not set election strategy to connectivity mode")
            raise Exception("Changing election strategy failed")
        
        # Perform conn score checks
        if not connection_score_checks(mon_nodes, rados_obj, mon_quorum, mon_nums): 
            raise Exception("Connection score checks failed")
    
        #  Add monitor  tests
        mon_host =  {  "hostname" : "ceph-vipin-squid-qqqj8c-node3" , "ip_address" : "10.0.66.71" }
        
        if not mon_workflow_obj.add_mon_service(host=mon_host):
            log.error("Could not add mon service")
            raise Exception("mon service not added error")
        
        # Update number of monitors, ranks and mon_hosts
        mon_quorum = mon_election_obj.get_mon_quorum()

        # Perform conn score checks
        if not connection_score_checks(mon_nodes, rados_obj, mon_quorum, len(mon_nodes)): 
            raise Exception("Connection score checks failed")

        # Selecting one mon host to be removed at random during OSD down

        return 0 
    
        mon_host = random.choice(mon_hosts)

        # Setting the mon service as unmanaged
        if not mon_workflow_obj.set_mon_service_managed_type(unmanaged=True):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not unmanaged error")

        # Removing mon service
        if not mon_workflow_obj.remove_mon_service(host=mon_host.hostname):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not removed error")

        # Update number of monitors, ranks and mon_hosts
        mon_quorum = mon_election_obj.get_mon_quorum()

        # Perform conn score checks
        if not connection_score_checks(mon_hosts, mon_quorum, num_mons-1): 
            raise Exception("Connection score checks failed")

    
        # restart mon service  - reuse function
        if not rados_obj.change_daemon_systemctl_state(
            action="start", daemon_type="mon", daemon_id=test_mon_host.hostname
        ):
            log.error(f"Failed to start mon on host {test_mon_host.hostname}")
            raise Exception("Mon start failure error")
        

        # Checks : Monitor should be removed from the quorum and its connection score should still be maintained by all mons 
        # Perform conn score checks
        if not connection_score_checks(rados_obj, mon_quorum_hosts_and_ranks, num_mons): 
            raise Exception("connection score checks failed")

        # Stop mon service -  - reuse function
        if not rados_obj.change_daemon_systemctl_state(
            action="start", daemon_type="mon", daemon_id=test_mon_host.hostname
        ):
            log.error(f"Failed to start mon on host {test_mon_host.hostname}")
            raise Exception("Mon start failure error")
        
        # Checks: Monitor should be removed from the quorum and its connection score should still be maintained by all mons 
        # Perform conn score checks
        if not connection_score_checks(rados_obj, mon_quorum_hosts_and_ranks, num_mons): 
            raise Exception("connection score checks failed")

        # start mon 
        if not rados_obj.change_daemon_systemctl_state(
            action="start", daemon_type="mon", daemon_id=test_mon_host.hostname
        ):
            log.error(f"Failed to start mon on host {test_mon_host.hostname}")
            raise Exception("Mon start failure error")
        
        # Perform conn score checks
        if not connection_score_checks(rados_obj, mon_quorum_hosts_and_ranks, num_mons): 
            raise Exception("connection score checks failed")


    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # # Setting the mon service as managed by cephadm
        # if not mon_workflow_obj.set_mon_service_managed_type(unmanaged=True):
        #     log.error("Could not set the mon service to managed")
        #     return 1

        # # log cluster health
        # rados_obj.log_cluster_health()

        # # check for crashes after test execution
        # if rados_obj.check_crash_status():
        #     log.error("Test failed due to crash at the end of test")
        #     return 1

        log.info(
            f"Successfully removed and added back the mon on host : {mon_host.hostname}"
            )
    return 0

def connection_score_checks(mon_nodes, rados_obj, mon_quorum, num_mons) -> bool:
    # TODO: Make this run on each host

    for node in mon_nodes:
    
        if not rados_obj.check_daemon_exists_on_host(host=node.hostname, daemon_type="mon"):
            continue

        cmd = f"cephadm shell ceph daemon mon.{node.hostname} connection scores dump"
        out,err = node.exec_command(sudo=True, cmd=cmd)
        mon_conn_score = json.loads(out)
        log.info(
            f"Connectivity score of all daemons in the cluster: \n {mon_conn_score}"
        )

        reports = mon_conn_score["reports"]

        log.info(f" reports : {len(reports)} mon_nums : {num_mons}")
        if len(reports) != num_mons:
            log.error("Invalid number of mons in connection scores dump")
            return False 

        for report in reports:
            current_mon_rank = report["rank"]
            peer_mons_rank = [ peer_mon_rank for peer_mon_rank in mon_quorum.values() if peer_mon_rank != current_mon_rank ]
            peer_scores = report["peer_scores"]

            log.info(f"curr: {current_mon_rank} peer: {peer_mons_rank}")

            if len(peer_scores) != len(peer_mons_rank):
                log.error("Invalid number of peers in connection scores dump")
                return False

            for peer_score_detail in peer_scores:
                if peer_score_detail["peer_rank"] not in peer_mons_rank:
                    log.error("peer mon score missing in connection scores dump")
                    return False
                
                if peer_score_detail["peer_score"] < 0 and peer_score_detail["peer_score"] > 1:
                    log.error("Invalid peer_score value in connection scores dump")
                    return False
                
    return True