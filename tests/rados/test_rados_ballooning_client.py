"""
1. From the client node blocked the outgoing connection to the MONs. In my case, I blocked for 3 Mon's
   Command used: iptables -A OUTPUT -d <Mon-IP>  -j DROP
   Example on Client node:
     [root@ceph-7-0-zmjrew-node7 ceph]# iptables -A OUTPUT -d 10.0.210.81  -j DROP

3. Executed any rados cli command.
    Example: rados df
4. Calculated the timeout of the command.The timeout should be 5 minutes.

Bugzilla: https://bugzilla.redhat.com/show_bug.cgi?id=2233800
"""

import datetime
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    ceph_nodes = kw.get("ceph_nodes")
    client = ceph_cluster.get_nodes(role="client")[0]

    mon_hosts = []
    try:
        for node in ceph_nodes:
            if node.role == "mon":
                mon_hosts.append(node)

        log.info(f"The mon ip address are -{mon_hosts}")

        for mon_node in mon_hosts:
            log.info(f"Dropping the packets on the {mon_node.hostname} MON node")
            if not rados_object.block_in_out_packets_on_host(
                source_host=client, target_host=mon_node
            ):
                log.error(
                    f"Failed to add IPtable rules to block {client.hostname} on {mon_node.hostname}"
                )
        start_time = datetime.datetime.now()
        try:
            rados_object.client.exec_command(cmd="ceph -s", sudo=True)
        except Exception as err:
            log.info(f"The exception while execution the ceph command-{err}")
            end_time = datetime.datetime.now()
            time_difference = end_time - start_time
            # Actual checking time is exactly 300 seconds. As a execution time included 3 seconds more.
            if time_difference.seconds > 303:
                log.error(
                    f"The time out is more than 303 seconds.The actual time out is- {time_difference.seconds}"
                )
                return 1
            log.info(f"Time out for the command is -{time_difference.seconds}")
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Execution of finally block")
        client.exec_command(sudo=True, cmd="iptables -F", long_running=True)
        log.debug("Sleeping for 30 seconds...")
        time.sleep(30)
        # log cluster health
        rados_object.log_cluster_health()
        # check for crashes after test execution
        if rados_object.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
