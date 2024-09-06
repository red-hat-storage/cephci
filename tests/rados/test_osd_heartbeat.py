import datetime
import json
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    #CEPH-10839
    Automates OSD heartbeat test scenario.
    1. Fetch two OSD node hosts other than installer node
    2. Drop the connection between installer node, two OSD nodes
    3. Wait for Ceph status / health status to say slow ops
    5. Fetch osd id of an OSD running on an Online host
    4. Verify that OSD logs for "heartbeat_check: no reply" entry
       should contain osd_hostname:osd_port
    5. Restore connection between all the nodes
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    timeout = config.get("timeout", 350)
    recovery_timeout = config.get("recovery_timeout", 2400)
    installer_node = ceph_cluster.get_nodes(role="installer")[0]

    log.info("Running osd heartbeat check test case")

    # Get all OSD nodes
    all_osd_nodes = ceph_cluster.get_nodes(role="osd")

    # Get all OSD nodes which are not installer node as well
    osd_nodes = [x for x in all_osd_nodes if x.hostname != installer_node.hostname]
    osd_node_heart = osd_nodes[0]
    osd_node_2 = osd_nodes[1]
    osd_heart_ip = osd_node_heart.ip_address
    heartbeat_log = f"heartbeat_check: no reply from {osd_heart_ip}:68"
    logs_found = False

    try:
        # Drop connection b/w installer node and obtained osd node
        out, _ = osd_node_2.exec_command(
            sudo=True, cmd=f"iptables -A INPUT -d {osd_heart_ip} -j REJECT"
        )
        out, _ = osd_node_2.exec_command(
            sudo=True, cmd=f"iptables -A OUTPUT -d {osd_heart_ip} -j REJECT"
        )
        out, _ = installer_node.exec_command(
            sudo=True, cmd=f"iptables -A INPUT -d {osd_heart_ip} -j REJECT"
        )
        out, _ = installer_node.exec_command(
            sudo=True, cmd=f"iptables -A OUTPUT -d {osd_heart_ip} -j REJECT"
        )

        starttime = datetime.datetime.now()
        timeout_time = starttime + datetime.timedelta(seconds=timeout)
        init_time, _ = installer_node.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")

        while True:
            try:
                ceph_health, _ = installer_node.exec_command(
                    sudo=True, cmd="cephadm shell -- ceph health"
                )
                host_ls = rados_obj.run_ceph_command(
                    cmd=f"ceph orch host ls --host_pattern {osd_node_heart.hostname}"
                )
                assert "HEALTH_WARN" in ceph_health
                assert host_ls[0]["status"] == "Offline"
                endtime, _ = installer_node.exec_command(
                    cmd="sudo date '+%Y-%m-%d %H:%M:%S'"
                )
                log.info(endtime)
                break
            except AssertionError:
                if datetime.datetime.now() > timeout_time:
                    log.error(
                        f"Cluster health could not reach \
                          HEALTH_WARN status or host status \
                          is not Offline within {timeout} secs"
                    )
                    raise
            time.sleep(30)

        # Determine osd_id of an osd running on another osd node
        osd, _ = osd_node_2.exec_command(
            sudo=True, cmd="cephadm shell -- ceph-volume lvm list --format json"
        )
        osd_dict = json.loads(osd)
        osd_id = list(osd_dict.keys())[0]

        osd_log_lines = rados_obj.get_journalctl_log(
            start_time=init_time,
            end_time=endtime,
            daemon_type="osd",
            daemon_id=osd_id,
        )

        log.debug(f"Journalctl logs : {osd_log_lines}")

        logs_found = True if heartbeat_log in osd_log_lines else False
    except Exception as err:
        log.error(f"Failed with exception: {err}")
    finally:
        log.info("*********** Execution of finally block starts ***********")
        # Flush iptables to reset the rules
        out, _ = osd_node_2.exec_command(sudo=True, cmd="iptables -F")
        out, _ = installer_node.exec_command(sudo=True, cmd="iptables -F")

        starttime = datetime.datetime.now()
        recovery_timeout = starttime + datetime.timedelta(seconds=recovery_timeout)
        host_online = False

        while recovery_timeout > datetime.datetime.now():
            host_ls = rados_obj.run_ceph_command(
                cmd=f"ceph orch host ls --host_pattern {osd_node_heart.hostname}"
            )
            if host_ls[0]["status"] != "Offline":
                host_online = True
                break
            time.sleep(30)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    if logs_found and host_online:
        log.info(f"Found {heartbeat_log}__ in osd logs")
        log.info("OSD node host is back Online")
        return 0
    else:
        log.error(f"{heartbeat_log} found in OSD logs: {logs_found}")
        log.error(f"OSD node host online: {host_online}")
        return 1
