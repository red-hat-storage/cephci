import json
import re
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83574284 - Deploy MDS with default values using cephadm.
    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create 1 file systems with --placements
    2. Validate MDS has come up on specific hosts with the labels
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        mds_list = ceph_cluster.get_ceph_objects("mds")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        client1 = clients[0]
        host_list = [mds.node.hostname for mds in mds_list]
        fs_name = "cephfs"

        log.info("Verify Cluster is healthy before test")
        if cephfs_common_utils.wait_for_healthy_ceph(client1, 300):
            log.error("Cluster health is not OK even after waiting for 300secs")
            out, _ = client1.exec_command(sudo=True, cmd="ceph health detail")
            mon_match = re.search(r"(mon\.\S+)\s+has slow ops", out)
            if not mon_match:
                return 1

            mon_daemon = mon_match.group(1)
            log.warning("Mon slow ops detected on %s", mon_daemon)

            orch_out, _ = client1.exec_command(
                sudo=True,
                cmd="ceph orch ps --daemon_type mon --format json",
            )
            mon_daemons = json.loads(orch_out)
            mon_host = None
            for daemon_info in mon_daemons:
                if daemon_info.get("daemon_name") == mon_daemon:
                    mon_host = daemon_info["hostname"]
                    break

            if mon_host:
                mon_node = ceph_cluster.get_node_by_hostname(mon_host)
                if mon_node:
                    log.info(
                        "Collecting debug ops from %s on host %s",
                        mon_daemon,
                        mon_host,
                    )
                    debug_out, _ = mon_node.exec_command(
                        sudo=True,
                        cmd=f"cephadm shell -- ceph daemon {mon_daemon} ops",
                    )
                    log.info("Debug ops output for %s:\n%s", mon_daemon, debug_out)

            # Workaround for BZ IBMCEPH-13663: restart affected mon on 7.1 builds
            if not build.startswith("7.1"):
                return 1
            log.info("Applying workaround for IBMCEPH-13663: restarting %s", mon_daemon)
            client1.exec_command(
                sudo=True,
                cmd=f"ceph orch daemon restart {mon_daemon}",
            )
            time.sleep(30)
            if cephfs_common_utils.wait_for_healthy_ceph(client1, 300):
                log.error(
                    "Cluster health is not OK even after restarting %s", mon_daemon
                )
                return 1
            log.info("Cluster is healthy after mon restart workaround")

        for host in host_list:
            if not fs_util.wait_for_mds_deamon(
                client=client1, process_name=fs_name, host=host
            ):
                raise CommandFailed(f"Failed to start MDS on particular nodes {host}")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
