"""
Module to verify client connection over v1 port does not result in crashes
"""

import datetime
import random
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83594645
    Covers:
        - BZ-2237038
    Test to verify client connection over v1 port does not result in crashes
    Steps
    1. Deploy a ceph cluster
    2. Choose a node at random which is not already configured as a client
    3. Copy client keyring and ceph conf file having only v1 ports
    4. Install ceph-common package
    5. Run ceph command using new client and monitor any crash
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    log.info(
        "Running test case to verify client connection over v1 port does not result in crashes"
    )

    try:
        """
        If you use a client that does not support reading options from the configuration database,
        or if you still need to use ceph.conf to change your cluster configuration for other reasons,
        run the following command:
               ceph config set mgr mgr/cephadm/manage_etc_ceph_ceph_conf false
        You must maintain and distribute the ceph.conf file across the storage cluster.
        Ref: https://red.ht/46YnXif
        """

        # set ceph_conf mgr setting to false
        assert mon_obj.set_config(
            section="mgr", name="mgr/cephadm/manage_etc_ceph_ceph_conf", value="false"
        )

        # logging any existing crashes on the cluster
        crash_list = rados_obj.do_crash_ls()
        if crash_list:
            log.error(
                "!!!ERROR: Crash exists on the cluster at the start of the test \n\n:"
                f"{crash_list}"
            )
            log.info(
                "As the existing crash may or may not be related, proceeding with the execution"
            )

        # choose an osd host at random
        osd_hosts = rados_obj.ceph_cluster.get_nodes(role="osd")
        osd_host = random.choice(osd_hosts)

        rados_obj.configure_host_as_client(host_node=osd_host)

        # modify the conf file and remove references to v2
        ceph_conf, _ = osd_host.exec_command(cmd="cat /etc/ceph/ceph.conf", sudo=True)
        # Use regex to remove only the v2 parts within the square brackets
        ceph_conf_v1 = re.sub(r"v2:[^,]+,?", "", ceph_conf)
        # Remove any trailing commas left by the removal
        ceph_conf_v1 = re.sub(r",\]", "]", ceph_conf_v1)
        file_name = "/etc/ceph/ceph.conf"
        file_ = osd_host.remote_file(sudo=True, file_name=file_name, file_mode="w")
        file_.write(ceph_conf_v1)
        file_.flush()
        file_.close()

        # print the content of modified ceph.conf from the chosen host
        conf_from_node, _ = osd_host.exec_command(
            cmd="cat /etc/ceph/ceph.conf", sudo=True
        )
        log.info(conf_from_node)

        assert "v2" not in conf_from_node

        # check for any crashes on the cluster for 120 secs
        timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=120)
        while datetime.datetime.now() < timeout_time:
            # run a set of ceph commands
            cmds = ["ceph status", "ceph osd tree", "ceph df"]
            for cmd in cmds:
                log.info(f"Running cmd: {cmd}")
                log.info(osd_host.exec_command(cmd=cmd, sudo=True)[0])
            # list any reported crashes on the cluster, should not have any
            crash_list = rados_obj.do_crash_ls()
            if len(crash_list) != 0:
                log.error(
                    f"Verification failed, crash observed on the cluster : \n {crash_list}"
                )
                raise Exception("Verification failed, crash observed on the cluster")
            log.info("No crashes reported on the cluster yet")
            time.sleep(30)

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        mon_obj.remove_config(
            section="mgr", name="mgr/cephadm/manage_etc_ceph_ceph_conf"
        )
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("V1 client connection verified successfully")
    return 0
