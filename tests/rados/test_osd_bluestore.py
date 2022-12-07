"""
This module tests the blue store related feature-
 1. Testing blue store v3
"""

import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    log.info("Running blue store V3 test case")
    if config.get("create_pools"):
        pools = config.get("create_pools")
        for each_pool in pools:
            pool_details = each_pool["create_pool"]
            pool_name = pool_details.get("pool_name")
            pg_num = pool_details.get("pg_num")
            method_should_succeed(
                rados_obj.create_pool, pool_name=pool_name, pg_num=pg_num
            )
            rados_obj.bench_write(pool_name=pool_name, rados_write_duration=300)

    # Get the osd clients
    clients = ceph_cluster.get_ceph_objects("osd")

    # get the OSD list as a dictionary.Node name as key and osd numbers as values
    rados_list = rados_obj.get_osd_list()

    # Randomly picking the osd node from the list and in that osd node again picking random osd.
    # kill the osd and checking the osd process will up and run automatically.
    # Executing the test three times.

    for _ in range(3):
        # Randomly pick the osd object.
        host_object = random.choice(clients)

        # Get the osd host name
        host_name = host_object.exec_command(sudo=True, cmd="hostname")
        host_name = ("".join(host_name)).strip()
        # From the osd node picking the random osd number
        osd_list = rados_list[host_name]
        osd = str(random.choice(osd_list))

        # get the process id of the osd
        osd_pid = rados_obj.get_osd_pid(host_object, osd)
        log.info(f"Killing the osd-{osd} on the cluster node {host_name}")
        # Kill the osd process
        rados_obj.kill_osd_process(host_object, osd_pid)

        # Check the process is coming up or not.Maximum time provided 5 minutes
        timeout = time.time() + 60 * 5
        status_chk_flag = False

        while time.time() < timeout:
            status = rados_obj.check_osd_status(host_object, osd)
            if status == 0:
                log.info(
                    f"osd-{osd} is up and running  on the cluster node {host_name}"
                )
                status_chk_flag = True
                break
            time.sleep(30)
        if status_chk_flag is False:
            log.error(f"osd-{osd} not up on the cluster node {host_name}")
            return 1
    log.info("Bluestore V3 test execution passed")
    return 0
