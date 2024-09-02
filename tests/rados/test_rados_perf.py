"""
Test module to execute performance benchmarking for rados
"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.perf_workflow import PerfWorkflows
from ceph.utils import get_node_by_id
from utility.log import Log

log = Log(__name__)


# Exception for pool creation failure`
class PoolCreationFailed(Exception):
    pass


# Exception for client setup failure`
class ClientSetupFailed(Exception):
    pass


def run(ceph_cluster, **kw):
    """
    Module to trigger Performance evaluation workflow
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    perf_obj = PerfWorkflows(node=cephadm)

    try:
        io_config = config.get("io_config")
        io_stage = io_config.get("io_stage")
        fill_percent = io_config.get("fill_percent")

        pool_config = config.get("pool_config")
        pool_name = pool_config["pool_name"]
        client_config = config.get("client_config")

        # creating necessary pool to start the test
        if io_stage == "pure_fill":
            if not perf_obj.create_pool_per_config(**pool_config):
                log.error("Creation of required pools have failed")
                raise PoolCreationFailed

        # setting up the nodes to run the test
        client_nodes = [x["node"] for x in client_config]

        if not perf_obj.setup_client_nodes(nodes=client_nodes):
            log.error("Setting up client nodes failed")
            raise ClientSetupFailed

        # number of clients
        client_count = len(client_config)

        # execute radosbench on each client node
        for entry in client_config.keys():
            # determine number of objects to be written
            obj_size = entry["obj_size"]
            obj_count = perf_obj.get_object_count(
                fill_percent, pool_name, obj_size, client_count
            )

            bench_cfg = {
                "rados_write_duration": 7200,
                "byte_size": entry["obj_size"],
                "max_objs": obj_count,
                "background": True,
            }

            node_obj = get_node_by_id(ceph_cluster, entry["node"])
            bench_pid = perf_obj.trigger_radosbench(
                node=node_obj, _pool_name=pool_name, bench_cfg=bench_cfg
            )

            if not bench_pid:
                log.error("radosbench process ID could not be determined")
                raise

            entry["node_obj"] = node_obj
            entry["bench_pid"] = bench_pid

        # monitor radosbench pids for 2 hours and report their completion periodically
        timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=7400)
        while datetime.datetime.now() < timeout_time:
            for entry in client_config.keys():
                node_obj = entry["node_obj"]
                bench_pid = entry["bench_pid"]
                out, _ = node_obj.exec_command(
                    cmd=f"kill -0 {bench_pid} &> /dev/null ; echo $?", sudo=True
                )

                if not out:
                    log.info(
                        f"radosbench process with pid {bench_pid} is still running on {node_obj.hostname}"
                    )
                else:
                    log.info(
                        f"radosbench process with pid {bench_pid} completed on {node_obj.hostname}"
                    )
            time.sleep(300)

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        rados_obj.delete_pool(pool="re_pool_concurrent_io")
        rados_obj.delete_pool(pool="re_pool_parallel_io")
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info(
        "Verification of concurrent and parallel IOPS on an object completed successfully"
    )
    return 0
