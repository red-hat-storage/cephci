"""
Module to perform specific functionalities needed to run
Performance Benchmarking with RADOS
"""

import time
from math import floor

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


class PerfWorkflows:
    """
    Contains various functions to run performance benchmarking at rados layer
    """

    def __init__(self, node: CephAdmin):
        """
        initializes the env to run Perf evaluation for rados
        Args:
            node: CephAdmin object
        """
        self.rados_obj = RadosOrchestrator(node=node)
        self.cluster = node.cluster
        self.clients = node.cluster.get_nodes(role="client")

    def create_pool_per_config(self, **pool_config):
        """
        Method to create required pools at the start of test
        Args:
            pool_config: config specific to the pool
        Returns:
            True -> pass, False -> fail
        """
        # Creating pools
        log.info(
            f"Creating {pool_config['pool_type']} pool on the cluster with name {pool_config['pool_name']}"
        )

        # setting pg_num_min to pg_num so that pools are created with required
        # number of PGs from the start
        pool_config["pg_num_min"] = pool_config["pg_num"]

        if pool_config.get("pool_type", "replicated") == "erasure":
            method_should_succeed(
                self.rados_obj.create_erasure_pool,
                pool_config["pool_name"],
                **pool_config,
            )
        else:
            method_should_succeed(self.rados_obj.create_pool, **pool_config)

        log.info(f"Pool {pool_config['pool_name']} created successfully")
        return True

    def get_object_count(self, fill_percent, pool_name, obj_size, num_client) -> int:
        """
        Method to calculate how many objects should be written of each size
        Args:
            fill_percent(int): pool fill percentage
            pool_name(str): name of the pool
            obj_size(str): object size
            num_client(int): number of clients
        Returns:
            obj count to be written by the client
        """
        # total available space for the pool
        pool_stat = self.rados_obj.get_cephdf_stats(pool_name=pool_name)
        max_avail_kb = pool_stat["stats"]["max_avail"]
        # max_avail_mb = floor(max_avail_kb / 1 << 10)

        # amount of data to be written to the pool
        total_write_data = floor(max_avail_kb * fill_percent / 100)

        # amount of data to be written by each client
        total_write_client = floor(total_write_data / num_client)

        # determine number of objects to be written
        obj_size_kb_int = int(obj_size[:-1])
        if "M" in obj_size:
            obj_size_kb_int *= 1 << 10
        obj_count = floor(total_write_client / obj_size_kb_int)

        pool_multi = self.get_pool_multiplier(pool_name=pool_name)
        obj_count = int(obj_count / pool_multi)
        return obj_count

    def get_pool_multiplier(self, pool_name) -> float:
        """
        Method to calculate the object count multiplier depending
        on the pool type, size and min size
        Args:
            pool_name:
        Returns:
            multiplier value (int)
        """
        pool_detail = self.rados_obj.get_pool_details(pool=pool_name)
        if pool_detail["erasure_code_profile"]:
            return pool_detail["size"] / pool_detail["min_size"]
        return pool_detail["size"]

    def setup_client_nodes(self, nodes):
        """
        Method to ensure required nodes have ceph client and other
        dependent packages installed
        Args:
            nodes: list of nodes which are to be configured
        """
        try:
            for node in nodes:
                self.rados_obj.configure_host_as_client(host_node=node)
        except Exception:
            log.error(f"Client setup failed for node: {node.hostname}")
            return False

        log.info(f"Client setup successful for {nodes}")
        return True

    def trigger_radosbench(self, node, _pool_name, bench_cfg) -> int:
        """
        Method to trigger radosbench on individual nodes
        with desired configs
        Args:
            node: node object
            _pool_name: pool name where data is to be written
            bench_cfg: dictionary object containing
            desired bench config
        Returns:
            PID of radosbench process triggered in the node
        """
        if not self.rados_obj.bench_write(pool_name=_pool_name, **bench_cfg):
            log.error(f"radosbench failed for config: {bench_cfg}")
            raise Exception(f"radosbench failed on node: {node.hostname}")

        time.sleep(5)
        return self.capture_radosbench_pid(node=node)

    @staticmethod
    def capture_radosbench_pid(node) -> int:
        """
        Method to collate radosbench PIDs running on
        each client perf node
        Args:
            node: node object
        Returns:
            PID of radosbench process
        """
        _cmd = 'pgrep -f "rados bench"'
        pid, _ = node.exec_command(cmd=_cmd, sudo=True)
        if not pid:
            log.error(
                f"Process ID for rados bench on {node.hostname} could not be captured"
            )
            raise ValueError(f"Process ID for rados bench on {node.hostname} not found")
        return pid
