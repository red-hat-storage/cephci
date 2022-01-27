"""
Module to change pool attributes
1. Autoscaler tunables
2. Snapshots
"""
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


class PoolFunctions:
    """
    Contains various functions that help in altering the behaviour, working of pools and verify the changes
    """

    def __init__(self, node: CephAdmin):
        """
        initializes the env to run rados commands
        Args:
            node: CephAdmin object
        """
        self.rados_obj = RadosOrchestrator(node=node)

    def verify_target_ratio_set(self, pool_name, ratio):
        """
        Sets the "target_size_ratio" on the given pool and verifies it from the auto-scale status
        Args:
            pool_name: name of the pool
            ratio: ratio to be set

        Returns: True -> pass, False -> fail

        """
        log.debug(f"Setting ratio: {ratio} on pool: {pool_name}")
        self.rados_obj.set_pool_property(
            pool=pool_name, props="target_size_ratio", value=ratio
        )

        # sleeping for 2 seconds for pg autoscaler updates the status and new PG's
        time.sleep(2)
        ratio_set = self.get_pg_autoscaler_value(pool_name, item="target_ratio")
        if not ratio_set == ratio:
            log.error("specified target ratio not set on the pool")
            return False
        return True

    def get_pg_autoscaler_value(self, pool_name, item):
        """
        Fetches the target ratio set on the pool given
        Args:
            pool_name: name of the pool
            item: Value of the item to be fetched.
                Allowed values: actual_capacity_ratio|actual_raw_used|bias|capacity_ratio|crush_root_id|target_bytes|
                effective_target_ratio|logical_used|pg_autoscale_mode|pg_num_target|pool_id|raw_used|target_ratio|

        Returns: Requested value
        """
        cmd = "ceph osd pool autoscale-status"
        autoscale_status = self.rados_obj.run_ceph_command(cmd=cmd)
        try:
            pool_details = [
                details
                for details in autoscale_status
                if details["pool_name"] == pool_name
            ][0]
        except Exception:
            log.error("Pool not found")
        return pool_details[item]

    def fill_omap_entries(self, pool_name, **kwargs):
        """
        creates key-value entries for objects on ceph pools and increase the omap entries on the pool
        eg : if obj_start, obj_end: 0, 3000 objects, with num_keys 1000,  the method would create 3000 objects with 1k
        KW pairs each. so total 3000*1000 KW entries
        Args:
            pool_name: name of the pool where the KW pairs needed to be added to objects
            **kwargs: other args that can be passed
                Valid args:
                1. obj_start: start count for object creation
                2. obj_end : end count for object creation
                3. num_keys_obj: Number of KW paris to be added to each object

        Returns: True -> pass, False -> fail
        """
        # Getting the client node to perform the operations
        client_node = self.rados_obj.ceph_cluster.get_nodes(role="client")[0]
        obj_start = kwargs.get("obj_start", 0)
        obj_end = kwargs.get("obj_end", 2000)
        num_keys_obj = kwargs.get("num_keys_obj", 20000)
        log.debug(
            f"Writing {(obj_end - obj_start) * num_keys_obj} Key paris"
            f" to increase the omap entries on pool {pool_name}"
        )
        script_loc = "https://raw.githubusercontent.com/red-hat-storage/cephci/master/utility/generate_omap_entries.py"
        client_node.exec_command(
            sudo=True,
            cmd=f"curl -k {script_loc} -O",
        )
        cmd_options = f"--pool {pool_name} --start {obj_start} --end {obj_end} --key-count {num_keys_obj}"
        cmd = f"python3 generate_omap_entries.py {cmd_options}"
        client_node.exec_command(sudo=True, cmd=cmd, long_running=True)

        # removing the py file copied
        client_node.exec_command(cmd="rm generate_omap_entries.py")

        log.debug("Checking the amount of omap entries created on the pool")
        pool_stats = self.rados_obj.run_ceph_command(cmd="ceph df detail")["pools"]
        for detail in pool_stats:
            if detail["name"] == pool_name:
                pool_1_stats = detail["stats"]
                total_omap_data = pool_1_stats["omap_bytes_used"]
                omap_data = pool_1_stats["stored_omap"]
                break
        if omap_data < 0:
            log.error("No omap entries written into pool")
            return False
        log.info(
            f"Wrote {omap_data} bytes of omap data on the pool."
            f"Total stored omap data on pool : {total_omap_data}"
        )
        return True
