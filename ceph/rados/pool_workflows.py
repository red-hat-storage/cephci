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
