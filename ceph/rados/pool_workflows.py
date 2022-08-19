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
        # Setup Script pre-requisites : docopt
        client_node.exec_command(
            sudo=True, cmd="pip3 install docopt", long_running=True
        )

        cmd_options = f"--pool {pool_name} --start {obj_start} --end {obj_end} --key-count {num_keys_obj}"
        cmd = f"python3 generate_omap_entries.py {cmd_options}"
        client_node.exec_command(sudo=True, cmd=cmd, long_running=True)

        # removing the py file copied
        client_node.exec_command(sudo=True, cmd="rm -rf generate_omap_entries.py")

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

    def do_rados_delete(self, pool_name: str, pg_id: str = None):
        """
        deletes all the objects from the given pool / PG ID
        Args:
            1. pool_name: name of the pool
            2. [ pg_id ]: Pg ID (Optional, but when provided, should be passed along with pool name )

        Returns: True -> pass, False -> fail
        """
        obj_cmd = f"rados -p {pool_name} ls"
        if pg_id:
            obj_cmd = f"rados --pgid {pg_id} ls"

        delete_obj_list = self.rados_obj.run_ceph_command(cmd=obj_cmd, timeout=1000)
        for obj in delete_obj_list:
            cmd = f"rados -p {pool_name} rm {obj['name']}"
            self.rados_obj.node.shell([cmd], long_running=True)

            # Sleeping for 3 seconds for object reference to be deleted
            time.sleep(3)

            # Checking if object is still present in the pool
            out = self.rados_obj.run_ceph_command(cmd=obj_cmd, timeout=1000)
            rem_objs = [obj["name"] for obj in out]
            if obj["name"] in rem_objs:
                log.error(f"Object {obj['name']} not deleted in the pool")
                return False
            log.debug(f"deleted object: {obj['name']} from pool {pool_name}")
        log.info(f"Completed deleting all objects from pool {pool_name}")
        return True

    def create_pool_snap(self, pool_name: str):
        """
        Creates snapshots of the given pool
        Args:
            pool_name: name of the pool
        Returns: Pass -> name of the snapshot created, Fail -> False

        """
        # Checking if snapshots can be created on the supplied pool
        cmd = "ceph osd dump"
        pool_status = self.rados_obj.run_ceph_command(cmd=cmd, timeout=800)
        for detail in pool_status["pools"]:
            if detail["pool_name"] != pool_name:
                continue
            if "selfmanaged_snaps" in detail["flags_names"]:
                # bz: https://bugzilla.redhat.com/show_bug.cgi?id=1425803#c2
                log.error(
                    f"Pool {pool_name} is a self managed pool, cannot create snaps manually"
                )
                return False

        # Creating snaps on the pool provided
        cmd = "uuidgen"
        out, err = self.rados_obj.node.shell([cmd])
        uuid = out[0:5]
        snap_name = f"{pool_name}-snap-{uuid}"
        cmd = f"ceph osd pool mksnap {pool_name} {snap_name}"
        self.rados_obj.node.shell([cmd], long_running=True)

        # Checking if snap was created successfully
        if not self.check_snap_exists(snap_name=snap_name, pool_name=pool_name):
            log.error("Snapshot of pool not created")
            return False
        log.debug(f"Created snapshot {snap_name} on pool {pool_name}")
        return snap_name

    def check_snap_exists(self, snap_name: str, pool_name: str) -> bool:
        """
        checks the existence of the snapshot name given on the pool
        Args:
            snap_name: Name of the snapshot
            pool_name: Name of the pool

        Returns: True -> Snapshot exists, False -> snapshot does not exist
        """
        snap_list = self.get_snap_names(pool_name=pool_name)
        return True if snap_name in snap_list else False

    def get_snap_names(self, pool_name: str) -> list:
        """
        Fetches the list of snapshots created on the given pool
        Args:
            pool_name: name of the pool

        Returns: list of the snaps created
        """
        cmd = "ceph osd dump"
        pool_status = self.rados_obj.run_ceph_command(cmd=cmd, timeout=800)
        for detail in pool_status["pools"]:
            if detail["pool_name"] == pool_name:
                snap_list = [snap["name"] for snap in detail["pool_snaps"]]
                log.debug(f"snapshots on pool : {snap_list}")
        return snap_list

    def delete_pool_snap(self, pool_name: str, snap_name: str = None) -> bool:
        """
        deletes snapshots of the given pool. If no snap name is provided, deletes all the snapshots on the pool
        Args:
            pool_name: name of the pool
            snap_name: name of the snapshot
        Returns: Pass -> snapshot Deleted, Fail -> snapshot not Deleted

        """
        if snap_name:
            delete_list = list(snap_name)
        else:
            delete_list = self.get_snap_names(pool_name=pool_name)

        # Deleting snaps on the pool provided
        for snap in delete_list:
            cmd = f"ceph osd pool rmsnap {pool_name} {snap}"
            self.rados_obj.node.shell([cmd])

            # Checking if snap was deleted successfully
            if self.check_snap_exists(snap_name=snap_name, pool_name=pool_name):
                log.error("Snapshot of pool exists")
                return False
            log.debug(f"deleted snapshot {snap} on pool {pool_name}")
        log.debug("Deleted provided snapshots on the pool")
        return True

    def get_bulk_details(self, pool_name: str) -> bool:
        """
        Checks the status of bulk flag on the pool given
        Args:
            pool_name: Name of the pool
        Returns: True -> pass, False -> fail

        """
        # Checking if the sent pool already exists.
        if pool_name not in self.rados_obj.list_pools():
            log.error(f"Pool {pool_name} does not exist")
            return False

        # Getting the bulk status
        obj = self.rados_obj.get_pool_property(pool=pool_name, props="bulk")
        return obj["bulk"]

    def set_bulk_flag(self, pool_name: str) -> bool:
        """
        Sets the bulk flag to true on existing pools
        Args:
            pool_name: Name of the pool
        Returns: True -> pass, False -> fail

        """
        # Checking if the sent pool already exists. If does not, creating new pool
        if pool_name not in self.rados_obj.list_pools():
            log.info(
                f"Pool {pool_name} does not exist, creating new pool with bulk enabled"
            )
            if not self.rados_obj.create_pool(pool_name=pool_name, bulk=True):
                log.error("Failed to create the replicated Pool")
                return False

        # Enabling bulk on already existing pool
        if not self.rados_obj.set_pool_property(
            pool=pool_name, props="bulk", value="true"
        ):
            log.error(f"Could not set the bulk flag on pool {pool_name}")
            return False

        # Sleeping for 2 seconds after pool create/Modify for PG's to be calculated with bulk
        time.sleep(2)

        # Checking if the bulk is enabled or not
        return self.get_bulk_details(pool_name=pool_name)

    def rm_bulk_flag(self, pool_name: str) -> bool:
        """
        Removes the bulk flag on existing pools
        Args:
            pool_name: Name of the pool
        Returns: True -> pass, False -> fail

        """
        # Checking if the sent pool already exists.
        if pool_name not in self.rados_obj.list_pools():
            log.info(f"Pool {pool_name} does not exist")
            return False

        # Enabling bulk on already existing pool
        if not self.rados_obj.set_pool_property(
            pool=pool_name, props="bulk", value="false"
        ):
            log.error(f"Could not unset the bulk flag on pool {pool_name}")
            return False

        # Sleeping for 2 seconds after pool create/Modify for PG's to be calculated with bulk
        time.sleep(2)

        # Checking if the bulk is enabled or not
        return not self.get_bulk_details(pool_name=pool_name)

    def get_target_pg_num_bulk_flag(self, pool_name: str) -> int:
        """
        Fetches the target PG counts for the given pool from the autoscaler status
        Args:
            pool_name: Name of the pool

        Returns: PG Count

        """
        # Checking the autoscaler status, final PG counts, bulk flags
        cmd = "ceph osd pool autoscale-status"
        pool_status = self.rados_obj.run_ceph_command(cmd=cmd)

        for entry in pool_status:
            if entry["pool_name"] == pool_name:
                return int(entry["pg_num_final"])

    def get_pool_id(self, pool_name) -> int:
        """
        Returns the pool ID of the pool passed
        Args:
            pool_name: Name of the pool
        Returns: ID of the pool
        """
        cmd = "ceph df"
        out = self.rados_obj.run_ceph_command(cmd=cmd)
        for entry in out["pools"]:
            if entry["name"] == pool_name:
                return entry["id"]
        log.error(f"Pool: {pool_name} not found")
