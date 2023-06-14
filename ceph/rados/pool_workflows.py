"""
Module to change pool attributes
1. Autoscaler tunables
2. Snapshots
"""
import datetime
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_scrub import RadosScrubber
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
        self.scrub_obj = RadosScrubber(node=node)
        self.node = node

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
                3. num_keys_obj: Number of KW pairs to be added to each object

        Returns: True -> pass, False -> fail
        """

        def check_omap_entries() -> bool:
            pool_stats = self.rados_obj.get_cephdf_stats(
                pool_name=pool_name, detail=True
            )
            if pool_stats["stats"]["stored_omap"] <= 0:
                # OMAP entries are not readily available,
                # restarting primary osd as metadata does not get updated automatically.
                # TBD: bug raised for the issue: https://bugzilla.redhat.com/show_bug.cgi?id=2210278
                primary_osd = self.rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
                self.rados_obj.change_osd_state(action="restart", target=primary_osd)
                pool_stats = self.rados_obj.get_cephdf_stats(
                    pool_name=pool_name, detail=True
                )
                if pool_stats["stats"]["stored_omap"] <= 0:
                    log.error(
                        f"No omap entries available for the pool {pool_name} even after restarting primary OSD"
                    )
                    return False
            return True

        # Getting the client node to perform the operations
        client_node = self.rados_obj.ceph_cluster.get_nodes(role="client")[0]
        obj_start = kwargs.get("obj_start", 0)
        obj_end = kwargs.get("obj_end", 2000)
        num_keys_obj = kwargs.get("num_keys_obj", 20000)
        log.debug(
            f"Writing {(obj_end - obj_start) * num_keys_obj} Key pairs"
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

        # Storing the pg dump log before setting the deep-scrub parameters
        before_deep_scrub_log = self.scrub_obj.get_pg_dump(
            "pgid", "last_deep_scrub_stamp"
        )

        # Triggering deep scrub on the pool
        self.rados_obj.run_deep_scrub(pool=pool_name)

        flag = 1
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=6000)
        while end_time > datetime.datetime.now():
            after_deep_scrub_log = self.scrub_obj.get_pg_dump(
                "pgid", "last_deep_scrub_stamp"
            )
            deep_scrub_status = self.scrub_obj.verify_scrub_deepscrub(
                before_deep_scrub_log, after_deep_scrub_log, "deepscrub"
            )

            if deep_scrub_status:
                log.info("deep scrubbing has not been completed on the pool")
                if check_omap_entries():
                    log.info("OMAP entries were available post OSD restart")
                    break
                log.info("Sleeping for 120 seconds and checking again")
                time.sleep(120)
                flag = 1
                continue
            flag = 0
            log.info("Completed deep-scrub on the cluster")
            break

        if not flag:
            log.error(
                f"Could not complete deep-scrub on the pool : {pool_name} even with 6000 sec wait"
            )
            return False

        log.debug("Checking the amount of omap entries created on the pool")
        if not check_omap_entries():
            log.error(
                "No omap entries available for the pool after complete deep-scrub and OSD restart"
            )
            return False
        pool_stats = self.rados_obj.get_cephdf_stats(pool_name=pool_name, detail=True)
        log.info(
            f"Wrote {pool_stats['stats']['stored_omap']} bytes of omap data on the pool."
        )
        log.info(
            f"Wrote {pool_stats['stats']['stored_omap']} bytes of omap data on the pool."
            f"Total stored omap data on pool : {pool_stats['stats']['omap_bytes_used']}"
        )
        return True

    def prepare_static_data(self, node):
        """
        creates a 4MB obj, same obj will be put nobj times
        because in do_rados_get we have to verify checksum
        """
        tmp_file = "/tmp/sdata.txt"
        DSTR = "hello world"
        sfd = None

        try:
            sfd = node.remote_file(file_name=tmp_file, file_mode="w+")
            sfd.write(DSTR * 4)
            sfd.flush()
            cmd = f"truncate -s 4M {tmp_file}"
            node.exec_command(cmd=cmd)
        except Exception as e:
            log.error(f"file creation failed with exception: {e}")
            log.error(traceback.format_exc())

        sfd.seek(0)
        sfd.close()
        return tmp_file

    def do_rados_put(
        self,
        client,
        pool: str,
        obj_name: str = None,
        nobj: int = 1,
        offset: int = 0,
        timeout: int = 600,
    ):
        """
        write static data to one object or nobjs in an app pool
        Args:
            client:                 client node
            pool (str):             pool name to which data needs to added
            optional args -
            obj_name (str):         Name of the existing object or
                                    new object to be created in the pool
            nobj (int):             Number of times data will be put to object 'obj_name'
                                    with incremental offset if 'obj_name' is specified, else
                                    data will be put once to nobj number of objects -
                                    obj0, obj1, obj2 and so on...
            offset (int):           write object with start offset, default - 0
            timeout (int):          timeout for rados put execution [defaults to 600 secs]

        E.g. -
            obj.do_rados_put(client=client_node, pool="pool_name")
            obj.do_rados_put(client=client_node, pool="pool_name", obj_name="test-obj)
            obj.do_rados_put(client=client_node, pool="pool_name", nobj=5)
            obj.do_rados_put(client=client_node, pool="pool_name", nobj=5, offset=512, timeout=300)
            obj.do_rados_put(client=client_node, pool="pool_name", obj_name="obj-test",
                             nobj=5, offset=512, timeout=300)

        Returns:
            0 -> pass, 1 -> fail
        """
        infile = self.prepare_static_data(client)
        log.debug(f"Input file is {infile}")

        for i in range(nobj):
            log.info(f"running command on {client.hostname}")
            put_cmd = (
                f"rados put -p {pool} obj{i} {infile}"
                if obj_name is None
                else f"rados put -p {pool} {obj_name} {infile}"
            )
            if offset:
                put_cmd = f"{put_cmd} --offset {offset}"
            try:
                out, _ = client.exec_command(sudo=True, cmd=put_cmd, timeout=timeout)
                if obj_name is not None and offset:
                    offset += offset
            except Exception:
                log.error(traceback.format_exc)
                return 1
        return 0

    def do_rados_append(self, client, pool: str, obj_name: str = None, nobj: int = 1):
        """
        Append static data to nobjs already present in a pool
        Args:
            client: client node
            pool: pool name to which the object belongs
            obj_name (optional): Name of the existing object in the pool
            nobj (optional): Number of times data will be appended
            to object 'obj_name' if 'obj_name' is specified, else
            data will be appended once to nobj number of objects -
            obj0, obj1, obj2 and so on...
        Returns: 0 -> pass, 1 -> fail
        """
        infile = self.prepare_static_data(client)
        log.debug(f"Input file is {infile}")

        for i in range(nobj):
            log.info(f"running command on {client.hostname}")
            append_cmd = (
                f"rados append obj{i} {infile} -p {pool}"
                if obj_name is None
                else f"rados append {obj_name} {infile} -p {pool}"
            )
            try:
                out, _ = client.exec_command(sudo=True, cmd=append_cmd)
            except Exception:
                log.error(traceback.format_exc)
                return False
        return True

    def do_rados_delete(self, pool_name: str, pg_id: str = None, objects: list = []):
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
        if objects:
            delete_obj_list = objects
        log.info(f"Objects to be deleted : {delete_obj_list}")
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

    def wait_for_clean_pool_pgs(self, pool_name: str, timeout: int = 9000) -> bool:
        """
        Waiting for up to 2.5 hours for the PG's to enter active + Clean state
        Args:
            pool_name: Name of the pool on which clean PGs should be checked
            timeout: timeout in seconds or "unlimited"

        Returns:  True -> pass, False -> fail
        """
        end_time = 0
        if timeout == "unlimited":
            condition = lambda x: "unlimited" == x
        elif isinstance(timeout, int):
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
            condition = lambda x: end_time > datetime.datetime.now()

        pool_id = self.get_pool_id(pool_name=pool_name)
        while condition(end_time if isinstance(timeout, int) else timeout):
            flag = False
            cmd = "ceph pg dump pgs"
            pg_dump = self.rados_obj.run_ceph_command(cmd=cmd)
            for entry in pg_dump["pg_stats"]:
                if str(entry["pgid"]).startswith(str(pool_id)):
                    # Proceeding to check if the PG is in active + clean on the pool
                    rec = (
                        "remapped",
                        "backfilling",
                        "degraded",
                        "incomplete",
                        "peering",
                        "recovering",
                        "recovery_wait",
                        "undersized",
                        "backfilling_wait",
                    )
                    flag = (
                        False
                        if any(key in rec for key in entry["state"].split("+"))
                        else True
                    )
                    log.info(
                        f"PG ID : {entry['pgid']}    ---------      PG State : {entry['state']}"
                    )
                    if not flag:
                        break
            if flag:
                log.info("The recovery and back-filling of the OSD is completed")
                return True
            log.info("Waiting for active + clean. checking status again in a minute")
            time.sleep(60)

        log.error("The cluster did not reach active + Clean state")

    def check_large_omap_warning(
        self, pool, obj_num: int, check: bool, timeout: int = 240
    ) -> bool:
        """
        Perform deep-scrub on given pool and check if "Large omap object"
        warning is found
        Args:
            pool: Name of the pool
            obj_num: number of objects which are expected to have
                    large omap entries
            check: boolean value to decide whether warning should exist or not
            timeout: timeout in seconds for warning to show up
        Returns:
            True-> pass | False-> Fail
        """
        if not check:
            timeout = 120
        warn_found = False
        threshold, _ = self.node.shell(
            ["ceph config get osd osd_deep_scrub_large_omap_object_key_threshold"]
        )
        if int(threshold) != 200000:
            log.error(
                f"Large omap object threshold has changed from 200000 to {threshold}."
                f"Aborting execution, please align test input params with new threshold"
            )
            return False

        self.rados_obj.run_deep_scrub(pool=pool)
        timeout_time = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout
        )  # Timeout to let health warning show up
        while datetime.datetime.now() < timeout_time:
            health_detail, _ = self.node.shell(["ceph health detail"])
            log.info(health_detail)
            if (
                f"{obj_num} large omap objects" in health_detail
                and f"{obj_num} large objects found in pool '{pool}'" in health_detail
            ):
                warn_found = True
                break
            time.sleep(30)

        if (warn_found and check) or (not warn_found and not check):
            log.info(
                f"Large omap objects warning found: {warn_found} | Expected: {check}"
            )
            return True
        log.error("Large omap warning did not appear as per expectation")
        return False
