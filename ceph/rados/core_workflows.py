"""
core_workflows module is a rados layer configuration module for Ceph cluster.
It allows us to perform various day1 and day2 operations such as
1. Creating , modifying, setting , getting, writing, scrubbing, reading various pools like EC and replicated
2. Increase decrease PG counts, enable - disable - configure modules that do this
3. Enable logging to file, set and reset config params and cluster checks
4. Set-up email alerts and other cluster operations
More operations to be added as needed

"""

import datetime
import json
import math
import re
import time
from collections import namedtuple

from ceph.ceph_admin import CephAdmin
from ceph.parallel import parallel
from utility.log import Log

log = Log(__name__)


class RadosOrchestrator:
    """
    RadosOrchestrator class contains various methods that perform various day1 and day2 operations on the cluster
    Usage: The class is initialized with the CephAdmin object for various operations
    """

    def __init__(self, node: CephAdmin):
        """
        initializes the env to run rados commands
        Args:
            node: CephAdmin object
        """
        self.node = node
        self.ceph_cluster = node.cluster
        self.client = node.cluster.get_nodes(role="client")[0]
        self.rhbuild = node.config.get("rhbuild")

    def change_recovery_flags(self, action, flags: list = None):
        """Sets and unsets the recovery flags on the cluster

        This method is used to control the recovery and backfill aspects of the cluster by setting/ un-setting
        the below flags on the cluster at global level.
        |nobackfill|norebalance|norecover|

        Args:
            action (str): "set" & "unset" are the allowed actions for the method
            flags (list): Any additional flags that need to be set on the cluster

        Examples::
            change_recovery_flags(action="set")

        Returns: None
        """
        cluster_flags = ["nobackfill", "norebalance", "norecover"]
        log.debug(
            f"{action}-ing recovery flags on the cluster to change recovery behaviour"
        )
        if flags:
            for flag in flags:
                cluster_flags.append(flag)
        for flag in cluster_flags:
            cmd = f"ceph osd {action.lower()} {flag}"
            self.node.shell([cmd])

    def check_pg_state(self, pgid: str) -> list:
        """Fetches and returns the state of PG given

        Args:
            pgid (str): PG ID

        Examples:
            check_pg_state(pgid=11.2f)

        Returns: list of PG states for the PG
        """
        log.debug(f"Checking the PG state for PG ID : {pgid} ")
        cmd = "ceph pg dump pgs"
        pg_stats = self.run_ceph_command(cmd)
        for pg in pg_stats["pg_stats"]:
            if pg["pgid"] == pgid:
                return pg["state"]
        log.error(f"could not find the given pg : {pgid}")
        return []

    def enable_email_alerts(self, **kwargs) -> bool:
        """
        Enables the email alerts module and configures alerts to be sent
        References : https://docs.ceph.com/en/latest/mgr/alerts/
        Args:
            **kwargs: Any other param that needs to be set
            Various args that can be passed are :
            1. smtp_host
            2. smtp_sender
            3. smtp_ssl
            4. smtp_port
            5. interval
            6. smtp_from_name
            7. smtp_destination
        Returns: True -> pass, False -> fail
        """
        alert_cmds = {
            "smtp_host": f"ceph config set mgr mgr/alerts/smtp_host "
            f"{kwargs.get('smtp_host', 'smtp.corp.redhat.com')}",
            "smtp_sender": f"ceph config set mgr mgr/alerts/smtp_sender "
            f"{kwargs.get('smtp_sender', 'ceph-iad2-c01-lab.mgr@redhat.com')}",
            "smtp_ssl": f"ceph config set mgr mgr/alerts/smtp_ssl {kwargs.get('smtp_ssl', 'false')}",
            "smtp_port": f"ceph config set mgr mgr/alerts/smtp_port {kwargs.get('smtp_port', '25')}",
            "interval": f"ceph config set mgr mgr/alerts/interval {kwargs.get('interval', '5')}",
            "smtp_from_name": f"ceph config set mgr mgr/alerts/smtp_from_name "
            f"'{kwargs.get('smtp_from_name', 'Rados 5.0 sanity Cluster')}'",
        }
        cmd = "ceph mgr module enable alerts"
        self.node.shell([cmd])

        for cmd in alert_cmds.values():
            self.node.shell([cmd])

        if kwargs.get("smtp_destination"):
            for email in kwargs.get("smtp_destination"):
                cmd = f"ceph config set mgr mgr/alerts/smtp_destination {email}"
                self.node.shell([cmd])
        else:
            log.error("email addresses not provided")
            return False

        # Printing all the configuration set
        cmd = "ceph config dump"
        log.info(self.run_ceph_command(cmd))

        # Disabling and enabling the email alert module after setting all the config
        states = ["disable", "enable"]
        for state in states:
            cmd = f"ceph mgr module {state} alerts"
            self.node.shell([cmd])
            time.sleep(1)

        # Triggering email alert
        try:
            cmd = "ceph alerts send"
            self.node.shell([cmd])
        except Exception:
            log.error("Error while Sending email alerts")
            return False

        log.info("Email alerts configured on the cluster")
        return True

    def run_ceph_command(self, cmd: str, timeout: int = 300, client_exec: bool = False):
        """
        Runs ceph commands with json tag for the action specified otherwise treats action as command
        and returns formatted output
        Args:
            cmd: Command that needs to be run
            timeout: Maximum time allowed for execution.
            client_exec: Selection if true, runs the command on the client node
        Returns: dictionary of the output
        """

        cmd = f"{cmd} -f json"
        try:
            if client_exec:
                out, err = self.client.exec_command(cmd=cmd, sudo=True, timeout=timeout)
            else:
                out, err = self.node.shell([cmd], timeout=timeout, print_output=False)
        except Exception as er:
            log.error(f"Exception hit while command execution. {er}")
            return None
        if out.isspace():
            return {}
        status = json.loads(out)
        return status

    def pool_inline_compression(self, pool_name: str, **kwargs) -> bool:
        """
        BlueStore supports inline compression using snappy, zlib, or lz4.
        This module sets various compression modes and other related configs
        Args:
            pool_name: pool name on which compression needs to be enabled and configured
            **kwargs: Various args that can be passed:
                1. compression_mode : Whether data in BlueStore is compressed is determined by  compression mode.
                    The modes are:
                        none: Never compress data.
                        passive: Do not compress data unless the write operation has a compressible hint set.
                        aggressive: Compress data unless the write operation has an incompressible hint set.
                        force: Try to compress data no matter what.
                2. compression_algorithm : compression algorithm to be used.
                    Supported:
                        <empty string>
                        snappy
                        zlib
                        zstd
                        lz4
                3. compression_required_ratio : The ratio of the size of the data chunk after compression.
                    eg : 0.7
                4. compression_min_blob_size : Chunks smaller than this are never compressed.
                    eg : 10B
                5. compression_max_blob_size : Chunks larger than this value are broken into smaller blobs
                    eg : 10G
        Returns: Pass -> true , Fail -> false
        """

        if pool_name not in self.list_pools():
            log.error(f"requested pool {pool_name} is not present on the cluster")
            return False

        value_map = {
            "compression_algorithm": kwargs.get("compression_algorithm", "snappy"),
            "compression_mode": kwargs.get("compression_mode", "none"),
            "compression_required_ratio": kwargs.get(
                "compression_required_ratio", 0.875
            ),
            "compression_min_blob_size": kwargs.get("compression_min_blob_size", "0B"),
            "compression_max_blob_size": kwargs.get("compression_max_blob_size", "0B"),
        }

        # Adding the config values
        for val in value_map.keys():
            if kwargs.get(val, False):
                cmd = f"ceph osd pool set {pool_name} {val} {value_map[val]}"
                self.node.shell([cmd])

        details = self.run_ceph_command(cmd="ceph osd dump")
        for detail in details["pools"]:
            if detail["pool_name"] == pool_name:
                compression_conf = detail["options"]
                if (
                    not compression_conf["compression_algorithm"]
                    == value_map["compression_algorithm"]
                ):
                    log.error("Compression algorithm not set")
                    return False
        # tbd: Verify if compression set is working as expected. Compression ratio to be maintained
        log.info(f"compression set on pool {pool_name} successfully")
        return True

    def list_pools(self) -> list:
        """
        Collect the list of pools present on the cluster
        Returns: list of pool names
        """
        cmd = "ceph df"
        out = self.run_ceph_command(cmd=cmd)
        return [entry["name"] for entry in out["pools"]]

    def get_pool_property(self, pool, props):
        """
        Used to fetch a given property set on the pool
        Args:
            pool: name of the pool
            props: property to be fetched.
            Allowed values :
            size|min_size|pg_num|pgp_num|crush_rule|hashpspool|nodelete|nopgchange|nosizechange|
            write_fadvise_dontneed|noscrub|nodeep-scrub|hit_set_type|hit_set_period|hit_set_count|
            hit_set_fpp|use_gmt_hitset|target_max_objects|target_max_bytes|cache_target_dirty_ratio|
            cache_target_dirty_high_ratio|cache_target_full_ratio|cache_min_flush_age|cache_min_evict_age|
            erasure_code_profile|min_read_recency_for_promote|all|min_write_recency_for_promote|fast_read|
            hit_set_grade_decay_rate|hit_set_search_last_n|scrub_min_interval|scrub_max_interval|
            deep_scrub_interval|recovery_priority|recovery_op_priority|scrub_priority|compression_mode|
            compression_algorithm|compression_required_ratio|compression_max_blob_size|
            compression_min_blob_size|csum_type|csum_min_block|csum_max_block|allow_ec_overwrites|
            fingerprint_algorithm|pg_autoscale_mode|pg_autoscale_bias|pg_num_min|target_size_bytes|
            target_size_ratio|dedup_tier|dedup_chunk_algorithm|dedup_cdc_chunk_size
        Returns: key value pair for the requested property
        Note : Trying to fetch the value for property, which has not been set will error out
        """
        # checking if the pool exists
        if pool not in self.list_pools():
            log.error(f"requested pool {pool} is not present on the cluster")
            return False

        cmd = f"ceph osd pool get {pool} {props}"
        return self.run_ceph_command(cmd=cmd, client_exec=True)

    def get_pool_details(self, pool) -> dict:
        """
        Method to fetch the properties of the pool via ceph osd pool ls commands

        Args:
            pool: name of the pool
        returns:
            Dictionary of pool properties for the selected pool
        """
        cmd = "ceph osd pool ls detail"
        out = self.run_ceph_command(cmd=cmd)
        for ele in out:
            if ele["pool_name"] == pool:
                return ele
        log.error(f"pool {pool} not found")
        return {}

    def host_maintenance_enter(
        self, hostname: str, retry: int = 10, yes_i_really_mean_it: bool = False
    ) -> bool:
        """
        Adds the specified host into maintenance mode
        Args:
            hostname: name of the host which needs to be added into maintenance mode
            retry: max number of retries to put host into maintenance mode
            yes_i_really_mean_it: Passes yes i really mean it flag during comand execution
        Returns:
            True -> Host successfully added to maintenance mode
            False -> Host Could not be added to maintenance mode
        """
        log.debug(f"Passed host : {hostname} to be added into maintenance mode")
        iteration = 0

        while iteration <= retry:
            iteration += 1
            cmd = f"ceph orch host maintenance enter {hostname} --force"
            if yes_i_really_mean_it and self.rhbuild.split(".")[0] >= "7":
                cmd = f"ceph orch host maintenance enter {hostname} --force --yes-i-really-mean-it"
            try:
                out, err = self.client.exec_command(cmd=cmd, sudo=True, timeout=600)
                log.debug(f"o/p of maintenance enter cmd : {out}, err stream : {err}")
            except Exception as e:
                log.debug(f"Exception hit, but was expected; {e}")
            time.sleep(20)

            if not self.check_host_status(hostname=hostname, status="Maintenance"):
                log.error(
                    f"Host: {hostname}, not in maintenance mode. Retrying again in 15 seconds, Retry count :{iteration}"
                )
                # retrying in 15 seconds
                time.sleep(15)
                if iteration == retry:
                    return False
            else:
                log.info(f"Added host {hostname} into maintenance mode on the cluster")
                return True

    def host_maintenance_exit(self, hostname: str, retry: int = 3) -> bool:
        """
        Removes the specified host from maintenance mode
        Args:
            hostname: name of the host which needs to be removed from maintenance mode
            retry: no of retries to be done to remove host from maintenance mode

        Returns:
            True -> Host successfully added to maintenance mode
            False -> Host Could not be added to maintenance mode
        """
        log.debug(f"Passed host : {hostname} to be removed from maintenance mode")
        iteration = 0
        if not self.check_host_status(hostname=hostname, status="Maintenance"):
            log.info(f"Host: {hostname}, not in maintenance mode. Pass")
            return True

        while iteration <= retry:
            iteration += 1
            cmd = f"ceph orch host maintenance exit {hostname} "
            try:
                out, _ = self.client.exec_command(cmd=cmd, sudo=True, timeout=600)
                log.debug(f"o/p of maintenance exit cmd : {out}")
            except Exception as e:
                log.debug(f"Exception hit, but was expected; {e}")
            time.sleep(20)

            if self.check_host_status(hostname=hostname, status="Maintenance"):
                log.error(
                    f"Host:{hostname}, in maintenance mode. Retrying again in 15 seconds, Retry count :{iteration}"
                )
                # retrying in 15 seconds
                time.sleep(15)
                if iteration == retry:
                    return False
            else:
                log.info(
                    f"Removed host {hostname} from maintenance mode on the cluster"
                )
                return True

    def set_pool_property(self, pool, props, value):
        """
        Used to fetch a given property set on the pool
        Args:
            pool: name of the pool
            props: property to be set on pool.
                Allowed values :
                size|min_size|pg_num|pgp_num|crush_rule|hashpspool|nodelete|nopgchange|nosizechange|
                write_fadvise_dontneed|noscrub|nodeep-scrub|hit_set_type|hit_set_period|hit_set_count|
                hit_set_fpp|use_gmt_hitset|target_max_objects|target_max_bytes|cache_target_dirty_ratio|
                cache_target_dirty_high_ratio|cache_target_full_ratio|cache_min_flush_age|cache_min_evict_age|
                erasure_code_profile|min_read_recency_for_promote|all|min_write_recency_for_promote|fast_read|
                hit_set_grade_decay_rate|hit_set_search_last_n|scrub_min_interval|scrub_max_interval|
                deep_scrub_interval|recovery_priority|recovery_op_priority|scrub_priority|compression_mode|
                compression_algorithm|compression_required_ratio|compression_max_blob_size|
                compression_min_blob_size|csum_type|csum_min_block|csum_max_block|allow_ec_overwrites|
                fingerprint_algorithm|pg_autoscale_mode|pg_autoscale_bias|pg_num_min|target_size_bytes|
                target_size_ratio|dedup_tier|dedup_chunk_algorithm|dedup_cdc_chunk_size
            value: value to be set for the property
        Returns: Pass -> True, Fail -> False
        """
        # checking if the pool exists
        if pool not in self.list_pools():
            log.error(f"requested pool {pool} is not present on the cluster")
            return False

        cmd = f"ceph osd pool set {pool} {props} {value}"
        out, err = self.node.shell([cmd])
        # sleeping for 2 seconds for the values to reflect
        time.sleep(2)
        log.info(f"property {props} set on pool {pool}")
        return True

    def bench_write(self, pool_name: str, **kwargs) -> bool:
        """
        Method to trigger Write operations via the Rados Bench tool
        Args:
            pool_name: pool on which the operation will be performed
            kwargs: Any other param that needs to passed
                - rados_write_duration -> duration of write operation (int)
                - byte_size -> size of objects to be written (str)
                    eg : 10KB, default - 4096KB
                - num_threads -> Number of threads to be used(int)
                - max_objs -> max number of objects to be written (int)
                - verify_stats -> arg to control whether obj stats need to
                  be verified after write (bool) | default: True
                - check_ec (bool) -> boolean to control exit code check
                - background (bool) -> run rados bench as background process and continue with test execution
                - nocleanup (bool) -> if false, the nocleanup flag will not be added and the objects would be deleted
                - timeout (int) -> user defined timeout for rados bench process
        Returns: True -> pass, False -> fail
        """
        duration = kwargs.get("rados_write_duration", 200)
        byte_size = kwargs.get("byte_size", 4096)
        num_threads = kwargs.get("num_threads")
        max_objs = kwargs.get("max_objs")
        verify_stats = kwargs.get("verify_stats", True)
        check_ec = kwargs.get("check_ec", True)
        nocleanup = kwargs.get("nocleanup", True)
        _timeout = kwargs.get("timeout", duration + 100)
        cmd = f"sudo rados --no-log-to-stderr -b {byte_size} -p {pool_name} bench {duration} write"
        if nocleanup:
            cmd = f"{cmd} --no-cleanup"
        org_objs = self.get_cephdf_stats(pool_name=pool_name)["stats"]["objects"]
        if num_threads:
            cmd = f"{cmd} -t {num_threads}"
        if max_objs:
            cmd = f"{cmd} --max-objects {max_objs}"
        if kwargs.get("background"):
            check_ec = False
            cmd = f"{cmd} &> /dev/null &"
        log.info(f"check_ec: {check_ec}")

        try:
            self.node.shell([cmd], check_status=check_ec, timeout=_timeout)
            if max_objs and verify_stats:
                time.sleep(90)
                new_objs = self.get_cephdf_stats(pool_name=pool_name)["stats"][
                    "objects"
                ]
                log.info(
                    f"Objs in the {pool_name} before IOPS: {org_objs} "
                    f"| Objs in the pool post IOPS: {new_objs} "
                    f"| Expected {org_objs + max_objs} or {org_objs + max_objs + 1}"
                )
                assert (new_objs == org_objs + max_objs) or (
                    new_objs == org_objs + max_objs + 1
                )
            else:
                time.sleep(15)
                new_objs = self.get_cephdf_stats(pool_name=pool_name)["stats"][
                    "objects"
                ]
                log.info(
                    f"Objs in the {pool_name} before IOPS: {org_objs} "
                    f"| Objs in the pool post IOPS: {new_objs} "
                    f"| Expected {new_objs} > 0"
                )
                assert new_objs > 0
            return True
        except Exception as err:
            log.error(f"Error running rados bench write on pool : {pool_name}")
            log.error(err)
            return False

    def bench_read(self, pool_name: str, **kwargs) -> bool:
        """
        Method to trigger Read operations via the Rados Bench tool
        Args:
            pool_name: pool on which the operation will be performed
            kwargs: Any other param that needs to passed
                1. rados_read_duration -> duration of read operation (int)
                2. timeout (int) -> user defined timeout for rados bench process
                3. check_ec (bool) -> boolean to control exit code check
        Returns: True -> pass, False -> fail
        """
        duration = kwargs.get("rados_read_duration", 80)
        _timeout = kwargs.get("timeout", duration + 100)
        check_ec = kwargs.get("check_ec", True)

        try:
            base_cmd = f"rados --no-log-to-stderr -p {pool_name} bench {duration} "
            for opt in ["seq", "rand"]:
                cmd = base_cmd + opt
                if kwargs.get("background"):
                    check_ec = False
                    cmd = f"{cmd} &> /dev/null &"
                log.info(f"check_ec: {check_ec}")
                self.node.shell([cmd], check_status=check_ec, timeout=_timeout)
            return True
        except Exception as err:
            log.error(f"Error running rados bench write on pool : {pool_name}")
            log.error(err)
            return False

    def create_pool(self, pool_name: str, **kwargs) -> bool:
        """
        Create a pool named from the pool_name parameter.
         Args:
            pool_name: name of the pool being created.
            kwargs: Any other args that need to be passed
                1. pg_num -> number of PG's and PGP's
                2. ec_profile_name -> name of EC profile if pool being created is an EC pool
                3. min_size -> min replication size for pool to serve data
                4. size -> min replication size for pool to write data
                5. erasure_code_use_overwrites -> allows overrides in an erasure coded pool
                6. allow_ec_overwrites -> This lets RBD and CephFS store their data in an erasure coded pool
                7. disable_pg_autoscale -> sets auto-scale mode off on the pool
                8. crush_rule -> custom crush rule for the pool
                9. pool_quota -> limit the maximum number of objects or the maximum number of bytes stored
                10. app_name -> name of the application to be set on the pool
                11. pg_num_min -> minimum no of PGs that the autoscaler should scale the PGs to
         Returns: True -> pass, False -> fail
        """

        log.debug(f"creating pool_name {pool_name}")
        cmd = f"ceph osd pool create {pool_name}"
        if kwargs.get("pg_num"):
            cmd = f"{cmd} {kwargs['pg_num']} {kwargs['pg_num']}"
        if kwargs.get("pg_num_max"):
            cmd = f"{cmd} --pg_num_max {kwargs['pg_num_max']}"
        if kwargs.get("ec_profile_name"):
            cmd = f"{cmd} erasure {kwargs['ec_profile_name']}"
        if kwargs.get("crush_rule"):
            cmd = f"{cmd} {kwargs['crush_rule']}"
        if kwargs.get("bulk"):
            cmd = f"{cmd} --bulk"
        if kwargs.get("pg_num_min"):
            cmd = f"{cmd} --pg_num_min {kwargs['pg_num_min']}"
        try:
            self.node.shell([cmd])
        except Exception as err:
            log.error(f"Error creating pool : {pool_name}")
            log.error(err)
            return False

        # Enabling rados application on the pool
        app_name = kwargs.get("app_name", "rados")
        if app_name:
            enable_app_cmd = (
                f"sudo ceph osd pool application enable {pool_name} {app_name}"
            )
            self.node.shell([enable_app_cmd])

        cmd_map = {
            "min_size": f"ceph osd pool set {pool_name} min_size {kwargs.get('min_size')}",
            "size": f"ceph osd pool set {pool_name} size {kwargs.get('size')}",
            "erasure_code_use_overwrites": f"ceph osd pool set {pool_name} "
            f"allow_ec_overwrites {kwargs.get('erasure_code_use_overwrites')}",
            "disable_pg_autoscale": f"ceph osd pool set {pool_name} pg_autoscale_mode off",
            "pool_quota": f"ceph osd pool set-quota {pool_name} {kwargs.get('pool_quota')}",
        }
        for key in kwargs:
            if cmd_map.get(key):
                try:
                    self.node.shell([cmd_map[key]])
                except Exception as err:
                    log.error(
                        f"Error setting the property : {key} for pool : {pool_name}"
                    )
                    log.error(err)
                    return False

        if kwargs.get("app_name") == "rbd":
            pool_init = f"rbd pool init -p {pool_name}"
            try:
                self.node.shell([pool_init])
            except Exception as err:
                log.error(f"failed to initialize the RBD pool. Error: {err}")
                return False

        time.sleep(5)
        log.info(f"Created pool {pool_name} successfully")
        return True

    def check_health_warning(self, warning: str):
        """
        Method to check if Warning passed is generated on cluster
        Args:
            warning: name of the health warning that should be checked
        Returns (bool) :
            True -> Warning present on the cluster
            False -> Warning not present on the cluster
        """
        status_report = self.run_ceph_command(cmd="ceph report", client_exec=True)
        ceph_health_status = list(status_report["health"]["checks"].keys())
        if warning in ceph_health_status:
            log.info(f"warning: {warning}  present on the cluster")
            log.info(
                f"Warning: {warning} generated on the cluster : {ceph_health_status}"
            )
            return True
        log.info(
            f"Warning: {warning} not present on the cluster. "
            f"all Generated warnings : {ceph_health_status}"
        )
        return False

    def change_recovery_threads(self, config: dict, action: str):
        """
        increases or decreases the recovery threads based on the action sent
        Args:
            config: Config from the suite file for the run
            action: Set or remove increase the backfill / recovery threads
                Values : "set" -> set the threads to specified value
                         "rm" -> remove the config changes made
        """

        cfg_map = {
            "osd_max_backfills": f"ceph config {action} osd osd_max_backfills",
            "osd_recovery_max_active": f"ceph config {action} osd osd_recovery_max_active",
        }
        if self.check_osd_op_queue(qos="mclock"):
            self.node.shell(
                ["ceph config set osd osd_mclock_override_recovery_settings true"]
            )
        for cmd in cfg_map:
            if action == "set":
                command = f"{cfg_map[cmd]} {config.get(cmd, 18)}"
            else:
                command = cfg_map[cmd]
            self.node.shell([command])

    def get_pg_acting_set(self, **kwargs) -> list:
        """
        Fetches the PG details about the given pool and then returns the acting set of OSD's from sample PG of the pool
        Args:
            kwargs: Args that can be passed to fetch acting set
                pool_name: name of the pool whose one of the acting OSD set is needed.
                pg_num: pg whose acting set needs to be fetched
                None: Collects the acting set of pool with ID 1
            eg:
        Returns: list osd's part of acting set
        eg : [3,15,20]
        """
        if kwargs.get("pool_name"):
            pool_name = kwargs["pool_name"]
            # get pool pg id
            pool_id = self.get_pool_id(pool_name=pool_name)
            # Collecting the details of the 1st PG in the pool <ID>.0
            pg_num = f"{pool_id}.0"

        elif kwargs.get("pg_num"):
            pg_num = kwargs["pg_num"]

        else:
            log.info("No argument provided, returning the acting set for PG 1.0")
            # Collecting the acting set for a random pool ID 1 from cluster
            pg_num = "1.0"

        log.debug(f"Collecting the acting set for the PG : {pg_num}")
        cmd = f"ceph pg map {pg_num}"
        out = self.run_ceph_command(cmd=cmd)
        log.debug(
            f"collected the acting set for the PG : {pg_num}. details of PG map : {out}"
        )
        return out["up"]

    def run_scrub(self, **kwargs):
        """
        Run scrub on the given OSD or on all OSD's
         Args:
            kwargs:
            1. osd : if an OSD id is passed , scrub to be triggered on that osd
                    eg- obj.run_scrub(osd=3)
            2. pgid: if a PGID is passed, scrubs are run on that PG
                    eg- obj.run_scrub(pgid=1.0)
            3. pool: if pool name is passed, scrubs are run on that pool
                    eg- obj.run_scrub(pool="test-pool")
         Returns: None
        """
        if kwargs.get("osd"):
            cmd = f"ceph osd scrub {kwargs.get('osd')}"
        elif kwargs.get("pgid"):
            cmd = f"ceph pg scrub {kwargs.get('pgid')}"
        elif kwargs.get("pool"):
            cmd = f"ceph osd pool scrub {kwargs.get('pool')}"
        else:
            # scrubbing all the OSD's
            cmd = "ceph osd scrub all"
        self.client.exec_command(cmd=cmd, sudo=True)

    def run_deep_scrub(self, **kwargs):
        """
        Run scrub on the given OSD or on all OSD's
            Args:
                kwargs:
                1. osd : if an OSD id is passed , deep-scrub to be triggered on that osd
                        eg- obj.run_deep_scrub(osd=3)
                2. pgid: if a PGID is passed, deep-scrubs are run on that PG
                        eg- obj.run_deep_scrub(pgid=1.0)
                3. pool: if pool name is passed, deep-scrubs are run on that pool
                        eg- obj.run_deep_scrub(pool="test-pool")
            Returns: None
        """
        if kwargs.get("osd"):
            cmd = f"ceph osd deep-scrub {kwargs.get('osd')}"
        elif kwargs.get("pgid"):
            cmd = f"ceph pg deep-scrub {kwargs.get('pgid')}"
        elif kwargs.get("pool"):
            cmd = f"ceph osd pool deep-scrub {kwargs.get('pool')}"
        else:
            # scrubbing all the OSD's
            cmd = "ceph osd deep-scrub all"
        self.client.exec_command(cmd=cmd, sudo=True)

    def collect_osd_daemon_ids(self, osd_node) -> list:
        """
        The method is used to collect the various OSD daemons present on a particular node
        :param osd_node: name of the OSD node on which osd daemon details are collected (ceph.ceph.CephNode): ceph node
        :return: list of OSD ID's
        """
        cmd = f"sudo ceph osd ls-tree {osd_node.hostname}"
        return self.run_ceph_command(cmd=cmd)

    def enable_balancer(self, **kwargs) -> bool:
        """
        Enables the balancer module with the given mode
        Args:
            kwargs: Any other args that need to be passed
            Supported kw args :
                1. balancer_mode: There are currently two supported balancer modes (str)
                   -> crush-compat
                   -> upmap (default )
                   -> upmap-read
                   -> read
                2. target_max_misplaced_ratio : the percentage of PGs that are allowed to misplaced by balancer (float)
                    target_max_misplaced_ratio = .07
                3. sleep_interval : number of seconds to sleep in between runs (int)
                    sleep_interval = 60
        Returns: True -> pass, False -> fail
        """
        # balancer is always enabled module, There is no need to enable the module via mgr.
        # To verify the same run ` ceph mgr module ls `, which would list all modules.
        # if found to be disabled, can be enabled by ` ceph mgr module enable balancer `
        mgr_modules = self.run_ceph_command(cmd="ceph mgr module ls")
        if not (
            "balancer" in mgr_modules["always_on_modules"]
            or "balancer" in mgr_modules["enabled_modules"]
        ):
            log.error(
                f"Balancer is not enabled. Enabled modules on cluster are:"
                f"{mgr_modules['always_on_modules']} & "
                f"{mgr_modules['enabled_modules']}"
            )

        # Turning on the balancer on the system
        cmd = "ceph balancer on"
        self.node.shell([cmd])

        # Setting the mode for the balancer. Available modes: none|crush-compat|upmap|upmap-read|read
        balancer_mode = kwargs.get("balancer_mode", "upmap")
        cmd = f"ceph balancer mode {balancer_mode}"
        self.node.shell([cmd])
        time.sleep(10)
        cmd = "ceph balancer status"
        balancer_status = self.run_ceph_command(cmd=cmd)
        if balancer_status["mode"] != balancer_mode:
            log.error(
                f"Could not set the desired balancer mode on the cluster."
                f"Balancer state on cluster : {balancer_status}"
            )
            return False

        if kwargs.get("target_max_misplaced_ratio"):
            cmd = f"ceph config set mgr target_max_misplaced_ratio {kwargs.get('target_max_misplaced_ratio')}"
            self.node.shell([cmd])

        if kwargs.get("sleep_interval"):
            cmd = f"ceph config set mgr mgr/balancer/sleep_interval {kwargs.get('sleep_interval')}"
            self.node.shell([cmd])

        # Sleeping for 10 seconds after enabling balancer and then collecting the evaluation status
        time.sleep(10)
        cmd = "ceph balancer status"
        out = self.run_ceph_command(cmd)
        if not out["active"]:
            log.error("Exception balancer is not active")
            return False
        log.info(f"the balancer status is \n {out}")
        return True

    def get_read_scores_on_cluster(self) -> dict:
        """
        Method to get the read balance scores for all the pools on the cluster
        Args:
        Returns:
            dictionary of all the pools with their read scores
            dict[pool_name] = "read_scores"

        """
        log.info("Checking the read balancer scores on the cluster for all pools")
        read_scores = {}
        existing_pools = self.list_pools()
        for pool_name in existing_pools:
            pool_details = self.get_pool_details(pool=pool_name)
            log.debug(f"Selected pool name : {pool_name}")
            if not pool_details["erasure_code_profile"]:
                log.debug(
                    f"Selected pool is a replicated pool. Checking read balancer scores.\n"
                    f" Fetched from pool {pool_details['read_balance']}"
                )
                read_score = round(pool_details["read_balance"]["score_acting"], 2)
                read_scores[pool_name] = read_score
            else:
                log.info(
                    f"Selected pool {pool_name} is a EC pool, skipping check of balance scores."
                )
        log.info(
            f"Completed collection of balance scores for all the pools"
            f"Scores are : {read_scores}"
        )
        return read_scores

    def check_file_exists_on_client(self, loc) -> bool:
        """Method to check if a particular file/ directory exists on the ceph client node

         Args::
            loc: Location from where the file needs to be checked
        Examples::
            status = obj.check_file_exists_on_client(loc="/tmp/crush.map.bin")
        Returns::
            True -> File exists
            False -> FIle does not exist
        """
        try:
            out, err = self.client.exec_command(cmd=f"ls {loc}", sudo=True)
            if not out:
                log.error(f"file : {loc} not present on the Client")
                return False
            log.debug(f"file : {loc} present on the Client")
            return True
        except Exception:
            log.error(f"Unable to fetch details for {log}")
            return False

    def configure_pg_autoscaler(self, **kwargs) -> bool:
        """
        Configures pg_Autoscaler as a global parameter and on pools
        Args:
            **kwargs: Any other param that needs to be set
                1. mon_target_pg_per_osd -> Sets the target number of PG's per OSD
                2. pool_config -> Config to be changed on the given pool (dict)
                    for supported args, look autoscaler_pool_settings() doc
                3. pg_autoscale_value -> Mode of pg auto-scaling to be set, if pool name is provided (str)
                    the allowed values are :
                    1. off -> turns off PG autoscaler on the given pool
                    2. warn -> displays warnings in ceph status, but does not trigger autoscale
                    3. on -> automatically autoscale based on PG count in pool
                4. default_mode -> Default mode to be set for all the newly created pools on the cluster (str)
                    the allowed values are :
                    1. off -> turns off PG autoscaler globally for subsequent pools
                    2. warn -> displays warnings in ceph status, but does not trigger autoscale
                    3. on -> automatically autoscale based on PG count in pool
        Returns: True -> pass, False -> fail
        """

        mgr_modules = self.run_ceph_command(cmd="ceph mgr module ls")
        if (
            "pg_autoscaler" not in mgr_modules["enabled_modules"]
            and "pg_autoscaler" not in mgr_modules["always_on_modules"]
        ):
            cmd = "ceph mgr module enable pg_autoscaler"
            self.node.shell([cmd])

        if kwargs.get("pool_config"):
            pool_conf = kwargs.get("pool_config")
            if not self.autoscaler_pool_settings(**pool_conf):
                return False

        if kwargs.get("default_mode"):
            cmd = f"ceph config set global osd_pool_default_pg_autoscale_mode {kwargs.get('default_mode')}"
            self.node.shell([cmd])

        if kwargs.get("mon_target_pg_per_osd"):
            cmd = f"ceph config set global mon_target_pg_per_osd {kwargs['mon_target_pg_per_osd']}"
            self.node.shell([cmd])

        cmd = "ceph osd pool autoscale-status"
        log.info(self.run_ceph_command(cmd))
        return True

    def autoscaler_pool_settings(self, **kwargs):
        """
        Sets various options on pools wrt PG Autoscaler
        Args:
            **kwargs: various kwargs to be sent
                Supported kw args:
                1. pg_autoscale_mode: PG saler mode for the indivudial pool. Values-> on, warn, off. (str)
                2. target_size_ratio: ratio of cluster pool will utilize. Values -> 0 - 1. (float)
                3. target_size_bytes: size the pool is assumed to utilize. eg: 10T (str)
                4. pg_num_min: minimum pg's for a pool. (int)
                5. pool_name: Name of the pool on which operation is to be done
        Returns:
        """
        pool_name = kwargs["pool_name"]
        value_map = {
            "pg_autoscale_mode": kwargs.get("pg_autoscale_mode"),
            "target_size_ratio": kwargs.get("target_size_ratio"),
            "target_size_bytes": kwargs.get("target_size_bytes"),
            "pg_num_min": kwargs.get("pg_num_min"),
        }
        for val in value_map.keys():
            if val in kwargs.keys():
                if not self.set_pool_property(
                    pool=pool_name, props=val, value=value_map[val]
                ):
                    log.error(f"failed to set property {val} on pool {pool_name}")
                    return False
        return True

    def set_cluster_configuration_checks(self, **kwargs) -> bool:
        """
        Sets up Cephadm to periodically scan each of the hosts in the cluster, and to understand the state of the OS,
         disks, NICs etc
         ref doc : https://docs.ceph.com/en/latest/cephadm/operations/#cluster-configuration-checks
        Args:
            kwargs: Any other param that needs to passed
            The various args that can be sent are :
            1. disable_check_list : list of config checks that need to be disabled. (list)
            2. enable_check_list : list of config checks that need to be Enabled. (list)
            The allowed list of configuration values that can be sent are :
            1. kernel_security : checks SELINUX/Apparmor profiles are consistent across cluster hosts
            2. os_subscription : checks subscription states are consistent for all cluster hosts
            3. public_network : check that all hosts have a NIC on the Ceph public_netork
            4. osd_mtu_size : check that OSD hosts share a common MTU setting
            5. osd_linkspeed : check that OSD hosts share a common linkspeed
            6. network_missing : checks that the cluster/public networks defined exist on the Ceph hosts
            7. ceph_release : check for Ceph version consistency - ceph daemons should be on the same release
            8. kernel_version :  checks that the MAJ.MIN of the kernel on Ceph hosts is consistent
        Returns: True -> pass, False -> fail
        """

        # Checking if the checks are enabled on cluster
        cmd = "ceph cephadm config-check status"
        out, err = self.node.shell([cmd])
        if not re.search("Enabled", out):
            log.info("Cluster config checks not enabled, Proceeding to enable them")
            cmd = "ceph config set mgr mgr/cephadm/config_checks_enabled true"
            self.node.shell([cmd])

        if kwargs.get("disable_check_list"):
            if not self.disable_configuration_checks(kwargs.get("disable_check_list")):
                log.error("failed to disable the given checks")
                return False

        if kwargs.get("enable_check_list"):
            if not self.enable_configuration_checks(kwargs.get("enable_check_list")):
                log.error("failed to enable the given checks")
                return False
        log.info("Completed setting the config checks ")
        return True

    def enable_configuration_checks(self, configs: list) -> bool:
        """
        Enables checks for the configs provided
        Note: Once enabled the module, all the config checks are enabled by default
        Args:
            configs: list of config checks that need to be Enabled. (list)
        Returns: True -> Pass, False -> fail
        """
        for check in configs:
            cmd = f"ceph cephadm config-check enable {check}"
            self.node.shell([cmd])

        cmd = "ceph cephadm config-check ls"
        all_conf_checks = self.run_ceph_command(cmd)

        changed = [entry for entry in all_conf_checks if entry["name"] in configs]
        for check in changed:
            if check["status"] != "enabled":
                return False
        return True

    def disable_configuration_checks(self, configs: list) -> bool:
        """
        disables checks for the configs provided
        Note: Once enabled the module, all the config checks are enabled by default
        Args:
            configs: list of config checks that need to be disabled. (list)
        Returns: True -> Pass, False -> fail
        """
        for check in configs:
            cmd = f"ceph cephadm config-check disable {check}"
            self.node.shell([cmd])

        cmd = "ceph cephadm config-check ls"
        all_conf_checks = self.run_ceph_command(cmd)

        changed = [entry for entry in all_conf_checks if entry["name"] in configs]
        for check in changed:
            if check["status"] == "enabled":
                return False
        return True

    def reweight_crush_items(self, **kwargs) -> bool:
        """
        Performs Re-weight of various CRUSH items, based on key-value pairs sent
        Args:
            **kwargs: Arguments for the commands
        Returns: True -> pass, False -> fail
        """
        # Collecting OSD utilization before re-weights
        cmd = "ceph osd df tree"
        out = self.run_ceph_command(cmd=cmd)
        osd_info_init = [entry for entry in out["nodes"] if entry["type"] == "osd"]
        affected_osds = []
        if kwargs.get("name"):
            name = kwargs["name"]
            weight = kwargs["weight"]
            cmd = f"ceph osd crush reweight {name} {weight}"
            out = self.run_ceph_command(cmd=cmd)
            affected_osds.append(name)

        else:
            # if no params are provided, Doing the re-balance by utilization.
            cmd = r"ceph osd reweight-by-utilization"
            out = self.run_ceph_command(cmd=cmd)
            if int(out["max_change_osds"]) >= 1:
                affected_osds = [entry["osd"] for entry in out["reweights"]]
                log.info(
                    f"re-weights have been triggered on these OSD's, Deatils\n"
                    f"PG's affected : {out['utilization']['moved_pgs']}\n"
                    f"OSd's affected: {[entry for entry in out['reweights']]}"
                )
                # Sleeping for 5 seconds after command execution for process to start
                time.sleep(5)
            else:
                log.info(
                    "No re-weights based on utilization were triggered. PG distribution is optimal"
                )
                return True

        if kwargs.get("verify_reweight"):
            if not self.verify_reweight(
                affected_osds=affected_osds, osd_info=osd_info_init
            ):
                log.error("OSD utilization was not reduced upon re-weight")
                return False
        log.info("Completed the re-weight of OSD's")
        return True

    def verify_reweight(self, affected_osds: list, osd_info: list) -> bool:
        """
        Verifies if Re-weight of various CRUSH items reduced the data on the re-weighted OSD's
        Args:
            affected_osds: osd's whose weights were changed
            osd_info: OSD details before the re-weight was performed
        Returns: Pass -> True, Fail -> False
        """
        # Increasing backfill & recovery rate
        self.change_recovery_threads(config={}, action="set")
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=1200)
        while end_time > datetime.datetime.now():
            status_report = self.run_ceph_command(cmd="ceph report")
            # Proceeding to check if all PG's are in active + clean
            for entry in status_report["num_pg_by_state"]:
                rec = (
                    "remapped",
                    "backfilling",
                )
                flag = (
                    False
                    if any(key in rec for key in entry["state"].split("+"))
                    else True
                )

            if flag:
                log.info("The recovery and back-filling of the OSD is completed")
                break
            log.info(
                f"Waiting for active + clean. Active aletrs: {status_report['health']['checks'].keys()},"
                f"PG States : {status_report['num_pg_by_state']}"
                f" checking status again in 1 minutes"
            )
            time.sleep(60)
        self.change_recovery_threads(config={}, action="rm")
        if not flag:
            log.error(
                "The cluster did not reach active + Clean After re-balancing by capacity"
            )
            return False

        # Checking OSD utilization after re-weight
        cmd = "ceph osd df tree"
        out = self.run_ceph_command(cmd=cmd)

        osd_info_end = [entry for entry in out["nodes"] if entry["id"] in affected_osds]
        for osd_end in osd_info_end:
            for osd_init in osd_info:
                if int(osd_init["id"]) == int(osd_end["id"]):
                    if int(osd_init["kb_used"]) > int(osd_end["kb_used"]):
                        log.error(
                            f"The utilization is higher for OSD : {osd_init['id']}"
                            f"end KB: {int(osd_end['kb_used'])}, init KB: {int(osd_init['kb_used'])}"
                        )
                        return False

        return True

    def delete_pool(self, pool: str) -> bool:
        """
        Deletes the given pool from the cluster
        Args:
            pool: name of the pool to be deleted
        Returns: True -> pass, False -> fail
        """
        # setting config is set to allow pool deletion
        cmd = "ceph config set mon mon_allow_pool_delete true"
        self.client.exec_command(cmd=cmd, sudo=True)

        existing_pools = self.run_ceph_command(cmd="ceph df", client_exec=True)
        if pool not in [ele["name"] for ele in existing_pools["pools"]]:
            log.error(f"Pool:{pool} does not exist on cluster, cannot delete")
            return True

        pool_details = self.get_pool_details(pool=pool)
        cmd = f"ceph osd pool delete {pool} {pool} --yes-i-really-really-mean-it"
        self.client.exec_command(cmd=cmd, sudo=True)

        time.sleep(2)
        # Improving cleanup for EC pools
        if pool_details["type"] == 3:
            try:
                log.info("deleting the profile created for the EC pool")
                profile_list = self.get_ec_profiles()
                if pool_details["erasure_code_profile"] in profile_list:
                    if not self.delete_ec_profile(
                        profile=pool_details["erasure_code_profile"]
                    ):
                        log.error("Could not delete the EC profile created")
                        return False

                log.info("Deleting the crush rule used for the EC pool")
                rule_list = self.get_crush_rule_names()
                if pool in rule_list:
                    cmd = f"ceph osd crush rule rm {pool}"
                    self.client.exec_command(cmd=cmd, sudo=True)
            except Exception as err:
                log.error(
                    f"hit issue while deleting crush rule and profile for the EC pool"
                    f"Error : {err}"
                )

        existing_pools = self.run_ceph_command(cmd="ceph df", client_exec=True)
        if pool not in [ele["name"] for ele in existing_pools["pools"]]:
            log.info(f"Pool:{pool} deleted Successfully")
            return True
        log.error(f"Pool:{pool} could not be deleted on cluster")
        return False

    def enable_file_logging(self) -> bool:
        """
        Enables the cluster logging into files at var/log/ceph and checks file permissions
        Returns: True -> pass, False -> fail
        """
        try:
            cmd = "ceph config set global log_to_file true"
            self.node.shell([cmd])
            cmd = "ceph config set global mon_cluster_log_to_file true"
            self.node.shell([cmd])
        except Exception:
            log.error("Error while enabling config to log into file")
            return False
        return True

    def disable_file_logging(self) -> bool:
        """
        Disable the cluster logging
        Returns: True -> pass, False -> fail
        """
        try:
            cmd = "ceph config set global log_to_file false"
            self.node.shell([cmd])
            cmd = "ceph config set global mon_cluster_log_to_file false"
            self.node.shell([cmd])
        except Exception:
            log.error("Error while disabling config to log into file")
            return False
        return True

    def get_ec_profiles(self) -> list:
        """
        Fetches all the EC profiles present on the cluster
        returns:
            list of ec profiles present on the cluster
        """
        profile_ls = "ceph osd erasure-code-profile ls"
        return self.run_ceph_command(cmd=profile_ls, client_exec=True)

    def get_ec_profile_detail(self, profile: str) -> dict:
        """
        Fetches he EC profile details present on the cluster
        returns:
            list of ec profiles present on the cluster
        """
        if profile in self.get_ec_profiles():
            profile_get = f"ceph osd erasure-code-profile get {profile}"
            return self.run_ceph_command(cmd=profile_get, client_exec=True)
        else:
            log.error(f"EC Profile : {profile} not present on the cluster")

    def delete_ec_profile(self, profile) -> bool:
        """
        Deletes the EC profile name given
        returns:
            Profile deleted -> True
            profile not deleted -> False
        """
        profile_del = f"ceph osd erasure-code-profile rm {profile}"
        try:
            self.run_ceph_command(cmd=profile_del, client_exec=True)
        except Exception as err:
            log.debug(f"Hit exception during profile delete. Error: {err}")

        if profile in self.get_ec_profiles():
            log.info(f"Profile : {profile} not deleted")
            return False
        log.info(f"Profile : {profile}  deleted")
        return True

    def get_crush_rule_names(self) -> list:
        """
        Fetches all the crush rule names present on the cluster
        returns:
            list of crush rule names present on the cluster
        """
        # Creating the pool with the profile created
        # https://docs.ceph.com/en/latest/rados/operations/crush-map/#rules
        cmd = "ceph osd crush rule ls"
        return self.run_ceph_command(cmd=cmd)

    def create_erasure_pool(self, **kwargs) -> bool:
        """
        Creates an erasure code profile and then creates a pool with the same
        References: https://docs.ceph.com/en/latest/rados/operations/erasure-code/
        Args:
            **kwargs: Any other param that needs to be set in the EC profile
                1. k -> the number of data chunks (int)
                2. m -> the number of coding chunks (int)
                3. l -> Group the coding and data chunks into sets of size locality (int)
                4. d -> Number of OSDs requested to send data during recovery of a single chunk
                        d needs to be chosen such that k+1 <= d <= k+m-1. (int)
                4. crush-failure-domain -> crush object to be us to store replica sets (str)
                5. plugin -> plugin to be set (str)
                    supported plugins:
                    1. jerasure (default)
                    2. lrc -> Upstream Only
                    3. clay -> Upstream Only
                6. pool_name -> pool name to create and associate with the EC profile being created
                7. force -> Override an existing profile by the same name.
                8. crush-osds-per-failure-domain -> number of OSDs per failure domain
                9. crush-num-failure-domains -> Number of failure domains present on the cluster
                10. create_rule -> Arg to specify if the CRUSH rule should be created or not
                11. profile_name -> Name of the profile to be created
                12. yes_i_mean_it -> Needed to be passed for profile modification along with --force from 8.0
                13. name: Name of the profile/rule to create if none is provided
                14. negative_test: pass true if performing -ve tests. min_compact_client won't be updated for pool
                creation when this param is set to true. Required for MSR EC pool tests with min_compact_client
        Returns: True -> pass, False -> fail
        """
        failure_domain = kwargs.get("crush-failure-domain", "osd")
        k = kwargs.get("k", 4)
        m = kwargs.get("m", 2)
        l = kwargs.get("l")
        d = kwargs.get("d", 5)
        crush_osds_per_failure_domain = kwargs.get(
            "crush-osds-per-failure-domain", None
        )
        crush_num_failure_domains = kwargs.get("crush-num-failure-domains", None)
        create_rule = kwargs.get("create_rule", False)
        plugin = kwargs.get("plugin", "jerasure")
        pool_name = kwargs.get("pool_name", "name")
        if not pool_name:
            log.error("No name provided. Exiting")
            return False
        force = kwargs.get("force", False)
        create_ecpool = kwargs.get("create_ecpool", True)
        negative_test = kwargs.get("negative_test", False)
        yes_i_mean_it = kwargs.get("yes_i_mean_it", False)
        profile_name = kwargs.get("profile_name", f"ecp_{pool_name}")
        rule_name = f"rule_{pool_name}"

        # Creating an erasure coded profile with the options provided
        cmd = (
            f"ceph osd erasure-code-profile set {profile_name}"
            f" crush-failure-domain={failure_domain} k={k} m={m} plugin={plugin}"
        )
        if crush_osds_per_failure_domain:
            if self.rhbuild and self.rhbuild.split(".")[0] >= "8":
                min_client_version = self.run_ceph_command(cmd="ceph osd dump")[
                    "require_min_compat_client"
                ]
                log.debug(
                    f"require_min_compat_client before starting the tests is {min_client_version}"
                )
                if not negative_test:
                    if min_client_version not in ["squid"]:
                        log.debug(
                            "Setting config to allow clients to create EC MSR rule based pool on the cluster"
                        )
                        config_cmd = "ceph osd set-require-min-compat-client squid --yes-i-really-mean-it"
                        self.client.exec_command(cmd=config_cmd, sudo=True)
                        time.sleep(5)
                        log.debug(
                            "Set the min_compact client on the cluster to Squid on the cluster"
                        )
                cmd = (
                    cmd
                    + f" crush-osds-per-failure-domain={crush_osds_per_failure_domain} "
                    f" crush-num-failure-domains={crush_num_failure_domains}"
                )
            else:
                log.info(
                    "Params 'crush-osds-per-failure-domain' only supported from 8.x"
                    "No modification of the command done."
                )

        if plugin == "lrc":
            cmd = cmd + f" l={l}"
        if plugin == "clay":
            cmd = cmd + f" d={d}"
        if force:
            cmd = cmd + " --force"
        if yes_i_mean_it:
            cmd = cmd + " --yes-i-really-mean-it "

        log.debug(f"Final command to create EC pool : {cmd}")
        try:
            self.run_ceph_command(cmd=cmd)
            time.sleep(5)
            profiles = self.get_ec_profiles()
            if profile_name not in profiles:
                raise Exception(
                    f"Profile not found in list error.\n profile name :{profile_name} "
                    f"\n profiles on cluster : {profiles}"
                )
        except Exception as err:
            log.error(f"Failed to create ec profile : {profile_name}")
            log.error(err)
            return False

        cmd = f"ceph osd erasure-code-profile get {profile_name}"
        log.info(self.node.shell([cmd]))

        # Creating the Crush rule for the profile created
        if create_rule:
            cmd = f"ceph osd crush rule create-erasure {rule_name} {profile_name}"
            self.run_ceph_command(cmd=cmd)
            time.sleep(5)

            rule_list = self.get_crush_rule_names()
            if rule_name not in rule_list:
                log.error(
                    f"unable to create rule: {rule_name}. list obtained from cluster: {rule_list}"
                )
                return False
            if create_ecpool:
                if not self.create_pool(
                    ec_profile_name=profile_name,
                    crush_rule=rule_name,
                    **kwargs,
                ):
                    log.error(f"Failed to create Pool {pool_name}")
                    return False
        else:
            if create_ecpool:
                if not self.create_pool(
                    ec_profile_name=profile_name,
                    **kwargs,
                ):
                    log.error(f"Failed to create Pool {pool_name}")
                    return False
        try:
            log.info(f"Created the ec profile : {profile_name} and pool : {pool_name}")
            cmd = f"ceph osd crush rule dump {pool_name}"
            log.debug(
                f"Printing the crush rule used : \n{self.run_ceph_command(cmd=cmd)}\n"
            )
        except Exception as err:
            log.error(f"Exception hit while listing the EC Crush rule used: {err}")
        return True

    def change_osd_state(self, action: str, target: int, timeout: int = 180) -> bool:
        """
        Changes the state of the OSD daemons wrt the action provided
        Args:
            action: operation to be performed on the service, i.e.
            start, stop, restart, disable, enable
            target: ID osd the target OSD
            timeout: timeout in seconds, (default = 60s)
        Returns: Pass -> True, Fail -> False
        """
        cluster_fsid = self.run_ceph_command(cmd="ceph fsid")["fsid"]
        host = self.fetch_host_node(daemon_type="osd", daemon_id=str(target))
        if not host:
            log.error("failed to find host for the osd")
            return False
        log.debug(f"Hostname of target host : {host.hostname}")
        init_time, _ = host.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        pass_status = True
        osd_status, status_desc = self.get_daemon_status(
            daemon_type="osd", daemon_id=target
        )

        if ((osd_status == 0 or status_desc == "stopped") and action == "stop") or (
            (osd_status == 1 or status_desc == "running") and action == "start"
        ):
            log.info(f"OSD {target} already in desired state: {action}")
            return True

        # If the OSD is stopped and started multiple times, the fail-count can increase
        # and the service cannot come up, without resetting the fail-count of the service.

        # Executing command to reset the fail count on the host and sleeping for 5 seconds
        cmd = "systemctl reset-failed"
        host.exec_command(sudo=True, cmd=cmd)
        time.sleep(5)

        # Executing command to perform desired action.
        cmd = f"systemctl {action} ceph-{cluster_fsid}@osd.{target}.service"
        log.info(
            f"Performing {action} on osd-{target} on host {host.hostname}. Command {cmd}"
        )
        host.exec_command(sudo=True, cmd=cmd)
        # verifying the osd state
        if action in ["start", "stop"]:
            start_time = datetime.datetime.now()
            timeout_time = start_time + datetime.timedelta(seconds=timeout)

            while datetime.datetime.now() <= timeout_time:
                osd_status, status_desc = self.get_daemon_status(
                    daemon_type="osd", daemon_id=target
                )
                log.info(f"osd_status: {osd_status}, status_desc: {status_desc}")
                if (osd_status == 0 or status_desc == "stopped") and action == "stop":
                    break
                elif (
                    osd_status == 1 or status_desc == "running"
                ) and action == "start":
                    break
                time.sleep(20)

            if action == "stop" and osd_status != 0:
                log.error(f"Failed to stop the OSD.{target} service on {host.hostname}")
                pass_status = False
            if action == "start" and osd_status != 1:
                log.error(
                    f"Failed to start the OSD.{target} service on {host.hostname}"
                )
                pass_status = False
            if not pass_status:
                log.error(
                    f"Collecting the journalctl logs for OSD.{target} service on {host.hostname} for the failure"
                )
                end_time, _ = host.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
                osd_log_lines = self.get_journalctl_log(
                    start_time=init_time,
                    end_time=end_time,
                    daemon_type="osd",
                    daemon_id=str(target),
                )
                log.error(
                    f"\n\n ------------ Log lines from journalctl ---------------- \n"
                    f"{osd_log_lines}\n\n"
                )
                return False
        else:
            # Baremetal systems take some time for daemon restarts. changing sleep accordingly
            time.sleep(20)
        return True

    def fetch_host_node(self, daemon_type: str, daemon_id: str = None) -> object:
        """
        Provides the Ceph cluster object for the given daemon. ceph_cluster
        Args:
            daemon_type: type of daemon
                Allowed values: alertmanager, crash, mds, mgr, mon, osd, rgw, prometheus, grafana, node-exporter
            daemon_id: name of the daemon, ID in case of OSD's

        Returns: ceph object for the node

        """
        host_nodes = self.ceph_cluster.get_nodes()
        cmd = f"ceph orch ps --daemon_type {daemon_type}"
        if daemon_id is not None:
            cmd += f" --daemon_id {daemon_id}"
        daemons = self.run_ceph_command(cmd=cmd)
        try:
            o_node = [entry["hostname"] for entry in daemons][0]
            for node in host_nodes:
                if (
                    re.search(o_node, node.hostname)
                    or re.search(o_node, node.vmname)
                    or re.search(o_node, node.shortname)
                ):
                    return node
        except Exception:
            log.error(
                f"Could not find host node for daemon {daemon_type} with name {daemon_id}"
            )
            return None

    def verify_ec_overwrites(self, **kwargs) -> bool:
        """
        Creates RBD image on overwritten EC pool & replicated metadata pool
        Args:
            **kwargs: various kwargs to be sent
                Supported kw args:
                    1. image_name : name of the RBD image
                    2. image_size : size of the RBD image
                    3. metadata_pool: Name of the metadata pool to be created
        Returns: True -> pass, False -> fail

        """

        # Creating a replicated pool for metadata
        metadata_pool = kwargs.get("metadata_pool", "re_pool_overwrite")
        if metadata_pool not in self.list_pools():
            if not self.create_pool(pool_name=metadata_pool, app_name="rbd"):
                log.error("Failed to create Metadata pool for rbd images")
        pool_name = kwargs["pool_name"]
        image_name = kwargs.get("image_name", "image_ec_pool")
        image_size = kwargs.get("image_size", "40M")
        try:
            image_create = f"rbd create --size {image_size} --data-pool {pool_name} {metadata_pool}/{image_name}"
            self.client.exec_command(cmd=image_create, sudo=True)
            cmd = f"rbd --image {image_name} info --pool {metadata_pool}"
            out, err = self.client.exec_command(cmd=cmd, sudo=True)
            log.info(f"The image details are : {out}")

            # mapping the image on to the cluster
            map_cmd = f"rbd device map {metadata_pool}/{image_name} --id admin"
            out, err = self.client.exec_command(cmd=map_cmd, sudo=True)
            log.debug(f"Device created : {out}")

            # Printing the devices mapped on the cluster
            out, err = self.client.exec_command(cmd="rbd device list", sudo=True)
            log.debug(f"Device created : {out}")
            # tbd: create filesystem on image and mount it. Part next PR.
        except Exception as error:
            log.error(
                f"Hit error with testing EC pools with overwrites enabled for RBD. Error : {error}"
            )
            return False

        # Tbd: Add rbd map and mount commands here.
        return True

    def check_compression_size(self, pool_name: str, **kwargs) -> bool:
        """
        Checks the given pool size against "compression_required_ratio" and verifies that data is
        compressed in accordance to the ratio provided
        Args:
            pool_name: Name of the pool
            **kwargs: additional params needed.
                Allowed values:
                    compression_required_ratio: ratio set on the pool for compression
        Returns: True -> pass, False -> fail
        """
        log.info(f"Collecting stats about pool : {pool_name}")
        pool_stats = self.run_ceph_command(cmd="ceph df detail")["pools"]
        flag = False
        for detail in pool_stats:
            if detail["name"] == pool_name:
                pool_1_stats = detail["stats"]
                stored_data = pool_1_stats["stored_data"]
                ratio_set = kwargs["compression_required_ratio"]
                if pool_1_stats["data_bytes_used"] >= (stored_data * ratio_set):
                    log.error(
                        f"The data stored on pool is not compressed in accordance with the ratio set."
                        f"Ideal size after compression <= {stored_data * ratio_set} \n"
                        f"Stored: {pool_1_stats['data_bytes_used']}"
                    )
                    return False
                flag = True
                break
        if not flag:
            log.error(f"Pool {pool_name} not found on cluster.")
            return False
        log.info(f"data on pool is compressed in accordance of ratio : {ratio_set}")
        return True

    def do_crash_ls(self):
        """Runs clash ls on the ceph cluster. returns crash ID's if any

        Examples::
            crash_list = obj.do_crash_ls()
        """

        cmd = "ceph crash ls-new"
        return self.run_ceph_command(cmd=cmd)

    def get_cluster_date(self):
        """
        Used to get the osd parameter value
        Args:
            cmd: Command that needs to be run on container

        Returns : string  value
        """

        cmd = f'{"date +%Y:%m:%d:%H:%u"}'
        out, err = self.node.shell([cmd])
        return out.strip()

    def get_journalctl_log(
        self, start_time, end_time, daemon_type: str, daemon_id: str
    ) -> str:
        """
        Retrieve logs for the requested daemon using journalctl command
        Args:
            start_time: time to start reading the journalctl logs - format ('2022-07-20 09:40:10')
            end_time: time to stop reading the journalctl logs - format ('2022-07-20 10:58:49')
            daemon_type: ceph service type (mon, mgr ...)
            daemon_id: Name of the service, OSD ID in case of OSDs
        Returns:  journal_logs
        """
        fsid = self.run_ceph_command(cmd="ceph fsid")["fsid"]
        host = self.fetch_host_node(daemon_type=daemon_type, daemon_id=daemon_id)
        if daemon_type == "osd" or daemon_type == "mgr":
            systemctl_name = f"ceph-{fsid}@{daemon_type}.{daemon_id}.service"
        elif daemon_type == "mon":
            systemctl_name = f"ceph-{fsid}@{daemon_type}.{host.hostname}.service"
        else:
            systemctl_name = f"ceph-{fsid}@{daemon_type}.{host.shortname}.service"
        try:
            log_lines, err = host.exec_command(
                cmd=f"sudo journalctl -u {systemctl_name} --since '{start_time.strip()}' --until '{end_time.strip()}'"
            )
        except Exception as er:
            log.error(f"Exception hit while command execution. {er}")
            raise
        return log_lines

    def set_mclock_profile(self, profile="balanced", osd="osd", reset=False):
        """Set OSD MClock Profile.

        ceph config set osd_mclock_profile <profile_name>

        profile names:
        - balanced
        - high_recovery_ops
        - high_client_ops

        Args:
            profile: mclock profile name
            osd: "osd" service by default or "osd.<Id>"
            reset: revert mClock profile to default - balanced
        """
        if self.rhbuild and self.rhbuild.split(".")[0] < "6":
            log.info(
                f"mClock specific settings are not valid below RHCS 6"
                f", as the current RH build is {self.rhbuild}, returing TRUE"
                f" to avoid false failure"
            )
            return True
        if not self.check_osd_op_queue(qos="mclock"):
            log.error(
                "Current OSD QoS is not mclock_scheduler. \n"
                "mClock specific settings cannot be implemented"
            )
            raise Exception(
                "Failed to set mClock profile. OSD OP Queue is not mclock_scheduler"
            )
        return (
            self.node.shell([f"ceph config rm {osd} osd_mclock_profile"])
            if reset
            else self.node.shell(
                [f"ceph config set {osd} osd_mclock_profile {profile}"]
            )
        )

    def check_osd_op_queue(self, qos) -> bool:
        """Matches the input OSD op queue against the active
        Qos running on the cluster
        Args:
            qos: QoS to match [WPQ / mClock]"
        Returns:
            True if input QoS matches the active QoS, False otherwise
        """
        current_qos, _ = self.node.shell(["ceph config get osd osd_op_queue"])
        return True if qos.lower() in str(current_qos).lower() else False

    def set_mclock_parameter(
        self, param: str, value, restart_osd: bool = False
    ) -> bool:
        """Set value for any of the valid mClock config parameters
        Args:
            param (str): mClock config parameter to be modified
            value: value to be set for the input parameter
            restart_osd (boolean): flag to control restart of all OSDs;
                necessary only for few parameters, hence added as a tunable setting.
        Returns:
            boolean: True if mClock parameter was set, False otherwise
        """
        if self.rhbuild and self.rhbuild.split(".")[0] < "6":
            log.info(
                f"mClock specific settings are not valid below RHCS 6"
                f", as the current RH build is {self.rhbuild}, returing TRUE"
                f" to avoid false failure"
            )
            return True
        if not self.check_osd_op_queue(qos="mclock"):
            log.error(
                "Current OSD QoS is not mclock_scheduler. \n"
                "mClock specific settings cannot be implemented"
            )
            raise Exception(
                "Failed to set mClock profile. OSD OP Queue is not mclock_scheduler"
            )
        self.node.shell(
            ["ceph config set osd osd_mclock_override_recovery_settings true"]
        )
        self.node.shell([f"ceph config set osd {param} {value}"])
        if restart_osd:
            if not self.restart_daemon_services(daemon="osd"):
                log.error("could not restart the OSD services")
                return False
        return True

    def update_osd_state_on_cluster(self, osd_id, state):
        """
        Method to update the state of the OSD in the cluster. This method can mark the osd "in, out, up, down"
        Args:
            osd_id: ID of the osd to be operated
            state: Property that needs to be updated.
                  Allowed states: "in, out, up, down"
        Returns: True -> pass, false -> fail
        """
        allowed_states = {"in", "out", "up", "down"}
        if state not in allowed_states:
            log.error(f"Invalid state '{state}'. Allowed states are {allowed_states}")
            return False

        log.info(f"Marking OSD.{osd_id} {state}")
        cmd = f"ceph osd {state} {osd_id}"
        self.run_ceph_command(cmd=cmd)
        time.sleep(5)

        log.debug(f"Checking OSD state post marking it {state}")
        cmd = "ceph osd dump"
        dump_status = self.run_ceph_command(cmd=cmd)

        osd_found = False

        for entry in dump_status["osds"]:
            if entry["osd"] == osd_id:
                osd_found = True
                is_in = entry.get("in", 0)
                is_up = entry.get("up", 0)

                if state == "in" and not is_in:
                    log.error(f"OSD {osd_id} not marked in the cluster")
                    return False
                elif state == "out" and is_in:
                    log.error(f"OSD {osd_id} not marked out of the cluster")
                    return False
                elif state == "up" and not is_up:
                    log.error(f"OSD {osd_id} not marked up in the cluster")
                    return False
                elif state == "down" and is_up:
                    log.error(f"OSD {osd_id} not marked down in the cluster")
                    return False
                else:
                    log.info(f"OSD {osd_id} marked {state} successfully.")
                    return True

        if not osd_found:
            log.error(f"OSD {osd_id} state not found in the cluster dump")
        return False

    def get_cephdf_stats(self, pool_name: str = None, detail: bool = False) -> dict:
        """
        Retrieves and returns the output ceph df command
        as a dictionary
        Args:
            pool_name: name of the pool whose stats are
            specifically required
            detail: enables ceph df detail command (default: False)
        Returns:  dictionary output of ceph df/ceph df detail
        """
        _cmd = "ceph df detail" if detail else "ceph df"
        cephdf_stats = self.run_ceph_command(cmd=_cmd, client_exec=True)

        if pool_name:
            try:
                pool_stats = cephdf_stats["pools"]
                for pool_stat in pool_stats:
                    if pool_stat.get("name") == pool_name:
                        return pool_stat
                raise KeyError
            except KeyError:
                log.error(f"{pool_name} not found in ceph df stats")
                return {}

        return cephdf_stats

    def get_pg_state(self, pg_id):
        """Function to get the current state of a PG for the specified PG ID.

        This method queries the PG to get teh current state of the PG.
        Example:
            get_pg_state(pg_id="1.f")
        Args:
            pg_id: PG id

        Returns: Pg state as a string of values
        """
        cmd = f"ceph pg {pg_id} query"
        try:
            pg_query = self.run_ceph_command(cmd=cmd, client_exec=True)
            log.debug(f"The status of pg : {pg_id} is {pg_query['state']}")
            return pg_query["state"]
        except Exception as err:
            log.error(f"Hit exception collecting PG state for : {pg_id}. Error:  {err}")
            return False

    def get_osd_map(self, pool: str, obj: str, nspace: str = None) -> dict:
        """
        Retrieve the osd map for an object in a pool
        Args:
            pool: pool name to which the object belongs
            obj: object name whose osd map is to retrieved
            nspace (optional): namespace
        Returns:  dictionary output of ceph osd map command
        """
        cmd = f"ceph osd map {pool} {obj}"
        if nspace:
            cmd += " " + nspace

        return self.run_ceph_command(cmd=cmd)

    def get_osd_df_stats(
        self, tree: bool = False, filter_by: str = None, filter: str = None
    ) -> dict:
        """
        Retrieves the output of ceph osd df command
        Args:
            tree: enables tree view
            filter_by: filter type, either class or name
            filter: a pool, crush node or device class name
        Returns: dictionary output of ceph osd df
        """
        cmd = "ceph osd df"
        if tree:
            cmd += " tree"
        if filter_by == "class" or filter_by == "name":
            cmd += " " + filter_by
        if filter:
            cmd += " " + filter

        return self.run_ceph_command(cmd=cmd)

    def get_daemon_status(self, daemon_type, daemon_id) -> tuple:
        """
        Returns the status of a specific daemon using ceph orch ps utility
        Usage: orch ps --daemon_type <> --daemon_id <>
        Args:
            daemon_type: type of daemon known to orchestrator
            daemon_id: id of daemon provided in daemon_type
        Returns: tuple containing status of the daemon (0 or 1) and
                 status description (running or stopped)
        """

        cmd_ = (
            f"ceph orch ps --daemon_type {daemon_type} "
            f"--daemon_id {daemon_id} --refresh"
        )
        orch_ps_out = self.run_ceph_command(cmd=cmd_)[0]
        log.debug(orch_ps_out)
        return orch_ps_out["status"], orch_ps_out["status_desc"]

    def daemon_check_post_tests(
        self, pre_test_orch_ps: dict, pre_crash_report: list = None
    ) -> bool:
        """
        Method that compares the daemons & their placement beforw and after the test, to check if any daemon is
        removed, added or placement is changed onto another host.

        Args:
            pre_test_orch_ps : output of "ceph orch ps" before the tests/ operations
            pre_crash_report : output of "ceph crash ls" before the tests/ operations
        Returns:
            Pass -> True, fail -> False
        """
        # Load JSON data
        orch_ps_data = pre_test_orch_ps
        cmd = "ceph orch ps"
        orch_ps_new_data = self.run_ceph_command(cmd=cmd)

        # Function to collect daemon_name and hostname by daemon_type
        def collect_daemon_info(data):
            daemon_info = {}
            for entry in data:
                daemon_type = entry["daemon_type"]
                daemon_name = entry["daemon_name"]
                hostname = entry["hostname"]
                if daemon_type not in daemon_info:
                    daemon_info[daemon_type] = {}
                daemon_info[daemon_type][daemon_name] = hostname
            return daemon_info

        # Collect daemon information from both JSON structures
        orch_ps_daemons = collect_daemon_info(orch_ps_data)
        orch_ps_new_daemons = collect_daemon_info(orch_ps_new_data)

        # Compare daemons and print the results
        all_match = True
        for daemon_type, daemons in orch_ps_daemons.items():
            if daemon_type in orch_ps_new_daemons:
                log.debug(f"Verification for Daemon Type: {daemon_type}")
                for daemon_name, hostname in daemons.items():
                    log.debug(
                        f"Verification for Daemon: {daemon_name} on host {hostname}"
                    )
                    if daemon_name in orch_ps_new_daemons[daemon_type]:
                        log.debug(
                            f"Daemon: {daemon_name} present post test."
                            f" Checking if host is updated post tests"
                        )
                        new_hostname = orch_ps_new_daemons[daemon_type][daemon_name]
                        log.debug(
                            f"Daemon: {daemon_name} present post test."
                            f"Old hostname {hostname} , New hostname : {new_hostname} fro daemon {daemon_name}"
                            f" Checking if host is updated post tests"
                        )
                        is_same = hostname == new_hostname
                        if not is_same:
                            all_match = False
                            log.error(f"Daemon Type: {daemon_type}")
                            log.error(f"  Daemon Name: {daemon_name}")
                            log.error(f"    Old Hostname: {hostname}")
                            log.error(f"    New Hostname: {new_hostname}")
                            log.error(f"    Same Hostname: {is_same}")
                    else:
                        all_match = False
                        log.error(f"Daemon Type: {daemon_type}")
                        log.error(f"  Daemon Name: {daemon_name}")
                        log.error(f"    Old Hostname: {hostname}")
                        log.error("    New Hostname: Not found")
                        log.error("    Same Hostname: False")
            else:
                all_match = False
                log.error(f"Daemon Type: {daemon_type} not found post tests")

        # Check for missing daemons in orch_ps_new
        for daemon_type, daemons in orch_ps_daemons.items():
            if daemon_type not in orch_ps_new_daemons:
                all_match = False
                log.error(f"Daemon Type: {daemon_type} is missing post tests")
            else:
                for daemon_name in daemons:
                    if daemon_name not in orch_ps_new_daemons[daemon_type]:
                        all_match = False
                        log.error(
                            f"Daemon Name: {daemon_name} in "
                            f"Daemon Type: {daemon_type} is missing in orch ps post tests."
                        )

        # Check for new daemons added in orch_ps post upgrade
        for daemon_type, daemons in orch_ps_new_daemons.items():
            if daemon_type not in orch_ps_daemons:
                # all_match = False
                log.error(f"New Daemon Type: {daemon_type} not present post upgrade")
            else:
                for daemon_name in daemons:
                    if daemon_name not in orch_ps_daemons[daemon_type]:
                        # Not failing the method if new daemons are added to the cluster post upgrade
                        # all_match = False
                        log.error(
                            f"New Daemon Name: {daemon_name} in Daemon Type: {daemon_type} found post tests."
                        )

        if pre_crash_report:
            # Checking for new crashes on the cluster since tests started
            crashes = self.run_ceph_command(cmd="ceph crash ls")
            # Convert lists to sets
            set_crash1 = set(pre_crash_report)
            set_crash2 = set(crashes)

            new_crashes = list(set_crash2 - set_crash1)
            log.debug(f"New crashes post start of test execution are : {new_crashes}")
            if len(new_crashes) > 0:
                log.error("New crashes observed on the cluster post starting the test")
                all_match = False

        return all_match

    def compare_df_stats(self, pre_test_df_stats):
        """
        Method to compare the 'total_bytes', 'total_avail_bytes', 'total_used_bytes' in ceph before and after the tests,
        and also check the 'stored', 'objects', 'stored_raw', 'avail_raw' for each pool on the cluster, to check if it's
        same before and after the tests.

        Note: If IO's are being run during the tests, this method cannot be used, as the method validates if the
        two df stats outputs are same
        Args:
            pre_test_df_stats: Output of "ceph df detail" in json format collected before starting the tests
        Returns:
            Pass -> True, Fail -> False
        """

        def bytes_to_gb(value):
            return round(value / (1024**3), 1)

        def within_variance(old_value, new_value, variance=0.25):
            if old_value == 0 and new_value == 0:
                return True  # Both are zero, so they are the same
            if old_value == 0 or new_value == 0:
                return False  # One is zero and the other is not, so they differ
            return abs(old_value - new_value) / old_value <= variance

        df_stats_data = pre_test_df_stats
        df_stats_new_data = self.run_ceph_command(cmd="ceph df detail")
        check_pass = True

        # Compare overall stats
        overall_keys = ["total_bytes", "total_avail_bytes", "total_used_bytes"]
        for key in overall_keys:
            old_value_gb = bytes_to_gb(df_stats_data["stats"].get(key, 0))
            new_value_gb = bytes_to_gb(df_stats_new_data["stats"].get(key, 0))
            log.info(f"Value in {key}: old {old_value_gb} GB , new {new_value_gb} GB")
            if not within_variance(old_value_gb, new_value_gb):
                log.error(
                    f"Difference in {key}: {old_value_gb} GB != {new_value_gb} GB"
                )
                check_pass = False

        # Compare each pool
        old_pools = {pool["name"]: pool["stats"] for pool in df_stats_data["pools"]}
        new_pools = {pool["name"]: pool["stats"] for pool in df_stats_new_data["pools"]}

        for pool_name, old_pool_stats in old_pools.items():
            if pool_name in new_pools:
                new_pool_stats = new_pools[pool_name]
                pool_keys = ["stored", "objects", "stored_raw", "avail_raw"]
                for key in pool_keys:
                    old_value_gb = bytes_to_gb(old_pool_stats.get(key, 0))
                    new_value_gb = bytes_to_gb(new_pool_stats.get(key, 0))
                    log.info(
                        f"Values in {key} for pool {pool_name}: old {old_value_gb} GB , new {new_value_gb} GB"
                    )
                    if not within_variance(old_value_gb, new_value_gb):
                        log.error(
                            f"Difference in {key} for pool {pool_name}: {old_value_gb} GB != {new_value_gb} GB"
                        )
                        check_pass = False
        log.info(
            "All compared values in the ceph df stats, and they are in the 5% variance"
        )
        return check_pass

    def get_ideal_max_avail_pools(self, default_replica_size: int = 3) -> float:
        """
        Method to calculate the MAX_AVAIL on the pools on the cluster, by calculating the formula below
        ([min(osd.avail for osd in OSD_up) - ( min(osd.avail for osd in OSD_up).total_size * (1 - full_ratio)) ] *
        len(osd.avail for osd in OSD_up))/pool.size()

        Args:
            default_replica_size: replica size on the pools. default is 3

        Returns:
            MAX_AVAIL size calculated in float
        """

        def kb_to_gb(kb):
            return round(kb / (1024 * 1024), 1)

        def calculate_max_avail(
            most_used_avail_gb, most_used_total_gb, full_ratio, replica_size, total_osds
        ):
            max_avail = (
                (most_used_avail_gb - (most_used_total_gb * (1 - full_ratio)))
                * total_osds
            ) / replica_size
            return round(max_avail, 1)

        data = self.run_ceph_command(cmd="ceph osd df tree")
        full_ratio = self.run_ceph_command(cmd="ceph osd dump")["full_ratio"]
        replica_size = default_replica_size
        total_osds = 0
        most_used_osd = None
        most_used_kb = 0
        most_used_total_kb = 0
        most_used_avail_kb = 0

        for node in data["nodes"]:
            if node["type"] == "osd":
                total_osds += 1
                if node["kb_used"] > most_used_kb:
                    most_used_kb = node["kb_used"]
                    most_used_osd = node
                    most_used_total_kb = node[
                        "kb"
                    ]  # Assuming 'kb' represents the total size of the OSD
                    most_used_avail_kb = node["kb_avail"]

        most_used_avail_gb = kb_to_gb(most_used_avail_kb)
        most_used_total_gb = kb_to_gb(most_used_total_kb)

        log.debug(
            f"total OSDs on cluster : {total_osds},\n"
            f"Most used OSD : {most_used_osd}"
            f"Most utilization on OSD avail space:  {most_used_avail_gb},\n"
            f"most used OSD total space : {most_used_total_gb})"
        )

        max_avail = calculate_max_avail(
            most_used_avail_gb, most_used_total_gb, full_ratio, replica_size, total_osds
        )
        log.info(f"max avail calculated is = {max_avail}")
        return max_avail

    def verify_max_avail(self, variance: float = 0.20):
        """
        method to verify if the max df calculated by the cluster is as expected on the pool
        bug : https://bugzilla.redhat.com/show_bug.cgi?id=2109129

        Args:
            variance: % of acceptable difference between the calculated vs actual

        Returns:
            Pass -> true, Fail -> False
        """

        def bytes_to_gb(kb):
            return round(kb / (1 << 30), 1)

        pool_data = {}
        check_pass = True
        ceph_df = self.get_cephdf_stats()
        pool_detail = self.run_ceph_command(cmd="ceph osd pool ls detail")

        # capture size of each replicated pool
        for entry in pool_detail:
            if entry["type"] == 1:
                pool_name = entry["pool_name"]
                size = entry["size"]
                pool_data[pool_name] = {"size": size}

        # note max avail for each replicated pool
        for pool in ceph_df["pools"]:
            _pool_name = pool["name"]
            if _pool_name in pool_data.keys():
                max_avail = bytes_to_gb(pool["stats"]["max_avail"])
                pool_data[_pool_name].update({"max_avail": max_avail})

        for pool in pool_data:
            ideal_max_avail = self.get_ideal_max_avail_pools(
                default_replica_size=pool_data[pool]["size"]
            )
            pool_max_avail = pool_data[pool]["max_avail"]
            is_within_variance = (
                lambda value, new_value, var: abs(value - new_value) / value <= variance
            )

            if not is_within_variance(ideal_max_avail, pool_max_avail, variance):
                log.error(
                    f"The MAX_AVAIL for pool : {pool} with size : {pool_data[pool]['size']} is not same as expected.\n"
                    f"Actual on cluster : {pool_max_avail}\n"
                    f"Calculated value : {ideal_max_avail}\n"
                )
                check_pass = False

            log.info(
                f"The MAX_AVAIL on the pool {pool} is as expected: {pool_max_avail}"
            )
        return check_pass

    def get_osd_stat(self):
        """
        This Function is to get the OSD stats.
           Example:
               get_osd_stat()
           Args:
           Returns:  OSD Statistics
        """

        cmd = "ceph osd stat"
        osd_stats = self.run_ceph_command(cmd=cmd)
        log.debug(f" The OSD Statistics are : {osd_stats}")
        return osd_stats

    def get_pgid(
        self,
        pool_name: str = None,
        pool_id: int = None,
        osd: int = None,
        osd_primary: int = None,
        states: str = None,
    ) -> list:
        """
        Retrieves all the PG IDs for a pool or PG IDs where a
        certain osd is primary in the acting set or PG IDs which are
        utilizing the concerned osd
        Args:
            pool_name: name of the pool
            pool_id: pool id
            osd: osd id whose pgs are to be retrieved
            osd_primary: primary osd id whose pgs are to be retrieved
            states: available choices: stale/creating/active/activating/clean/recovery_wait/recovery_toofull/
            recovering/forced_recovery/down/recovery_unfound/backfill_unfound/undersized/degraded/
            remapped/premerge/scrubbing/deep/inconsistent/peering/repair/backfill_wait/backfilling/
            forced_backfill/backfill_toofull/incomplete/peered/snaptrim/snaptrim_wait/snaptrim_error
        E.g:
            cph pg ls [<pool:int>] [<states>...]
            ceph pg ls-by-osd <id|osd.id> [<pool:int>] [<states>...]
            ceph pg ls-by-pool <poolstr> [<states>...]
            ceph pg ls-by-primary <id|osd.id> [<pool:int>] [<states>...]
            ceph pg ls 6 remapped
            ceph pg ls-by-pool repli_32 backfilling
            ceph pg ls-by-pool repli_32 clean
        Returns:
            list having pgids in string format
        """

        pgid_list = []
        cmd = "ceph pg "
        if pool_name:
            cmd += f"ls-by-pool {pool_name}"
        elif osd is not None:
            cmd += f"ls-by-osd {osd}"
            cmd = f"{cmd} {pool_id}" if pool_id else cmd
        elif osd_primary is not None:
            cmd += f"ls-by-primary {osd_primary}"
            cmd = f"{cmd} {pool_id}" if pool_id else cmd
        elif pool_id is not None:
            cmd += f"ls {pool_id}"
        else:
            log.info("No argument was provided.")
            return pgid_list

        if states:
            cmd = f"{cmd} {states}"

        pgid_dict = self.run_ceph_command(cmd=cmd, client_exec=True)

        if not pgid_dict["pg_stats"]:
            return []
        for pg_stats in pgid_dict["pg_stats"]:
            pgid_list.append(pg_stats["pgid"])

        return pgid_list

    def run_pool_sanity_check(self):
        """
        Runs sanity on the pools after triggering scrub and deep-scrub on pools, waiting 600 Secs

        This method is used to assess the health of Pools after any operation, where in a scrub and deep scrub is
        triggered, and the method scans the cluster for few health warnings, if generated

        Returns: True-> Pass,  false -> Fail
        """
        self.run_scrub()
        self.run_deep_scrub()
        time.sleep(10)

        end_time = datetime.datetime.now() + datetime.timedelta(seconds=1000)
        flag = False
        while end_time > datetime.datetime.now():
            status_report = self.run_ceph_command(cmd="ceph report", client_exec=True)
            ceph_health_status = status_report["health"]
            health_warns = (
                "PG_AVAILABILITY",
                "PG_DEGRADED",
                "PG_RECOVERY_FULL",
                "PG_BACKFILL_FULL",
                "PG_DAMAGED",
                "OSD_SCRUB_ERRORS",
                "OSD_TOO_MANY_REPAIRS",
                "CACHE_POOL_NEAR_FULL",
                "OBJECT_MISPLACED",
                "OBJECT_UNFOUND",
                "RECENT_CRASH",
            )

            flag = (
                False
                if any(
                    key in health_warns for key in ceph_health_status["checks"].keys()
                )
                else True
            )
            if flag:
                log.info("No warnings on the cluster")
                break

            log.info(
                f"Observing a health warning on cluster {ceph_health_status['checks'].keys()}"
            )
            time.sleep(10)

        if not flag:
            log.error(
                "Health warning generated on cluster and not cleared post waiting of 600 seconds"
            )
            return False

        log.info("Completed check on the cluster. Pass!")
        return True

    def create_n_az_stretch_pool(
        self,
        pool_name: str,
        rule_name: str,
        rule_id: int,
        peer_bucket_barrier: str = "datacenter",
        num_sites: int = 3,
        num_copies_per_site: int = 2,
        total_buckets: int = 3,
        req_peering_buckets: int = 2,
    ) -> bool:
        """Method to create a replicated pool and enable stretch mode on the pool

        Note: Most of the params have a default value. when created with defaults, pool is crated for 3AZ cluster,
         with 2 copies per site.
        Args:
             pool_name: name of the pool
             rule_id: rule ID
             rule_name: rule name
             peer_bucket_barrier: Crush level at which failures are accepted
             num_sites: number of "peer_bucket_barrier"s the data should be stored.
                eg : data has to be stored acorss 3 DCs. num_sites is 3
            num_copies_per_site: number of copies of data to be stored in each site
            total_buckets: total no of "peer_bucket_barrier" present on cluster.
                note: In most cases, total_buckets = num_sites. this changes when CU does not want each site to
                        hold data copy
            req_peering_buckets: number of "peer_bucket_barrier" buckets to perform successful peering process
        Returns:
            bool. Pass -> True, Fail -> False
        """

        # Creating test pool to check the effect of Netsplit scenarios on the Pool IO
        if not self.create_pool(pool_name=pool_name):
            log.error(f"Failed to create pool : {pool_name}")
            return False

        rules = f"""id {rule_id}
type replicated
step take default
step choose firstn {num_sites} type {peer_bucket_barrier}
step chooseleaf firstn {num_copies_per_site} type host
step emit"""
        log.debug(f"Rule to be added :\n {rules}\n")

        if not self.add_custom_crush_rules(rule_name=rule_name, rules=rules):
            log.error("Failed to add the new crush rule")
            return False

        size = num_sites * num_copies_per_site
        min_size = math.ceil(size / 2)

        # Enabling stretch mode on the pool
        if not self.enable_nsite_stretch_pool(
            pool_name=pool_name,
            peering_crush_bucket_count=req_peering_buckets,
            peering_crush_bucket_target=total_buckets,
            peering_crush_bucket_barrier=peer_bucket_barrier,
            crush_rule=rule_name,
            size=size,
            min_size=min_size,
        ):
            log.error(f"Unable to enable stretch mode on the pool : {pool_name}")
            return False
        log.info(
            f"Successfully created pool : {pool_name} and enabled stretch mode on the pool"
        )
        return True

    def get_multi_az_stretch_site_hosts(
        self, num_data_sites, stretch_bucket: str = "datacenter"
    ) -> tuple:
        """
        Method to get the site hosts from the stretch cluster
        Uses osd tree and mon dump commands to prepare a set of all the hosts from each DC.
        Args:
            num_data_sites: number of data sites in the cluster
            stretch_bucket: bucket level at which the stretch rules are set
        Returns:
            Hosts: A named tuple containing information about the hosts.
                - {site_name} (list): A list of hosts in the respective data center.
        """

        # Getting the CRUSH buckets added into the cluster via osd tree
        osd_tree_cmd = "ceph osd tree"
        buckets = self.run_ceph_command(cmd=osd_tree_cmd)
        dc_buckets = [d for d in buckets["nodes"] if d.get("type") == stretch_bucket]
        dc_names = [name["name"] for name in dc_buckets]
        log.debug(
            f"DC names obtained from OSD tree : {dc_names}, count : {len(dc_names)}"
        )

        # Dynamically create named tuple fields based on data center names (site names)
        fields = [dc["name"] for dc in dc_buckets[:num_data_sites]]

        # Create a namedtuple class dynamically based on the site names
        Hosts = namedtuple("Hosts", fields)

        # Initialize all fields with empty lists
        hosts = Hosts(**{field: [] for field in fields})

        # Fetching the Mon daemon placement in each CRUSH location
        def get_mon_from_dc(site_name) -> list:
            """
            Returns the list of dictionaries that are part of the site_name passed.
            Args:
                site_name: Name of the site, whose mons have to be fetched.
            Return:
                List of dictionaries that are present in a particular site.
            """
            mon_dump = "ceph mon dump"
            mons = self.run_ceph_command(cmd=mon_dump)
            site_mons = [
                d
                for d in mons["mons"]
                if d.get("crush_location")
                == "{" + stretch_bucket + "=" + site_name + "}"
            ]
            return site_mons

        for i in range(num_data_sites):
            dc = dc_buckets.pop()
            dc_name = dc["name"]  # Use the actual data center name (site name)
            osd_hosts = []

            # Fetching the OSD hosts of the DCs
            for crush_id in dc["children"]:
                for entry in buckets["nodes"]:
                    if entry.get("id") == crush_id:
                        osd_hosts.append(entry.get("name"))

            # Fetch MON hosts for the site
            dc_mons = [
                entry.get("name") for entry in get_mon_from_dc(site_name=dc_name)
            ]

            # Combine each DC's OSD & MON hosts and update the respective field in the namedtuple
            combined_hosts = list(set(osd_hosts + dc_mons))
            field_name = dc_name  # Use the site name as the field name

            # Using _replace to update the field
            hosts = hosts._replace(**{field_name: combined_hosts})

            log.debug(f"Hosts present in Datacenter : {dc_name} : {combined_hosts}")

        log.info(f"Hosts present in Cluster : {hosts}")
        return hosts

    def enable_nsite_stretch_pool(
        self,
        pool_name,
        peering_crush_bucket_count,
        peering_crush_bucket_target,
        peering_crush_bucket_barrier,
        crush_rule,
        size,
        min_size,
    ) -> bool:
        """
        Module to enable stretch mode on the pools in a multi AZ setup
        Args:
            pool_name: name of the pool
            peering_crush_bucket_count: number of buckets for peering to happen
            peering_crush_bucket_target: number of peering buckets
            peering_crush_bucket_barrier: CRUSH object used for various AZs
            crush_rule: name of the crush rule. Make sure the crush rule already exists on the cluster
            size: size for the pool
            min_size: min_size for the pool
        """
        cmd = (
            f"ceph osd pool stretch set {pool_name} {peering_crush_bucket_count} {peering_crush_bucket_target} "
            f"{peering_crush_bucket_barrier} {crush_rule} {size} {min_size}"
        )

        try:
            self.run_ceph_command(cmd=cmd)
            time.sleep(5)
            log.debug(f"Checking if the stretch mode op the pool : {pool_name}")
            cmd = f"ceph osd pool stretch show {pool_name}"
            out = self.run_ceph_command(cmd=cmd)
            log.debug(out)
            return True
        except Exception as err:
            log.error(
                f"hit exception while enabling/ checking stretch pool details. Error : {err}"
            )
            return False

    def add_custom_crush_rules(self, rule_name: str, rules: str) -> bool:
        """
        Adds the given crush rules into the crush map
        Args:
            rule_name: Name of the crush rule to add
            rules: The rules for crush
        Returns: True -> pass, False -> fail
        """
        try:
            # Getting the crush map
            cmd = "ceph osd getcrushmap > /tmp/crush.map.bin"
            self.client.exec_command(cmd=cmd, sudo=True)

            # changing it to text for editing
            cmd = "crushtool -d /tmp/crush.map.bin -o /tmp/crush.map.txt"
            self.client.exec_command(cmd=cmd, sudo=True)

            # Adding the crush rules into the file
            cmd = f"""cat <<EOF >> /tmp/crush.map.txt
rule {rule_name} {"{"}
{rules}
{"}"}
EOF"""
            log.debug(f"Command to add crush rules : \n {cmd} \n")
            self.client.exec_command(cmd=cmd, sudo=True)

            # Changing back the text file into bin
            cmd = "crushtool -c /tmp/crush.map.txt -o /tmp/crush2.map.bin"
            self.client.exec_command(cmd=cmd, sudo=True)

            # Setting the new crush map
            cmd = "ceph osd setcrushmap -i /tmp/crush2.map.bin"
            self.client.exec_command(cmd=cmd, sudo=True)

            time.sleep(5)

            out = self.run_ceph_command(cmd="ceph osd crush rule ls", client_exec=True)
            if rule_name not in out:
                log.error(
                    f"New rule added in the cluster is not listed in the cluster."
                    f"rule added : {rule_name}, \n"
                    f"rules present on cluster : {out}"
                )
                return False

            log.info(f"Crush rule: {rule_name} added successfully")
            return True
        except Exception as err:
            log.error("Failed to set the crush rules")
            log.error(err)
            return False

    def check_inactive_pgs_on_pool(self, pool_name) -> bool:
        """
        Method to check if the provided pool has any PGs in inactive state

        Args:
            pool_name: Name of the pool, on which inactive PGs should be checked

        Returns: True-> Pass,  false -> Fail
        """
        log.debug(f"Checking for inactive PGs on pool : {pool_name}")
        pool_pgids = self.get_pgid(pool_name=pool_name)
        for pgid in pool_pgids:
            # Checking the PG state. There Should not be inactive state
            pg_state = self.get_pg_state(pg_id=pgid)
            if pg_state:
                if any("unknown" in key for key in pg_state.split("+")):
                    log.error(f"PG: {pgid} in inactive state)")
                    return False
            else:
                log.error(f"PG : {pgid} not present on cluster")
                continue
        log.info(
            f"Completed checking for inactive PGs on Pool : {pool_name}. No inactive PGs found"
        )
        return True

    def get_osd_hosts(self):
        """
        lists the names of the OSD hosts in the cluster
        Returns: list of osd host names as used in the crush map

        """
        cmd = "ceph osd tree"
        osds = self.run_ceph_command(cmd)
        return [entry["name"] for entry in osds["nodes"] if entry["type"] == "host"]

    def change_heap_profiler_state(self, osd_list, action) -> tuple:
        """
        Start/stops the OSD heap profile
        Usage: ceph tell osd.<osd.ID> heap start_profiler
               ceph tell osd.<osd.ID> heap stop_profiler
        Args:
             osd_list: The list with the osd IDs
             action : start  or stop actions for heap profiler
        Return: tuple of exit status with the OSD list
        eg : (1, []) -> Fail
        (0, [1,2,3,4,5]) -> Pass
        """
        if not osd_list:
            log.error("OSD list is empty")
            return 1, []
        for osd_id in osd_list:
            osd_status, status_desc = self.get_daemon_status(
                daemon_type="osd", daemon_id=osd_id
            )
            if not (osd_status == 0 or status_desc == "stopped"):
                log.info(
                    f"OSD {osd_id} is in running state, enabling/Disabling Heap profiler"
                )
                cmd = f"ceph tell osd.{osd_id} heap {action}_profiler"
                self.node.shell([cmd])
            else:
                log.error(
                    f"OSD {osd_id} in stopped state. Not enabling/disabling the heap profiler on the OSD"
                )
                osd_list.remove(osd_id)
        log.info(f"The OSD {osd_list} heap profile is in {action} state")
        return 0, osd_list

    def get_heap_dump(self, osd_list):
        """
        Returns the heap dump of the all OSDs in the osd_list
        Usage: ceph tell osd.<osd.ID> heap dump
        Example:
             get_heap_dump(osd_list)
             where osd_list is the list of OSD ids like[1,2,4]
        Args:
            osd_list: The list with the osd IDs
        Return :
            A dictionary output with the key as OSD id and values are the
            heap dump of the OSD.
        """
        if not osd_list:
            log.error("OSD list is empty")
            return 1
        heap_dump = {}
        for osd_id in osd_list:
            cmd = f"ceph tell osd.{osd_id} heap dump"
            out, err = self.node.shell([cmd])
            heap_dump[osd_id] = out.strip()
        return heap_dump

    def list_orch_services(self, service_type=None, export=None) -> list:
        """
        Retrieves the list of orch services
        Args:
            service_type(optional): service name | e.g. mon, mgr, osd, etc
            export(optional): return export of orch service
        Returns:
            list of service names using ceph orch ls [<service>] [--export]
        """
        base_cmd = "ceph orch ls"

        cmd = f"{base_cmd} {service_type}" if service_type else base_cmd
        cmd = f"{cmd} --export" if export else cmd
        orch_ls_op = self.run_ceph_command(cmd=cmd, client_exec=True)

        if export:
            return orch_ls_op

        if orch_ls_op:
            return [service["service_name"] for service in orch_ls_op]

    def check_host_status(self, hostname, status: str = None) -> bool:
        """
        Checks the status of host(offline or online) using
        ceph orch host ls and return boolean
        Args:
            hostname: hostname of host to be checked
            status: custom status check for the host
        Returns:
            (bool) True -> online | False -> offline
        """
        host_cmd = f"ceph orch host ls --host_pattern {hostname}"
        out = self.run_ceph_command(cmd=host_cmd, client_exec=True)
        host_status = out[0]["status"].lower().strip()
        log.info(f"Status of the host is {host_status}")
        if status:
            return True if status.lower() == host_status else False
        elif "offline" in host_status:
            return False
        return True

    def run_concurrent_io(self, pool_name: str, obj_name: str, obj_size: int):
        """
        Use rados put to perform concurrent IOPS on a particular object in a pool.
        Args:
            pool_name: name of the pool
            obj_name: name of the object
            obj_size: size of the object in MB
        """
        obj_name = f"{obj_name}_{obj_size}"
        installer_node = self.ceph_cluster.get_nodes(role="installer")[0]
        put_cmd = f"rados put -p {pool_name} {obj_name} /mnt/sample_1M"
        out, _ = installer_node.exec_command(
            sudo=True, cmd="truncate -s 1M ~/sample_1M"
        )
        out, _ = self.client.exec_command(
            sudo=True, cmd="truncate -s 1M /mnt/sample_1M"
        )

        def rados_put_installer(installer_offset=1048576):
            for i in range(int(obj_size / 2)):
                inst_put_cmd = f"{put_cmd} --offset {installer_offset}"
                self.node.shell(
                    args=[inst_put_cmd],
                    base_cmd_args={"mount": "~/sample_1M"},
                    check_status=False,
                )
                installer_offset = installer_offset + 2097152

        def rados_put_client(client_offset=0):
            for i in range(int(obj_size / 2)):
                client_put_cmd = f"{put_cmd} --offset {client_offset}"
                self.client.exec_command(sudo=True, cmd=client_put_cmd, check_ec=False)
                client_offset = client_offset + 2097152

        with parallel() as p:
            p.spawn(rados_put_client)
            p.spawn(rados_put_installer)

    def run_parallel_io(self, pool_name: str, obj_name: str, obj_size: int):
        """
        Use rados put to perform parallel IOPS on a particular object in a pool.
        Args:
            pool_name: name of the pool
            obj_name: name of the object
            obj_size: size of the object in MB
        """
        obj_name = f"{obj_name}_{obj_size}"
        installer_node = self.ceph_cluster.get_nodes(role="installer")[0]
        try:
            out, rc = installer_node.exec_command(
                sudo=True, cmd="rpm -qa | grep ceph-common"
            )
        except Exception:
            installer_node.exec_command(
                sudo=True, cmd="yum install -y ceph-common --nogpgcheck"
            )

        put_cmd = "rados put -p $pool_name $obj_name ~/sample_1M --offset $offset"
        loop_cmd = (
            f"for ((i=1 ; i<=$END ; i++));"
            f"do {put_cmd}; export offset=$(($offset + 2097152));"
            f"done"
        )

        export_cmd = (
            f"export pool_name={pool_name} obj_name={obj_name} END={int(obj_size / 2)}"
        )
        inst_run_cmd = f"{export_cmd}; export offset=1048576; {loop_cmd}"
        client_run_cmd = f"{export_cmd}; export offset=0; {loop_cmd}"

        out, _ = installer_node.exec_command(
            sudo=True, cmd="truncate -s 1M ~/sample_1M"
        )
        out, _ = self.client.exec_command(sudo=True, cmd="truncate -s 1M ~/sample_1M")

        log.info(f"Running cmd: {client_run_cmd} on {self.client.hostname}")
        self.client.exec_command(sudo=True, cmd=client_run_cmd, check_ec=False)
        log.info(f"Running cmd: {inst_run_cmd} on {installer_node.hostname}")
        installer_node.exec_command(sudo=True, cmd=inst_run_cmd)

    def get_fragmentation_score(self, osd_id) -> float:
        """
        Retrieves and returns the fragmentation score for a particular osd
        Args:
            osd_id: OSD ID
        Return:
            (float) fragmentation score for the given OSD
        """
        # fragmentation scores for OSD
        frag_cmd = f"ceph tell osd.{osd_id} bluestore allocator score block"
        return self.run_ceph_command(cmd=frag_cmd)["fragmentation_rating"]

    def check_fragmentation_score(self, osd_id) -> bool:
        """
        Checks whether fragmentation score of the given osd is within
        acceptable range (below 0.9)
        Args:
             osd_id: OSD ID
        Return:
            True -> pass, False -> Fail
        """
        log.info(f"Checking the Fragmentation score for OSD.{osd_id}")
        frag_score = self.get_fragmentation_score(osd_id=osd_id)
        log.info(f"Fragmentation score for the OSD.{osd_id} : {frag_score}")

        if 0.9 < float(frag_score) < 1.0:
            log.error(
                f"Fragmentation on osd {osd_id} is dangerously high."
                f"Ideal range 0.0 to 0.7. Actual fragmentation on OSD.{osd_id}: {frag_score}"
            )
            return False
        return True

    def get_stretch_mode_dump(self) -> dict:
        """
        retrieves the dump values for the stretch mode from the osd dump

        Return:
            Dict with the stretch mode details
            {
                'stretch_mode_enabled': False,
                'stretch_bucket_count': 0,
                'degraded_stretch_mode': 0,
                'recovering_stretch_mode': 0,
                'stretch_mode_bucket': 0
            }
        """
        cmd = "ceph osd dump"
        osd_dump = self.run_ceph_command(cmd=cmd, client_exec=True)
        stretch_details = osd_dump["stretch_mode"]
        log.debug(f"Stretch mode dump : {stretch_details}")
        return stretch_details

    def get_ceph_pg_dump(self, pg_id: str) -> dict:
        """
        Fetches 'ceph pg dump' in json format and returns the data
        for input PG
        Args:
            pg_id: Placement Group ID for which pg dump has to be fetched

        Returns: dictionary output of ceph pg dump for input PG ID
        """
        _cmd = "ceph pg dump_json pgs"
        dump_out_str, _ = self.client.exec_command(cmd=_cmd)
        if dump_out_str.isspace():
            return {}
        dump_out = json.loads(dump_out_str)
        pg_stats = dump_out["pg_map"]["pg_stats"]
        for pg_stat in pg_stats:
            if pg_stat["pgid"] == pg_id:
                return pg_stat

        log.error(f"PG {pg_id} not found in ceph pg dump output")
        raise KeyError(f"PG {pg_id} not found in ceph pg dump output")

    def get_ceph_pg_dump_pools(self, pool_id: any) -> dict:
        """
        Fetches 'ceph pg dump pools' in json format and returns the data
        for input PG
        Args:
            pool_id: ID of the pool for which pg dump has to be fetched

        Returns: dictionary output of ceph pg dump for input PG ID
        """
        # cmd if manual run : "ceph pg dump pools"
        _cmd = "ceph pg dump_pools_json"
        pool_dump_str, _ = self.client.exec_command(cmd=_cmd)
        if pool_dump_str.isspace():
            return {}
        dump_json = json.loads(pool_dump_str)
        pool_stats = dump_json["pool_stats"]
        for pool_stat in pool_stats:
            if pool_stat["poolid"] == pool_id:
                return pool_stat

        log.error(f"Pool ID {pool_id} not found in 'ceph pg dump pools' output")
        raise KeyError(f"Pool ID {pool_id} not found in 'ceph pg dump pools' output")

    def restart_daemon_services(self, daemon: str):
        """Module to restart all Orchestrator services belonging to the input
        daemon.
        Args:
            daemon (str): name of daemon whose service has to be restarted
        Returns:
            True -> Orch service restarted successfully.

            False -> One or more daemons part of the service could not restart
            within timeout
        """
        daemon_map = dict()
        success = False
        daemon_services = self.list_orch_services(service_type=daemon)
        # capture current start time for each daemon part of the services
        for service in daemon_services:
            daemon_status_ls = self.run_ceph_command(
                cmd=f"ceph orch ps --service_name {service} --refresh"
            )
            for entry in daemon_status_ls:
                start_time, _ = self.client.exec_command(
                    cmd=f"date -d {entry['started']} +'%Y%m%d%H%M%S'"
                )
                daemon_map[entry["daemon_name"]] = start_time

        # restart each service for the input daemon
        for service in daemon_services:
            self.client.exec_command(cmd=f"ceph orch restart {service}", sudo=True)

        end_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
        # wait for each daemon to restart
        for service in daemon_services:
            while datetime.datetime.now() <= end_time:
                daemon_status_ls = self.run_ceph_command(
                    cmd=f"ceph orch ps --service_name {service} --refresh"
                )
                for entry in daemon_status_ls:
                    try:
                        restart_time, _ = self.client.exec_command(
                            cmd=f"date -d {entry['started']} +'%Y%m%d%H%M%S'"
                        )
                        assert restart_time > daemon_map[entry["daemon_name"]]
                        assert entry["status_desc"] != "stopped"
                        log.info(f"{entry['daemon_name']} has started")
                        success = True
                    except AssertionError:
                        log.info(
                            f"{daemon} daemon {entry['daemon_name']} is yet to restart. "
                            f"Sleeping for 30 secs"
                        )
                        self.client.exec_command(
                            cmd=f"ceph orch daemon restart {entry['daemon_name']}",
                            sudo=True,
                        )
                        time.sleep(30)
                        success = False
                        break
                if success:
                    break
            else:
                log.error(
                    f"All the daemons part of the service {service} did not restart within "
                    f"timeout of 5 mins"
                )
                return False

        log.info(f"Ceph Orch Service(s) {daemon_services} has been restarted")
        return True

    def check_pool_pg_states(self, pool: str, disallowed_states: list):
        """
        the method fetches the states for the pg belonging to particular pool.
        If any of the PG in the pool, have states that are in the disallowed states list, returns fail

        Args::
            pool: Name of the pool whose pg states need to be checked
            disallowed_states: list of pg states, that are not allowed, if found, module returns fail

        Returns::
            Pass -> True
            Fail -> False
        """
        # fetching the pool ID
        cmd = "ceph df"
        out = self.run_ceph_command(cmd=cmd)
        pool_names = [entry["name"] for entry in out.get("pools", [])]
        pool_id = out["pools"][pool_names.index(pool)]["id"]

        cmd = f" ceph pg ls {pool_id}"
        out = self.run_ceph_command(cmd=cmd)
        for ele in out["pg_stats"]:
            if any(key in disallowed_states for key in ele["state"].split("+")):
                log.error(
                    f"PG : {ele['pgid']} is in state : {ele['state']}. "
                    "PG expected to be active+clean"
                )
                return False
            else:
                log.info(f"PG : {ele['pgid']} is in expected state : {ele['state']}. ")
        log.info("Completed checking PG states on all PGs of the pool. Pass")
        return True

    def get_object_list(self, pool_name) -> list:
        """
        Method retrives the objects form the pool
        Args:
            pool_name: pool name that the objects are created

        Returns: List of objects that exists in the pool
        """
        cmd_get_obj_list = f"rados -p {pool_name} ls"
        out_put = self.run_ceph_command(cmd=cmd_get_obj_list)

        obj_list = []
        if not out_put:
            log.info(f"Objects not exists in the provided {pool_name} pool")
        else:
            for omap_obj in out_put:
                obj_list.append(omap_obj["name"])
        return obj_list

    def get_object_key_list(self, osd_id, pg_id, object_name):
        """
        Method returns the key list of an object
        Args:
            osd: osd id number
            pg_id: pg id number
            object_name: object name

        Returns: List of keys mapped to that object

        """
        cmd_base = f"cephadm shell --name osd.{osd_id} --"
        acting_osd_node = self.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
        cmd_get_obj_key = (
            f"{cmd_base} ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-"
            f"{osd_id} --pgid {pg_id} {object_name}  list-omap"
        )
        out_put = acting_osd_node.exec_command(sudo=True, cmd=cmd_get_obj_key)
        key_list = list(filter(None, out_put[0].split("\n")))
        return key_list

    def rm_object_key(self, osd_id, pg_id, object_name, object_key):
        """
        Method removes the object key
        Args:
            osd_id: osd id number
            pg_id: pg id number
            object_name: object name
            object_key: object key to remove that mapped to the object

        Returns: True -> Key deleted False -> Key not deleted
        """
        cmd_base = f"cephadm shell --name osd.{osd_id} --"
        acting_osd_node = self.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
        cmd_rm_obj_key = (
            f"{cmd_base} ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-"
            f"{osd_id} --pgid {pg_id} {object_name}  rm-omap {object_key}"
        )
        try:
            acting_osd_node.exec_command(sudo=True, cmd=cmd_rm_obj_key)
        except Exception:
            log.error(
                f"{object_key} object key is not removed for the {object_name} object"
            )
            return False
        log.info(f"{object_key} object key is removed for the {object_name} object")
        return True

    def get_inconsistent_pg_list(self, pool_name):
        """
        Method returns the inconsistent pg list
        Args:
            pool_name:  pool name

        Returns: Inconsistent pg list
        """
        cmd_inconsist_pg = f"rados list-inconsistent-pg  {pool_name}"
        inconsistent_pg_list = self.run_ceph_command(cmd_inconsist_pg)
        if not inconsistent_pg_list:
            log.info(f"The inconsistent pg list is empty in the pool {pool_name}")
        return inconsistent_pg_list

    def get_inconsistent_object_details(self, pg_id):
        """
        Method returns the inconsistent object list from a PG
        Args:
            pg_id: pg id

        Returns: Return the inconsistent object list
        """
        cmd_inconsistent_object = f"rados  list-inconsistent-obj  {pg_id}"
        object_details = self.run_ceph_command(cmd_inconsistent_object)
        if not object_details:
            log.info(f"The inconsistent objects list is empty in the PG {pg_id}")
        return object_details

    def check_inconsistent_health(self, inconsistent_present: bool = True):
        """
        Method perform the health check for the inconsistent objects
        Args:
            inconsistent_present : Flag for Checking inconsistent pg present or not

        Returns: True -> Contain inconsistent objects
                 False -> Not contain inconsistent objects
        """
        # Check in the cluster maximum 10 minutes
        time_check = 600
        start_time = time.time()
        while time.time() - start_time < time_check:
            health_detail = self.node.shell(args=["ceph health detail"])
            log.info(f"Health warning on cluster: \n {health_detail} \n\n")
            status_report = self.run_ceph_command(cmd="ceph report", client_exec=True)
            ceph_health_status = list(status_report["health"]["checks"].keys())
            health_warn = "PG_DAMAGED"
            if health_warn not in ceph_health_status and inconsistent_present is False:
                log.info("pg inconsistent not exists in the cluster")
                return True
            elif health_warn in ceph_health_status and inconsistent_present is True:
                log.info("pg inconsistent generated in the cluster")
                return True
            else:
                log.info("Waiting for the correct status in the cluster")
                time.sleep(30)
        if inconsistent_present is True:
            log.error("Failed to generate pg inconsistent in the cluster")
            return False
        elif inconsistent_present is False:
            log.error("Failed to clean pg inconsistent in the cluster")
            return False

    def create_inconsistent_object(self, pool_name, object_name, num_keys: int = 3):
        """
        The method converts the object into inconsistent object
        The logic implemented in the code is-
        1. Get the primary osd and pg_id
        2. Stopping the OSD
        3. Get the key list of the object
        4. Remove few keys that is mapped to the object
        5. Start the OSD and perform deep-scrub on that pg id
        6. Check the status
        Args:
            pool_name: pool name
            object_name: object name in the pool
            num_keys: number of keys to be deleted for inconsistent object to be generated
        Returns: After converting the object in to inconsistent,method returns the pg id
        """
        osd_map_output = self.get_osd_map(pool=pool_name, obj=object_name)
        primary_osd = osd_map_output["acting_primary"]
        log.info(f"The object stored in the primary osd number-{primary_osd}")
        pg_id = osd_map_output["pgid"]
        log.info(f"The object {object_name} is created in the pg-{pg_id}")

        # stopping the OSD
        if not self.change_osd_state(action="stop", target=primary_osd):
            log.error(f"Unable to stop the OSD : {primary_osd}")
            raise Exception("Execution error")
        log.debug(f"Stopped OSD : {primary_osd} to create inconsistent object")
        time.sleep(5)
        # Getting the key list
        key_list = self.get_object_key_list(primary_osd, pg_id, object_name)
        log.debug(f"Key list before deletion : {key_list}")
        rm_key_count = 0
        deleted_keys = []
        for obj_key in key_list:
            log.info(f" Deleting the key :{obj_key} in the {object_name} object")
            self.rm_object_key(primary_osd, pg_id, object_name, obj_key)
            time.sleep(5)
            new_key_list = self.get_object_key_list(primary_osd, pg_id, object_name)
            if obj_key in new_key_list:
                log.error(
                    f"The key:{obj_key} from object:{object_name} could not be deleted. Fail"
                )
                raise Exception("Object key not deleted error")
            rm_key_count += 1
            deleted_keys.append(obj_key)
            log.debug(
                f"Successfully Deleted the {obj_key} from object: {object_name}"
                f"Total keys deleted on the object : {rm_key_count}"
                f"Keys removed : {deleted_keys}"
            )
            if rm_key_count == num_keys:
                break
        log.debug(
            f"Done with deleting KW pairs on the OSD : {primary_osd} for Obj : object_name"
        )
        key_list = self.get_object_key_list(primary_osd, pg_id, object_name)
        log.debug(f"Key list After deletion : {key_list}")
        if not self.change_osd_state(action="start", target=primary_osd):
            log.error(f"Unable to start the OSD : {primary_osd}")
            raise Exception("OSD could not be started error")
        log.debug(
            f"Started the OSD: {primary_osd} and performing deep scrubs on the PG"
        )
        log.info(f"Performing the deep-scrub on the pg-{pg_id}")
        if not self.start_check_deep_scrub_complete(pg_id=pg_id):
            log.debug(f"deep-scrubbing could not be completed on PG : {pg_id}")
            raise Exception("PG not deep-scrubbed error")
        log.debug(f"Completed deep-scrubbing the pg : {pg_id}")
        # sleeping for 10 seconds
        time.sleep(10)
        assert self.check_inconsistent_health(inconsistent_present=True)
        log.info(f"The inconsistent object is created in the pg: {pg_id}")
        return pg_id

    def start_check_scrub_complete(
        self, pg_id, user_initiated: bool = True, wait_time: int = 900
    ):
        """
        Initiates scrubbing on the PG provided and waits until the scrubbing is complete.

        """

        init_pool_pg_dump = self.get_ceph_pg_dump(pg_id=pg_id)
        log.info("Dumping scrub stats before starting scrub")
        log.info(f"last_scrub : {init_pool_pg_dump['last_scrub']}")
        log.info(f"last_scrub_stamp: {init_pool_pg_dump['last_scrub_stamp']}")

        # Parse the timestamp string into a datetime object
        init_scrub_stamp = datetime.datetime.strptime(
            init_pool_pg_dump["last_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f%z"
        )
        if user_initiated is True:
            # Running scrub on the PG provided
            log.debug(f"Initiating scrubbing on pg : {pg_id}")
            self.run_scrub(pgid=pg_id)
            time.sleep(2)
        else:
            log.debug("Initiated scheduled scrub")

        start_time = datetime.datetime.now()
        while datetime.datetime.now() <= start_time + datetime.timedelta(
            seconds=wait_time
        ):
            pool_pg_dump = self.get_ceph_pg_dump(pg_id=pg_id)
            current_scrub_stamp = datetime.datetime.strptime(
                pool_pg_dump["last_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f%z"
            )
            if current_scrub_stamp > init_scrub_stamp:
                log.info(f"Scrubbing complete on the PG: {pg_id}")
                log.debug(f"Final last_scrub: {pool_pg_dump['last_scrub']}")
                log.debug(f"Final last_scrub_stamp: {pool_pg_dump['last_scrub_stamp']}")
                log.debug(
                    f"Total time taken for scrubbing to complete on the pg : {pg_id} is"
                    f" {current_scrub_stamp - init_scrub_stamp}"
                )
                return True
            else:
                log.debug(f"Current last_scrub: {pool_pg_dump['last_scrub']}")
                log.debug(
                    f"Current last_scrub_stamp: {pool_pg_dump['last_scrub_stamp']}"
                )
                log.debug(
                    f"Total time elapsed since starting scrub on PG: {pg_id} is"
                    f" {current_scrub_stamp - init_scrub_stamp}"
                )
                log.info(
                    f"scrub is yet to complete, pg state: {pool_pg_dump['state']}. Sleeping for 30 secs"
                )
                time.sleep(30)
        else:
            log.error(f"PG :{pg_id} could not be scrubbed in time")
            raise Exception("Objects not scrubbed error")

    def start_check_deep_scrub_complete(
        self, pg_id, user_initiated: bool = True, wait_time: int = 900
    ):
        """
        Initiates deep-scrubbing on the PG provided and waits until the deep-scrubbing is complete.

        """

        init_pool_pg_dump = self.get_ceph_pg_dump(pg_id=pg_id)
        log.info("Dumping deep-scrub stats before starting deep-scrub")
        log.info(f"last_deep_scrub : {init_pool_pg_dump['last_deep_scrub']}")
        log.info(f"last_deep_scrub_stamp: {init_pool_pg_dump['last_deep_scrub_stamp']}")

        # Parse the timestamp string into a datetime object
        init_scrub_stamp = datetime.datetime.strptime(
            init_pool_pg_dump["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f%z"
        )

        if user_initiated:
            # Running deep-scrub on the PG provided
            log.debug(f"Initiating deep-scrubbing on pg : {pg_id}")
            self.run_deep_scrub(pgid=pg_id)
            time.sleep(2)
        else:
            log.debug("Initiated scheduled deep-scrub")

        start_time = datetime.datetime.now()
        while datetime.datetime.now() <= start_time + datetime.timedelta(
            seconds=wait_time
        ):
            pool_pg_dump = self.get_ceph_pg_dump(pg_id=pg_id)
            # Parse the timestamp string into a datetime object
            current_scrub_stamp = datetime.datetime.strptime(
                pool_pg_dump["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f%z"
            )
            if current_scrub_stamp > init_scrub_stamp:
                log.info(f"Scrubbing complete on the PG: {pg_id}")
                log.debug(f"Final last_deep_scrub: {pool_pg_dump['last_deep_scrub']}")
                log.debug(
                    f"Final last_deep_scrub_stamp: {pool_pg_dump['last_deep_scrub_stamp']}"
                )
                log.debug(
                    f"Total time taken for scrubbing to complete on the pg : {pg_id} is"
                    f" {current_scrub_stamp - init_scrub_stamp}"
                )
                return True
            else:
                log.debug(f"Current last_deep_scrub: {pool_pg_dump['last_deep_scrub']}")
                log.debug(
                    f"Current last_deep_scrub_stamp: {pool_pg_dump['last_deep_scrub_stamp']}"
                )
                log.debug(
                    f"Total time elapsed since starting deep-scrub on PG: {pg_id} is"
                    f" {current_scrub_stamp - init_scrub_stamp}"
                )
                log.info(
                    f"Deep-scrub is yet to complete, pg state: {pool_pg_dump['state']}. Sleeping for 30 secs"
                )
                time.sleep(30)
        else:
            log.error(f"PG : {pg_id} could not be deep-scrubbed in time")
            raise Exception("Objects not scrubbed error")

    def crash_ceph_daemon(self, daemon: str, id, manual_inject: bool = False):
        """
        Module to crash any existing daemon on the cluster
        Args:
            daemon: daemon type | ex - mon, mgr, osd
            id: daemon ID | ex - 12, mon/mgr hostname
            manual_inject: flag to control manual injection of crash meta, waits
            for automatic detection of crash if not enabled
        Returns: True if crash was successfully generated | False otherwise
        """

        def warning_check(timeout):
            # wait for input timeout secs for crash to be reported in ceph health
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
            while datetime.datetime.now() < end_time:
                try:
                    health, _ = self.node.shell(args=["ceph health detail"])
                    log.info(f"Health warning: \n {health}")
                    assert "daemons have recently crashed" in health
                    assert f"{daemon}.{id} crashed" in health
                    return True
                except AssertionError:
                    log.error(
                        "Crash error is yet to appear in ceph health. Sleeping for 30 secs"
                    )
                    time.sleep(30)
                    if datetime.datetime.now() >= end_time:
                        log.error("daemon crash warning not found within timeout")
                        return False

        # check if input daemon exists on the cluster
        status, desc = self.get_daemon_status(daemon_type=daemon, daemon_id=id)
        if int(status) != 1 or "running" not in desc:
            log.error(
                f"Input daemon {daemon}.{id} either does not exist or "
                f"currently not running."
            )
            return False

        # fetch daemon host
        daemon_host = self.fetch_host_node(daemon_type=daemon, daemon_id=id)

        # ensure crash module is enabled and always on
        always_on_list = self.run_ceph_command("ceph mgr module ls")[
            "always_on_modules"
        ]
        log.debug(f"MGR always ON modules: \n {always_on_list}")
        if "crash" not in always_on_list:
            log.error("Crash module found in MGR always on module list")
            return False

        # capture initial number of crash reports in the /var/lib/ceph/crash directory
        _cmd = "cephadm shell -- ls /var/lib/ceph/crash/ | wc -l"
        init_crash_reports, err = daemon_host.exec_command(cmd=_cmd, sudo=True)
        # capture initial number of crash list from crash ls-new
        init_crash_ls = len(self.run_ceph_command("ceph crash ls-new"))
        log.debug(f"Output of crash ls-new: {init_crash_ls}")
        # generate crash using ceph daemon command
        asok_cmd = (
            f"cephadm shell -- ceph daemon /var/run/ceph/ceph-{daemon}.{id}.asok assert"
        )
        try:
            out, err = daemon_host.exec_command(cmd=asok_cmd, sudo=True)
        except Exception as err_exec:
            if "exception" not in str(err_exec):
                log.error(f"STDERR: {err_exec}")
                log.error("Could not crash the daemon with ceph daemon utility")
                return False

        crash_report_post, err = daemon_host.exec_command(cmd=_cmd, sudo=True)
        if not crash_report_post > init_crash_reports:
            log.error("New crash report directory not generated in /var/lib/ceph/crash")
            return False
        log.info("New crash report generated successfully")

        # get the crash report directory
        _cmd = "cephadm shell -- ls -rt /var/lib/ceph/crash/ | tail -1"
        dir, err = daemon_host.exec_command(cmd=_cmd, sudo=True)
        dir = str(dir).strip()

        # copy the crash directory to tmp
        fsid = self.run_ceph_command(cmd="ceph fsid")["fsid"]
        _cmd = f"cp -r /var/lib/ceph/{fsid}/crash/'{dir}' /tmp/"
        daemon_host.exec_command(cmd=_cmd, sudo=True)

        if manual_inject:
            # inject the crash into the cluster manually
            self.configure_host_as_client(daemon_host)
            inject_cmd = f"ceph crash post -i /tmp/{dir}/meta"
            daemon_host.exec_command(sudo=True, cmd=inject_cmd)
            if not warning_check(timeout=100):
                log.error("Crash warning not found even after manual injection")
                return False
        else:
            # wait for 900 secs for crash to be reported in ceph health
            if not warning_check(timeout=900):
                log.error("Crash warning was not generated automatically")

        # ensure crash detail is populated in crash ls-new
        crash_ls_post = self.run_ceph_command("ceph crash ls-new")
        if not len(crash_ls_post) > init_crash_ls:
            log.error("New crash not listed in crash ls-new output")
            return False
        log.debug(
            f"Output of crash ls-new: {crash_ls_post}" f"\n Count: {len(crash_ls_post)}"
        )

        log.info("Daemon crash warning found in ceph health")
        return True

    def add_network_delay_on_host(
        self, hostname, delay="20ms", packet_loss="0.1", set_delay=True
    ) -> bool:
        """
        This method makes use of tc utility to introduce network delays and packet drops on the nodes.

        Args::
            hostname: Name of the host
            delay: delay to be added on the host
            packet_loss: percentage of packets to be dropped
            set_delay: If true, sets the network delay, otherwise removes the delay added on the host

        Return:
            Pass -> True
            Fail -> False
        """
        host_nodes = self.ceph_cluster.get_nodes()
        host_obj = None
        for node in host_nodes:
            if (
                re.search(hostname, node.hostname)
                or re.search(hostname, node.vmname)
                or re.search(hostname, node.shortname)
            ):
                host_obj = node
        if not host_obj:
            log.error(f"Host object for host {hostname} could not be found")
            return False

        # Checking and installing iproute package on node
        try:
            host_obj.exec_command(sudo=True, cmd="rpm -qa | grep iproute")
        except Exception:
            host_obj.exec_command(sudo=True, cmd="yum install iproute -y")
        log.debug("IProute package is present on the host")

        log.info(
            "Getting the network interface on the host to add network delay & packet loss"
        )
        interface = host_obj.search_ethernet_interface(host_nodes)
        log.debug(f"Fetched interface for host is : {interface}")

        if set_delay:
            log.debug(
                "Removing any netem configurations if already present on host before setting new configs"
            )
            rm_cmd = f"sudo tc qdisc del dev {interface} root netem"
            try:
                host_obj.exec_command(sudo=True, cmd=rm_cmd)
                time.sleep(2)
            except Exception:
                log.debug(
                    "No configs already present on the host. Proceeding to set the rules"
                )

            log.debug(
                f"Adding the network delay on the host : {hostname} on interface : {interface}."
                f" delay : {delay}, Packet loss: {packet_loss}%"
            )
            delay_cmd = f"tc qdisc add dev {interface} root netem delay {delay} loss {packet_loss}%"

        if not set_delay:
            log.debug(
                f"Removing the network delay & packet loss on the host : {hostname} on interface : {interface}."
            )
            delay_cmd = f"sudo tc qdisc del dev {interface} root netem"

        try:
            host_obj.exec_command(sudo=True, cmd=delay_cmd)
            time.sleep(2)
        except Exception as err:
            log.error(
                f"Hit Exception while running the tc utility commands. Error : {err}"
            )
            return False

        display_cmd = f"tc qdisc show dev {interface}"
        out, err = host_obj.exec_command(sudo=True, cmd=display_cmd)
        log.debug(f"configured network delay & drop settings are : {out} ")
        log.info("Completed setting/unsetting the network delay and packet drops")
        return True

    def configure_host_as_client(self, host_node):
        """
        Purpose of this module is to configure the ceph keyring and conf
        on a cluster host which is not installer to run ceph commands from
        it
        Args:
            host_node: node object of the host which needs to be converted
        Returns: None
        """
        installer_node = self.ceph_cluster.get_nodes(role="installer")[0]
        # copy /etc/ceph/ files to input host
        ceph_conf, _ = installer_node.exec_command(
            cmd="cat /etc/ceph/ceph.conf", sudo=True
        )
        ceph_keyring, _ = installer_node.exec_command(
            cmd="cat /etc/ceph/ceph.client.admin.keyring", sudo=True
        )
        host_node.exec_command(sudo=True, cmd="mkdir -p /etc/ceph")

        for cont in [ceph_conf, ceph_keyring]:
            file_name = (
                "/etc/ceph/ceph.conf"
                if cont == ceph_conf
                else "/etc/ceph/ceph.client.admin.keyring"
            )
            file_ = host_node.remote_file(sudo=True, file_name=file_name, file_mode="w")
            file_.write(cont)
            file_.flush()
            file_.close()

        # Checking and installing ceph-common package on host
        try:
            out, rc = host_node.exec_command(
                sudo=True, cmd="rpm -qa | grep ceph-common"
            )
        except Exception:
            host_node.exec_command(
                sudo=True, cmd="yum install -y ceph-common --nogpgcheck"
            )

    def fetch_osd_status(self, _osd_id) -> str:
        """
        Return the status of input osd from ceph osd tree output
        Args:
            _osd_id: ID of OSD for which status if to be fetched
        Returns:
            status of the input OSD in string format
        """
        osd_found = False
        # check flag value in ceph osd tree output for chosen OSD
        try:
            osd_tree_out = self.run_ceph_command(cmd="ceph osd tree")["nodes"]
            for value in osd_tree_out:
                if int(value["id"]) == int(_osd_id):
                    osd_found = True
                    break
            if not osd_found:
                log.error("Input OSD not found in ceph osd tree output")
                raise Exception("Input OSD not found in ceph osd tree output")
            return value["status"]
        except Exception:
            raise

    def change_daemon_systemctl_state(
        self, action, daemon_type: str, daemon_id: str, timeout=6000
    ):
        """
        Method to Start, stop & Restart any ceph daemons using systemctl commands
        Args:
            action: operation to be performed on the service, i.e. start, stop, restart
            daemon_type: name of ceph service type (mon, mgr,osd ...)
            daemon_id: Name of the daemon
            timeout: time for which the method would wait for state change

        Note: A separate method exists if the daemon is an OSD.
        Returns:  Pass -> True, Fail -> False
        """
        fsid = self.run_ceph_command(cmd="ceph fsid")["fsid"]
        host = self.fetch_host_node(daemon_type=daemon_type, daemon_id=daemon_id)
        if daemon_type == "osd":
            return self.change_osd_state(action=action, target=int(daemon_id))
        elif daemon_type == "mgr":
            systemctl_name = (
                f"ceph-{fsid}@{daemon_type}.{host.hostname}.{daemon_id}.service"
            )
        elif daemon_type == "mon":
            systemctl_name = f"ceph-{fsid}@{daemon_type}.{host.hostname}.service"
        else:
            systemctl_name = f"ceph-{fsid}@{daemon_type}.{host.shortname}.service"

        if not host:
            log.error(f"failed to find host for the {daemon_type} daemon {daemon_id} ")
            return False

        log.debug(f"Hostname of target host : {host.hostname}")

        # Collecting start time in case of failures
        init_time, _ = host.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        pass_status = True
        daemon_status, status_desc = self.get_daemon_status(
            daemon_type=daemon_type, daemon_id=daemon_id
        )

        if ((daemon_status == 0 or status_desc == "stopped") and action == "stop") or (
            (daemon_status == 1 or status_desc == "running") and action == "start"
        ):
            log.info(
                f"{daemon_type} daemon {daemon_id} already in desired state: {action}"
            )
            return True

        # Executing command to reset the fail count on the host and sleeping for 5 seconds
        cmd = "systemctl reset-failed"
        host.exec_command(sudo=True, cmd=cmd)
        time.sleep(5)

        # Executing command to perform desired action.
        systemctl_cmd = f"systemctl {action} {systemctl_name}"
        log.info(
            f"Performing {action} on {daemon_type}.{daemon_id} on host {host.hostname}. Command {systemctl_cmd}"
        )
        host.exec_command(sudo=True, cmd=systemctl_cmd)
        time.sleep(5)

        # verifying the daemon state
        if action in ["start", "stop"]:
            start_time = datetime.datetime.now()
            timeout_time = start_time + datetime.timedelta(seconds=timeout)

            while datetime.datetime.now() <= timeout_time:
                daemon_status, status_desc = self.get_daemon_status(
                    daemon_type=daemon_type, daemon_id=daemon_id
                )
                log.info(f"osd_status: {daemon_status}, status_desc: {status_desc}")
                if (
                    daemon_status == 0 or status_desc == "stopped"
                ) and action == "stop":
                    break
                elif (
                    daemon_status == 1 or status_desc == "running"
                ) and action == "start":
                    break
                time.sleep(20)

            if action == "stop" and daemon_status != 0:
                log.error(
                    f"Failed to stop daemon {daemon_type}.{daemon_id} service on {host.hostname}"
                )
                pass_status = False
            if action == "start" and daemon_status != 1:
                log.error(
                    f"Failed to start daemon {daemon_type}.{daemon_id}  service on {host.hostname}"
                )
                pass_status = False
            if not pass_status:
                log.error(
                    f"Collecting the journalctl logs for daemon  {daemon_type}.{daemon_id} "
                    f"service on {host.hostname} for the failure"
                )
                end_time, _ = host.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
                daemon_log_lines = self.get_journalctl_log(
                    start_time=init_time,
                    end_time=end_time,
                    daemon_type=daemon_type,
                    daemon_id=daemon_id,
                )
                log.error(
                    f"\n\n ------------ Log lines from journalctl ---------------- \n"
                    f"{daemon_log_lines}\n\n"
                )
                return False
        else:
            # Baremetal systems take some time for daemon restarts. changing sleep accordingly
            time.sleep(60)
            daemon_status, status_desc = self.get_daemon_status(
                daemon_type=daemon_type, daemon_id=daemon_id
            )
            if (daemon_status == 1 or status_desc == "running") and action == "restart":
                log.info(f"{daemon_type} daemon {daemon_id} is rebooted successfully")
                return True
        return True

    def get_osd_uuid(self, osd_id):
        """
        Method return ths osd fsid
        Args:
            osd_id: OSD id

        Returns: Return the ceph osd fsid

        """
        cmd_osd_info = f"ceph osd info osd.{osd_id}"
        osd_fsid = self.run_ceph_command(cmd=cmd_osd_info)["uuid"]
        return osd_fsid

    def get_orch_device_list(self, node=None):
        """
        Method returns the node or cluster device list in a json format.
        If node name not exists method return the whole cluster device list.
        Args:
            node: Node name in the cluster.
        Returns: Return the node/cluster device information

        """

        if node is None:
            cmd_orch_device = "ceph orch device ls --refresh"
        else:
            cmd_orch_device = f"ceph orch device ls {node} --refresh"

        orch_device_output = self.run_ceph_command(cmd=cmd_orch_device)
        return orch_device_output

    def get_ceph_volume_lvm_list(self, osd_node, osd_id, full_node=False):
        """
        Method returns the lvm list of a current node osds or a specific OSD in the node.
        Args:
            osd_node: OSD node object
            osd_id: osd id number
            full_node: True -> Returns the all OSD's lvm list in the node
                       False -> Return a specific OSD lvm list
        Returns: LVM list in the json format

        """
        if full_node and osd_id is None:
            cmd_get_lvm_list = "cephadm shell ceph-volume lvm list --format json"
        else:
            cmd_get_lvm_list = (
                f"cephadm shell ceph-volume lvm list {osd_id} --format json"
            )

        lvm_list = (osd_node.exec_command(sudo=True, cmd=cmd_get_lvm_list))[0]
        return json.loads(lvm_list)

    def get_osd_memory_usage(self, node_object, osd_id):
        """
        Methods returns the OSD memory usage
        Args:
            node_object: The osd node object
            osd_id:  The osd id(number)

        Returns: Memory usage of the OSD at that time

        """
        try:
            cmd_get_memory = (
                f'ps -eo pmem,args | grep -E "ceph-osd.*osd.{osd_id}\\b" | '
                + ('grep -v "init\\| grep" ' "| awk '{print $1}'")
            )
            memory_usage = node_object.exec_command(cmd=cmd_get_memory, sudo=True)
            return float(memory_usage[0].strip())
        except Exception:
            raise

    def get_osd_cpu_usage(self, node_object, osd_id):
        """
        Methods returns the CPU usage
        Args:
            node_object:  The osd node object
            osd_id: The osd id(number)

        Returns: CPU usage of the OSD at that time.

        """
        try:
            cmd_get_cpu = (
                f'ps -eo pcpu,args | grep -E "ceph-osd.*osd.{osd_id}\\b" | '
                + ('grep -v "init\\| grep" ' "| awk '{print $1}'")
            )
            cpu_usage = node_object.exec_command(cmd=cmd_get_cpu, sudo=True)
            return float(cpu_usage[0].strip())
        except Exception:
            raise

    @staticmethod
    def block_in_out_packets_on_host(source_host, target_host) -> bool:
        """
        Method to use iptables utility to block incoming and outgoing packets from one host to another.

        Args:
            source_host: Host where the rules need to be added.
            target_host: Host, traffic to which is stopped on the source host.

        Return:
            Pass -> True, fail -> False
        """

        source_hostname = source_host.hostname
        source_host_ip = source_host.ip_address
        target_hostname = target_host.hostname
        target_host_ip = target_host.ip_address

        log.debug(
            f"Blocking incoming and outgoing traffic to host: {target_hostname}. IP: {target_host_ip}. "
            f"Adding iptable rules on host {source_hostname}. IP: {source_host_ip}"
        )

        cmd = f"iptables -A INPUT -s {target_host_ip} -j DROP; iptables -A OUTPUT -d {target_host_ip} -j DROP"
        log.debug(f"Adding iptable rules via cmd: {cmd}")

        try:
            out, err = source_host.exec_command(sudo=True, cmd=cmd)
            time.sleep(10)
            ver_cmd = f"iptables -C INPUT -s {target_host_ip} -j DROP && iptables -C OUTPUT -d {target_host_ip} -j DROP"
            out, err = source_host.exec_command(sudo=True, cmd=ver_cmd)
            if err:
                log.error(f"iptable rules not set on host : {source_hostname}")
                return False
            log.debug(f"IPtable rules set on host : {source_hostname}")
            return True
        except Exception as err:
            log.error(f"Hit error: {err} while trying to add IPtable rules.")
            return False

    def get_host_object(self, hostname):
        """
        Hostname of host, whose ceph_object is required
        Args:
            hostname: hostname whose ceph object is required
        returns:
            Pass -> ceph_object for the hostname
            Fail -> None
        """
        for node in self.ceph_cluster:
            if (
                re.search(hostname, node.hostname)
                or re.search(hostname, node.vmname)
                or re.search(hostname, node.shortname)
            ):
                return node

        log.error(f"Could not find host object for host {hostname}")
        return None

    def get_object_details_head(self, osd_id, object_name):
        """
        Method returns the object head in json format
        Args:
            osd_id: osd id number
            object_name : object name
        Return : Returns the object head details if exists or None

        """
        cmd_get_object = (
            f"cephadm shell --name osd.{osd_id} -- ceph-objectstore-tool --data-path "
            f"/var/lib/ceph/osd/ceph-{osd_id} --head --op list {object_name}"
        )
        acting_osd_node = self.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
        out_put = acting_osd_node.exec_command(sudo=True, cmd=cmd_get_object)
        object_head = out_put[0].strip()
        if object_name not in object_head:
            log.error(
                f"Not able to retrive the object head for the object-{object_name} "
            )
            return None
        log.info(f"The object head of the {object_name} is {object_head}")
        return object_head

    def set_snapset_corrupt(self, osd_id, object_head):
        """
        Method to corrupt the snapset
        Args:
            osd_id : osd id
            object_head: object head in json format
        Return: None
        """
        cmd_snapset_corrupt = (
            f"cephadm shell --name osd.{osd_id} -- ceph-objectstore-tool --data-path"
            f" /var/lib/ceph/osd/ceph-{osd_id}  '{object_head}'   clear-snapset corrupt"
        )
        cmd_snapset_corrupt = cmd_snapset_corrupt.replace("\\", "")
        acting_osd_node = self.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
        acting_osd_node.exec_command(sudo=True, cmd=cmd_snapset_corrupt)
        return None

    def create_inconsistent_obj_snap(
        self, pool_name, object_name, secondary: bool = False
    ):
        """
        The method converts the object into inconsistent object
        The logic implemented in the code is-
        1. Get the target osd and pg_id
        2. Stopping the OSD
        3. Get the head of an object in json format and corrupt using the clear-snapset
        4. Start the OSD and perform deep-scrub on that pg id
        5. Check the status
        Args:
            pool_name: pool name
            object_name: object name in the pool
            secondary: If true, the inconsistent object would be generated on secondary OSD of the PG
        Returns: After converting the object in to inconsistent,method returns the pg id
                 If it fail returns None
        """
        osd_map_output = self.get_osd_map(pool=pool_name, obj=object_name)
        log.debug(
            f"\nThe acting set details for the object : {object_name}"
            f" on pool : {pool_name} is {osd_map_output}\n"
        )
        target_osd = osd_map_output["acting_primary"]
        if secondary:
            target_osd = osd_map_output["acting"][-1]
        log.info(f"The object stored in the target osd number-{target_osd}")
        pg_id = osd_map_output["pgid"]
        log.info(f"The object {object_name} is created in the pg-{pg_id}")
        cmd_update_obj = f"rados -p {pool_name} append {object_name} /etc/hosts"
        self.client.exec_command(sudo=True, cmd=cmd_update_obj)
        # stoping the OSD
        if not self.change_osd_state(action="stop", target=target_osd):
            log.error(f"Unable to stop the OSD : {target_osd}")
            return None
        object_head = self.get_object_details_head(target_osd, object_name)
        if object_head is None:
            log.error("The object head is None.Cannot execute further tests")
            return None
        self.set_snapset_corrupt(target_osd, object_head)
        if not self.change_osd_state(action="start", target=target_osd):
            log.error(f"Unable to start the OSD : {target_osd}")
            return None
        log.info(f"Performing the deep-scrub on the pg-{pg_id}")
        if not self.start_check_deep_scrub_complete(pg_id=pg_id):
            log.debug(f"deep-scrubbing could not be completed on PG : {pg_id}")
            raise Exception("PG not deep-scrubbed error")
        log.debug(f"Completed deep-scrubbing the pg : {pg_id}")

        if not self.check_inconsistent_health(inconsistent_present=True):
            log.error(
                "Inconsistent object details not exist in the health detail output"
            )
            return None
        log.info(f"The inconsistent object is created in the pg{pg_id}")
        return pg_id

    def set_noautoscale_flag(self, retry: int = 15) -> bool:
        """
        sets/Unsets the noautoscale flag on the cluster
        Args:
            retry: max number of retries to set the noautoscale flag on the cluster
        Returns:
            True -> noautoscale flag set successfully
            False -> noautoscale flag not set
        """
        log.debug("Setting the noautoscale flag on the cluster")
        iteration = 0
        noautoscale_cmd = "ceph osd pool set noautoscale"

        while iteration <= retry:
            iteration += 1

            try:
                out, _ = self.client.exec_command(
                    cmd=noautoscale_cmd, sudo=True, timeout=600
                )
                log.debug(f"o/p of no-autoscale enter cmd : {out}")
            except Exception as e:
                log.debug(f"Exception hit, but was expected; {e}")

            log.debug(
                "sleeping for 60 seconds before checking status of autoscale flag"
            )
            time.sleep(60)
            cmd = "ceph osd dump"
            out = self.run_ceph_command(cmd=cmd)
            flags_set = out["flags_set"]
            log.debug(f"Flags set on the cluster : {flags_set}")
            if "noautoscale" in flags_set:
                log.info("No autoscale flag successfully set on the cluster")
                return True
            log.info(
                "Noautoscale flag not yet set on the cluster, checking in 60 seconds"
            )

        log.error("Noautoscale flag not set on the cluster. Returning Fail..")
        return False

    def get_pg_autoscale_status(self, pool_name=None):
        """
        Executes autoscale-status command and returns o/p

        Args:
            pool_name: Name of the pool

        Returns:
            json object of the pool values.
        """
        cmd = "ceph osd pool autoscale-status"
        out = self.run_ceph_command(cmd=cmd)
        if pool_name:
            for pool in out:
                if pool["pool_name"] == pool_name:
                    return pool
        return out

    def create_rbd_image(self, pool_name, img_name, **kwargs):
        """
        Creates rbd image on the given pool
        Args:
            pool_name: Name of the pool where the image needs to be created
            img_name: name of the image
            **kwargs: Any other KW args that need to be sent

        """
        size = kwargs.get("image_size", "10G")
        img_cmd = f"rbd create {img_name} --size {size} --pool {pool_name}"
        self.client.exec_command(sudo=True, cmd=img_cmd)

        # printing image details
        img_details = f"rbd info {img_name} --pool {pool_name}"
        out, err = self.client.exec_command(sudo=True, cmd=img_details)
        log.info(
            f"Image : {img_name} created on pool : {pool_name}. Image details : {out}"
        )

    def mount_image_on_client(self, **kwargs):
        """
        Mounts the image provided on the given client node
        Args:
            kwargs: allowed kwargs are :
                pool_name: Name of the pool whose image needs to be mounted
                img_name: name of the image to be mounted
                client_obj: Ceph object for the client where image needs to be mounted
                mount_path: directory where the rbd block device needs to be mounted ( optional )
        """
        img = kwargs["img_name"]
        client = kwargs["client_obj"]
        pool = kwargs["pool_name"]
        mnt_path = kwargs.get("mount_path", "/tmp/rbd_mounts/")

        log.info(
            f"Proceeding to mount the images created on to client: {client.hostname},"
            f"Which has IP: {client.ip_address}, present in subnet: {client.subnet}"
        )

        map_cmd = f"rbd map {img} --pool {pool}"
        out, err = client.exec_command(sudo=True, cmd=map_cmd)
        mnt_name = out.strip()
        log.debug(
            f"Block device created: {mnt_name}, Creating ext4 filesystem on the device"
        )

        # Creating ext4 filesystem on the device
        mkfs_cmd = f"mkfs.ext4 -m0 {mnt_name}"
        client.exec_command(sudo=True, cmd=mkfs_cmd)

        # creating mount directory
        dir_cmd = f"mkdir -p {mnt_path}"
        client.exec_command(sudo=True, cmd=dir_cmd)

        # Mounting the device on mount path
        mnt_cmd = f"mount {mnt_name} {mnt_path}"
        client.exec_command(sudo=True, cmd=mnt_cmd)

        log.debug("Sleeping for 20 seconds for the device to be listed in mount -l")
        time.sleep(20)

        log.debug(
            f"All the mounted devices are : {client.exec_command(sudo=True, cmd='mount -l')}"
        )

        ver_cmd = "mount -l"
        out, err = client.exec_command(sudo=True, cmd=ver_cmd)

        if mnt_name in out:
            log.info("Verification successful. Device is listed in mount points.")
            return f"{mnt_path}{mnt_name}"
        else:
            log.error("Verification failed. Device is not listed in mount points.")
            return None

    def get_rbd_client_ips(self, pool_name, image_name):
        """
        Run the 'rbd status' command, parse the output, and collect the IP addresses of the watchers.
        Args:
            pool_name: Name of the pool
            image_name: Name of the image
        Returns:
            List of IP addresses of the watchers
        """
        command = f"rbd status {pool_name}/{image_name} --debug-rbd 0"
        try:
            output, err = self.client.exec_command(sudo=True, cmd=command)
            output = output.strip()
            log.debug(
                f"clients on pool : {pool_name} and image : {image_name} are :\n {output}"
            )
            watchers_start_index = output.find("Watchers:")
            if watchers_start_index != -1:
                watchers_output = output[watchers_start_index:]
                lines = watchers_output.split("\n")[1:]
                watchers_ips = [
                    re.search(r"watcher=([\d.]+):\d+", line).group(1) for line in lines
                ]
                log.debug(f"Client IPs :\n {watchers_ips}\n")
                return watchers_ips
            else:
                log.debug("No watchers found.")
                return []
        except Exception as e:
            log.error(f"Error running command: {e}")
            return []

    def rbd_bench_write(self, pool_name, image_name, client_obj):
        """
        Method to write RBD objects into the pool
        Args:
            pool_name: Name of the pool where IOs should be written
            image_name: Name of the image where IOs should be written
            client_obj: Ceph object for client from where objects should be written
        Returns:
            Tuple(Output of the rbdbench command, execution status)
        """
        command = f"sudo rbd bench-write {image_name} --pool={pool_name}"
        try:
            out, _ = client_obj.exec_command(sudo=True, cmd=command)
        except Exception as err:
            log.debug(f"Exception expected. Error faced : {err}")
            return err, False
        log.info(f"Completed writing IOs into the pool. output : {out}")
        return out, True

    def add_client_blocklisting(self, **kwargs):
        """
        Method to blocklist client IPs on ceph cluster
        Args:
            kwargs: KW args that are accepted are :
                ip: IP of the host which is to be blocklisted
                cidr: CIDR of the host which is to be blocklisted
        """
        ip = kwargs.get("ip", None)
        cidr = kwargs.get("cidr", None)
        base_cmd = "ceph osd blocklist"
        if cidr:
            blklist_cmd = f"{base_cmd} range add {cidr}"
        else:
            blklist_cmd = f"{base_cmd} add {ip}"
        self.run_ceph_command(cmd=blklist_cmd)
        log.debug(
            "Sleeping for 20 seconds for IP to be blocked and appear in blocklist ls"
        )
        time.sleep(20)
        blocked_ips = self.get_blocklist_ips()
        if blocked_ips is None:
            log.error("Error fetching the blocklisted IPs")
            return False
        if cidr:
            ip = cidr.split("/")[0]
        if ip not in blocked_ips:
            log.error(
                f"IP/CIDR : {ip} not present in block list. Blocked IPs on cluster : {blocked_ips}"
            )
            return False
        log.info(f"IP/CIDR : {ip} blocklisted successfully")
        return True

    def get_blocklist_ips(self):
        """
        Fetches the blocklisted IPs on the cluster
        returns:
            List of blocklisted IPs/ CIDRs
        """
        cmd = "ceph osd blocklist ls"
        try:
            out = self.node.shell([cmd])
            ip_pattern = re.compile(r"(\d+\.\d+\.\d+\.\d+:\d+/\d+)")
            ips = ip_pattern.findall(out[0])
            ips = [ip.split(":")[0] for ip in ips]
            log.debug(
                f"Blocklisted IPs on client : {out[0]} \n\n IPs collected : {ips}"
            )
            return ips
        except Exception as err:
            log.error(f"hit issue : during command execution : {err}")
            return None

    def rm_client_blocklisting(self, **kwargs):
        """
        Method to remove blocklisted client IPs on ceph cluster
        Args:
            kwargs: KW args that are accepted are :
                ip: IP of the host which is to be removed from blocked list
                cidr: CIDR of the host which is to be removed from blocked list
        """
        ip = kwargs.get("ip", None)
        cidr = kwargs.get("cidr", None)
        base_cmd = "ceph osd blocklist"
        if cidr:
            blklist_cmd = f"{base_cmd} range rm {cidr}"
        else:
            blklist_cmd = f"{base_cmd} rm {ip}"
        self.run_ceph_command(cmd=blklist_cmd)
        log.debug(
            "Sleeping for 20 seconds for IP to be unblocked and be removed from blocklist ls"
        )
        time.sleep(20)
        blocked_ips = self.get_blocklist_ips()
        if blocked_ips is None:
            log.error("Error fetching the blocklisted IPs")
            return False
        if cidr:
            ip = cidr.split("/")[0]
        if ip in blocked_ips:
            log.error(
                f"IP/CIDR : {ip} present in block list after unblocking. Blocked IPs on cluster : {blocked_ips}"
            )
            return False
        log.info(f"IP/CIDR : {ip} removed from blocklist successfully")
        return True

    def get_daemon_metadata(self, daemon_type: str, daemon_id: str = None):
        """
        Method to fetch the metadata for any daemon
        If daemon id is not provided, return complete output of
        ceph <daemon_type> metadata
        Returns:
            metadata (List) -> If daemon_id is not provided
            metadata (Dict) -> If daemon_id is provided
            None if metadata is not found
        """
        log.debug(f"Passed daemon type : {daemon_type}, Daemon ID : {daemon_id}")
        base_cmd = f"ceph {daemon_type} metadata"
        if daemon_id is None:
            return self.run_ceph_command(cmd=base_cmd, client_exec=True)

        if daemon_type == "osd" and daemon_id is not None:
            return self.run_ceph_command(
                cmd=f"{base_cmd} {daemon_type}.{daemon_id}", client_exec=True
            )

        out = self.run_ceph_command(cmd=f"{base_cmd} {daemon_id}", client_exec=True)
        if out is None:
            log.error(
                f"Metadata info for the input daemon: {daemon_type} {daemon_id} not found"
            )
        return out

    def get_osd_status(self, osd_id):
        """
        method to fetch the output of osd status command -
        ceph tell osd.N status command
        Args:
            osd_id: ID of desired osd daemon
        Returns:
            dict output of ceph tell osd.N status command
        """
        base_cmd = f"ceph tell osd.{osd_id} status"
        return self.run_ceph_command(cmd=base_cmd)

    def get_osd_perf_dump(self, osd_id, filter=None):
        """
        method to get the perf dump of osd-
        ceph tell osd.N perf dump command
        Args:
            osd_id: ID of desired osd daemon
            filter: perf dump filter
        Returns:
            dict output of ceph tell osd.N perf dump command
        e.g:
            ceph tell osd.# perf dump
            ceph tell osd.# perf dump bluefs
            ceph tell osd.# perf dump bluestore
        """
        base_cmd = f"ceph tell osd.{osd_id} perf dump "
        if filter:
            base_cmd += filter
        return self.run_ceph_command(cmd=base_cmd)

    def get_available_devices(self, node_name: str, device_type: str):
        """
        Method to fetch list of available device paths on the provided node.
            Args:
                node_name: node hostname
                device_type: hdd or ssd or nvme
            Returns:
                List of available device paths on the node.
        """
        device_paths = []
        available_device_list = self.get_orch_device_list(node_name)
        for path_list in available_device_list[0]["devices"]:
            if path_list["human_readable_type"] != device_type:
                continue
            if (
                path_list["human_readable_type"] == device_type
                and path_list["available"] is True
            ):
                device_paths.append(path_list["path"])
        return device_paths

    def get_pool_id(self, pool_name) -> int:
        """
        Method to fetch pool ID for a given pool
        Args:
            pool_name: name of the pool
        Returns:
            pool ID in integer format
        """
        _cmd = f"ceph osd pool stats {pool_name}"
        out = self.run_ceph_command(cmd=_cmd, client_exec=True)
        return int(out[0]["pool_id"])

    def rados_pool_cleanup(self):
        """
        Method to remove any existing rados pool on the cluster
        Returns:
                True -> All pools removed successfully
                Raise exception if any of the pool removal fails
        """
        # fetch list of all the pools on the cluster
        _cmd = "ceph osd pool ls detail"
        pool_json = self.run_ceph_command(cmd=_cmd, client_exec=True)

        # filter out pools which have rados application enable
        rados_pools = [
            pool for pool in pool_json if "rados" in pool["application_metadata"].keys()
        ]

        if not rados_pools:
            log.info(f"List of rados pool on the cluster is empty: {rados_pools}")
            log.info("Nothing to remove")
            return True

        for pool in rados_pools:
            if not self.delete_pool(pool=pool["pool_name"]):
                log.error(f"Failed to delete pool {pool['pool_name']}")
                raise Exception(f"Failed to delete pool {pool['pool_name']}")
            log.info(f"Pool {pool['pool_name']} deleted successfully")

        return True

    def log_cluster_health(self):
        """
        Method to log cluster health detail to STDOUT
        Returns:
            Output of ceph health detail
        """
        log.debug("Printing cluster health and status")
        health_detail, _ = self.node.shell(args=["ceph health detail"])
        log.info(f"\n****\n Cluster health detail: \n {health_detail} \n****")
        log.info(
            f"\n****\n Cluster status: \n {self.client.exec_command(cmd='ceph -s', sudo=True)[0]} \n****"
        )
        return health_detail

    def create_ecpool_inconsistent_obj(
        self, pool_object, client_node, pool_name, no_of_objects
    ):
        """
        The method converts the object into inconsistent object in EC pool
        The logic implemented in the code is-
        1. Get the target osd and pg_id
        2. Get the object details by using the ceph-objectstore-tool
        3. Remove the “hinfo_key” of the object
        4. perform scrub
        Args:
            pool_object: pool object
            client_node: client object
            pool_name: pool name
            no_of_objects: no of objcts to convert in to inconsistent objects
        Returns: After converting the object in to inconsistent,method returns the pg id and object count
                 If it fail returns None
        """
        # Creating more number of objects
        for obj_num in range(no_of_objects):
            object_name = f"object_{obj_num}"
            cmd_create_obj = f"rados -p {pool_name} put {object_name} /etc/group"
            client_node.exec_command(cmd=cmd_create_obj, sudo=True)

        osd_map_output = self.get_osd_map(pool=pool_name, obj="object_1")
        log.debug(
            f"\nThe acting set details for the object : {object_name}"
            f" on pool : {pool_name} is {osd_map_output}\n"
        )
        target_osd = osd_map_output["acting_primary"]

        log.info(f"The object stored in the target osd number-{target_osd}")
        pg_id = osd_map_output["pgid"]
        log.info(f"The object {object_name} is created in the pg-{pg_id}")

        acting_osd_node = self.fetch_host_node(daemon_type="osd", daemon_id=target_osd)
        # stoping the OSD
        if not self.change_osd_state(action="stop", target=target_osd):
            log.error(f"Unable to stop the OSD : {target_osd}")
            return None

        for obj_num in range(no_of_objects):
            time.sleep(3)
            object_name = f"object_{obj_num}"
            object_head = self.get_object_details_head(target_osd, object_name)
            if object_head is None:
                log.error("The object head is None.Cannot execute further tests")
                return None
            data_list = json.loads(object_head)
            ec_pg_id = data_list[0]
            obj_attr = data_list[1]
            json_str = json.dumps(obj_attr)

            cmd = (
                f"cephadm shell --name osd.{target_osd} -- ceph-objectstore-tool --data-path "
                f"/var/lib/ceph/osd/ceph-{target_osd} --pgid {ec_pg_id}  '{json_str}' rm-attr hinfo_key"
            )
            acting_osd_node.exec_command(sudo=True, cmd=cmd)

        if not self.change_osd_state(action="start", target=target_osd):
            log.error(f"Unable to stop the OSD : {target_osd}")
            return None
        log.info(f"Performing the scrub on the pg-{pg_id}")
        self.run_scrub(pgid=pg_id)
        self.start_check_scrub_complete(
            pg_id=pg_id, user_initiated=True, wait_time=1800
        )
        inconsistent_details = self.get_inconsistent_object_details(pg_id)
        obj_count = len(inconsistent_details["inconsistents"])
        log.info(f"The inconsistent object count is -{obj_count}")
        log.info(f"The inconsistent object is created in the pg{pg_id}")
        return pg_id, obj_count

    def set_unmanaged_flag(self, daemon):
        """
        Method to set the unmanaged flag to a daemon
        Args:
            daemon : Cluster daemon Example: mgr,mon, osd, rgw
        Return:
             True, if unmanaged flag is set
             False, if unmanaged flag is not set
        """

        cmd_set_unmanaged_flag = f"ceph orch set-unmanaged {daemon}"
        self.client.exec_command(sudo=True, cmd=cmd_set_unmanaged_flag)
        base_cmd = "ceph orch ls"
        cmd = f"{base_cmd} {daemon}"
        duration = 300  # 5 minutes
        start_time = time.time()
        while time.time() - start_time < duration:
            orch_ls_op = self.run_ceph_command(cmd=cmd)
            for entry in orch_ls_op:
                if entry["unmanaged"]:
                    log.info("The unmanaged flag is set to True ")
                    return True
            time.sleep(30)
        log.error("The unmanaged flag is not set to True  ")
        return False

    def set_managed_flag(self, daemon):
        """
        Method to unset the unmanaged flag to a daemon
        Args:
            daemon : Cluster daemon Example: mgr,mon, osd, rgw
        Return:
            True, if unmanaged flag is unset
            False, if unmanaged flag is not unset
        """

        cmd_set_managed_flag = f"ceph orch set-managed {daemon}"
        self.client.exec_command(sudo=True, cmd=cmd_set_managed_flag)

        base_cmd = "ceph orch ls"
        cmd = f"{base_cmd} {daemon}"
        duration = 300  # 5 minutes
        start_time = time.time()
        while time.time() - start_time < duration:
            orch_ls_op = self.run_ceph_command(cmd=cmd)
            for entry in orch_ls_op:
                if "unmanaged" not in entry:
                    log.info("The unmanaged flag is unset")
                    return True
            time.sleep(30)
        log.error("The unmanaged flag is not unset")
        return False

    def check_daemon_exists_on_host(self, host, daemon_type=None) -> bool:
        """
        Method to check a daemon is running on the given host
        Args:
            host: name of the host where existence of mgr needs to be checked
            daemon_type: daemon type to check on the node.
                         Example: mgr,mon
        Return:
            Pass -> True, Fail -> false
        """
        cmd = f"ceph orch ps {host}"
        out = self.run_ceph_command(cmd=cmd, client_exec=True)
        if daemon_type is None:
            if not out:
                log.info(f"There are no daemons in the {host} node.")
                return False
            else:
                log.info(f"The {host} node contain the daemons- {out}.")
                return True
        for entry in out:
            if entry["daemon_type"] == daemon_type:
                log.debug(f"{daemon_type} daemon present on the host")
                return True
        log.debug(f"{daemon_type} daemon not present on the host")
        return False

    def get_daemon_list_fromCluster(self, daemon_type):
        """
        Method to get the required daemon list from the cluster
        Args:
             daemon_type: daemon type
                         Example: mgr,mon
        Returns:
              nodes list that contain the daemon type
        """
        mgr_node_list = []
        cmd_orch = "ceph orch host ls"
        host_output = self.run_ceph_command(cmd=cmd_orch, client_exec=True)
        for item in host_output:
            host_name = item["hostname"]
            cmd_orch = f"ceph orch ps {host_name}"
            out = self.run_ceph_command(cmd=cmd_orch, client_exec=True)
            for entry in out:
                if entry["daemon_type"] == daemon_type:
                    mgr_node_list.append(host_name)
        return mgr_node_list

    def get_osd_list(self, status: str) -> list:
        """
        Method to fetch list of OSDs which are in input state
        Args:
            status: state of OSD [UP | IN | DOWN | OUT | DESTROYED]
        Returns:
            o/p of "ceph osd tree <status>"
        """
        osd_dict = {"up": [], "down": [], "in": [], "out": [], "destroyed": []}
        # prepare separate OSD lists
        for key in osd_dict:
            osd_tree = self.run_ceph_command(cmd=f"ceph osd tree {key}")
            if osd_tree["nodes"]:
                for entry in osd_tree["nodes"]:
                    if entry["type"] == "osd":
                        osd_dict[key].append(entry["id"])
            log.info(f"List of {key} OSDs: {osd_dict[key]}")

        return osd_dict[status]

    def get_osd_details(self, osd_id: int):
        """
        Method to fetch the OSD details deom the cluster for the selected OSD ID
        Args:
            osd_id: OSD ID whose details need to be fetched
        Returns:
            Dictionary of the KW paris of the OSD details.
            eg:
                {
                    "id": 0,
                    "device_class": "hdd",
                    "name": "osd.0",
                    "type": "osd",
                    "type_id": 0,
                    "crush_weight": 0.0243988037109375,
                    "depth": 2,
                    "pool_weights": {},
                    "reweight": 1,
                    "kb": 26210304,
                    "kb_used": 46600,
                    "kb_used_data": 10240,
                    "kb_used_omap": 0,
                    "kb_used_meta": 36352,
                    "kb_avail": 26163704,
                    "utilization": 0.17779267268323176,
                    "var": 1,
                    "pgs": 10,
                    "status": "up"
                }
        """
        log.info(f"Passed OSD : {osd_id} to fetch the details")
        cmd = f"ceph osd df osd.{osd_id}"
        out = self.run_ceph_command(cmd=cmd)
        details = out["nodes"][0]
        log.debug(f" OSD : {osd_id} details are : {details}")
        return details

    def get_osd_size(self, osd_id):
        """
        Method returns the size of the OSD in KB
        Args:
            osd_id: osd id
        Returns: The total size of the osd in KB

        """
        total_size_osd = self.get_osd_df_stats(
            tree=False, filter_by="name", filter=f"osd.{osd_id}"
        )["summary"]["total_kb"]
        return total_size_osd

    def check_crash_status(self):
        """
        Module to check crashes on the cluster
        Returns:
            True -> crash detected
            False -> No crashes observed
        """
        # logging any existing crashes on the cluster
        crash_list = self.do_crash_ls()
        if crash_list:
            log.error("!!!ERROR: Crash exists in the cluster \n\n:" f"{crash_list}")
            log.info("Logging crash info for each crash")
            for entry in crash_list:
                log.info(f"crash ID: {entry['crash_id']}")
                crash_info = self.run_ceph_command(
                    f"ceph crash info {entry['crash_id']}", client_exec=True
                )
                log.info(crash_info)

            log.info("Archiving all the existing crashes on the cluster")
            out, _ = self.node.shell(["ceph crash archive-all"])
            log.info(out)
            return True
        return False

    def list_obj_snaps(self, pool_name: str, obj_name: str):
        """
        Module to fetch the list of snaps for an object
        Args:
            pool_name: name of the pool where obj is present
            obj_name: name of the object
        Return:
            o/p of rados listsnaps -p <pool_name> <objectname>
        Example:
            # rados listsnaps -p test-snap obj-1
            obj-1:
            cloneid	snaps	size	overlap
            2	1,2	4194304	[]
            head	-	4194304
        """
        _cmd = f"rados listsnaps -p {pool_name} {obj_name}"
        return self.run_ceph_command(cmd=_cmd, client_exec=True)

    def list_pool_snaps(self, pool_name: str):
        """
        Module to list pool snapshots
        Args:
            pool_name: name of the pool
        Returns:
            o/p of rados lssnap -p <pool_name>
        Example:
            # rados lssnap -p test-snap
            2	snap-2	2024.05.29 10:45:30
            3	snap-3	2024.05.29 10:58:17
            2 snaps
        """
        _cmd = f"rados lssnap -p {pool_name}"
        return self.client.exec_command(cmd=_cmd, sudo=True)[0]

    def get_rados_df(self, pool_name: str = None):
        """
        Module to fetch rados df output
        should return only pool detail if pool_name is provided
        Args:
            pool_name(optional): name of the pool
        Returns:
            rados df o/p if no pool name if provided
            rados df o/p for the pool whose pool name is provided
        """
        _cmd = f"rados df -p {pool_name}" if pool_name else "rados df"
        out = self.run_ceph_command(cmd=_cmd, client_exec=True)

        return out["pools"][0] if pool_name else out

    def set_service_managed_type(self, service_type, unmanaged) -> bool:
        """
        Method to set the service to either managed or unmanaged
        Args:
            unmanaged: True or false, for the service management
            service_type : service types are- mon,mgr,osd,rgw, mds
        returns:
            Pass -> True, Fail -> false
        """
        file_name = (
            f"/tmp/{service_type}_spec_{self.set_service_managed_type.__name__}.yaml"
        )

        # Creating service config file
        self.client.exec_command(sudo=True, cmd=f"touch {file_name}")

        cmd_export = f"ceph orch ls {service_type} --export"
        out = self.run_ceph_command(cmd=cmd_export, client_exec=True)
        for osd_service in out:
            if unmanaged:
                log.debug(
                    f"Setting the {service_type} service as unmanaged by cephadm. current status : {out}"
                )
                osd_service["unmanaged"] = "true"
            else:
                log.debug(
                    f"Setting the {service_type} service as unmanaged by cephadm. current status : {out}"
                )
                osd_service["unmanaged"] = "false"
            json_out = json.dumps(osd_service)
            # Adding the spec rules into the file
            cmd = f"echo '{json_out}' > {file_name}"
            self.client.exec_command(cmd=cmd, sudo=True)
            log.debug(f"Contents of {service_type} spec file : {out}")
            apply_cmd = f"ceph orch apply -i {file_name}"
            log.info(f"Applying the spec file via cmd : {apply_cmd}")
            self.client.exec_command(cmd=apply_cmd, sudo=True)
            time.sleep(10)
        out = self.list_orch_services(service_type=service_type, export=True)
        for osd_service in out:
            status = osd_service.get("unmanaged", False)
            if status == "false":
                unmanaged_check = False
            else:
                unmanaged_check = True

            if unmanaged_check != unmanaged:
                log.error(
                    f"{service_type} Service with {osd_service['service_id']}not unmanaged={unmanaged} state. Fail"
                )
                return False
        log.info(f" All {service_type} Service in unmanaged={unmanaged} state. Pass")
        return True
