"""
core_workflows module is a rados layer configuration module for Ceph cluster.
It allows us to perform various day1 and day2 operations such as
1. Creating , modifying, setting , getting, writing, scrubbing, reading various pools like EC and replicated
2. Increase decrease PG counts, enable - disable - configure modules that do this
3. Enable logging to file, set and reset config params and cluster checks
4. Set-up email alerts and other cluster operations
More operations to be added as needed

"""

import json
import logging
import time

from ceph.ceph_admin import CephAdmin

log = logging.getLogger(__name__)


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

    def run_ceph_command(self, cmd: str) -> dict:
        """
        Runs ceph commands with json tag for the action specified otherwise treats action as command
        and returns formatted output
        Args:
            cmd: Command that needs to be run
        Returns: dictionary of the output
        """

        cmd = f"{cmd} -f json"
        out, err = self.node.shell([cmd])
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

        cmd = f"ceph osd pool get {pool} {props} -f json"
        out, err = self.node.shell([cmd])
        if err:
            log.info(f"The property : {props} not set on the cluster. Error: {err}")
            return False
        prop_details = json.loads(out)
        return prop_details

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
            1. rados_write_duration -> duration of write operation (int)
            2. byte_size -> size of objects to be written (str)
                eg : 10KB, 4096
        Returns: True -> pass, False -> fail
        """
        duration = kwargs.get("rados_write_duration", 200)
        byte_size = kwargs.get("byte_size", 4096)
        cmd = f"sudo rados --no-log-to-stderr -b {byte_size} -p {pool_name} bench {duration} write --no-cleanup"
        try:
            self.node.shell([cmd])
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
        Returns: True -> pass, False -> fail
        """
        duration = kwargs.get("rados_read_duration", 80)
        try:
            cmd = f"rados --no-log-to-stderr -p {pool_name} bench {duration} seq"
            self.node.shell([cmd])
            cmd = f"rados --no-log-to-stderr -p {pool_name} bench {duration} rand"
            self.node.shell([cmd])
            return True
        except Exception as err:
            log.error(f"Error running rados bench write on pool : {pool_name}")
            log.error(err)
            return False
