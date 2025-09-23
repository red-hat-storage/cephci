import concurrent.futures
import json
import time

from ceph.ceph import CephNode
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_scrub import RadosScrubber
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.rados_test_util import wait_for_daemon_status
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_bluestore_data_compression import get_pool_stats
from utility.log import Log

log = Log(__name__)


class BLUESTORE_ALLOC_HINTS:
    SEQUENTIAL_WRITE = 1
    RANDOM_WRITE = 2
    SEQUENTIAL_READ = 4
    RANDOM_READ = 8
    APPEND_ONLY = 16
    IMMUTABLE = 32
    COMPRESSIBLE = 256
    INCOMPRESSIBLE = 512
    NOHINT = 0


class COMPRESSION_MODES:
    PASSIVE = "passive"
    AGGRESSIVE = "aggressive"
    FORCE = "force"
    NONE = "none"


class BluestoreDataCompression:
    """
    Provides methods to validate and manage Bluestore compression.
    Args:
        rados_obj (RadosOrchestrator): Interface to perform cluster-level RADOS operations.
        cephadm (CephAdmin): Ceph admin interface for running orchestration commands.
        mon_obj (MonConfigMethods): Interface for setting and retrieving monitor-level daemon configs.
        client_node (CephNode): A Ceph client node object used to execute shell commands.

    Usage:
        compression_manager = BluestoreDataCompression(rados_obj, cephadm, mon_obj, client_node)
        compression_manager.validate_default_compression_values()
        compression_manager.enable_bluestore_write_v2()
        compression_manager.disable_bluestore_write_v2()
    """

    default_parameters = {
        "bluestore_write_v2": "false",
        "bluestore_recompression_min_gain": "1.200000",
        "bluestore_compression_algorithm": "snappy",
        "bluestore_compression_max_blob_size": "0",
        "bluestore_compression_max_blob_size_hdd": "65536",
        "bluestore_compression_max_blob_size_ssd": "65536",
        "bluestore_compression_min_blob_size": "0",
        "bluestore_compression_min_blob_size_hdd": "8192",
        "bluestore_compression_min_blob_size_ssd": "65536",
        "bluestore_compression_mode": "none",
        "bluestore_compression_required_ratio": "0.875000",
        "bluestore_write_v2_random": "false",
    }

    def __init__(
        self,
        rados_obj: RadosOrchestrator,
        cephadm: CephAdmin,
        mon_obj: MonConfigMethods,
        client_node: CephNode,
    ):
        self.rados_obj = rados_obj
        self.cephadm = cephadm
        self.rados_scrubber_obj = RadosScrubber(node=cephadm)
        self.mon_obj = mon_obj
        self.client_node = client_node
        self.thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)

    def validate_osd_compression_value(self, osd_id, parameter, default_value):
        parameter_dict = self.mon_obj.daemon_config_get(
            daemon_type="osd", daemon_id=osd_id, param=parameter
        )
        parameter_value = parameter_dict[parameter]
        log_debug_msg = (
            f"\nOSD ID -> {osd_id}"
            f"\nexpected {parameter} -> {default_value}"
            f"\ncurrent {parameter} -> {parameter_value}"
        )
        log.debug(log_debug_msg)
        if str(parameter_value) != default_value:
            log.error(log_debug_msg)
            raise Exception(log_debug_msg)

    def validate_default_compression_values(self, pg_id=None, osd_id=None):
        """
        Validates that OSDs have expected default compression-related configuration values.
        This method checks each OSD for a predefined set of compression parameters to ensure
        their current values match expected defaults.

        The OSDs to validate can be determined in one of three ways:
            - If `pg_id` is provided, the OSDs in the acting set of the PG will be validated.
            - If `osd_id` is provided, only that specific OSD will be validated.
            - If neither is provided, all currently UP OSDs will be validated.

        Args:
            pg_id (str, optional): Placement group ID to fetch the acting OSD set.
            osd_id (str, optional): Specific OSD ID to validate.

        Raises:
            Exception: If any OSD does not have the expected default value for a parameter.
        """
        if pg_id is not None:
            osd_list = self.rados_obj.get_pg_acting_set(pg_num=pg_id)
        elif osd_id is not None:
            osd_list = [osd_id]
        else:
            osd_list = self.rados_obj.get_osd_list(status="up")

        log.info("Starting to validate default bluestore data compression parameters")
        # for parameter, default_value in self.default_parameters.items():
        keys = list(self.default_parameters.keys())
        values = list(self.default_parameters.values())
        for osd in osd_list:
            list(
                self.thread_executor.map(
                    self.validate_osd_compression_value, [osd] * len(keys), keys, values
                )
            )

        log.info(
            "Completed validating default bluestore data compression parameters"
            "osds: ",
            osd_list,
        )

    def toggle_bluestore_write_v2(self, pg_id=None, osd_id=None, toggle_value="true"):
        """
        Enables the 'bluestore_write_v2' configuration on target OSDs.
        - If `pg_id` is provided: Enables the flag on OSDs in the acting set of the PG.
        - If `osd_id` is provided: Enables the flag only on the specified OSD.
        - If neither is provided: Enables the flag cluster-wide via default OSD config,
          restarts all OSD daemons, and waits for the cluster to reach a clean state.

        After applying the configuration, the method validates that the flag is active on
        each OSD. If any OSD fails to apply the configuration, an exception is raised.

        Args:
            pg_id (str, optional): Placement group ID to determine acting OSDs.
            osd_id (str, optional): Specific OSD ID to apply the setting.
            toggle_value (str, default="true"): toggle bluestore_write_v2 to "true" or "false"

        Raises:
            Exception: If configuration setting fails, daemon restarts fail,
                       or validation indicates the flag was not properly set.
        """
        # fetch OSD list
        if pg_id is not None:
            osd_list = self.rados_obj.get_pg_acting_set(pg_num=pg_id)
        elif osd_id is not None:
            osd_list = [osd_id]
        else:
            osd_list = self.rados_obj.get_osd_list(status="up")

        # IF PG ID or OSD ID is not passed, Enable bluestore_write_v2 to all OSD
        # using 'ceph config set bluestore_write_v2 true' followed by all OSD restart
        if pg_id is None and osd_id is None:
            self.mon_obj.set_config(
                section="osd", name="bluestore_write_v2", value=toggle_value
            )

            log.info(osd_list)
            if (
                self.rados_obj.restart_daemon_services(daemon="osd", timeout=900)
                is False
            ):
                raise Exception(
                    "OSD daemon part of the service could not restart within timeout"
                )

            wait_for_clean_pg_sets(rados_obj=self.rados_obj)

        # If PG id is passed or OSD ID is passed, Set bluestore_write_v2 for osd.osd_id
        # and not for all OSDs
        if pg_id or osd_id:
            for osd in osd_list:
                self.mon_obj.set_config(
                    section=f"osd.{osd}", name="bluestore_write_v2", value=toggle_value
                )
                self.client_node.exec_command(
                    cmd=f"ceph orch daemon restart osd.{osd}",
                    sudo=True,
                )
                wait_for_daemon_status(
                    daemon_type="osd",
                    daemon_id=osd,
                    status="running",
                    rados_obj=self.rados_obj,
                )
                wait_for_clean_pg_sets(rados_obj=self.rados_obj)

        # for each OSD perform validation
        # using 'cephadm shell -- ceph daemon {daemon_type}.{daemon_id} config get bluestore_write_v2'
        # If bluestore_write_v2 is False for any OSD, add the OSD to invalids list and display at the end.
        invalids_list = list()

        list(
            self.thread_executor.map(
                self.validate_osd_compression_value,
                osd_list,
                ["bluestore_write_v2"] * len(osd_list),
                [toggle_value] * len(osd_list),
            )
        )

        if len(invalids_list) != 0:
            err_msg = (
                f"failed to set bluestore_write_v2 to {toggle_value} on below OSDs"
                f"{invalids_list}"
            )
            raise Exception(err_msg)

        log_info_msg = (
            f"Successfully set bluestore_write_v2 to {toggle_value} on {osd_list}"
        )
        log.info(log_info_msg)

    def check_compression_based_on_mode_and_hints(self, mode, hint):
        """
        Determine whether compression should be applied based on the given mode and hint.

        This method evaluates compression behavior depending on the specified
        compression mode and allocation hint.

        Args:
            mode : compression mode.
                - passsive
                - aggresive
                - force
                - none
            hint : allocation hint provided for the data.
                - 0 - no hint
                - 256 - compressible hint
                - 512 - incompressible hint

        Returns:
            bool: True if compression should be applied, False otherwise.
        """
        if mode == COMPRESSION_MODES.PASSIVE:
            if hint == BLUESTORE_ALLOC_HINTS.COMPRESSIBLE:
                return True
            return False

        elif mode == COMPRESSION_MODES.AGGRESSIVE:
            if hint == BLUESTORE_ALLOC_HINTS.INCOMPRESSIBLE:
                return False
            else:
                return True

        elif mode == COMPRESSION_MODES.FORCE:
            return True

        elif mode == COMPRESSION_MODES.NONE:
            return False

    def validate_compression_modes(self, **kwargs):
        """
        1> Create a pool
        2> Enable compression on pool
        Compression algorithm :- Snappy

        NOTE: Based on Compression mode passed step3 - step8 will vary
        Compression mode :- Passive
        3> Write an object to the pool with compressible hint
        4> In passive mode compression will be applied only when compressible hint is set, So data should be compressed
        5> Write an object to the pool without any hint
        6> Data should not be compressed
        7> Write an object to the pool with incompressible hint only.
        8> Data should not be compressed.

        Compression mode :- Aggressive
        3> Write an object to the pool with compressible hint
        4> Data should compress
        5> Write an object to the pool without any hint
        6> Data should compress
        7> Write an object to the pool with an incompressible hint.
        8> Data should not be compressed.

         Compression mode :- Force
        3> Write an object to the pool with compressible hint
        4> Data should compress
        5> Write an object to the pool without any hint
        6> Data should compress
        7> Write an object to the pool with an incompressible hint.
        8> Data should compress

         Compression mode :- None
        3> Write an object to the pool with compressible hint
        4> Data should not compress
        5> Write an object to the pool without any hint
        6> Data should not compress
        7> Write an object to the pool with an incompressible hint.
        8> Data should not compress
        """
        compression_algorithm = kwargs.get("compression_algorithm", "snappy")
        compression_required_ratio = kwargs.get("compression_required_ratio", "0.875")
        object_size = kwargs.get("object_size", "100000")
        compression_mode = kwargs.get("compression_mode", "force")
        pool_name = kwargs["pool_name"]
        alloc_hint = kwargs.get("alloc_hint", BLUESTORE_ALLOC_HINTS.NOHINT)

        log.info("---1. Create pools to test bluestore data compression---")
        log_info_msg = f"Creating pool {pool_name}"
        log.info(log_info_msg)
        assert self.rados_obj.create_pool(
            pool_name=pool_name, pg_num=1, disable_pg_autoscale=True, app_name="rbd"
        )

        log_info_msg = f"---2. Enabling compression on pool {pool_name} \
                    \n compression_mode: {compression_mode} \
                    \n compression_algorithm: {compression_algorithm} \
                    \n compression_required_ratio: {compression_required_ratio} \
                    \n compression_mode: {compression_mode}"
        log.info(log_info_msg)

        if (
            self.rados_obj.pool_inline_compression(
                pool_name=pool_name,
                compression_mode=compression_mode,
                compression_algorithm=compression_algorithm,
                compression_required_ratio=compression_required_ratio,
            )
            is False
        ):
            err_msg = f"Error enabling compression on pool : {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        log_info_msg = f"---3. Write IO to the pool {pool_name}---"
        log.info(log_info_msg)

        # install dependencies for g++
        cmd = "yum groupinstall 'Development Tools' -y"
        self.client_node.exec_command(
            cmd=cmd, pretty_print=True, verbose=True, sudo=True
        )

        cmd = "yum install librados2-devel libradospp-devel -y"
        self.client_node.exec_command(
            cmd=cmd, pretty_print=True, verbose=True, sudo=True
        )

        cpp_file_name = "write_object_with_alloc_hints.cpp"
        cmd = f"""curl -L https://github.com/s-vipin/test/raw/refs/heads/main/{cpp_file_name} -o ~/{cpp_file_name};
                g++ -std=c++17 -Wall -O2 -o ~/write_obj ~/{cpp_file_name} -lrados ;chmod 700 ~/write_obj;
                ~/write_obj {pool_name} 1 {object_size} {alloc_hint}"""
        self.client_node.exec_command(
            cmd=cmd, pretty_print=True, verbose=True, sudo=True
        )

        log.info("Sleeping for 120 seconds until ceph df populates")
        time.sleep(120)

        log_info_msg = f"---4. Validate compression mode on pool {pool_name}---"
        log.info(log_info_msg)

        pool_stats = get_pool_stats(rados_obj=self.rados_obj, pool_name=pool_name)
        log.info(json.dumps(pool_stats))
        compress_under_bytes = pool_stats["compress_under_bytes"]

        log_msg = (
            f"\n compressed bytes -> {compress_under_bytes} "
            f"\n mode -> {compression_mode}"
            f"\n hint -> {alloc_hint}"
        )
        if (
            self.check_compression_based_on_mode_and_hints(compression_mode, alloc_hint)
            is True
        ):
            log_msg += "\n compression should occur"
            log.info(log_msg)
            assert int(compress_under_bytes) > 0
        else:
            log_msg += "\n compression should not occur"
            log.info(log_msg)
            assert int(compress_under_bytes) == 0

        log.info("Validated compression mode successfully")

        if self.rados_obj.delete_pool(pool=pool_name) is False:
            raise Exception(f"Deleting pool {pool_name} failed")

    def get_expected_blob_size(self, device_class, object_allocation_hint):
        """
        Max_blob_size is selected when:
        Object is hinted as immutable and sequential read
        Object is hinted as append-only and sequential read
        else min_blob_size is used.

        For HDD -
            min blob size -> 8kib
            max blob size -> 64kib

        For SSD -
            min blob size -> 64kib
            max blob size -> 64kib
        """

        if device_class == self.DEVICE_CLASS_SSD:
            if (
                BLUESTORE_ALLOC_HINTS.SEQUENTIAL_READ
                & BLUESTORE_ALLOC_HINTS.APPEND_ONLY
            ) or (
                BLUESTORE_ALLOC_HINTS.SEQUENTIAL_READ & BLUESTORE_ALLOC_HINTS.IMMUTABLE
            ):
                return self.MAX_BLOB_SIZE_SSD
            return self.MIN_BLOB_SIZE_SSD

        elif device_class == self.DEVICE_CLASS_HDD:
            if (
                BLUESTORE_ALLOC_HINTS.SEQUENTIAL_READ
                & BLUESTORE_ALLOC_HINTS.APPEND_ONLY
            ) or (
                BLUESTORE_ALLOC_HINTS.SEQUENTIAL_READ & BLUESTORE_ALLOC_HINTS.IMMUTABLE
            ):
                return self.MAX_BLOB_SIZE_HDD
            return self.MIN_BLOB_SIZE_HDD

        else:
            err_msg = f"Device class not supported {device_class}"
            raise Exception(err_msg)

    def test_blob_sizes(self, **kwargs):
        """
        Steps:-
        1) Create pool
        2) Enable compression on the pool
        3) Write IO to the pool with object allocation hints
        4) Fetch log lines printed in OSD logs
        5) Validate blob sizes
        NOTE:-
        IF immutable and sequential read or  apppend-only and sequential read is object allocation hint -> max blob size
        else -> min_blob_size

        ---- min blob size tests ----
        Write object to the pool with compressible hint -> min blob size should be chosen as target blob size
        Write object to the pool with incompressible hint -> min blob size should be chosen as target blob size

        ---- max blob size tests -----
        Write object to the pool with immutable and sequential read -> max blob size should be chosen as
         target blob size
        Write object to the pool with apppend-only and sequential read -> max blob size should be chosen as
         target blob size
        6) Delete pool
        """
        log.info(self.test_blob_sizes.__doc__)
        compression_algorithm = kwargs.get("compression_algorithm", "snappy")
        compression_required_ratio = kwargs.get("compression_required_ratio", "0.875")
        object_size = kwargs.get("object_size", "64000")
        compression_mode = kwargs.get("compression_mode", "force")
        pool_name = kwargs["pool_name"]
        alloc_hint = kwargs.get("alloc_hint", BLUESTORE_ALLOC_HINTS.NOHINT)
        device_class = kwargs.get("device_class", "hdd").lower()

        log_info_msg = f"""
        ---Starting test for blob sizes in bluestore compression---
        compression_mode: {compression_mode}
        compression_algorithm: {compression_algorithm}
        compression_required_ratio: {compression_required_ratio}
        compression_mode: {compression_mode}
        device class: {device_class}
        object alloc hint: {alloc_hint}
        NOTE:-
            SEQUENTIAL_WRITE = 1
            RANDOM_WRITE = 2
            SEQUENTIAL_READ = 4
            RANDOM_READ = 8
            APPEND_ONLY = 16
            IMMUTABLE = 32
            COMPRESSIBLE = 256
            INCOMPRESSIBLE = 512
            NOHINT = 0
        """
        log.info(log_info_msg)

        log.info("---1. Create pools to test bluestore data compression---")
        log_info_msg = f"Creating pool {pool_name}"
        log.info(log_info_msg)
        assert self.rados_obj.create_pool(
            pool_name=pool_name, pg_num=1, disable_pg_autoscale=True, app_name="rbd"
        )

        log_info_msg = f"""
---2. Enabling compression on pool {pool_name}---
compression_mode: {compression_mode}
compression_algorithm: {compression_algorithm}
compression_required_ratio: {compression_required_ratio}
compression_mode: {compression_mode}
device class: {device_class}
object alloc hint: {alloc_hint}
NOTE:-
    SEQUENTIAL_WRITE = 1
    RANDOM_WRITE = 2
    SEQUENTIAL_READ = 4
    RANDOM_READ = 8
    APPEND_ONLY = 16
    IMMUTABLE = 32
    COMPRESSIBLE = 256
    INCOMPRESSIBLE = 512
    NOHINT = 0
"""
        log.info(log_info_msg)

        if (
            self.rados_obj.pool_inline_compression(
                pool_name=pool_name,
                compression_mode=compression_mode,
                compression_algorithm=compression_algorithm,
                compression_required_ratio=compression_required_ratio,
            )
            is False
        ):
            err_msg = f"Error enabling compression on pool : {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        log_info_msg = f"---3. Write IO to the pool {pool_name} with {alloc_hint} object allocation hint ---"
        log.info(log_info_msg)

        osd_nodes = self.ceph_cluster.get_nodes(role="osd")

        self.rados_obj.remove_log_file_content(osd_nodes, daemon_type="osd")

        # set the debug log to 20
        acting_pg_set = self.rados_obj.get_pg_acting_set(pool_name=pool_name)
        primary_osd_id = acting_pg_set[0]

        if device_class == "ssd":
            self.rados_obj.switch_osd_device_type(osd_id=primary_osd_id, rota_val=0)
        elif device_class == "hdd":
            self.rados_obj.switch_osd_device_type(osd_id=primary_osd_id, rota_val=1)
        else:
            err_msg = f"Unsupported device class {device_class}"
            raise Exception(err_msg)

        self.mon_obj.set_config(
            section=f"osd.{primary_osd_id}", name="debug_osd", value="20/20"
        )
        self.mon_obj.set_config(
            section=f"osd.{primary_osd_id}",
            name="debug_bluestore_compression",
            value="20/20",
        )
        self.mon_obj.set_config(
            section=f"osd.{primary_osd_id}", name="debug_bluestore", value="20/20"
        )

        init_time, _ = self.client_node.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        init_time = init_time.strip()

        # Adding a sleep of 5 seconds, because sometimes it is observed the logs did not print the
        # required log line, possibly due to fast execution.
        time.sleep(10)
        cpp_file_name = "write_object_with_alloc_hints.cpp"
        cmd = f"""curl -L https://github.com/s-vipin/test/raw/refs/heads/main/{cpp_file_name} -o ~/{cpp_file_name};
                g++ -std=c++17 -Wall -O2 -o ~/write_obj ~/{cpp_file_name} -lrados ;chmod 700 ~/write_obj;
                ~/write_obj {pool_name} 1 {object_size} {alloc_hint}"""
        self.client_node.exec_command(cmd=cmd, pretty_print=True, verbose=True)
        time.sleep(10)

        end_time, _ = self.client_node.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        end_time = end_time.strip()

        self.mon_obj.remove_config(
            section=f"osd.{primary_osd_id}", name="debug_osd", value="20/20"
        )
        self.mon_obj.remove_config(
            section=f"osd.{primary_osd_id}",
            name="debug_bluestore_compression",
            value="20/20",
        )
        self.mon_obj.remove_config(
            section=f"osd.{primary_osd_id}", name="debug_bluestore", value="20/20"
        )

        log_info_msg = "---4. Fetch log lines printed in OSD logs ---"
        log.info(log_info_msg)

        fsid = self.rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]

        host = self.rados_obj.fetch_host_node(
            daemon_type="osd", daemon_id=primary_osd_id
        )

        base_cmd_get_log_line = (
            f'awk \'$1 >= "{init_time}" && $1 <= "{end_time}"\' '
            f"/var/log/ceph/{fsid}/ceph-osd.{primary_osd_id}.log"
        )

        grep_line = "grep -E 'target_blob_size 0x'"
        base_cmd_get_log_line = f"{base_cmd_get_log_line} | {grep_line}"
        chk_log_msg, err = host.exec_command(sudo=True, cmd=base_cmd_get_log_line)

        log.info("________Fetched log lines________")
        log.info(chk_log_msg)
        log.info("_________________________________")

        log_info_msg = f"---5. Validate blob size on pool {pool_name}---"
        log.info(log_info_msg)

        for log_line in chk_log_msg.split("\n"):
            log.info(f"\nProcessing log line" f"\n___" f"\n{log_line}" f"\n___")

            if "target_blob_size" not in log_line:
                continue

            size_in_hex = log_line.split("target_blob_size ")[1].split(" compress=")[0]
            size_in_dec = int(size_in_hex, 16)

            log.info(f"Target blob size in decimal is {size_in_dec}")
            expected_target_blob_size = self.get_expected_blob_size(
                device_class, alloc_hint
            )
            if size_in_dec != expected_target_blob_size:
                err_msg = (
                    f"\ntarget blob size is not as expected"
                    f"\nActual -> {size_in_dec}"
                    f"\nExpected -> {expected_target_blob_size}"
                )
                log.error(err_msg)
                raise Exception(err_msg)
            else:
                info_msg = (
                    f"\ntarget blob size is not as expected"
                    f"\nActual -> {size_in_dec}"
                    f"\nExpected -> {expected_target_blob_size}"
                )
                log.info(info_msg)

        log_info = (
            f"Validated min/max blob sizes successfully"
            f"compression_mode: {compression_mode}"
            f"compression_algorithm: {compression_algorithm}"
            f"compression_required_ratio: {compression_required_ratio} "
            f"compression_mode: {compression_mode}"
            f"device class: {device_class}"
            f"object alloc hint: {alloc_hint}"
        )
        log.info(log_info)

        log_info_msg = f"---6. Delete Pool {pool_name}---"
        log.info(log_info_msg)

        if self.rados_obj.delete_pool(pool=pool_name) is False:
            raise Exception(f"Deleting pool {pool_name} failed")
