import concurrent.futures
import json
import math
import re
import time
from typing import List

from libcloud.utils.misc import get_secure_random_string

from ceph.ceph import CephNode
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_scrub import RadosScrubber
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.rados_test_util import wait_for_daemon_status
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_bluestore_data_compression import get_pool_stats
from utility.log import Log
from utility.retry import retry

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
        ceph_cluster,
    ):
        self.rados_obj = rados_obj
        self.cephadm = cephadm
        self.rados_scrubber_obj = RadosScrubber(node=cephadm)
        self.mon_obj = mon_obj
        self.client_node = client_node
        self.thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)
        self.ceph_cluster = ceph_cluster
        self.MIN_BLOB_SIZE_SSD = 65536
        self.MAX_BLOB_SIZE_SSD = 65536
        self.MIN_BLOB_SIZE_HDD = 8192
        self.MAX_BLOB_SIZE_HDD = 65536

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
        url = f"https://raw.githubusercontent.com/red-hat-storage/cephci/refs/heads/main/tests/rados/{cpp_file_name}"
        cmd = f"""curl -L {url} -o ~/{cpp_file_name};
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

    def enable_OSD_debug_and_perform_write_operation(
        self,
        primary_osd_id: str,
        type: str,
        pool_name: str,
        alloc_hint: int,
        object_size: int,
        blob_size: int,
        rbd_img: str,
        write_offset: int = None,
        compress_percentage: int = 50,
    ) -> List[str]:
        """
        Temporarily raise OSD debug levels, perform a write using the selected utility,
        then restore the original debug config and return the time window of the write.

        This helper:
        - Sets per-OSD debug levels for OSD logs, bluestore, and bluestore compression.
        - Records the start time.
        - Runs either `librados` write utility or `fio` (RBD) to perform IO.
        - Records the end time.
        - Removes the debug overrides set at the start.
        - Returns a two-element list: [start_time, end_time] both as strings.

        Args:
            primary_osd_id (int): OSD id to target for debug config changes.
            type (str): Write utility type; expected values: `"librados"` or other (treated as `"fio"`).
            pool_name (str): Ceph pool name where the IO is performed.
            alloc_hint (int): Allocation hint value passed to the write utility.
            object_size (int): Total write size passed to the write utility.
            blob_size (int): Blob/block size parameter used by the write utility.
            rbd_img (str): RBD image name when using `fio` mode.
            write_offset (int, optional): Offset for partial or overwrite writes (only used for fio).
            compress_percentage (int, optional): `--buffer_compress_percentage` for fio (default 50).

        Returns:
            list: [init_time, end_time] where each is timestamp.
        """
        log_info_msg = "--- Increase OSD debug log levels ---"
        log.info(log_info_msg)
        self.mon_obj.set_config(
            section=f"osd.{primary_osd_id}", name="debug_osd", value="30/30"
        )
        self.mon_obj.set_config(
            section=f"osd.{primary_osd_id}",
            name="debug_bluestore_compression",
            value="30/30",
        )
        self.mon_obj.set_config(
            section=f"osd.{primary_osd_id}", name="debug_bluestore", value="30/30"
        )

        init_time, _ = self.client_node.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        init_time = init_time.strip()

        if type == "librados":
            cpp_file_name = "write_object_with_alloc_hints.cpp"
            cmd = f"""curl -L https://github.com/s-vipin/test/raw/refs/heads/main/{cpp_file_name} -o ~/{cpp_file_name};
                     g++ -std=c++17 -Wall -O2 -o ~/write_obj ~/{cpp_file_name} -lrados ;chmod 700 ~/write_obj;
                     ~/write_obj {pool_name} 1 {object_size} {alloc_hint}"""
        else:
            cmd = f"fio --ioengine=rbd --direct=1 --name=test --iodepth=1 --bs={blob_size} --rw=write "
            cmd += f"--numjobs=1 --size={object_size} --pool={pool_name} --rbdname={rbd_img} --zero_buffers=0 "
            cmd += f"--refill_buffers=1 --buffer_compress_percentage={compress_percentage} "

            if write_offset:
                cmd += f"--offset {write_offset}"

        _, _, exit_code, _ = self.client_node.exec_command(
            cmd=cmd, pretty_print=True, verbose=True, sudo=True
        )

        if exit_code == 1:
            raise Exception("write operation failed using write utility type -> ", type)

        end_time, _ = self.client_node.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        end_time = end_time.strip()

        self.mon_obj.remove_config(
            section=f"osd.{primary_osd_id}", name="debug_osd", value="30/30"
        )
        self.mon_obj.remove_config(
            section=f"osd.{primary_osd_id}",
            name="debug_bluestore_compression",
            value="30/30",
        )
        self.mon_obj.remove_config(
            section=f"osd.{primary_osd_id}", name="debug_bluestore", value="30/30"
        )

        return [init_time, end_time]

    @retry(Exception, tries=3, backoff=1, delay=10)
    def write_io_and_fetch_log_lines(
        self,
        write_utility_type: str,
        primary_osd_id: str,
        pool_name: str,
        object_size: int,
        alloc_hint: int,
        blob_size: int,
        rbd_img: str,
        patterns: List[str],
        write_offset: int = None,
        compression_percentage: int = 50,
    ) -> str:
        """
        Perform IO on a pool and fetch OSD log lines produced during the IO window.

        This helper:
        - Raises OSD debug levels, performs the requested IO (via `librados` or `fio`),
          and captures the start and end timestamps of the IO window.
        - Uses the timestamps to extract log lines from the target OSD log file
          and filters those lines by the provided `patterns`.
        - Returns the filtered log output as a single string.

        Args:
            write_utility_type (str): Type of write utility to run. Expected values:
                - "librados" to use the C++ helper
                - any other value will use `fio` for RBD writes
            primary_osd_id (str): OSD id whose logs should be inspected.
            pool_name (str): Ceph pool name where IO is performed.
            object_size (int): Total I/O size to write.
            alloc_hint (int): Bluestore allocation hint value to pass to the write utility.
            blob_size (int): Blob or block size for the write utility.
            rbd_img (str): RBD image name when using `fio` (ignored for `librados`).
            patterns (list[str]): List of regex patterns to filter log lines. Lines matching
                any of the patterns are returned.
            write_offset (int, optional): Offset for partial/overwrite writes (used for fio).
            compression_percentage (int, optional): `--buffer_compress_percentage` for fio.
                Defaults to 50.

        Returns:
            str: Filtered log output from the OSD log file for the IO.
        """
        log_info_msg = (
            f"--- Perform IO operation with object allocation hints -> {alloc_hint} ---"
        )
        log.info(log_info_msg)

        init_time, end_time = self.enable_OSD_debug_and_perform_write_operation(
            primary_osd_id,
            write_utility_type,
            pool_name,
            alloc_hint,
            object_size,
            blob_size,
            rbd_img,
            write_offset,
            compression_percentage,
        )

        log_info_msg = "--- Fetch log lines printed in OSD logs ---"
        log.info(log_info_msg)

        fsid = self.rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]

        host = self.rados_obj.fetch_host_node(
            daemon_type="osd", daemon_id=primary_osd_id
        )

        base_cmd_get_log_line = (
            f'awk \'$1 >= "{init_time}" && $1 <= "{end_time}"\' '
            f"/var/log/ceph/{fsid}/ceph-osd.{primary_osd_id}.log"
        )

        grep_line = "grep -a -E '"
        for i, pattern in enumerate(patterns):
            if i == 0:
                grep_line += f"{pattern}"
            else:
                grep_line += f"|{pattern}"
        grep_line += "'"

        base_cmd_get_log_line = f"{base_cmd_get_log_line} | {grep_line}"
        try:
            chk_log_msg, err = host.exec_command(sudo=True, cmd=base_cmd_get_log_line)
        except Exception as e:
            log_msg = f"Exception while fetching log lines -> {e}"
            log.error(log_msg)
            raise Exception(log_msg)

        return chk_log_msg

    def recompression_checks(self, logs, recompression_min_gain):
        """
        Parse OSD logs and validate recompression decisions.

        Scans the provided multiline logs
        For each line which matches the pattern:
          - parses compr occupy and compr size (hex -> int),
          - converts both to allocation units (AU = ceil(bytes / 4096)),
          - computes the gain and compares it to `recompression_min_gain`,
          - increments pass/fail counters and collects failed log lines.

        Args:
            logs (str): Multiline log text to scan for compression/recompression info.
            recompression_min_gain (float): when calculated gain is >= this value recompression is expected.

        Returns:
            tuple: (pass_count, fail_count, failed_logs)
                pass_count (int): number of log entries where recompression is valid/expected.
                fail_count (int): number of log entries where recompression is not expected but occurred.
                failed_logs (list): list of log lines that failed validation.
        """
        chk_log_msg = logs.split("\n")
        pattern = r"compr-occup=(0x[0-9a-fA-F]+)\s+compr-size\?=(0x[0-9a-fA-F]+)"

        failed_logs = []
        pass_count = 0
        fail_count = 0
        for i in range(len(chk_log_msg)):
            log_line = chk_log_msg[i]
            match = re.search(pattern, log_line)
            if match:
                log.info(" ---- start of validation of line %d ---- " % (i))
                info_msg = (
                    f"\n--- validating log line (def recompression_checks())---"
                    f"\n{log_line}"
                    f"\n----- end of log line -----"
                )
                log.info(info_msg)
                compr_occupy = int(match.group(1), 16)
                compr_size = int(match.group(2), 16)

                compr_occupy_au = math.ceil(compr_occupy / 4096)
                compr_size_au = math.ceil(compr_size / 4096)
                gain = 0
                if compr_size_au != 0:
                    gain = compr_occupy_au / compr_size_au

                msg = f"compr occupy-> {compr_occupy} compr size-> {compr_size} "
                log.info(msg)

                msg = f"compr occupy AU -> {compr_occupy_au} compr size AU-> {compr_size_au} "
                log.info(msg)

                msg = f"gain-> {gain} recompression_min_gain-> {recompression_min_gain}"
                log.info(msg)

                if gain >= recompression_min_gain:
                    log.info(
                        "Since gain >= recompression_min_gain."
                        " Recompression should occur"
                    )
                    if "recompress" in chk_log_msg[i + 1]:
                        log.info("Recompression occurred as expected. PASS")
                        pass_count += 1
                    else:
                        log.info("Recompression did not occur. FAIL")
                        fail_count += 1
                        failed_logs.append({"line": i, "log": chk_log_msg[i : i + 2]})
                else:
                    log.info(
                        "Since gain < recompression_min_gain. "
                        "Recompression should not occur"
                    )
                    if "recompress" in chk_log_msg[i + 1]:
                        log.info("Recompression occurred. FAIL")
                        fail_count += 1
                        failed_logs.append({"line": i, "log": chk_log_msg[i : i + 2]})
                    else:
                        log.info("Recompression did not occur. PASS")
                        pass_count += 1
                log.info(" ---- start of log line ---- ")
                for log_debug_msg in chk_log_msg[i : i + 2]:
                    log.debug(log_debug_msg)
                log.info(" ---- end of log line ---- ")
                log.info(" ---- end of validation ---- ")

        return [pass_count, fail_count, failed_logs]

    def partial_overwrite(self, **kwargs):
        """
        Perform a partial-overwrite recompression validation.

        This test:
        1. Creates a pool and enables inline compression with the provided
           compression settings.
        2. Sets the per-OSD `bluestore_recompression_min_gain`.
        3. Creates an RBD image and writes initial IO to it.
        4. Performs a partial overwrite (offset = write_size // 2).
        5. Fetches OSD logs and verifies recompression behavior by scanning for
           `Estimator::` and `recompress:` log patterns and evaluating them
           against `recompression_min_gain`.

        Args (via kwargs):
            compression_algorithm (str): Compression algorithm, default "snappy".
            compression_required_ratio (str): Required compression ratio, default "0.875".
            compression_mode (str): Compression mode, default "force".
            pool_name (str): Name of the pool to create (required).
            alloc_hint (int): Object allocation hint, default BLUESTORE_ALLOC_HINTS.NOHINT.
            recompression_min_gain (float): Threshold for recompression, default 1.2.
            write_size (int): Size (bytes) used for IO, default 64000.
            rbd_img (str): Optional rbd image name; if not provided one is generated.

        Returns:
            None

        Raises:
            Exception: On failures such as pool creation, enabling compression,
                       image creation, IO failures, or recompression validation failures.
        """
        log.info(self.__doc__)
        compression_algorithm = kwargs.get("compression_algorithm", "snappy")
        compression_required_ratio = kwargs.get("compression_required_ratio", "0.875")
        compression_mode = kwargs.get("compression_mode", "force")
        pool_name = kwargs["pool_name"]
        alloc_hint = kwargs.get("alloc_hint", BLUESTORE_ALLOC_HINTS.NOHINT)
        recompression_min_gain = kwargs.get("recompression_min_gain", 1.2)
        write_size = kwargs.get("write_size", 64000)
        rbd_img = "rbd_img" + get_secure_random_string(6)
        separator = "=" * 80

        log.info(
            "\n %s \n Step1 : Create pool and enable compression on pool %s \n %s"
            % (separator, pool_name, separator)
        )

        assert self.rados_obj.create_pool(
            pool_name=pool_name, pg_num=1, disable_pg_autoscale=True, app_name="rbd"
        )
        acting_pg_set = self.rados_obj.get_pg_acting_set(pool_name=pool_name)
        primary_osd_id = acting_pg_set[0]

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

        self.mon_obj.set_config(
            section="osd",
            name="bluestore_recompression_min_gain",
            value=recompression_min_gain,
        )

        log.info(
            "\n [COMPLETED] Step1 : Create pool and enable compression on pool %s "
            % (pool_name)
        )
        log.info(
            "\n %s \n [STARTING] Step2 : Create rbd image %s \n %s"
            % (separator, rbd_img, separator)
        )

        cmd = f"rbd create {rbd_img} --size 10G --pool {pool_name}"
        try:
            self.rados_obj.client.exec_command(cmd=cmd, sudo=True)
            log.info("Created device I/O image: %s", rbd_img)
        except Exception as e:
            raise Exception("Failed to create device I/O image: %s", e)

        log.info("\n [COMPLETED] Step2 : Create rbd image %s " % (rbd_img))
        log.info(
            "\n %s \n [STARTING] Step3 : Perform IO operation of size %s \n %s"
            % (separator, write_size, separator)
        )

        _ = self.write_io_and_fetch_log_lines(
            write_utility_type="fio",
            primary_osd_id=primary_osd_id,
            pool_name=pool_name,
            object_size=write_size,
            alloc_hint=alloc_hint,
            blob_size=write_size,
            rbd_img=rbd_img,
            patterns=[],
        )

        log.info(
            "\n [COMPLETED] Step3 : Perform IO operation of size %s " % (write_size)
        )
        log.info(
            "\n %s \n [STARTING] Step4 : Perform overwrite operation of size %s at offset %s \n %s"
            % (separator, write_size, write_size // 2, separator)
        )
        # offset => 500000/2 -> 250000
        # size => 500000
        # [0 ...............500,000]
        #         [250,000................750,000]
        overwrite_logs = self.write_io_and_fetch_log_lines(
            write_utility_type="fio",
            primary_osd_id=primary_osd_id,
            pool_name=pool_name,
            object_size=write_size,
            alloc_hint=alloc_hint,
            blob_size=write_size,
            rbd_img=rbd_img,
            patterns=["Estimator::", "recompress:"],
            write_offset=write_size // 2,
        )

        log.info(
            "\n [COMPLETED] Step4 : Perform overwrite operation of size %s at offset %s"
            % (write_size, write_size // 2)
        )
        log.info(
            "\n %s \n [STARTING] Step5 : Validating recompression \n %s"
            % (separator, separator)
        )

        log.info(" ---- overwrite operations logs : start of log line ---- ")
        log.info(" Overwrite logs \n%s" % (overwrite_logs))
        log.info(" ----- overwrite operations logs : end of log line ---- ")
        pass_count, fail_count, failed_logs = self.recompression_checks(
            overwrite_logs, recompression_min_gain
        )

        info_msg = (
            "When calculated gain >= recompression_min_gain, recompression should occur."
            "We can confirm that from the logs 'recompress' keyword would be present. "
            "When calculated gain < recompression_min_gain, recompression should not occur."
            "We can confirm that from the logs 'recompress' keyword would not be present."
        )

        if fail_count > 0:
            for failed_log_line in failed_logs:
                log.info(failed_log_line)
            log.error(info_msg)
            raise Exception(info_msg)

        log.info("\n [COMPLETED] Step5 : Validating recompression")

        log.info("Recompression is as expected")
        log.error(info_msg)
