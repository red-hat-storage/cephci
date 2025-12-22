import concurrent.futures
import datetime
import json
import math
import random
import re
import string
import time
from enum import Enum
from typing import List

from ceph.ceph import CephNode
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.rados_scrub import RadosScrubber
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.rados_test_util import wait_for_daemon_status
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
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


class IOTools(Enum):
    FIO = "fio"
    LIBRADOS = "librados"


class COMPRESSION_ALGORITHMS:
    snappy = "snappy"
    libz = "libz"
    zstd = "zstd"
    lz4 = "lz4"


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
        pool_type: str = "replicated",
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
        self.pool_type = pool_type

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

        log_info_msg = (
            "Completed validating default bluestore data compression parameters"
            f"osds: {osd_list}"
        )
        log.info(log_info_msg)

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

    def enable_compression(
        self,
        compression_mode,
        compression_algorithm,
        compression_required_ratio=0.875,
        pool_level_compression=True,
        pool_name=None,
        compression_min_blob_size=None,
    ):
        """
        Enable compression on a pool or cluster-wide at the OSD level.

        This method configures compression settings for pool or for
        all OSDs in the cluster.

        Args:
            compression_mode (str): Compression mode to apply. Valid values:
                - "passive"
                - "aggressive"
                - "force"
                - "none"
            compression_algorithm (str): "snappy", "zlib", "lz4"
            compression_required_ratio (float, optional): Defaults to 0.875.
            pool_level_compression (bool, optional): If True, enable compression at pool level.
                If False, enable compression at OSD level. Defaults to True.
            pool_name (str, optional): Name of the pool to enable compression on.
                Required when pool_level_compression is True. Ignored when pool_level_compression is False.
        """
        if pool_level_compression and pool_name is None:
            raise Exception(
                "If pool_level_compression is True, pool_name must be provided"
            )

        if pool_level_compression:
            log.info("Enabling compression at pool level %s " % pool_name)
            if compression_min_blob_size is not None:
                if (
                    self.rados_obj.pool_inline_compression(
                        pool_name=pool_name,
                        compression_mode=compression_mode,
                        compression_algorithm=compression_algorithm,
                        compression_required_ratio=(
                            0.875
                            if compression_required_ratio is None
                            else compression_required_ratio
                        ),
                        compression_min_blob_size=compression_min_blob_size,
                    )
                    is False
                ):
                    err_msg = f"Error enabling compression on pool : {pool_name}"
                    log.error(err_msg)
                    raise Exception(err_msg)
            else:
                if (
                    self.rados_obj.pool_inline_compression(
                        pool_name=pool_name,
                        compression_mode=compression_mode,
                        compression_algorithm=compression_algorithm,
                        compression_required_ratio=(
                            0.875
                            if compression_required_ratio is None
                            else compression_required_ratio
                        ),
                    )
                    is False
                ):
                    err_msg = f"Error enabling compression on pool : {pool_name}"
                    log.error(err_msg)
                    raise Exception(err_msg)
        else:
            log.info("Enabling compression on OSD level")
            if (
                self.mon_obj.set_config(
                    section="osd",
                    name="bluestore_compression_algorithm",
                    value=compression_algorithm,
                )
                is False
            ):
                err_msg = (
                    f"Error enabling compression algorithm at OSD level : {pool_name}"
                )
                log.error(err_msg)
                raise Exception(err_msg)
            if (
                self.mon_obj.set_config(
                    section="osd",
                    name="bluestore_compression_mode",
                    value=compression_mode,
                )
                is False
            ):
                err_msg = f"Error enabling compression mode at OSD level : {pool_name}"
                log.error(err_msg)
                raise Exception(err_msg)

            if compression_required_ratio:
                if (
                    self.mon_obj.set_config(
                        section="osd",
                        name="bluestore_compression_required_ratio",
                        value=compression_required_ratio,
                    )
                    is False
                ):
                    err_msg = f"Error enabling compression required ratio at OSD level : {pool_name}"
                    log.error(err_msg)
                    raise Exception(err_msg)

        log.info(
            "Successfully enabled compression"
            "Mode: %s, Algorithm: %s, Required ratio: %s"
            % (compression_mode, compression_algorithm, compression_required_ratio)
        )

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
        pool_level_compression = kwargs.get("pool_level_compression", True)

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

        self.enable_compression(
            compression_mode=compression_mode,
            compression_algorithm=compression_algorithm,
            pool_level_compression=pool_level_compression,
            pool_name=pool_name,
        )

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

        pool_stats = self.get_pool_stats(pool_name=pool_name)
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
        type: IOTools,
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

        Args:
            primary_osd_id (int): OSD id
            type (str): fio or librados
            pool_name (str): name of the pool
            alloc_hint (int): Allocation hint value passed to the write utility.
            object_size (int): Total write size
            blob_size (int): Blob/block size parameter
            rbd_img (str): RBD image name (only used for fio)
            write_offset (int, optional): Offset for partial or overwrite writes (only used for fio)
            compress_percentage (int, optional): `--buffer_compress_percentage` for fio (default 50)

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

        if type == IOTools.LIBRADOS:
            # install dependencies for g++
            cmd = "yum groupinstall 'Development Tools' -y"
            self.client_node.exec_command(cmd=cmd, sudo=True)

            cmd = "yum install librados2-devel libradospp-devel -y"
            self.client_node.exec_command(cmd=cmd, sudo=True)
            cpp_file_name = "write_object_with_alloc_hints.cpp"
            cmd = (
                f"curl -L "
                f"https://raw.githubusercontent.com/red-hat-storage/cephci/refs/heads/main/tests/rados/{cpp_file_name} "
                f"-o ~/{cpp_file_name}; "
                f"g++ -std=c++17 -Wall -O2 -o ~/write_obj ~/{cpp_file_name} -lrados ;"
                f"chmod 700 ~/write_obj;"
            )
            if write_offset:
                cmd += f"~/write_obj {pool_name} 1 {object_size} {alloc_hint} {write_offset}"
            else:
                cmd += f"~/write_obj {pool_name} 1 {object_size} {alloc_hint}"
        elif type == IOTools.FIO:
            cmd = "yum install fio -y"
            self.client_node.exec_command(cmd=cmd, sudo=True)
            cmd = f"fio --ioengine=rbd --direct=1 --name=test --iodepth=1 --bs={blob_size} --rw=write "
            cmd += f"--numjobs=1 --size={object_size} --pool={pool_name} --rbdname={rbd_img} --zero_buffers=0 "
            cmd += f"--refill_buffers=1 --buffer_compress_percentage={compress_percentage} "

            if write_offset:
                cmd += f"--offset {write_offset}"

        else:
            raise Exception("Unsupported write utility type -> ", type)

        _, _, exit_code, _ = self.client_node.exec_command(
            cmd=cmd, pretty_print=True, verbose=True, sudo=True
        )

        if exit_code == 1:
            raise Exception("write operation failed using write utility type -> ", type)

        end_time, _ = self.client_node.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        end_time = end_time.strip()

        self.mon_obj.remove_config(section=f"osd.{primary_osd_id}", name="debug_osd")
        self.mon_obj.remove_config(
            section=f"osd.{primary_osd_id}",
            name="debug_bluestore_compression",
        )
        self.mon_obj.remove_config(
            section=f"osd.{primary_osd_id}", name="debug_bluestore"
        )

        return [init_time, end_time]

    @retry(Exception, tries=3, delay=10, backoff=1)
    def write_io_and_fetch_log_lines(
        self,
        write_utility_type: IOTools,
        primary_osd_id: str,
        pool_name: str,
        object_size: int,
        blob_size: int,
        rbd_img: str,
        patterns: List[str],
        alloc_hint: int = 256,
        write_offset: int = None,
        compression_percentage: int = 70,
    ) -> str:
        """
        Perform IO on a pool and fetch OSD log lines produced during the IO window.

        Args:
            write_utility_type (str): Type of write utility to run. Expected values:
                - "librados" to use the C++ helper
                - any other value will use `fio` for RBD writes
            primary_osd_id (str): OSD id
            pool_name (str): Ceph pool name
            object_size (int): Total I/O size to write.
            alloc_hint (int): Bluestore allocation hint
            blob_size (int): Blob or block size
            rbd_img (str): RBD image name when using `fio`.
            patterns (list[str]): List of regex patterns to filter log lines.
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
            f"/var/log/ceph/{fsid}/ceph-osd.{primary_osd_id}.log >"
            f" /var/log/ceph/{fsid}/ceph-osd.{primary_osd_id}.hexa.log"
        )
        _, _ = host.exec_command(sudo=True, cmd=base_cmd_get_log_line)
        filename = "convert_log_hexa_to_decimal.py"
        cmd = (
            f"curl -L "
            f"https://raw.githubusercontent.com/red-hat-storage/cephci/refs/heads/main/tests/rados/{filename} "
            f"-o ~/convert.py; "
            f"python3 ~/convert.py /var/log/ceph/{fsid}/ceph-osd.{primary_osd_id}.hexa.log"
        )
        _, _ = host.exec_command(sudo=True, cmd=cmd)

        base_cmd_get_log_line = (
            f"cat /var/log/ceph/{fsid}/ceph-osd.{primary_osd_id}.hexa_converted.log"
        )

        grep_line = None
        if len(patterns) > 0:
            grep_line = "grep -a -E '"
            for i, pattern in enumerate(patterns):
                if i == 0:
                    grep_line += f"{pattern}"
                else:
                    grep_line += f"|{pattern}"
            grep_line += "'"

        base_cmd_get_log_line = f"{base_cmd_get_log_line}"
        if grep_line:
            base_cmd_get_log_line += f" | {grep_line}"
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
        pattern = (
            r"compr-occup=(0x[0-9a-fA-F]+|\d+)\s+compr-size\?=(0x[0-9a-fA-F]+|\d+)"
        )

        failed_logs = []
        pass_count = 0
        fail_count = 0
        match_count = 0
        for i in range(len(chk_log_msg)):
            log_line = chk_log_msg[i]
            match = re.search(pattern, log_line)
            if match:
                match_count += 1
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
                    if "recompress" in chk_log_msg[i + 2]:
                        log.info("Recompression occurred as expected. PASS")
                        pass_count += 1
                    else:
                        log.info("Recompression did not occur. FAIL")
                        fail_count += 1
                        failed_logs.append({"line": i, "log": chk_log_msg[i : i + 3]})
                else:
                    log.info(
                        "Since gain < recompression_min_gain. "
                        "Recompression should not occur"
                    )
                    if "recompress" in chk_log_msg[i + 2]:
                        log.info("Recompression occurred. FAIL")
                        fail_count += 1
                        failed_logs.append({"line": i, "log": chk_log_msg[i : i + 2]})
                    else:
                        log.info("Recompression did not occur. PASS")
                        pass_count += 1
                log.info(" ---- start of log line ---- ")
                for log_debug_msg in chk_log_msg[i : i + 3]:
                    log.debug(log_debug_msg)
                log.info(" ---- end of log line ---- ")
                log.info(" ---- end of validation ---- ")

        if match_count == 0:
            raise Exception("Bluestore related log lines are not present in OSD logs")

        return [pass_count, fail_count, failed_logs]

    def set_unset_osd_config_and_redeploy(
        self, osd_id, param, value, set=True, without_redeploy=False
    ):
        if set:
            self.mon_obj.set_config(section=f"osd.{osd_id}", name=param, value=value)
        else:
            self.mon_obj.remove_config(section=f"osd.{osd_id}", name=param)
            log.info("Removed config osd.%s %s=%s" % (osd_id, param, value))

        test_host = self.rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
        log.info("Fetched host node for osd.%s -> %s" % (osd_id, test_host))

        if without_redeploy:
            log.info(
                "without_redeploy flag passed, hence skipping redeploy osd step..."
            )
        else:
            cmd = f"ceph orch osd rm {osd_id} --force --zap"
            _, _, _, _ = self.client_node.exec_command(
                cmd=cmd, print_output=True, verbose=True
            )
            log.info("Triggered orchestrator remove for osd.%s" % osd_id)

            end_time = datetime.datetime.now() + datetime.timedelta(seconds=600)
            log.info("Waiting up to 600s for osd.%s removal to complete" % osd_id)
            while end_time > datetime.datetime.now():
                cmd = "ceph orch osd rm status"
                try:
                    status = self.rados_obj.run_ceph_command(cmd=cmd)
                    for osd_id_ in status:
                        if int(osd_id_["osd_id"]) == int(osd_id):
                            log.info("OSDs removal in progress: " + str(osd_id_))
                            time.sleep(10)
                            continue
                        else:
                            break

                except json.decoder.JSONDecodeError:
                    log.info("osd.%s removal completed" % osd_id)
                    break
                except Exception as e:
                    log.error(
                        "osd.%s removal status check failed with exception %s"
                        % (osd_id, e)
                    )
                    raise e
            else:
                log.error(
                    "OSD.%s removal could not be completed within %d secs"
                    % (osd_id, 600)
                )
                raise Exception()

        end_time = datetime.datetime.now() + datetime.timedelta(seconds=600)
        log.info("Waiting up to 600s for osd.%s to redeploy" % osd_id)
        while end_time > datetime.datetime.now():
            try:
                if (
                    wait_for_daemon_status(self.rados_obj, "osd", osd_id, "running")
                    is False
                ):
                    raise Exception(f"OSD {osd_id} failed to come back after removal")
                log.info("osd.%s successfully redeployed" % osd_id)
                break
            except Exception as e:
                log.info("osd.%s redeploy check failed with exception %s" % (osd_id, e))
                log.info(
                    "Waiting for osd.%s to redeploy...retry after 10 seconds" % osd_id
                )
                time.sleep(10)
                continue
        if set:
            config_value = self.mon_obj.get_config(section=f"osd.{osd_id}", param=param)
            log.info(
                "Retrieved config %s for osd.%s -> %s" % (osd_id, param, config_value)
            )

            if int(config_value) != int(value):
                raise Exception(
                    "\nFailed to set bluestore_min_alloc_size on OSD"
                    f"\nconfig min alloc size -> {config_value}"
                    f"\n min alloc size -> {param}"
                )
            log.info("Verified %s == %s on osd.%s" % (param, config_value, osd_id))

    def create_pool_wrapper(self, pool_name: str):
        """
        Create a pool with a single placement group and wait for it to be ready.

        This method creates a new RADOS pool with exactly one placement group (PG)
        and disables PG autoscaling.

        Args:
            pool_name (str): Name of the pool to create.

        Raises:
            AssertionError: If pool creation fails.
            Exception: If the pool's pg_num does not reach 1 within 600 seconds.
        """
        assert self.rados_obj.create_pool(
            pool_name=pool_name,
            disable_pg_autoscale=True,
            app_name="rbd",
            pg_num_max=1,
        )
        pg_num_on_pool = int(
            self.rados_obj.get_pool_property(pool=pool_name, props="pg_num")["pg_num"]
        )
        log.info(f"PG num on the pool: {pg_num_on_pool}")

    def partial_overwrite(self, **kwargs):
        """
        Perform both partial-overwrite and inbetween-overwrite recompression validations.
           a. Partial overwrite:
                1. Creates pool
                2. enables compression
                3. creates RBD image
                4. writes initial IO, performs partial overwrite [ offset = write_size // 2, size = write_size ]
           b. Inbetween overwrite:
                1. Creates pool
                2. enables compression
                3. creates RBD image
                4. writes initial IO, performs partial overwrite [offset = write_size // 4, size = write_size // 2]

        Args (via kwargs):
            compression_algorithm (str): Compression algorithm
            compression_required_ratio (str): compression ratio
            compression_mode (str): Compression mode
            pool_name (str): name for pools (required)
            alloc_hint (int): Object allocation hint
            recompression_min_gain (float): default 1.2.
            write_size (int): Size (bytes) used for IO default is 64000.
            rbd_img (str): rbd image name
        """
        log.info(self.partial_overwrite.__doc__)
        compression_algorithm = kwargs.get("compression_algorithm", "snappy")
        compression_mode = kwargs.get("compression_mode", "force")
        base_pool_name = kwargs["pool_name"]
        alloc_hint = kwargs.get("alloc_hint", BLUESTORE_ALLOC_HINTS.COMPRESSIBLE)
        recompression_min_gain = kwargs.get("recompression_min_gain", 1.2)
        write_size = kwargs.get("write_size", 64000)
        separator = "=" * 80
        pool_level_compression = kwargs.get("pool_level_compression", True)

        info_msg = (
            f"\n {separator} \n[STARTING]"
            f"\ncompression_algorithm -> {compression_algorithm}"
            f"\ncompression_mode -> {compression_mode} "
            f"\nbase_pool_name -> {base_pool_name}"
            f"\n[write] object size -> {write_size} write offset -> {0}"
            f"\n[partial overwrite] object size -> {write_size} write offset -> {write_size//2}"
            f"\n[inbetween overwrite] object size -> {write_size//2} write offset -> {write_size//4}\n{separator}"
        )
        log.info(info_msg)

        # Define overwrite test configurations
        overwrite_tests = [
            {
                "name": "partial",
                "overwrite_size": write_size,
                "overwrite_offset": write_size // 2,
                "description": "partial overwrite",
                "comment": (
                    "[0 ...............500,000]\n"
                    "        [250,000................750,000]"
                ),
            },
            {
                "name": "inbetween",
                "overwrite_size": write_size // 2,
                "overwrite_offset": write_size // 4,
                "description": "inbetween overwrite",
                "comment": (
                    "[0......................................................500,000]\n"
                    "            [125000.....................375000]"
                ),
            },
        ]

        info_msg = (
            "When calculated gain >= recompression_min_gain, recompression should occur."
            "We can confirm that from the logs 'recompress' keyword would be present. "
            "When calculated gain < recompression_min_gain, recompression should not occur."
            "We can confirm that from the logs 'recompress' keyword would not be present."
        )

        # Loop through each overwrite test type
        for test_idx, test_config in enumerate(overwrite_tests, start=1):
            overwrite_size = test_config["overwrite_size"]
            overwrite_offset = test_config["overwrite_offset"]
            test_description = test_config["description"]

            log.info("%s \n %s" % (test_description.upper(), separator))

            # Generate unique pool name for each test
            pool_id = "".join(random.choices(string.ascii_letters + string.digits, k=6))
            pool_name = f"{base_pool_name}-{test_config['name']}-{pool_id}"

            # Generate unique RBD image name for each test
            rbd_img = "rbd_img" + "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )

            # Step 1: Create pool and enable compression, set bluestore_compression_min_blob_size and
            # bluestore_recompression_min_gain
            self.create_pool_wrapper(pool_name=pool_name)
            acting_pg_set = self.rados_obj.get_pg_acting_set(pool_name=pool_name)
            primary_osd_id = acting_pg_set[0]

            self.set_unset_osd_config_and_redeploy(
                primary_osd_id,
                "bluestore_compression_min_blob_size",
                65536,
                set=True,
            )

            self.mon_obj.set_config(
                section=f"osd.{primary_osd_id}",
                name="bluestore_recompression_min_gain",
                value=recompression_min_gain,
            )

            self.enable_compression(
                compression_mode=compression_mode,
                compression_algorithm=compression_algorithm,
                pool_level_compression=pool_level_compression,
                pool_name=pool_name,
            )

            # Step 2: Create RBD image
            cmd = f"rbd create {rbd_img} --size 10G --pool {pool_name}"
            try:
                self.rados_obj.client.exec_command(cmd=cmd, sudo=True)
                log.info("Created device I/O image: %s", rbd_img)
            except Exception as e:
                raise Exception("Failed to create device I/O image: %s", e)

            # Step 3: Perform initial IO
            write_logs = self.write_io_and_fetch_log_lines(
                write_utility_type=IOTools.FIO,
                primary_osd_id=primary_osd_id,
                pool_name=pool_name,
                object_size=write_size,
                alloc_hint=alloc_hint,
                blob_size=write_size,
                rbd_img=rbd_img,
                patterns=["bluecompr", "_do_put_new_blobs", "Estimator", "recompress:"],
            )

            log.info(" ---- write operations logs : start of log line ---- ")
            log.info(" write logs \n%s" % (write_logs))
            log.info(" ----- write operations logs : end of log line ---- ")

            # Step 4: Perform overwrite
            log.info(test_config["comment"])

            overwrite_logs = self.write_io_and_fetch_log_lines(
                write_utility_type=IOTools.FIO,
                primary_osd_id=primary_osd_id,
                pool_name=pool_name,
                object_size=overwrite_size,
                alloc_hint=alloc_hint,
                blob_size=overwrite_size,
                rbd_img=rbd_img,
                patterns=["bluecompr", "_do_put_new_blobs", "Estimator", "recompress:"],
                write_offset=overwrite_offset,
            )

            # Step 5: Validate recompression
            log.info(
                " ---- %s operations logs : start of log line ---- " % test_description
            )
            log.info(
                " %s overwrite logs \n%s"
                % (test_description.capitalize(), overwrite_logs)
            )
            log.info(
                " ----- %s operations logs : end of log line ---- " % test_description
            )

            pass_count, fail_count, failed_logs = self.recompression_checks(
                overwrite_logs, recompression_min_gain
            )

            if fail_count > 0:
                for failed_log_line in failed_logs:
                    log.info(failed_log_line)
                log.error(info_msg)
                # raise Exception(
                #     f"{test_description.capitalize()} overwrite validation failed: " + info_msg
                # )

            # Clean up RBD image after each test
            try:
                cmd = f"rbd rm {rbd_img} --pool {pool_name}"
                self.rados_obj.client.exec_command(cmd=cmd, sudo=True)
                log.info(
                    "Removed RBD image %s after %s test" % (rbd_img, test_description)
                )
            except Exception as e:
                log.warning("Failed to remove RBD image %s: %s" % (rbd_img, e))

            # Cleanup OSD configs for this test
            self.set_unset_osd_config_and_redeploy(
                primary_osd_id,
                "bluestore_compression_min_blob_size",
                65536,
                set=False,
            )

            self.mon_obj.remove_config(
                section=f"osd.{primary_osd_id}",
                name="bluestore_recompression_min_gain",
            )

            # Clean up pool after each test
            if not self.rados_obj.delete_pool(pool=pool_name):
                log.error(
                    "Failed to delete pool %s during cleanup for %s test."
                    % (pool_name, test_description)
                )

            log.info(
                "\n %s \n %s \n %s" % (separator, test_description.upper(), separator)
            )

        log.info(
            "Recompression is as expected for both partial and inbetween overwrites"
        )
        log.error(info_msg)

        info_msg = (
            f"\n {separator} \n[PASS]"
            f"\ncompression_algorithm -> {compression_algorithm}"
            f"\ncompression_mode -> {compression_mode} "
            f"\nbase_pool_name -> {base_pool_name}"
            f"\n[write] object size -> {write_size} write offset -> {0}"
            f"\n[partial overwrite] object size -> {write_size} write offset -> {write_size//2}"
            f"\n[inbetween overwrite] object size -> {write_size//2} write offset -> {write_size//4}\n{separator}"
        )
        log.info(info_msg)

    def min_alloc_size_test(self, **kwargs):
        """
        Test that Blustore inline compression respects the osd bluestore_min_alloc_size setting.
        This test performs the following high-level steps:
        1. Create pool with given name.
        2. Configure the primary OSD for the pool to use passed bluestore_min_alloc_size
            value and force the OSD to reload (orchestrator remove + wait for OSD to come back).
        3. Enable inline compression on the pool with the provided compression parameters.
        4. Create RBD image and write IO using fio to produce objects based on min_alloc_size variations
              - smaller than min_alloc_size (min_alloc_size - 10)
              - larger than min_alloc_size (min_alloc_size + 10)
        5. Verify compression behavior:
              - For object size < min_alloc_size: the object should NOT be compressed
              - For object size > min_alloc_size: the object SHOULD be compressed
        Parameters (passed via kwargs):
        - pool_name (str, required): Name of the pool to create.
        - compression_algorithm (str, optional): Compression algorithm  (default: "snappy").
        - compression_required_ratio (str, optional): (default: "0.875").
        - compression_mode (str, optional): Compression mode (default: "force").
        - min_alloc_size (int, optional): Value to set for bluestore_min_alloc_size (default: 4096).
        """
        log.info(self.__doc__)
        compression_algorithm = kwargs.get("compression_algorithm", "snappy")
        compression_required_ratio = kwargs.get("compression_required_ratio", "0.875")
        compression_mode = kwargs.get("compression_mode", "force")
        pool_name = kwargs["pool_name"]
        min_alloc_size = kwargs.get("min_alloc_size", 4096)
        separator = "=" * 80
        pool_level_compression = kwargs.get("pool_level_compression", True)

        for offset in kwargs.get("min_alloc_size_variations"):  # default -> [-10, 10]
            # write IO of size min_alloc_size + offset
            # object size < min_alloc_size => should NOT be compressed
            # object size > min_alloc_size => should be compressed
            # if offset = -2 and min_alloc_size = 4096 => object size = 4094 (not compressed)
            # if offset = 10 and min_alloc_size = 4096 => object size = 4106 (compressed)
            pool_name = pool_name + str(offset)
            rbd_img = "rbd_img" + "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )

            info_msg = (
                f"\n {separator} \n[Starting test] "
                f"\nMin alloc size -> {min_alloc_size} "
                f"\nobject size -> {min_alloc_size + offset}"
                f"\ncompression_algorithm -> {compression_algorithm}"
                f"\ncompression_mode -> {compression_mode} "
                f"\npool_name -> {pool_name}"
                f"\nrbd image name -> {rbd_img} \n {separator}"
            )
            log.info(info_msg)

            self.create_pool_wrapper(pool_name=pool_name)
            log.info("Created pool %s" % pool_name)

            acting_pg_set = self.rados_obj.get_pg_acting_set(pool_name=pool_name)
            primary_osd_id = acting_pg_set[0]
            log.info(
                "Acting PG set for pool %s -> %s; primary_osd_id -> %s"
                % (pool_name, acting_pg_set, primary_osd_id)
            )

            self.set_unset_osd_config_and_redeploy(
                osd_id=primary_osd_id,
                param="bluestore_min_alloc_size",
                value=min_alloc_size,
                set=True,
            )
            log.info(
                "Set config osd.%s bluestore_min_alloc_size=%s"
                % (primary_osd_id, min_alloc_size)
            )

            self.enable_compression(
                compression_mode=compression_mode,
                compression_algorithm=compression_algorithm,
                pool_level_compression=pool_level_compression,
                pool_name=pool_name,
            )
            log.info(
                "Enabled inline compression on pool %s (mode=%s, algo=%s, req_ratio=%s)"
                % (
                    pool_name,
                    compression_mode,
                    compression_algorithm,
                    compression_required_ratio,
                )
            )

            cmd = f"rbd create {rbd_img} --size 10G --pool {pool_name}"
            try:
                self.rados_obj.client.exec_command(cmd=cmd, sudo=True)
                log.info("Created device I/O image: %s" % rbd_img)
            except Exception as e:
                raise Exception("Failed to create device I/O image: %s" % e)

            _ = self.write_io_and_fetch_log_lines(
                write_utility_type=IOTools.FIO,
                primary_osd_id=primary_osd_id,
                pool_name=pool_name,
                object_size=min_alloc_size + offset,
                blob_size=min_alloc_size + offset,
                rbd_img=rbd_img,
                patterns=[],
            )
            log.info(
                "Completed IO with object_size=%s (min_alloc_size + offset) on pool %s"
                % (min_alloc_size + offset, pool_name)
            )

            pool_id = self.rados_obj.get_pool_id(pool_name)
            log.info("Pool %s has id %s" % (pool_name, pool_id))
            time.sleep(5)

            cmd = f"ceph tell osd.{primary_osd_id} bluestore collections | grep ^{pool_id}"
            collections = self.client_node.exec_command(cmd=cmd, print_output=True)[0]
            object_name = None
            log.info(
                "Fetched bluestore collections for pool %s on osd.%s"
                % (pool_name, primary_osd_id)
            )
            log.info(collections)

            for collection in collections.split("\n"):
                if collection == "":
                    continue
                cmd = f"ceph tell osd.{primary_osd_id} bluestore list {collection}"
                out = self.client_node.exec_command(cmd=cmd, print_output=True)
                out = str(out[0])
                log.info(
                    "Blustore list output for osd.%s (truncated): %s"
                    % (primary_osd_id, out[:200])
                )

                for object in out.split("\n"):
                    if "rbd_data" in object:
                        object_name = object
                        break

            if object_name is None:
                raise Exception("Object not found in OSD")
            log.info("Found bluestore object: %s" % object_name)

            # Object size < Min alloc size, should not compress
            cmd = f'ceph tell osd.{primary_osd_id} bluestore onode metadata "{object_name}"'
            primary_osd_metadata, _, _, _ = self.client_node.exec_command(
                cmd=cmd, print_output=True, check_ec=True, verbose=True
            )
            log.info(
                "Fetched onode metadata for %s on osd.%s"
                % (object_name, primary_osd_id)
            )

            if offset < 0:
                info_msg = f"\nVerifying object size < min_alloc_size -> {min_alloc_size + offset} < {min_alloc_size}"
                log.info(info_msg)
                for line in primary_osd_metadata.split("\n"):
                    log.info(line)
                    if line.startswith("Blob") and "len=" in line:
                        raise Exception(
                            "Expectation :- When Object size < min alloc size = Data should not be compressed"
                        )
            else:
                info_msg = f"\nVerifying object size > min_alloc_size -> {min_alloc_size + offset} > {min_alloc_size}"
                log.info(info_msg)
                for line in primary_osd_metadata.split("\n"):
                    if line.startswith("Blob") and "len=" not in primary_osd_metadata:
                        raise Exception(
                            "Expectation :- When Object size > min alloc size = Data should compress"
                        )

            self.set_unset_osd_config_and_redeploy(
                osd_id=primary_osd_id,
                param="bluestore_min_alloc_size",
                value=min_alloc_size,
                set=False,
            )
            log.info(
                "Removed config bluestore_min_alloc_size for osd.%s" % primary_osd_id
            )

            log.info("Attempting to delete pool: %s" % pool_name)
            if not self.rados_obj.delete_pool(pool=pool_name):
                log.error("Failed to delete pool %s during final cleanup." % pool_name)

            info_msg = (
                f"\n {separator} \n[PASS] "
                f"\nMin alloc size -> {min_alloc_size} "
                f"\nobject size -> {min_alloc_size + offset}"
                f"\ncompression_algorithm -> {compression_algorithm}"
                f"\ncompression_mode -> {compression_mode} "
                f"\npool_name -> {pool_name}"
                f"\nrbd image name -> {rbd_img}"
                f"\nlogs -> {primary_osd_metadata} \n {separator}"
            )
            log.info(info_msg)

    def fetch_blobs_from_osd(
        self,
        osd_id: str,
        objectstore_obj: objectstoreToolWorkflows,
        pool_name: str,
        obj_name: object = "rbd_data",
    ) -> object:
        """
        Fetch blobs from the specified OSD for the given pool.

        Queries bluestore collections for the pool, finds rbd_data objects,
        and returns the object dump containing blob information.
        """
        pool_id = self.rados_obj.get_pool_id(pool_name)
        log.info("Pool %s has id %s" % (pool_name, pool_id))
        time.sleep(5)

        cmd = f"ceph tell osd.{osd_id} bluestore collections | grep ^{pool_id}"
        collections = self.client_node.exec_command(cmd=cmd, print_output=True)[0]
        object_name = None

        for collection in collections.split("\n"):
            if collection == "":
                continue
            cmd = f"ceph tell osd.{osd_id} bluestore list {collection}"
            out = self.client_node.exec_command(cmd=cmd, print_output=True)
            out = str(out[0])
            log.info(
                "Blustore list output for osd.%s (truncated): %s" % (osd_id, out[:200])
            )

            for object in out.split("\n"):
                if obj_name in object:
                    object_name = object
                    break

        if object_name is None:
            raise Exception("Object not found in OSD")
        log.info("Found bluestore object: %s" % object_name)

        object_name = object_name.split(":head#")[0].split(":::")[1]
        obj_json = objectstore_obj.list_objects(
            osd_id=int(osd_id), obj_name=object_name
        ).strip()
        if not obj_json:
            log.error(f"Object {object_name} could not be listed in OSD {osd_id}")
            return 1
        log.info(obj_json)
        out = objectstore_obj.fetch_object_dump(osd_id=int(osd_id), obj=obj_json)
        return out

    def validate_blobs_are_compressed(
        self, osd_id: str, objectstore_obj: objectstoreToolWorkflows, pool_name: str
    ):
        """
        Validate that blobs are compressed by checking compressed_length > 0.
        """
        blobs_json = self.fetch_blobs_from_osd(
            osd_id=osd_id, objectstore_obj=objectstore_obj, pool_name=pool_name
        )
        blobs_json = json.loads(blobs_json)
        log.info(
            "Blobs JSON on OSD.%s: %s" % (osd_id, json.dumps(blobs_json, indent=4))
        )
        for blob in blobs_json["onode"]["extents"][:-1]:
            if not (int(blob["blob"]["compressed_length"]) > 0):
                return False
        return True

    def get_pool_stats(self, pool_name: str):
        """
        Retrieves the default value of the Bluestore compression required ratio for a given pool.

        Args:
            rados_obj (object): RadosOrchestrator object
            mon_obj (object): MonConfigMethods object
            pool_name (str): The name of the pool for which the compression ratio is required.

        Returns:
            str: bluestore_compression_required_ratio
        """
        try:
            pool_stats = self.rados_obj.run_ceph_command(cmd="ceph df detail")["pools"]
            pool_stats_after_compression = [
                detail for detail in pool_stats if detail["name"] == pool_name
            ][0]["stats"]
            return pool_stats_after_compression
        except KeyError as e:
            err_msg = f"No stats about the pools requested found on the cluster {e}"
            log.error(err_msg)
            raise Exception(err_msg)


class CompressionIO:
    def __init__(
        self,
        rados_obj,
        mon_obj,
        client_node,
        cephadm,
        workload_type,
        rgw_endpoint,
        rgw_secret,
        rgw_key,
    ):
        self.rados_obj = rados_obj
        self.mon_obj = mon_obj
        self.client_node = client_node
        self.cephadm = cephadm
        self.workload_type = workload_type
        self.rgw_endpoint = rgw_endpoint
        self.rgw_key = rgw_key
        self.rgw_secret = rgw_secret

    def write(
        self,
        file_size: str,
        compression_percentage: str,
        file_full_path: str,
        offset: str,
        rbd_cephfs_folder_full_path: str,
        rgw_bucket_name: str,
    ):
        timestamp = get_cluster_timestamp(self.rados_obj.node)
        fio_test_name = "fio_test_" + str(timestamp)
        log.info("== performing write operation ==")
        cmd = f"fio \
          --name={fio_test_name} \
          --ioengine=libaio \
          --iodepth=1 \
          --rw=write \
          --size={file_size} \
          --nrfiles=1 \
          --offset={offset} \
          --filename={file_full_path} \
          --direct=1 \
          --group_reporting=1 \
          --zero_buffers=0 \
          --refill_buffers=1 \
          --buffer_compress_percentage={compression_percentage} \
          --verify=md5 \
          --do_verify=0 \
          --verify_dump=1"

        self.rados_obj.client.exec_command(cmd=cmd, sudo=True, check_ec=True)

        if self.workload_type == "rbd" or self.workload_type == "cephfs":
            cmd = f"cp {file_full_path} {rbd_cephfs_folder_full_path}"
            self.rados_obj.client.exec_command(cmd=cmd, sudo=True, check_ec=True)
        elif self.workload_type == "rgw":
            cmd = (
                f"aws s3 cp {file_full_path} s3://{rgw_bucket_name}/ "
                f"--endpoint-url http://{self.rgw_endpoint} --acl private"
            )
            self.rados_obj.client.exec_command(cmd=cmd, sudo=True, check_ec=True)
        time.sleep(60)  # waiting for ceph df to settle down
        log.info("== completed write operation ==")
        return fio_test_name

    def clean(
        self,
        rbd_cephfs_folder_full_path,
        rgw_bucket_name,
        pool_name,
        compression_obj: BluestoreDataCompression,
    ):
        if self.workload_type == "rbd" or self.workload_type == "cephfs":
            cmd = f"rm -rf {rbd_cephfs_folder_full_path}/*"
            self.rados_obj.client.exec_command(cmd=cmd, sudo=True)
        elif self.workload_type == "rgw":
            cmd = f"radosgw-admin bucket list --bucket={rgw_bucket_name}"
            out = self.rados_obj.client.exec_command(
                cmd=cmd, sudo=True, pretty_print=True, check_ec=True
            )
            out = json.loads(out[0])
            for obj in out:
                obj_name = obj["name"]
                cmd = f"radosgw-admin object rm --bucket={rgw_bucket_name} --object={obj_name}"
                self.rados_obj.client.exec_command(cmd=cmd, sudo=True, check_ec=True)
            cmd = "radosgw-admin gc process --include-all"
            self.rados_obj.client.exec_command(cmd=cmd, sudo=True, check_ec=True)

        # rbd pools do not delete the objects
        # If the files in the filesystem are deleted from mount point.
        cmd = f"rados purge {pool_name} --yes-i-really-really-mean-it"

        # Waiting till objects reaches 0.
        for _ in range(10):
            self.rados_obj.client.exec_command(
                cmd=cmd, sudo=True, check_ec=True, pretty_print=True, verbose=True
            )
            pool_stats = compression_obj.get_pool_stats(pool_name)
            if pool_stats["objects"] == 0:
                log.info("Objects reached 0")
                break
            else:
                log.info("sleeping for 20 seconds...")
                time.sleep(20)
        else:
            raise Exception("Objects did not reach 0 within timeout")

    def read_and_verify_checksum(
        self,
        fio_job_name: str,
        file_size: str,
        compression_percentage: str,
        file_full_path: str,
        offset: str,
        rbd_cephfs_folder_full_path: str,
        rgw_bucket_name: str,
    ):
        log.info("== performing read integrity test ==")
        cmd = f"fio \
          --name={fio_job_name} \
          --ioengine=libaio \
          --iodepth=1 \
          --rw=read \
          --size={file_size} \
          --nrfiles=1 \
          --offset={offset} \
          --filename={file_full_path} \
          --direct=1 \
          --group_reporting=1 \
          --zero_buffers=0 \
          --refill_buffers=1 \
          --buffer_compress_percentage={compression_percentage} \
          --verify=md5 \
          --do_verify=1 \
          --verify_fatal=1 \
          --verify_state_load=1"

        # Raises exception if error code != 0
        self.rados_obj.client.exec_command(
            cmd=cmd, sudo=True, check_ec=True, pretty_print=True
        )
        log.info("== completed read integrity test ==")
