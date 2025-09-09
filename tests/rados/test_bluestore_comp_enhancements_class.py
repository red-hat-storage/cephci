import concurrent.futures

from ceph.ceph import CephNode
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_scrub import RadosScrubber
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.rados_test_util import wait_for_daemon_status
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log

log = Log(__name__)


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
