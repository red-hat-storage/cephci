from collections import namedtuple

from ceph.ceph import CephNode
from utility.log import Log

log = Log(__name__)

Hosts = namedtuple("Hosts", ["dc_1_hosts", "dc_2_hosts", "tiebreaker_hosts"])


class StretchMode:
    def __init__(self, **kwargs):
        self.rados_obj = kwargs.get("rados_obj")
        self.tiebreaker_mon_site_name = kwargs.get(
            "tiebreaker_mon_site_name", "tiebreaker"
        )
        self.tiebreaker_mon = self.get_tiebreaker_mon()
        self.pool_obj = kwargs.get("pool_obj")
        self.custom_crush_rule = {}

    def get_tiebreaker_mon(self):
        """
        Identifies and returns the name of the tiebreaker monitor in a Ceph cluster.

        This method runs the `ceph mon dump` command to retrieve monitor information,
        then searches for a monitor whose CRUSH location matches the specified
        tiebreaker site name. If found, it returns the name of that monitor.

        Returns:
            str or None: The name of the tiebreaker monitor if found, otherwise None.
        """
        mon_dump_cmd = "ceph mon dump"
        mons = self.rados_obj.run_ceph_command(mon_dump_cmd)
        tiebreaker_mon = None
        for mon in mons["mons"]:
            if (
                mon["crush_location"]
                == "{datacenter=" + f"{self.tiebreaker_mon_site_name}" + "}"
            ):
                tiebreaker_mon = mon["name"]
        return tiebreaker_mon

    def enable_stretch_mode(self, tiebreaker_mon):
        """
        Enables stretch mode in the Ceph cluster using the specified tiebreaker monitor.

        Args:
            tiebreaker_mon (str): The name of the monitor to be used as the tiebreaker.

        Returns:
            None

        Raises Exception:-
            When command `ceph mon enable_stretch_mode {tiebreaker_mon} stretch_rule datacenter` fails
        """
        stretch_enable_cmd = (
            f"ceph mon enable_stretch_mode {tiebreaker_mon} stretch_rule datacenter"
        )
        self.rados_obj.run_ceph_command(cmd=stretch_enable_cmd, print_output=True)

    def create_pool_and_write_io(self, pool_name: str, client_node: CephNode):
        """
        method to create pool with passed <pool_name> and writes IO to the pool
        using rados put command.
        """
        if not self.rados_obj.create_pool(pool_name=pool_name):
            err_msg = f"Failed to create pool : {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)
        if (
            self.pool_obj.do_rados_put(
                client=client_node, pool=pool_name, nobj=200, timeout=100
            )
            == 1
        ):
            err_msg = f"Failed to write IO using rados put command to pool {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)

    def create_or_retrieve_crush_rule(self, crush_rule_name: str):
        """
        method to create CRUSH rule if it does not exist and if CRUSH rule exists
        It will return the CRUSH rule ID.
        Args:
                crush_rule_name: (type: str) Name of the crush rule to create or retrieve
        Returns:
                CRUSH rule ID
        Raise Exception:
            If `ceph osd crush rule create-simple` or `ceph osd crush rule dump <rule name> fails`
        """
        if self.custom_crush_rule.get(crush_rule_name, None) is not None:
            return self.custom_crush_rule["crush_rule_name"]

        command = f"ceph osd crush rule create-simple {crush_rule_name} default osd"
        self.rados_obj.run_ceph_command(
            cmd=command, client_exec=True, print_output=True
        )

        command = f"ceph osd crush rule dump {crush_rule_name}"
        out = self.rados_obj.run_ceph_command(
            cmd=command, client_exec=True, print_output=True
        )
        self.custom_crush_rule[crush_rule_name] = out["rule_id"]
        return self.custom_crush_rule[crush_rule_name]


class RevertStretchModeFunctionalities(StretchMode):
    """
    Class contains revert stretch mode related methods.
    validate_pool_configurations_post_revert()
    validate_osd_configurations_post_revert()
    validate_mon_configurations_post_revert()
    revert_stretch_mode()
    """

    def revert_stretch_mode(self, crush_rule_name=None):
        """
        method to exit stretch mode. Executes command -
        ceph mon disable_stretch_mode [{crush_rule}] --yes-i-really-mean-it

        args:
            crush_rule: (Optional , type: string) Name of the crush rule to revert
        returns:
            None
        raises Exception:
            When command execution fails
        """
        command = "ceph mon disable_stretch_mode"
        if crush_rule_name is not None:
            command += f" {crush_rule_name}"
        command += " --yes-i-really-mean-it"
        _ = self.rados_obj.run_ceph_command(
            command, print_output=True, client_exec=True
        )

    def validate_pool_configurations_post_revert(self, pool_properties):
        """
        method takes pool_properties as input in dict and validates each property on all pools
        in `ceph osd pool ls detail`.
        Example:-
            pool_properties = {
                "size": "3",
                "min_size": "2",
                "crush_rule": "0",
            }
            revert_stretch_mode_obj = RevertStretchModeFunctionalities(**config)
            revert_stretch_mode_obj.validate_pool_configurations_post_revert(pool_properties)
        Args:-
            pool_properties: (type : dict) properties of each pool to validate.
        Returns:-
            None
        Raises Exception:-
            When the one of the pool properties are not matching the passed properties.
        """
        log.info("Validating pool properties for each pool : ")
        log.info(pool_properties)
        cmd = "ceph osd pool ls detail"
        pool_detail = self.rados_obj.run_ceph_command(cmd=cmd)
        for pool_detail in pool_detail:
            pool_name = pool_detail["pool_name"]
            for property_name, property_value in pool_properties.items():
                current_pool_property = pool_detail[property_name]
                log_msg = (
                    f"\nPool name: {pool_name}"
                    f"\nExpected {property_name} -> {property_value}"
                    f"\nCurrent {property_name} -> {current_pool_property}"
                )
                log.info(log_msg)
                if str(current_pool_property) != str(property_value):
                    err_msg = f"Failed to reset pool configuration {property_name} for pool {pool_name}"
                    log.error(err_msg)
                    raise Exception(err_msg + log_msg)
        log.info("Successfully validated pool properties for all pool : ")

    def validate_osd_configurations_post_revert(self, expected_osd_map_values):
        """
        method takes expected_osd_map_values as input in dict format and validates properties of OSD
        MAP in "ceph osd dump" output.
        Example:-
            expected_osd_map_values = {
                "stretch_mode_enabled": False,
                "stretch_bucket_count": 0,
                "degraded_stretch_mode": 0,
                "recovering_stretch_mode": 0,
                "stretch_mode_bucket": 0
            }
            revert_stretch_mode_obj = RevertStretchModeFunctionalities(**config)
            revert_stretch_mode_obj.validate_osd_configurations_post_revert(expected_osd_map_values)
        Args:-
            expected_osd_map_values: (type : dict) OSD MAP property to validate
        Returns:-
            None
        Raises Exception:-
            When the current OSD MAP property does not match the expected property.
        """
        cmd = "ceph osd dump"
        ceph_osd_dump_output = self.rados_obj.run_ceph_command(
            cmd=cmd, print_output=True
        )
        osd_map_configs_to_validate = (
            expected_osd_map_values.keys()
        )  # ["stretch_mode_enabled", "stretch_bucket_count".....]
        log.info("Validating OSD map configs of stretch mode")
        for config_name in osd_map_configs_to_validate:
            expected_config_value = expected_osd_map_values[config_name]
            current_config_value = ceph_osd_dump_output["stretch_mode"][config_name]

            log_msg = (
                f" Expected {config_name} -> {expected_config_value}"
                f" Current {config_name} -> {current_config_value}"
            )
            log.info(log_msg)
            if str(current_config_value) != str(expected_config_value):
                log.error(
                    "Failed to reset Stretch mode related OSD map configurations."
                    + log_msg
                )
                raise Exception(log_msg)
        log.info("Successfully validated OSD map configs of stretch mode")

    def validate_mon_configurations_post_revert(self, expected_mon_map_values):
        """
        method takes expected_mon_map_values as input in dict and validates each property
        in `ceph mon dump` command.
        Example:-
            expected_mon_map_values = {
                "tiebreaker_mon": "",
                "stretch_mode": False,
                "tiebreaker_mon": "",
            }
            revert_stretch_mode_obj = RevertStretchModeFunctionalities(**config)
            revert_stretch_mode_obj.validate_mon_configurations_post_revert(expected_mon_map_values)
        Args:-
            expected_mon_map_values: (type : dict) properties of MON map to validate.
        Returns:-
            None
        Raises Exception:-
            When the expected MON map property does not match the current MON map values.
        """
        cmd = "ceph mon dump"
        ceph_mon_dump_output = self.rados_obj.run_ceph_command(
            cmd=cmd, print_output=True
        )

        mon_map_configs_to_validate = (
            expected_mon_map_values.keys()
        )  # ["tiebreaker_mon", "stretch_mode", ..... ]

        log.info("Validating MON map configs of stretch mode")
        for config_name in mon_map_configs_to_validate:
            expected_config_value = expected_mon_map_values[config_name]
            current_config_value = ceph_mon_dump_output[config_name]

            log_msg = (
                f" Expected {config_name} -> {expected_config_value}"
                f" Current {config_name} -> {current_config_value}"
            )
            log.info(log_msg)

            if str(current_config_value) != str(expected_config_value):
                log.error(
                    "Failed to reset Stretch mode related MON map configurations."
                    + log_msg
                )
                raise Exception(log_msg)

        log.info("Successfully validated MON map configs of stretch mode")
