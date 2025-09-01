import time
from collections import namedtuple

from ceph.ceph import CephNode
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.test_stretch_site_down import get_stretch_site_hosts
from tests.rados.test_stretch_site_reboot import get_host_obj_from_hostname
from utility.log import Log

log = Log(__name__)

Hosts = namedtuple("Hosts", ["dc_1_hosts", "dc_2_hosts", "tiebreaker_hosts"])


class StretchMode:
    """
    Usage:-
        config = {
            "rados_obj": rados_obj,
            "pool_obj": pool_obj,
            "tiebreaker_mon_site_name": tiebreaker_mon_site_name,
            "stretch_bucket": stretch_bucket,
            "client_node": client_node,
        }
        stretch_mode = StretchMode(**config)
        stretch_mode.enable_stretch_mode(tiebreaker_mon)
    """

    def __init__(self, **kwargs):
        self.rados_obj: RadosOrchestrator = kwargs.get("rados_obj")
        self.tiebreaker_mon_site_name = kwargs.get(
            "tiebreaker_mon_site_name", "tiebreaker"
        )
        self.tiebreaker_mon = self.get_tiebreaker_mon()
        self.pool_obj = kwargs.get("pool_obj")
        self.custom_crush_rule = {}
        self.stretch_bucket = kwargs.get("stretch_bucket", "datacenter")

        # parameters to be populated by segregate_hosts_based_on_stretch_bucket()
        self.site_1_hosts = None
        self.site_2_hosts = None
        self.tiebreaker_hosts = None
        self.site_1_name = None
        self.site_2_name = None
        self.segregate_hosts_based_on_stretch_bucket()

        self.client_node = kwargs.get("client_node", None)

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
        if (
            self.rados_obj.run_ceph_command(cmd=stretch_enable_cmd, print_output=True)
            is None
        ):
            raise Exception("Failed to enable stretch mode")
        log.info("Successfully enabled stretch mode")

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

    def segregate_hosts_based_on_stretch_bucket(self):
        """
        Method to segregate hosts based on stretch bucket.
        Populates site_1_hosts, site_2_hosts and tiebreaker_hosts
        Populates site_1_name, site_2_name
        Returns:
            None
        """
        osd_tree_cmd = "ceph osd tree"
        buckets = self.rados_obj.run_ceph_command(osd_tree_cmd)
        dc_buckets = [
            d for d in buckets["nodes"] if d.get("type") == self.stretch_bucket
        ]
        while len(dc_buckets) > 2:
            dc_buckets.pop()
        dc_2 = dc_buckets.pop()
        dc_2_name = dc_2["name"]
        dc_1 = dc_buckets.pop()
        dc_1_name = dc_1["name"]
        all_hosts = get_stretch_site_hosts(
            rados_obj=self.rados_obj,
            tiebreaker_mon_site_name=self.tiebreaker_mon_site_name,
        )
        self.site_1_hosts = all_hosts.dc_1_hosts
        self.site_2_hosts = all_hosts.dc_2_hosts
        self.tiebreaker_hosts = all_hosts.tiebreaker_hosts
        self.site_1_name = dc_1_name
        self.site_2_name = dc_2_name
        log.debug(f"Hosts present in Datacenter : {dc_1_name} : {self.site_1_hosts}")
        log.debug(f"Hosts present in Datacenter : {dc_2_name} : {self.site_2_hosts}")
        log.debug(
            f"Hosts present in Datacenter : {self.tiebreaker_mon_site_name} : { self.tiebreaker_hosts}"
        )

    def write_io_and_validate_objects(
        self, pool_name: str, init_objects: int, obj_name: str
    ) -> object:
        """
        Method to write IO and validate the count of objects.
        Args:
            init_objects (int) : Initial number of objects on the pool
            pool_name (str) : name of the pool to write IO and validate
            obj_name (str) : name of the objects to write
        Returns:
            None
        Raises:
            Exception If the number of objects post IO is less than or equal to init_objects
        """
        log_msg = (
            f"\n Writing IO to the pool {pool_name}."
            f"\n init_objects -> {init_objects}"
        )
        log.info(log_msg)

        if (
            self.pool_obj.do_rados_put(
                client=self.client_node,
                pool=pool_name,
                nobj=200,
                timeout=100,
                obj_name=obj_name,
            )
            == 1
        ):
            err_msg = f"Failed to write IO using rados put command to pool {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.debug("sleeping for 20 seconds for the objects to be displayed in ceph df")
        time.sleep(20)

        pool_stat = self.rados_obj.get_cephdf_stats(pool_name=pool_name)
        current_objects = pool_stat["stats"]["objects"]
        log.debug(pool_stat)

        # Objects should be more than the initial no of objects
        if current_objects <= init_objects:
            log.error(
                "Write ops should be possible, number of objects in the pool has not changed"
            )
            raise Exception(
                f"Pool {pool_name} has {pool_stat['stats']['objects']} objs"
            )
        log_msg = (
            f"\n Post IO to the pool {pool_name}."
            f"\n init_objects -> {init_objects}"
            f"\n current_objects -> {current_objects}"
        )
        log.info(log_msg)

    def is_degraded_stretch_mode(self):
        """
        Method to check if stretch mode is degraded stretch mode or not.
        Retuns:
            True:- If Ceph cluster is in degraded stretch mode
            False:- If Ceph cluster is not in degraded stretch mode
        """
        stretch_details = self.rados_obj.get_stretch_mode_dump()
        if not stretch_details["degraded_stretch_mode"]:
            log.error(
                f"Stretch Cluster is not marked as degraded even though we have DC down : {stretch_details}"
            )
            return False
        return True


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
        out = self.rados_obj.run_ceph_command(
            command, print_output=True, client_exec=True
        )
        if out is None:
            raise Exception("Failed to disable stretch mode")
        log.info("Successfully disabled stretch mode")

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
        pool_detail = self.rados_obj.run_ceph_command(cmd=cmd, client_exec=True)
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
            cmd=cmd, print_output=True, client_exec=True
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
            cmd=cmd, print_output=True, client_exec=True
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


def simulate_netsplit_between_hosts(rados_obj, group1, group2):
    """
    Method to simulate netsplit between hosts. Netsplit will be simulate by dropping incoming & outgoing
    traffic using "iptables -A INPUT -s {target_host_ip} -j DROP; iptables -A OUTPUT -d {target_host_ip} -j DROP"
    group1 : node1, node2, node3
    group2 : node4, node5

    All incoming/outgoing traffic from group2 hosts will be blocked on group1 hosts.
    Ip table rules will be added on:-
     node4 to drop incoming/outgoing packets to/from node1
     node4 to drop incoming/outgoing packets to/from node2
     node4 to drop incoming/outgoing packets to/from node3
     node5 to drop incoming/outgoing packets to/from node1
     node5 to drop incoming/outgoing packets to/from node2
     node5 to drop incoming/outgoing packets to/from node3

    Args:
        group1: List of hosts ( type: list of strings containing hostnames ) ["host1", "host2"]
        group2: List of hosts ( type: list of strings containing hostnames ) ["host3", "host4"]
    Returns:
        None
    """
    info_msg = f"Adding IPtable rules between {group1} and {group2}"
    log.info(info_msg)

    for host1 in group1:
        target_host_obj = rados_obj.get_host_object(hostname=host1)
        if not target_host_obj:
            err_msg = f"target host : {host1} not found . Exiting..."
            log.error(err_msg)
            raise Exception("Test execution Failed")
        debug_msg = f"Proceeding to add IPtables rules to block incoming - outgoing traffic to host {host1} "
        log.debug(debug_msg)
        for host2 in group2:
            source_host_obj = rados_obj.get_host_object(hostname=host2)
            debug_msg = (
                f"Proceeding to add IPtables rules to block incoming - outgoing traffic to host {host1}"
                f". Applying rules on host : {host2}"
            )
            log.debug(debug_msg)
            if not source_host_obj:
                err_msg = f"Source host : {host2} not found . Exiting..."
                log.error(err_msg)
            if not rados_obj.block_in_out_packets_on_host(
                source_host=source_host_obj, target_host=target_host_obj
            ):
                err_msg = f"Failed to add IPtable rules to block {host1} on {host2}"
                log.error(err_msg)
                raise Exception("Test execution Failed")

    info_msg = f"Completed adding IPtable rules between {group1} and {group2}"
    log.info(info_msg)


def flush_ip_table_rules_on_all_hosts(rados_obj, hosts):
    """
    Method to flush iptable rules on all hosts part of the cluster
    Executes iptables -F and reboots the host
    Returns:
        None
    """
    log.info("Proceeding to flush IP table rules on all hosts")
    for hostname in hosts:
        host = get_host_obj_from_hostname(hostname=hostname, rados_obj=rados_obj)
        debug_msg = f"Proceeding to flush iptable rules on host : {host.hostname}"
        log.debug(debug_msg)
        host.exec_command(sudo=True, cmd="iptables -F", long_running=True)
        host.exec_command(sudo=True, cmd="reboot", check_ec=False)
        time.sleep(20)
    log.info("Completed flushing IP table rules on all hosts")
