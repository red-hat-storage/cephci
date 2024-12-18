"""
monitor_configurations.py module is a rados layer module to test Various monitor configurations

1. MonElectionStrategies : Module that allows to set and test various election strategies employed by monitors for
elections
    It allows us to change b/w various election strategies:
    1 - “classic”
    2 - “disallow”
    3 - “connectivity”

    Doc ref: https://docs.ceph.com/en/latest/rados/operations/change-mon-elections/

2. MonConfigMethods: Module that allows to set, remove and verify various config params into monitor
 configuration database.
"""

import json
import re
import time

from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


class MonElectionStrategies:
    """
    MonElectionStrategies class contains various methods that change and verify the various available
    election strategies
    Usage: The class is initialized with the RadosOrchestrator object for various operations
    """

    def __init__(self, rados_obj: RadosOrchestrator):
        """
        initializes the env to run rados commands
        Args:
            rados_obj: CephAdmin object
        """
        self.rados_obj = rados_obj

    def set_disallow_mon(self, mon):
        """
        allows to add mon to  disallowed leaders list in ceph
        Args:
            mon: name of the monitor to be added
        Returns: True -> Pass, False -> fail
        """
        cmd = f" ceph mon add disallowed_leader {mon}"
        self.rados_obj.node.shell([cmd])

        # Checking if the mon is successfully added to list
        if mon not in self.get_disallowed_mon_list():
            log.error(f"given mon {mon} not was not added to disallowed mon list")
            return False

        log.debug(f"Mon {mon} added to disallowed leader mon list")
        return True

    def remove_disallow_mon(self, mon):
        """
        allows to remove a mon from the disallowed leaders' list
        Args:
            mon: name of the monitor to be removed
        Returns: True -> Pass, False -> fail
        """
        cmd = f" ceph mon rm disallowed_leader {mon}"
        self.rados_obj.node.shell([cmd])

        # Checking if the mon is successfully removed to list
        if mon in self.get_disallowed_mon_list():
            log.error(f"given mon {mon} not was not removed to disallow mon list")
            return False

        log.debug(f"Mon {mon} removed from disallowed leader mon list")
        return True

    def get_mon_quorum(self):
        """
        Fetches mon details and returns the names of monitors present in the quorum
        Returns: dictionary with mon names as keys and Rank as value
                eg: {'magna045': 0, 'magna046': 1, 'magna047': 2}
        """
        cmd = "ceph mon dump"
        quorum = self.rados_obj.run_ceph_command(cmd)
        mon_members = {}
        for entry in quorum["mons"]:
            mon_members.setdefault(entry["name"], entry["rank"])
        return mon_members

    def get_disallowed_mon_list(self):
        """
        Fetches mon details and returns the names of monitors present in the disallowed list
        Returns: list of mons present in the mon disallowed leader list
        """
        cmd = "ceph mon dump"
        quorum = self.rados_obj.run_ceph_command(cmd)
        return quorum["disallowed_leaders: "].split(",")

    def get_mon_quorum_leader(self):
        """
        Fetches mon details and returns the name of quorum leader
        Returns: name of the leader mon in quorum
        """
        cmd = "ceph mon stat"
        quorum = self.rados_obj.run_ceph_command(cmd)
        return quorum["leader"]

    def set_election_strategy(self, mode):
        """
        sets the election strategy for mon election on the cluster
        Args:
            mode:
                “classic”
                “disallow”
                “connectivity”
        Returns: True -> pass, False -> fail

        """
        cmd = f"ceph mon set election_strategy {mode}"
        self.rados_obj.node.shell([cmd])
        # sleep for 1  sec after command execution for election strategy change
        time.sleep(1)

        strategies = {1: "classic", 2: "disallow", 3: "connectivity"}
        # Verifying the strategy set
        set_strategy = self.get_election_strategy()
        if strategies[set_strategy] != mode:
            log.error(
                f"could not set the election strategy to : {mode}. "
                f"currently set : {strategies[set_strategy]}"
            )
            return False

        log.debug("Changed the election strategy")
        return True

    def get_election_strategy(self):
        """
        returns the election strategy set for mon elections
        Returns:
            1 for “classic”
            2 for “disallow”
            3 for “connectivity”
        """
        cmd = "ceph mon dump"
        quorum = self.rados_obj.run_ceph_command(cmd)
        return quorum["election_strategy"]

    def get_osd_hosts(self):
        """
        lists the names of the OSD hosts in the cluster
        Returns: list of osd host names as used in the crush map

        """
        cmd = "ceph osd tree"
        osds = self.rados_obj.run_ceph_command(cmd)
        return [entry["name"] for entry in osds["nodes"] if entry["type"] == "host"]


class MonConfigMethods:
    """
    MonConfigMethods class contains various methods that change and verify the various available config parameters
    Usage: The class is initialized with the RadosOrchestrator object for various operations
    """

    def __init__(self, rados_obj: RadosOrchestrator):
        """
        initializes the env to run rados commands
        Args:
            rados_obj: CephAdmin object
        """
        self.rados_obj = rados_obj

    def set_config(self, **kwargs):
        """
        sets the sent config param in monitor config database
        Config parameters
        Args:
            **kwargs: Any other param that needs to be set
                1. section: which section of daemons to target
                    allowed values: global, mon, mgr, osd, mds, client
                2. name: name of the config param for the selection
                3. value: Value to be set
            Optional args:
                4. location_type: CRUSH property like rack or host
                5. device_class: Value for location_type
                6. no_delay: should be set to true if a transient config
                needs to be verified whose value might reset shortly
                7. custom_delay: arg to decide the wait time before starting
                validation. Defaulted to 10 secs
        Returns: True -> Pass, False -> fail

        """
        no_delay = kwargs.get("no_delay", False)
        delay = int(kwargs.get("custom_delay", 10))
        base_cmd = "ceph config set"
        cmd = f"{base_cmd} {kwargs['section']}"
        if kwargs.get("location_type"):
            cmd = f"{cmd}/{kwargs['location_type']}:{kwargs['location_value']}"
        cmd = f"{cmd} {kwargs['name']} {kwargs['value']}"
        self.rados_obj.node.shell([cmd])

        if not no_delay:
            # Sleeping for 10 second for config to be applied
            time.sleep(delay)
        log.debug("verifying the value set")
        if not self.verify_set_config(**kwargs):
            log.error(f"Value for config: {kwargs['name']} could not be set")
            return False

        log.info(f"Value for config: {kwargs['name']} was set")
        return True

    def get_config(
        self, section: str = None, param: str = None, daemon_name: str = None
    ) -> str:
        """
        Retrieves the config param in monitor config database,
        Usage - config get <section> <name>
        This is not symmetric with 'config set' because it also
        returns compiled-in default values along with values which
        have been explicitly set

        Args:
            - section: which section of daemons to target
                allowed values: mon, mgr, osd, mds, client
            - param: name of the config param for the selection
            - daemon (optional): name of specific daemon
        Returns: string output of ceph config get <section> <name> command

        E.g.
            - # ceph config get mon public_network
                10.0.208.0/22
            - # ceph config get osd bluestore_min_alloc_size_hdd
                4096
        """

        cmd = f"ceph config get {section} {param}"
        if daemon_name:
            cmd = f"ceph config get {daemon_name}"
        return str(self.rados_obj.node.shell([cmd])[0]).strip()

    def remove_config(self, **kwargs):
        """
        Removes the sent config param from monitor config database
        Config parameters
        Args:
            **kwargs: Any other param that needs to be set
                1. section: which section of daemons to target
                    allowed values: global, mon, mgr, osd, mds, client
                2. name: name of the config param for the selection
                3. verify_rm: (bool) specifies if the removal should be verified
            Optional args:
                3. location_type: CRUSH property like rack or host
                4. location_value: Value for location_type
        Returns: True -> Pass, False -> fail

        """

        base_cmd = "ceph config rm"
        verify_removal = kwargs.get("verify_rm", True)
        cmd = f"{base_cmd} {kwargs['section']}"
        if kwargs.get("location_type"):
            cmd = f"{cmd}/{kwargs['location_type']}:{kwargs['location_value']}"
        cmd = f"{cmd} {kwargs['name']}"
        self.rados_obj.node.shell([cmd])

        # Sleeping for 10 second for config to be applied
        time.sleep(10)
        log.debug("verifying the value removed")
        if verify_removal:
            if self.verify_set_config(**kwargs):
                log.error(f"Value for config: {kwargs['name']} is still set")
                return False

        log.info(f"Value for config: {kwargs['name']} was removed")
        return True

    def verify_set_config(self, **kwargs):
        """
        Verifies the values of configurations set in the mon config db via ceph config dump command
        Args:
            **kwargs: Any other param that needs to be set
                1. section: which section of daemons to target
                    allowed values: global, mon, mgr, osd, mds, client
                2. name: name of the config param for the selection
                3. value: Value to be set
            Optional args:
                4. location_type: CRUSH property like rack or host
                5. device_class: Value for location_type
                6. location_value: value for location_type
        Returns: True -> Pass, False -> fail
        """
        cmd = "ceph config dump"
        config_dump = self.rados_obj.run_ceph_command(cmd)
        for entry in config_dump:
            if (
                entry["name"].lower() == kwargs["name"].lower()
                and entry["section"].lower() == kwargs["section"].lower()
            ):

                # temporary fix, should be changed later, current logic is not robust
                # problem: the module will return True if user provides 'location_value' as
                # an argument to this method but, it is actually not set in the cluster config
                if (
                    kwargs.get("location_value")
                    and kwargs.get("location_value") not in entry["mask"]
                ):
                    continue
                entry["value"] = str(entry["value"]).strip("\n").strip()
                kwargs["value"] = str(kwargs["value"]).strip("\n").strip()
                if not entry["value"].lower() == kwargs["value"].lower():
                    log.error(
                        f"Value for config: {entry['name']} does not match in the ceph config\n"
                        f"sent value : {kwargs['value']}, Set value : {entry['value']}"
                    )
                    return False
                if kwargs.get("location_type"):
                    if kwargs.get("location_type").lower() != "class":
                        if (
                            not entry["location_type"].lower()
                            == kwargs["location_type"].lower()
                        ):
                            log.error(
                                f"Value for config: {entry['name']} does not match in the ceph config\n"
                                f"sent value : {kwargs['location_type']}, Set value : {entry['location_type']}"
                            )
                            return False
                        if (
                            not entry["location_value"].lower()
                            == kwargs["location_value"].lower()
                        ):
                            log.error(
                                f"Value for config: {entry['name']} does not match in the ceph config\n"
                                f"sent value : {kwargs['location_value']}, Set value : {entry['location_value']}"
                            )
                            return False
                    else:
                        if (
                            not entry["device_class"].lower()
                            == kwargs["location_value"].lower()
                        ):
                            log.error(
                                f"Value for config: {entry['name']} does not match in the ceph config\n"
                                f"sent value : {kwargs['location_value']}, Set value : {entry['device_class']}"
                            )
                            return False
                log.info(f"Verified the value set for the config: {entry['name']}")
                return True
        log.error(f"The Config: {kwargs['name']} not listed under in the dump")
        return False

    def get_ceph_log(self, count: int = None) -> dict:
        """
        Shows recent history of config changes. If count  option  is  omitted  it defaults to 10.
        Args:
            count: number of events to be fetched

        Returns: dict of <count> events
                {
                    "version": 47,
                    "timestamp": "2021-11-15 21:33:43.167172",
                    "name": "reset to 44",
                    "changes": [
                        {
                            "name": "osd/osd_max_backfills",
                            "previous_value": "32"
                        },
                        {
                            "name": "osd/osd_recovery_max_active",
                            "previous_value": "32"
                        }
                    ]
                }
        """
        cmd = "ceph config log"
        if count:
            cmd = f"{cmd} {count}"
        config_log = self.rados_obj.run_ceph_command(cmd)
        return config_log

    def ceph_config_reset(self, version: int) -> bool:
        """
        this method reverts configuration to the specified historical version.
        Args:
            version: log version to be reverted to

        Returns: True -> Pass, False -> fail

        """
        log.info(f"reverting to version : {version} ")
        cmd = f"ceph config reset {version}"
        self.rados_obj.run_ceph_command(cmd=cmd)

        # Checking in the logs if the revert has been captured
        conf_log = self.get_ceph_log(count=1)[0]
        try:
            if not re.search(conf_log["name"], f"reset to {version}"):
                log.error(
                    f"The log is not updated with new config changes."
                    f"Changes made: {conf_log['changes']}"
                )
                return False
            log.info(f"Successfully reverted to version : {version}\n Log: {conf_log}")
            return True
        except Exception:
            log.error("The log collected does not contain the name of change made")
            return False

    def show_config(self, daemon: str, id: str, param: str = None) -> str:
        """
        Reports the  running configuration value of a paramter for a running daemon
        Usage - config show <daemon>.<id> <param>
        Args:
            daemon: Ceph daemons like osd,mon,mgr.
            id: Id's of the mon
            param: Parameter name
        Returns: null if no output from the command or string output of ceph config shoe <daemon>.<id>  command.
        E.g.
            - # ceph config show osd.0 osd_recovery_max_active
        """
        cmd = f"ceph config show {daemon}.{id}"
        if param:
            cmd += f" {param}"
        try:
            result, _ = self.rados_obj.node.shell([cmd])
        except Exception as error:
            log.error("The parameter not exist or getting error while execution")
            log.error(f"The error is:{error}")
            return "null"
        return result

    def daemon_config_show(self, daemon_type: str, daemon_id: str) -> dict:
        """
        Fetches the full configuration of the given daemon in cluster.

        Args:
            daemon_type (str): Type of daemon (e.g., 'osd', 'mon', 'mgr')
            daemon_id (str): Name or ID of the daemon.

        Returns:
            dict: Configuration in JSON format.

        Example:
            ceph daemon mgr.ceph-dpentako-bluestore-fku9su-node1-installer.ssjeao config show --format json

        """

        cmd = f"cephadm shell -- ceph daemon {daemon_type}.{daemon_id} config show --format json"
        daemon_node = self.rados_obj.fetch_host_node(daemon_type, daemon_id)
        json_str, _ = daemon_node.exec_command(cmd=cmd, sudo=True)
        json_output = json.loads(json_str)
        return json_output

    def daemon_config_get(self, daemon_type: str, daemon_id: str, param: str) -> dict:
        """
                Fetches a specific configuration parameter of a given daemon.

                Args:
                    daemon_type (str): Type of daemon (e.g., 'osd', 'mon', 'mgr')
                    daemon_id (str): Name or ID of the daemon.
                    param (str): Configuration parameter to retrieve.

                Returns:
                    str: Value of the configuration parameter.

                Example:
                     ceph daemon osd.9 config get bluestore_min_alloc_size_hdd --format json
        {"bluestore_min_alloc_size_hdd":"8192"}

        """

        cmd = f"cephadm shell -- ceph daemon {daemon_type}.{daemon_id} config get {param} --format json"
        daemon_node = self.rados_obj.fetch_host_node(daemon_type, daemon_id)
        param_str, _ = daemon_node.exec_command(cmd=cmd, sudo=True)
        param_output = json.loads(param_str)
        return param_output
