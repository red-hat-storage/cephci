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

import logging
import time

from ceph.rados.core_workflows import RadosOrchestrator

log = logging.getLogger(__name__)


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
            mon: name of the monitor to be added/removed
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
        allows to add mon to  disallowed leaders list in ceph
        Args:
            mon: name of the monitor to be added/removed
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
        Returns: True -> Pass, False -> fail

        """

        base_cmd = "ceph config set"
        cmd = f"{base_cmd} {kwargs['section']}"
        if kwargs.get("location_type"):
            cmd = f"{cmd}/{kwargs['location_type']}:{kwargs['location_value']}"
        cmd = f"{cmd} {kwargs['name']} {kwargs['value']}"
        self.rados_obj.node.shell([cmd])

        # Sleeping for 1 second for config to be applied
        log.debug("verifying the value set")
        if not self.verify_set_config(**kwargs):
            log.error(f"Value for config: {kwargs['name']} could not be set")
            return False

        log.info(f"Value for config: {kwargs['name']} was set")
        return True

    def remove_config(self, **kwargs):
        """
        Removes the sent config param from monitor config database
        Config parameters
        Args:
            **kwargs: Any other param that needs to be set
                1. section: which section of daemons to target
                    allowed values: global, mon, mgr, osd, mds, client
                2. name: name of the config param for the selection
            Optional args:
                3. location_type: CRUSH property like rack or host
                4. location_value: Value for location_type
        Returns: True -> Pass, False -> fail

        """

        base_cmd = "ceph config rm"
        cmd = f"{base_cmd} {kwargs['section']}"
        if kwargs.get("location_type"):
            cmd = f"{cmd}/{kwargs['location_type']}:{kwargs['location_value']}"
        cmd = f"{cmd} {kwargs['name']}"
        self.rados_obj.node.shell([cmd])

        # Sleeping for 1 second for config to be applied
        log.debug("verifying the value set")
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
                entry["name"] == kwargs["name"]
                and entry["section"] == kwargs["section"]
            ):
                if not entry["value"] == kwargs["value"]:
                    log.error(
                        f"Value for config: {entry['name']} does not match in the ceph config"
                        f"sent value : {kwargs['value']}, Set value : {entry['value']}"
                    )
                    return False
                if kwargs.get("location_type"):
                    if kwargs.get("location_type") != "class":
                        if not entry["location_type"] == kwargs["location_type"]:
                            log.error(
                                f"Value for config: {entry['name']} does not match in the ceph config\n"
                                f"sent value : {kwargs['location_type']}, Set value : {entry['location_type']}"
                            )
                            return False
                        if not entry["location_value"] == kwargs["location_value"]:
                            log.error(
                                f"Value for config: {entry['name']} does not match in the ceph config\n"
                                f"sent value : {kwargs['location_value']}, Set value : {entry['location_value']}"
                            )
                            return False
                    else:
                        if not entry["device_class"] == kwargs["location_value"]:
                            log.error(
                                f"Value for config: {entry['name']} does not match in the ceph config\n"
                                f"sent value : {kwargs['location_value']}, Set value : {entry['device_class']}"
                            )
                            return False
                log.info(f"Verified the value set for the config : {entry['name']}")
                return True
        log.error(f"The Config: {kwargs['name']} not listed under in the dump")
        return False
