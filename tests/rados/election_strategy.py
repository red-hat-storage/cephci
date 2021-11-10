"""
election_strategy.py module is a rados layer module to test and set Various election strategies employed
by Monitors to form quorum.
It allows us to change b/w various election strategies:
1 - “classic”
2 - “disallow”
3 - “connectivity”

Doc ref: https://docs.ceph.com/en/latest/rados/operations/change-mon-elections/
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
