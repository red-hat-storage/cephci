"""Perform the manager operations via Ceph's cephadm CLI."""
from json import loads

from .ceph import CephCLI


class Manager(CephCLI):
    """Manager interface for ceph mgr <action>"""

    def get_active(self) -> str:
        """Fetch the current active mgr node of the cluster.

        Returns:
            hostname of the active mgr
        """
        out, _ = self.shell(args=["ceph", "-s"])
        active_mgr = (
            out.split("mgr:")[1].split("(active")[0].split(".")[0].replace(" ", "")
        )
        return active_mgr

    def is_active(self, hostname) -> bool:
        """Check whether the host with given hostname has an active mgr or not

        Args:
            hostname (str): hostname of the node which needs to be checked for active mgr

        Returns:
            True if the host with given hostname has active mgr, else False
        """
        active_mgr = self.get_active()
        return hostname in active_mgr

    def get_standby(self) -> str:
        """Fetch the current standby mgr nodes of the cluster.

        Returns:
            list containing the hostnames of all standby mgrs
        """
        out, _ = self.shell(args=["ceph", "-s"])
        standby_mgrs = (
            out.split("mgr:")[1].split("standbys:")[1].replace(" ", "").split(",")
        )

        return standby_mgrs

    def is_standby(self, hostname) -> bool:
        """Check whether the host with given hostname has a standby mgr or not

        Args:
            hostname (str): hostname of the node which needs to be checked for standby mgr

        Returns:
            True if the host with given hostname has standby mgr, else False
        """
        standby_mgrs = self.get_standby()
        return any(hostname in mgr for mgr in standby_mgrs)

    def is_standby_present(self) -> bool:
        """Returns whether a standby mgr is present in the cluster or not

        Returns:
            True if atleast one standby mgr is present, else False
        """
        out, _ = self.shell(args=["ceph", "-s", "-f", "json"])
        status = loads(out)
        standby_count = status["mgrmap"]["num_standbys"]
        return standby_count > 0

    def switch_active(self, node) -> bool:
        """Switches active mgr node to a node other than the node passed to the input

        Args:
            node (CephNode): The node which should not be an active mgr

        Returns:
            True if mgr was switched to a different node or if the given node was not an active mgr
            False if there is no standby mgr to switch to or if the active mgr switching failed
        """
        if not self.is_active(node.hostname):
            return True

        if not self.is_standby_present():
            return False

        out, _ = self.shell(args=["ceph", "mgr", "fail"])

        if not self.is_active(node.hostname):
            return True
        else:
            return False
