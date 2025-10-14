import json

from cli import Cli
from cli.utilities.utils import build_cmd_from_args
from tests.cephfs.exceptions import (
    SubvolumeGroupCreateError,
    SubvolumeGroupDeleteError,
    SubvolumeGroupGetError,
)
from utility.log import Log

from .charmap import Charmap

log = Log(__name__)


class SubVolumeGroup(Cli):
    """This module provides CLI interface for FS subvolume group related operations"""

    def __init__(self, nodes, base_cmd):
        super(SubVolumeGroup, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} subvolumegroup"
        self.charmap = Charmap(nodes, self.base_cmd)

    def create(self, volume, group, **kwargs):
        """
        Creates ceph subvolume group
        Args:
            volume (str): Name of vol where subvol group has to be created
            group (str): Name of the subvol group
            kw: Key/value pairs of configuration information to be used in the test.
        """
        cmd = f"{self.base_cmd} create {volume} {group} {build_cmd_from_args(**kwargs)}"

        try:
            out = self.execute(sudo=True, cmd=cmd, check_ec=True)
        except Exception as e:
            raise SubvolumeGroupCreateError(
                "Failed to build command to create subvolumegroup {}: {}".format(
                    group, str(e)
                )
            )

        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm(self, volume, group, force=False):
        """
        Removes ceph subvol group
        Args:
            volume (str): Name of vol where subvolgroup has to be removed
            group (str): subvol group name
            force (bool): Force tag
        """
        cmd = f"{self.base_cmd} rm {volume} {group}"
        if force:
            cmd += " --force"
        try:
            out = self.execute(sudo=True, cmd=cmd, check_ec=True)
        except Exception as e:
            raise SubvolumeGroupDeleteError(
                "Failed to remove subvolumegroup {}: {}".format(group, str(e))
            )
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def ls(self, volume):
        """
        List subvol groups
        Args:
            volume (str): Name of vol where subvolgroup is present
        """
        cmd = f"{self.base_cmd} ls {volume}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def getpath(self, volume, group):
        """
        Get the absolute path of a subvolume group
        Args:
            volume (str): Name of vol where subvolgroup is present
            group (str): subvol group name
        """
        cmd = f"{self.base_cmd} getpath {volume} {group}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def info(self, volume, group):
        """
        Get information about a subvolume group
        Args:
            volume (str): Name of vol where subvol is present
            group (str): subvol group name
        Returns:
            dict: Information about the subvolume group in JSON format
        Raises:
            SubvolumeGroupGetError: If the command execution fails or JSON parsing fails
        """
        cmd = f"{self.base_cmd} info {volume} {group}"
        try:
            out = self.execute(sudo=True, cmd=cmd, check_ec=True)
            log.debug("output: {}".format(out))
        except Exception as e:
            raise SubvolumeGroupGetError(
                "Failed to get subvolumegroup info for {}: {}".format(group, str(e))
            )
        if isinstance(out, tuple):
            out = out[0]
        out = out.strip()

        try:
            return json.loads(out)
        except json.JSONDecodeError as e:
            raise SubvolumeGroupGetError(
                "Failed to parse subvolumegroup info JSON output: {}".format(e)
            )
