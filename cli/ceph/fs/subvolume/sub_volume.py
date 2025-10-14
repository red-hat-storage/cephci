import json

from cli import Cli
from cli.utilities.utils import build_cmd_from_args
from tests.cephfs.exceptions import (
    SubvolumeCreateError,
    SubvolumeDeleteError,
    SubvolumeGetError,
    SubvolumeResizeError,
)
from utility.log import Log

from .charmap import Charmap
from .earmark import Earmark

log = Log(__name__)


class SubVolume(Cli):
    """This module provides CLI interface for FS subvolume related operations"""

    def __init__(self, nodes, base_cmd):
        super(SubVolume, self).__init__(nodes)
        self.base_cmd = "{} subvolume".format(base_cmd)
        self.earmark = Earmark(nodes, self.base_cmd)
        self.charmap = Charmap(nodes, self.base_cmd)

    def create(self, volume, subvolume, **kwargs):
        """
        Creates ceph subvolume
        Args:
            volume (str): Name of vol where subvol has to be created
            subvolume (str): Name of the subvol
            kw: Key/value pairs of configuration information to be used in the test.
        """
        cmd = "{} create {} {} {}".format(
            self.base_cmd, volume, subvolume, build_cmd_from_args(**kwargs)
        )
        try:
            out = self.execute(sudo=True, cmd=cmd, check_ec=True)
        except Exception as e:
            raise SubvolumeCreateError(
                "Failed to create subvolume {}: {}".format(subvolume, str(e))
            )

        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm(self, volume, subvolume, group=None, force=False, **kwargs):
        """
        Removes ceph subvol
        Args:
            volume (str): Name of vol where subvol has to be removed
            subvolume (str): subvol group name
            group (str): subvol group name
            force (bool): Force tag
        """
        cmd = f"{self.base_cmd} rm {volume} {subvolume}"
        if group:
            cmd += f" {group}"
        if force:
            cmd += " --force"

        cmd += f" {build_cmd_from_args(**kwargs)}"
        try:
            out = self.execute(sudo=True, cmd=cmd, check_ec=True)
        except Exception as e:
            raise SubvolumeDeleteError(
                "Failed to remove subvolume {}: {}".format(subvolume, str(e))
            )
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def ls(self, volume, group=None, **kwargs):
        """
        List subvol groups
        Args:
            volume (str): Name of vol where subvol is present
            group (str): subvol group name
        """
        cmd = f"{self.base_cmd} ls {volume}"
        if group:
            cmd += f" {group}"

        cmd += f" {build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def resize(self, volume, subvolume, size, **kwargs):
        """
        Resize a subvolume
        Args:
            volume (str): Name of vol where subvol is present
            subvolume (str): subvol name
            size (str): new size of the sub volume
        """
        cmd = f"{self.base_cmd} resize {volume} {subvolume} {size} {build_cmd_from_args(**kwargs)}"
        try:
            out = self.execute(sudo=True, cmd=cmd, check_ec=True)
        except Exception as e:
            raise SubvolumeResizeError(
                "Failed to resize subvolume {}: {}".format(subvolume, str(e))
            )
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def getpath(self, volume, subvolume, **kwargs):
        """
        Get the absolute path of a subvolume
        Args:
            volume (str): Name of vol where subvol is present
            subvolume (str): subvol name
        """
        cmd = f"{self.base_cmd} getpath {volume} {subvolume} {build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def info(self, volume, subvolume, **kwargs):
        """
        Get information about a subvolume
        Args:
            volume (str): Name of vol where subvol is present
            subvolume (str): subvol name
        Returns:
            dict: Information about the subvolume in JSON format
        Raises:
            SubvolumeGetError: If the command execution fails or JSON parsing fails
        """
        cmd = (
            f"{self.base_cmd} info {volume} {subvolume} {build_cmd_from_args(**kwargs)}"
        )
        try:
            out = self.execute(sudo=True, cmd=cmd, check_ec=True)
            log.debug("output: {}".format(out))
        except Exception as e:
            raise SubvolumeGetError(
                "Failed to get subvolume info for {}: {}".format(subvolume, str(e))
            )

        if isinstance(out, tuple):
            out = out[0]

        out = out.strip()

        try:
            return json.loads(out)
        except json.JSONDecodeError as e:
            raise SubvolumeGetError(
                "Failed to parse subvolume info JSON output: {}".format(e)
            )
