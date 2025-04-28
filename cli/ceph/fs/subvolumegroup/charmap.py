import json

from cli import Cli
from cli.utilities.utils import build_cmd_from_args
from tests.cephfs.exceptions import CharMapGetError, CharMapRemoveError, CharMapSetError
from utility.log import Log

log = Log(__name__)


class Charmap(Cli):
    """This module provides CLI interface for FS subvolume charmaps related operations"""

    def __init__(self, nodes, base_cmd):
        super(Charmap, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} charmap"

    def set(self, volume, subvolume_group, **kwargs):
        """
        Sets charmaps to the subvolume
        Args:
            volume (str): Name of vol where subvol is present
            subvolume_group (str): subvol group name
            kwargs: Key/value pairs of charmaps to be set
            Example:
                {
                    "casesensitive": "true",
                    "normalization": "nfc"
                }
        Raises:
            CharMapSetError: If the command execution fails
        """
        for attr, val in kwargs.items():
            cmd = f"{self.base_cmd} set {volume} {subvolume_group} {attr} {val}"

            try:
                self.execute(sudo=True, cmd=cmd, check_ec=True)
            except Exception as e:
                raise CharMapSetError(
                    "Failed to set charmaps: {} for subvolume {}".format(
                        e, subvolume_group
                    )
                )
        return True

    def get(self, volume, subvolume_group, charmap_attribute=None, **kwargs):
        """
        Gets the charmaps from the subvolume
        Args:
            volume (str): Name of vol where subvol is present
            subvolume_group (str): subvol group name
            charmap_attribute (str, optional): Specific charmaps attribute to get. Ex: "casesensitive", "normalization".
            kwargs: Additional command arguments
        Returns:
            str: Charmaps information from the subvolume
        Raises:
            CharMapGetError: If the command execution fails
        """
        cmd = f"{self.base_cmd} get {volume} {subvolume_group}"

        cmd += build_cmd_from_args(**kwargs)

        if charmap_attribute:
            cmd += f" {charmap_attribute}"

        try:
            out = self.execute(sudo=True, cmd=cmd, check_ec=True)
            log.debug("output: {}".format(out))
        except Exception as e:
            raise CharMapGetError(
                "Failed to get charmaps: {} for subvolumegroup {} with output: {}".format(
                    e, subvolume_group, out
                )
            )

        if isinstance(out, tuple):
            out = out[0]

        out = out.strip()

        if not charmap_attribute:
            try:
                return json.loads(out)
            except json.JSONDecodeError as e:
                raise CharMapGetError(f"Failed to parse charmaps JSON output: {e}")

        return out

    def remove(self, volume, subvolume_group, **kwargs):
        """
        Remove the charmaps from the subvolume
        Args:
            volume (str): Name of vol where subvol is present
            subvolumegroup (str): subvol group name
        Returns:
            str: Output from the command
        Raises:
            CharMapRemoveError: If the command execution fails
        """
        cmd = f"{self.base_cmd} rm {volume} {subvolume_group} {build_cmd_from_args(**kwargs)}"

        try:
            out = self.execute(sudo=True, cmd=cmd, check_ec=True)
            log.debug("output: {}".format(out))
        except Exception as e:
            raise CharMapRemoveError(
                "Failed to remove charmaps: {} for subvolumegroup {} with output: {}".format(
                    e, subvolume_group, out
                )
            )

        if isinstance(out, tuple):
            return out[0].strip()
        return out
