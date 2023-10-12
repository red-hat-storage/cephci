from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Pool(Cli):
    """This module provides CLI interface to manage the MGR service."""

    def __init__(self, nodes, base_cmd):
        super(Pool, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} pool"

    def ls(self, **kw):
        """List pool details"""
        cmd = f"{self.base_cmd} ls details{build_cmd_from_args(**kw)}"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rename(self, current, new):
        """To rename a pool

        Args:
            current_name (str): Existing pool name
            new_name (str): New name to be assigned
        """
        cmd = f"{self.base_cmd} rename {current} {new}"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def delete(self, name):
        """To delete a pool

        Args:
            name (str): Name of the pool to be deleted
        """
        cmd = f"{self.base_cmd} delete {name}"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def set_quota(self, name, max_objects=None, max_bytes=None):
        """To set pool quotas for the maximum number of bytes and the
           maximum number of objects per pool

        Args:
            name (str): Name of the pool to be deleted
            max_objects (str): object count
            max_bytes (str): bytes count
        """
        cmd = f"{self.base_cmd} set-quota {name}"
        if max_objects:
            cmd += f" max_objects {max_objects}"
        elif max_bytes:
            cmd += f" max_bytes {max_bytes}"
        else:
            return None

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def set(self, name, key, value):
        """To set a value to a pool

        Args:
            name (str): Pool name
            key (str): Key to be set
            value (str): Value to be set to key
        """
        cmd = f"{self.base_cmd} set {name} {key} {value}"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def get(self, name, key):
        """To get a value to a pool

        Args:
            name (str): Pool name
            key (str): Key value to get
        """
        cmd = f"{self.base_cmd} get {name} {key}"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def application(self, name, application, operation, key=None, value=None):
        """Perform the desired operation on the given application

        Args:
            name (str): Pool name:
            application (str): cephfs/rbd/rgw
            operation (str): Operation to be performed (enable/disable/set)
            key (str): key to be set
            value (str): value to be set on key
        """
        cmd = f"{self.base_cmd} application {operation} {name} {application}"
        if key and value:
            cmd += f" {key} {value}"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
