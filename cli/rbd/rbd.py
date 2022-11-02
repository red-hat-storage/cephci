from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_args

from .config.config import Config
from .device import Device
from .feature import Feature
from .group.group import Group
from .image_meta import ImageMeta
from .journal import Journal
from .lock import Lock
from .migration import Migration
from .mirror.mirror import Mirror
from .namespace import Namespace
from .pool import Pool
from .snap import Snap
from .trash import Trash


class Rbd(Cli):
    """
    This module provides CLI interface to manage block device images in a cluster.
    """

    def __init__(self, nodes, base_cmd=""):
        self.base_cmd = f"{base_cmd}rbd"
        self.device = Device(nodes, self.base_cmd)
        self.mirror = Mirror(nodes, self.base_cmd)
        self.trash = Trash(nodes, self.base_cmd)
        self.snap = Snap(nodes, self.base_cmd)
        self.migration = Migration(nodes, self.base_cmd)
        self.pool = Pool(nodes, self.base_cmd)
        self.namespace = Namespace(nodes, self.base_cmd)
        self.image_meta = ImageMeta(nodes, self.base_cmd)
        self.journal = Journal(nodes, self.base_cmd)
        self.feature = Feature(nodes, self.base_cmd)
        self.config = Config(nodes, self.base_cmd)
        self.lock = Lock(nodes, self.base_cmd)
        self.group = Group(nodes, self.base_cmd)

    def create(self, **kw):
        """
        Creates a block device image.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str) : name of the pool into which image should be stored
                size(int)      : size of image in MegaBytes
                image_name(str):  name of image to be created
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        size = kw.get("size")
        pool_name = kw.get("pool_name")
        cmd = self.base_cmd + f" create {image_name} --size {size} --pool {pool_name}"

        return self.execute(cmd=cmd)

    def ls(self, **kw):
        """
        Lists block device images within pool.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str)  : name of the pool(optional)
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        pool_name = kw.get("pool_name", "")
        cmd = self.base_cmd + f" ls {pool_name}"

        return self.execute(cmd=cmd)

    def list_(self, **kw):
        """
        Lists block device images.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            None
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        cmd = self.base_cmd + " list"

        return self.execute(cmd=cmd)

    def info(self, **kw):
        """
        Retrieves information on the block device image.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str)  : name of the pool from which info should be retreived(optional)
                image_name(str) :  name of the image from which info should be retreived
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        if kw.get("pool_name"):
            pool_name = kw.get("pool_name")
            cmd = self.base_cmd + f" --image {image_name} -p {pool_name} info"
        else:
            cmd = self.base_cmd + f" --image {image_name} info"

        return self.execute(cmd=cmd)

    def status(self, **kw):
        """
        Retrieves current state of the block device image operation.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                target_name(str): The operation for which status should be known
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        target = kw.get("target_name")
        cmd = self.base_cmd + f" status {target}"

        return self.execute(cmd=cmd)

    def help(self, **kw):
        """
        Displays help for a particular rbd command and its subcommand.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                command(str): name of command
                sub-command(str): name of sub-command
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        command = kw.get("command")
        sub_command = kw.get("sub-command")
        cmd = self.base_cmd + f" help {command} {sub_command}"

        return self.execute(cmd=cmd)

    def map(self, **kw):
        """
        Maps or unmaps RBD for one or more RBD images.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str): Name of the pool(default is rbd)
                image_name(str): image name
                option(str): options to be passed
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        kw_copy = deepcopy(kw)
        image_name = kw_copy.pop("image_name", "")
        pool_name = kw_copy.pop("pool_name", "rbd")
        cmd = (
            self.base_cmd
            + f" map {pool_name}/{image_name}"
            + build_cmd_args(kw=kw_copy)
        )

        return self.execute(cmd=cmd)

    def resize(self, **kw):
        """
        Resize the size of image.
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image_name(str): name of the image on which resize should happen
                size (int)     :  updated size of image
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        size = kw.get("size")
        cmd = self.base_cmd + f" resize --image {image_name} --size {size}"

        return self.execute(sudo=True, check_ec=False, long_running=False, cmd=cmd)

    def rm(self, **kw):
        """
        Removes the block device image.
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image_name(str) : name of the image that should be removed
                pool_name(str)  : name of the pool from which image should be retreived(optional)
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        if kw.get("pool_name"):
            pool_name = kw.get("pool_name")
            cmd = self.base_cmd + f" rm {image_name} -p {pool_name}"
        else:
            cmd = self.base_cmd + f" rm {image_name}"

        return self.execute(cmd=cmd)

    def flatten(self, **kw):
        """
        Flattens the snapshot.
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str)      : name of the pool
                image_name(str)     : name of the image
                snap_name(str)      : name of the snapshot
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        cmd = self.base_cmd + f" flatten {pool_name}/{image_name}"

        return self.execute(cmd=cmd)

    def clone(self, **kw):
        """
        Clones the snapshot.
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str)         : name of the pool
                parent_image_name(str) : name of the parent image
                child_image_name(str)  : name of the child image
                snap_namestr)          : name of the snapshot
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        parent_image_name = kw.get("parent_image_name")
        child_image_name = kw.get("child_image_name")
        pool_name = kw.get("pool_name")
        snap_name = kw.get("snap_name")
        cmd = (
            self.base_cmd
            + f" clone {pool_name}/{parent_image_name}@{snap_name} {pool_name}/{child_image_name}"
        )

        return self.execute(cmd=cmd)

    def children(self, **kw):
        """
        Lists the children of a snapshot.
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str)  : name of the pool
                image_name(str) : name of the image
                snap_name(str)  : name of the snapshot
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        snap_name = kw.get("snap_name")
        cmd = self.base_cmd + f" children {pool_name}/{image_name}@{snap_name}"

        return self.execute(cmd=cmd)

    def bench(self, **kw):
        """
        This method is used to generate a series of IOs to the image and measure the IO throughput and latency.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              io-type(str): can be read | write | readwrite | rw.
              image_name(str): name of the image.
              pool_name(str): name of the pool.
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        kw_copy = deepcopy(kw)
        image_name = kw_copy.pop("image_name")
        pool_name = kw_copy.pop("pool_name")
        io_type = kw_copy.pop("io-type")
        cmd_args = build_cmd_args(kw=kw_copy)
        cmd = (
            self.base_cmd
            + f" bench {image_name}/{pool_name} --io-type {io_type}"
            + cmd_args
        )

        return self.execute(cmd=cmd)
