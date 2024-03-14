from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_from_args

from .config.config import Config
from .device import Device
from .feature import Feature
from .group import Group
from .image_meta import Image_meta
from .mirror.mirror import Mirror
from .namespace import Namespace
from .pool import Pool
from .snap import Snap


class Rbd(Cli):
    def __init__(self, nodes, base_cmd=""):
        super(Rbd, self).__init__(nodes)
        self.base_cmd = f"{base_cmd}rbd"
        self.pool = Pool(nodes, self.base_cmd)
        self.mirror = Mirror(nodes, self.base_cmd)
        self.device = Device(nodes, self.base_cmd)
        self.snap = Snap(nodes, self.base_cmd)
        self.feature = Feature(nodes, self.base_cmd)
        self.image_meta = Image_meta(nodes, self.base_cmd)
        self.config = Config(nodes, self.base_cmd)
        self.namespace = Namespace(nodes, self.base_cmd)
        self.group = Group(nodes, self.base_cmd)

    def create(self, **kw):
        """
        Creates a block device image.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image-spec(str) : <pool_name>/<image_name>
                pool(str) : name of the pool into which image should be stored
                size(str) : size of image
                image(str): name of image to be created
                See rbd help create for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        size = kw_copy.pop("size", "")
        cmd = f"{self.base_cmd} create {image_spec} --size {size} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def ls(self, **kw):
        """
        Lists block device images within pool.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str)  : name of the pool(optional)
                namespace(str): name of the namespace(optional)
                pool-spec(str) : <pool_name>[/<namespace>]
                See rbd help ls for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool-spec", "")
        cmd = f"{self.base_cmd} ls {pool_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def list_(self, **kw):
        """
        Lists block device images.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str)  : name of the pool(optional)
                pool-spec(str) : <pool_name>[/<namespace>]
                See rbd help list for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool-spec", "")
        cmd = f"{self.base_cmd} list {pool_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def info(self, **kw):
        """
        Retrieves information on the block device image.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image-or-snap-spec(str) : [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>]
                pool(str)  : name of the pool from which info should be retreived(optional)
                image(str) :  name of the image from which info should be retreived
                See rbd help info for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_or_snap_spec = kw_copy.pop("image-or-snap-spec", "")
        cmd = f"{self.base_cmd} info {image_or_snap_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def status(self, **kw):
        """
        Retrieves current state of the block device image.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image-spec(str) : [<pool-name>/[<namespace>/]]<image-name>
                See rbd help status for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} status {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def help(self, **kw):
        """
        Displays help for a particular rbd command.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                command(str): name of command
                See rbd help for more supported keys
        """
        command = kw.get("command")
        cmd = f"{self.base_cmd} help {command}"

        return self.execute(cmd=cmd)

    def map(self, **kw):
        """
        Maps RBD for one or more RBD images.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image-or-snap-spec : [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>]
                device-type: device type [ggate, krbd (default), nbd, ubbd]
                pool(str): Name of the pool(default is rbd)
                image(str): image name
                options(str): options to be passed
                See rbd help map for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_or_snap_spec = kw_copy.pop("image-or-snap-spec", "")
        cmd = (
            f"{self.base_cmd} map {image_or_snap_spec} {build_cmd_from_args(**kw_copy)}"
        )

        return self.execute_as_sudo(cmd=cmd)

    def unmap(self, **kw):
        """
        Unmaps RBD for one or more RBD images.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image-or-snap-or-device-spec(str) : [<pool-name>/[<namespace>/]]<image-name>
                                                        [@<snap-name>] or <device-path>
                pool(str): Name of the pool(default is rbd)
                image(str): image name
                options(str): options to be passed
                See rbd help unmap for more supported keys
        """

        kw_copy = deepcopy(kw)
        image_or_snap_or_device_spec = kw_copy.pop("image-or-snap-or-device-spec", "")
        cmd = f"{self.base_cmd} unmap {image_or_snap_or_device_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def resize(self, **kw):
        """
        Resize the size of image.
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image-spec(str) : [<pool-name>/[<namespace>/]]<image-name>
                image(str): name of the image on which resize should happen
                size (int)     :  updated size of image
                encryption_config (list): ["encryption-passphrase-file":<filepath>]
                See rbd help resize for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} resize {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd, check_ec=False, long_running=False)

    def rm(self, **kw):
        """
        Removes the block device image.
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image-spec(str) : [<pool-name>/[<namespace>/]]<image-name
                image(str) : name of the image that should be removed
                pool(str)  : name of the pool from which image should be retreived(optional)
                namespace(str): name of the namespace from which image should be remoevd(optional)
                See rbd help rm for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} rm {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def flatten(self, **kw):
        """
        Flattens the snapshot.
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str)      : name of the pool
                image(str)     : name of the image
                encryption-passphrase-file(str)      : file path containing encryption passphrase
                image-spec(str): <pool>/<image>
                See rbd help flatten for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} flatten {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def clone(self, **kw):
        """
        Clones the snapshot.
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str)         : name of the pool
                snap(str)         : name of the snapshot
                image(str)        : name of the parent image
                dest-pool(str)    : name of the destination pool
                dest(str)         : name of the cloned image
                source-snap-spec  : <parent_pool>/<parent_image>@<snap>
                dest-image-spec   : <dest_pool>/<clone>
                See rbd help clone for more supported keys
        """
        kw_copy = deepcopy(kw)
        source_snap_spec = kw_copy.pop("source-snap-spec", "")
        dest_image_spec = kw_copy.pop("dest-image-spec", "")
        cmd = f"{self.base_cmd} clone {source_snap_spec} {dest_image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def children(self, **kw):
        """
        Lists the children of a snapshot.
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                image-or-snap-spec : [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>]
                pool(str)  : name of the pool
                image(str) : name of the image
                snap(str)  : name of the snapshot
                See rbd help children for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_or_snap_spec = kw_copy.pop("image-or-snap-spec", "")
        cmd = f"{self.base_cmd} children {image_or_snap_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)

    def bench(self, **kw):
        """
        This method is used to generate a series of IOs to the image and measure the IO throughput and latency.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
                image-spec(str) : [<pool-name>/[<namespace>/]]<image-name>
                io-type(str): can be read | write | readwrite | rw.
                image(str): name of the image.
                pool(str): name of the pool.
                See rbd help bench for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} bench {image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def encryption_format(self, **kw):
        """
        This method is used to apply the given encryption format on the image
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
                image-spec(str) : [<pool-name>/[<namespace>/]]<image-name>
                format(str) : encryption format [possible values: luks1, luks2]
                passphrase-file(str) : path of file containing passphrase for unlocking the image
                See rbd help encryption format for more supported keys
        """
        kw_copy = deepcopy(kw)
        passphrase_file = kw_copy.pop("passphrase-file", "")
        format = kw_copy.pop("format", "")
        image_spec = kw_copy.pop("image-spec", "")
        cmd = f"{self.base_cmd} encryption format {image_spec} {format} {passphrase_file} \
              {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def export(self, **kw):
        """
        This method is used to export image to file
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
                source-image-or-snap-spec(str) : [<pool-name>/[<namespace>/]]<image-name>
                                                    [@<snap-name>])
                path-name(str) : export file (or '-' for stdout)
                See rbd help export for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("source-image-or-snap-spec", "")
        export_path = kw_copy.pop("path-name", "")
        cmd = f"{self.base_cmd} export {image_spec} {export_path} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def copy(self, **kw):
        """
        This method is used to copy image
        Args:
            kw(dict): Key/Value pairs that needs to be provided
            Example:
                Supported keys:
                    source-image-or-snap-spec(str) : [<pool-name>/[<namespace>/]]<image-name>
                                                    [@<snap-name>])
                    dest-image-spec(str) : [<pool-name>/[<namespace>/]]<image-name>
                    pool(str) : source pool name
                    image(str) : source image name
                    snap(str) : source snap name
                    dest-pool(str) : destination pool name
                    dest(str) : destination image name
                    See rbd help copy for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("source-image-or-snap-spec", "")
        dest_image_spec = kw_copy.pop("dest-image-spec", "")
        cmd = f"{self.base_cmd} copy {image_spec} {dest_image_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def rename(self, **kw):
        """
        Rename the size of image.
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                source-image-spec(str) : [<pool-name>/[<namespace>/]]<image-name>
                dest-image-spec(str) : [<pool-name>/[<namespace>/]]<image-name>
                pool(str) : source pool name
                image(str): name of the image on which resize should happen
                size (int)     :  updated size of image
                dest-pool(str) : destination pool name
                dest(str) : destination image name
                See rbd help rename for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("source-image-spec", "")
        dest_spec = kw_copy.pop("dest-image-spec", "")
        cmd = f"{self.base_cmd} rename {image_spec} {dest_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd, check_ec=False, long_running=False)
