from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Device(Cli):
    """
    This Class provides wrappers for rbd device commands.
    """

    def __init__(self, nodes, base_cmd):
        super(Device, self).__init__(nodes)
        self.base_cmd = base_cmd + " device"

    def attach(self, **kw):
        """Wrapper for rbd device attach.

        Attach image to device.
        Args:
        kw: Key value pair of method arguments
            Example::
                rbd device attach <image-or-snap-spec> --device-type nbd
            Supported keys:
                device-type (str): device type [ggate, krbd (default), nbd]
                pool(str): pool name
                namespace(str): namespace name
                image(str): image name
                snap(str): snapshot name
                device(str): specify device path
                show-cookie(bool): show device cookie
                cookie(str): specify device cookie
                read-only(bool): attach read-only
                force(bool): force attach
                exclusive(bool): disable automatic exclusive lock transitions
                quiesce(bool): use quiesce hooks
                quiesce-hook(str): quiesce hook path
                options(str): device specific options
                image-or-snap-spec(str): [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>]
                See rbd help device attach for more supported keys
        """

        kw_copy = deepcopy(kw)
        image_or_snap_spec = kw_copy.pop("image-or-snap-spec", "")
        cmd = f"{self.base_cmd} attach {image_or_snap_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def detach(self, **kw):
        """Wrapper for rbd device detach.

        Detach image from device.
        Args:
        kw: Key value pair of method arguments
            Example::
                rbd device detach <image-or-snap-spec> --device-type nbd
            Supported keys:
                device-type (str): device type [ggate, krbd (default), nbd]
                pool(str): pool name
                namespace(str): namespace name
                image(str): image name
                snap(str): snapshot name
                options(str): device specific options
                image-snap-or-device-spec: [<pool-name>/]<image-name>[@<snap-name>]
                                            or <device-path>
                See rbd help device detach for more supported keys
        """

        kw_copy = deepcopy(kw)
        image_snap_or_device_spec = kw_copy.pop("image-snap-or-device-spec", "")
        cmd = f"{self.base_cmd} detach {image_snap_or_device_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def list_(self, **kw):
        """Wrapper for rbd device list.

        List mapped rbd images.
        Args:
        kw: Key value pair of method arguments
            Example::
                rbd device list
            Supported keys:
                device-type (str): device type [ggate, krbd (default), nbd]
                format(str): output format (plain, json, or xml) [default: plain]
                pretty-format(bool): True
                See rbd help device list for more supported keys
        """
        cmd = f"{self.base_cmd} list {build_cmd_from_args(**kw)}"

        return self.execute_as_sudo(cmd=cmd)

    def map(self, **kw):
        """Wrapper for rbd device map.

        Map an image to a block device.
        Args:
        kw: Key value pair of method arguments
            Example::
                rbd device map <image-or-snap-spec> --device-type nbd
            Supported keys:
                device-type (str): device type [ggate, krbd (default), nbd]
                pool(str): pool name
                namespace(str): namespace name
                image(str): image name
                snap(str): snapshot name
                device(str): specify device path
                show-cookie(bool): show device cookie
                cookie(str): specify device cookie
                read-only(bool): attach read-only
                force(bool): force attach
                exclusive(bool): disable automatic exclusive lock transitions
                quiesce(bool): use quiesce hooks
                quiesce-hook(str): quiesce hook path
                options(str): device specific options
                image-or-snap-spec(str): [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>]
                See rbd help device map for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_or_snap_spec = kw_copy.pop("image-or-snap-spec", "")
        cmd = (
            f"{self.base_cmd} map {image_or_snap_spec} {build_cmd_from_args(**kw_copy)}"
        )

        return self.execute_as_sudo(cmd=cmd)

    def unmap(self, **kw):
        """Wrapper for rbd device unmap.

        Unmap a rbd device.
        Args:
        kw: Key value pair of method arguments
            Example::
                rbd device unmap <image-snap-or-device-spec>
            Supported keys:
                device-type (str): device type [ggate, krbd (default), nbd]
                pool(str): pool name
                namespace(str): namespace name
                image(str): image name
                snap(str): snapshot name
                options(str): device specific options
                image-snap-or-device-spec: [<pool-name>/]<image-name>[@<snap-name>]
                                            or <device-path>
                See rbd help device unmap for more supported keys
        """
        kw_copy = deepcopy(kw)
        image_snap_or_device_spec = kw_copy.pop("image-snap-or-device-spec", "")
        cmd = f"{self.base_cmd} unmap {image_snap_or_device_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)
