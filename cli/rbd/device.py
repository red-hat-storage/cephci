from copy import deepcopy

from cli.utilities.utils import build_cmd_args


class Device:
    """
    This Class provides wrappers for rbd device commands.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " device"

    def attach(self, **kw):
        """Wrapper for rbd device attach.

        Attach image to device.
        Args:
        kw: Key value pair of method arguments
            Example::
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
                image_or_snap_spec(str): [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>]

        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """

        kw_copy = deepcopy(**kw)
        image_or_snap_spec = kw_copy.pop("image_or_snap_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} attach {image_or_snap_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def detach(self, **kw):
        """Wrapper for rbd device detach.

        Detach image from device.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                device-type (str): device type [ggate, krbd (default), nbd]
                pool(str): pool name
                namespace(str): namespace name
                image(str): image name
                snap(str): snapshot name
                options(str): device specific options
                image_snap_or_device_spec: [<pool-name>/]<image-name>[@<snap-name>]
                                            or <device-path>
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """

        kw_copy = deepcopy(**kw)
        image_snap_or_device_spec = kw_copy.pop("image_snap_or_device_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} detach {image_snap_or_device_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def list_(self, **kw):
        """Wrapper for rbd device list.

        List mapped rbd images.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                device-type (str): device type [ggate, krbd (default), nbd]
                format(str): output format (plain, json, or xml) [default: plain]
                pretty-format(bool): True
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """

        cmd_args = build_cmd_args(kw=kw)

        cmd = f"{self.base_cmd} list{cmd_args}"

        return self.execute(cmd=cmd)

    def map(self, **kw):
        """Wrapper for rbd device map.

        Map an image to a block device.
        Args:
        kw: Key value pair of method arguments
            Example::
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
                image_or_snap_spec(str): [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>]
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """

        kw_copy = deepcopy(**kw)
        image_or_snap_spec = kw_copy.pop("image_or_snap_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} map {image_or_snap_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def unmap(self, **kw):
        """Wrapper for rbd device unmap.

        Unmap a rbd device.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                device-type (str): device type [ggate, krbd (default), nbd]
                pool(str): pool name
                namespace(str): namespace name
                image(str): image name
                snap(str): snapshot name
                options(str): device specific options
                image_snap_or_device_spec: [<pool-name>/]<image-name>[@<snap-name>]
                                            or <device-path>
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """

        kw_copy = deepcopy(**kw)
        image_snap_or_device_spec = kw_copy.pop("image_snap_or_device_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} unmap {image_snap_or_device_spec}{cmd_args}"

        return self.execute(cmd=cmd)
