from copy import deepcopy

from cli.utilities.utils import build_cmd_args


class Image:

    """
    This module provides CLI interface to modify the RBD image configuration
    settings of a Image via rbd config image command.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " image"

    def set_(self, **kw):
        """
        This method is used to override the RBD image configuration settings for a particular Image.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool_name(str): name of the pool.
              image_name(str): name of the image.
              parameter(str): name of the parameter to be modified.
              value(bool): True to enable the parameter and False to disable it.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        parameter = kw.get("parameter")
        value = kw.get("value")
        cmd = self.base_cmd + " set" + f" {pool_name}/{image_name} {parameter} {value}"

        return self.execute(cmd=cmd)

    def get_(self, **kw):
        """
        This method is used to get an image-level configuration override.
         Args:
           kw(dict): Key/value pairs that needs to be provided to the installer
           Example:
             Supported keys:
               image_spec(str): image specification.
               key(str): This is the config key.
         Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_spec = kw.get("image_spec")
        key = kw.get("key")
        cmd = self.base_cmd + " get" + f" {image_spec} {key}"

        return self.execute(cmd=cmd)

    def list_(self, **kw):
        """
        This method is used to get an image-level configuration override.
         Args:
           kw(dict): Key/value pairs that needs to be provided to the installer
           Example:
             Supported keys:
               image_spec(str): image specification.
               format(str): It can be [plain | json | xml].(Optional)
               pretty-format(str): set this to true to prettify the output.(Optional)

         Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        kw_copy = deepcopy(kw)
        image_spec = kw.pop("image_spec")
        cmd_args = build_cmd_args(kw=kw_copy)
        cmd = self.base_cmd + " get" + f" {image_spec}" + cmd_args

        return self.execute(cmd=cmd)

    def remove_(self, **kw):
        """
        This method is used to remove an image-level configuration override.
        Args:
           kw(dict): Key/value pairs that needs to be provided to the installer
           Example:
             Supported keys:
               image_spec(str): image specification.
               key(str): This is the config key.
         Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_spec = kw.get("image_spec")
        key = kw.get("key")
        cmd = self.base_cmd + " get" + f" {image_spec} {key}"

        return self.execute(cmd=cmd)
