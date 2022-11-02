class ImageMeta:
    """
    This module provides CLI interface to manage block device image metadata.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " image-meta"

    def set_(self, **kw):
        """
        This method is used to set a new metadata key-value pair.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool_name(str): name of the pool.
              image_name(str): name of the image.
              key(str): name of the metadata key.
              value(str): value corresponding to the metadata key.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        key = kw.get("key")
        value = kw.get("value")
        cmd = self.base_cmd + " set" + f" {pool_name}/{image_name} {key} {value}"

        return self.execute(cmd=cmd)

    def remove_(self, **kw):
        """
        This method is used to remove a metadata key-value pair.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool_name(str): name of the pool.
              image_name(str): name of the image.
              key(str): name of the metadata key.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        key = kw.get("key")
        cmd = self.base_cmd + " remove" + f" {pool_name}/{image_name} {key}"

        return self.execute(cmd=cmd)

    def get_(self, **kw):
        """
        This method is used to view the value of a key.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool_name(str): name of the pool.
              image_name(str): name of the image.
              key(str): name of the metadata key.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        key = kw.get("key")
        cmd = self.base_cmd + " get" + f" {pool_name}/{image_name} {key}"

        return self.execute(cmd=cmd)

    def list_(self, **kw):
        """
        This method is used to show all metadata on an image.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool_name(str): name of the pool.
              image_name(str): name of the image.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_name = kw.get("image_name")
        pool_name = kw.get("pool_name")
        cmd = self.base_cmd + " list" + f" {pool_name}/{image_name}"

        return self.execute(cmd=cmd)
