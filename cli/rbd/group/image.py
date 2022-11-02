class Image:
    """
    This module provides CLI interface to manage the image configuration using rbd group image command.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " image"

    def add_(self, **kw):
        """
        This method is used to add an image to a group.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              image_spec(str): image specification.
              group-spec(str): group specification.([pool-name/[namespace-name/]]group-name])

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_spec = kw.get("image_spec")
        group_spec = kw.get("group-spec")
        cmd = self.base_cmd + " add" + f" {group_spec} {image_spec}"

        return self.execute(cmd=cmd)

    def list_(self, **kw):
        """
        This method is used to list images in a group.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              group-spec(str): group specification.([pool-name/[namespace-name/]]group-name])

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        group_spec = kw.get("group-spec")
        cmd = self.base_cmd + " list" + f" {group_spec}"

        return self.execute(cmd=cmd)

    def remove_(self, **kw):
        """
        This method is used to remove images in a group.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              group-spec(str): group specification.([pool-name/[namespace-name/]]group-name])
              image_spec(str): image specification.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        image_spec = kw.get("image-spec")
        group_spec = kw.get("group-spec")
        cmd = self.base_cmd + " remove" + f" {group_spec} {image_spec}"

        return self.execute(cmd=cmd)
