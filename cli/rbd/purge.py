from .schedule import Schedule

base_cmd_trash = None
schedule_obj = None


class TrashPurge:
    """
    This module provides CLI interface to rbd trash purge.
    """

    global schedule_obj
    schedule = schedule_obj

    def __new__(self, **kw):
        """
        Purges images from the trash.
        Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str) : name of the pool
        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        pool_name = kw.get("pool_name")
        global base_cmd_trash
        cmd = base_cmd_trash + f" purge {pool_name}"

        return self.execute(cmd=cmd)


class Purge:
    """
    This module provides abstract CLI interface to rbd trash purge.
    """

    def __new__(self, nodes, base_cmd):
        global base_cmd_trash
        base_cmd_trash = base_cmd
        self.base_cmd = base_cmd + " purge"
        global schedule_obj
        schedule_obj = Schedule(nodes, self.base_cmd)
        return TrashPurge
