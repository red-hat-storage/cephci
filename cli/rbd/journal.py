from copy import deepcopy

from cli.utilities.utils import build_cmd_args

from .client import Client


class Journal:

    """
    This Class provides wrappers for rbd journal commands.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " journal"
        self.client = Client(nodes, self.base_cmd)

    def import_(self, **kw):
        """Wrapper for rbd journal import.

        Import image journal.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                path_name(str): import file
                dest_journal_spec(str): destination journal specification.
                                    [<pool-name>/[<namespace>/]]<journal-name>
                path(str): import file
                dest-pool(str): destination pool name
                dest-namespace(str): destination namespace name
                dest(str): destination image name
                dest-journal(str): destination journal name
                verbose(bool): True
                no-error(bool): Continue after error
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        dest_journal_spec = kw_copy.pop("dest_journal_spec", "")
        path_name = kw_copy.pop("path_name", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} import {path_name} {dest_journal_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def export(self, **kw):
        """Wrapper for rbd journal export.

        Export image journal.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                source_journal_spec(str): source journal specification.
                                    [<pool-name>/[<namespace>/]]<journal-name>
                path_name(str): import file
                namespace(str): name of the namespace
                pool(str): pool name
                image(str): image name
                journal(str): journal name
                path(str): import file
                verbose(bool): True
                no-error(bool): Continue after error
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        source_journal_spec = kw_copy.pop("source_journal_spec", "")
        path_name = kw_copy.pop("path_name", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} export {source_journal_spec} {path_name}{cmd_args}"

        return self.execute(cmd=cmd)

    def info(self, **kw):
        """Wrapper for rbd journal info.

        Show information about image journal.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                journal_spec(str): journal specification.
                                    [<pool-name>/[<namespace>/]]<journal-name>
                pool(str): pool name
                namespace(str): name of the namespace
                image(str): image name
                journal(str): journal name
                format(str): output format (plain, json, or xml) [default: plain]
                pretty-format(bool): True
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        journal_spec = kw_copy.pop("journal_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} info {journal_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def inspect(self, **kw):
        """Wrapper for rbd journal inspect.

        Inspect image journal for structural errors.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                journal_spec(str): journal specification.
                                    [<pool-name>/[<namespace>/]]<journal-name>
                pool(str): pool name
                namespace(str): name of the namespace
                image(str): image name
                journal(str): journal name
                verbose(bool): True
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        journal_spec = kw_copy.pop("journal_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} inspect {journal_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def reset(self, **kw):
        """Wrapper for rbd journal reset.

        Reset image journal.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                journal_spec(str): journal specification.
                                    [<pool-name>/[<namespace>/]]<journal-name>
                pool(str): pool name
                namespace(str): name of the namespace
                image(str): image name
                journal(str): journal name
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        journal_spec = kw_copy.pop("journal_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} reset {journal_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def status(self, **kw):
        """Wrapper for rbd journal status.

        Show status of image journal.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                journal_spec(str): journal specification.
                                    [<pool-name>/[<namespace>/]]<journal-name>
                pool(str): pool name
                namespace(str): name of the namespace
                image(str): image name
                journal(str): journal name
                format(str): output format (plain, json, or xml) [default: plain]
                pretty-format(bool): True
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        journal_spec = kw_copy.pop("journal_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} status {journal_spec}{cmd_args}"

        return self.execute(cmd=cmd)
