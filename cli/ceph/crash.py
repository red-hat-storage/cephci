from cli import Cli


class Crash(Cli):
    """This module provides CLI interface to manage the crash module."""

    def __init__(self, nodes, base_cmd):
        super(Crash, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} crash"

    def post(self, metafile):
        """Save a crash dump
        Args:
            metafile (str): config key
        """
        cmd = f"{self.base_cmd} post {metafile}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm(self, crash_id):
        """
        Remove a specific crash dump.
        Args:
            crash_id (str): Crash id to be removed
        """
        cmd = f"{self.base_cmd} rm {crash_id}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def ls(self, new=False):
        """
        List the timestamp/uuid crashids for all new and archived crash info
        Args:
            new (bool): True to list the timestamp/uuid crashids for all newcrash info
        """
        cmd = f"{self.base_cmd} ls"
        if new:
            cmd += "-new"  # e.g: "ceph crash ls-new"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def stat(self):
        """
        Show a summary of saved crash info grouped by age.
        """
        cmd = f"{self.base_cmd} stat"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def info(self, crash_id):
        """
        Show all details of a saved crash
        Args:
            crash_id (str): Crash id
        """
        cmd = f"{self.base_cmd} info {crash_id}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def prune(self, keep):
        """
        Remove saved crashes older than ‘keep’ days
        Args:
            keep (int): No. of days (must be int, no floating values)
        """
        cmd = f"{self.base_cmd} prune {str(keep)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def archive(self, crash_id=None, all=False):
        """
        Archive a crash report so that it is no longer considered
        for the RECENT_CRASH health check and does not appear
        in the crash ls-new output
        Args:
            crash_id (str): Crash id
            all (bool): Archive all new crash reports.
        """
        cmd = f"{self.base_cmd} archive"
        cmd += "-all" if all else f" {crash_id}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
