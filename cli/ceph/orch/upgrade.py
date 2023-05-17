from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Upgrade(Cli):
    def __init__(self, nodes, base_cmd):
        super(Upgrade, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} upgrade"

    def check(self, **kw):
        """
        Check target upgrade image
        Args:
            kw (dict): Key/value pairs that needs to be provided to the installer

            Supported keys:
                image (str) : image
                ceph_version (str) : ceph version
        """
        cmd = f"{self.base_cmd} check {build_cmd_from_args(**kw)}"
        return self.execute(sudo=True, long_running=True, cmd=cmd)

    def start(self, **kw):
        """
        Start upgrade
        Args:
            kw (dict): Key/value pairs that needs to be provided to the installer

            Supported keys:
                image (str) : image
                ceph_version (str) : ceph version
                daemon_types (str) : daemon name
                hosts (str) : host name
                service (str) : services name
                limit (int) : limit value
        """
        cmd = f"{self.base_cmd} start {build_cmd_from_args(**kw)}"
        return self.execute(sudo=True, long_running=True, cmd=cmd)

    def status(self, **kw):
        """
        Check upgrade status
        """
        cmd = f"{self.base_cmd} status"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
