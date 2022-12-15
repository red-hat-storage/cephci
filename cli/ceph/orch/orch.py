from cli import Cli
from cli.utilities.utils import build_cmd_from_args
from utility.log import Log

from .host import Host
from .label import Label
from .tuned_profile import TunedProfile

log = Log(__name__)


class OrchApplyCommandError(Exception):
    pass


class Orch(Cli):
    def __init__(self, nodes, base_cmd):
        super(Orch, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} orch"
        self.tuned_profile = TunedProfile(nodes, self.base_cmd)
        self.label = Label(nodes, self.base_cmd)
        self.host = Host(nodes, self.base_cmd)

    def ls(self, **kw):
        """
        Lists the status of the given services running in the Ceph cluster.

        Args:
            kw (dict): Key/value pairs that needs to be provided to the installer.

            Supported keys:
                service_type (str): type of service (mon, osd, mgr, mds, rgw).
                service_name (str): name of host.
                export (bool): whether to export the service specifications knows to
                              the orchestrator.
                format (str): the type to be formatted(yaml).
                refresh (bool): whether to refresh or not.
        """
        cmd = f"{self.base_cmd} ls {build_cmd_from_args(**kw)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def apply(self, service_name, **kw):
        """
        Applies the configuration to hosts.

        Args:
            service_name : name of the service
            kw (dict): key/value pairs that needs to be provided to the installer.

            Supported keys:
              hosts (list): name of host.
              placement (dict): placement on hosts.
              count (int): number of hosts to be applied
        """
        cmd = f"{self.base_cmd} apply {service_name}"
        for arg in kw.pop("pos_args"):
            cmd += f" {arg}"
        cmd += build_cmd_from_args(**kw)
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm(self, service_name):
        """
        Removes a service.
        Args:
          service_name (str): name of the service to be removed
        """
        cmd = f"{self.base_cmd} rm {service_name}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def ps(self, **kw):
        """
        List daemon running in the node
        Args:
            kw (dict): Key/value pairs that needs to be provided to the installer

            Supported keys:
                hostname (str) : host name
                service_name (str) : list daemons
                daemon_type (str) : type of daemon (mon, osd, mgr, mds, rgw).
                format (str): the type to be formatted(yaml).
                refresh (bool): whether to refresh or not.
        """
        cmd = f"{self.base_cmd} ps {build_cmd_from_args(**kw)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
