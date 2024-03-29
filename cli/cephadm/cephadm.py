from cli import Cli
from cli.ceph.ceph import Ceph
from cli.ceph.ceph_volume.ceph_volume import CephVolume
from cli.utilities.utils import build_cmd_from_args


class CephAdm(Cli):
    """This module provides CLI interface to manage the CephAdm operations"""

    def __init__(self, nodes, src_mount=None, mount=None, base_cmd="cephadm"):
        super(CephAdm, self).__init__(nodes)

        self.base_cmd = base_cmd
        self.base_shell_cmd = f"{self.base_cmd} shell"
        if src_mount:
            self.base_shell_cmd += f" --mount {src_mount}:{mount} --"
        elif mount:
            self.base_shell_cmd += f" --mount {mount}:{mount} --"

        self.ceph = Ceph(nodes, self.base_shell_cmd)
        self.ceph_volume = CephVolume(nodes, self.base_shell_cmd)

    def shell(self, cmd):
        """Ceph orchestrator shell interface to run ceph commands.

        Args:
            cmd (str): command to be executed
        """
        cmd = f"{self.base_shell_cmd} {cmd}"
        return self.execute(sudo=True, long_running=True, cmd=cmd)

    def bootstrap(self, image=None, yes_i_know=False, **kw):
        """Bootstrap cluster with options

        Args:
            image (str): Ceph container image URL
            yes_i_know (bool): Flag to set option `yes-i-know`
            kw (dict): Key/Value pairs to be provided to bootstrap command
                Supported keys:
                    fsid (str): Cluster FSID
                    image (str): Container image
                    registry-url (str): URL for custom registry
                    registry-username (str): Username for custom registry
                    registry-password (str): Password for custom registry
                    registry-json (str): json file with custom registry login info
                    mon-ip (str): Mon IP address
                    config (str): Ceph conf file to incorporate
                    skip-dashboard (str): Do not enable the Ceph Dashboard
                    initial-dashboard-user (str): Initial user for the dashboard
                    initial-dashboard-password (str): Initial password for the initial dashboard user
                    allow-overwrite (str): Allow overwrite of existing --output-* config/keyring/ssh files
                    allow-fqdn-hostname (str): Allow hostname that is fully-qualified
                    output-dir (str): Directory to write config, keyring, and pub key files
                    apply-spec (str): Apply cluster spec after bootstrap
                    cluster-network (str): Subnet to use for cluster replication, recovery and heartbeats
        """
        cmd = self.base_cmd
        cmd += f" --image {image} " if image else ""
        cmd += f" bootstrap{build_cmd_from_args(**kw)}"
        cmd += " --yes-i-know" if yes_i_know else ""

        return self.execute(sudo=True, long_running=True, cmd=cmd)

    def rm_cluster(self, fsid, zap_osds=True, force=True):
        """Remove cephadm cluster from nodes

        Args:
            node (str): ceph node object
            fsid (str): cluster FSID
            zap_osds (bool): remove OSDS
            force (bool): use `--force`
        """
        cmd = f"{self.base_cmd} rm-cluster --fsid {fsid}"
        if zap_osds:
            cmd += " --zap-osds"
        if force:
            cmd += " --force"

        return self.execute(sudo=True, long_running=True, cmd=cmd)

    def prepare_host(self, hostname=None):
        """Verify prepare host command

        Args:
            hostname (str): hostname
        """
        cmd = f"{self.base_cmd} prepare-host"
        if hostname:
            cmd += f" --expect-hostname {hostname}"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def logs(self, fsid, name):
        """Run the cephadm logs command

        Args:
            fsid (str): cluster FSID
            name (str): daemon name
        """
        cmd = f"{self.base_cmd} logs --fsid {fsid} --name {name}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
