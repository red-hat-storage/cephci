import json

from cli import Cli
from cli.utilities.utils import build_cmd_from_args
from utility.log import Log

from .qos import Qos

log = Log(__name__)


class Cluster(Cli):
    def __init__(self, nodes, base_cmd):
        super(Cluster, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} cluster"
        self.qos = Qos(nodes, self.base_cmd)

    def create(
        self,
        name,
        nfs_server,
        ha=False,
        vip=None,
        active_standby=False,
        nfs_nodes_obj=None,
        check_ec=True,
        **kwargs,
    ):
        """
        Perform create operation for nfs cluster
        Args:
            name (str): Name of the cluster
            nfs_server (list,tuple): Name of the server on which NFS Cluster to be created
            ha (bool): Flag to check if HA is required
            vip (str): Vip for the HA cluster
            active_standby (bool): Flag to check if active standby is required
        """
        nfs_server = nfs_server if type(nfs_server) in (list, tuple) else [nfs_server]
        if nfs_nodes_obj:
            for node_obj in nfs_nodes_obj:
                self.validate_rpcbind_running(node_obj)
        nfs_server = " ".join(nfs_server)
        if active_standby and ha:
            nfs_server = "1 " + nfs_server
        cmd = "{0} create {1} '{2}'".format(self.base_cmd, name, nfs_server)
        if ha:
            cmd += " --ingress --virtual-ip {0}".format(vip)

        cmd = "".join(cmd + build_cmd_from_args(**kwargs))
        try:
            out = self.execute(sudo=True, cmd=cmd, check_ec=check_ec)
        except Exception as e:
            raise RuntimeError(f"Failed to create NFS cluster: {e}")
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def delete(self, name):
        """
        Perform delete operation for nfs cluster
        Args:
            name (str): Name of the cluster
        """
        cmd = "{0} delete {1}".format(self.base_cmd, name)
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def ls(self):
        """
        List the NFS clusters and return as a Python list
        """
        cmd = f"{self.base_cmd} ls --format json"
        out = self.execute(sudo=True, cmd=cmd)

        # Extract stdout if tuple (output, err)
        if isinstance(out, tuple):
            out = out[0].strip()

        # Convert JSON string to Python list
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            raise ValueError("Failed to parse JSON output")

    def info(self, name):
        """
        Get information about a specific NFS cluster
        Args:
            name (str): Name of the cluster
        """
        cmd = "{0} info {1} --format json".format(self.base_cmd, name)
        out = self.execute(sudo=True, cmd=cmd)

        # Extract stdout if tuple (output, err)
        if isinstance(out, tuple):
            out = out[0].strip()

        # Convert JSON string to Python dictionary
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            raise ValueError("Failed to parse JSON output")

    def validate_rpcbind_running(self, nfs_node):
        """
        Check if the rpcbind service is running on RHEL 10.1 OS
        """
        try:
            # Check rpcbind service status
            out, err = nfs_node.exec_command(
                sudo=True, cmd="systemctl is-active rpcbind", check_ec=False
            )

            status = out.strip() if out else "inactive"

            if status != "active":
                log.info("rpcbind service is not running, start the service")
                nfs_node.exec_command(sudo=True, cmd="systemctl enable --now rpcbind")
            else:
                log.info("rpcbind already running")

        except Exception as e:
            log.error(f"Failed to check/start rpcbind: {e}")
