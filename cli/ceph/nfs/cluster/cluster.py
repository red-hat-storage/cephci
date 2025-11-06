import json

from cli import Cli
from cli.utilities.utils import build_cmd_from_args

from .qos import Qos


class Cluster(Cli):
    def __init__(self, nodes, base_cmd):
        super(Cluster, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} cluster"
        self.qos = Qos(nodes, self.base_cmd)

    def create(
        self, name, nfs_server, ha=False, vip=None, active_standby=False, **kwargs
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
        nfs_server = " ".join(nfs_server)
        if active_standby and ha:
            nfs_server = "1 " + nfs_server
        cmd = "{0} create {1} '{2}'".format(self.base_cmd, name, nfs_server)
        if ha:
            cmd += " --ingress --virtual-ip {0}".format(vip)

        cmd = "".join(cmd + build_cmd_from_args(**kwargs))
        out = self.execute(sudo=True, cmd=cmd)
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
