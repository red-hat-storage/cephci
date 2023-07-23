from cli import Cli


class Cluster(Cli):
    def __init__(self, nodes, base_cmd):
        super(Cluster, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} cluster"

    def create(self, name, nfs_server):
        """
        Perform create operation for nfs cluster
        Args:
            name (str): Name of the cluster
            nfs_server (list,tuple): Name of the server on which NFS Cluster to be created
        """
        nfs_server = nfs_server if type(nfs_server) in (list, tuple) else [nfs_server]
        nfs_server = " ".join(nfs_server)
        cmd = f"{self.base_cmd} create {name} '{nfs_server}'"
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
        cmd = f"{self.base_cmd} delete {name}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
