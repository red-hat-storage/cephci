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
            nfs_server (str): Name of the servere for which OSD to be created
        """
        cmd = f"{self.base_cmd} create {name} {nfs_server}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
