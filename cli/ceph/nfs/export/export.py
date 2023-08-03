from cli import Cli


class Export(Cli):
    def __init__(self, nodes, base_cmd):
        super(Export, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} export"

    def create(
        self,
        fs_name,
        nfs_name,
        nfs_export,
        fs,
        subvol_path=None,
        readonly=None,
        squash=None,
    ):
        """
        Perform create operation for nfs cluster
        Args:
            fs_name (str): File system name
            nfs_name (str): Name of nfs cluster
            nfs_export (str): Name of nfs export
            fs (str) : fs path
            subvol_path (str) : subvolume path
            readonly (Boolean) : enable readonly on export
            squash (str) : value to squash
        """
        cmd = f"{self.base_cmd} create {fs_name} {nfs_name} {nfs_export} {fs}"
        if subvol_path:
            cmd += f" path={subvol_path}"
        if readonly:
            cmd += " --readonly"
        if squash:
            cmd += f" --squash={squash}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def delete(self, cluster, export):
        """
        Deletes a given nfs export
        Args:
            cluster (str): Nfs cluster name
            export (str): Nfs export name
        """
        cmd = f"{self.base_cmd} delete {cluster} {export}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
