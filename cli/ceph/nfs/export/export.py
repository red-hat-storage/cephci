from cli import Cli
from cli.utilities.utils import build_cmd_from_args
from utility.log import Log

from .qos import Qos

log = Log(__name__)


class Export(Cli):
    def __init__(self, nodes, base_cmd):
        super(Export, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} export"
        self.qos = Qos(nodes, self.base_cmd)

    def create(
        self,
        fs_name,
        nfs_name,
        nfs_export,
        fs,
        subvol_path=None,
        readonly=None,
        squash=None,
        client_addr=None,
        **kwargs,
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
            client-addr (str) : Authorized client hostname/IP
        """
        # Step 1: Check if the subvolume group is present.If not, create subvolume group
        cmd = "ceph fs subvolumegroup ls cephfs"
        out = self.execute(sudo=True, cmd=cmd)
        if "ganeshagroup" not in out[0]:
            cmd = "ceph fs subvolumegroup create cephfs ganeshagroup "
            self.execute(sudo=True, cmd=cmd)
            log.info("Subvolume group created successfully")
        subvol_name = nfs_export.replace("/", "")

        # Step 2: Create subvolume
        cmd = f"ceph fs subvolume create cephfs {subvol_name} --group_name ganeshagroup --namespace-isolated "
        cmd = "".join(cmd + build_cmd_from_args(**kwargs))
        self.execute(sudo=True, cmd=cmd)
        # Get volume path
        cmd = (
            f"ceph fs subvolume getpath cephfs {subvol_name} --group_name ganeshagroup "
        )
        out = self.execute(sudo=True, cmd=cmd)
        path = out[0].strip()

        # Step 3: Create export
        cmd = f"{self.base_cmd} create {fs_name} {nfs_name} {nfs_export} {fs} --path={path} "
        enctag = kwargs.get("enctag")
        if enctag:
            cmd = f"{cmd} --kmip_key_id={enctag}"
        if readonly:
            cmd += " --readonly"
        if squash:
            cmd += f" --squash={squash}"
        if client_addr:
            cmd += f" --client-addr={client_addr}"
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

    def get(self, nfs_name, nfs_export, **kwargs):
        """
        get a given nfs export
        Args:
            nfs_name (str): Name of nfs cluster
            nfs_export (str): Name of nfs export
        """
        cmd = f"{self.base_cmd} get {nfs_name} {nfs_export} "
        cmd = "".join(cmd + build_cmd_from_args(**kwargs))
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def apply(self, nfs_name, export_conf):
        """
        apply a given nfs export
        Args:
            nfs_name (str): Name of nfs cluster
            export_conf (str): Export conf file name
        """
        cmd = f"{self.base_cmd} apply {nfs_name} -i {export_conf}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def ls(self, nfs_name=None, **kwargs):
        """
        List nfs exports
        Args:
            nfs_name (str): Name of nfs cluster
        """
        cmd = f"{self.base_cmd} ls"
        if nfs_name:
            cmd += f" {nfs_name} " + build_cmd_from_args(**kwargs)
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
