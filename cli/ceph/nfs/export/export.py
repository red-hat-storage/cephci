from random import randint
from cli import Cli
from cli.exceptions import OperationFailedError
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
        client_addr="",
        installer = None
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
        conf_template = """
EXPORT {
    Export_ID = %s;
    Path = "%s";
    Pseudo = "%s";
    Protocols = 4;
    Transports = TCP;
    Access_Type = RW;
    Squash = %s;
    FSAL {
        Name = "CEPH";
    }
    %s
}"""
        client_entry = """
    CLIENT {
        Clients = %s
    }
"""
        # stop ganesha service
        pid = ""
        try:
            cmd = "pgrep ganesha"
            out = installer.exec_command(sudo=True, cmd=cmd)
            pid = out[0].strip()
            print("PID : ", pid)
        except Exception:
            print("Ganesha process not running")

        if pid:
            cmd = f"kill -9 {pid}"
            installer.exec_command(sudo=True, cmd=cmd)

        subvol_name = nfs_export.replace("/", "")
        # Create subvolume
        cmd = f"ceph fs subvolume create cephfs {subvol_name} --group_name ganeshagroup --namespace-isolated"
        installer.exec_command(sudo=True, cmd=cmd)

        # Get volume path
        cmd = (
            f"ceph fs subvolume getpath cephfs {subvol_name} --group_name ganeshagroup"
        )
        out = installer.exec_command(sudo=True, cmd=cmd)
        path = out[0].strip()

        cmd = "cat /etc/ganesha/ganesha.conf | grep -o -P '(?<=Export_ID = ).*(?=;)' | tail -1"
        out = installer.exec_command(cmd=cmd, sudo=True)
        _id = out[0].strip()
        id = str(int(_id) + 1)
        if client_addr:
            client_entry = client_entry % client_addr
        else:
            client_entry = ""
        ganesha_conf = conf_template % (id, path, nfs_export, squash, client_entry)
        ganesha_conf_file = "/etc/ganesha/ganesha.conf"
        with installer.remote_file(sudo=True, file_name=ganesha_conf_file, file_mode="a") as _f:
            _f.write(ganesha_conf)
        log.info(f"Updated Conf file :  {ganesha_conf}")
        # Restart Ganesha
        cmd = f"nfs-ganesha/build/ganesha.nfsd -f /etc/ganesha/ganesha.conf -L /var/log/ganesha.log"
        installer.exec_command(sudo=True, cmd=cmd)

        # Check if ganesha service is up
        cmd = "pgrep ganesha"
        out = installer.exec_command(sudo=True, cmd=cmd)
        pid = out[0].strip()
        if not pid:
            raise OperationFailedError("Failed to restart nfs service")

#         # Step 1: Check if the subvolume group is present.If not, create subvolume group
#         cmd = "ceph fs subvolumegroup ls cephfs"
#         out = installer.exec_command(sudo=True, cmd=cmd)
#         if "[]" in out[0]:
#             cmd = "ceph fs subvolumegroup create cephfs ganeshagroup"
#             installer.exec_command(sudo=True, cmd=cmd)
#             log.info("Subvolume group created successfully")
#         subvol_name = nfs_export.replace("/", "")
#
#         # Step 2: Create subvolume
#         cmd = f"ceph fs subvolume create cephfs {subvol_name} --group_name ganeshagroup --namespace-isolated"
#         installer.exec_command(sudo=True, cmd=cmd)
#         # Get volume path
#         cmd = (
#             f"ceph fs subvolume getpath cephfs {subvol_name} --group_name ganeshagroup"
#         )
#         out = installer.exec_command(sudo=True, cmd=cmd)
#         path = out[0].strip()
#         conf_template = """
# EXPORT {
#     Export_ID = %s;
#     Path = "%s";
#     Pseudo = "%s";
#     Protocols = 4;
#     Transports = TCP;
#     Access_Type = %s;
#     Squash = None;
#     FSAL {
#         Name = "CEPH";
#     }
# } """
#         if client_addr:
#             conf_template += """
#     CLIENT
#     {
#          Clients = %s
#     }
# """
#         access_type = "R" if readonly else "RW"
#         conf_template = conf_template % (randint(200, 300), path, nfs_export, access_type)
#         ganesha_conf_file = "/etc/ganesha/ganesha.conf"
#         with installer.remote_file(sudo=True, file_name=ganesha_conf_file, file_mode="a") as _f:
#             _f.write(conf_template)
#
#         # stop ganesha service
#         pid = ""
#         try:
#             cmd = "pgrep ganesha"
#             out = installer.exec_command(sudo=True, cmd=cmd)
#             pid = out[0].strip()
#             print("PID : ", pid)
#         except Exception:
#             pass
#
#         if pid:
#             cmd = f"kill -9 {pid}"
#             installer.exec_command(sudo=True, cmd=cmd)
#         # Restart Ganesha
#         cmd = f"nfs-ganesha/build/ganesha.nfsd -f /etc/ganesha/ganesha.conf -L /var/log/ganesha.log"
#         installer.exec_command(sudo=True, cmd=cmd)
#
#         # Check if ganesha service is up
#         cmd = "pgrep ganesha"
#         out = installer.exec_command(sudo=True, cmd=cmd)
#         pid = out[0].strip()
#         if not pid:
#             raise OperationFailedError("Failed to restart nfs service")

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

    def get(self, nfs_name, nfs_export):
        """
        get a given nfs export
        Args:
            nfs_name (str): Name of nfs cluster
            nfs_export (str): Name of nfs export
        """
        cmd = f"{self.base_cmd} get {nfs_name} {nfs_export}"
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
