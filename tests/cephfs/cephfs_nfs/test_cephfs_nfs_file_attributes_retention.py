import json
import random
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    * File Attribute change and transfer to other locations and verify its attributes are intact.
        - Export a file with specific permissions and confirm that the correct permissions are applied
          to the transferred file in the local file system.
        - Export a file to the local file system with a different name and ensure the exported file is
          correctly renamed during the transfer.
    """
    try:
        tc = "CEPH-83575937"
        log.info(f"Running cephfs {tc} test case")
        config = kw["config"]
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        local_fs_path = "/local/"
        files = ["file_777", "file_444", "file_600", "file_755"]
        client1.exec_command(sudo=True, cmd="ceph mgr module enable nfs")
        client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        if "5.0" in rhbuild:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                f"{nfs_export_name} path={export_path}",
            )
        else:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_name} {fs_name} path={export_path}",
            )
        out, rc = client1.exec_command(sudo=True, cmd=f"ceph nfs export ls {nfs_name}")

        if nfs_export_name not in out:
            raise CommandFailed("Failed to create nfs export")

        log.info("ceph nfs export created successfully")
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph nfs export get {nfs_name} {nfs_export_name}"
        )
        json.loads(out)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        nfs_mounting_dir = f"/mnt/cephfs_nfs{mounting_dir}_1/"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
        command = f"mount -t nfs -o port=2049 {nfs_server}:{nfs_export_name} {nfs_mounting_dir}"
        client1.exec_command(sudo=True, cmd=command, check_ec=False)
        client1.exec_command(sudo=True, cmd=f"mkdir -p {local_fs_path}")

        log.info(
            "Move a file from NFS share to a local file system by renaming the src file name on "
        )
        file = "file.txt"
        file_rename = "new_file.txt"
        # Move the file from the NFS mount to the local filesystem with the new name.
        client1.exec_command(sudo=True, cmd=f"touch {nfs_mounting_dir}{file}")
        client1.exec_command(
            sudo=True, cmd=f"mv {nfs_mounting_dir}{file} {local_fs_path}{file_rename}"
        )

        # Get the file attributes from the local filesystem.
        file_attributes = fs_util.get_stats(
            client1, file_path=f"{local_fs_path}{file_rename}"
        )
        if file_attributes["File"] == f"{local_fs_path}{file_rename}":
            log.info("File has been renamed successfully.")
        else:
            raise AssertionError("File rename failed.")

        log.info(
            "Move/Copy files with specific permissions and validate the permissions applied are retained \
        once its copied or moved to another location"
        )
        # Create files with the specified permissions in both the local filesystem and the NFS mount.
        for file, permission in zip(files, ["777", "444", "600", "755"]):
            client1.exec_command(sudo=True, cmd=f"touch {local_fs_path}local_{file}")
            client1.exec_command(sudo=True, cmd=f"touch {nfs_mounting_dir}nfs_{file}")
            client1.exec_command(
                sudo=True, cmd=f"chmod {permission} {local_fs_path}local_{file}"
            )
            client1.exec_command(
                sudo=True, cmd=f"chmod {permission} {nfs_mounting_dir}nfs_{file}"
            )

        # Move the files from the local filesystem to the NFS mount.
        for file in files:
            client1.exec_command(
                sudo=True, cmd=f"mv {local_fs_path}local_{file} {nfs_mounting_dir}"
            )

        # Copy the files from the NFS mount to the local filesystem.
        for file in files:
            client1.exec_command(
                sudo=True, cmd=f"cp -p {nfs_mounting_dir}nfs_{file} {local_fs_path}"
            )

        # Get the file attributes from the NFS mount and the local filesystem.
        nfs_file_attributes = []
        local_file_attributes = []

        for file in files:
            nfs_file_attributes.append(
                fs_util.get_stats(client1, file_path=f"{nfs_mounting_dir}nfs_{file}")
            )
            local_file_attributes.append(
                fs_util.get_stats(client1, file_path=f"{local_fs_path}nfs_{file}")
            )

        # Check that the file permissions are the same in both locations.
        for i in range(len(files)):
            if (
                nfs_file_attributes[i]["Octal_Permission"]
                != local_file_attributes[i]["Octal_Permission"]
            ):
                raise AssertionError("Permissions are not retained")
            else:
                log.info("Permissions are retained")

        log.info("Test completed successfully")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning Up")
        log.info("Unmount NFS export")
        client1.exec_command(
            sudo=True, cmd=f"umount -l {nfs_mounting_dir}", check_ec=False
        )
        log.info("Remove Paths")
        client1.exec_command(sudo=True, cmd=f"rm -rf {local_fs_path}")
        client1.exec_command(sudo=True, cmd=f"rm -rf {nfs_mounting_dir}")
        log.info("Removing the Export")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} {nfs_export_name}",
            check_ec=False,
        )
        log.info("Removing NFS Cluster")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster rm {nfs_name}",
            check_ec=False,
        )
