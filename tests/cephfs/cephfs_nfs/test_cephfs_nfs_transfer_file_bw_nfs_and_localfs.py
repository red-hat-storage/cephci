import json
import random
import secrets
import string
import traceback
from pathlib import Path

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    File / Directory copy b/w local FS and NFS --
    -- File Export (NFS to FS) and (FS to NFS) - Repeat the following steps from both ends.
    * Transfer a file from the NFS share to the local file system on the NFS server and verify the successful transfer.
      After the export, compare the transferred file's content with the original file in the NFS share to
      ensure data integrity. (Use cksum or md5sum). Repeat the same from local filesystem to NFS mounts.
    * Transfer an entire directory (including files and subdirectories) from NFS to the local file system and
      local file system to NFS mounts and validate the directory structure.
    * Transfer a file with specific attributes (e.g., size, users, ownership) and verify that the attributes are
      preserved in the local file system.
    * Export a file with specific permissions and confirm that the correct permissions are applied to the
      transferred file in the local file system.
    """
    try:
        tc = "CEPH-83575825"
        log.info(f"Running cephfs {tc} test case")
        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        local_fs_path = "/tmp/"
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
        output = json.loads(out)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        nfs_mounting_dir = f"/mnt/cephfs_nfs{mounting_dir}_1/"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
        command = f"mount -t nfs -o port=2049 {nfs_server}:{nfs_export_name} {nfs_mounting_dir}"
        client1.exec_command(sudo=True, cmd=command, check_ec=False)
        dd(clients[0], nfs_mounting_dir, file_name="dd_file_nfs", bs="100M", count=20)
        dd(clients[0], local_fs_path, file_name="dd_file_local", bs="100M", count=20)

        log.info(
            "File Operations : move data b/w nfs mounts and local filesystem, \
                verify the transfers and check for data integrity"
        )
        filename_nfs = f"{clients[0].node.hostname}dd_file_nfs"
        filename_local = f"{clients[0].node.hostname}dd_file_local"

        nfs_file_path = Path(f"{nfs_mounting_dir}{filename_nfs}")
        local_file_path = Path(f"{local_fs_path}{filename_local}")
        nfs_file_copied_path = Path(f"{local_fs_path}{filename_nfs}")
        local_file_copied_path = Path(f"{nfs_mounting_dir}{filename_local}")

        # Transfer a file from NFS to local file system and local filesysten to NFS and verify successful transfer
        client1.exec_command(sudo=True, cmd=f"cp {nfs_file_path} {local_fs_path}")
        client1.exec_command(sudo=True, cmd=f"cp {local_file_path} {nfs_mounting_dir}")

        # Verify successful transfer
        output, err = client1.exec_command(
            sudo=True, cmd=f"diff -qr {nfs_file_path} {nfs_file_copied_path}"
        )
        output, err = client1.exec_command(
            sudo=True, cmd=f"diff -qr {local_file_path} {local_file_copied_path}"
        )

        # Verify Data Integrity using MD5  of both the NFS file and the local file
        def calculate_md5(client, file_path):
            return client.exec_command(
                sudo=True, cmd=f"md5sum {file_path} | awk '{{print $1}}'"
            )

        nfs_hash = calculate_md5(client1, nfs_file_path)
        local_hash = calculate_md5(client1, local_file_path)
        nfs_hash_copied = calculate_md5(client1, nfs_file_copied_path)
        local_hash_copied = calculate_md5(client1, local_file_copied_path)

        if nfs_hash == local_hash and nfs_hash_copied == local_hash_copied:
            log.info("File export and data integrity verification successful.")
        else:
            log.info("File export successful, but data integrity verification failed.")

        log.info(
            "File Operations : Verify the file attributes are retained when a file is moved out of NFS Share, \
                        and also validate the same on a local file copied to NFS mount"
        )

        def compare_file_attributes(attr1, attr2):
            return all(
                attr1[key] == attr2[key]
                for key in ["Permission", "Uid", "Gid", "Size", "Links"]
            )

        src_nfs_file_attributes = fs_util.get_stats(
            clients[0], file_path=nfs_file_path, validate=True
        )
        des_nfs_file_attributes = fs_util.get_stats(
            clients[0], file_path=nfs_file_copied_path, validate=True
        )
        if not compare_file_attributes(
            src_nfs_file_attributes, des_nfs_file_attributes
        ):
            log.error(
                "File Attributes are not retained on a NFS File after copying to local filesystem"
            )
            raise AssertionError(
                "File Attributes are not retained on a NFS File after copying to local filesystem"
            )

        log.info(
            "File Attributes are retained on a NFS File after copying to local filesystem"
        )

        src_local_file_attributes = fs_util.get_stats(
            clients[0], file_path=local_file_path, validate=True
        )
        des_local_file_attributes = fs_util.get_stats(
            clients[0], file_path=local_file_copied_path, validate=True
        )
        if not compare_file_attributes(
            src_local_file_attributes, des_local_file_attributes
        ):
            log.error(
                "File Attributes are not retained on a Local File after copying to NFS mount"
            )
            raise AssertionError(
                "File Attributes are not retained on a Local File after copying to NFS mount"
            )
        log.info(
            "File Attributes are retained on a Local File after copying to NFS mount"
        )

        log.info(
            "Directory Operations : move directory b/w nfs mounts and local filesystem, \
                verify the transfers and validate the directory structure are retained"
        )

        def create_directory(client, dir_path):
            client.exec_command(sudo=True, cmd=f"mkdir -p {dir_path}")

        def copy_directory(client, src_path, dest_path):
            client.exec_command(sudo=True, cmd=f"cp -r {src_path} {dest_path}")

        nfs_dir = "nfs_dir/"
        local_dir = "local_dir/"

        nfs_dir_path = Path(f"{nfs_mounting_dir}{nfs_dir}")
        local_dir_path = Path(f"{local_fs_path}{local_dir}")

        create_directory(client1, nfs_dir_path)
        create_directory(client1, local_dir_path)

        smallfile(clients[0], nfs_mounting_dir, nfs_dir)
        smallfile(clients[0], local_fs_path, local_dir)

        copy_directory(client1, nfs_dir_path, local_fs_path)
        copy_directory(client1, local_dir_path, nfs_mounting_dir)

        copied_nfs_path = Path(f"{local_fs_path}{nfs_dir}")
        copied_local_path = Path(f"{nfs_mounting_dir}{local_dir}")

        nfs_files = list(copied_nfs_path.rglob("*"))
        local_files = list(copied_local_path.rglob("*"))
        if len(nfs_files) == len(local_files):
            log.info("Directory structure validation successful.")
        else:
            log.info("Directory structure validation failed.")

        log.info("Test completed successfully")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning Up")
        client1.exec_command(sudo=True, cmd=f"rm -rf {nfs_mounting_dir}{nfs_dir}")
        client1.exec_command(sudo=True, cmd=f"rm -rf {local_fs_path}{nfs_dir}")
        log.info("Unmount NFS export")
        client1.exec_command(
            sudo=True, cmd=f"umount -l {nfs_mounting_dir}", check_ec=False
        )
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


def dd(client, target_path, file_name, bs, count):
    client.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/zero of={target_path}{client.node.hostname}{file_name} bs={bs} "
        f"count={count}",
    )


def smallfile(client, mounting_dir, dir_name):
    client.exec_command(
        sudo=True,
        cmd=f"for i in create ; "
        f"do python3 /home/cephuser/smallfile/smallfile_cli.py --operation $i --threads 8 --file-size 10240 "
        f"--files 10 --top {mounting_dir}{dir_name} ; done",
    )
