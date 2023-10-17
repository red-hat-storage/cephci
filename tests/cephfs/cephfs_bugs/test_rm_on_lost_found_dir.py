import json
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        """
        CEPH-83575781 - Allow entries to be removed from lost+found directory in File System

        Steps to create lost+found directory in FS
        1. Create filesystem
        2. mount the Fs
        3. write data
        4. purge the metadata pool
        5. get rados objects from the data written and deleta object
        6. Reset the sessions,snaps,inodes and file system
        7. Fail the Fs
        8. Reset the FS
        9. Initate recovery tools on FS
        10. This will create lost+found dir

        As part of TC delete the contents on lost+found  directory

        Cleanup:
        1. unmount the FS
        2. Remove FS

        """
        tc = "CEPH-83575781"
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_name = "cephfs_lost_found"
        fs_util.create_fs(client1, fs_name)
        fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util.fuse_mount(
            [client1],
            fuse_mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs_name}",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"cd {fuse_mount_dir};"
            f"mkdir subdir;"
            f"dd if=/dev/urandom of=subdir/sixmegs bs=1M conv=fdatasync count=6 seek=0;",
        )

        out, rc = client1.exec_command(
            sudo=True, cmd=f"rados ls -p cephfs.{fs_name}.data -f json"
        )
        rados_obj_ls = json.loads(out)
        log.info("Delete one of the rados object")
        if not rados_obj_ls:
            raise CommandFailed(
                "Can not execute the test cases as there are no rados objects created"
            )
        client1.exec_command(
            sudo=True,
            cmd=f"rados rm {rados_obj_ls[0]['name']} -p cephfs.{fs_name}.data",
        )
        client1.exec_command(
            sudo=True, cmd=f"cephfs-table-tool {fs_name}:0 reset session"
        )
        log.info("Fail File System")
        client1.exec_command(sudo=True, cmd=f"ceph fs fail {fs_name}")
        log.info("Reset sessions snaps and inodes")
        cmd_list = [
            f"cephfs-table-tool {fs_name}:0 reset session",
            f"cephfs-table-tool {fs_name}:0 reset snap",
            f"cephfs-table-tool {fs_name}:0 reset inode",
        ]
        for cmd in cmd_list:
            client1.exec_command(sudo=True, cmd=cmd)

        log.info("Reset the filesystem")
        client1.exec_command(
            sudo=True, cmd=f"ceph fs reset {fs_name} --yes-i-really-mean-it"
        )
        log.info("Reset journal")
        client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank={fs_name}:0 journal reset --force",
        )

        log.info("Initiate recovery tools on filesystem")
        cmd_list = [
            f"cephfs-data-scan init --force-init --filesystem {fs_name}",
            f"cephfs-data-scan scan_extents --filesystem {fs_name}",
            f"cephfs-data-scan scan_inodes --filesystem {fs_name}",
            f"cephfs-data-scan scan_links --filesystem {fs_name}",
            f"cephfs-data-scan cleanup --filesystem {fs_name}",
        ]
        for cmd in cmd_list:
            client1.exec_command(sudo=True, cmd=cmd)

        log.info("Mark MDS as repaired")
        client1.exec_command(sudo=True, cmd=f"ceph mds repaired {fs_name}:0")

        out, rc = client1.exec_command(sudo=True, cmd=f"ls -lrt {fuse_mount_dir}")
        if "lost+found" not in out:
            raise CommandFailed("Unable to create lost+found directory")
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ls -lrt {fuse_mount_dir}/lost+found/"
        )
        log.info(f"Contents of {fuse_mount_dir}/lost+found/ :\n {out}")
        client1.exec_command(sudo=True, cmd=f"rm -rf {fuse_mount_dir}/lost+found/*")
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ls -lrt {fuse_mount_dir}/lost+found/"
        )
        if "total 0" not in out:
            log.error(
                f"Contents of {fuse_mount_dir}/lost+found/ after deletion:\n {out}"
            )
            raise CommandFailed(
                f"Some of the contents left over in {fuse_mount_dir}/lost+found/"
            )
        log.info(
            f"Contents of {fuse_mount_dir}/lost+found/ after deletion:\n {out}"
            f"\nWe are able to delete contents of lost+found dir"
        )

        return 0

    except CommandFailed as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        client1.exec_command(sudo=True, cmd=f"umount {fuse_mount_dir}", check_ec=False)
        client1.exec_command(sudo=True, cmd=f"rm -rf {fuse_mount_dir}", check_ec=False)
        fs_util.remove_fs(client1, fs_name)
