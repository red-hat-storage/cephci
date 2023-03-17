import json
import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-11304 - file locking -- utility for file locking

    Steps Performed:
    1. Create FS and mount on all the ways (Fuse, kernel, NFS)
    2. Create a file in mount and lock it.
    3. Try to acquire the lock from other mounts on same file
    Args:
        ceph_cluster:
        **kw:
    Exception : FCNTL locking is not working on nfs mounts
    Returns:
        0 --> if test PASS
        1 --> if test FAIL

    """
    try:
        log.info(f"MetaData Information {log.metadata} in {__name__}")
        fs_util = FsUtils(ceph_cluster)

        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        clients[0].upload_file(
            "tests/cephfs/clients/file_lock_utitlity.py",
            "/home/cephuser/file_lock_utility.py",
            sudo=True,
        )
        clients[1].upload_file(
            "tests/cephfs/clients/file_lock_utitlity.py",
            "/home/cephuser/file_lock_utility.py",
            sudo=True,
        )
        version, rc = clients[0].exec_command(
            sudo=True, cmd="ceph version --format json"
        )
        ceph_version = json.loads(version)
        nfs_mounting_dir = "/mnt/nfs/"
        dir_name = "dir"
        fs_util.prepare_clients([clients[0]], build)
        fs_util.auth_list([clients[0], clients[1]])
        if not build.startswith(("3", "4", "5")):
            if not fs_util.validate_fs_info(clients[0], "cephfs"):
                log.error("FS info Validation failed")
                return 1
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}/"
        fs_util.fuse_mount([clients[0], clients[1]], fuse_mounting_dir)

        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0], clients[1]], kernel_mounting_dir, ",".join(mon_node_ips)
        )

        if "nautilus" not in ceph_version["version"]:
            nfs_server = ceph_cluster.get_ceph_objects("nfs")
            nfs_client = ceph_cluster.get_ceph_objects("client")
            fs_util.auth_list(nfs_client)
            nfs_name = "cephfs-nfs"
            fs_name = "cephfs"
            nfs_export_name = "/export1"
            path = "/"
            nfs_server_name = nfs_server[0].node.hostname
            # Create ceph nfs cluster
            nfs_client[0].exec_command(sudo=True, cmd="ceph mgr module enable nfs")
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server_name}"
            )
            # Verify ceph nfs cluster is created
            if wait_for_process(
                client=nfs_client[0], process_name=nfs_name, ispresent=True
            ):
                log.info("ceph nfs cluster created successfully")
            else:
                raise CommandFailed("Failed to create nfs cluster")
            # Create cephfs nfs export
            if "5.0" in build:
                nfs_client[0].exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                    f"{nfs_export_name} path={path}",
                )
            else:
                nfs_client[0].exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export create cephfs {nfs_name} "
                    f"{nfs_export_name} {fs_name} path={path}",
                )

            # Verify ceph nfs export is created
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"ceph nfs export ls {nfs_name}"
            )
            if nfs_export_name in out:
                log.info("ceph nfs export created successfully")
            else:
                raise CommandFailed("Failed to create nfs export")
            # Mount ceph nfs exports
            nfs_client[0].exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
            assert fs_util.wait_for_cmd_to_succeed(
                nfs_client[0],
                cmd=f"mount -t nfs -o port=2049 {nfs_server_name}:{nfs_export_name} {nfs_mounting_dir}",
            )
            nfs_client[0].exec_command(
                sudo=True,
                cmd=f"mount -t nfs -o port=2049 {nfs_server_name}:{nfs_export_name} {nfs_mounting_dir}",
            )
            out, rc = nfs_client[0].exec_command(cmd="mount")
            mount_output = out.split()
            log.info("Checking if nfs mount is is passed of failed:")
            assert nfs_mounting_dir.rstrip("/") in mount_output
            log.info("Creating Directory")
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"mkdir {nfs_mounting_dir}{dir_name}"
            )
            rc = lock_file(
                clients[0],
                clients[1],
                "fuse_mount.txt",
                fuse_mounting_dir,
                validate_from=[kernel_mounting_dir, nfs_mounting_dir],
            )
            # import pdb
            # pdb.set_trace()
            if rc:
                raise CommandFailed("Able to acquire lock from different mount")
            rc = lock_file(
                clients[0],
                clients[1],
                "kernel_mount.txt",
                kernel_mounting_dir,
                validate_from=[fuse_mounting_dir, nfs_mounting_dir],
            )
            if rc:
                raise CommandFailed("Able to acquire lock from different mount")
            # rc = lock_file(
            #     clients[0],
            #     "nfs_mount.txt",
            #     nfs_mounting_dir,
            #     validate_from=[fuse_mounting_dir, kernel_mounting_dir],
            # )
            # if rc:
            #     raise CommandFailed("Able to acquire lock from different mount")

            return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("---clean up---------")
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir
        )
        fs_util.client_clean_up(
            "umount",
            kernel_clients=[clients[0]],
            mounting_dir=kernel_mounting_dir,
        )


def lock_file(client1, client2, file_name, lock_from_mount_dir, validate_from=[]):
    client1.exec_command(sudo=True, cmd=f"touch {lock_from_mount_dir}{file_name}")
    client1.exec_command(
        sudo=True,
        cmd=f"python3 /home/cephuser/file_lock_utility.py {lock_from_mount_dir}{file_name} lock --timeout 120 &",
        long_running=True,
    )
    # out, rc = client.exec_command(
    #     sudo=True,
    #     cmd=f"python3 /home/cephuser/file_lock_utility.py {validate_from[1]}{file_name} lock",
    # )
    # if "The file is already locked" not in out:
    #     log.error(
    #         f"Able to lock the file which has been locked using {lock_from_mount_dir} in {validate_from[1]}"
    #     )
    #     return 1
    out, rc = client2.exec_command(
        sudo=True,
        cmd=f"python3 /home/cephuser/file_lock_utility.py {validate_from[0]}{file_name} lock",
    )
    if "The file is already locked" not in out:
        log.error(
            f"Able to lock the file which has been locked using {lock_from_mount_dir} in {validate_from[0]}"
        )
        return 1

    client1.exec_command(
        sudo=True,
        cmd=f"python3 /home/cephuser/file_lock_utility.py {lock_from_mount_dir}{file_name} unlock",
    )
