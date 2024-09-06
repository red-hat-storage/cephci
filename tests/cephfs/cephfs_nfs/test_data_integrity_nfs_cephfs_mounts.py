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
    CEPH-11312 - Create the Files from NFS client and Modify their contents from CephFS. Verify that
    the changes done at CephFS side are reflected in the corresponding files without losing the data integrity.
    Perform this operation vice-versa
    Pre-requisites:
    1. Create cephfs volume
       create fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>
    Test operation:
    1. Mount cephfs with kernel & fuse clients
    2. Create cephfs nfs export with a valid path
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> path=<export_path>
    3. Verify path of cephfs nfs export
       ceph nfs export get <nfs_name> <nfs_export_name>
    4. Mount nfs export.
    5. Create a file on cephfs nfs export
    6. Write data to file created above on kernel mount
    7. Verify data integrity between nfs & kernel mount
    8. Modify contents of file on fuse mount
    9. Verify data integrity between nfs & fuse mount
    10. Modify contents of file on nfs mount
    11. Verify data integrity between nfs mount & kernel,fuse mounts

    Clean-up:
    1. Remove all data from cephfs
    2. Remove cephfs nfs export
    3. Remove NFS Cluster
    """
    try:
        tc = "CEPH-11309"
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
        default_fs = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, default_fs)

        if not fs_details:
            fs_util.create_fs(client1, default_fs)
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
        fs_name = "cephfs"
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
        output, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={default_fs}",
        )

        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" --client_fs {default_fs}",
        )

        clients[0].exec_command(sudo=True, cmd=f"touch {nfs_mounting_dir}nfs_file_1")
        file_data = "file_data" * 100
        clients[0].exec_command(
            sudo=True, cmd=f"echo -n {file_data} >> {kernel_mounting_dir_1}nfs_file_1"
        )
        out, rc = clients[0].exec_command(
            sudo=True, cmd=f"cat {nfs_mounting_dir}nfs_file_1"
        )
        if out != file_data:
            log.error("Data integrity check is failed")
            return 1
        clients[0].exec_command(
            sudo=True,
            cmd=f"diff {kernel_mounting_dir_1}nfs_file_1 {nfs_mounting_dir}nfs_file_1",
        )

        append_data = "append_data" * 50
        clients[0].exec_command(
            sudo=True, cmd=f"echo -n {append_data} >> {fuse_mounting_dir_1}nfs_file_1"
        )
        new_data = file_data + append_data
        out, rc = clients[0].exec_command(
            sudo=True, cmd=f"cat {nfs_mounting_dir}nfs_file_1"
        )
        if out != new_data:
            log.error("Data integrity check is failed")
            return 1
        clients[0].exec_command(
            sudo=True,
            cmd=f"diff {fuse_mounting_dir_1}nfs_file_1 {nfs_mounting_dir}nfs_file_1",
        )

        new_data = "new_data" * 50
        clients[0].exec_command(
            sudo=True, cmd=f"echo -n {new_data} > {nfs_mounting_dir}nfs_file_1"
        )
        out, rc = clients[0].exec_command(
            sudo=True, cmd=f"cat {kernel_mounting_dir_1}nfs_file_1"
        )
        if out != new_data:
            log.error("Data integrity check is failed")
            return 1
        clients[0].exec_command(
            sudo=True,
            cmd=f"diff {kernel_mounting_dir_1}nfs_file_1 {nfs_mounting_dir}nfs_file_1",
        )
        clients[0].exec_command(
            sudo=True,
            cmd=f"diff {fuse_mounting_dir_1}nfs_file_1 {nfs_mounting_dir}nfs_file_1",
        )

        log.info(
            "Data integrity check between cephfs fuse mount , kernel mount & nfs mount is successful"
        )
        log.info("Test completed successfully")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning Up")
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {nfs_mounting_dir}*", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"umount -l {nfs_mounting_dir}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"umount -l {kernel_mounting_dir_1}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"umount -l {fuse_mounting_dir_1}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {nfs_mounting_dir}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {kernel_mounting_dir_1}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {fuse_mounting_dir_1}", check_ec=False
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
