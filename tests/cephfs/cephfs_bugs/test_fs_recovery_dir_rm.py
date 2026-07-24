import random
import string
import traceback

from ceph.ceph import CommandFailed
from ceph.utils import get_node_by_id
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from tests.cephfs.lib.cephfs_recovery_lib import FSRecovery
from utility.log import Log

log = Log(__name__)


def test_setup(fs_util, ceph_cluster, client):
    """
    This method is Setup to create test configuration - subvolumegroup,subvolumes,nfs servers
    """
    log.info("Create fs volume if the volume is not there")
    default_fs = "cephfs_recovery"
    fs_details = fs_util.get_fs_info(client, fs_name=default_fs)

    if not fs_details:
        fs_util.create_fs(client, default_fs)

    nfs_servers = ceph_cluster.get_ceph_objects("nfs")
    nfs_server = nfs_servers[0].node.hostname
    nfs_name = "cephfs-nfs"

    fs_util.create_nfs(
        client,
        nfs_cluster_name=nfs_name,
        nfs_server_name=nfs_server,
    )
    if wait_for_process(client=client, process_name=nfs_name, ispresent=True):
        log.info("ceph nfs cluster created successfully")
    else:
        raise CommandFailed("Failed to create nfs cluster")
    nfs_export_name = "/export_" + "".join(
        random.choice(string.digits) for i in range(3)
    )
    log.info(
        "Create subvolumegroup, Create subvolume in subvolumegroup and default group"
    )
    subvolumegroup = {"vol_name": default_fs, "group_name": "subvolgroup_1"}
    fs_util.create_subvolumegroup(client, **subvolumegroup)
    sv_def = {
        "vol_name": default_fs,
        "subvol_name": "sv_def_1",
        "size": "5368706371",
    }
    fs_util.create_subvolume(client, **sv_def)
    sv_non_def = {
        "vol_name": default_fs,
        "subvol_name": "sv_non_def_1",
        "group_name": "subvolgroup_1",
        "size": "5368706371",
    }
    fs_util.create_subvolume(client, **sv_non_def)
    sv_list = [sv_def, sv_non_def]
    setup_params = {
        "default_fs": default_fs,
        "subvolumegroup": subvolumegroup,
        "sv_list": sv_list,
        "nfs_name": nfs_name,
        "nfs_export_name": nfs_export_name,
        "nfs_server": nfs_server,
    }
    return setup_params


def test_io(default_fs, client, fs_util, sv_list, nfs_params):
    """
    This method is to mount and add dataset on test subvolumes
    """
    try:
        mnt_type_list = ["kernel", "fuse", "nfs"]
        mnt_details = {}
        for sv in sv_list:
            cmd = f"ceph fs subvolume getpath {default_fs} {sv['subvol_name']}"
            if sv.get("group_name"):
                cmd += f" {sv['group_name']}"

            subvol_path, rc = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            mnt_path = subvol_path.strip()
            sv_name = sv["subvol_name"]
            nfs_export = f"{nfs_params['nfs_export_name']}_{sv_name}"
            mount_params = {
                "fs_util": fs_util,
                "client": client,
                "mnt_path": mnt_path,
                "fs_name": default_fs,
                "export_created": 0,
                "nfs_export_name": nfs_export,
                "nfs_server": nfs_params["nfs_server"],
                "nfs_name": nfs_params["nfs_name"],
            }
            sv_name = sv["subvol_name"]
            mnt_details.update({sv_name: {}})
            for mnt_type in mnt_type_list:
                mounting_dir, _ = fs_util.mount_ceph(mnt_type, mount_params)
                mnt_details[sv_name].update({mnt_type: mounting_dir})

            io_mnt = random.choice(mnt_type_list)
            mounting_dir = mnt_details[sv_name][io_mnt]
            file_list = add_dataset(client, mounting_dir)
        io_data = {"file_list": file_list, "mnt_details": mnt_details}
        return io_data
    except Exception as ex:
        log.info(ex)
        return 1


def add_dataset(client, mounting_dir):
    """
    This method is to add add files with data using fio to directory path created as - Nested(depth-5,breadth-1),
    regular(breadth-5,depth-1) and mix format(depth-5,breadth-5)
    """
    file_list = []
    log.info("Add nested directory dir_nested")
    dir_path = f"{mounting_dir}/testdir/dir_nested/"
    cmd = f"mkdir -p {dir_path}"
    client.exec_command(sudo=True, cmd=cmd)

    for i in range(1, 5):
        dir_path += f"dir_{i}/"
    cmd = f"mkdir -p {dir_path}"
    client.exec_command(sudo=True, cmd=cmd)
    file_path = f"{dir_path}/fio_file_1m"
    file_list.append(file_path)

    log.info("Add directory with one-level depth and multi breadth")
    dir_path = f"{mounting_dir}/testdir/dir_reg/"
    cmd = f"mkdir -p {dir_path}"
    client.exec_command(sudo=True, cmd=cmd)

    for i in range(1, 5):
        cmd = f"mkdir {dir_path}/dir_{i}/"
        client.exec_command(sudo=True, cmd=cmd)
        file_path = f"{dir_path}/dir_{i}/fio_file_1m"
        file_list.append(file_path)

    log.info("Add directory with multi-level breadth and depth")
    dir_path = f"{mounting_dir}/testdir/dir_mix/"
    cmd = f"mkdir -p {dir_path}"
    client.exec_command(sudo=True, cmd=cmd)
    # multi-depth
    for i in range(1, 5):
        dir_path += f"dir_{i}/"
    cmd = f"mkdir -p {dir_path}"
    client.exec_command(sudo=True, cmd=cmd)
    file_path = f"{dir_path}/fio_file_1m"
    file_list.append(file_path)
    # multi-breadth
    dir_path = f"{mounting_dir}/testdir/dir_mix/"
    for i in range(2, 6):
        cmd = f"mkdir {dir_path}/dir_{i}/"
        client.exec_command(sudo=True, cmd=cmd)
        file_path = f"{dir_path}/dir_{i}/fio_file_1m"
        file_list.append(file_path)

    for file_path in file_list:
        client.exec_command(
            sudo=True,
            cmd=f"fio --name={file_path} --ioengine=libaio --size 2M --rw=write --bs=1M --direct=1 "
            f"--numjobs=1 --iodepth=5 --runtime=10",
            timeout=20,
        )
    return file_list


def rados_metadata_obj_rm(node, fs_name, dir_objs):
    """
    This method removes directory object corresponding to inode from rados metadata pool
    """
    for sv_name in dir_objs:
        for dir_path in dir_objs[sv_name]:
            dir_obj = dir_objs[sv_name][dir_path]
            cmd = (
                f"cephadm shell rados --pool cephfs.{fs_name}.meta ls | grep {dir_obj}"
            )
            out = node.exec_command(sudo=True, cmd=cmd)
            log.info(out)
            cmd = f"cephadm shell rados --pool cephfs.{fs_name}.meta rm {dir_obj}.00000000"
            node.exec_command(sudo=True, cmd=cmd)


def validate_after_recovery(client, file_list, inode_details):
    """
    This method compares inode num for dir object after recoery, Run RW ops on files within
    """
    log.info("Verify all dir objects are recovered")
    for sv_name in inode_details:
        for dir_path in inode_details[sv_name]:
            cmd = f"ls -id {dir_path}"
            inode_orig = inode_details[sv_name][dir_path]
            try:
                out, _ = client.exec_command(sudo=True, cmd=cmd)
                log.info(out)
                inode_num, _ = out.split()
                if int(inode_num) == int(inode_orig):
                    log.info(
                        f"Dir object recovery suceeded,current_inode:{inode_num},old_inode:{inode_orig}"
                    )
                else:
                    log.error(
                        "Dir object recovery failed,current_inode:%s,old_inode:%s",
                        inode_num,
                        inode_orig,
                    )
            except BaseException as ex:
                log.info(ex)
                log.error(f"Could not list the directory inode in {dir_path}")
    for file_path in file_list:
        client.exec_command(
            sudo=True,
            cmd=f"fio --name={file_path} --ioengine=libaio --size 2M --rw=readwrite --bs=1M --direct=1 "
            f"--numjobs=1 --iodepth=5 --runtime=10",
            timeout=20,
        )


def get_inodes(client, mnt_details):
    """
    This method will fetch inode num for desired directories
    """
    inode_details = {}
    for sv_name in mnt_details:
        inode_details.update({sv_name: {}})
        mnt_list = ["kernel", "fuse", "nfs"]
        mnt_type = random.choice(mnt_list)
        mounting_dir = mnt_details[sv_name][mnt_type]
        dir_path = f"{mounting_dir}/testdir"
        for dir_name in ["dir_nested", "dir_reg", "dir_mix"]:
            out, _ = client.exec_command(sudo=True, cmd=f"ls -id {dir_path}/{dir_name}")
            inode_id, _ = out.split()
            dir_path_1 = f"{dir_path}/{dir_name}"
            inode_details[sv_name].update({dir_path_1: inode_id})
    return inode_details


def get_dir_objs(inode_details):
    """
    This method derives object id in hexadecimal format for relevant inode num
    """
    dir_objs = {}
    for sv_name in inode_details:
        dir_objs.update({sv_name: {}})
        for dir_path in inode_details[sv_name]:
            inode_num = int(inode_details[sv_name][dir_path])
            dir_obj = hex(inode_num)
            dir_obj_1 = dir_obj.replace("0x", "", 1)
            dir_objs[sv_name].update({dir_path: dir_obj_1})
    return dir_objs


def run(ceph_cluster, **kw):
    """
    Polarion TC CEPH-83609202, BZ 2342729:
    1. We need atleast one client node to execute this test case
    2. Create fs volume and subvolumes within.
    3. Mount subvolumes across kernel,fuse and nfs.
    4. Add nested directories and file with data
    5. Note the inode id of ~5 non-immediate directories across subvolumes
    6. Verify dir object id corresponding to directories noted in step5 in rados metadata pool
    7. Fail FS volume and remove ~5 directory objects
    8. Run FS recovery
    9. Verify the corresponding directories are accessible in all mountpoints
    10. Validate directories and file within restored directories
    11. Perform read-write ops inside restored directory and verify it suceeds

    Clean-up: Umount and Delete Subvolumes
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        fs_recovery = FSRecovery(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if len(clients) < 1:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        client1 = clients[0]
        log.info("Setup test configuration")
        setup_params = test_setup(fs_util, ceph_cluster, client1)
        default_fs = setup_params["default_fs"]

        sv_list = setup_params["sv_list"]
        nfs_params = {
            "nfs_name": setup_params["nfs_name"],
            "nfs_server": setup_params["nfs_server"],
            "nfs_export_name": setup_params["nfs_export_name"],
        }
        log.info("Mount subvolumes and add dataset")
        io_data = test_io(default_fs, client1, fs_util, sv_list, nfs_params)
        file_list = io_data["file_list"]
        mnt_details = io_data["mnt_details"]
        log.info(
            "Flush MDS journal to ensure required dir objects are in metadata pool"
        )
        active_mds_ranks = fs_recovery.get_active_mds_ranks(client1, default_fs)
        for mds_rank in active_mds_ranks:
            cmd = f"ceph tell mds.{default_fs}:{mds_rank} flush journal"
            client1.exec_command(sudo=True, cmd=cmd)

        log.info(f"subvolume mount details:{mnt_details}")
        inode_details = get_inodes(client1, mnt_details)
        log.info(f"inode details:{inode_details}")
        dir_objs = get_dir_objs(inode_details)
        log.info(f"dir objs:{dir_objs}")
        log.info(f"Fail Ceph FS volume {default_fs}")
        client1.exec_command(sudo=True, cmd=f"ceph fs fail {default_fs}")
        log.info(f"Remove dir objects from metadata pool in {default_fs}")
        node_installer = get_node_by_id(ceph_cluster, "node1")
        rados_metadata_obj_rm(node_installer, default_fs, dir_objs)
        log.info("Perform FS recovery workflow after metadata damage")
        fs_recovery.fs_recovery(client1, active_mds_ranks, fs_name=default_fs)
        log.info(
            "Validate the mountpoint after Recovery for data access and perform rw ops"
        )
        validate_after_recovery(client1, file_list, inode_details)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        for sv_name in mnt_details:
            for mnt_type in mnt_details[sv_name]:
                mnt_pt = mnt_details[sv_name][mnt_type]
                client1.exec_command(sudo=True, cmd=f"umount {mnt_pt}")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster delete {setup_params['nfs_name']}",
            check_ec=False,
        )
        for sv in sv_list:
            fs_util.remove_subvolume(client1, **sv, validate=False, check_ec=False)
        fs_util.remove_subvolumegroup(
            client1, default_fs, "subvolgroup_1", validate=True
        )
