import json
import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisites :
    1. Create a subvolume with Create a subvolume with sufficient data (around 500 files of 1 MB each)
    2. Create a snapshot of the above subvolume
    3. Create 4 number of clones from above snapshot

    Test operation:
    1. When the clone is in 'in-progress' state, delete the all the clone subvolumes with force option.
    2. Check if clone operation status is in 'in-progress' state
    3. Writing sufficient data in step 1 would provide enough time for you achieve that
    4. Try to delete the subvolume of the clone in 'in-progress' state
    5. The subvolume should not be able to be deleted
    6. Try to cancel the cloning
    7. After canceling the cloning, it should be able to delete the subvolume
    """
    try:
        bz = "1980920"
        tc = "CEPH-83574681"
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )

        log.info(f"Running CephFS tests for BZ-{bz}")
        log.info(f"Running CephFS tests for BZ-{tc}")
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        client1 = clients[0]
        default_fs = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, default_fs)
        if not fs_details:
            fs_util.create_fs(client1, default_fs)
        subvolume_name_generate = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": f"subvol_{subvolume_name_generate}",
        }
        subvolume_name = subvolume["subvol_name"]
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rcc = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolume getpath {default_fs} {subvolume_name}"
        )
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.auth_list([client1])
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
            extra_params=f",fs={default_fs}",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4000 "
            f"--files 100 --files-per-dir 100 --dirs-per-dir 5 --top "
            f"{kernel_mounting_dir_1}",
            long_running=True,
        )
        log.info("Checking Pre-requisites")
        fs_util.create_snapshot(
            client1, default_fs, subvolume_name, f"subvol_1_snap{subvolume_name}"
        )
        new_subvolume_name = f"subvol_1_snap_clone{subvolume_name}_1"
        fs_util.create_clone(
            client1,
            default_fs,
            subvolume_name,
            f"subvol_1_snap{subvolume_name}",
            new_subvolume_name,
        )
        stdout, stderr = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume rm {default_fs} {new_subvolume_name}",
            check_ec=False,
        )
        log.info(stdout, stderr)
        error_result = stderr
        log.info(error_result)
        if "is not ready for operation rm" in error_result:
            log.info("Clone is in-progress as expected")
        client1.exec_command(
            sudo=True, cmd=f"ceph fs clone cancel {default_fs} {new_subvolume_name}"
        )
        result2, error2 = client1.exec_command(
            sudo=True, cmd=f"ceph fs clone status {default_fs} {new_subvolume_name}"
        )
        out1 = json.loads(result2)
        out2 = out1["status"]["state"]
        if out2 == "canceled":
            fs_util.remove_subvolume(
                client1, default_fs, new_subvolume_name, force=True
            )
        fs_util.remove_snapshot(
            client1, default_fs, subvolume_name, f"subvol_1_snap{subvolume_name}"
        )
        fs_util.remove_subvolume(client1, default_fs, subvolume_name)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
