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
        fs_util = FsUtils(ceph_cluster)
        log.info(f"Running CephFS tests for BZ-{bz}")
        log.info(f"Running CephFS tests for BZ-{tc}")
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        create_cephfs = "ceph fs volume create cephfs"
        client1.exec_command(sudo=True, cmd=create_cephfs)
        subvolume_name_generate = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        subvolume = {
            "vol_name": "cephfs",
            "subvol_name": f"subvol_{subvolume_name_generate}",
            "size": "5368706371",
        }
        subvolume_name = subvolume["subvol_name"]
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rcc = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolume getpath cephfs {subvolume_name}"
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
        )
        for i in range(250):
            client1.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={kernel_mounting_dir_1}"
                + str(i)
                + ".txt bs=1 count=0 seek=5M",
                long_running=True,
            )
        log.info("Checking Pre-requisites")
        fs_util.create_snapshot(
            client1, "cephfs", subvolume_name, f"subvol_1_snap{subvolume_name}"
        )
        for i in range(1, 4):
            new_subvolume_name = f"subvol_1_snap_clone{subvolume_name}{str(i)}"
            fs_util.create_clone(
                client1,
                "cephfs",
                subvolume_name,
                f"subvol_1_snap{subvolume_name}",
                new_subvolume_name,
            )
            out1, err1 = client1.exec_command(
                sudo=True, cmd=f"ceph fs clone status cephfs {new_subvolume_name}"
            )
            output1 = json.loads(out1)
            output2 = output1["status"]["state"]
            log.info(new_subvolume_name + " status: " + str(output2))
            result, error = client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume rm cephfs {new_subvolume_name} --force",
                check_ec=False,
            )
            log.info("Subvolume Remove Executed")
            if "clone in-progress" in error:
                log.info("Clone is in-progress as expected")
            if output2 == "in-progress":
                client1.exec_command(
                    sudo=True, cmd=f"ceph fs clone cancel cephfs {new_subvolume_name}"
                )
            result2, error2 = client1.exec_command(
                sudo=True, cmd=f"ceph fs clone status cephfs {new_subvolume_name}"
            )
            out1 = json.loads(result2)
            out2 = out1["status"]["state"]
            if out2 == "canceled":
                fs_util.remove_subvolume(
                    client1, "cephfs", new_subvolume_name, force=True
                )
        fs_util.remove_subvolume(client1, "cephfs", subvolume_name)
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
