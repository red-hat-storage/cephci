import json
import logging
import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    try:
        bz = "1980920"

        fs_util = FsUtils(ceph_cluster)

        log.info("Running cephfs test for bug %s" % bz)

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
            "subvol_name": "subvol_" + str(subvolume_name_generate),
            "size": "5368706371",
        }
        subvolume_name = "subvol_" + str(subvolume_name_generate)
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
            sub_dir=f"{subvol_path.read().decode().strip()}",
        )

        for i in range(250):
            client1.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={kernel_mounting_dir_1}"
                + str(i)
                + ".txt bs=1 count=0 seek=5M",
                long_running=True,
            )

        fs_util.mount_dir_update(kernel_mounting_dir_1)

        log.info("checking Pre-requisites")

        fs_util.create_snapshot(
            client1, "cephfs", subvolume_name, "subvol_1_snap" + subvolume_name
        )

        for i in range(1, 4):

            new_subvolume_name = "subvol_1_snap_clone" + subvolume_name + str(i)
            fs_util.create_clone(
                client1,
                "cephfs",
                subvolume_name,
                "subvol_1_snap" + subvolume_name,
                new_subvolume_name,
            )
            out1, err1 = client1.exec_command(
                sudo=True, cmd="ceph fs clone status cephfs " + new_subvolume_name
            )

            output1 = json.loads(out1.read().decode())
            output2 = output1["status"]["state"]
            log.info(new_subvolume_name + " status: " + str(output2))

            result, error = client1.exec_command(
                sudo=True,
                cmd="ceph fs subvolume rm cephfs " + new_subvolume_name + " --force",
                check_ec=False,
            )
            log.info("Subvolume Remove executed")

            error_result = error.read().decode()

            if "clone in-progress" in error_result:
                log.info("Clone is in-progress as expected")

            if output2 == "in-progress":
                client1.exec_command(
                    sudo=True, cmd="ceph fs clone cancel cephfs " + new_subvolume_name
                )

            result2, error2 = client1.exec_command(
                sudo=True, cmd="ceph fs clone status cephfs " + new_subvolume_name
            )

            out1 = json.loads(result2.read().decode())
            out2 = out1["status"]["state"]

            if out2 == "canceled":
                client1.exec_command(
                    sudo=True,
                    cmd="ceph fs subvolume rm cephfs "
                    + new_subvolume_name
                    + " --force",
                )

        result3, error3 = client1.exec_command(
            sudo=True, cmd="ceph fs subvolume ls cephfs"
        )

        output = json.loads(result3.read().decode())

        for name in output:
            if name == subvolume_name:
                return 1

        return 0

    except Exception as e:
        print("exception occured")
        log.info(e)
        log.info(traceback.format_exc())
        return 1
