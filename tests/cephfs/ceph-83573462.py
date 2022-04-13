import json
import random
import string
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisites :
    1. Fill 60% date of the cluster
    Test operation:
    1. Create a volume
    2. Mount the cephfs on both fuse and kernel clients
    3. Create few directory from the both clients
    4. Execute the command "ceph fs set <fs_name> max_mds n [where n is the number]"
    5. Check if the number of mds increases and decreases properly
    """
    try:
        tc = "CEPH-83573462"
        log.info(f"Running CephFS tests for BZ-{tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        client2 = clients[1]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        mon_node_ips = fs_util.get_mon_node_ips()
        kernel_dir_generate = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        kernel_mounting_dir = f"/mnt/cephfs_kernel{kernel_dir_generate}/"
        fs_util.auth_list([client1])
        fs_util.kernel_mount([client1], kernel_mounting_dir, ",".join(mon_node_ips))
        client1.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={kernel_mounting_dir}" + ".txt bs=5M count=1000",
            long_running=True,
        )
        for i in range(10):
            dir_name_generate = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(5))
            )
            client1.exec_command(
                sudo=True, cmd=f"mkdir {kernel_mounting_dir}dir_{dir_name_generate}"
            )
        fuse_dir_generate = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse{fuse_dir_generate}/"
        client2.exec_command(sudo=True, cmd="dnf install ceph-fuse")
        fs_util.auth_list([client2])
        fs_util.fuse_mount([client2], fuse_mounting_dir)
        for i in range(10):
            dir_name_generate = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(5))
            )
            client2.exec_command(
                sudo=True, cmd=f"mkdir {fuse_mounting_dir}dir_{dir_name_generate}"
            )
        c1_out, c1_result = client1.exec_command(
            sudo=True, cmd="ceph fs get cephfs -f json"
        )
        decoded_out = json.loads(c1_out)
        number_of_up_temp = decoded_out["mdsmap"]["up"]
        number_of_up = len(number_of_up_temp)
        number_of_mds_max = decoded_out["mdsmap"]["max_mds"]
        c1_out2, result2 = client1.exec_command(sudo=True, cmd="ceph -s -f json")
        decoded_out2 = json.loads(c1_out2)
        number_of_standby = decoded_out2["fsmap"]["up:standby"]
        log.info(number_of_standby)
        counts = number_of_standby
        for i in range(counts):
            number_of_mds_max = number_of_mds_max + 1
            client1.exec_command(
                sudo=True, cmd=f"ceph fs set cephfs max_mds {str(number_of_mds_max)}"
            )
            number_of_standby = number_of_standby - 1
            number_of_up = number_of_up + 1
            time.sleep(50)
            kernel_output, kernel_result = client1.exec_command(
                sudo=True, cmd="ceph fs get cephfs -f json"
            )
            kernel_decoded = json.loads(kernel_output)
            current_max_mds = kernel_decoded["mdsmap"]["max_mds"]
            kernel_output2, kernel_result2 = client1.exec_command(
                sudo=True, cmd="ceph -s -f json"
            )
            kernel_decoded2 = json.loads(kernel_output2)
            current_standby = kernel_decoded2["fsmap"]["up:standby"]
            if current_max_mds != number_of_mds_max:
                return 1
            if number_of_up != number_of_mds_max:
                return 1
            if number_of_standby != current_standby:
                return 1
        for i in range(counts):
            number_of_mds_max = number_of_mds_max - 1
            client1.exec_command(
                sudo=True, cmd=f"ceph fs set cephfs max_mds {str(number_of_mds_max)}"
            )
            number_of_standby = number_of_standby + 1
            number_of_up = number_of_up - 1
            time.sleep(50)
            kernel_output, kernel_result = client1.exec_command(
                sudo=True, cmd="ceph fs get cephfs -f json"
            )
            kernel_decoded = json.loads(kernel_output)
            current_max_mds = kernel_decoded["mdsmap"]["max_mds"]
            kernel_output2, kernel_result2 = client1.exec_command(
                sudo=True, cmd="ceph -s -f json"
            )
            kernel_decoded2 = json.loads(kernel_output2)
            current_standby = kernel_decoded2["fsmap"]["up:standby"]
            if current_max_mds != number_of_mds_max:
                return 1
            if number_of_up != number_of_mds_max:
                return 1
            if number_of_standby != current_standby:
                return 1
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
