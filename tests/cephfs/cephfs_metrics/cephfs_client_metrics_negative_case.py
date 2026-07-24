import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)
"""
Client metrics negative test case
1. Run IOs on the mounted directory
2. Do other operations like increasing dentry, inode, opened_files, etc
3. Reboot the MDS node
4. Check if the metrics are changed as expected
"""


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83588357"
        log.info(f"Running CephFS tests for - {tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        fs_util.auth_list(clients)
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        client1 = clients[0]
        # if cephfs is not created, create cephfs
        cephfs = "cephfs"
        fs_util.create_fs(client1, cephfs)
        mdss = ceph_cluster.get_ceph_objects("mds")
        # set standby mds to 1
        client1.exec_command(sudo=True, cmd="ceph fs set cephfs standby_count_wanted 1")
        client1.exec_command(sudo=True, cmd="ceph fs set cephfs max_mds 2")
        # Check if jq package is installed
        jq_check = client1.exec_command(cmd="rpm -qa | grep jq")
        if "jq" not in jq_check:
            client1.exec_command(sudo=True, cmd="yum install -y jq")
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{rand}"
        fs_util.fuse_mount([client1], fuse_mounting_dir_1)
        # incerae metrics in the mounted directory
        # Icrease read and wrtie metrics in the mounted directory
        # dd 5GB file and copy 5GB file in the mounted directory with different name
        client1.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={fuse_mounting_dir_1}/5GBfile bs=10M count=512",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"cp {fuse_mounting_dir_1}/5GBfile {fuse_mounting_dir_1}/5GBfile_copy",
        )
        # Increase dentry related metrics in the mounted directory
        # Create 100 directories and 100 small files in the mounted directory
        for i in range(100):
            client1.exec_command(sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}/dir_{i}")
            client1.exec_command(
                sudo=True,
                cmd=f"echo 'CephFS Test {i}' > {fuse_mounting_dir_1}/file_{i}.txt",
            )
            # cat the files in the mounted directory
            client1.exec_command(
                sudo=True, cmd=f"cat {fuse_mounting_dir_1}/file_{i}.txt"
            )
        # Increase inode related metrics in the mounted directory
        # Create 100 files in the mounted directory
        for i in range(100):
            client1.exec_command(
                sudo=True, cmd=f"touch {fuse_mounting_dir_1}/file_{i}.txt"
            )
        # Increase opened_files metrics
        # Open 100 files in the mounted directory
        log.info("Open 100 files in the mounted directory")
        file_paths = []
        for i in range(10):
            file_path = f"{fuse_mounting_dir_1}/test_file_{i}.txt"
            client1.exec_command(sudo=True, cmd=f"echo 'CephFS Test {i}' > {file_path}")
            file_paths.append(file_path)
        # Open files using tail -f
        pids = []
        log.info("Opening files using tail -f")
        for file_path in file_paths:
            open_command = f"nohup tail -f {file_path} > /dev/null 2>&1 &"
            client1.exec_command(sudo=True, cmd=open_command)
        time.sleep(2)
        # Get process IDs of the opened files
        for file_path in file_paths:
            pid_command = f"ps aux | grep 'tail -f {file_path}' | grep -v grep | awk '{{print $2}}'"
            pid = client1.exec_command(sudo=True, cmd=pid_command)[0].strip()
            if pid:
                pids.append(pid)
        log.info(f"Opened file PIDs: {pids}")
        time.sleep(10)
        # get the metrics of the mounted directory
        before_reboot_metrics = fs_util.get_mds_metrics(
            client1, 0, fuse_mounting_dir_1
        )["counters"]
        log.info(f"Metrics before reboot: {before_reboot_metrics}")
        # find out current rank 0 mds name
        rank_0_mds_name = "ceph fs status cephfs --format json | jq -r '.mdsmap[] | select(.rank == 0) | .name'"
        rank_0 = client1.exec_command(sudo=True, cmd=rank_0_mds_name)[0].strip()

        @retry(EOFError, tries=5, delay=100)
        def wait_mds_node(mds_client):
            result, _ = mds_client.exec_command(sudo=True, cmd="ls")
            log.info(f"mds status: {result}")

        target_mds = []
        for mds in mdss:
            # reboot the node that contains the rank 0 mds
            log.info(mds.node)
            log.info(mds.node.hostname)
            if mds.node.hostname in rank_0:
                target_mds.append(mds)
                mds.node.exec_command(sudo=True, cmd="reboot")
                break
        wait_mds_node(mds.node)
        # wait for the mds to come back online
        log.info("Waiting for the MDS to come back online")
        time.sleep(60)
        # get the metrics of the mounted directory
        after_reboot_metrics = fs_util.get_mds_metrics(client1, 0, fuse_mounting_dir_1)[
            "counters"
        ]
        log.info(f"Metrics after reboot: {after_reboot_metrics}")
        # check if the metrics are same before and after the reboot
        for (key1, value1), (key2, value2) in zip(
            before_reboot_metrics.items(), after_reboot_metrics.items()
        ):
            log.info(f"{key1}: {value1} {key2}: {value2}")
            if key1 == "cap_hits" or key1 == "cap_miss":
                log.info(f"Skipping {key1} as it is not stable")
                continue
            elif (
                key1 == "opened_inodes"
                or key1 == "pinned_icaps"
                or key1 == "total_inodes"
            ):
                if value1 <= value2:
                    log.error(
                        f"the {key1} value should be decrease after the reboot from {value1} to {value2}"
                    )
                    raise Exception(
                        f"Metrics {key1} are not decreased after the reboot"
                    )
            else:
                if value1 != value2:
                    log.error(
                        f"the {key1} value should be same before and after the reboot"
                    )
                    raise Exception(
                        f"Metrics {key1} are not same before and after the reboot"
                    )
        log.info("Metrics are same before and after the reboot")
        try:
            for pid in pids:
                client1.exec_command(sudo=True, cmd=f"kill {pid}")
        except CommandFailed as e:
            log.error(f"Failed to kill tail processes: {e}")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        try:
            for pid in pids:
                client1.exec_command(sudo=True, cmd=f"kill {pid}")
        except CommandFailed as e:
            log.error(f"Failed to kill tail processes: {e}")
        # Cleanup
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
