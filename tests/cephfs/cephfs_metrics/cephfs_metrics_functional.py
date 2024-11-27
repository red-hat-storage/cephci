import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)
"""
Testing cephfs client metrics
1. Inode related metrics -> opened_inodes, pinned_icaps, total_inodes
- Write a file to the mounted directory and verify if inode metrics have increased
2. Cap related metrics -> cap_hits, cap_miss
- List the mounted directory and verify if cap metrics have increased
3. Dentry related metrics -> dentry_lease_hits, dentry_lease_miss
- Create directories and files, then read and list them to verify if dentry metrics have increased
4. File related metrics -> opened_files
- Open files using tail -f and verify if opened_files have increased and decrease after killing the tail processes
5. Read and write related metrics -> total_read_ops, total_read_size, total_write_ops, total_write_size
- Perform read and write operations to verify if read and write metrics have increased
"""


class Metrics_Value_Not_Matching(Exception):
    pass


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83588303"
        log.info(f"Running CephFS tests for - {tc}")

        # Initialize the utility class for CephFS
        fs_util = FsUtils(ceph_cluster)

        # Get the client nodes
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")

        # Authenticate the clients
        fs_util.auth_list(clients)
        build = config.get("build", config.get("rhbuild"))

        # Prepare the clients
        fs_util.prepare_clients(clients, build)
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        # Check if jq package is installed, install if not
        jq_check = client1.exec_command(cmd="rpm -qa | grep jq")
        if "jq" not in jq_check:
            client1.exec_command(sudo=True, cmd="yum install -y jq")

        # Generate random string for directory names
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )

        # Define mount directories
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{rand}"
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel_{rand}"

        # Mount CephFS using ceph-fuse and kernel
        fs_util.fuse_mount([client1], fuse_mounting_dir_1)
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount([client1], kernel_mounting_dir_1, ",".join(mon_node_ips))

        # Get initial MDS metrics
        mds_metric = fs_util.get_mds_metrics(client1, 0, fuse_mounting_dir_1)
        log.info("Verifying opened_inodes, pinned_icaps and total_inodes")
        ceph_health, _ = client1.exec_command(sudo=True, cmd="ceph -s")
        log.info(ceph_health)
        fs_status, _ = client1.exec_command(sudo=True, cmd="ceph fs status")
        log.info(fs_status)
        # Initialize dictionaries to store initial inode metrics
        inode_dic = {}
        inode_list = ["opened_inodes", "pinned_icaps", "total_inodes"]
        log.info(f"Initial inode related mds_metric_counters: {mds_metric}")
        for inode in inode_list:
            inode_dic[inode] = mds_metric["counters"][inode]
            log.info(f"{inode}: {mds_metric['counters'][inode]}")
        log.info(f"inode_dic: {inode_dic}")

        # Write a file to the mounted directory
        client1.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={fuse_mounting_dir_1}/{rand} bs=1M count=10",
        )
        time.sleep(5)

        # Get MDS metrics after writing the file
        mds_metric2 = fs_util.get_mds_metrics(client1, 0, fuse_mounting_dir_1)
        log.info(f"mds_metric after writing a file: {mds_metric2}")

        # Verify if inode metrics have increased
        for inode in inode_list:
            previous_inode = inode_dic[inode]
            current_inode = mds_metric2["counters"][inode]
            log.info(
                f"previous_inode: {previous_inode}, current_inode: {current_inode}"
            )
            if previous_inode >= current_inode:
                log.error(f"Failed to verify {inode}")
                raise Metrics_Value_Not_Matching(f"Failed to verify {inode}")

        # Verify cap_hits and cap_miss
        log.info("Verifying cap_hits, cap_miss")
        cap_dic = {}
        cap_list = ["cap_hits", "cap_miss"]
        for cap in cap_list:
            cap_dic[cap] = mds_metric2["counters"][cap]
            log.info(f"{cap}: {mds_metric2['counters'][cap]}")
        log.info(f"cap_dic: {cap_dic}")

        # List the mounted directory
        client1.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_1}")
        time.sleep(5)

        # Get MDS metrics after listing the directory
        mds_metric3 = fs_util.get_mds_metrics(client1, 0, fuse_mounting_dir_1)
        log.info(f"mds_metric after ls command: {mds_metric3}")

        # Verify if cap metrics have increased
        for cap in cap_list:
            previous_cap = cap_dic[cap]
            current_cap = mds_metric3["counters"][cap]
            log.info(f"previous_cap: {previous_cap}, current_cap: {current_cap}")
            if previous_cap >= current_cap:
                raise Metrics_Value_Not_Matching(f"Failed to verify {cap}")

        # Verify dentry_lease_hits and dentry_lease_miss
        log.info("Verifying dentry_lease_hits, dentry_lease_miss")
        dentry_dic = {}
        dentry_list = ["dentry_lease_hits", "dentry_lease_miss"]
        for dentry in dentry_list:
            dentry_dic[dentry] = mds_metric3["counters"][dentry]
            log.info(f"{dentry}: {mds_metric3['counters'][dentry]}")
        log.info(f"dentry_dic: {dentry_dic}")

        # Create directories and files, then read and list them
        for i in range(1, 50):
            client1.exec_command(
                sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}/dir_{rand}{i}/"
            )
            client1.exec_command(
                sudo=True,
                cmd=f"mkdir {fuse_mounting_dir_1}/dir_{rand}{i}/dir_{rand}{i}/",
            )
            client1.exec_command(
                sudo=True,
                cmd=f"touch {fuse_mounting_dir_1}/dir_{rand}{i}/file_{rand}{i}.txt",
            )
            client1.exec_command(
                sudo=True,
                cmd=f"cat {fuse_mounting_dir_1}/dir_{rand}{i}/file_{rand}{i}.txt",
            )
            client1.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_1}/")
            client1.exec_command(
                sudo=True, cmd=f"ls {fuse_mounting_dir_1}/dir_{rand}{i}/"
            )

        time.sleep(5)

        # Get MDS metrics after creating directories and files
        mds_metric4 = fs_util.get_mds_metrics(client1, 0, fuse_mounting_dir_1)
        log.info(f"mds_metric after mkdir command: {mds_metric4}")

        # Verify if dentry metrics have increased
        for dentry in dentry_list:
            previous_dentry = dentry_dic[dentry]
            current_dentry = mds_metric4["counters"][dentry]
            log.info(
                f"previous_dentry: {previous_dentry}, current_dentry: {current_dentry}"
            )
            if previous_dentry >= current_dentry:
                log.error(f"Failed to verify {dentry}")
                raise Metrics_Value_Not_Matching(f"Failed to verify {dentry}")

        # Verify opened files
        log.info("Verifying opened files")
        file_paths = []
        for i in range(5):
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

        # Get final MDS metrics after opening files
        final_metrics = fs_util.get_mds_metrics(client1, 0, fuse_mounting_dir_1)
        final_opened_files = final_metrics["counters"]["opened_files"]
        log.info(f"Increased opened_files: {final_opened_files}")
        if final_opened_files <= mds_metric4["counters"]["opened_files"]:
            raise Metrics_Value_Not_Matching("Failed to verify opened_files")
        # Kill the tail processes
        try:
            for pid in pids:
                client1.exec_command(sudo=True, cmd=f"kill {pid}")
        except CommandFailed as e:
            log.error(f"Failed to kill tail processes: {e}")
        time.sleep(2)
        # Get MDS metrics after killing the tail processes
        final_metrics2 = fs_util.get_mds_metrics(client1, 0, fuse_mounting_dir_1)
        log.info(f"final_metrics after killing tail process: {final_metrics2}")

        # Verify total_read_ops, total_read_size, total_write_ops, and total_write_size
        log.info(
            "Verifying total_read_ops, total_read_size, total_write_ops, total_write_size"
        )
        rw_dic = {}
        rw_list = [
            "total_read_ops",
            "total_read_size",
            "total_write_ops",
            "total_write_size",
        ]
        for read in rw_list:
            rw_dic[read] = final_metrics2["counters"][read]
            log.info(f"{read}: {final_metrics2['counters'][read]}")
        log.info(f"rw_dic: {rw_dic}")

        # Perform write operations
        client1.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={fuse_mounting_dir_1}/{rand}_read bs=10M count=400",
        )
        client1.exec_command(
            sudo=True, cmd=f"cp {fuse_mounting_dir_1}/{rand}_read /{rand}_copy"
        )

        # Perform read operations to increase total_read_ops and total_read_size
        log.info("Reading files to increase total_read_ops and total_read_size")
        for file_path in file_paths:
            for _ in range(5):  # Repeat the read operation multiple times
                client1.exec_command(
                    sudo=True,
                    cmd=f"head -c 100 {fuse_mounting_dir_1}/{rand}_read > /dev/null",
                )
                client1.exec_command(sudo=True, cmd=f"tail {file_path} > /dev/null")
        time.sleep(90)

        # Get final MDS metrics after reading and writing files
        final_metrics3 = fs_util.get_mds_metrics(client1, 0, fuse_mounting_dir_1)
        log.info(f"final_metrics after reading and writing a file: {final_metrics3}")
        # Verify if read and write metrics have increased
        for read in rw_list:
            previous_read = rw_dic[read]
            current_read = final_metrics3["counters"][read]
            log.info(f"previous_read: {previous_read}, current_read: {current_read}")
            if previous_read >= current_read:
                raise Metrics_Value_Not_Matching(f"Failed to verify {read}")
        ceph_health, _ = client1.exec_command(sudo=True, cmd="ceph -s")
        log.info(ceph_health)
        fs_status, _ = client1.exec_command(sudo=True, cmd="ceph fs status")
        log.info(fs_status)
        return 0
    except Metrics_Value_Not_Matching as e:
        log.error(e)
        log.error(traceback.format_exc())
        log.error("Metrics value not matching print all the metrics")
        log.error(fs_util.get_mds_metrics(client1, 0, fuse_mounting_dir_1, "cephfs"))
        return 1
    except CommandFailed as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        ceph_health, _ = client1.exec_command(sudo=True, cmd="ceph -s")
        log.info(ceph_health)
        fs_status, _ = client1.exec_command(sudo=True, cmd="ceph fs status")
        log.info(fs_status)
        # Cleanup
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
