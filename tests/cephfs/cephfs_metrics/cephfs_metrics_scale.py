import json
import random
import string
import time
import traceback
from json import JSONDecodeError

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)

"""
Testing CephFS metrics Scale
1. Client Node1: try to increase "opened_files" and check only designed MDS increases the metric
2. Client Node2: try to increase "opened_inodes"and "opened_icaps" and "total_inodes"
3. Client Node2,4: Increase "dentry" realated metrcis
4. Client Node2 try to increase "total_read_ops" and "total_read_size"
5. Client Node4 try to increase "total_write_ops" and "total_write_size"
6. Do "scp" operation from Node1 to Node2
"""


class Metrics_Value_Not_Matching(Exception):
    pass


@retry((JSONDecodeError, CommandFailed), tries=5, delay=5)
def get_client_id(client, rank=0, mounted_dir="", fs_name="cephfs"):
    ranked_mds, _ = client.exec_command(
        sudo=True,
        cmd=f"ceph fs status {fs_name} -f json | jq '.mdsmap[] | select(.rank == {rank}) | .name'",
    )
    log.info("Executing MDS name with rank command: %s", ranked_mds)
    ranked_mds = ranked_mds.replace('"', "").replace("\n", "")
    client_id_cmd = (
        f"ceph tell mds.{ranked_mds} session ls | jq '.[] | select(.client_metadata.mount_point"
        f' != null and (.client_metadata.mount_point | contains("{mounted_dir}"))) | .id\''
    )
    log.info(f"Executing Client ID Command : {client_id_cmd}")
    client_id, _ = client.exec_command(sudo=True, cmd=client_id_cmd)
    client_id = client_id.replace('"', "").replace("\n", "")
    if client_id == "":
        log.error(f"Client not found for Mounted Directory : {mounted_dir}")
        raise CommandFailed(f"Client not found for Mounted Directory : {mounted_dir}")
    log.info(f"Client ID :[{client_id}] for Mounted Directory : [{mounted_dir}]")
    return client_id, rank


@retry((JSONDecodeError, CommandFailed), tries=5, delay=10)
def get_mds_metrics_for_client(
    client, client_id, rank, mds_rank=0, mounted_dir="", fs_name="cephfs"
):
    ranked_mds, _ = client.exec_command(
        sudo=True,
        cmd=f"ceph fs status {fs_name} -f json | jq '.mdsmap[] | select(.rank == {mds_rank}) | .name'",
    )
    log.info(f"Executing MDS name with rank command: {ranked_mds}")
    ranked_mds = ranked_mds.replace('"', "").replace("\n", "")
    log.info(f"Client ID :[{client_id}] for Mounted Directory : [{mounted_dir}]")
    cmd = f""" ceph tell mds.{ranked_mds} counter dump 2>/dev/null | \
                 jq -r '. | to_entries | map(select(.key | match("mds_client_metrics"))) | \
                 .[].value[] | select(.labels.client != null and (.labels.client | contains("{client_id}"))
                 and (.labels.rank == "{rank}"))'
                 """
    metrics_out, _ = client.exec_command(sudo=True, cmd=cmd)
    log.info(
        f"Metrics for MDS : {ranked_mds} Mounted Directory: {mounted_dir} and Client : {client_id} is {metrics_out}"
    )
    if metrics_out == "":
        log.error(f"Metrics not found for MDS : {ranked_mds}")
        raise CommandFailed(f"Client not found for Mounted Directory : {mounted_dir}")
    metrics_out = json.loads(str(metrics_out))
    return metrics_out


def get_mds_metrics_from_ranks(ranks, fs_util, client, mount_dir, cephfs):
    """
    Try fetching MDS metrics for the given client and mount_dir from the list of ranks.

    Returns:
        dict: MDS metrics if found.

    Raises:
        CommandFailed: If no metrics are found from any rank.
    """
    client_id = None
    client_rank = None

    # Step 1: Get client_id from one of the MDS ranks
    for rank in ranks:
        try:
            client_id, client_rank = get_client_id(client, rank, mount_dir, cephfs)
            if client_id:
                log.info(f"Found client ID '{client_id}' from rank {client_rank}")
                break
        except Exception as e:
            log.warning(
                f"Rank {rank}: Failed to get client ID for mount {mount_dir}: {e}"
            )
            continue

    if not client_id:
        raise CommandFailed(f"Client not found in any MDS ranks for mount {mount_dir}")

    # Step 2: Use client_id and try to collect metrics from all MDS ranks
    for rank in ranks:
        try:
            mds_metric = get_mds_metrics_for_client(
                client,
                client_id,
                client_rank,
                mds_rank=rank,
                mounted_dir=mount_dir,
                fs_name=cephfs,
            )
            if mds_metric and mds_metric != 1:
                log.info(f"Successfully got MDS metrics from rank {rank}")
                return mds_metric
        except Exception as e:
            log.warning(
                f"Rank {rank}: Failed to fetch metrics for client {client_id}: {e}"
            )
            continue

    raise CommandFailed(
        f"Metrics not found for client {client_id} in any of the MDS ranks for mount {mount_dir}"
    )


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83588355"
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
        client2 = clients[1]
        client3 = clients[2]
        client4 = clients[3]

        # install JQ package for all the clients
        for client in [client1, client2, client3, client4]:
            jq_check, _ = client.exec_command(cmd="rpm -qa | grep jq")
            if "jq" not in jq_check:
                client.exec_command(sudo=True, cmd="yum install -y jq")
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        cephfs = "cephfs"
        # Generate random string for directory names
        ranked_mds, _ = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs status {cephfs} -f json | jq '.mdsmap[] | select(.rank == 0) | .name'",
        )
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        ceph_health, _ = client1.exec_command(sudo=True, cmd="ceph -s")
        print(ceph_health)
        fs_status, _ = client1.exec_command(sudo=True, cmd="ceph fs status")
        print(fs_status)
        # Create CephFS
        fs_util.create_fs(client1, cephfs)
        mds_nodes = ceph_cluster.get_nodes("mds")
        host_list = [node.hostname for node in mds_nodes]
        hosts = " ".join(host_list)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph orch apply mds cephfs --placement='4 {hosts}'",
            check_ec=False,
        )
        time.sleep(60)
        # max mds set 3
        client1.exec_command(sudo=True, cmd="ceph fs set cephfs max_mds 3")
        # standby replay true
        client1.exec_command(
            sudo=True, cmd="ceph fs set cephfs allow_standby_replay true"
        )
        ceph_health, _ = client1.exec_command(sudo=True, cmd="ceph -s")
        log.info(ceph_health)
        fs_status, _ = client1.exec_command(sudo=True, cmd="ceph fs status")
        log.info(fs_status)
        # Define mount directories
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{rand}_1"
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse_{rand}_2"
        fuse_mounting_dir_3 = f"/mnt/cephfs_fuse_{rand}_3"
        fuse_mounting_dir_4 = f"/mnt/cephfs_fuse_{rand}_4"
        cephfs = "cephfs"
        # Mount CephFS using ceph-fuse and kernel
        out_test, _ = client1.exec_command(sudo=True, cmd="ceph fs status")
        log.info(out_test)
        fs_util.fuse_mount([client1], fuse_mounting_dir_1)
        client1.exec_command(
            sudo=True,
            cmd=f"touch {fuse_mounting_dir_1}/.ping && ls {fuse_mounting_dir_1}/.ping",
        )
        fs_util.fuse_mount([client2], fuse_mounting_dir_2)
        client2.exec_command(
            sudo=True,
            cmd=f"touch {fuse_mounting_dir_2}/.ping && ls {fuse_mounting_dir_2}/.ping",
        )
        fs_util.fuse_mount([client3], fuse_mounting_dir_3)
        client3.exec_command(
            sudo=True,
            cmd=f"touch {fuse_mounting_dir_3}/.ping && ls {fuse_mounting_dir_3}/.ping",
        )
        fs_util.fuse_mount([client4], fuse_mounting_dir_4)
        client4.exec_command(
            sudo=True,
            cmd=f"touch {fuse_mounting_dir_4}/.ping && ls {fuse_mounting_dir_4}/.ping",
        )

        # Allow MDS session registration update
        time.sleep(3)
        # Get initial MDS metrics
        # pdb.set_trace()

        mds_metric_client1 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client1, fuse_mounting_dir_1, cephfs
        )
        mds_metric_client2 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client2, fuse_mounting_dir_2, cephfs
        )
        mds_metric_client3 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client3, fuse_mounting_dir_3, cephfs
        )
        mds_metric_client4 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client4, fuse_mounting_dir_4, cephfs
        )

        log.info(f"mds_metric_client1: {mds_metric_client1}")
        log.info(f"mds_metric_client2: {mds_metric_client2}")
        log.info(f"mds_metric_client3: {mds_metric_client3}")
        log.info(f"mds_metric_client4: {mds_metric_client4}")
        # Initialize dictionaries to store initial inode metrics
        log.info("Verifying inode metrics")
        log.info("Verify if targeted clients metrics increase in terms of inode")
        log.info(
            "Increase only Client1 and Client3 inode metrics and other clients should remain same"
        )
        inode_dic_client1_pre = {}
        inode_dic_client3_pre = {}
        inode_list = ["opened_inodes", "pinned_icaps", "total_inodes"]

        # Get initial inode metrics for client1 and client3
        client1_pre_inode_metrics = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client1, fuse_mounting_dir_1, cephfs
        )
        client3_pre_inode_metrics = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client3, fuse_mounting_dir_3, cephfs
        )
        log.info(f"client1_pre_inode_metrics: {client1_pre_inode_metrics}")
        log.info(f"client3_pre_inode_metrics: {client3_pre_inode_metrics}")
        # Store initial inode metrics for client1 and client3
        for inode in inode_list:
            inode_dic_client1_pre[inode] = client1_pre_inode_metrics["counters"][inode]
            inode_dic_client3_pre[inode] = client3_pre_inode_metrics["counters"][inode]

        for i in range(100):
            client1.exec_command(
                sudo=True, cmd=f"touch {fuse_mounting_dir_1}/test_file_{i}.txt"
            )
        for i in range(100):
            client3.exec_command(
                sudo=True, cmd=f"touch {fuse_mounting_dir_3}/test_file_{i}.txt"
            )
        client1.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={fuse_mounting_dir_1}/{rand} bs=1M count=100",
        )
        client3.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={fuse_mounting_dir_3}/{rand} bs=1M count=100",
        )
        time.sleep(5)

        log.info("Writing files is done for client1 and client3")
        log.info("Get metrics only for client1 and client3")
        client1_post_inode_metrics = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client1, fuse_mounting_dir_1, cephfs
        )
        client3_post_inode_metrics = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client3, fuse_mounting_dir_3, cephfs
        )
        log.info(f"client1_post_inode_metrics: {client1_post_inode_metrics}")
        log.info(f"client3_post_inode_metrics: {client3_post_inode_metrics}")
        for inode in inode_list:
            if (
                inode_dic_client1_pre[inode]
                >= client1_post_inode_metrics["counters"][inode]
            ):
                log.error(f"Failed to verify {inode} for client1")
                raise Metrics_Value_Not_Matching(
                    f"Failed to verify {inode} for client1"
                )
            if (
                inode_dic_client3_pre[inode]
                >= client3_post_inode_metrics["counters"][inode]
            ):
                log.error(f"Failed to verify {inode} for client3")
                raise Metrics_Value_Not_Matching(
                    f"Failed to verify {inode} for client3"
                )
        log.info("Verified inode metrics for client1 and client3")
        log.info("Verify opened_files metrics for client2 and client4")
        log.info(
            "Increase only Client2 and Client4 opened_files metrics and other clients should remain same"
        )
        file_paths_client2 = []
        file_paths_client4 = []
        pre_opened_files_client2 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client2, fuse_mounting_dir_2, cephfs
        )["counters"]["opened_files"]
        pre_opened_files_client4 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client4, fuse_mounting_dir_4, cephfs
        )["counters"]["opened_files"]
        log.info(f"pre_opened_files_client2: {pre_opened_files_client2}")
        log.info(f"pre_opened_files_client4: {pre_opened_files_client4}")
        for i in range(100):
            file_path_client2 = f"{fuse_mounting_dir_2}/test_file_opened_files_{i}.txt"
            file_path_client4 = f"{fuse_mounting_dir_4}/test_file_opened_files_{i}.txt"
            client2.exec_command(
                sudo=True, cmd=f"echo 'CephFS Test {i}' > {file_path_client2}"
            )
            client4.exec_command(
                sudo=True, cmd=f"echo 'CephFS Test {i}' > {file_path_client4}"
            )
            file_paths_client2.append(file_path_client2)
            file_paths_client4.append(file_path_client4)
        pids2 = []
        pids4 = []
        log.info("Opening files using tail -f")
        for file_path in file_paths_client2:
            open_command = f"nohup tail -f {file_path} > /dev/null 2>&1 &"
            client2.exec_command(sudo=True, cmd=open_command)
        for file_path in file_paths_client4:
            open_command = f"nohup tail -f {file_path} > /dev/null 2>&1 &"
            client4.exec_command(sudo=True, cmd=open_command)
        time.sleep(2)
        for file_path in file_paths_client2:
            pid_command = f"ps aux | grep 'tail -f {file_path}' | grep -v grep | awk '{{print $2}}'"
            pid = client2.exec_command(sudo=True, cmd=pid_command)[0].strip()
            if pid:
                pids2.append(pid)
        for file_path in file_paths_client4:
            pid_command = f"ps aux | grep 'tail -f {file_path}' | grep -v grep | awk '{{print $2}}'"
            pid = client4.exec_command(sudo=True, cmd=pid_command)[0].strip()
            if pid:
                pids4.append(pid)
        log.info(f"Number of PID2s from opening files: {pids2}")
        log.info(f"Number of PID4s from opening files: {pids4}")
        time.sleep(10)
        log.info("Get final MDS metrics after opening files")
        client2_post_opened_files = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client2, fuse_mounting_dir_2, cephfs
        )["counters"]["opened_files"]
        client4_post_opened_files = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client4, fuse_mounting_dir_4, cephfs
        )["counters"]["opened_files"]
        log.info(f"client2_post_opened_files: {client2_post_opened_files}")
        log.info(f"client4_post_opened_files: {client4_post_opened_files}")
        if client2_post_opened_files <= pre_opened_files_client2:
            log.error("Failed to verify opened_files for client2")
            raise Metrics_Value_Not_Matching(
                "Failed to verify opened_files for client2"
            )
        if client4_post_opened_files <= pre_opened_files_client4:
            log.error("Failed to verify opened_files for client4")
            raise Metrics_Value_Not_Matching(
                "Failed to verify opened_files for client4"
            )

        try:
            for pid in pids2:
                client2.exec_command(sudo=True, cmd=f"kill {pid}")
            for pid in pids4:
                client4.exec_command(sudo=True, cmd=f"kill {pid}")
        except CommandFailed as e:
            log.error(f"Failed to kill tail processes: {e}")
        time.sleep(5)
        log.info("Get final MDS metrics after killing the PIDs")
        post_opened_files_client2 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client2, fuse_mounting_dir_2, cephfs
        )["counters"]["opened_files"]
        post_opened_files_client4 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client4, fuse_mounting_dir_4, cephfs
        )["counters"]["opened_files"]
        log.info(f"post_opened_files_client2: {post_opened_files_client2}")
        log.info(f"post_opened_files_client4: {post_opened_files_client4}")
        # check if it decreased to 0
        if int(post_opened_files_client2) != 0:
            log.error("Failed to verify opened_files for client2 expected 0")
            raise Metrics_Value_Not_Matching(
                "Failed to verify opened_files for client2"
            )
        if int(post_opened_files_client4) != 0:
            log.error("Failed to verify opened_files for client4 expected 0")
            raise Metrics_Value_Not_Matching(
                "Failed to verify opened_files for client4"
            )
        log.info("Verify if other clients opened_files metrics remain same")
        post_opened_files_client1 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client1, fuse_mounting_dir_1, cephfs
        )["counters"]["opened_files"]
        post_opened_files_client3 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client3, fuse_mounting_dir_3, cephfs
        )["counters"]["opened_files"]
        log.info(f"post_opened_files_client1: {post_opened_files_client1}")
        log.info(f"post_opened_files_client3: {post_opened_files_client3}")
        if int(post_opened_files_client1) != 0:
            log.error("Failed to verify opened_files for client1 expected 0")
            raise Metrics_Value_Not_Matching(
                "Failed to verify opened_files for client1"
            )
        if int(post_opened_files_client3) != 0:
            log.error("Failed to verify opened_files for client3 expected 0")
            raise Metrics_Value_Not_Matching(
                "Failed to verify opened_files for client3"
            )
        log.info("Verified opened_files metrics for client2 and client4")

        # verify dentry_lease_hits and dentry_lease_miss metrics for client2 and client4
        log.info(
            "Verify dentry_lease_hits and dentry_lease_miss metrics for client2 and client4"
        )
        log.info(
            "Increase only Client2 and Client4 dentry metrics and other clients should remain same"
        )
        pre_dentry_client1 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client1, fuse_mounting_dir_1, cephfs
        )["counters"]
        pre_dentry_client2 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client2, fuse_mounting_dir_2, cephfs
        )["counters"]
        pre_dentry_client3 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client3, fuse_mounting_dir_3, cephfs
        )["counters"]
        pre_dentry_client4 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client4, fuse_mounting_dir_4, cephfs
        )["counters"]
        log.info(f"pre_dentry_client2: {pre_dentry_client2}")
        log.info(f"pre_dentry_client4: {pre_dentry_client4}")
        dentry_list = ["dentry_lease_hits", "dentry_lease_miss"]
        log.info("Create directories and files and 'cat'ing for client2 and client4")
        for i in range(1, 50):
            dir = f"client_2_dir_{rand}{i}"
            client2.exec_command(sudo=True, cmd=f"mkdir {fuse_mounting_dir_2}/{dir}/")
            client2.exec_command(
                sudo=True,
                cmd=f"mkdir {fuse_mounting_dir_2}/{dir}/dir_{rand}{i}/",
            )
            client2.exec_command(
                sudo=True,
                cmd=f"touch {fuse_mounting_dir_2}/{dir}/file_{rand}{i}.txt",
            )
            client2.exec_command(
                sudo=True,
                cmd=f"cat {fuse_mounting_dir_2}/{dir}/file_{rand}{i}.txt",
            )
            client2.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_2}/")
            client2.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_2}/{dir}/")
        time.sleep(5)
        for i in range(1, 50):
            dir = f"client_4_dir_{rand}{i}"
            client4.exec_command(sudo=True, cmd=f"mkdir {fuse_mounting_dir_4}/{dir}/")
            client4.exec_command(
                sudo=True,
                cmd=f"mkdir {fuse_mounting_dir_4}/{dir}/dir_{rand}{i}/",
            )
            client4.exec_command(
                sudo=True,
                cmd=f"touch {fuse_mounting_dir_4}/{dir}/file_{rand}{i}.txt",
            )
            client4.exec_command(
                sudo=True,
                cmd=f"cat {fuse_mounting_dir_4}/{dir}/file_{rand}{i}.txt",
            )
            client4.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_4}/")
            client4.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_4}/{dir}/")
        time.sleep(5)
        log.info("Get final MDS metrics after creating directories and files")
        post_dentry_client2 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client2, fuse_mounting_dir_2, cephfs
        )["counters"]
        post_dentry_client4 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client4, fuse_mounting_dir_4, cephfs
        )["counters"]
        log.info(f"post_dentry_client2: {post_dentry_client2}")
        log.info(f"post_dentry_client4: {post_dentry_client4}")
        for dentry in dentry_list:
            if pre_dentry_client2[dentry] >= post_dentry_client2[dentry]:
                log.error(f"Failed to verify {dentry} for client2")
                raise Metrics_Value_Not_Matching(
                    f"Failed to verify {dentry} for client2"
                )
            if pre_dentry_client4[dentry] >= post_dentry_client4[dentry]:
                log.error(f"Failed to verify {dentry} for client4")
                raise Metrics_Value_Not_Matching(
                    f"Failed to verify {dentry} for client4"
                )
        log.info("Verify if other clients dentry metrics remain same")
        post_dentry_client1 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client1, fuse_mounting_dir_1, cephfs
        )["counters"]
        post_dentry_client3 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client3, fuse_mounting_dir_3, cephfs
        )["counters"]
        log.info(f"post_dentry_client1: {post_dentry_client1}")
        log.info(f"post_dentry_client3: {post_dentry_client3}")
        for dentry in dentry_list:
            if pre_dentry_client1[dentry] != post_dentry_client1[dentry]:
                log.error(f"Failed to verify {dentry} for client1")
                raise Metrics_Value_Not_Matching(
                    f"Failed to verify {dentry} for client1"
                )
            if pre_dentry_client3[dentry] != post_dentry_client3[dentry]:
                log.error(f"Failed to verify {dentry} for client3")
                raise Metrics_Value_Not_Matching(
                    f"Failed to verify {dentry} for client3"
                )
        log.info("Verified dentry metrics for client2 and client4")

        # Verify total_read_ops, total_read_size, total_write_ops, and total_write_size
        log.info(
            "Verifying total_read_ops, total_read_size, total_write_ops, total_write_size"
        )
        # Using scp from client1 to client2, it will increase total_read_ops and total_read_size in client1
        # In client2, it will increase total_write_ops and total_write_size
        pre_read_ops_client1 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client1, fuse_mounting_dir_1, cephfs
        )["counters"]
        pre_write_ops_client2 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client2, fuse_mounting_dir_2, cephfs
        )["counters"]
        log.info(f"pre_read_ops_client1: {pre_read_ops_client1}")
        log.info(f"pre_write_ops_client2: {pre_write_ops_client2}")
        # create a 1GB file in client1
        client1_scp_file = f"{fuse_mounting_dir_1}/{rand}_read"
        client1.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={client1_scp_file} bs=10M count=300",
        )
        client1.exec_command(sudo=True, cmd=f"chmod 755 {client1_scp_file}")
        # get ip from client2
        client2_ip = client2.exec_command(
            sudo=True,
            cmd="ip route get 8.8.8.8 | awk '{print $7}'",
        )[0].strip()
        # install sshpass package if not installed
        client2.exec_command(
            sudo=True, cmd=f"chown -R cephuser:cephuser {fuse_mounting_dir_2}"
        )
        client2.exec_command(sudo=True, cmd=f"chmod -R u+w {fuse_mounting_dir_2}")
        for client in clients:
            sshpass_check, _ = client.exec_command(
                cmd="rpm -qa | grep sshpass", check_ec=False
            )
            if "sshpass" not in sshpass_check:
                client.exec_command(sudo=True, cmd="yum install -y sshpass")
        # Copy the file from client1 to client2
        client1.exec_command(
            sudo=True,
            cmd=f"sshpass -p 'cephuser' scp -o StrictHostKeyChecking=no {client1_scp_file}"
            f" cephuser@{client2_ip}:{fuse_mounting_dir_2}/{rand}_read_copied",
        )
        time.sleep(90)
        rw_list = [
            "total_read_ops",
            "total_read_size",
            "total_write_ops",
            "total_write_size",
        ]
        log.info("Get final MDS metrics after copying a file")
        post_read_ops_client1 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client1, fuse_mounting_dir_1, cephfs
        )["counters"]
        post_write_ops_client2 = get_mds_metrics_from_ranks(
            [0, 1, 2], fs_util, client2, fuse_mounting_dir_2, cephfs
        )["counters"]
        log.info(f"post_read_ops_client1: {post_read_ops_client1}")
        log.info(f"post_write_ops_client2: {post_write_ops_client2}")
        for rw in rw_list[:2]:
            if pre_read_ops_client1[rw] > post_read_ops_client1[rw]:
                raise Metrics_Value_Not_Matching(f"Failed to verify {rw} for client1")
            log.info(f"Verified {rw} for client1")
        for rw in rw_list[2:]:
            if pre_write_ops_client2[rw] >= post_write_ops_client2[rw]:
                log.error(f"Failed to verify {rw} for client2")
                raise Metrics_Value_Not_Matching(f"Failed to verify {rw} for client2")
            log.info(f"Verified {rw} for client2")
        log.info(
            "Verified total_read_ops, total_read_size, total_write_ops, and total_write_size"
        )
        return 0

    except Metrics_Value_Not_Matching as e:
        log.error(e)
        log.error(traceback.format_exc())
        log.error("Metrics value not matching print all the metrics")
        log.error(fs_util.get_mds_metrics(client1, 0, fuse_mounting_dir_1, cephfs))
        log.error(fs_util.get_mds_metrics(client2, 0, fuse_mounting_dir_2, cephfs))
        log.error(fs_util.get_mds_metrics(client3, 0, fuse_mounting_dir_3, cephfs))
        log.error(fs_util.get_mds_metrics(client4, 0, fuse_mounting_dir_4, cephfs))
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
            "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[client2], mounting_dir=fuse_mounting_dir_2
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[client3], mounting_dir=fuse_mounting_dir_3
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[client4], mounting_dir=fuse_mounting_dir_4
        )
