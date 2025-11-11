import random
import re
import string
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)

"""
Testing description:

Basic functional test for cephfs-journal-tool event command

Test steps:
1. mount cephfs using ceph-fuse and kernel
2. store the journal address and inodes in a list
3. run "cephfs-journal-tool --rank cephfs:0 event get" with options
4. run "cephfs-journal-tool --rank cephfs:0 event get --path" with options
5. run "cephfs-journal-tool --rank cephfs:0 event get --inode" with options
6. run "cephfs-journal-tool --rank cephfs:0 event get --type" with options
7. run "cephfs-journal-tool --rank cephfs:0 event splice" with options
8. Apply function as a problem for now
"""


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83595482"
        log.info(f"Running CephFS tests for ceph - {tc}")
        # Initialize the utility class for CephFS
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        # Get the client nodes
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        # Authenticate the clients
        fs_util.auth_list(clients)
        build = config.get("build", config.get("rhbuild"))
        # Prepare the clients
        fs_util.prepare_clients(clients, build)
        client1 = clients[0]
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name, placement="label:mds")
            fs_util.wait_for_mds_process(clients[0], process_name=fs_name)
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        # Define mount directories
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{rand}"
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel_{rand}"
        # Mount CephFS using ceph-fuse and kernel
        fs_util.fuse_mount(
            [client1], fuse_mounting_dir_1, extra_params=f" --client_fs {fs_name}"
        )
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={fs_name}",
        )

        log.info("Creating sample files to populate inode-based journal entries...")
        test_dir = f"{fuse_mounting_dir_1}/journal_test"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {test_dir}")
        client1.exec_command(sudo=True, cmd=f"touch {test_dir}/file1 {test_dir}/file2")
        client1.exec_command(
            sudo=True, cmd=f"echo 'cephfs test data' > {test_dir}/file1"
        )
        client1.exec_command(sudo=True, cmd=f"mv {test_dir}/file1 {test_dir}/file3")
        client1.exec_command(sudo=True, cmd=f"rm -f {test_dir}/file2")
        log.info("Sample file operations completed.")

        # down all the osd nodes
        mdss = ceph_cluster.get_ceph_objects("mds")
        mds_fs = fs_util.get_fs_status_dump(client1, vol_name=fs_name)
        mds_nodes_from_map = [
            ".".join(entry["name"].split(".")[1:-1])
            for entry in mds_fs.get("mdsmap", [])
        ]
        log.info("MDS nodes (from mdsmap): " + str(mds_nodes_from_map))

        mds_nodes_names = []
        for mds in mdss:
            if mds.node.hostname in mds_nodes_from_map:
                out, ec = mds.exec_command(
                    sudo=True,
                    cmd="systemctl list-units | grep -o 'ceph-.*mds.*\\.service'",
                )
                mds_nodes_names.append((mds, out.strip()))
        log.info(f"NODES_MDS_info :{mds_nodes_names}")
        for mds in mds_nodes_names:
            mds[0].exec_command(sudo=True, cmd=f"systemctl stop {mds[1]}")
        time.sleep(10)
        log.info(mds_nodes_names)
        log.info("All the mds nodes are down")
        health_output = client1.exec_command(sudo=True, cmd="ceph -s")
        log.info(health_output[0])
        if "offline" not in health_output[0]:
            log.error("Ceph cluster is not in offline state after stopping MDS nodes")
            return 1

        log.info("Test 1: Testing Event Get")
        client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 event get list > /list_input.txt",
        )
        client1.exec_command(sudo=True, cmd="cat /list_input.txt")
        client1.exec_command(
            sudo=True,
            cmd="sed ':a;N;$!ba;s/\\n[[:space:]]\\+/ /g' /list_input.txt > /list_output.txt",
        )
        list_out, _ = client1.exec_command(sudo=True, cmd="cat /list_output.txt")
        list_out = list_out.split("\n")
        log.debug(list_out)
        result = []
        for line in list_out:
            log.info(line)
            parts = line.split()
            if len(parts) > 4:
                log.info(parts)
                journal_address = parts[1]
                inodes = parts[4]
                event_type = parts[2].rstrip(":")
                result.append((journal_address, inodes, event_type))
            elif len(line) > 1 and len(line) < 5:
                result.append((parts[1], ""))
            else:
                continue

        log.info("Test 2: Testing Event Get with JSON options")
        out1, ec1 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 event get json --path output.json",
            timeout=600,
        )
        if "Wrote" not in ec1:
            log.error(out1)
            log.error(ec1)
            log.error("json failed")
            return 1

        log.info("Test 3: Testing Event Get with Binary options")
        out2, ec2 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 event get binary --path output.bin",
        )
        if "Wrote" not in ec2:
            log.error("binary failed")
            return 1

        log.info("Test 4: Testing Event Get with Summary")
        out3, ec3 = client1.exec_command(
            sudo=True, cmd=f"cephfs-journal-tool --rank {fs_name}:0 event get summary"
        )
        if "Events" not in out3:
            log.error(out3)
            log.error(ec3)
            log.error("summary failed")
            return 1

        log.info("Test 5: Testing Event Get with Path options")
        valid_paths = [entry for entry in result if "/" in entry[1]]
        log.info("Valid paths: " + str(valid_paths))
        if valid_paths:
            random_index = random.randint(0, len(valid_paths) - 1)
            rand_addr = valid_paths[random_index][0]
            rand_inode = valid_paths[random_index][1]
            log.info("Random address: " + rand_addr)
            log.info("Random inode: " + rand_inode)
            for option in ["list", "summary"]:
                out4, ec4 = client1.exec_command(
                    sudo=True,
                    cmd=f"cephfs-journal-tool --rank {fs_name}:0 event get --path {rand_inode} {option}",
                )
                if option == "list":
                    if rand_inode not in out4:
                        log.error(out4)
                        log.error(ec4)
                        log.error(f"--path {option} failed")
                        return 1
                else:
                    if "Errors" not in out4:
                        log.error(out4)
                        log.error(ec4)
                        log.error(f"--path {option} failed")
                        return 1

        log.info("Test 6: Testing Event Get with Inode options")
        valid_hex_paths = []
        for addr, inode_str, _ in result:
            candidate = inode_str.split("/")[-1]
            if re.fullmatch(r"[0-9a-fA-F]+", candidate):  # hex validation
                valid_hex_paths.append((addr, candidate))

        log.info("Valid hex inodes: " + str(valid_hex_paths))
        if valid_hex_paths:
            for option in ["list", "summary"]:
                random_index = random.randint(0, len(valid_hex_paths) - 1)
                rand_addr = valid_hex_paths[random_index][0]
                rand_inode = valid_hex_paths[random_index][1]
                log.info("Random address: " + rand_addr)
                log.info("Random inode: " + rand_inode)

                log.info("Inode test with option: " + option)
                inode_to_dec = int(rand_inode, 16)
                out5, ec5 = client1.exec_command(
                    sudo=True,
                    cmd=f"cephfs-journal-tool --rank {fs_name}:0 event get --inode {inode_to_dec} {option}",
                )
                if option == "list":
                    if rand_inode not in out5:
                        log.error(out5)
                        log.error(ec5)
                        log.error(f"--inode {option} failed")
                        return 1
                else:
                    if "Errors" not in out5:
                        log.error(out5)
                        log.error(ec5)
                        log.error(f"--inode {option} failed")
                        return 1

        log.info("Test 7: Testing Event Get with Type options")
        type_list = ["SESSION", "UPDATE", "OPEN", "SUBTREEMAP", "NOOP", "SESSIONS"]
        for type in type_list:
            log.info("Testing for type : " + type)
            out6, ec6 = client1.exec_command(
                sudo=True,
                cmd=f"cephfs-journal-tool --rank {fs_name}:0 event get --type {type} list",
            )
            log.info("Output for type " + type + " : " + out6)
            if ec6:
                log.error(f"--type {type} list failed")
                return 1

        log.info("Test 8: Testing Event Splice with valid hex inode paths")
        valid_hex_splice_paths = [
            (addr, path, event)
            for addr, path, event in result
            if event == "UPDATE" and "/" in path and not path.startswith("stray")
        ]

        if valid_hex_splice_paths:
            if len(valid_hex_splice_paths) < 2:
                log.warning("Not enough valid hex paths to perform splice.. Skipping")
            else:
                for option in ["list", "summary"]:
                    log.info("Splice test with option: " + option)
                    start_index = random.randrange(len(valid_hex_splice_paths) - 2)
                    end_index = min(
                        start_index + random.randint(1, 4),
                        len(valid_hex_splice_paths) - 1,
                    )
                    start_addr = valid_hex_splice_paths[start_index][0]
                    end_addr = valid_hex_splice_paths[end_index][0]

                    out8, ec8 = client1.exec_command(
                        sudo=True,
                        cmd=f"cephfs-journal-tool --rank {fs_name}:0 event splice"
                        f" --range {start_addr}..{end_addr} {option}",
                    )
                    log.info("Splice Output with option " + option + " : " + out8)
                    if ec8:
                        log.error(f"splice with {option} failed")
                        return 1

        log.info("All tests passed successfully.")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        # start mds
        for mds in mds_nodes_names:
            mds_node = mds[0]
            mds_name = mds[1]
            # reset failed counter
            mds_node.exec_command(sudo=True, cmd=f"systemctl reset-failed {mds_name}")
            mds_node.exec_command(sudo=True, cmd=f"systemctl restart {mds_name}")

        client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank {fs_name}:0 journal reset --yes-i-really-really-mean-it",
            check_ec=False,
        )
        cephfs_common_utils.wait_for_healthy_ceph(client1, 300)
        # Cleanup
        fs_util.client_clean_up(
            "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[client1], mounting_dir=kernel_mounting_dir_1
        )
