import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
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
        log.info("Testing Event Get")
        client1.exec_command(
            sudo=True,
            cmd="cephfs-journal-tool --rank cephfs:0 event get list > /list_input.txt",
        )
        client1.exec_command(
            sudo=True,
            cmd="sed ':a;N;$!ba;s/)\\n/)/g' /list_input.txt > /list_output.txt",
        )
        list_out, _ = client1.exec_command(sudo=True, cmd="cat /list_output.txt")
        list_out = list_out.split("\n")
        result = []
        for line in list_out:
            log.info(line)
            parts = line.split()
            if len(parts) > 4:
                log.info(parts)
                journal_address = parts[1]
                inodes = parts[4]
                result.append((journal_address, inodes))
            elif len(line) > 1 and len(line) < 5:
                result.append((parts[1], ""))
            else:
                continue
        out1, ec1 = client1.exec_command(
            sudo=True,
            cmd="cephfs-journal-tool --rank cephfs:0 event get json --path output.json",
        )
        if "Wrote" not in ec1:
            log.error(out1)
            log.error(ec1)
            log.error("json failed")
            return 1
        out2, ec2 = client1.exec_command(
            sudo=True,
            cmd="cephfs-journal-tool --rank cephfs:0 event get binary --path output.bin",
        )
        if "Wrote" not in ec2:
            log.error("binary failed")
            return 1
        out3, ec3 = client1.exec_command(
            sudo=True, cmd="cephfs-journal-tool --rank cephfs:0 event get summary"
        )
        if "Events" not in out3:
            log.error(out3)
            log.error(ec3)
            log.error("summary failed")
            return 1
        random_index = random.randint(0, len(result))
        rand_addr = result[random_index][0]
        rand_inode = result[random_index][1]
        log.info("Random address: " + rand_addr)
        log.info("Random inode: " + rand_inode)
        for option in ["list", "summary"]:
            out4, ec4 = client1.exec_command(
                sudo=True,
                cmd=f"cephfs-journal-tool --rank cephfs:0 event get --path {rand_inode} {option}",
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
            inode_dec = []
            if "/" in rand_inode:
                inode = rand_inode.split("/")[-1]
                inode_to_dec = int(inode, 16)
                inode_dec.append(inode_to_dec)
            else:
                inode_to_dec = int(rand_inode, 16)
                inode_dec.append(inode_to_dec)
            out5, ec5 = client1.exec_command(
                sudo=True,
                cmd=f"cephfs-journal-tool --rank cephfs:0 event get --inode {inode_dec[0]} {option}",
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
        type_list = ["SESSION", "UPDATE", "OPEN", "SUBTREEMAP", "NOOP", "SESSIONS"]
        for type in type_list:
            out6, ec6 = client1.exec_command(
                sudo=True,
                cmd=f"cephfs-journal-tool --rank cephfs:0 event get --type {type} list",
            )
            if ec6:
                log.error(f"--type {type} list failed")
                return 1
        inode_to_dec = int(rand_inode, 16)
        out7, ec7 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank cephfs:0 event get --inode={str(inode_to_dec)} list",
        )
        if not out7:
            log.error(f"--inode={inode_to_dec} list failed")
            return 1
        log.info("Splice test")
        out8, ec8 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank cephfs:0 event splice"
            f" --range {result[random_index][0]}..{result[random_index+4][0]} summary",
        )
        if not out8:
            log.error("splice with summary failed")
            return 1
        out9, ec9 = client1.exec_command(
            sudo=True,
            cmd=f"cephfs-journal-tool --rank cephfs:0 event splice"
            f" --range {result[random_index][0]}..{result[random_index + 4][0]} list",
        )
        if not out9:
            log.error("splice with list failed")
            return 1

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        # Cleanup
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
