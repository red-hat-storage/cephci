import random
import string
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

"""
Testing description:

Verify cephfs-top package installation and its functionality(metrics)

Steps to Reproduce:
1. Try to test install cephfs-top package
2. Enable stats in mgr module
3. Create cephfs-top client user and verify
4. Mount CephFS using ceph-fuse and kernel
5. Get the cephfs top dump
6. Open files and verify the count
7. Close files and verify the count
8. Verify iocaps
9. Verify client counts
10. Verify total read and write IO
10. Create multiple volumes and verify it.
"""


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83573848"
        log.info(f"Running CephFS tests for ceph {tc}")
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
        log.info("Install cephfs-top by dnf install cephfs-top")
        client1.exec_command(
            sudo=True,
            cmd="dnf install cephfs-top -y",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="dnf list cephfs-top",
        )
        if "cephfs-top" not in out:
            log.error("cephfs-top package could not be installed")
            return 1

        log.info("Enable stats in mgr module")
        client1.exec_command(
            sudo=True,
            cmd="ceph mgr module enable stats",
        )
        log.info("Create cephfs-top client user and verify")
        client_user = "client.fstop"
        cmd = f"ceph auth get-or-create {client_user} mon 'allow r' mds 'allow r' osd 'allow r' mgr 'allow r'"
        cmd += " > /etc/ceph/ceph.client.fstop.keyring"
        client1.exec_command(
            sudo=True,
            cmd=cmd,
        )
        out, _ = client1.exec_command(
            sudo=True,
            cmd="ceph auth ls | grep fstop",
        )
        if client_user not in out:
            log.error(f"{client_user} user not created")
            return 1
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
        output = fs_util.get_cephfs_top_dump(client1)
        log.info(f"Output of cephfs top dump: {output}")
        # get client ID using mounted directory
        client_id = fs_util.get_client_id(client1, "cephfs", fuse_mounting_dir_1)
        # open files
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        file_list = []
        opened_files = 10
        for i in range(opened_files):
            file_name = f"file_{rand}_{i}"
            file_list.append(file_name)
        pids = fs_util.open_files(client1, fuse_mounting_dir_1, file_list)
        time.sleep(60)
        out_open = fs_util.get_cephfs_top_dump(client1)["filesystems"]["cephfs"][
            client_id
        ]["ofiles"]
        log.info(f"Output of cephfs top dump after opening files: {out_open}")
        if int(out_open) != opened_files:
            log.error(f"Open files did not match with {opened_files}")
            return 1
        else:
            log.info("Open files count matched")
        fs_util.close_files(client1, pids)
        time.sleep(60)
        out_closed = fs_util.get_cephfs_top_dump(client1)["filesystems"]["cephfs"][
            client_id
        ]["ofiles"]
        if int(out_closed) != 0:
            log.error("Open files count mismatch with 0")
            return 1
        else:
            log.info("Open files count matched")
        log.info("verifying iocaps")
        num_files = 10
        iocaps_value_before = fs_util.get_cephfs_top_dump(client1)["filesystems"][
            "cephfs"
        ][client_id]["oicaps"]
        for i in range(num_files):
            file_name = f"files_{rand}_{i}"
            client1.exec_command(
                sudo=True, cmd=f"touch {fuse_mounting_dir_1}/{file_name}"
            )
        time.sleep(60)
        iocaps_value_after = fs_util.get_cephfs_top_dump(client1)["filesystems"][
            "cephfs"
        ][client_id]["oicaps"]
        if int(iocaps_value_after) != int(iocaps_value_before) + num_files:
            log.error("IO Caps value mismatch")
            return 1
        else:
            log.info("IO Caps value matched")
        log.info("Decrease the iocaps value")
        for i in range(num_files):
            file_name = f"files_{rand}_{i}"
            client1.exec_command(
                sudo=True, cmd=f"rm -f {fuse_mounting_dir_1}/{file_name}"
            )
        time.sleep(60)
        iocaps_value_after = fs_util.get_cephfs_top_dump(client1)["filesystems"][
            "cephfs"
        ][client_id]["oicaps"]
        if int(iocaps_value_after) != int(iocaps_value_before):
            log.error("IO Caps value mismatch")
            return 1
        else:
            log.info("IO Caps value matched")
        log.info("Verifying client conuts")
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse_{rand}_2"
        fuse_mounting_dir_3 = f"/mnt/cephfs_fuse_{rand}_3"
        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel_{rand}_2"
        current_fuse_count = fs_util.get_cephfs_top_dump(client1)["client_count"][
            "fuse"
        ]
        current_kernel_count = fs_util.get_cephfs_top_dump(client1)["client_count"][
            "kclient"
        ]
        current_total_count = fs_util.get_cephfs_top_dump(client1)["client_count"][
            "total_clients"
        ]
        log.info("mounting clients")
        fs_util.fuse_mount([client1], fuse_mounting_dir_2)
        fs_util.fuse_mount([client1], fuse_mounting_dir_3)
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount([client1], kernel_mounting_dir_2, ",".join(mon_node_ips))
        time.sleep(120)
        new_fuse_count = fs_util.get_cephfs_top_dump(client1)["client_count"]["fuse"]
        new_kernel_count = fs_util.get_cephfs_top_dump(client1)["client_count"][
            "kclient"
        ]
        new_total_count = fs_util.get_cephfs_top_dump(client1)["client_count"][
            "total_clients"
        ]
        if new_fuse_count != current_fuse_count + 2:
            log.error(
                f"Current fuse count: {current_kernel_count} New fuse count: {new_kernel_count}"
            )
            log.error("Fuse client count mismatch")
            return 1
        else:
            log.info("Fuse client count matched")
        if new_kernel_count != current_kernel_count + 1:
            log.error(
                f"Current kernel count: {current_kernel_count} New kernel count: {new_kernel_count}"
            )
            log.error("Kernel client count mismatch")
            return 1
        else:
            log.info("Kernel client count matched")
        if new_total_count != current_total_count + 3:
            log.error(
                f"Current total count: {current_total_count} New total count: {new_total_count}"
            )
            log.error("Total client count mismatch")
            return 1
        else:
            log.info("Total client count matched")

        log.info("Create multiple filesystems and verify client metrics")
        cephfs_name1 = f"cephfs_top_name_{rand}_1"
        cephfs_name2 = f"cephfs_top_name_{rand}_2"
        client1.exec_command(sudo=True, cmd=f"ceph fs volume create {cephfs_name1}")
        client1.exec_command(sudo=True, cmd=f"ceph fs volume create {cephfs_name2}")
        fs_name_dump = fs_util.get_cephfs_top_dump(client1)["filesystems"]
        if cephfs_name1 not in fs_name_dump:
            log.error(f"{cephfs_name1} not found in filesystems dump")
            return 1
        else:
            log.info(f"{cephfs_name1} found in filesystems dump")
        if cephfs_name2 not in fs_name_dump:
            log.error(f"{cephfs_name2} not found in filesystems dump")
            return 1
        else:
            log.info(f"{cephfs_name2} found in filesystems dump")
        log.info("create increase read and write IO")
        log.info("create a file 4GB file with dd")
        file_name = f"file_{rand}"
        before_total_read = fs_util.get_cephfs_top_dump(client1)["filesystems"][
            "cephfs"
        ][client_id]["rtio"]
        before_total_write = fs_util.get_cephfs_top_dump(client1)["filesystems"][
            "cephfs"
        ][client_id]["wtio"]
        client1.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={fuse_mounting_dir_1}/{file_name} bs=1M count=4096",
        )
        log.info("copy the file to other directories")
        client1.exec_command(
            sudo=True,
            cmd=f"cp {fuse_mounting_dir_1}/{file_name} {fuse_mounting_dir_1}/{file_name}_copy",
        )
        after_total_read = fs_util.get_cephfs_top_dump(client1)["filesystems"][
            "cephfs"
        ][client_id]["rtio"]
        after_total_write = fs_util.get_cephfs_top_dump(client1)["filesystems"][
            "cephfs"
        ][client_id]["wtio"]
        if int(after_total_read) < int(before_total_read):
            log.error("Total read IO mismatch")
            return 1
        else:
            log.info("Total read IO matched")
        if int(after_total_write) < int(before_total_write):
            log.error("Total write IO mismatch")
            return 1
        else:
            log.info("Total write IO matched")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_2
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_3
        )
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rm {cephfs_name1} --yes-i-really-mean-it",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rm {cephfs_name2} --yes-i-really-mean-it",
        )
