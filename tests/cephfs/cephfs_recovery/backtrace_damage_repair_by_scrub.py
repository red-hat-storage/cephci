import json
import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

"""
Testing description:

Reproducing the damage in the cluster by moving the file to
the upper directory and setting the parent xattr for the object.

Steps to Reproduce:

1.Mount a client
2.In the client do “mkdir -p dir1/dir2”
3.touch dir1/dir2/file
4.ceph tell mds.[active mds name] flush journal
5.Get inode number for the “file”
6.Convert the inode number to hex
7.Get parent xattr for the object and save it ->
 rados --pool [pool name] getxattr [object name] parent > output
8.Move the file to the upper directory
9.Flush journal again
10.Set parent xattr for the object with 
11.Run scrub with “recursive,force” option
12.Check if the damage is reproduced.
"""


def run(ceph_cluster, **kw):
    try:

        log.info(f"Running CephFS tests for ceph tracker CEPH-83593391")
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
        # In the client do “mkdir -p dir1/dir2”
        client1.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_1}/dir1/dir2")
        # touch dir1/dir2/file
        client1.exec_command(
            sudo=True, cmd=f"touch {fuse_mounting_dir_1}/dir1/dir2/file"
        )
        # get active mds name
        active_mds = fs_util.get_active_mdss(client1, fs_name="cephfs")
        # ceph tell mds.[active mds name] flush journal
        log.info(f"ceph tell mds.{active_mds[0]} flush journal")
        client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{active_mds[0]} flush journal"
        )
        # Get inode number for the “file”
        inode_num, _ = client1.exec_command(
            sudo=True, cmd=f"stat -c %i {fuse_mounting_dir_1}/dir1/dir2/file"
        )
        inode_num_hex = str(hex(int(inode_num)))[2:]
        # get parent xattr for the object and save it
        rados_cmd1 = f"rados --pool cephfs.cephfs.data getxattr {inode_num_hex}.00000000 parent > output"
        log.info(rados_cmd1)
        client1.exec_command(sudo=True, cmd=rados_cmd1)
        # Move the file to the upper directory
        client1.exec_command(
            sudo=True,
            cmd=f"mv {fuse_mounting_dir_1}/dir1/dir2/file {fuse_mounting_dir_1}/dir1/file",
        )
        # Flush journal again
        log.info(f"ceph tell mds.{active_mds[0]} flush journal")
        client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{active_mds[0]} flush journal"
        )
        # Set parent xattr for the object with
        rados_cmd2 = f"rados --pool cephfs.cephfs.data setxattr {inode_num_hex}.00000000 parent - < output"
        log.info(rados_cmd2)
        client1.exec_command(sudo=True, cmd=rados_cmd2)
        # flush journal again
        log.info(f"ceph tell mds.{active_mds[0]} flush journal")
        client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{active_mds[0]} flush journal"
        )
        # Run scrub with “recursive,force” option
        log.info(f"Run scrub with recursive,force option")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph tell mds.{active_mds[0]} scrub start / recursive,force",
        )
        # Check if the damage is reproduced.
        log.info(f"Check if the damage is reproduced")
        damage_out, _ = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{active_mds[0]} damage ls"
        )
        damage_info = json.loads(damage_out)
        damage_id = []
        for damage in damage_info:
            log.info(damage)
            damage_id.append(damage["id"])
        log.info(damage_id)
        if len(damage_id) == 0:
            log.error("Damage is not reproduced")
            return 1
        # do scrub with repair option
        log.info(f"Run scrub with repair option")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph tell mds.{active_mds[0]} scrub start / recursive,force,repair",
        )
        # remove the damage
        log.info(f"Remove the damage")
        log.info(damage_info)
        log.info(damage_info[0])
        # remove the damage in damage_id
        for damage in damage_id:
            log.info(f"Removing the damage {damage}")
            client1.exec_command(
                sudo=True, cmd=f"ceph tell mds.{active_mds[0]} damage rm {damage}"
            )
        damage_out2, _ = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{active_mds[0]} damage ls"
        )
        damage_info2 = json.loads(damage_out2)
        damage_id2 = []
        for damage in damage_info2:
            log.info(damage)
            damage_id2.append(damage["id"])
        if len(damage_id2) == 0:
            log.info("Damage is removed")
            return 0
        else:
            log.error("Damage is not removed")
            return 1
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