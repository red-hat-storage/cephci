import json
import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83575038 - verify CRUD operation on metadata of subvolume's snapshot

    Pre-requisites :
    1. We need atleast one client node to execute this test case
    1. creats fs volume create cephfs if the volume is not there
    2. ceph fs subvolumegroup create <vol_name> <group_name> --pool_layout <data_pool_name>
        Ex : ceph fs subvolumegroup create cephfs subvolgroup_metadata_snapshot_1
    3. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
       Ex: ceph fs subvolume create cephfs subvol_2 --size 5368706371 --group_name subvolgroup_


    Script Flow:
    1. Mount the subvolume on the client using Kernel
    2. Write data into the mount point
    3.verify CRUD operation on metadata of subvolume's snapshot
        Perform Creation of metadata
        ceph fs subvolume snapshot metadata set cephfs subvol_1 "test_meta" fsid --group_name subvolgroup_1
        Listing of metadata
        ceph fs subvolume snapshot metadata ls cephfs subvol_1 --group_name subvolgroup_1
        updatation of metadata
        ceph fs subvolume snapshot metadata set cephfs subvol_1 "test_meta" fsid --group_name subvolgroup_1
        deletion of metadata
        ceph fs subvolume snapshot metadata rm cephfs subvol_1 "test meta" --group_name subvolgroup_1 --force

    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if len(clients) < 1:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        default_fs = "cephfs"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        subvolumegroup_list = [
            {"vol_name": default_fs, "group_name": "subvolgroup_metadata_snapshot_1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": "subvol_metadata_snapshot",
            "group_name": "subvolgroup_metadata_snapshot_1",
            "size": "5368706371",
        }
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_metadata_snapshot subvolgroup_metadata_snapshot_1",
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
        )
        fs_util.create_file_data(
            client1, kernel_mounting_dir_1, 3, "snap1", "snap_1_data "
        )
        snapshot = {
            "vol_name": default_fs,
            "subvol_name": "subvol_metadata_snapshot",
            "snap_name": "snap_1",
            "group_name": "subvolgroup_metadata_snapshot_1",
        }
        fs_util.create_snapshot(client1, **snapshot)
        metadata_dict = {"fsid": "test_metadata"}
        fs_util.set_snapshot_metadata(client1, **snapshot, metadata_dict=metadata_dict)
        metadata_value, rc = fs_util.get_snapshot_metadata(
            client1, **snapshot, metadata_key=list(metadata_dict.keys())[0]
        )
        log.info(f"{metadata_value}")
        if metadata_value.strip() != metadata_dict["fsid"]:
            log.error(
                f"metadata info is not matching Expected metadata value: {metadata_dict['fsid']} "
                f"Actual metadata : {metadata_value}"
            )
            return 1
        metadata, rc = fs_util.list_snapshot_metadata(client1, **snapshot)
        log.info(f"{metadata}")
        log.info(f"{metadata_dict}")
        if json.loads(metadata) != metadata_dict:
            log.error(
                f"metadata info is not matching Expected metadata : {metadata_dict} "
                f"Actual metadata : {json.loads(metadata)}"
            )
            return 1
        metadata_dict_update = {
            "fsid": "test_metadata_update",
            "fsid_2": "add new value",
        }
        for k, v in metadata_dict_update.items():
            fs_util.set_snapshot_metadata(client1, **snapshot, metadata_dict={k: v})
        metadata_update, rc = fs_util.list_snapshot_metadata(client1, **snapshot)
        log.info(f"{metadata_update}")
        if json.loads(metadata_update) != metadata_dict_update:
            log.error(
                f"metadata info is not matching Expected metadata : {metadata_dict_update} "
                f"Actual metadata : {json.loads(metadata_update)}"
            )
            return 1
        metadata_list_delete = ["fsid"]
        fs_util.remove_snapshot_metadata(
            client1, **snapshot, metadata_list=metadata_list_delete
        )
        del metadata_dict_update[metadata_list_delete[0]]
        metadata_del, rc = fs_util.list_snapshot_metadata(client1, **snapshot)
        log.info(f"{metadata_del}")
        if json.loads(metadata_del) != metadata_dict_update:
            log.error(
                f"metadata info is not matching Expected metadata : {metadata_dict_update} "
                f"Actual metadata : {json.loads(metadata_del)}"
            )
            return 1
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Clean Up in progess")
        fs_util.remove_snapshot(client1, **snapshot)
        fs_util.remove_subvolume(client1, **subvolume)
