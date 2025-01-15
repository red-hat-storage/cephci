import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Scenarios for Volume Info Validation with Data Write:

    1. Verify Volume Info Details Without Any Data
    2. Create Subvolumes and Validate Usage Details
    3. Mount Subvolumes, Write Data, and Validate Usage
    4. Create Subvolume Group and Subvolumes, Validate Usage Details
    5. Delete Subvolumes and Validate Usage During Deletion
    """
    try:
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        if erasure:
            log.info("volume is created with erasure pools")
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]

        # Ensure filesystem exists

        fs_util.auth_list([client1])
        fs_util.prepare_clients(clients, build)

        random_suffix = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )

        volume_name = f"vol_info_{random_suffix}"
        subvolume_name = f"subvol_info_01_{random_suffix}"
        subvolume_group_name = f"subvol_group_01_{random_suffix}"
        fs_util.create_fs(client1, volume_name)
        log.info("Step 1: Verify Volume Info Details Without Any Data")
        fs_volume_info_dict = fs_util.collect_fs_volume_info_for_validation(
            client1, volume_name, human_readable=True
        )
        assert (
            fs_volume_info_dict["data_used"].strip() == "0"
        ), f"Data pool used size should be 0, found {fs_volume_info_dict['data_used']}"
        assert (
            fs_volume_info_dict["meta_used"].strip() >= "0"
        ), f"Metadata pool used size unexpected: {fs_volume_info_dict['meta_used']}"

        log.info("Verified: Data and metadata pools show no significant usage.")

        log.info("Step 2: Create Subvolumes and Validate Usage Details")
        fs_util.create_subvolume(client1, volume_name, subvolume_name)

        fs_volume_info_dict = fs_util.collect_fs_volume_info_for_validation(
            client1, volume_name, human_readable=True
        )
        assert (
            "M" not in fs_volume_info_dict["data_used"]
        ), "Data pool used size should not exceed K"
        log.info("Verified: Used size did not exceed K after subvolume creation.")

        log.info("Step 3: Mount Subvolumes, Write Data, and Validate Usage")

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f"--client_fs {volume_name}",
        )

        client1.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={fuse_mounting_dir_1}/testfile bs=1M count=1024",
            long_running=True,
        )
        fs_volume_info_dict = fs_util.collect_fs_volume_info_for_validation(
            client1, volume_name, human_readable=True
        )

        assert (
            fs_volume_info_dict["data_used"].strip() != "0"
        ), "Used size should reflect data written"
        log.info(
            f"Verified: Data write reflected in used size. {fs_volume_info_dict['data_used'].strip()}"
        )

        log.info("Step 4: Create Subvolume Group and Add Subvolumes")
        fs_util.create_subvolumegroup(client1, volume_name, subvolume_group_name)
        fs_util.create_subvolume(
            client1, volume_name, subvolume_name, group_name=subvolume_group_name
        )

        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        subvol_path, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {volume_name} {subvolume_name} {subvolume_group_name}",
        )
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
            extra_params=f",fs={volume_name}",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={kernel_mounting_dir_1}/testfile bs=1M count=1024",
            long_running=True,
        )
        volume_info = fs_util.collect_fs_volume_info_for_validation(
            client1, volume_name, human_readable=True
        )
        data_pool_used_kernel = volume_info["data_used"].strip()
        log.info(volume_info)
        assert (
            data_pool_used_kernel > fs_volume_info_dict["data_used"].strip()
        ), f"Used size should reflect data written {fs_volume_info_dict['data_used'].strip()}"

        log.info("Step 5: Delete Subvolumes and Validate Usage During Deletion")
        fs_util.remove_subvolume(client1, volume_name, subvolume_name, validate=False)
        fs_util.remove_subvolume(
            client1,
            volume_name,
            subvolume_name,
            group_name=subvolume_group_name,
            validate=False,
        )
        volume_info = fs_util.get_fs_info_dump(
            client1, volume_name, human_readable=True
        )
        log.info(volume_info)
        pending_subvolume_deletions = volume_info["pending_subvolume_deletions"]
        assert_size(fs_util, client1, volume_name)
        log.info(f"Pending subvolume deletions : {pending_subvolume_deletions}")
        log.info("Verified: Subvolume deletion reflected in volume info.")

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("cleanup")
        fs_util.remove_fs(client1, volume_name)


@retry(CommandFailed, tries=3, delay=30)
def assert_size(fs_util, client1, volume_name):
    volume_info = fs_util.get_fs_info_dump(client1, volume_name, human_readable=True)
    log.info(volume_info)
    used_size = volume_info["used_size"].strip()
    if used_size != "0":
        raise CommandFailed("Data pool used size should not exceed K")
