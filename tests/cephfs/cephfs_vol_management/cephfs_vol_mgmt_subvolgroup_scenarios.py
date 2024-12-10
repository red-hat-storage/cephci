import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CephFS Subvolumegroup Test

    This test suite validates the functionality of the `ceph fs subvolumegroup` command,
    covering various scenarios including:

    1. Creating a subvolumegroup with invalid and valid pool layout flags
    2. Creating a subvolumegroup with desired and invalid modes
    3. Creating a subvolumegroup with desired and invalid UID/GID
    4. Creating and resizing a subvolumegroup with specific size
    5. Creating a subvolumegroup with various options (mode, uid, gid, size)

    """
    try:
        tc = "CEPH-83604079"
        log.info(f"Running CephFS tests for - {tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        fs_util.auth_list(clients)
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        client1 = clients[0]
        vol_name = f"cephfs_{random.randint(0, 1000)}"
        fs_util.create_fs(client1, vol_name)
        mount_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse_{mount_dir}/"
        fs_util.fuse_mount(
            [client1], fuse_mounting_dir, extra_params=f" --client_fs {vol_name}"
        )
        random_string = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5))
        # Create a subvolumegroup with invalid pool layout
        group_name = f"invalid_layout_group_{random_string}"
        command = f"ceph fs subvolumegroup create {vol_name} {group_name} --pool_layout invalid"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if "Error EINVAL" not in err:
            log.error(
                f"Subvolumegroup creation with invalid pool layout should have failed: {out}"
            )
            return 1
        log.info("Subvolumegroup creation with invalid pool layout failed as expected")
        # Create a subvolumegroup with valid pool layout
        pool_name = f"cephfs.data.{vol_name}"
        client1.exec_command(
            sudo=True, cmd=f"ceph osd pool create {pool_name}  128 128"
        )
        client1.exec_command(
            sudo=True, cmd=f"ceph fs add_data_pool {vol_name} {pool_name}"
        )
        group_name = f"valid_layout_group_{random_string}"
        command = f"ceph fs subvolumegroup create {vol_name} {group_name} --pool_layout {pool_name}"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error EINVAL" in err:
            log.error(f"Subvolumegroup creation with valid pool layout failed: {out}")
            return 1
        out, err = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolumegroup info {vol_name} {group_name}"
        )
        if "Error" in err:
            log.error(f"Subvolumegroup creation with valid pool layout failed: {err}")
            return 1
        out, err = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolumegroup ls {vol_name}"
        )
        if group_name not in out:
            log.error(f"Subvolumegroup creation with valid pool layout failed: {err}")
            return 1
        log.info("Subvolumegroup creation with valid pool layout successful")
        # Create a subvolumegroup with desired mode
        modes_to_test = [
            "700",  # owner: rwx
            "750",  # owner: rwx, group: r-x
            "755",  # owner: rwx, group: r-x, others: r-x
            "640",  # owner: rw-, group: r--, others: ---
            "644",  # owner: rw-, group: r--, others: r--
            "600",  # owner: rw-, group: ---, others: ---
        ]
        for mode in modes_to_test:
            group_name = f"mode_{mode}_group_{random_string}"
            command = (
                f"ceph fs subvolumegroup create {vol_name} {group_name} --mode {mode}"
            )
            log.info(f"Executing command: {command}")
            out, err = client1.exec_command(sudo=True, cmd=command)
            if "Error" in err:
                log.error(f"Subvolumegroup creation with mode {mode} failed: {err}")
                return 1
            out, err = client1.exec_command(
                sudo=True, cmd=f"ceph fs subvolumegroup getpath {vol_name} {group_name}"
            )
            ls_out, ls_err = client1.exec_command(
                sudo=True, cmd=f"ls -ld {fuse_mounting_dir}/"
            )
            log.info(f"ls_out: {ls_out}")
            stat_out, stat_err = client1.exec_command(
                sudo=True, cmd=f"stat {fuse_mounting_dir}{out}"
            )
            if f"0{mode}" not in stat_out:
                log.error(f"Subvolumegroup creation with mode {mode} failed: {err}")
                return 1
            log.info(f"Subvolumegroup creation with mode {mode} successful")
            client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolumegroup rm {vol_name} {group_name} --force",
            )
        # Create a subvolumegroup with invalid mode
        group_name = f"invalid_mode_group_{random_string}"
        command = f"ceph fs subvolumegroup create {vol_name} {group_name} --mode abcd"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if "Error EINVAL" not in (err or out):
            log.error(
                f"Subvolumegroup creation with invalid mode should have failed: {out}"
            )
            return 1
        log.info("Subvolumegroup creation with invalid mode failed as expected")
        # Create a subvolumegroup with desired UID/GID
        group_name = f"desired_uid_gid_group_{random_string}"
        command = f"ceph fs subvolumegroup create {vol_name} {group_name} --uid 1000 --gid 1000"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolumegroup creation with desired UID/GID failed: {err}")
            return 1
        log.info("Subvolumegroup creation with desired UID/GID successful")

        # Create a subvolumegroup with invalid UID/GID
        group_name = f"invalid_uid_gid_group_{random_string}"
        command = (
            f"ceph fs subvolumegroup create {vol_name} {group_name} --uid -1 --gid abcd"
        )
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if "Error EINVAL" not in err:
            log.error(
                f"Subvolumegroup creation with invalid UID/GID should have failed: {out}"
            )
            return 1
        log.info("Subvolumegroup creation with invalid UID/GID failed as expected")

        # Create a subvolumegroup with specific size
        group_name = f"specific_size_group_{random_string}"
        command = f"ceph fs subvolumegroup create {vol_name} {group_name} --size 10000"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolumegroup creation with specific size failed: {err}")
            return 1
        log.info("Subvolumegroup creation with specific size successful")

        # Resize the subvolumegroup increasing the size
        command = f"ceph fs subvolumegroup resize {vol_name} {group_name} 40000"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolumegroup resize (increasing size) failed: {err}")
            return 1
        log.info("Subvolumegroup resize (increasing size) successful")

        # Resize the subvolumegroup decreasing the size
        command = f"ceph fs subvolumegroup resize {vol_name} {group_name} 10000"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolumegroup resize (decreasing size) failed: {err}")
            return 1
        log.info("Subvolumegroup resize (decreasing size) successful")
        # Create a subvolumegroup with various options
        group_name = f"various_options_group_{random_string}"
        command = (
            f"ceph fs subvolumegroup create {vol_name} {group_name} --mode 750"
            f" --uid 1001 --gid 1001 --size 1000000"
        )
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolumegroup creation with various options failed: {err}")
            return 1
        out, err = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolumegroup getpath {vol_name} {group_name}"
        )
        # verify the result with stat
        stat_out, stat_err = client1.exec_command(
            sudo=True, cmd=f"stat {fuse_mounting_dir}{out}"
        )
        if "1001" not in stat_out:
            log.error(f"Subvolumegroup creation with various options failed: {err}")
            return 1
        log.info("Subvolumegroup creation with various options successful")
        info_out, info_err = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolumegroup info {vol_name} {group_name}"
        )
        if "1000000" not in info_out:
            log.error(f"Subvolumegroup creation with various options failed: {err}")
            return 1
        log.info("Subvolumegroup creation with various options successful")
        if "rwxr-x---" not in stat_out:
            log.error(f"Subvolumegroup creation with various options failed: {err}")
            return 1
        log.info(f"Subvolumegroup options verification successful for {group_name}")

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Cleaning up")
        # unmount the fuse mount
        client1.exec_command(
            sudo=True, cmd=f"umount {fuse_mounting_dir}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {fuse_mounting_dir}", check_ec=False
        )
        # Clean up the subvolumegroups and volume
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {vol_name} "
            f"invalid_layout_group_{random_string} --force || true",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {vol_name} "
            f"valid_layout_group_{random_string} --force || true",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {vol_name} "
            f"desired_mode_group_{random_string} --force || true",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {vol_name} "
            f"invalid_mode_group_{random_string} --force || true",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {vol_name} "
            f"desired_uid_gid_group_{random_string} --force || true",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {vol_name} "
            f"invalid_uid_gid_group_{random_string} --force || true",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {vol_name} "
            f"specific_size_group_{random_string} --force || true",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {vol_name} "
            f"various_options_group_{random_string} --force || true",
        )
        out, err = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolumegroup exist {vol_name}"
        )
        log.info(f"Subvolumegroup cleanup: {out}")
        if "no subvolumegroup" not in out:
            log.error(f"Subvolumegroup cleanup failed: {err}")
            return 1
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rm {vol_name} --yes-i-really-mean-it || true",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph osd pool rm {pool_name} {pool_name} --yes-i-really-mean-it || true",
        )
