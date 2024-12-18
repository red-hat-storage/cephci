import random
import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CephFS Subvolume Test

    This test suite validates the functionality of the `ceph fs subvolume` command,
    covering various scenarios including:

    * Idempotence with and without parameters (modes, uid, gid)
    * Creating subvolumes with isolated namespace
    * Failure handling and cleanup of incomplete subvolumes
    * Creating subvolumes with desired uid, gid, modes, with and without groups
    * Creating subvolumes with invalid size (negative and 0)
    * Creating subvolumes with --group_name=_nogroup
    * Setting and getting earmarks during subvolume creation and on existing subvolumes
    * Setting earmarks with group_name
    * Setting and removing earmarks while IO is in progress
    """
    try:
        tc = "CEPH-83604184"
        log.info(f"Running CephFS tests for - {tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        fs_util.auth_list(clients)
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        client1 = clients[0]
        vol_name = f"cephfs_subvolume_test_{random.randint(0, 1000)}"
        fs_util.create_fs(client1, vol_name)
        random_str = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5))
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{random_str}"
        fs_util.fuse_mount(
            [client1], fuse_mounting_dir_1, extra_params=f" --client_fs {vol_name}"
        )
        subvol_name = f"subvol1_{random_str}"
        # Idempotence default - same command 2-3 times
        for _ in range(3):
            command = f"ceph fs subvolume create {vol_name} {subvol_name}"
            log.info(f"Executing command: {command}")
            out, err = client1.exec_command(sudo=True, cmd=command)
            if "Error" in err:
                log.error(f"Subvolume creation failed: {err}")
                return 1
        log.info("Subvolume creation idempotence (default) successful")
        # Idempotence with modes, uid, gid - multiple retries
        subvol_name = f"subvol2_{random_str}"
        for _ in range(3):
            command = f"ceph fs subvolume create {vol_name} {subvol_name} --mode 755 --uid 1000 --gid 1000"
            log.info(f"Executing command: {command}")
            out, err = client1.exec_command(sudo=True, cmd=command)
            if "Error" in err:
                log.error(f"Subvolume creation failed: {err}")
                return 1
        log.info("Subvolume creation idempotence (with modes, uid, gid) successful")
        # With isolated_namespace
        subvol_name = f"subvol3_{random_str}"
        command = (
            f"ceph fs subvolume create {vol_name} {subvol_name} --namespace-isolated"
        )
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolume creation with isolated_namespace failed: {err}")
            return 1
        log.info("Subvolume creation with isolated_namespace successful")
        # Failure in creation should cleanup incomplete subvolumes
        subvol_name = f"subvol4_{random_str}"
        command = f"ceph fs subvolume create {vol_name} {subvol_name} --invalid_option"  # Simulate failure
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if "Error" not in err:
            log.error(f"Subvolume creation should have failed: {out}")
            return 1
        command = f"ceph fs subvolume ls {vol_name}"
        out, err = client1.exec_command(sudo=True, cmd=command)
        if subvol_name in out:
            log.error(f"Incomplete subvolume {subvol_name} was not cleaned up")
            return 1
        log.info("Subvolume creation failure and cleanup successful")
        # Desired uid, gid, modes, with and without groups
        subvol_name = f"subvol5_{random_str}"
        command = f"ceph fs subvolume create {vol_name} {subvol_name} --mode 755 --uid 1000 --gid 1000"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolume creation with uid, gid, modes failed: {err}")
            return 1
        subvol_name = f"subvol6_{random_str}"
        subvol_group_name = f"mygroup_{random_str}"
        fs_util.create_subvolumegroup(client1, vol_name, subvol_group_name)
        command = (
            f"ceph fs subvolume create {vol_name} {subvol_name}"
            f" --mode 700 --uid 1001 --gid 1001 --group_name {subvol_group_name}"
        )
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(
                f"Subvolume creation with uid, gid, modes, and group failed: {err}"
            )
            return 1
        log.info(
            "Subvolume creation with desired uid, gid, modes, and group successful"
        )
        # Invalid Size [Negative, 0]
        subvol_name = f"subvol7_{random_str}"
        command = f"ceph fs subvolume create {vol_name} {subvol_name} --size -1"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if "Error EINVAL" not in err:
            log.error(f"Subvolume creation with invalid size should have failed: {out}")
            return 1
        subvol_name = f"subvol8_{random_str}"
        command = f"ceph fs subvolume create {vol_name} {subvol_name} --size 0"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if "Error EINVAL" in err:
            log.error(
                f"Subvolume creation with invalid size [0] should have failed: {out}"
            )
            return 1
        log.info("Subvolume creation with invalid size failed as expected")
        # With --group_name=_nogroup
        subvol_name = f"subvol9_{random_str}"
        command = (
            f"ceph fs subvolume create {vol_name} {subvol_name} --group_name=_nogroup"
        )
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if "Error" not in err:
            log.error(
                f"Subvolume creation with --group_name=_nogroup should have failed: {err}"
            )
            return 1
        log.info(
            "Subvolume creation --group_name=_nogroup(reserved keyword) failed as expected"
        )
        # ceph version cheeck, earmark is only valid from squid
        version, _ = client1.exec_command(sudo=True, cmd="ceph version")
        if "squid" in version:
            # set earmark [tag] while subvolume creation
            subvol_name = f"subvol10_{random_str}"
            fs_util.create_subvolume(
                client1, vol_name, subvol_name, earmark="nfs.share1"
            )
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, vol_name, subvol_name)
            if "nfs.share1" not in out:
                log.error(f"Subvolume get earmark failed: {err}")
                return 1
            # https://bugzilla.redhat.com/show_bug.cgi?id=2332723
            # create subvolume with earmark with group_name
            # subvol_name = f"subvol11_{random_str}"
            # fs_util.create_subvolume(client1, vol_name, subvol_name,
            # group_name=subvol_group_name, earmark="smb.share2")
            # # Get earmark
            # out,err=fs_util.get_subvolume_earmark(client1, vol_name, subvol_name)
            # if "smb.share2" not in out:
            #     log.error(f"Subvolume get earmark failed: {err}")
            #     return 1
            # Create a subvolume and set earmark
            subvol_name = f"subvol12_{random_str}"
            fs_util.create_subvolume(client1, vol_name, subvol_name)
            # Set earmark
            fs_util.set_subvolume_earmark(
                client1, vol_name, subvol_name, earmark="nfs.share3"
            )
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, vol_name, subvol_name)
            if "nfs.share3" not in out:
                log.error(f"Subvolume get earmark failed: {err}")
                return 1
            # create a subvolume and set earmark with invalid name (other than nfs/smb)
            subvol_name = f"subvol13_{random_str}"
            fs_util.create_subvolume(client1, vol_name, subvol_name)
            # Set earmark
            command = f"ceph fs subvolume earmark set {vol_name} {subvol_name} --earmark invalid.share"
            log.info(f"Executing command: {command}")
            out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
            if "Invalid earmark" not in err:
                log.error(
                    f"Subvolume set earmark with invalid name should have failed: {out}"
                )
                return 1
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, vol_name, subvol_name)
            if "invalid.share" in out:
                log.error(f"Invalid earmark should have failed: {out}")
                return 1
            # create a subvolume and set earmark and remove the earmark
            subvol_name = f"subvol14_{random_str}"
            fs_util.create_subvolume(client1, vol_name, subvol_name)
            # Set earmark
            fs_util.set_subvolume_earmark(
                client1, vol_name, subvol_name, earmark="nfs.share4"
            )
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, vol_name, subvol_name)
            if "nfs.share4" not in out:
                log.error(f"Subvolume get earmark failed: {err}")
                return 1
            # Remove earmark
            fs_util.remove_subvolume_earmark(client1, vol_name, subvol_name)
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, vol_name, subvol_name)
            if "nfs.share4" in out:
                log.error(f"Subvolume get earmark failed: {err}")
                return 1
            # Set earmark and rm earmark while IO is in progress
            subvol_name = f"subvol15_{random_str}"
            fs_util.create_subvolume(client1, vol_name, subvol_name)
            # run IOs in the subvolume
            sub_path, _ = client1.exec_command(
                sudo=True, cmd=f"ceph fs subvolume getpath {vol_name} {subvol_name}"
            )
            with parallel() as p:
                p.spawn(
                    fs_util.run_ios(
                        client1, f"{fuse_mounting_dir_1}{sub_path}", ["dd", "smallfile"]
                    )
                )
            # Set earmark
            fs_util.set_subvolume_earmark(
                client1, vol_name, subvol_name, earmark="nfs.share5"
            )
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, vol_name, subvol_name)
            log.info(f"Get earmark output: {out}")
            if "nfs.share5" not in out:
                log.error(f"Subvolume get earmark failed: {err}")
                return 1
            # Remove earmark
            fs_util.remove_subvolume_earmark(client1, vol_name, subvol_name)
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, vol_name, subvol_name)
            if "nfs.share5" in out:
                log.error(f"Subvolume get earmark failed: {err}")
                return 1
            log.info("Subvolume earmark operations successful")
            return 0

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Cleaning up")
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        # Clean up the subvolumes and volume
        for i in range(1, 15):
            subvol_name = f"subvol{i}_{random_str}"
            client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume rm {vol_name}_{random_str} {subvol_name} --force || true",
            )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rm {vol_name} --yes-i-really-mean-it || true",
        )
        # remove the group
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {vol_name}_{random_str} {subvol_group_name} --force || true",
        )
