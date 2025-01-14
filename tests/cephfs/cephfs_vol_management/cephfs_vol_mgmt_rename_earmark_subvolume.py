import random
import time
import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.io.fs_io import fs_io
from utility.log import Log

log = Log(__name__)
"""
ceph fs volume rename <start_volume> <new_start_volume> [--yes-i-really-mean-it]
# Delete Scenarios
1. Create a volume
2. Try to delete the volume wihh mon_allow_pool_delete to false
3. Check volume delete fails and validate the error message
4. Set mon_allow_pool_delete to true
5. Delete the volume and check if the volume is deleted
# Rename scenarios
6. Try to rename the volume without refuse_client_session flag
7. Try to rename the volume without fail the volume
8. Try to rename the volume with additional pools
9. Try to rename the volume with subvolumegroup and subvolume
10. Rename the volume while IO is running in the mounted directory
# subvolume earmark and subvolume group idempotence
11 .Creating subvolumes with desired uid, gid, modes, with and without groups
12. Creating subvolumes with invalid size (negative and 0)
13. Creating subvolumes with --group_name=_nogroup
14. Setting and getting earmarks during subvolume creation and on existing subvolumes
15. Setting earmarks with group_name
16. Setting and removing earmarks while IO is in progress
"""


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83604978"
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
        ran_string = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5))
        start_volume = f"cephfs1_{ran_string}"
        fs_util.create_fs(client1, start_volume)
        log.info(client1.exec_command(sudo=True, cmd="ceph fs ls"))
        volume_name_list = []
        # Try to delete the volume with mon_allow_pool_delete to false
        log.info(
            "\n"
            "\n---------------***************-------------------------------------------------------"
            "\n          Scenario 1: Volume Delete scenarios"
            "\n---------------***************-------------------------------------------------------"
            "\n"
        )
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete false"
        )
        delete_result, delete_ec = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rm {start_volume} --yes-i-really-mean-it",
            check_ec=False,
        )
        if delete_result == 0:
            log.error(
                "Volume deletetion should not succeed when mon_allow_pool_delete is false"
            )
            return 1
        log.info("Volume deletion failed as expected")
        # Set mon_allow_pool_delete to true
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        # Delete the volume and check if the volume is deleted
        delete_result2, delete_ec2 = client1.exec_command(
            sudo=True, cmd=f"ceph fs volume rm {start_volume} --yes-i-really-mean-it"
        )
        if delete_ec2 == 0:
            log.error("Volume deletion failed")
            return 1
        else:
            log.info("Volume deletion successful")
        # Rename the volume
        log.info(
            "\n"
            "\n---------------***************-------------------------------------------------------"
            "\n          Scenario 2: Volume rename scenarios"
            "\n---------------***************-------------------------------------------------------"
            "\n"
        )
        fs_name = f"cephfs1_{ran_string}"
        client1.exec_command(sudo=True, cmd=f"ceph fs volume create {fs_name}")
        mds_nodes = ceph_cluster.get_nodes("mds")
        host_list = [node.hostname for node in mds_nodes]
        hosts = " ".join(host_list)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph orch apply mds {fs_name} --placement='3 {hosts}'",
            check_ec=False,
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{ran_string}"
        fs_util.fuse_mount(
            [client1], fuse_mounting_dir_1, extra_params=f" --client_fs {fs_name}"
        )
        # fill the cluster up to 50
        cephfs = {
            "fill_data": 50,
            "io_tool": "smallfile",
            "mount": "fuse",
            "filesystem": fs_name,
            "mount_dir": f"{fuse_mounting_dir_1}",
        }
        # fill up to 50% of the cluster
        fs_io(client=clients[0], fs_config=cephfs, fs_util=fs_util)
        volume_name_list.append(f"cephfs1_{ran_string}")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rename cephfs1_{ran_string} cephfs2_{ran_string} --yes-i-really-mean-it",
            check_ec=False,
        )
        # check if the volume is renamed with active volume and without refuse_client flag
        rename_result, rename_ec = client1.exec_command(sudo=True, cmd="ceph fs ls")
        if f"cephfs2_{ran_string}" in rename_result:
            log.error(
                "Volume rename should not succeed when the volume is active "
                "and refuse_client_session is false"
            )
            return 1
        else:
            log.info("Volume rename successful")
        # Rename the volume only when the volume is down
        client1.exec_command(sudo=True, cmd=f"ceph fs fail cephfs1_{ran_string}")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rename cephfs1_{ran_string} cephfs2_{ran_string} --yes-i-really-mean-it",
            check_ec=False,
        )
        rename_result, rename_ec = client1.exec_command(sudo=True, cmd="ceph fs ls")
        if f"cephfs2_{ran_string}" in rename_result:
            log.error(
                "[Volume rename should not succeed without refuse_client_session flag true]"
            )
            return 1
        else:
            log.info(
                "Volume rename did not go through as expected without refuse_client_session flag true"
            )
        log.info(
            "[Rename the volume with refuse_client_session flag and volume is down]"
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs set cephfs1_{ran_string} refuse_client_session true",
        )
        result = fs_util.rename_volume(
            client1, f"cephfs1_{ran_string}", f"cephfs2_{ran_string}"
        )
        if result == 1:
            log.error("Volume rename failed")
            return 1
        else:
            volume_name_list.append(f"cephfs2_{ran_string}")
            log.info("Volume rename successful")

        # Rename the volume with additional pools
        pool_name = f"cephfs2_data_pool_{ran_string}"
        client1.exec_command(sudo=True, cmd=f"ceph osd pool create {pool_name} 32 32")
        client1.exec_command(
            sudo=True, cmd=f"ceph fs add_data_pool cephfs2_{ran_string} {pool_name}"
        )
        result = fs_util.rename_volume(
            client1, f"cephfs2_{ran_string}", f"cephfs3_{ran_string}"
        )
        if result == 1:
            volume_name_list.append(f"cephfs2_{ran_string}")
            log.error("Volume rename failed")
            return 1
        else:
            volume_name_list.append(f"cephfs3_{ran_string}")
            log.info("Volume rename successful with additional pools")
        # Rename the volume with subvolumegroup and subvolume
        group_name = f"cephfs3_subvolumegroup_{ran_string}"
        substart_volume = f"cephfs3_subvolume_{ran_string}"
        fs_util.create_subvolumegroup(
            client1, f"cephfs3_{ran_string}", group_name=group_name
        )
        time.sleep(5)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume create cephfs3_{ran_string} {substart_volume} --group_name {group_name}",
        )
        volume_name_list.append(f"cephfs3_{ran_string}")
        result = fs_util.rename_volume(
            client1, f"cephfs3_{ran_string}", f"cephfs4_{ran_string}"
        )
        if result == 1:
            log.error("Volume rename failed")
            return 1
        else:
            volume_name_list.append(f"cephfs4_{ran_string}")
            log.info("Volume rename successful with subvolumegroup and subvolume")
        log.info("[Rename the volume while IO is going on in the mounted directory]")
        with parallel() as p:
            p.spawn(fs_util.run_ios(client1, fuse_mounting_dir_1, ["dd", "smallfile"]))
        result = fs_util.rename_volume(
            client1, f"cephfs4_{ran_string}", f"cephfs5_{ran_string}"
        )
        if result == 1:
            log.error("Volume rename failed")
            return 1
        else:
            volume_name_list.append(f"cephfs5_{ran_string}")
        log.info("Volume rename successful with IOs")
        log.info("[Rename scenario is successful]")
        log.info(
            "\n"
            "\n---------------***************-------------------------------------------------------"
            "\n          Scenario 3: Subvolumegroup create scenarios"
            "\n---------------***************-------------------------------------------------------"
            "\n"
        )
        group_name = f"invalid_layout_group_{ran_string}"
        fs_name = f"cephfs5_{ran_string}"
        command = f"ceph fs subvolumegroup create {fs_name} {group_name} --pool_layout invalid"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if "Error EINVAL" not in err:
            log.error(
                f"Subvolumegroup creation with invalid pool layout should have failed: {out}"
            )
            return 1
        log.info("Subvolumegroup creation with invalid pool layout failed as expected")
        # Create a subvolumegroup with valid pool layout
        pool_name = f"cephfs.data.{fs_name}"
        client1.exec_command(sudo=True, cmd=f"ceph osd pool create {pool_name}  16 16")
        client1.exec_command(
            sudo=True, cmd=f"ceph fs add_data_pool {fs_name} {pool_name}"
        )
        group_name = f"valid_layout_group_{ran_string}"
        command = f"ceph fs subvolumegroup create {fs_name} {group_name} --pool_layout {pool_name}"
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error EINVAL" in err:
            log.error(f"Subvolumegroup creation with valid pool layout failed: {out}")
            return 1
        out, err = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolumegroup info {fs_name} {group_name}"
        )
        if "Error" in err:
            log.error(f"Subvolumegroup creation with valid pool layout failed: {err}")
            return 1
        out, err = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolumegroup ls {fs_name}"
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
            group_name = f"mode_{mode}_group_{ran_string}"
            command = (
                f"ceph fs subvolumegroup create {fs_name} {group_name} --mode {mode}"
            )
            log.info(f"Executing command: {command}")
            out, err = client1.exec_command(sudo=True, cmd=command)
            if "Error" in err:
                log.error(f"Subvolumegroup creation with mode {mode} failed: {err}")
                return 1
            out, err = client1.exec_command(
                sudo=True, cmd=f"ceph fs subvolumegroup getpath {fs_name} {group_name}"
            )
            ls_out, ls_err = client1.exec_command(
                sudo=True, cmd=f"ls -ld {fuse_mounting_dir_1}/"
            )
            log.info(f"ls_out: {ls_out}")
            stat_out, stat_err = client1.exec_command(
                sudo=True, cmd=f"stat {fuse_mounting_dir_1}{out}"
            )
            if f"0{mode}" not in stat_out:
                log.error(f"Subvolumegroup creation with mode {mode} failed: {err}")
                return 1
            log.info(f"Subvolumegroup creation with mode {mode} successful")
            client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolumegroup rm {fs_name} {group_name} --force",
            )
        # Create a subvolumegroup with invalid mode
        group_name = f"invalid_mode_group_{ran_string}"
        command = f"ceph fs subvolumegroup create {fs_name} {group_name} --mode abcd"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if "Error EINVAL" not in (err or out):
            log.error(
                f"Subvolumegroup creation with invalid mode should have failed: {out}"
            )
            return 1
        log.info("Subvolumegroup creation with invalid mode failed as expected")
        # Create a subvolumegroup with desired UID/GID
        group_name = f"desired_uid_gid_group_{ran_string}"
        command = f"ceph fs subvolumegroup create {fs_name} {group_name} --uid 1000 --gid 1000"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolumegroup creation with desired UID/GID failed: {err}")
            return 1
        log.info("Subvolumegroup creation with desired UID/GID successful")

        # Create a subvolumegroup with invalid UID/GID
        group_name = f"invalid_uid_gid_group_{ran_string}"
        command = (
            f"ceph fs subvolumegroup create {fs_name} {group_name} --uid -1 --gid abcd"
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
        group_name = f"specific_size_group_{ran_string}"
        command = f"ceph fs subvolumegroup create {fs_name} {group_name} --size 10000"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolumegroup creation with specific size failed: {err}")
            return 1
        out, err = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolumegroup exist {fs_name}"
        )
        if "no subvolumegroup" in out:
            log.error(f"Subvolumegroup creation with various options failed: {err}")
            return 1
        log.info("Subvolumegroup creation with specific size successful")
        # Resize the subvolumegroup increasing the size
        command = f"ceph fs subvolumegroup resize {fs_name} {group_name} 40000"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolumegroup resize (increasing size) failed: {err}")
            return 1
        log.info("Subvolumegroup resize (increasing size) successful")

        # Resize the subvolumegroup decreasing the size
        command = f"ceph fs subvolumegroup resize {fs_name} {group_name} 10000"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolumegroup resize (decreasing size) failed: {err}")
            return 1
        log.info("Subvolumegroup resize (decreasing size) successful")
        # Create a subvolumegroup with various options
        group_name = f"various_options_group_{ran_string}"
        command = (
            f"ceph fs subvolumegroup create {fs_name} {group_name} --mode 750"
            f" --uid 1001 --gid 1001 --size 1000000"
        )
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolumegroup creation with various options failed: {err}")
            return 1
        out, err = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolumegroup exist {fs_name}"
        )
        if "no subvolumegroup" in out:
            log.error(f"Subvolumegroup creation with various options failed: {err}")
            return 1
        out, err = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolumegroup getpath {fs_name} {group_name}"
        )
        # verify the result with stat
        stat_out, stat_err = client1.exec_command(
            sudo=True, cmd=f"stat {fuse_mounting_dir_1}{out}"
        )
        if "1001" not in stat_out:
            log.error(f"Subvolumegroup creation with various options failed: {err}")
            return 1
        log.info("Subvolumegroup creation with various options successful")
        info_out, info_err = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolumegroup info {fs_name} {group_name}"
        )
        if "1000000" not in info_out:
            log.error(f"Subvolumegroup creation with various options failed: {err}")
            return 1
        log.info("Subvolumegroup creation with various options successful")
        if "rwxr-x---" not in stat_out:
            log.error(f"Subvolumegroup creation with various options failed: {err}")
            return 1
        log.info(f"Subvolumegroup options verification successful for {group_name}")
        subfs_name = f"subvol1_{ran_string}"
        # Idempotence default - same command 2-3 times
        for _ in range(3):
            command = f"ceph fs subvolume create {fs_name} {subfs_name}"
            log.info(f"Executing command: {command}")
            out, err = client1.exec_command(sudo=True, cmd=command)
            if "Error" in err:
                log.error(f"Subvolume creation failed: {err}")
                return 1
        log.info("Subvolume creation idempotence (default) successful")
        # Idempotence with modes, uid, gid - multiple retries
        subfs_name = f"subvol2_{ran_string}"
        for _ in range(3):
            command = f"ceph fs subvolume create {fs_name} {subfs_name} --mode 755 --uid 1000 --gid 1000"
            log.info(f"Executing command: {command}")
            out, err = client1.exec_command(sudo=True, cmd=command)
            if "Error" in err:
                log.error(f"Subvolume creation failed: {err}")
                return 1
        log.info("Subvolume creation idempotence (with modes, uid, gid) successful")
        # With isolated_namespace
        subfs_name = f"subvol3_{ran_string}"
        command = (
            f"ceph fs subvolume create {fs_name} {subfs_name} --namespace-isolated"
        )
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolume creation with isolated_namespace failed: {err}")
            return 1
        log.info("Subvolume creation with isolated_namespace successful")
        # Failure in creation should cleanup incomplete subvolumes
        subfs_name = f"subvol4_{ran_string}"
        command = f"ceph fs subvolume create {fs_name} {subfs_name} --invalid_option"  # Simulate failure
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if "Error" not in err:
            log.error(f"Subvolume creation should have failed: {out}")
            return 1
        command = f"ceph fs subvolume ls {fs_name}"
        out, err = client1.exec_command(sudo=True, cmd=command)
        if subfs_name in out:
            log.error(f"Incomplete subvolume {subfs_name} was not cleaned up")
            return 1
        log.info("Subvolume creation failure and cleanup successful")
        # Desired uid, gid, modes, with and without groups
        subfs_name = f"subvol5_{ran_string}"
        command = f"ceph fs subvolume create {fs_name} {subfs_name} --mode 755 --uid 1000 --gid 1000"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Subvolume creation with uid, gid, modes failed: {err}")
            return 1
        subfs_name = f"subvol6_{ran_string}"
        subvol_group_name = f"mygroup_{ran_string}"
        fs_util.create_subvolumegroup(client1, fs_name, subvol_group_name)
        command = (
            f"ceph fs subvolume create {fs_name} {subfs_name}"
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
        subfs_name = f"subvol7_{ran_string}"
        command = f"ceph fs subvolume create {fs_name} {subfs_name} --size -1"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if "Error EINVAL" not in err:
            log.error(f"Subvolume creation with invalid size should have failed: {out}")
            return 1
        subfs_name = f"subvol8_{ran_string}"
        command = f"ceph fs subvolume create {fs_name} {subfs_name} --size 0"
        log.info(f"Executing command: {command}")
        out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if "Error EINVAL" in err:
            log.error(
                f"Subvolume creation with invalid size [0] should have failed: {out}"
            )
            return 1
        log.info("Subvolume creation with invalid size failed as expected")
        # With --group_name=_nogroup
        subfs_name = f"subvol9_{ran_string}"
        command = (
            f"ceph fs subvolume create {fs_name} {subfs_name} --group_name=_nogroup"
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
            log.info(
                "\n"
                "\n---------------***************-------------------------------------------------------"
                "\n          Scenario 4: Subvolume earmark scenarios"
                "\n---------------***************-------------------------------------------------------"
                "\n"
            )
            # set earmark [tag] while subvolume creation
            subfs_name = f"subvol10_{ran_string}"
            fs_util.create_subvolume(client1, fs_name, subfs_name, earmark="nfs.share1")
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, fs_name, subfs_name)
            if "nfs.share1" not in out:
                log.error(f"Subvolume get earmark failed: {err}")
                return 1
            # https://bugzilla.redhat.com/show_bug.cgi?id=2332723
            # create subvolume with earmark with group_name
            # subfs_name = f"subvol11_{ran_string}"
            # fs_util.create_subvolume(client1, fs_name, subfs_name,
            # group_name=subvol_group_name, earmark="smb.share2")
            # # Get earmark
            # out,err=fs_util.get_subvolume_earmark(client1, fs_name, subfs_name)
            # if "smb.share2" not in out:
            #     log.error(f"Subvolume get earmark failed: {err}")
            #     return 1
            # Create a subvolume and set earmark
            subfs_name = f"subvol12_{ran_string}"
            fs_util.create_subvolume(client1, fs_name, subfs_name)
            # Set earmark
            fs_util.set_subvolume_earmark(
                client1, fs_name, subfs_name, earmark="nfs.share3"
            )
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, fs_name, subfs_name)
            if "nfs.share3" not in out:
                log.error(f"Subvolume get earmark failed: {err}")
                return 1
            # create a subvolume and set earmark with invalid name (other than nfs/smb)
            subfs_name = f"subvol13_{ran_string}"
            fs_util.create_subvolume(client1, fs_name, subfs_name)
            # Set earmark
            command = f"ceph fs subvolume earmark set {fs_name} {subfs_name} --earmark invalid.share"
            log.info(f"Executing command: {command}")
            out, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
            if "Invalid earmark" not in err:
                log.error(
                    f"Subvolume set earmark with invalid name should have failed: {out}"
                )
                return 1
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, fs_name, subfs_name)
            if "invalid.share" in out:
                log.error(f"Invalid earmark should have failed: {out}")
                return 1
            # create a subvolume and set earmark and remove the earmark
            subfs_name = f"subvol14_{ran_string}"
            fs_util.create_subvolume(client1, fs_name, subfs_name)
            # Set earmark
            fs_util.set_subvolume_earmark(
                client1, fs_name, subfs_name, earmark="nfs.share4"
            )
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, fs_name, subfs_name)
            if "nfs.share4" not in out:
                log.error(f"Subvolume get earmark failed: {err}")
                return 1
            # Remove earmark
            fs_util.remove_subvolume_earmark(client1, fs_name, subfs_name)
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, fs_name, subfs_name)
            if "nfs.share4" in out:
                log.error(f"Subvolume get earmark failed: {err}")
                return 1
            # Set earmark and rm earmark while IO is in progress
            subfs_name = f"subvol15_{ran_string}"
            fs_util.create_subvolume(client1, fs_name, subfs_name)
            # run IOs in the subvolume
            sub_path, _ = client1.exec_command(
                sudo=True, cmd=f"ceph fs subvolume getpath {fs_name} {subfs_name}"
            )
            with parallel() as p:
                p.spawn(fs_util.run_ios(client1, f"{fuse_mounting_dir_1}{sub_path}"))
            # Set earmark
            fs_util.set_subvolume_earmark(
                client1, fs_name, subfs_name, earmark="nfs.share5"
            )
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, fs_name, subfs_name)
            log.info(f"Get earmark output: {out}")
            if "nfs.share5" not in out:
                log.error(f"Subvolume get earmark failed: {err}")
                return 1
            # Remove earmark
            fs_util.remove_subvolume_earmark(client1, fs_name, subfs_name)
            # Get earmark
            out = fs_util.get_subvolume_earmark(client1, fs_name, subfs_name)
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
        log.info(
            "\n"
            "\n---------------***************-------------------------------------------------------"
            "\n          Cleaning Up"
            "\n---------------***************-------------------------------------------------------"
            "\n"
        )
        client1.exec_command(
            sudo=True, cmd=f"umount {fuse_mounting_dir_1}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {fuse_mounting_dir_1}", check_ec=False
        )
        # Clean up the subvolumegroups and volume
        for i in range(1, 15):
            substart_volume = f"subvol{i}_{ran_string}"
            client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume rm {fs_name} {substart_volume} --force || true",
                check_ec=False,
            )
        fs_name = f"cephfs5_{ran_string}"
        subvol_group_name = f"mygroup_{ran_string}"
        subvolume_name = f"subvol15_{ran_string}"
        # client1.exec_command(
        #     sudo=True,
        #     cmd=f"ceph fs subvolume rm {fs_name} subvol6_{ran_string} --group_name mygroup_{ran_string}",
        # )
        client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolume rm {fs_name} {subvolume_name} "
        )
        group_rm_list = [
            f"cephfs3_subvolumegroup_{ran_string}",
            f"mygroup_{ran_string}",
            f"invalid_layout_group_{ran_string}",
            f"valid_layout_group_{ran_string}",
            f"desired_mode_group_{ran_string}",
            f"invalid_mode_group_{ran_string}",
            f"desired_uid_gid_group_{ran_string}",
            f"invalid_uid_gid_group_{ran_string}",
            f"specific_size_group_{ran_string}",
            f"various_options_group_{ran_string}",
        ]
        for group_name in group_rm_list:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolumegroup rm {fs_name} {group_name} --force || true",
            )
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        fs_util.remove_fs(client1, fs_name)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {fs_name}_{ran_string} {subvol_group_name} --force || true",
        )
