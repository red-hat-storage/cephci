import random
import time
import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.io.fs_io import fs_io
from utility.log import Log

log = Log(__name__)
"""
ceph fs volume rename <vol_name> <new_vol_name> [--yes-i-really-mean-it]
# Delete Scenarios
1. Create a volume
2. Try to delete the volume wihh mon_allow_pool_delete to false
3. Check volume delete fails and validate the error message
4. Set mon_allow_pool_delete to true
5. Delete the volume and check if the volume is deleted
# Rename scenarios
6. Try to rename the volume
7. Try to rename the volume with additional pools
8. Try to rename the volume with subvolumegroup and subvolume
9. Rename the volume when the volume is down -> fail fs
10. Rename the volume with and without refuse_client_session flags

"""


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83603354"
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
        fs_name = f"cephfs1_{ran_string}"
        client1.exec_command(sudo=True, cmd=f"ceph fs volume create {fs_name}")
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
        with parallel() as p:
            p.spawn(fs_util.run_ios(client1, fuse_mounting_dir_1, ["dd", "smallfile"]))
        volume_name_list.append(f"cephfs1_{ran_string}")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rename cephfs1_{ran_string} cephfs2_{ran_string} --yes-i-really-mean-it",
        )
        # check if the volume is renamed
        rename_result, rename_ec = client1.exec_command(sudo=True, cmd="ceph fs ls")
        if "cephfs2" not in rename_result:
            log.error("Volume rename failed")
            return 1
        else:
            log.info("Volume rename successful")
        # Rename the volume with additional pools
        pool_name = f"cephfs2_data_pool_{ran_string}"
        client1.exec_command(sudo=True, cmd=f"ceph osd pool create {pool_name} 128 128")
        client1.exec_command(
            sudo=True, cmd=f"ceph fs add_data_pool cephfs2_{ran_string} {pool_name}"
        )
        volume_name_list.append(f"cephfs2_{ran_string}")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rename cephfs2_{ran_string} cephfs3_{ran_string} --yes-i-really-mean-it",
        )
        vol_list, _ = client1.exec_command(sudo=True, cmd="ceph fs ls")
        if "cephfs3" not in vol_list:
            log.error("Volume rename failed")
            return 1
        else:
            log.info("Volume rename successful with additional pools")
        # Rename the volume with subvolumegroup and subvolume
        group_name = f"cephfs3_subvolumegroup_{ran_string}"
        subvol_name = f"cephfs3_subvolume_{ran_string}"
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup create cephfs3_{ran_string} {group_name}",
        )
        time.sleep(5)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume create cephfs3_{ran_string} {subvol_name} --group_name {group_name}",
        )
        volume_name_list.append(f"cephfs3_{ran_string}")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rename cephfs3_{ran_string} cephfs4_{ran_string} --yes-i-really-mean-it",
        )
        vol_list, _ = client1.exec_command(sudo=True, cmd="ceph fs ls")
        if "cephfs4" not in vol_list:
            log.error("Volume rename failed")
            return 1
        else:
            log.info("Volume rename successful with subvolumegroup and subvolume")
        # Rename the volume when the volume is down -> ceph fs fail cephfs4
        client1.exec_command(sudo=True, cmd=f"ceph fs fail cephfs4_{ran_string}")
        volume_name_list.append(f"cephfs4_{ran_string}")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rename cephfs4_{ran_string} cephfs5_{ran_string} --yes-i-really-mean-it",
        )
        vol_list, _ = client1.exec_command(sudo=True, cmd="ceph fs ls")
        log.info(vol_list)
        if "cephfs5" not in vol_list:
            log.error("Volume rename failed")
            return 1
        else:
            log.info("Volume rename successful when the volume is down")
        # Rename the volume with and without refuse_client_session flags
        log.info("Rename the volume with refuse_client_session flag")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs set cephfs5_{ran_string} refuse_client_session true",
        )
        volume_name_list.append(f"cephfs5_{ran_string}")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rename cephfs5_{ran_string} cephfs6_{ran_string} --yes-i-really-mean-it",
        )
        vol_list, _ = client1.exec_command(sudo=True, cmd="ceph fs ls")
        log.info(vol_list)
        if "cephfs6" not in vol_list:
            log.error("Volume rename failed")
            return 1
        else:
            log.info("Volume rename successful with refuse_client_session flag")
        log.info("Rename the volume without refuse_client_session flag")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs set cephfs6_{ran_string} refuse_client_session false",
        )
        volume_name_list.append(f"cephfs6_{ran_string}")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rename cephfs6_{ran_string} cephfs7_{ran_string} --yes-i-really-mean-it",
        )
        volume_name_list.append(f"cephfs7_{ran_string}")
        vol_list, _ = client1.exec_command(sudo=True, cmd="ceph fs ls")
        log.info(vol_list)
        if "cephfs7" not in vol_list:
            log.error("Volume rename failed")
            return 1
        else:
            log.info("Volume rename successful without refuse_client_session flag")

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rm {volume_name_list[-1]} --yes-i-really-mean-it",
        )
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete false"
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph osd pool delete {pool_name} {pool_name} --yes-i-really-really-mean-it",
        )
