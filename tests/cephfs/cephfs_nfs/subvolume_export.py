import secrets
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83573993 - Export the nfs share with cli using an existing path created using subvolume
    Pre-requisites:
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>

    Test operation:
    1. Create a subvolume & a subvolumegroup
    2. Create second subvolume in sibvolumegroup
    3. Create cephfs nfs export on first subvolume path
    4. Create cephfs nfs export on second subvolume path
    5. Mount both cephfs nfs exports
    6. Run IOs

    Clean-up:
    1. Remove all the data in Cephfs subvolumes
    2. Umount all cephfs mounts
    3. Remove all cephfs subvolumes
    """

    try:
        tc = "CEPH-83574024"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))

        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        nfs_export_1 = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        nfs_export_2 = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        fs_name = "cephfs"
        nfs_mount_1 = "/mnt/nfs_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        nfs_mount_2 = "/mnt/nfs_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        subvol_grp = "subvol_group_nfs_1"
        subvol_1 = "subvol_nfs_1"
        subvol_2 = "subvol_nfs_2"
        commands = [
            f"ceph fs subvolume create {fs_name} {subvol_1}",
            f"ceph fs subvolumegroup create {fs_name} {subvol_grp}",
            f"ceph fs subvolume create {fs_name} {subvol_2} {subvol_grp}",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        subvol_1_path, rc = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolume getpath {fs_name} {subvol_1}"
        )
        subvol_2_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} {subvol_2} {subvol_grp}",
        )
        if "5.0" in rhbuild:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                f"{nfs_export_1} path={subvol_1_path}",
            )
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                f"{nfs_export_2} path={subvol_2_path}",
            )
        else:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_1} {fs_name} path={subvol_1_path}",
            )
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_2} {fs_name} path={subvol_2_path}",
            )
        rc = fs_util.cephfs_nfs_mount(client1, nfs_server, nfs_export_1, nfs_mount_1)
        if not rc:
            log.error("cephfs nfs export mount failed")
            return 1
        rc = fs_util.cephfs_nfs_mount(client1, nfs_server, nfs_export_2, nfs_mount_2)
        if not rc:
            log.error("cephfs nfs export mount failed")
            return 1
        fs_util.run_ios(client1, nfs_mount_1)
        fs_util.run_ios(client1, nfs_mount_2)
        log.info("Test completed successfully")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up")
        client1.exec_command(sudo=True, cmd=f"rm -rf {nfs_mount_1}/*")
        client1.exec_command(sudo=True, cmd=f"umount {nfs_mount_1}")
        client1.exec_command(sudo=True, cmd=f"rm -rf {nfs_mount_2}/", check_ec=False)
        client1.exec_command(sudo=True, cmd=f"umount {nfs_mount_2}/", check_ec=False)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume rm {fs_name} {subvol_1}",
            check_ec=False,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume rm {fs_name} {subvol_2} {subvol_grp}",
            check_ec=False,
        )
