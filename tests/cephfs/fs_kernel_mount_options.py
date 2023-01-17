import json
import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def umount_fs(client, mounting_dir):
    log.info("Unmount the File sytem")
    client.exec_command(sudo=True, cmd=f"umount {mounting_dir} -l")


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83575389 - [Kernel] - Ensure kernel mounts works with all available options and validate the
                   functionality of each option.
    We are covering below options as part of this script
    1. conf
    2. mount_timeout
    3. ms_mode
    4. mon_addr
    5. fsid
    6. secret
    7. secretfile
    8. recover_session
    """
    try:
        log.info(f"MetaData Information {log.metadata} in {__name__}")
        fs_util = FsUtils(ceph_cluster)

        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        if not build.startswith(("3", "4", "5")):
            if not fs_util.validate_fs_info(clients[0], "cephfs"):
                log.error("FS info Validation failed")
                return 1
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )

        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}/"
        mon_node_ips = fs_util.get_mon_node_ips()
        log.info("Validate conf option while mounting the cluster")
        log.info("mount with invalid conf file path")

        clients[0].exec_command(
            sudo=True, cmd="cp /etc/ceph/ceph.conf /home/cephuser/ceph_dup.conf"
        )
        clients[0].exec_command(
            sudo=True, cmd="sed -i 's/fsid = /fsid = t1/g' /home/cephuser/ceph_dup.conf"
        )
        mon_node_ip = ",".join(mon_node_ips)
        kernel_cmd = (
            f"mount -t ceph {mon_node_ip}:/ {kernel_mounting_dir} "
            f"-o conf=/home/cephuser/ceph_dup.conf"
        )
        out, rc = clients[0].exec_command(sudo=True, cmd=kernel_cmd, check_ec=False)
        log.info(
            f"Returned code for the invalid conf file path is {rc} and out is {out}"
        )
        if not rc:
            raise CommandFailed(
                f"Mount command succeed where it is expected to fail with rc 32 but rc returned is {rc}"
            )

        log.info("mount with Valid conf file path")
        rc = fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=",conf=/etc/ceph/ceph.conf",
            validate=True,
        )

        clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={kernel_mounting_dir}/valid_conf_file_path bs=10M count=10",
        )

        umount_fs(clients[0], kernel_mounting_dir)

        log.info("mount with Valid conf file path and mount_timeout")
        rc = fs_util.kernel_mount(
            clients,
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=",conf=/etc/ceph/ceph.conf,mount_timeout=100,",
            validate=True,
        )
        clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={kernel_mounting_dir}/valid_conf_file_path_timeout bs=10M count=10",
        )

        umount_fs(clients[0], kernel_mounting_dir)

        log.info("mount with Valid conf file path and mount_timeout")
        rc = fs_util.kernel_mount(
            clients,
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=",conf=/home/cephuser/ceph_dup.conf,mount_timeout=300,",
            validate=True,
        )

        umount_fs(clients[0], kernel_mounting_dir)

        clients[0].exec_command(
            sudo=True, cmd="rm -rf /home/cephuser/ceph_dup.conf", check_ec=False
        )

        log.info("mount with Valid conf file path and mount_timeout and ms_mode")
        rc = fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=",conf=/etc/ceph/ceph.conf,mount_timeout=100,ms_mode=legacy",
            validate=True,
        )
        clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={kernel_mounting_dir}/ms_mode bs=10M count=10",
        )
        umount_fs(clients[0], kernel_mounting_dir)

        log.info("mount secret as argument")
        secert_file = f"/etc/ceph/{clients[0].node.hostname}.secret"
        out, _ = clients[0].exec_command(cmd=f"cat {secert_file}", sudo=True)
        log.info("Output : %s" % out)
        mon_node_ip = ",".join(mon_node_ips)
        kernel_cmd = (
            f"mount -t ceph {mon_node_ip}:/ {kernel_mounting_dir} "
            f"-o name={clients[0].node.hostname},"
            f"secret={out}"
        )
        clients[0].exec_command(sudo=True, cmd=kernel_cmd)
        fs_util.wait_until_mount_succeeds(clients[0], kernel_mounting_dir)
        clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={kernel_mounting_dir}/secret bs=10M count=10",
        )
        umount_fs(clients[0], kernel_mounting_dir)

        log.info("mount with mon_address")
        kernel_cmd = (
            f"mount -t ceph {clients[0].node.hostname}@.cephfs=/ {kernel_mounting_dir} "
            f"-o name={clients[0].node.hostname},"
            f"secretfile=/etc/ceph/{clients[0].node.hostname}.secret,mon_addr={mon_node_ip.replace(',','/')}"
        )
        clients[0].exec_command(sudo=True, cmd=kernel_cmd)
        fs_util.wait_until_mount_succeeds(clients[0], kernel_mounting_dir)
        clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={kernel_mounting_dir}/mon_address bs=10M count=10",
        )
        umount_fs(clients[0], kernel_mounting_dir)

        log.info("mount with invalid fsid")
        rc = fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=",fsid=test_dup",
            validate=False,
        )
        if rc == 0:
            raise CommandFailed(
                f"Mount command succeed where it is expected to fail with rc 32 but rc returned is {rc}"
            )
        log.info("mount with valid fsid")
        out, rc = clients[0].exec_command(sudo=True, cmd="ceph fsid -f json")
        fsid = json.loads(out)["fsid"]
        rc = fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=f",fsid={fsid}",
            validate=True,
        )
        clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={kernel_mounting_dir}/fsid bs=10M count=10",
        )
        umount_fs(clients[0], kernel_mounting_dir)

        log.info("mount with recovery_session Options")
        default_fs = "cephfs"
        log.info("mount with recovery_session with option no")
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": "subvol_recover_session_no",
            "size": "5368706371",
        }
        fs_util.create_subvolume(clients[0], **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_recover_session_no",
        )
        rc = fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
            extra_params=",conf=/etc/ceph/ceph.conf,mount_timeout=100,recover_session=no",
            validate=True,
        )

        out, rc = clients[0].exec_command(
            sudo=True, cmd="ceph tell mds.0 client ls -f json"
        )
        get_client_details = json.loads(out)
        for kernel_client in get_client_details:
            if "subvol_recover_session_no" in kernel_client.get("client_metadata").get(
                "root"
            ):
                evict_client = kernel_client
                break
        log.info("write data to client before evicting")
        fs_util.create_file_data(
            clients[0], kernel_mounting_dir, 3, "snap1", "data_from_fuse_mount "
        )
        clients[0].exec_command(
            sudo=True, cmd=f"ceph tell mds.0 client evict id={evict_client.get('id')}"
        )
        out, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"touch {kernel_mounting_dir}/test_{mounting_dir}",
            check_ec=False,
        )
        if rc == 0:
            raise CommandFailed(
                "we are able to Access directory even after evict.. which is not correct"
            )
        umount_fs(clients[0], kernel_mounting_dir)

        rc = fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
            extra_params=",conf=/etc/ceph/ceph.conf,mount_timeout=100,recover_session=clean",
            validate=True,
        )
        out, rc = clients[0].exec_command(
            sudo=True, cmd="ceph tell mds.0 client ls -f json"
        )
        get_client_details = json.loads(out)
        for kernel_client in get_client_details:
            if "subvol_recover_session_no" in kernel_client.get("client_metadata").get(
                "root"
            ):
                evict_client = kernel_client
                break
        log.info("write data to client before evicting")
        clients[0].exec_command(
            sudo=True, cmd=f"ceph tell mds.0 client evict id={evict_client.get('id')}"
        )
        out, rc = clients[0].exec_command(
            sudo=True, cmd=f"touch {kernel_mounting_dir}/test_{mounting_dir}"
        )

        umount_fs(clients[0], kernel_mounting_dir)
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Clean Up in progess")
        fs_util.remove_subvolume(clients[0], **subvolume)
