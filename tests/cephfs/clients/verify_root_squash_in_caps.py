import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83573868
    Test Steps:
    1. Create 2 CepFS and subvolume in each FS
    2. Create client user with root_squash cap for / and regular permissions for /volumes on both CephFS and verify
    3. Perform Kernel and Fuse mount of CephFS
    4. Validate root_squash is working on / and write permissions work on /volumes
    5. cleanup
    """
    try:
        tc = "CEPH-83573868"
        log.info("Running cephfs %s test case" % (tc))
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        client = clients[0]
        log.info("checking Pre-requisites")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        log.info(
            "Create Test configuration - Additional CephFS and one subvolume in each CephFS"
        )
        default_fs = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(clients[0], default_fs)

        if not fs_details:
            fs_util.create_fs(clients[0], default_fs)
        fs_util.create_fs(client, "cephfs_1")
        fs_list = [default_fs, "cephfs_1"]
        for fs in fs_list:
            subvolume = {"vol_name": fs, "subvol_name": "subvolume1"}
            fs_util.create_subvolume(client, **subvolume)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        log.info("Add data to / to verify read permission with root_squash")
        filename = "mount_file"
        dirname = "squash_dir"
        client_name = "client.add_data"
        for fs_name in fs_list:
            create_cmd = f"ceph fs authorize {fs_name} client.add_data / rw"
            client.exec_command(sudo=True, cmd=create_cmd)
            get_cmd = "ceph auth get client.add_data -o /etc/ceph/ceph.client.add_data.keyring"
            client.exec_command(sudo=True, cmd=get_cmd)
            mnt_path = f"/mnt/{fs_name}_fuse_tmp_{mounting_dir}_1/"
            fs_util.fuse_mount(
                [client],
                mnt_path,
                new_client_hostname="add_data",
                extra_params=f" --client_fs {fs_name}",
            )
            cmd = f"mount > {mnt_path}/{filename};mount > {mnt_path}/volumes/{filename}"
            client.exec_command(
                sudo=True,
                cmd=cmd,
                check_ec=False,
            )
            cmd = f"mkdir {mnt_path}/{dirname};mkdir {mnt_path}/volumes/{dirname}"
            client.exec_command(sudo=True, cmd=cmd)
            client.exec_command(sudo=True, cmd=f"umount {mnt_path}")
            client.exec_command(sudo=True, cmd="ceph auth del client.add_data")

        client_name = "client.verify_root_squash"
        mds_cap_fs1 = f"'allow rw fsname={fs_list[0]} root_squash, allow rw fsname={fs_list[0]} path=/volumes"
        mds_cap_fs2 = f",allow rw fsname={fs_list[1]} root_squash, allow rw fsname={fs_list[1]} path=/volumes'"
        mon_cap_fs = f"'allow r fsname={fs_list[0]},allow r fsname={fs_list[1]}'"
        osd_cap_fs = f"'allow rw tag cephfs data={fs_list[0]},allow rw tag cephfs data={fs_list[1]}'"
        caps = f"mds {mds_cap_fs1}{mds_cap_fs2} mon {mon_cap_fs} osd {osd_cap_fs}"
        create_cmd = f"ceph auth get-or-create {client_name} {caps}"
        log.info(f"Set client user {client_name} with caps for root squash")
        client.exec_command(sudo=True, cmd=create_cmd)
        key_ring_path = f"/etc/ceph/ceph.{client_name}.keyring"
        get_cmd = f"ceph auth get {client_name} -o {key_ring_path}"
        client.exec_command(sudo=True, cmd=get_cmd)
        out, rc = client.exec_command(sudo=True, cmd=f"cat {key_ring_path}")
        if "root_squash" not in out:
            raise CommandFailed(f"Failed to add root_squash cap to {client_name}")
        log.info(
            f"ceph keyring file for {client_name} with root_squash enabled : \n\n{out}"
        )

        log.info("Verify root_squash cap works on kernel and fuse mount clients")
        mount_path = {}
        for fs_name in fs_list:
            mount_path[fs_name] = {}
            mount_path[fs_name]["kernel"] = f"/mnt/{fs_name}_kernel_{mounting_dir}_1/"
            mount_path[fs_name]["fuse"] = f"/mnt/{fs_name}_fuse_{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        for fs_name in fs_list:
            client.exec_command(
                sudo=True, cmd=f"mkdir -p {mount_path[fs_name]['kernel']}"
            )
            fs_util.kernel_mount(
                [client],
                mount_path[fs_name]["kernel"],
                ",".join(mon_node_ips),
                new_client_hostname="verify_root_squash",
                extra_params=f",fs={fs_name}",
            )
        for fs_name in fs_list:
            client.exec_command(
                sudo=True, cmd=f"mkdir -p {mount_path[fs_name]['fuse']}"
            )
            client.exec_command(
                sudo=True,
                cmd=f"ceph-fuse -n {client_name} --client_fs {fs_name} {mount_path[fs_name]['fuse']}",
            )
        log.info("Checking if the root_squash path / has write permissions")
        cmds_to_run = {
            "mkdir": "test_mkdir",
            "touch": "test_file",
            "rm -f": filename,
            "rmdir": dirname,
        }

        for fs_name in fs_list:
            for mnt_path in mount_path[fs_name].values():
                for cmd in cmds_to_run:
                    suffix = "".join(
                        random.choice(string.ascii_lowercase + string.digits)
                        for _ in list(range(2))
                    )
                    cmd_str = f"{cmd} {mnt_path}/{cmds_to_run[cmd]}"
                    if cmd in ["mkdir", "touch"]:
                        cmd_str += f"_{suffix}"
                    try:
                        out, err = client.exec_command(sudo=True, cmd=cmd_str)
                        log.error(
                            f"Write ops succeed in path that has root_squash enabled, its Unexpected. \n {out}"
                        )
                        return 1
                    except CommandFailed:
                        log.info(out)

        log.info("Checking if the root_squash path / has read permissions")
        for fs_name in fs_list:
            for mnt_path in mount_path[fs_name].values():
                cmd_str = f" cat {mnt_path}/{filename}"
                try:
                    out, err = client.exec_command(sudo=True, cmd=cmd_str)
                    log.info(out)
                except CommandFailed:
                    log.info(f"Known issue - BZ 2246545. \n {out}")
                    # log.error(f"Read ops failed in path that has root_squash enabled, its Unexpected. \n {out}")
                    # return 1

        log.info("Verify path /volumes has write permissions")
        for fs_name in fs_list:
            rmdir_done = 0
            for mnt_path in mount_path[fs_name].values():
                for cmd in cmds_to_run:
                    log.info(f"{cmd},{rmdir_done}")
                    if ("rmdir" in cmd) and (rmdir_done == 1):
                        log.info("continue")
                        continue
                    suffix = "".join(
                        random.choice(string.ascii_lowercase + string.digits)
                        for _ in list(range(2))
                    )
                    cmd_str = f"{cmd} {mnt_path}/volumes/{cmds_to_run[cmd]}"
                    if cmd in ["mkdir", "touch"]:
                        cmd_str += f"_{suffix}"
                    try:
                        out, err = client.exec_command(sudo=True, cmd=cmd_str)
                        if "rmdir" in cmd:
                            rmdir_done = 1
                        log.info(out)
                    except CommandFailed:
                        log.error(
                            f"Write ops failed in path /volumes, its Unexpected. \n {out}"
                        )
                        return 1
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        for fs_name in fs_list:
            client.exec_command(
                sudo=True,
                cmd=f"umount {mount_path[fs_name]['kernel']};umount {mount_path[fs_name]['fuse']}",
            )
            client.exec_command(
                sudo=True,
                cmd=f"rmdir {mount_path[fs_name]['kernel']};rmdir {mount_path[fs_name]['fuse']}",
            )
        rm_cmd = f"ceph auth del {client_name}"
        client.exec_command(sudo=True, cmd=rm_cmd)
        client.exec_command(sudo=True, cmd=f"rm {key_ring_path}")
        for fs_name in fs_list:
            subvolume = {"vol_name": fs_name, "subvol_name": "subvolume1"}
            fs_util.remove_subvolume(
                client, **subvolume, validate=False, check_ec=False
            )
        client.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        client.exec_command(
            sudo=True, cmd=f"ceph fs volume rm {fs_list[1]} --yes-i-really-mean-it"
        )
        client.exec_command(sudo=True, cmd="ceph config rm mon mon_allow_pool_delete")
