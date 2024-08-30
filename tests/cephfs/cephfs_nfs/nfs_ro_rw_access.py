import json
import secrets
import string
import traceback
from json import JSONDecodeError

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83574001 - Make necessary changes in config file to export the nfs share with RO and RW
                access and mount the share accordingly and verify if the permission set on export path works.
    Pre-requisites:
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>
    Test operation:
    1. Create nfs cluster with same name
       ceph nfs cluster create <nfs_name> <nfs_server>
    2. Create cephfs nfs export
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> path=<export_path>
    3. Make export Readonly
    4. Mount nfs mount with cephfs export
       mount -t nfs -o port=2049 <nfs_server>:<nfs_export> <nfs_mounting_dir>
    5. Create a file and it should fail with permission denaied error
    6. Make export to ReadWrite(RW)
    7. mount it and try creating the file again. it should pass

    Clean-up:
    1. Remove data in cephfs
    2. Remove cephfs nfs export
    3. Remove all nfs mounts
    """
    try:
        tc = "CEPH-83574001"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
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
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        nfs_mounting_dir = "/mnt/nfs_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if not wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            raise CommandFailed("Cluster has not been created")
        try:
            out, rc = client1.exec_command(sudo=True, cmd="ceph nfs cluster ls -f json")
            output = json.loads(out)
        except JSONDecodeError:
            output = json.dumps([out])
        if nfs_name in output:
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        if "5.0" in rhbuild:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                f"{nfs_export_name} path={export_path}",
            )
        else:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_name} {fs_name} path={export_path}",
            )

        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export get {nfs_name} {nfs_export_name} > export.conf",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="cat export.conf",
        )
        log.info(f"config file for {nfs_export_name}: {out}")
        log.info("Make export as Readonly")
        client1.exec_command(
            sudo=True,
            cmd="sed -i 's/RW/RO/g' export.conf",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="cat export.conf",
        )
        log.info(f"config file for {nfs_export_name}: {out}")
        log.info("Apply the config")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export apply {nfs_name} -i export.conf",
        )

        rc = fs_util.cephfs_nfs_mount(
            client1, nfs_server, nfs_export_name, nfs_mounting_dir
        )
        if not rc:
            log.error("cephfs nfs export mount failed")
            return 1

        touch_file(client1, nfs_mounting_dir)

        log.info("umount the export")
        client1.exec_command(sudo=True, cmd=f"umount {nfs_mounting_dir}")

        log.info("Make export as ReadWrite")
        client1.exec_command(
            sudo=True,
            cmd="sed -i 's/RO/RW/g' export.conf",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="cat export.conf",
        )
        log.info("Apply the config")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export apply {nfs_name} -i export.conf",
        )

        rc = fs_util.cephfs_nfs_mount(
            client1, nfs_server, nfs_export_name, nfs_mounting_dir
        )
        retry_exec_command = retry(CommandFailed, tries=3, delay=30)(
            client1.exec_command
        )
        retry_exec_command(sudo=True, cmd=f"touch {nfs_mounting_dir}/file")

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        commands = [
            f"rm -rf {nfs_mounting_dir}/*",
            f"umount {nfs_mounting_dir}",
            f"ceph nfs export delete {nfs_name} {nfs_export_name}",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {nfs_mounting_dir}/", check_ec=False
        )
        client1.exec_command(sudo=True, cmd="rm -rf export.conf", check_ec=False)


@retry(CommandFailed, tries=3, delay=30)
def touch_file(client1, nfs_mounting_dir):
    out, err = client1.exec_command(
        sudo=True, cmd=f"touch {nfs_mounting_dir}/file", check_ec=False
    )
    log.info(err)
    if not err:
        raise CommandFailed("NFS export has permission to write")
