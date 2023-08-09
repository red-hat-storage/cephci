import json
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83573669 - Create cephfs nfs export on non-existing directory
    Pre-requisites:
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>

    Test operation:
    1. Create cephfs nfs export with non existing path
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> path=<export_path>
    2. Verify path of cephfs nfs export
       ceph nfs export get <nfs_name> <nfs_export_name>
    3. Mount nfs export and validate if it fails

    Clean-up:
    1. Remove cephfs nfs export
    """
    try:
        tc = "CEPH-83573669"
        log.info(f"Running cephfs {tc} test case")
        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        client1.exec_command(sudo=True, cmd="ceph mgr module enable nfs")
        client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/non_exist"
        fs_name = "cephfs"
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
        out, rc = client1.exec_command(sudo=True, cmd=f"ceph nfs export ls {nfs_name}")

        if nfs_export_name not in out:
            raise CommandFailed("Failed to create nfs export")

        log.info("ceph nfs export created successfully")
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph nfs export get {nfs_name} {nfs_export_name}"
        )
        output = json.loads(out)
        nfs_mounting_dir = "/mnt/nfs_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        export_get_path = output["path"]
        if export_get_path != export_path:
            log.error("Export path is not correct")
            return 1

        log.info("Test completed successfully")
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
        command = f"mount -t nfs -o port=2049 {nfs_server}:{nfs_export_name} {nfs_mounting_dir}"
        output, err = client1.exec_command(sudo=True, cmd=command, check_ec=False)
        if not err:
            log.error("cephfs nfs export mount passed with non exiting path")
            return 1
        log.info(f"Unable to mount with non-exist path, return code {err}")
        if "No such file or directory" not in err:
            return 1
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} {nfs_export_name}",
            check_ec=False,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster delete {nfs_name}",
            check_ec=False,
        )
