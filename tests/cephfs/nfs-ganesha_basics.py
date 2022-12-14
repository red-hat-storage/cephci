import datetime
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def wait_for_cmd_to_succeed(client, cmd, timeout=180, interval=5):
    """
    Checks for the mount point and returns the status based on mount command
    :param client:
    :param mount_point:
    :param timeout:
    :param interval:
    :return: boolean
    """
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    log.info("Wait for the command to pass")
    while end_time > datetime.datetime.now():
        try:
            client.exec_command(sudo=True, cmd=cmd)
            return True
        except CommandFailed:
            time.sleep(interval)
    return False


def run(ceph_cluster, **kw):
    try:
        log.info(f"MetaData Information {log.metadata} in {__name__}")
        tc = "nfs-ganesha"
        nfs_mounting_dir = "/mnt/nfs/"
        dir_name = "dir"
        log.info("Running cephfs %s test case" % (tc))
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        rhbuild = config.get("rhbuild")
        if not rhbuild.startswith(("3", "4")):
            from tests.cephfs.cephfs_utilsV1 import FsUtils

            fs_util = FsUtils(ceph_cluster)
            nfs_server = ceph_cluster.get_ceph_objects("nfs")
            nfs_client = ceph_cluster.get_ceph_objects("client")
            fs_util.auth_list(nfs_client)
            nfs_name = "cephfs-nfs"
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd="ceph fs ls | awk {' print $2'} "
            )
            fs_name = out.rstrip()
            fs_name = fs_name.strip(",")
            nfs_export_name = "/export1"
            path = "/"
            nfs_server_name = nfs_server[0].node.hostname
            # Create ceph nfs cluster
            nfs_client[0].exec_command(sudo=True, cmd="ceph mgr module enable nfs")
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server_name}"
            )
            # Verify ceph nfs cluster is created
            if wait_for_process(
                client=nfs_client[0], process_name=nfs_name, ispresent=True
            ):
                log.info("ceph nfs cluster created successfully")
            else:
                raise CommandFailed("Failed to create nfs cluster")
            # Create cephfs nfs export
            if "5.0" in rhbuild:
                nfs_client[0].exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                    f"{nfs_export_name} path={path}",
                )
            else:
                nfs_client[0].exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export create cephfs {nfs_name} "
                    f"{nfs_export_name} {fs_name} path={path}",
                )

            # Verify ceph nfs export is created
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"ceph nfs export ls {nfs_name}"
            )
            if nfs_export_name in out:
                log.info("ceph nfs export created successfully")
            else:
                raise CommandFailed("Failed to create nfs export")
            # Mount ceph nfs exports
            nfs_client[0].exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
            assert wait_for_cmd_to_succeed(
                nfs_client[0],
                cmd=f"mount -t nfs -o port=2049 {nfs_server_name}:{nfs_export_name} {nfs_mounting_dir}",
            )
            nfs_client[0].exec_command(
                sudo=True,
                cmd=f"mount -t nfs -o port=2049 {nfs_server_name}:{nfs_export_name} {nfs_mounting_dir}",
            )
            out, rc = nfs_client[0].exec_command(cmd="mount")
            mount_output = out.split()
            log.info("Checking if nfs mount is is passed of failed:")
            assert nfs_mounting_dir.rstrip("/") in mount_output
            log.info("Creating Directory")
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"mkdir {nfs_mounting_dir}{dir_name}"
            )
            nfs_client[0].exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
                f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top "
                f"{nfs_mounting_dir}{dir_name}",
                long_running=True,
            )
            nfs_client[0].exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 "
                f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top "
                f"{nfs_mounting_dir}{dir_name}",
                long_running=True,
            )
            # Unmount nfs
            nfs_client[0].exec_command(sudo=True, cmd=f"umount {nfs_mounting_dir}")
            # Delete cephfs nfs export
            nfs_client[0].exec_command(
                sudo=True, cmd=f"ceph nfs export delete {nfs_name} {nfs_export_name}"
            )
            # Verify cephfs nfs export is deleted
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"ceph nfs export ls {nfs_name}"
            )

            if nfs_export_name not in out:
                log.info("cephf nfs export deleted successfully")
            else:
                raise CommandFailed("Failed to delete cephfs nfs export")
            # Delete nfs cluster
            nfs_client[0].exec_command(
                sudo=True, cmd=f"ceph nfs cluster delete {nfs_name}"
            )
            # Adding Delay to reflect in cluster list
            time.sleep(5)
            if not wait_for_process(
                client=nfs_client[0], process_name=nfs_name, ispresent=False
            ):
                raise CommandFailed("Cluster has not been deleted")
            # Verify nfs cluster is deleted
            out, rc = nfs_client[0].exec_command(sudo=True, cmd="ceph nfs cluster ls")
            if nfs_name not in out:
                log.info("ceph nfs cluster deleted successfully")
            else:
                raise CommandFailed("Failed to delete nfs cluster")

        else:
            from tests.cephfs.cephfs_utils import FsUtils

            fs_util = FsUtils(ceph_cluster)
            client_info, rc = fs_util.get_clients(build)
            if rc == 0:
                log.info("Got client info")
            else:
                raise CommandFailed("fetching client info failed")
            nfs_server = [client_info["kernel_clients"][0]]
            nfs_client = [client_info["kernel_clients"][1]]
            rc1 = fs_util.auth_list(nfs_server)
            rc2 = fs_util.auth_list(nfs_client)
            print(rc1, rc2)
            if rc1 == 0 and rc2 == 0:
                log.info("got auth keys")
            else:
                raise CommandFailed("auth list failed")
            rc = fs_util.nfs_ganesha_install(nfs_server[0])
            if rc == 0:
                log.info("NFS ganesha installed successfully")
            else:
                raise CommandFailed("NFS ganesha installation failed")
            rc = fs_util.nfs_ganesha_conf(nfs_server[0], "admin")
            if rc == 0:
                log.info("NFS ganesha config added successfully")
            else:
                raise CommandFailed("NFS ganesha config adding failed")
            rc = fs_util.nfs_ganesha_mount(
                nfs_client[0], nfs_mounting_dir, nfs_server[0].node.hostname
            )
            if rc == 0:
                log.info("NFS-ganesha mount passed")
            else:
                raise CommandFailed("NFS ganesha mount failed")

            mounting_dir = nfs_mounting_dir + "ceph/"
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"mkdir {mounting_dir}{dir_name}"
            )
            nfs_client[0].exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
                f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top "
                f"{mounting_dir}{dir_name}",
                long_running=True,
            )
            nfs_client[0].exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 "
                f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top "
                f"{mounting_dir}{dir_name}",
                long_running=True,
            )
            log.info("Cleaning up")
            nfs_client[0].exec_command(sudo=True, cmd=f"rm -rf {mounting_dir}*")
            log.info("Unmounting nfs-ganesha mount on client:")
            nfs_client[0].exec_command(
                sudo=True, cmd=" umount %s -l" % (nfs_mounting_dir)
            )
            log.info("Removing nfs-ganesha mount dir on client:")
            nfs_client[0].exec_command(sudo=True, cmd="rm -rf  %s" % (nfs_mounting_dir))

            log.info("Cleaning up successfull")
        return 0
    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
