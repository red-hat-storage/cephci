import json
import time
import traceback
from json import JSONDecodeError

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Post-suite cleanup and CephFS environment reinitialization.

    This function performs a comprehensive cleanup and reinitialization
    of the CephFS environment after each test suite execution.
    The following operations are performed:
    1. Detect and unmount all CephFS mounts from all clients:
       - ceph-fuse mounts
       - kernel (native) Ceph mounts
       - NFS mounts (if any)
    2. Delete all existing CephFS volumes.
    3. Delete all NFS Ganesha clusters.
    4. Validate that the Ceph cluster health is 'HEALTH_OK'.
    5. Recreate default CephFS (`cephfs`) if not present.
    6. Recreate EC-backed CephFS volume (`cephfs-ec`) with necessary pools and flags.
    7. Validate the cluster health again after setup.

    Returns:
        int: 0 if all operations complete successfully, 1 otherwise.
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")
        if len(clients) < 2:
            log.info(
                f"This test requires minimum 2 client nodes.This has only {len(clients)} clients"
            )
            return 1
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        for client in clients:
            log.info(
                f"Checking and unmounting Ceph-related mounts on {client.node.hostname}"
            )

            # All mount types to check
            mount_types = {
                "ceph-fuse": "grep fuse.ceph-fuse || true",
                "kernel-ceph": "grep ' type ceph ' || true",
                "nfs": "grep -E ' type nfs| type nfs4' || true",
            }

            for label, grep_cmd in mount_types.items():
                try:
                    log.info(f"Looking for {label} mounts...")
                    cmd = f"mount | {grep_cmd}"
                    out, rc = client.exec_command(sudo=True, cmd=cmd)
                    mount_points = [
                        line.split()[2] for line in out.splitlines() if line.strip()
                    ]

                    for mp in mount_points:
                        log.info(f"Attempting to unmount {label} mount: {mp}")
                        try:
                            client.exec_command(sudo=True, cmd=f"umount -f {mp}")
                            log.info(f"Successfully unmounted {mp}")
                        except Exception as umount_err:
                            log.warning(f"Failed to unmount {mp}: {umount_err}")
                            log.debug(traceback.format_exc())
                except Exception as e:
                    log.warning(f"Error while processing {label} mounts: {e}")
                    log.debug(traceback.format_exc())
        log.info("Cleaning UP all filesystems")
        rc, ec = clients[0].exec_command(
            sudo=True, cmd="ceph fs ls --format json-pretty"
        )
        result = json.loads(rc)
        clients[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        for fs in result:
            fs_name = fs["name"]
            clients[0].exec_command(
                sudo=True, cmd=f"ceph fs volume rm {fs_name} --yes-i-really-mean-it"
            )
        time.sleep(30)

        log.info("Cleaning UP NFS clusters")
        try:
            out, rc = client.exec_command(sudo=True, cmd="ceph nfs cluster ls -f json")
            output = json.loads(out)
        except JSONDecodeError:
            output = json.dumps([out])
        for nfs in output:
            clients[0].exec_command(sudo=True, cmd=f"ceph nfs cluster delete {nfs}")

        log.info("Proceed only if Ceph Health is OK.")
        if cephfs_common_utils.wait_for_healthy_ceph(clients[0], 1200):
            log.error("Cluster health is not OK even after waiting for 20 mins.")
            return 1
        out, _ = clients[0].exec_command(sudo=True, cmd="ceph health detail")
        log.info("Ceph health details After 3 min sleep:\n%s", out)
        time.sleep(180)  # Waiting to remove all stale entries for cephfs
        out, _ = clients[0].exec_command(sudo=True, cmd="ceph health detail")
        log.info("Ceph health details:\n%s", out)
        fs_util.wait_for_mds_process(clients[0], process_name="", ispresent=False)
        default_fs = "cephfs"
        fs_details = fs_util.get_fs_info(clients[0], default_fs)
        retry_create_fs = retry(CommandFailed, tries=3, delay=30)(fs_util.create_fs)
        if not fs_details:
            retry_create_fs(clients[0], default_fs)
        for i in range(3):
            cmd = f"ceph orch apply mds {default_fs} --placement='label:mds'; ceph fs set {default_fs} max_mds 2"
            clients[0].exec_command(sudo=True, cmd=cmd)

            if fs_util.wait_for_mds_process(clients[0], process_name=default_fs):
                break  # Exit early if MDS is successfully applied

            time.sleep(5)  # optional: wait before retrying
        else:
            raise Exception("Failed to apply MDS after 3 attempts")
        list_cmds = [
            "ceph fs flag set enable_multiple true",
            "ceph osd pool create cephfs-data-ec 64 erasure",
            "ceph osd pool create cephfs-metadata 64",
            "ceph osd pool set cephfs-data-ec allow_ec_overwrites true",
            "ceph fs new cephfs-ec cephfs-metadata cephfs-data-ec --force",
        ]
        if not fs_util.get_fs_info(clients[0], "cephfs-ec"):
            for cmd in list_cmds:
                clients[0].exec_command(sudo=True, cmd=cmd)
        log.info("Proceed only if Ceph Health is OK After Clean UP")
        if cephfs_common_utils.wait_for_healthy_ceph(clients[0], 600):
            log.error("Cluster health is not OK even after waiting for 5 mins.")
            return 1
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
