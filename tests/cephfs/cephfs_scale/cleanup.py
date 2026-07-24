import json
import time
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Main function to execute the CephFS cleanup and check data usage.

    This function performs the following steps:
    1. Prepares the clients and cleans up any mounted directories.
    2. Unmounts and removes directories in the /mnt directory on all clients.
    3. Lists and removes all subvolumes in the specified subvolume group.
    4. Checks the data usage of the cephfs data pool and waits until it is reduced to 0%.

    :param ceph_cluster: Ceph cluster object
    :param kw: Additional configuration parameters
    :return: 0 on success, 1 on failure
    """
    try:
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        default_fs = "cephfs-scale"
        subvolume_group_name = "subvolgroup1"

        mount_path = "/mnt/cephfs_scale"

        with parallel() as p:
            for client in clients:
                p.spawn(remove_directory, client, mount_path)

            for client in clients:
                p.spawn(umount_and_remove, client, mount_path)

        list_and_remove_subvolumes(clients[0], default_fs, subvolume_group_name)
        # NFS Cleanup
        log.info("Initiating NFS export and cluster cleanup.")
        cleanup_nfs_exports_and_cluster(clients[0], fs_util_v1)

        check_cephfs_data_usage(clients[0], default_fs)

        fs_util_v1.disable_mds_logs(clients[0], default_fs, validate=True)

        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        for mds in mds_nodes:
            mds.exec_command(sudo=True, cmd="rm -rf /var/log/ceph/*/ceph-mds.*")

        log.info("Cleanup process completed successfully")

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1


def check_cephfs_data_usage(client, fs_name):
    """
    Check the data usage of the cephfs data pool until it is reduced to 0.00%.

    This function repeatedly checks the data usage percentage of the specified
    CephFS data pool. It waits until the usage is reduced to 0% before returning.

    :param client: Client object to execute the command
    :param fs_name: Name of the CephFS filesystem
    """
    while True:
        out, rc = client.exec_command(sudo=True, cmd="ceph df --format json")
        ceph_df_out = json.loads(out)

        for pool in ceph_df_out["pools"]:
            if pool["name"] == f"cephfs.{fs_name}.data":
                percent_used = pool["stats"]["percent_used"]
                log.info(
                    f"Current percent used for cephfs.{fs_name}.data: {percent_used * 100:.2f}%"
                )

                if percent_used < 0.0001:
                    log.info(
                        f"cephfs.{fs_name}.data's percentage used has been reduced to ~0%."
                    )
                    return
                else:
                    log.info(
                        f"Waiting for cephfs.{fs_name}.data's percentage used to be reduced to ~0%..."
                    )
        time.sleep(30)


def list_and_remove_subvolumes(client, default_fs, subvolume_group_name):
    """
    List and remove all subvolumes in the specified subvolume group.

    This function retrieves the list of all subvolumes in the specified subvolume
    group and removes each subvolume.

    :param client: Client object to execute the command
    :param default_fs: Name of the CephFS filesystem
    :param subvolume_group_name: Name of the subvolume group
    """
    out, rc = client.exec_command(
        sudo=True,
        cmd=f"ceph fs subvolume ls {default_fs} {subvolume_group_name} --format json",
    )

    subvolumes = json.loads(out)

    for subvolume in subvolumes:
        subvolume_name = subvolume["name"]
        log.info(f"Removing subvolume: {subvolume_name}")
        client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume rm {default_fs} {subvolume_name} {subvolume_group_name}",
            long_running=True,
        )


def remove_directory(client, mount_path):
    """
    Remove files in the specified directory on the client.

    :param client: Client object to execute the command
    :param mount_path: Path to the directory to be cleaned up
    """
    client.exec_command(
        sudo=True,
        cmd=f"rm -rf {mount_path}*/*",
        timeout=72000,
    )
    log.info(f"Successfully deleted files on {client.node.hostname}")


def umount_and_remove(client, mount_path):
    """
    Unmount and remove the specified mount path on the client.
    Continues even if umount fails due to no mount point.

    :param client: Client object to execute the command
    :param mount_path: Path to be unmounted and removed
    """
    try:
        client.exec_command(
            sudo=True,
            cmd=f"umount -l {mount_path}*/",
            timeout=72000,
            check_ec=False,  # Don't raise exception on non-zero exit
        )
        log.info(f"Successfully unmounted on {client.node.hostname}")
    except CommandFailed as e:
        if "no mount point specified" in str(e):
            log.warning(
                f"No mount point found on {client.node.hostname}, continuing..."
            )
        else:
            log.error(f"Unmount failed on {client.node.hostname}: {e}")
            raise

    try:
        client.exec_command(
            sudo=True, cmd=f"rm -rf {mount_path}*/", timeout=72000, check_ec=False
        )
        log.info(f"Successfully deleted mount path on {client.node.hostname}")
    except CommandFailed as e:
        log.error(f"Failed to delete mount path on {client.node.hostname}: {e}")
        raise


def cleanup_nfs_exports_and_cluster(client, fs_util_v1):
    """
    Cleans up all NFS exports and removes the NFS cluster.

    This function performs the following steps:
    1. Lists all existing NFS clusters.
    2. For each cluster:
       - Lists all exports.
       - Removes each export.
       - Deletes the NFS cluster.

    :param client: Client object to execute the commands.
    :param fs_util_v1: Instance of FsUtilsV1 containing NFS utility methods.
    """
    out, rc = client.exec_command(sudo=True, cmd="ceph nfs cluster ls -f json")
    try:
        cluster_list = json.loads(out)
    except json.JSONDecodeError as e:
        log.error(f"Failed to decode JSON from ceph nfs cluster ls: {e}")
        return

    if not cluster_list:
        log.info("No NFS clusters found for cleanup.")
        return

    for nfs_cluster in cluster_list:
        log.info(f"Cleaning up NFS cluster: {nfs_cluster}")

        # List and remove all exports
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph nfs export ls {nfs_cluster} -f json", check_ec=False
        )
        try:
            exports = json.loads(out.strip()) if out.strip() else []
        except json.JSONDecodeError:
            log.warning(
                f"Failed to parse exports for cluster {nfs_cluster}, skipping..."
            )
            exports = []

        for export in exports:
            log.info(f"Removing NFS export: {export}")
            fs_util_v1.remove_nfs_export(
                client, nfs_cluster_name=nfs_cluster, binding=export, validate=True
            )

        # Remove the cluster
        log.info(f"Removing NFS cluster: {nfs_cluster}")
        fs_util_v1.remove_nfs_cluster(
            client, nfs_cluster_name=nfs_cluster, validate=True
        )

    log.info("All NFS exports and clusters have been successfully cleaned up.")
