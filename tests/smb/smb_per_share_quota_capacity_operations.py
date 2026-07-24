import time

from smb_operations import (
    check_smb_cluster,
    get_smb_shares,
    samba_kernel_mount,
    smb_cleanup,
    verify_smb_service,
)

from ceph.ceph import CommandFailed
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


def get_current_storage_capacity_of_mount(samba_client, mount_point):
    """
    Get current capacity of subvolume/volume/share
    Args:
        samba_client:
        mount_point:
    """
    out = samba_client.exec_command(
        sudo=True,
        cmd=f"df -h | grep '{mount_point}$' | awk '{{print \"Used: \" $3 \", Available: \" $4}}'",
    )
    return out


def set_quota_and_capacity(samba_client, mount_point, quota):
    """
    Set quota and capacity on subvolume or volume
    Args:
        samba_client:
        mount_point:
        quota:

    """
    log.info("Setting quota and capacity of {} bytes".format(quota))
    out, err = samba_client.exec_command(
        sudo=True, cmd=f"setfattr -n ceph.quota.max_bytes -v {quota} {mount_point}"
    )
    if err:
        raise CommandFailed(f"Set quota and capacity failed with error: {out}")


def get_quota_and_capacity(samba_client, mount_point):
    """
    get quota and capacity on subvolume or volume
    Args:
        samba_client:
        mount_point:
    """
    out, _ = samba_client.exec_command(
        sudo=True,
        cmd=f"getfattr -n ceph.quota.max_bytes {mount_point} | awk -F'\"' '/ceph.quota.max_bytes/{{print $2}}'",
    )
    if not out:
        raise CommandFailed(f"Get quota and capacity failed with error: {out}")
    log.info("quota and capacity is set to {} bytes".format(out))
    out = get_current_storage_capacity_of_mount(samba_client, mount_point)
    log.info(f"Storage capacity available after setting quota: {out}")


def delete_quota_and_capacity(samba_client, mount_point):
    """
    delete quota and capacity on subvolume or volume
    Args:
        samba_client:
        mount_point:
    """
    out, _ = samba_client.exec_command(
        sudo=True, cmd=f"setfattr -x ceph.quota.max_bytes {mount_point}"
    )
    out, err = samba_client.exec_command(
        sudo=True, cmd=f"getfattr -n ceph.quota.max_bytes {mount_point}", check_ec=False
    )
    if not err:
        raise CommandFailed("Get quota should throw error, but passed")
    log.info("quota and capacity is deleted {}".format(out))
    out = get_current_storage_capacity_of_mount(samba_client, mount_point)
    log.info(f"Storage capacity available after deleting quota: {out}")


def run(ceph_cluster, **kw):
    """Deploy samba with auth_mode 'user' using imperative style(CLI Commands)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get config
    config = kw.get("config")

    # Get installer node
    installer_node = ceph_cluster.get_nodes(role="installer")[0]

    # Get client node
    client = ceph_cluster.get_nodes(role="client")[0]

    # Get smb cluster id
    smb_cluster_id = config.get("smb_cluster_id")

    # Check smb cluster
    check_smb_cluster(installer_node, smb_cluster_id)

    # Check smb service
    verify_smb_service(installer_node, service_name="smb")

    # Get smb shares
    smb_shares = get_smb_shares(installer_node, smb_cluster_id)
    log.info("smb_shares: {}".format(smb_shares))
    print(type(smb_shares))

    # Get quota operations to perform
    quota_operation = config.get("quota_operation")

    # Get kernel mount point
    mount_point = config.get("mount_point", "/mnt/mount")

    # Get CIFS mount point
    cifs_mount_point = config.get("cifs_mount_point", "/mnt/smb")

    # Share 2 CIFS mount point
    share2_mount_point = cifs_mount_point + "share2"

    # Get sub directory path
    sub_dir = config.get("sub_dir", "/volumes/smb/sv1")

    # Get quota
    quota = config.get("quota", "1000000")

    # Get cleanup value, True if cleanup has to be done, False by default
    smb_cluster_cleanup = config.get("smb_cluster_cleanup", False)

    # Get mount cleanup value, True if cleanup has to be done, False by default
    mount_cleanup = config.get("mount_cleanup", False)

    try:

        log.info("mount subvolume using kernel mount")
        samba_kernel_mount(client, mount_point, installer_node.ip_address, sub_dir)

        if quota_operation == "set_and_get_quota":
            log.info("Set quota and capacity on {}".format(mount_point))
            set_quota_and_capacity(client, mount_point, quota)
            log.info("Get quota and capacity on {}".format(mount_point))
            get_quota_and_capacity(client, mount_point)

        elif quota_operation == "write_within_quota":
            rc = client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={cifs_mount_point}/file1 bs=600M count=1",
                long_running=True,
            )
            time.sleep(5)
            if rc:
                raise OperationFailedError(f"Write operation failed with quota: {rc}")

            out = get_current_storage_capacity_of_mount(client, cifs_mount_point)
            log.info(f"Storage capacity used & available after writing data: {out}")
        elif quota_operation == "write_above_quota":
            rc = client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={cifs_mount_point}/file2 bs=600M count=1",
                check_ec=False,
            )
            if rc == 0:
                raise OperationFailedError(
                    "Write operation should fail above the quota, but did not fail."
                )

            out = get_current_storage_capacity_of_mount(client, cifs_mount_point)
            log.info(f"Storage capacity used & available to write data: {out}")
        elif quota_operation == "multiple_shares_quota":
            rc = client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={cifs_mount_point}/file3 bs=100M count=1",
                long_running=True,
            )
            if rc:
                raise OperationFailedError(f"Write operation failed with quota: {rc}")
            time.sleep(5)
            rc = client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={share2_mount_point}/file4 bs=100M count=1",
                long_running=True,
            )
            if rc:
                raise OperationFailedError(f"Write operation failed with quota: {rc}")
            time.sleep(5)
            out = get_current_storage_capacity_of_mount(client, mount_point)
            log.info(f"Storage capacity used & available after writing data: {out}")

        elif quota_operation == "delete_quota":
            delete_quota_and_capacity(client, mount_point)

    except Exception as e:
        log.error(f"Failed to perform quota operations {quota_operation} : {e}")
        return 1
    finally:
        if smb_cluster_cleanup:
            client.exec_command(
                sudo=True, cmd=f"umount {cifs_mount_point}", long_running=True
            )
            client.exec_command(sudo=True, cmd=f"rm -rf {cifs_mount_point}")
            client.exec_command(
                sudo=True, cmd=f"umount {share2_mount_point}", long_running=True
            )
            client.exec_command(sudo=True, cmd=f"rm -rf {share2_mount_point}")
            smb_cleanup(installer_node, smb_shares, smb_cluster_id)

        client.exec_command(sudo=True, cmd=f"umount {mount_point}", long_running=True)
        client.exec_command(sudo=True, cmd=f"rm -rf {mount_point}", long_running=True)
        if mount_cleanup:
            client.exec_command(sudo=True, cmd=f"rm -rf {cifs_mount_point}/*")
            # TO DO : uncomment once the multiple share access issue is resolved
            # client.exec_command(sudo=True, cmd=f"rm -rf {share2_mount_point}/*")

    return 0
