from ceph.ceph import CommandFailed
from cli.exceptions import OperationFailedError
from smb_operations import (
    check_smb_cluster,
    get_smb_shares,
    smb_cifs_mount,
    smb_cleanup,
    verify_smb_service,
)

from utility.log import Log

log = Log(__name__)


def samba_kernel_mount(samba_client, mount_point, mon_node_ip, sub_dir):
    """
    Mounts the sub volume or volume using kernel mount
    Args:
        samba_client:
        mount_point:
        mon_node_ip:
        sub_dir: specific directory subvolume or volume
    Returns:

    Exceptions:
        assertion error will occur if the device is not mounted
    """
    log.info("Creating mounting dir:")
    samba_client.exec_command(sudo=True, cmd=f"mkdir -p %{mount_point}")
    samba_client.exec_command(
        sudo=True,
        cmd=f"ceph auth get-key client.admin -o "
            f"/etc/ceph/{samba_client.hostname}.secret",
    )
    cmd = (
        f"mount -t ceph {mon_node_ip}:{sub_dir} {mount_point} "
        f"-o name=admin,"
        f"secretfile=/etc/ceph/{samba_client.hostname}.secret,"
        f"noshare"
    )
    cmd_rc = samba_client.exec_command(sudo=True, cmd=cmd, long_running=True)

    if cmd_rc:
        raise CommandFailed(
            f"Ceph Kernel Command failed with error: {cmd_rc}"
        )
    return cmd_rc


def set_quota_and_capacity(samba_client, mount_point, quota):
    """
    Set quota and capacity on subvolume or volume
    Args:
        samba_client:
        mount_point:
        quota:

    """
    log.info("Setting quota and capacity of {} bytes".format(quota))
    out = samba_client.exec_command(sudo=True, cmd=f"setfattr -n ceph.quota.max_bytes -v {quota} {mount_point}")
    if out:
        raise CommandFailed(
            f"Set quota and capacity failed with error: {out}"
        )


def get_quota_and_capacity(samba_client, mount_point, quota):
    """
    get quota and capacity on subvolume or volume
    Args:
        samba_client:
        mount_point:
        quota:

    """
    out, _ = out = samba_client.exec_command(
        sudo=True,
        cmd=f"getfattr -n ceph.quota.max_bytes {mount_point} | awk -F'\"' '/ceph.quota.max_bytes/{{print $2}}'"
    )
    if not out:
        raise CommandFailed(
            f"Get quota and capacity failed with error: {out}"
        )
    log.info("quota and capacity is set to {} bytes".format(out))


def run(ceph_cluster, **kw):
    """Deploy samba with auth_mode 'user' using imperative style(CLI Commands)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get config
    config = kw.get("config")

    # Get installer node
    installer_node = ceph_cluster.get_nodes(role="installer")[0]

    # Get smb nodes
    smb_nodes = ceph_cluster.get_nodes("smb")

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

    # Get auth_mode
    auth_mode = config.get("auth_mode", "user")

    # Get domain_realm
    domain_realm = config.get("domain_realm", None)

    # Get smb user name
    smb_user_name = config.get("smb_user_name", "user1")

    # Get smb user password
    smb_user_password = config.get("smb_user_password", "passwd")

    # Get quota operations to perform
    quota_operation = config.get("quota_operation")

    # Get kernel mount point
    mount_point = config.get("mount_point", "/mnt/mount")

    # Get CIFS mount point
    cifs_mount_point = config.get("mount_point", "/mnt/smb")

    # Get sub directory path
    sub_dir = config.get("sub_dir", "/volumes/smb/sv1")

    # Get quota
    quota = config.get("quota", "1000000")

    # Get cleanup value, True if cleanup has to be done, False by default
    smb_cluster_cleanup = config.get("smb_cluster_cleanup", False)

    try:

        # Mount smb share with cifs
        smb_cifs_mount(
            smb_nodes[0],
            client,
            smb_shares[0],
            smb_user_name,
            smb_user_password,
            auth_mode,
            domain_realm,
            cifs_mount_point,
        )

        log.info("mount subvolume using kernel mount")
        rc = samba_kernel_mount(client, mount_point, installer_node.ip_address, sub_dir)

        if quota_operation == "set_and_get_quota":
            log.info("Set quota and capacity on {}".format(mount_point))
            set_quota_and_capacity(client, mount_point, quota)
            log.info("Get quota and capacity on {}".format(mount_point))
            get_quota_and_capacity(client, mount_point, quota)

        elif quota_operation == "write_within_quota":
            rc, err = client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={cifs_mount_point}/file1 bs=100M count=5",
            )
            if err:
                raise OperationFailedError(
                    f"Ceph Kernel Command failed with error: {err}"
                )

            log.info("Print Samba and container Info Samba Control")
        elif quota_operation == "write_above_quota":
            rc, err = client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={cifs_mount_point}/file1 bs=100M count=10",
            )
            if not err:
                raise OperationFailedError(
                    f"Ceph Kernel Command failed with error"
                )

            log.info("Print Samba and container Info Samba Control ")
        elif quota_operation == "multiple_shares_quota":

            log.info("Print Samba and container Info Samba Control ")
        elif quota_operation == "delete_quota":

            log.info("Print Samba and container Info Samba Control ")

    except Exception as e:
        log.error(f"Failed to perform quota operations {quota_operation} : {e}")
        return 1
    finally:
        if smb_cluster_cleanup:
            smb_cleanup(installer_node, smb_shares, smb_cluster_id)
            client.exec_command(
                sudo=True,
                cmd=f"umount {cifs_mount_point}",
            )
            client.exec_command(sudo=True, cmd=f"rm -rf {cifs_mount_point}")

        client.exec_command(
            sudo=True,
            cmd=f"umount {mount_point}",
        )
        client.exec_command(sudo=True, cmd=f"rm -rf {mount_point}")

        return 0


