from smb_operations import (
    deploy_smb_service_imperative,
    smb_cifs_mount,
    smb_cleanup,
    smbclient_check_shares,
)

from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Deploy samba with auth_mode 'user' using imperative style(CLI Commands)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get config
    config = kw.get("config")

    # Get cephfs volume
    cephfs_vol = config.get("cephfs_volume", "cephfs")

    # Get smb subvloume group
    smb_subvol_group = config.get("smb_subvolume_group", "smb")

    # Get smb subvloumes
    smb_subvols = config.get("smb_subvolumes", ["sv1", "sv2"])

    # Get smb subvolume mode
    smb_subvolume_mode = config.get("smb_subvolume_mode", "0777")

    # Get smb cluster id
    smb_cluster_id = config.get("smb_cluster_id", "smb1")

    # Get auth_mode
    auth_mode = config.get("auth_mode", "user")

    # Get domain_realm
    domain_realm = config.get("domain_realm", None)

    # Get custom_dns
    custom_dns = config.get("custom_dns", None)

    # Get smb user name
    smb_user_name = config.get("smb_user_name", "user1")

    # Get smb user password
    smb_user_password = config.get("smb_user_password", "passwd")

    # Get smb shares
    smb_shares = config.get("smb_shares", ["share1", "share2"])

    # Get smb path
    path = config.get("path", "/")

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get smb nodes
    smb_nodes = ceph_cluster.get_nodes("smb")

    # Get client node
    client = ceph_cluster.get_nodes(role="client")[0]

    # Get cifs mount point
    cifs_mount_point = config.get("cifs_mount_point", "/mnt/smb")

    # Get system users
    system_users = config.get("system_users")
    user1 = system_users.get("user1")
    user2 = system_users.get("user2")

    # Get file name
    file_name = config.get("file_name")

    try:
        # deploy smb services
        deploy_smb_service_imperative(
            installer,
            cephfs_vol,
            smb_subvol_group,
            smb_subvols,
            smb_subvolume_mode,
            smb_cluster_id,
            auth_mode,
            smb_user_name,
            smb_user_password,
            smb_shares,
            path,
            domain_realm,
            custom_dns,
        )

        # Check smb share using smbclient
        smbclient_check_shares(
            smb_nodes,
            client,
            smb_shares,
            smb_user_name,
            smb_user_password,
            auth_mode,
            domain_realm,
        )

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

        # Create a file on Mount point
        cmd = f"touch {cifs_mount_point}/{file_name}"
        client.exec_command(cmd=cmd, sudo=True)

        # Add Test1 and Test2 User
        cmd = f"useradd {user1} && useradd {user2}"
        client.exec_command(cmd=cmd, sudo=True)

        # Change the owner of file as user
        cmd = f"chown {user1} {cifs_mount_point}/{file_name}"
        client.exec_command(cmd=cmd, sudo=True)

        # Change the permission of file to Readonly
        cmd = f"chmod 444 {cifs_mount_point}/{file_name}"
        client.exec_command(cmd=cmd, sudo=True)

        # Try removing read only file from different user
        cmd = f'su -c "rm -rvf {cifs_mount_point}/{file_name}" {user2}'
        _, out = client.exec_command(cmd=cmd, sudo=True, check_ec=False)
        if "Permission denied" in str(out):
            log.info(f"Expected: User {user2} cannot remove readonly file")
        else:
            raise OperationFailedError("Unexpected: Readonly file has been removed")

    except Exception as e:
        log.error(f"Failed to deploy samba with auth_mode {auth_mode} : {e}")
        return 1
    finally:
        for user in system_users.values():
            client.exec_command(sudo=True, cmd=f"userdel -rf {user}")
        client.exec_command(
            sudo=True,
            cmd=f"umount {cifs_mount_point}",
        )
        client.exec_command(
            sudo=True,
            cmd=f"rm -rf {cifs_mount_point}",
        )
        smb_cleanup(installer, smb_shares, smb_cluster_id)
    return 0
