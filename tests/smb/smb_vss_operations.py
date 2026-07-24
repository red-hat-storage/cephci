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

    # Get snapshot
    snap = config.get("snapshot", "snap1")

    # Get smb path
    path = config.get("path", "/")

    # Get cifs mount point
    cifs_mount_point = config.get("cifs_mount_point", "/mnt/smb")

    # Get VSS operations to perform
    operations = config.get("operations")

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get smb nodes
    smb_nodes = ceph_cluster.get_nodes("smb")

    # Get client node
    client = ceph_cluster.get_nodes(role="client")[0]

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

        for operation in operations:
            # Check .snap Directory
            if operation == "access_snap":
                cmd = f"cd {cifs_mount_point}/.snap"
                out = client.exec_command(sudo=True, cmd=cmd)
                if "No such file or directory" in out[0]:
                    raise OperationFailedError(".snap directory is not enabled")
                else:
                    out = client.exec_command(
                        sudo=True, cmd=f"cd {cifs_mount_point}/.snap && pwd"
                    )
                    log.info(".snap directory is accessible, the path : {}".format(out))

            # Verify snapshot creation
            elif operation == "create_snapshot":
                cmd1 = (
                    f"cd {cifs_mount_point} && ceph fs subvolume snapshot create {cephfs_vol} {smb_subvols[0]} {snap} "
                    f"{smb_subvol_group}"
                )
                out1 = client.exec_command(sudo=True, cmd=cmd1)
                log.info("Created snapshot : {}".format(out1))
                out3 = client.exec_command(
                    sudo=True, cmd=f"cd {cifs_mount_point}/.snap && ls -al"
                )
                log.info("Snapshot created in .snap folder: {}".format(out3))

            # Verify list snapshots
            elif operation == "list_snapshot":
                cmd2 = (
                    f"cd {cifs_mount_point} && ceph fs subvolume snapshot ls {cephfs_vol} {smb_subvols[0]} "
                    f"{smb_subvol_group}"
                )
                out2, _ = client.exec_command(sudo=True, cmd=cmd2)
                if snap not in out2:
                    raise OperationFailedError("Snapshot is not listed")
                else:
                    log.info("Snapshots listed {}".format(out2))

            # Create file with content on share
            elif operation == "create_file":
                cmd = f"""cd {cifs_mount_point} && cat << EOF >test.txt
                            Hello!
                            """
                client.exec_command(sudo=True, cmd=cmd)
                cmd = f"cd {cifs_mount_point} && cat test.txt"
                out, _ = client.exec_command(sudo=True, cmd=cmd)

            # Update the file on share
            elif operation == "update_file":
                cmd3 = f"""cd {cifs_mount_point} && cat << EOF >test.txt
                            Hello! This is updated
                            """
                client.exec_command(sudo=True, cmd=cmd3)

            # Verify content of snapshot file and the file before update is same
            elif operation == "verify_snapshot_content":
                cmd4 = f"cd {cifs_mount_point}/.snap/*{snap}* && cat test.txt"
                out4, _ = client.exec_command(sudo=True, cmd=cmd4)
                if out4 != out:
                    raise OperationFailedError(
                        f"VSS feature not working. File content before editing {out}, "
                        f"File content of snapshot {out4}"
                    )
            # Delete snapshot
            elif operation == "delete_snapshot":
                cmd = (
                    f"cd {cifs_mount_point} && ceph fs subvolume snapshot rm {cephfs_vol} {smb_subvols[0]} {snap} "
                    f"{smb_subvol_group}"
                )
                out, _ = client.exec_command(sudo=True, cmd=cmd)
                log.info("Deleted snapshot : {}".format(snap))

    except Exception as e:
        log.error(f"Failed to deploy samba with auth_mode {auth_mode} : {e}")
        return 1
    finally:
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
