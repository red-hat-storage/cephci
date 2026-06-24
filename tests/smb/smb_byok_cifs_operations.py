import time

from smb_operations import (
    check_smb_cluster,
    get_smb_shares,
    remove_smb_share,
    smb_cifs_mount,
    smb_cleanup,
    verify_smb_service,
)
from cli.exceptions import OperationFailedError
from tests.smb.smb_operations import ceph_smb_show
from utility.log import Log

log = Log(__name__)


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

    # Get smb standalone user names
    smb_user1_name = config.get("smb_user1_name")
    smb_user2_name = config.get("smb_user2_name")
    smb_user3_name = config.get("smb_user3_name")

    # Check smb cluster
    check_smb_cluster(installer_node, smb_cluster_id)

    # Check smb service
    verify_smb_service(installer_node, service_name="smb")

    # Get smb shares
    smb_shares = get_smb_shares(installer_node, smb_cluster_id)
    log.info("smb_shares: {}".format(smb_shares))
    print(type(smb_shares))

    # Get domain_realm
    domain_realm = config.get("domain_realm", None)

    # Get auth mode
    cluster_resource = ceph_smb_show(installer_node, smb_resource="ceph.smb.cluster")
    authmode = cluster_resource["authmode"]

    # Get user_group name and password
    users_groups = ceph_smb_show(ceph_smb_show, smb_resource="ceph.smb.usersgroups")
    if users_groups["values"]["users"]["name"] == smb_user1_name:
        smb_user1_password = users_groups["values"]["users"]["password"]
    if users_groups["values"]["users"]["name"] == smb_user2_name:
        smb_user2_password = users_groups["values"]["users"]["password"]
    if users_groups["values"]["users"]["name"] == smb_user3_name:
        smb_user3_password = users_groups["values"]["users"]["password"]

    # Get byok operations to perform
    byok_operation = config.get("byok_operation")

    # Get kernel mount point
    mount_point = config.get("mount_point", "/mnt/mount")

    # Get CIFS mount point
    cifs_mount_point = config.get("cifs_mount_point", "/mnt/smb")

    # Share 2 CIFS mount point
    share2_mount_point = cifs_mount_point + "share2"

    # Share 3 CIFS mount point
    share3_mount_point = cifs_mount_point + "share3"

    # # Get sub directory path
    # sub_dir = config.get("sub_dir", "/volumes/smb/sv1")

    # Get cleanup value, True if cleanup has to be done, False by default
    smb_cluster_cleanup = config.get("smb_cluster_cleanup", False)

    # Get mount cleanup value, True if cleanup has to be done, False by default
    mount_cleanup = config.get("mount_cleanup", False)

    try:
        if byok_operation == "test_cifs_mount":
            log.info("Mount encrypted share on {}".format(cifs_mount_point))
            # Mount smb share with cifs
            smb_cifs_mount(
                installer_node,
                client,
                smb_shares[0],
                smb_user1_name,
                smb_user1_password,
                authmode,
                domain_realm,
                cifs_mount_point,
            )

        elif byok_operation == "test_io_cifs_mount":
            rc = client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={cifs_mount_point}/file1 bs=600M count=1",
                long_running=True,
            )
            time.sleep(5)
            if rc:
                raise OperationFailedError(f"Write operation failed with : {rc}")

        elif byok_operation == "test_encrypted_and_normal_shares":
            rc = client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={cifs_mount_point}/file3 bs=100M count=1",
                long_running=True,
            )
            if rc:
                raise OperationFailedError(f"Write operation failed with: {rc}")
            time.sleep(5)
            # Mount smb normal share2 with cifs
            smb_cifs_mount(
                installer_node,
                client,
                smb_shares[1],
                smb_user2_name,
                smb_user2_password,
                authmode,
                domain_realm,
                share2_mount_point,
            )
            rc = client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={share2_mount_point}/file4 bs=100M count=1",
                long_running=True,
            )
            if rc:
                raise OperationFailedError(f"Write operation failed with : {rc}")
            time.sleep(5)

        elif byok_operation == "test_multiple_encrypted_shares":
            # Mount smb normal share2 with cifs
            smb_cifs_mount(
                installer_node,
                client,
                smb_shares[2],
                smb_user3_name,
                smb_user3_password,
                authmode,
                domain_realm,
                share3_mount_point,
            )
            rc = client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={share3_mount_point}/file5 bs=100M count=1",
                long_running=True,
            )
            if rc:
                raise OperationFailedError(f"Write operation failed with : {rc}")
            time.sleep(5)

        elif byok_operation == "test_delete_encrypted_share":
            remove_smb_share(
                installer_node, [smb_shares[0], smb_shares[2]], smb_cluster_id
            )
            # Remove subvolumes
            subvols = ["sv1", "sv3"]
            for sv in subvols:
                installer_node.exec_command(
                    sudo=True,
                    cmd=f"cephadm shell -- ceph fs subvolume rm cephfs {sv} --group_name smb",
                )
            ls = get_smb_shares(installer_node, smb_cluster_id)
            if smb_shares[0] in ls or smb_shares[2] in ls:
                raise OperationFailedError("Encrypted shares not deleted")

    except Exception as e:
        log.error(f"Failed to perform byok operations {byok_operation} : {e}")
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
            client.exec_command(sudo=True, cmd=f"rm -rf {share3_mount_point}")
            smb_cleanup(installer_node, smb_shares, smb_cluster_id)
            client.exec_command(
                sudo=True, cmd=f"umount {share3_mount_point}", long_running=True
            )
            client.exec_command(sudo=True, cmd=f"rm -rf {share2_mount_point}")
            smb_cleanup(installer_node, smb_shares, smb_cluster_id)

            client.exec_command(
                sudo=True, cmd=f"umount {mount_point}", long_running=True
            )
            client.exec_command(
                sudo=True, cmd=f"rm -rf {mount_point}", long_running=True
            )
        if mount_cleanup:
            client.exec_command(sudo=True, cmd=f"rm -rf {cifs_mount_point}/*")
            # TO DO : uncomment once the multiple share access issue is resolved
            client.exec_command(sudo=True, cmd=f"rm -rf {share2_mount_point}/*")
            client.exec_command(sudo=True, cmd=f"rm -rf {share3_mount_point}/*")

    return 0
