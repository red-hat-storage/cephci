from smb_operations import (
    check_smb_cluster,
    get_smb_shares,
    samba_kernel_mount,
    verify_smb_service,
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

    # Get byok operations to perform
    byok_operation = config.get("byok_operation")

    # Get kernel mount point
    mount_point = config.get("mount_point", "/mnt/mount")

    # Get sub directory path
    sub_dir = config.get("sub_dir", "/volumes/smb/sv1")

    # Get mount cleanup value, True if cleanup has to be done, False by default
    mount_cleanup = config.get("mount_cleanup", False)

    try:

        if byok_operation == "test_ceph_fuse_mount":
            log.info("mount share using kernel mount")
            samba_kernel_mount(client, mount_point, installer_node.ip_address, sub_dir)

        elif byok_operation == "test_io_ceph_fuse_mount":
            rc = client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={mount_point}/ceph_file1 bs=600M count=1",
                check_ec=False,
            )
            if rc == 0:
                raise OperationFailedError(
                    "Write operation should fail, but did not fail."
                )

        elif byok_operation == "test_file_ops_ceph_fuse_mount":
            rc = client.exec_command(
                sudo=True,
                cmd=f"mkdir {mount_point}/ceph_file1",
                check_ec=False,
            )
            if rc == 0:
                raise OperationFailedError(
                    "Write operation should fail, but did not fail."
                )

    except Exception as e:
        log.error(f"Failed to perform quota operations {byok_operation} : {e}")
        return 1
    finally:
        if mount_cleanup:
            client.exec_command(
                sudo=True, cmd=f"umount {mount_point}", long_running=True
            )
            client.exec_command(
                sudo=True, cmd=f"rm -rf {mount_point}", long_running=True
            )

    return 0
