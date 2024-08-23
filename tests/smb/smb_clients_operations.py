from threading import Thread
from time import sleep

from smb_operations import (
    clients_cleanup,
    deploy_smb_service_declarative,
    smb_cifs_mount,
    smb_cleanup,
    win_mount,
)

from cli.exceptions import ConfigError
from cli.utilities.utils import create_files, perform_lookups, remove_files, rename_file
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Deploy samba with auth_mode 'user' using imperative style(CLI Commands)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get config
    config = kw.get("config")

    # Check mandatory parameter file_type
    if not config.get("file_type"):
        raise ConfigError("Mandatory config 'file_type' not provided")

    # Get spec file type
    file_type = config.get("file_type")

    # Check mandatory parameter spec
    if not config.get("spec"):
        raise ConfigError("Mandatory config 'spec' not provided")

    # Get smb spec
    smb_spec = config.get("spec")

    # Get smb spec file mount path
    file_mount = config.get("file_mount", "/tmp")

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get smb nodes
    smb_nodes = ceph_cluster.get_nodes("smb")

    # Get smb subvolume mode
    smb_subvolume_mode = config.get("smb_subvolume_mode", "0777")

    # Get window mount point
    mount_point = config.get("mount_point", "Z:")

    # Client operations
    operations = config.get("operations")

    # Get file count
    file_count = config.get("file_count", 3)

    # Get Windows client flag
    windows_client = config.get("windows_client", True)

    # Get smb service value from spec file
    smb_shares = []
    smb_subvols = []
    for spec in smb_spec:
        if spec["resource_type"] == "ceph.smb.cluster":
            smb_cluster_id = spec["cluster_id"]
            auth_mode = spec["auth_mode"]
            if "domain_settings" in spec:
                domain_realm = spec["domain_settings"]["realm"]
            else:
                domain_realm = None
        elif spec["resource_type"] == "ceph.smb.usersgroups":
            smb_user_name = spec["values"]["users"][0]["name"]
            smb_user_password = spec["values"]["users"][0]["password"]
        elif spec["resource_type"] == "ceph.smb.join.auth":
            smb_user_name = spec["auth"]["username"]
            smb_user_password = spec["auth"]["password"]
        elif spec["resource_type"] == "ceph.smb.share":
            cephfs_vol = spec["cephfs"]["volume"]
            smb_subvol_group = spec["cephfs"]["subvolumegroup"]
            smb_subvols.append(spec["cephfs"]["subvolume"])
            smb_shares.append(spec["share_id"])

    # Create windows clients obj
    if windows_client:
        clients = []
        for client in setup_windows_clients(config.get("windows_clients")):
            clients.append(client)
    else:
        clients = ceph_cluster.get_nodes(role="client")

    try:
        # deploy smb services
        deploy_smb_service_declarative(
            installer,
            cephfs_vol,
            smb_subvol_group,
            smb_subvols,
            smb_cluster_id,
            smb_subvolume_mode,
            file_type,
            smb_spec,
            file_mount,
        )

        # Mount samba share
        if windows_client:
            win_mount(
                clients,
                mount_point,
                smb_nodes[0].ip_address,
                smb_shares[0],
                smb_user_name,
                smb_user_password,
            )
        else:
            smb_cifs_mount(
                smb_nodes[0],
                client[0],
                smb_shares[0],
                smb_user_name,
                smb_user_password,
                auth_mode,
                domain_realm,
                mount_point,
            )

        # Client Operations
        thread_operations = []
        for client, operation in operations.items():
            if operation == "create_file":
                thread_operations.append(
                    Thread(
                        target=create_files,
                        args=(
                            clients[int(client[-2:]) - 1],
                            mount_point,
                            file_count,
                            windows_client,
                        ),
                    )
                )
            elif operation == "perform_lookups":
                thread_operations.append(
                    Thread(
                        target=perform_lookups,
                        args=(
                            clients[int(client[-2:]) - 1],
                            mount_point,
                            file_count,
                            windows_client,
                        ),
                    )
                )
            elif operation == "rename_file":
                thread_operations.append(
                    Thread(
                        target=rename_file,
                        args=(
                            clients[int(client[-2:]) - 1],
                            mount_point,
                            file_count,
                            windows_client,
                        ),
                    )
                )
            elif operation == "remove_files":
                thread_operations.append(
                    Thread(
                        target=remove_files,
                        args=(
                            clients[int(client[-2:]) - 1],
                            mount_point,
                            file_count,
                            windows_client,
                        ),
                    )
                )

        # start opertaion on each client
        for thread_operation in thread_operations:
            thread_operation.start()
            sleep(0.5)

        # Wait to complete operations
        for thread_operation in thread_operations:
            thread_operation.join()
    except Exception as e:
        log.error(f"Samba operation failed: {e}")
        return 1
    finally:
        clients_cleanup(
            clients,
            mount_point,
            smb_nodes[0].ip_address,
            smb_shares[0],
            smb_user_name,
            smb_user_password,
            windows_client,
        )
        smb_cleanup(installer, smb_shares, smb_cluster_id)
    return 0
