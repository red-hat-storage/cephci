from smb_operations import (
    check_smb_cluster,
    get_smb_shares,
    smb_cifs_mount,
    smb_cleanup,
    verify_smb_service,
)

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

    # Get smb nodes
    smb_nodes = ceph_cluster.get_nodes("smb")

    # Get client node
    client = ceph_cluster.get_nodes(role="client")[0]

    # Get gRPC operations to perform
    grpc_operation = config.get("grpc_operation")

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

    # Get cifs mount point
    cifs_mount_point = config.get("cifs_mount_point", "/mnt/smb")

    # Get cleanup value, True if cleanup has to be done, False by default
    smb_cluster_cleanup = config.get("smb_cluster_cleanup", False)

    try:
        if grpc_operation == "describe_all_service_calls":
            cmd = (
                "cd sambacc && grpcurl -import-path sambacc/grpc/protobufs/ -proto control.proto "
                "describe SambaControl"
            )
            out = installer_node.exec_command(sudo=True, cmd=cmd)
            log.info("Describe Samba Control : {}".format(out))

        elif grpc_operation == "service_info":
            cmd = (
                f"cd sambacc && grpcurl -cacert /root/grpc_ca.ca -cert /root/grpc_cert.crt  -key /root/grpc_key.key "
                f"-import-path sambacc/grpc/protobufs/ "
                f"-proto control.proto  {installer_node.ip_address}:54445  SambaControl/Info"
            )
            out = installer_node.exec_command(sudo=True, cmd=cmd)
            log.info("Print Samba and container Info Samba Control : {}".format(out))

        elif grpc_operation == "status_when_no_clients":
            cmd = (
                f"cd sambacc && grpcurl -cacert /root/grpc_ca.ca -cert /root/grpc_cert.crt  -key /root/grpc_key.key "
                f"-import-path sambacc/grpc/protobufs/ "
                f"-proto control.proto  {installer_node.ip_address}:54445  SambaControl/Status"
            )
            out = installer_node.exec_command(sudo=True, cmd=cmd)
            log.info("grpcurl status : {}".format(out))

        elif grpc_operation == "status_with_linux_client":
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
            cmd = (
                f"cd sambacc && grpcurl -cacert /root/grpc_ca.ca -cert /root/grpc_cert.crt  -key /root/grpc_key.key "
                f"-import-path sambacc/grpc/protobufs/ "
                f"-proto control.proto  {installer_node.ip_address}:54445  SambaControl/Status"
            )
            out = installer_node.exec_command(sudo=True, cmd=cmd)
            log.info("grpcurl status : {}".format(out))
            client.exec_command(
                sudo=True,
                cmd=f"umount {cifs_mount_point}",
            )
            client.exec_command(sudo=True, cmd=f"rm -rf {cifs_mount_point}")
        elif grpc_operation == "kill_linux_client":
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
            cmd = (
                f"cd sambacc && grpcurl -cacert /root/grpc_ca.ca -cert /root/grpc_cert.crt  -key /root/grpc_key.key "
                f"-import-path sambacc/grpc/protobufs/ -proto control.proto "
                f'-d \'{{"ip_address": "{client.ip_address}"}}\' {installer_node.ip_address}:54445 '
                f"SambaControl/KillClientConnection"
            )
            out = installer_node.exec_command(sudo=True, cmd=cmd)
            log.info("grpcurl status : {}".format(out))
            client.exec_command(
                sudo=True,
                cmd=f"umount {cifs_mount_point}",
            )
            client.exec_command(sudo=True, cmd=f"rm -rf {cifs_mount_point}")

    except Exception as e:
        log.error(f"Failed to perform gRPC operations {grpc_operation} : {e}")
        return 1
    finally:
        if smb_cluster_cleanup:
            smb_cleanup(installer_node, smb_shares, smb_cluster_id)

    return 0
