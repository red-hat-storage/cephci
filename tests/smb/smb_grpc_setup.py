import yaml
from smb_operations import (
    add_port_to_firewalld,
    deploy_smb_service_declarative,
    smbclient_check_shares,
)

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

SMB_gRPC_PORT = 54445

log = Log(__name__)


class LiteralString(str):
    pass


def literal_presenter(dumper, data):
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


yaml.add_representer(LiteralString, literal_presenter)


def generate_self_signed_certificate_for_smb_node(server_node, client_node):
    """
    Generate self signed certificates for samba server and client node.

    Args:
        server_node   (obj):  samba server node obj
        client_node   (obj):  samba client node obj

    Returns:
        key, cert, ca  Tuple as strings
    """
    # Step 1: Generate CA private key
    server_node.exec_command(sudo=True, cmd="openssl genrsa -out grpc_ca.key 2048")

    # Step 2: Generate self-signed CA certificate
    server_node.exec_command(
        sudo=True,
        cmd=(
            "openssl req -x509 -noenc -key grpc_ca.key -days 1826 -out grpc_ca.ca "
            "-subj '/C=IN/ST=Karnataka/L=Bengaluru/O=Red Hat Inc/OU=Storage/CN=CA'"
        ),
    )

    # Step 3: Generate server key + CSR
    server_node.exec_command(
        sudo=True,
        cmd=(
            f"openssl req -new -noenc -out server_grpc_cert.csr -newkey rsa:2048 "
            f"-keyout server_grpc_key.key "
            f"-subj '/C=IN/ST=Karnataka/L=Bengaluru/O=Red Hat Inc/OU=Storage/CN={server_node.hostname}' "
            f"-addext 'subjectAltName = DNS:{server_node.hostname},IP:{server_node.ip_address}'"
        ),
    )

    # Step 4: Sign the server certificate
    server_node.exec_command(
        sudo=True,
        cmd=(
            "openssl x509 -req -in server_grpc_cert.csr -CA grpc_ca.ca -CAkey grpc_ca.key "
            "-CAcreateserial -out server_grpc_cert.crt -days 730 -copy_extensions copy"
        ),
    )

    # Step 5: Generate client key + CSR on client node
    server_node.exec_command(
        sudo=True,
        cmd=(
            f"openssl req -new -noenc -out grpc_cert.csr -newkey rsa:2048 "
            f"-keyout grpc_key.key "
            f"-subj '/C=IN/ST=Karnataka/L=Bengaluru/O=Red Hat Inc/OU=Storage/CN={client_node.hostname}' "
            f"-addext 'subjectAltName = DNS:{client_node.hostname},IP:{client_node.ip_address}'"
        ),
    )

    # Step 6: Sign the client certificate
    server_node.exec_command(
        sudo=True,
        cmd=(
            "openssl x509 -req -in grpc_cert.csr -CA grpc_ca.ca -CAkey grpc_ca.key "
            "-CAcreateserial -out grpc_cert.crt -days 730 -copy_extensions copy"
        ),
    )

    key = server_node.exec_command(sudo=True, cmd="cat server_grpc_key.key")[0]
    cert = server_node.exec_command(sudo=True, cmd="cat server_grpc_cert.crt")[0]
    ca = server_node.exec_command(sudo=True, cmd="cat grpc_ca.ca")[0]

    return key, cert, ca


def copy_self_signed_certificate_to_smb_client_node(server_node, client_node):
    """
    Generate self signed certificates for samba client node.

    Args:
        server_node   (obj):  samba server node obj
        client_node   (obj):  samba client node obj

    Returns:
        client_key, client_cert Tuple as strings
    """

    # Read client cert files from server node
    grpc_key = server_node.exec_command(sudo=True, cmd="cat grpc_key.key")[0]
    grpc_cert = server_node.exec_command(sudo=True, cmd="cat grpc_cert.crt")[0]
    grpc_ca = server_node.exec_command(sudo=True, cmd="cat grpc_ca.ca")[0]

    # Write client key to client node
    key_file = client_node.remote_file(
        sudo=True, file_name="grpc_key.key", file_mode="w+"
    )
    key_file.write(grpc_key)
    key_file.flush()
    key_file.close()

    # Write client cert to client node
    cert_file = client_node.remote_file(
        sudo=True, file_name="grpc_cert.crt", file_mode="w+"
    )
    cert_file.write(grpc_cert)
    cert_file.flush()
    cert_file.close()

    # Write CA cert to client node
    ca_file = client_node.remote_file(sudo=True, file_name="grpc_ca.ca", file_mode="w+")
    ca_file.write(grpc_ca)
    ca_file.flush()
    ca_file.close()

    client_key = client_node.exec_command(sudo=True, cmd="cat grpc_key.key")[0]
    client_cert = client_node.exec_command(sudo=True, cmd="cat grpc_cert.crt")[0]

    return client_key, client_cert


def install_grpcurl(smb_node):
    """Install grpcurl package"""
    log.info("install grpcurl package")
    wget_cmd = (
        "curl -LO "
        "https://github.com/fullstorydev/grpcurl/releases/download/v1.8.9/grpcurl_1.8.9_linux_x86_64.tar.gz"
    )

    tar_cmd = "tar -xvzf grpcurl_1.8.9_linux_x86_64.tar.gz"
    chmod_cmd = "chmod +x grpcurl"
    rename_cmd = "sudo mv grpcurl /usr/local/bin/"
    version_cmd = "grpcurl --version"
    out = smb_node.exec_command(sudo=True, cmd=wget_cmd)
    log.info(out)
    smb_node.exec_command(
        sudo=True,
        cmd=f"{tar_cmd} && {chmod_cmd} && {rename_cmd}",
    )
    out = smb_node.exec_command(
        sudo=True,
        cmd=version_cmd,
    )
    log.info(out)
    if "grpcurl" not in out[1]:
        raise Exception("grpcurl not installed")


def clone_the_samba_in_kubernetes_repo(node):
    """clone the repo on to the node.

    Args:
        node (obj): ceph node
    """
    log.info("cloning the https://github.com/samba-in-kubernetes/sambacc.git repo")
    git_clone_cmd = "git clone https://github.com/samba-in-kubernetes/sambacc.git"
    node.exec_command(cmd=git_clone_cmd, sudo=True)


def check_remotectl_service(smb_node):
    """Check if grpc remotectl service is running
    Args:
        smb_node (obj): samba installer server node obj
    """
    # Get grpc remotectl service
    cmd = "systemctl list-units --type=service | grep remotectl"
    out = smb_node.exec_command(sudo=True, cmd=cmd)[0].strip()
    log.info(out)

    if "remotectl" in out:
        log.info(f"gRPC remotectl service has been loaded: {out}")
    else:
        raise OperationFailedError(f"gRPC remotectl service has not been loaded {out}")

    if "remotectl" in out and "active" in out and "running" in out:
        log.info(
            f"gRPC remotectl service is running activetly: {out}, grpc smd cluster is created successfully"
        )
    else:
        raise OperationFailedError(
            f"gRPC remotectl service has been loaded but failed {out}"
        )


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

    # Get smb spec file mount path
    file_mount = config.get("file_mount", "/tmp")

    # Get smb subvolume mode
    smb_subvolume_mode = config.get("smb_subvolume_mode", "0777")

    # Get smb spec
    smb_spec = config.get("spec")
    log.info("smb_spec_log: {} ".format(smb_spec))

    # Get smb service value from spec file
    smb_shares = []
    smb_subvols = []
    for spec in smb_spec:
        if spec["resource_type"] == "ceph.smb.cluster":
            smb_cluster_id = spec["cluster_id"]
            auth_mode = spec["auth_mode"]
            cert_tls_credential_id = spec["remote_control"]["cert"]["ref"]
            key_tls_credential_id = spec["remote_control"]["key"]["ref"]
            cacert_tls_credential_id = spec["remote_control"]["ca_cert"]["ref"]
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

    # Get installer node
    installer_node = ceph_cluster.get_nodes(role="installer")[0]

    # Get smb nodes
    smb_nodes = ceph_cluster.get_nodes("smb")

    # Get client node
    client = ceph_cluster.get_nodes(role="client")[0]

    # Generate self signed certificates crt, cacrt, key for server node
    key, cert, ca = generate_self_signed_certificate_for_smb_node(
        installer_node, client
    )

    grpc_spec_tld_credential_dict = [
        {
            "resource_type": "ceph.smb.tls.credential",
            "tls_credential_id": cert_tls_credential_id,
            "credential_type": "cert",
            "value": LiteralString(cert.rstrip("\n")),
        },
        {
            "resource_type": "ceph.smb.tls.credential",
            "tls_credential_id": key_tls_credential_id,
            "credential_type": "cert",
            "value": LiteralString(key.rstrip("\n")),
        },
        {
            "resource_type": "ceph.smb.tls.credential",
            "tls_credential_id": cacert_tls_credential_id,
            "credential_type": "ca-cert",
            "value": LiteralString(ca.rstrip("\n")),
        },
    ]
    smb_spec.extend(grpc_spec_tld_credential_dict)
    log.info("smb_spec: {}".format(smb_spec))

    try:
        # deploy smb services
        deploy_smb_service_declarative(
            installer_node,
            cephfs_vol,
            smb_subvol_group,
            smb_subvols,
            smb_cluster_id,
            smb_subvolume_mode,
            file_type,
            smb_spec,
            file_mount,
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

        # Check grpc remotectl service on server node
        check_remotectl_service(installer_node)

        # Add GRPC port to the firewall allowlist
        add_port_to_firewalld(installer_node, SMB_gRPC_PORT)

        # Install grpcurl on client node
        install_grpcurl(client)

        # Clone samba in kubernetes repo in se
        clone_the_samba_in_kubernetes_repo(client)

        # Generate self-signed certificates crt, cacert, key for client node
        copy_self_signed_certificate_to_smb_client_node(installer_node, client)

    except Exception as e:
        log.error(f"Failed to deploy samba with auth_mode {auth_mode} : {e}")
        return 1
    return 0
