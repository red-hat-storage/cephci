import yaml

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError
from cli.utilities.packages import Package
from tests.nfs.nfs_operations import create_nfs_via_file_and_verify, log
from tests.nfs.test_nfs_multiple_operations_for_upgrade import (
    create_file,
    lookup_in_directory,
)
from utility.gklm_client.gklm_client import GklmClient


def get_enctag(
    gklm_client, created_client_data, gkml_client_name, gklm_cert_alias, gklm_user, cert
):
    """
    Initialize a GKLM client, assign a certificate, and create a symmetric key object for encryption/decryption.

    Args:
        gklm_client: Instance of the GKLM REST client.
        created_client_ Dictionary containing the newly created GKLM client data.
        gkml_client_name: Name of the GKLM client entity to be created.
        gklm_cert_alias: Alias under which the certificate will be stored in GKLM.
        gklm_user: User to be assigned access to the symmetric key.
        cert: Certificate (PEM format) to be associated with the client.

    Returns:
        str: The UUID of the newly created symmetric key object (enctag).

    Raises:
        Exception: If any step fails, logs the error and re-raises.
    """
    try:
        log.info(
            f"Assigning certificate '{gklm_cert_alias}' to GKLM client '{gkml_client_name}'"
        )
        assign_cert_data = gklm_client.clients.assign_client_certificate(
            client_name=gkml_client_name, cert_pem=cert, alias=gklm_cert_alias
        )
        log.info(
            f"Successfully created GKLM client {created_client_data} and assigned certificate {assign_cert_data}"
        )

        log.info(
            f"Creating symmetric key object for client '{gkml_client_name}', alias prefix 'AUT'"
        )
        symmetric_data = gklm_client.objects.create_symmetric_key_object(
            number_of_objects=1,
            client_name=gkml_client_name,
            alias_prefix_name="AUT",
            cryptoUsageMask="Encrypt,Decrypt",
        )
        enctag = symmetric_data["id"]
        log.info(
            f"Created symmetric key object with UUID {enctag} for client '{gkml_client_name}'"
        )

        log.info(
            f"Assigning user '{gklm_user}' to GKLM client '{gkml_client_name}' for KMIP access"
        )
        gklm_client.clients.assign_users_to_generic_kmip_client(
            client_name=gkml_client_name, users=[gklm_user]
        )
        log.info(f"User '{gklm_user}' successfully assigned to client")

        return enctag

    except Exception as e:
        log.error(
            f"Failed to initialize GKLM client '{gkml_client_name}' or assign certificate '{gklm_cert_alias}': {e}"
        )
        raise


def validate_enc_for_nfs_export_via_fuse(client, fuse_mount, nfs_mount):
    """
    Validate that files are visible in the NFS mount but not in the corresponding FUSE mount,
    ensuring that encryption is correctly applied to the NFS export.

    Args:
        client: Node object where mounts exist.
        fuse_mount: FUSE mountpoint path (should NOT show files).
        nfs_mount: NFS mountpoint path (should show files).

    Raises:
        ConfigError: If files are missing in NFS or present in FUSE (indicating a test failure).
    """
    test_files = ["test_file.txt", "test_file_name.txt"]

    log.info("Creating test files on NFS export for validation")
    for filename in test_files:
        create_file(client, nfs_mount, filename)

    log.info("Verifying files are present in NFS mount")
    nfs_files = lookup_in_directory(client, nfs_mount).strip().split("\n")
    for filename in test_files:
        if filename not in nfs_files:
            log.error(f"Test file '{filename}' not found in NFS mount {nfs_mount}")
            raise ConfigError(
                f"Test file '{filename}' missing in NFS mount {nfs_mount}; expected: {test_files}"
            )
        log.info(f"Test file '{filename}' found in NFS mount {nfs_mount}")

    log.info("Verifying files are NOT present in FUSE mount (expected if encrypted)")
    fuse_files = lookup_in_directory(client, fuse_mount).strip().split("\n")
    for filename in test_files:
        if filename in fuse_files and len(fuse_files) == len(test_files):
            log.error(
                f"Test file '{filename}' found in FUSE mount {fuse_mount}, indicating encryption failure"
            )
            raise ConfigError(
                f"Test file '{filename}' unexpectedly present in FUSE mount {fuse_mount}, possibly unencrypted"
            )
        log.info(
            f"Test file '{filename}' not found in FUSE mount {fuse_mount} (expected due to encryption)"
        )
    log.info("Encryption validation completed: NFS visible, FUSE not visible")


def create_nfs_instance_for_byok(
    installer, nfs_node, nfs_name, kmip_host_list, rsa_key, cert, ca_cert
):
    """
    Create an NFS Ganesha service instance with BYOK (Bring Your Own Key) KMIP configuration,
    using the provided certificates and key material.

    Args:
        installer: Node where Ceph orchestrator commands are run.
        nfs_node: NFS Ganesha server node.
        nfs_name: Name for the new NFS service.
        kmip_host_list: List of KMIP server endpoints (host:port).
        rsa_key: RSA private key (PEM format).
        cert: Client certificate (PEM format).
        ca_cert: CA certificate (PEM format).

    Raises:
        Exception: If cluster or NFS service creation fails, logs the error and re-raises.
    """
    log.info(
        f"Creating NFS Ganesha service '{nfs_name}' with BYOK/KMIP security configuration from node {nfs_node.hostname}"
    )
    nfs_cluster_dict = {
        "service_type": "nfs",
        "service_id": nfs_name,
        "placement": {"host_pattern": nfs_node.hostname},
        "spec": {
            "kmip_cert": "|\n" + cert.rstrip("\\n"),
            "kmip_key": "|\n" + rsa_key.rstrip("\\n"),
            "kmip_ca_cert": "|\n" + ca_cert.rstrip("\\n"),
            "kmip_host_list": [kmip_host_list],
        },
    }
    log.debug(f"NFS service spec: {nfs_cluster_dict}")

    create_nfs_via_file_and_verify(
        installer_node=installer, nfs_objects=[nfs_cluster_dict], timeout=300
    )
    log.info("NFS Ganesha BYOK service creation successful")


def setup_gklm_infrastructure(
    nfs_nodes, gklm_ip, gklm_node_password, gklm_hostname, gklm_node_username
):
    """
    Prepare cluster nodes for GKLM integration: install sshpass, set up passwordless SSH,
    and ensure hostname/IP resolution is bidirectional between NFS nodes and GKLM server.

    Args:
        nfs_nodes: List of node objects in the NFS cluster.
        gklm_ip: IP address of the GKLM server.
        gklm_password: Password for the node_username on the GKLM server.
        gklm_hostname: Hostname of the GKLM server.
        node_username: Username for SSH access to the GKLM server.

    Returns:
        Node: The execution node (first in nfs_nodes).

    Raises:
        Exception: If any step fails, logs the error and re-raises.
    """
    log.info("Setting up GKLM requirments")
    exe_node = nfs_nodes[0]
    log.info(
        f"Installing sshpass on node {exe_node.hostname} for non-interactive SSH to GKLM"
    )
    Package(exe_node).install("sshpass")

    log.info(
        f"Setting up passwordless SSH from node {exe_node.hostname} to GKLM server {gklm_ip} as {gklm_node_username}"
    )
    cmd = f"sshpass -p {gklm_node_password} ssh-copy-id {gklm_node_username}@{gklm_ip}"
    Ceph(exe_node).execute(cmd)
    log.info(
        f"Passwordless SSH established to GKLM server {gklm_ip} as user {gklm_node_username}"
    )

    for node in nfs_nodes:
        log.info(
            f"Updating /etc/hosts on NFS node {node.hostname} with GKLM server {gklm_hostname} at {gklm_ip}"
        )
        cmd = (
            rf"gklm_ip={gklm_ip}; gklm_hostname={gklm_hostname}; "
            rf'sudo sed -i -e "/$gklm_hostname\>/d" -e "/^$gklm_ip\>/d" /etc/hosts && '
            rf'echo "$gklm_ip $gklm_hostname" | sudo tee -a /etc/hosts'
        )
        out = Ceph(node).execute(sudo=True, cmd=cmd)
        log.info(
            f"Updated /etc/hosts on {node.hostname} with GKLM entry. Result: {out}"
        )

        log.info(
            f"Updating /etc/hosts on GKLM server with NFS node {node.hostname} at {node.ip_address}"
        )
        cmd = (
            rf"sshpass -p {gklm_node_password} ssh -o StrictHostKeyChecking=no {gklm_node_username}@{gklm_ip} "
            rf'"gklm_ip={node.ip_address}; gklm_hostname={node.hostname}; '
            rf'sudo sed -i -e "/$gklm_hostname\>/d" -e "/^$gklm_ip\>/d" /etc/hosts && '
            rf'echo "$gklm_ip $gklm_hostname" | sudo tee -a /etc/hosts"'
        )
        out = Ceph(exe_node).execute(sudo=True, cmd=cmd)
        log.info(
            f"Updated /etc/hosts on GKLM server with NFS node {node.hostname} entry. Result: {out}"
        )

    log.info(
        "GKLM infrastructure setup completed: sshpass installed, SSH keys exchanged, hostnames synchronized"
    )
    return exe_node


def get_gklm_ca_certificate(
    gklm_ip, gklm_node_password, gklm_node_username, exe_node, gklm_rest_client
):
    """
    Retrieve the GKLM CA certificate for use in the cluster:
    - Locate the target certificate by alias and usage.
    - Export to the GKLM filesystem if not already present.
    - Read and return the certificate contents.

    Args:
        gklm_ip: IP address of the GKLM server.
        gklm_password: Password for the node_username on the GKLM server.
        node_username: Username for SSH access to the GKLM server.
        exe_node: Node capable of executing remote commands.
        gklm_rest_client: GKLM REST client for certificate operations.

    Returns:
        str: The CA certificate (PEM) content.

    Raises:
        Exception: If any step fails, logs the error and re-raises.
    """
    log.info(
        "Locating target SSL server certificate (alias: self-signed-cert1, usage: SSLSERVER) in GKLM"
    )
    certs = gklm_rest_client.certificates.list_certificates()
    try:
        certificate_uuid_to_export = [
            x["uuid"]
            for x in certs
            if x.get("usage") == "SSLSERVER" and x.get("alias") == "self-signed-cert1"
        ][0]
        log.info(f"Found target certificate with UUID: {certificate_uuid_to_export}")
    except IndexError:
        log.error(
            "No certificate with alias 'self-signed-cert1' and usage 'SSLSERVER' found in GKLM"
        )
        raise

    log.info(
        "Checking if CA certificate is already exported at "
        "/opt/IBM/WebSphere/Liberty/products/sklm/data/export1/exportedCert"
    )
    file_check_cmd = (
        f"sshpass -p {gklm_node_password} ssh -o StrictHostKeyChecking=no {gklm_node_username}@{gklm_ip} "
        '\'[ -f /opt/IBM/WebSphere/Liberty/products/sklm/data/export1/exportedCert ] && echo "File exists" '
        '|| echo "File does not exist"\''
    )
    log.debug(f"Executing remote file check: {file_check_cmd}")
    is_cert_exists = Ceph(exe_node).execute(cmd=file_check_cmd)
    if isinstance(is_cert_exists, (list, tuple)) and len(is_cert_exists) >= 1:
        log.info(f"Remote file check result: {is_cert_exists[0]}")
    else:
        log.warning(f"Remote file check returned unexpected output: {is_cert_exists}")

    if is_cert_exists[0] == "File does not exist\n":
        log.info("CA certificate not found; initiating export via REST API")
        mkdir_cmd = (
            f"sshpass -p {gklm_node_password} ssh -o StrictHostKeyChecking=no {gklm_node_username}@{gklm_ip} "
            '"mkdir -p /opt/IBM/WebSphere/Liberty/products/sklm/data/export1"'
        )
        Ceph(exe_node).execute(cmd=mkdir_cmd)
        log.info("Created export directory for CA certificate")

        chmod_cmd = (
            f"sshpass -p {gklm_node_password} ssh -o StrictHostKeyChecking=no {gklm_node_username}@{gklm_ip} "
            '"chmod 777 /opt/IBM/WebSphere/Liberty/products/sklm/data/export1"'
        )
        Ceph(exe_node).execute(sudo=True, cmd=chmod_cmd)
        log.info("Set required permissions on export directory")

        gklm_rest_client.certificates.export_certificate(
            uuid=certificate_uuid_to_export, file_name="export1/exportedCert"
        )
        log.info(
            f"Exported certificate (UUID {certificate_uuid_to_export}) from GKLM server"
        )
    else:
        log.info("CA certificate already exists at configured path; skipping export")

    log.info(
        "Fetching CA certificate contents from /opt/IBM/WebSphere/Liberty/products/sklm/data/export1/exportedCert"
    )
    cert_fetch_cmd = (
        f"sshpass -p {gklm_node_password} ssh -o StrictHostKeyChecking=no {gklm_node_username}@{gklm_ip} "
        "'cat /opt/IBM/WebSphere/Liberty/products/sklm/data/export1/exportedCert'"
    )
    ca_cert = Ceph(exe_node).execute(cmd=cert_fetch_cmd)[0]
    log.info("CA certificate successfully retrieved from GKLM server")
    return ca_cert


def pre_requisite_for_gklm_get_ca(
    nfs_nodes,
    gklm_ip,
    gklm_node_password,
    gklm_hostname,
    gklm_node_username,
    gklm_rest_client,
):
    """
    Orchestrate all prerequisites for GKLM integration:
    - Set up infrastructure (SSH, /etc/hosts).
    - Retrieve the CA certificate from GKLM.
    Handle exceptions and provide detailed logging.

    Args:
        nfs_nodes: List of node objects in the NFS cluster.
        gklm_ip: IP address of the GKLM server.
        gklm_password: Password for the node_username on the GKLM server.
        gklm_hostname: Hostname of the GKLM server.
        node_username: Username for SSH access to the GKLM server.
        gklm_rest_client: GKLM REST client for certificate operations.

    Returns:
        str: The CA certificate (PEM) content.

    Raises:
        Exception: If any step fails, logs the error and re-raises.
    """
    try:
        log.info("Starting GKLM infrastructure and CA certificate setup")
        exe_node = setup_gklm_infrastructure(
            nfs_nodes=nfs_nodes,
            gklm_ip=gklm_ip,
            gklm_node_password=gklm_node_password,
            gklm_hostname=gklm_hostname,
            gklm_node_username=gklm_node_username,
        )
        ca_cert = get_gklm_ca_certificate(
            gklm_ip=gklm_ip,
            gklm_node_password=gklm_node_password,
            gklm_node_username=gklm_node_username,
            exe_node=exe_node,
            gklm_rest_client=gklm_rest_client,
        )
        log.info("Successfully retrieved GKLM CA certificate")
        return ca_cert
    except Exception as e:
        log.error(f"Failed to setup prerequisites for GKLM: {e}")
        raise


def clean_up_gklm(gklm_rest_client, gkml_client_name, gklm_cert_alias):
    """
    Clean up GKLM test resources:
    - Deletes all symmetric key objects associated with the client.
    - Deletes the certificate using the provided alias.
    - Deletes the GKLM client.

    Args:
        gklm_rest_client: Instance of the GKLM REST client.
        gkml_client_name (str): Name of the GKLM client to delete.
        gklm_cert_alias (str): Certificate alias to delete.
    """
    log.info("Starting GKLM resource cleanup.")

    # Step 1: Delete symmetric keys associated with the client
    log.info("Retrieving symmetric key objects for client '%s'.", gkml_client_name)
    try:
        objects = gklm_rest_client.objects.list_client_objects(gkml_client_name)
        ids = [obj["uuid"] for obj in objects]
        log.info("Found %d symmetric key object(s) to delete.", len(ids))
    except Exception as e:
        log.error("Failed to retrieve symmetric key objects: %s", str(e))
        return

    for key_id in ids:
        try:
            log.debug("Deleting symmetric key object: %s", key_id)
            gklm_rest_client.objects.delete_object(key_id)
        except Exception as e:
            log.warning("Failed to delete symmetric key '%s': %s", key_id, str(e))
    if ids:
        log.info("Symmetric key objects deleted successfully.")
    else:
        log.info("No symmetric keys found to delete.")

    # Step 2: Delete the certificate associated with the client
    log.info("Deleting GKLM certificate alias: '%s'", gklm_cert_alias)
    try:
        gklm_rest_client.certificates.delete_certificate(gklm_cert_alias)
        log.info("Certificate alias '%s' deleted successfully.", gklm_cert_alias)
    except Exception as e:
        log.warning(
            "Failed to delete certificate alias '%s': %s", gklm_cert_alias, str(e)
        )

    # Step 3: Delete the GKLM client itself
    log.info("Deleting GKLM client: '%s'", gkml_client_name)
    try:
        gklm_rest_client.clients.delete_client(gkml_client_name)
        log.info("Client '%s' deleted successfully.", gkml_client_name)
    except Exception as e:
        log.warning("Failed to delete client '%s': %s", gkml_client_name, str(e))

    log.info("GKLM resource cleanup completed.")


def load_gklm_config(custom_data, config, cephci_data):
    """
    Load GKLM parameters in this order of precedence:
      1. explicit --custom-config list items
      2. --custom-config-file YAML
      3. cephci_data['gklm_config'][cloud_type] (with baremetal → openstack fallback)
      4. Raises ConfigError if required keys are still missing

    Args:
        custom_data (dict): {'custom-config': [], 'custom-config-file': None}
        config (dict): CLI config, contains 'cloud-type'
        cephci_data (dict): global cephci config, contains 'gklm_config' section

    Returns:
        dict: GKLM connection parameters

    Raises:
        ConfigError: If GKLM data is missing after all lookup methods
    """
    # 1. Defaults placeholder (no valid defaults; will validate later)
    merged = {}

    # 2. Attempt load from cephci_data using cloud type
    cloud_type = config.get("cloud-type")
    lookup_type = "openstack" if cloud_type == "baremetal" else cloud_type
    cloud_gklm = cephci_data.get("gklm_config", {}).get(lookup_type, {})
    if cloud_gklm:
        merged.update(cloud_gklm)
        log.info(
            "Loaded GKLM config from cephci_data for cloud '%s': %s",
            lookup_type,
            cloud_gklm,
        )
    else:
        log.debug("No GKLM config in cephci_data for cloud '%s'", lookup_type)

    # 3. Override from YAML file if provided
    yaml_file = custom_data.get("custom-config-file")
    if yaml_file:
        try:
            with open(yaml_file) as f:
                data = yaml.safe_load(f).get("gklm", {})
            merged.update({k: data[k] for k in data if k in data})
            log.info("Loaded GKLM config from file '%s': %s", yaml_file, data)
        except Exception as e:
            log.error("Failed to load GKLM config file '%s': %s", yaml_file, e)
            raise ConfigError(f"Unable to parse custom-config-file: {e}")

    # 4. Override from --custom-config list
    for item in custom_data.get("custom-config", []):
        try:
            key, val = item.split("=", 1)
        except ValueError:
            log.warning("Skipping invalid custom-config entry: '%s'", item)
            continue

        if key in {
            "gklm_ip",
            "gklm_user",
            "gklm_password",
            "gklm_node_username",
            "gklm_node_password",
            "gklm_hostname",
        }:
            merged[key] = val
            log.info("Overrode GKLM config '%s' via custom-config: %s", key, val)
        else:
            log.warning("Unknown GKLM config key in custom-config: '%s'", key)

    # 5. Validate that all required keys are present
    required = [
        "gklm_ip",
        "gklm_user",
        "gklm_password",
        "gklm_node_username",
        "gklm_node_password",
        "gklm_hostname",
    ]
    missing = [k for k in required if not merged.get(k)]
    if missing:
        raise ConfigError(
            f"GKLM configuration incomplete. Missing keys: {missing}. "
            "Provide via cephci_data, custom-config-file, or --custom-config."
        )

    log.info("Final GKLM configuration: %s", {k: merged[k] for k in required})
    return merged


def nfs_byok_test_setup(byok_setup_params):
    """
    This method creates GKLM rest client instance,gets CA cert, generates Cert.pem and RSA-key
    and add them into NFS spec file and deploys NFS through spec file.
    It also creates GKLM client and key object and returns key as enctag
    Required params: GKLM params as dict in below format,
    byok_setup_params = {
        'gklm_ip':gklm_ip,
        'gklm_user':gklm_user,
        'gklm_password':gklm_password,
        'gklm_node_user':gklm_node_user,
        'gklm_node_password':gklm_node_password,
        'gklm_hostname':gklm_hostname,
        'gklm_client_name':gklm_client_name,
        'gklm_cert_alias':gklm_cert_alias,
        'gklm_ca_cert_alias':gklm_ca_cert_alias,
        'nfs_nodes':nfs_nodes,
        'installer':installer,
        'nfs_name':nfs_name
    }
    Returns:4 variables,
    byok setup status - 0 for pass, 1 for fail
    enctag - Key object,gklm_rest_client- GKLM rest client object for reuse,cert-cert.pem of nfs instance
    """
    gklm_ip = byok_setup_params["gklm_ip"]
    gklm_user = byok_setup_params["gklm_user"]
    gklm_password = byok_setup_params["gklm_password"]
    gklm_node_user = byok_setup_params["gklm_node_user"]
    gklm_node_password = byok_setup_params["gklm_node_password"]
    gklm_hostname = byok_setup_params["gklm_hostname"]
    gklm_client_name = byok_setup_params["gklm_client_name"]
    gklm_cert_alias = byok_setup_params["gklm_cert_alias"]
    nfs_nodes = byok_setup_params["nfs_nodes"]
    nfs_node = nfs_nodes[0]
    installer = byok_setup_params["installer"]
    nfs_name = byok_setup_params["nfs_name"]
    try:
        exe_node = setup_gklm_infrastructure(
            nfs_nodes=nfs_nodes,
            gklm_ip=gklm_ip,
            gklm_node_username=gklm_node_user,
            gklm_node_password=gklm_node_password,
            gklm_hostname=gklm_hostname,
        )
        gklm_rest_client = GklmClient(
            gklm_ip, user=gklm_user, password=gklm_password, verify=False
        )
        log.info(
            f"Initialized GKLM REST client for server {gklm_ip}, user {gklm_user}. "
            f"Client name: {gklm_client_name}, certificate alias: {gklm_cert_alias}"
        )

        log.info("Fetching RSA key and certificate from GKLM for NFS node credentials")
        # Request device-specific certificate and key from GKLM
        rsa_key, cert, _ = gklm_rest_client.certificates.get_certificates(
            subject={
                "common_name": nfs_node.hostname,
                "ip_address": nfs_node.ip_address,
            }
        )
        created_client_data = gklm_rest_client.clients.create_client(gklm_client_name)

        log.info("Creating symmetric key encryption tag in GKLM")
        enctag = get_enctag(
            gklm_rest_client,
            created_client_data,
            gklm_client_name,
            gklm_cert_alias,
            gklm_user,
            cert,
        )

        # ------------------- Prerequisites and Certificate Export -------------------
        # Ensure SSH access, hostname resolution, and certificate availability
        log.info(
            "Setting up SSH and CA certificate prerequisites on NFS and GKLM nodes"
        )
        ca_cert = get_gklm_ca_certificate(
            gklm_ip=gklm_ip,
            gklm_node_username=gklm_node_user,
            gklm_node_password=gklm_node_password,
            exe_node=exe_node,
            gklm_rest_client=gklm_rest_client,
        )
        log.info("CA certificate successfully retrieved \n %s", ca_cert)

        # ------------------- NFS Ganesha Instance Creation with BYOK -------------------
        log.info("Creating NFS Ganesha instance with BYOK/KMIP configuration")
        create_nfs_instance_for_byok(
            installer, nfs_node, nfs_name, gklm_ip, rsa_key, cert, ca_cert
        )
        return (0, enctag, gklm_rest_client, cert)
    except Exception as ex:
        log.error(ex)
        return (1, None, None, None)
