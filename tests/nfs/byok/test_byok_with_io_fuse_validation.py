import json

import yaml

from ceph.ceph import CommandFailed
from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from tests.nfs.byok.byok_tools import (
    create_nfs_instance_for_byok,
    get_enctag,
    pre_requisite_for_gklm_get_ca,
    validate_enc_for_nfs_export_via_fuse,
)
from tests.nfs.nfs_operations import (
    cleanup_custom_nfs_cluster_multi_export_client,
    fuse_mount_retry,
)
from tests.nfs.test_nfs_io_operations_during_upgrade import (
    create_export_and_mount_for_existing_nfs_cluster,
    perform_io_operations_in_loop,
)
from utility.gklm_client.gklm_client import GklmClient
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Main BYOK (Bring Your Own Key) NFS test runner.

    Coordinates end-to-end orchestration:
    1. Sets up GKLM (IBM Security Key Lifecycle Manager) clients and certificates.
    2. Creates an NFS Ganesha instance configured with BYOK/KMIP security.
    3. Mounts NFS exports and performs I/O operations to validate the setup.
    4. Cleans up all created resources (symmetric keys, certificates, NFS, mounts).

    Args:
        ceph_cluster: The deployed Ceph cluster object.
        **kw: Additional keyword arguments including configuration, client nodes, etc.

    Returns:
        int: 0 on success, with full log trace.
    """
    config = kw.get("config")

    # ------------------- Cluster Configuration Extraction -------------------
    # Fetch and sanitize required config values with defaults
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    clients = ceph_cluster.get_nodes("client")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs_byok")
    nfs_export = config.get("nfs_export", "/export_byok")
    version = config.get("nfs_version", "4.0")
    nfs_name = config.get("nfs_instance_name", "nfs_byok")
    export_num = config.get("total_export_num", 2)
    no_clients = int(config.get("clients", "1"))
    fs_name = config.get("fs_name", "cephfs")
    port = config.get("nfs_port", "2049")
    subvolume_group = "ganeshagroup"
    nfs_node = nfs_nodes[0]
    fs = fs_name

    # ------------------- Retrieving GKML DATA -------------------
    custom_data = kw.get("test_data", None)
    if not custom_data:
        raise CommandFailed(
            "This script requires either --custom-config <key>=<value> or "
            "--custom-config-file <file> with GKLM server information. "
            "No GKLM details were found in the configuration."
            "--custom-config-file <yaml file>"
            "gklm: \n"
            '   gklm_ip: "10.0.67.226"                  # GKLM server IP \n'
            '   gklm_user: "SKLMAdmin"                  # GKLM REST API user\n'
            '   gklm_password: "Db2@pass123"            # GKLM REST API password (PROTECT THIS)\n'
            '   gklm_node_username: "root"              # OS user for SSH access to GKLM node\n'
            '   gklm_node_password: "passwd"            # OS password for SSH access (PROTECT THIS)\n'
            '   gklm_hostname: "ceph-gklm-servers-nfs-ttjxzl-node2"\n'
        )

    gklm_ip = "0.0.0.0"
    gklm_user = "user"
    gklm_password = "password"
    gklm_node_username = "user"
    gklm_node_password = "password"
    gklm_hostname = "myhost"

    if custom_data.get("custom-config-file"):
        with open(custom_data["custom-config-file"], "r") as f:
            data = yaml.safe_load(f)
            gklm_ip = data["gklm"].get("gklm_ip")
            gklm_user = data["gklm"].get("gklm_user")
            gklm_password = data["gklm"].get("gklm_password")
            gklm_node_username = data["gklm"].get("gklm_node_username")
            gklm_node_password = data["gklm"].get("gklm_node_password")
            gklm_hostname = data["gklm"].get("gklm_hostname")

    if custom_data.get("custom-config"):
        # Update variables from the list
        for item in custom_data["custom-config"]:
            key, value = item.split("=", 1)
            if key == "gklm_ip":
                gklm_ip = value
            elif key == "gklm_user":
                gklm_user = value
            elif key == "gklm_password":
                gklm_password = value
            elif key == "gklm_node_username":
                gklm_node_username = value
            elif key == "gklm_node_password":
                gklm_node_password = value
            elif key == "gklm_hostname":
                gklm_hostname = value

    # ------------------- Client Availability Check -------------------
    if no_clients > len(clients):
        log.error(
            f"Test requires {no_clients} clients, but only {len(clients)} available"
        )
        raise ConfigError("Insufficient clients for test")

    log.info("BYOK NFS test run initiated")
    clients = clients[:no_clients]
    # ------------------- GKLM/Rest Client Setup -------------------
    # Initialize GKLM REST client for secure certificate and key management
    gkml_client_name = "automation"
    gklm_cert_alias = "cert2"
    client_export_mount_dict = None
    try:
        gklm_rest_client = GklmClient(
            gklm_ip, user=gklm_user, password=gklm_password, verify=False
        )
        log.info(
            f"Initialized GKLM REST client for server {gklm_ip}, user {gklm_user}. "
            f"Client name: {gkml_client_name}, certificate alias: {gklm_cert_alias}"
        )

        log.info("Fetching RSA key and certificate from GKLM for NFS node credentials")
        # Request device-specific certificate and key from GKLM
        rsa_key, cert, _ = gklm_rest_client.certificates.get_certificates(
            subject={
                "common_name": nfs_node.hostname,
                "ip_address": nfs_node.ip_address,
            }
        )

        # Register a new symmetric key encryption object in GKLM for BYOK
        log.info(
            f"Creating GKLM client '{gkml_client_name}' and assigning certificate '{gklm_cert_alias}'"
        )
        created_client_data = gklm_rest_client.clients.create_client(gkml_client_name)
        log.info("Creating symmetric key encryption tag in GKLM")

        enctag = get_enctag(
            gklm_rest_client,
            created_client_data,
            gkml_client_name,
            gklm_cert_alias,
            gklm_user,
            cert,
        )

        # ------------------- Prerequisites and Certificate Export -------------------
        # Ensure SSH access, hostname resolution, and certificate availability
        log.info(
            "Setting up SSH and CA certificate prerequisites on NFS and GKLM nodes"
        )
        ca_cert = pre_requisite_for_gklm_get_ca(
            gklm_ip=gklm_ip,
            nfs_nodes=nfs_nodes,
            gklm_password=gklm_node_password,
            gklm_hostname=gklm_hostname,
            node_username=gklm_node_username,
            gklm_rest_client=gklm_rest_client,
        )
        log.info(f"CA certificate successfully retrieved \n {ca_cert}")

        # ------------------- Ceph NFS Subvolume Group -------------------
        log.info(f"Creating CephFS subvolume group {subvolume_group} for NFS exports")
        Ceph(clients[0]).fs.sub_volume_group.create(
            volume=fs_name, group=subvolume_group
        )

        # ------------------- NFS Ganesha Instance Creation with BYOK -------------------
        log.info("Creating NFS Ganesha instance with BYOK/KMIP configuration")
        create_nfs_instance_for_byok(
            installer, nfs_node, nfs_name, gklm_ip, rsa_key, cert, ca_cert
        )

        # ------------------- Export Creation and Mounting -------------------
        log.info(
            f"Creating NFS exports for filesystem {fs_name}, "
            f"exporting {export_num} times to clients"
        )

        client_export_mount_dict = create_export_and_mount_for_existing_nfs_cluster(
            clients,
            nfs_export,
            nfs_mount,
            export_num,
            fs_name,
            nfs_name,
            fs,
            port,
            version=version,
            enctag=enctag,
            nfs_server=nfs_node.hostname,
        )

        # ------------------- IO Validation -------------------
        log.info("Starting I/O operations on mounted NFS exports to validate setup")
        perform_io_operations_in_loop(
            client_export_mount_dict=client_export_mount_dict,
            clients=clients,
            file_count=10,
            dd_command_size_in_M=20,
        )

        # ------------------- FUSE Mount Validation -------------------
        export_ls = Ceph(clients[0]).nfs.export.ls(nfs_name)
        log.info(f"Exports listed on NFS server {nfs_name}: {export_ls}")
        exports_data = {
            x: json.loads(Ceph(clients[0]).nfs.export.get(nfs_name, x, format="json"))
            for x in export_ls
        }
        for client in clients:
            if client_export_mount_dict[client].get("mount"):
                fuse_mount_retry(
                    client=client,
                    mount=client_export_mount_dict[client].get("mount")[0] + "_fuse",
                    extra_params=f'-r {exports_data[client_export_mount_dict[client].get("export")[0]]["path"]}',
                )
                validate_enc_for_nfs_export_via_fuse(
                    client=client,
                    fuse_mount=client_export_mount_dict[client].get("mount")[0]
                    + "_fuse",
                    nfs_mount=client_export_mount_dict[client].get("mount")[0],
                )
        return 0

    except Exception as e:
        log.error(f"BYOK testcase failed during setup or validation: {e}")
        raise
    finally:
        # ------------------- Cleanup: GKLM and NFS Resources -------------------
        log.info("Starting cleanup phase: removing all test resources")

        # GKLM Symmetric Key Objects
        log.info("Retrieving GKLM symmetric key object IDs for cleanup")
        ids = [
            obj["uuid"]
            for obj in gklm_rest_client.objects.list_client_objects(gkml_client_name)
        ]
        log.info(f"Found {len(ids)} symmetric key objects to remove")

        log.info("Deleting GKLM symmetric key objects")
        for id_ in ids:
            log.debug(f"Deleting object {id_}")
            gklm_rest_client.objects.delete_object(id_)
        log.info("GKLM symmetric key objects deleted")

        # GKLM Certificate
        log.info(f"Deleting GKLM certificate alias: {gklm_cert_alias}")
        gklm_rest_client.certificates.delete_certificate(gklm_cert_alias)

        # GKLM Client
        log.info(f"Deleting GKLM client: {gkml_client_name}")
        gklm_rest_client.clients.delete_client(gkml_client_name)
        log.info("GKLM client cleanup completed")

        # NFS Cluster and Mounts
        log.info("Starting NFS cluster and mount cleanup")
        cleanup_custom_nfs_cluster_multi_export_client(
            clients, nfs_mount, nfs_name, nfs_export, export_num
        )
        log.info("NFS cluster and mount cleanup completed")
        log.info("Unmounting and removing fuse mounts")
        for client in clients:
            for mount in [
                x + "_fuse" for x in client_export_mount_dict[client]["mount"]
            ]:
                client.exec_command(
                    sudo=True, cmd=f"rm -rf {mount}/*", long_running=True
                )
                if Unmount(client).unmount(mount):
                    raise OperationFailedError(
                        f"Failed to unmount nfs on {client.hostname}"
                    )
        log.info("cleaned Fuse mounts")
        log.info("All test resources have been cleaned up")
