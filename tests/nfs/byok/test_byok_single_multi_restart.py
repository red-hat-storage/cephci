from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.nfs.byok.byok_tools import (
    clean_up_gklm,
    create_multiple_nfs_instance_for_byok,
    create_nfs_instance_for_byok,
    get_enctag,
    get_gklm_ca_certificate,
    load_gklm_config,
    perform_io_operations_and_validate_fuse,
    setup_gklm_infrastructure,
)
from tests.nfs.nfs_operations import (
    cleanup_custom_nfs_cluster_multi_export_client,
    dynamic_cleanup_common_names,
    get_ganesha_info_from_container,
)
from tests.nfs.test_nfs_io_operations_during_upgrade import (
    create_export_and_mount_for_existing_nfs_cluster,
)
from utility.gklm_client.gklm_client import GklmClient
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


def validate_sighup(
    installer,
    nfs_node,
    nfs_name,
    gklm_rest_client,
    gkml_client_name,
    gklm_hostname,
    ca_cert,
    gklm_user,
    gklm_cert_alias_old,
    gklm_cert_alias_new,
):
    """
    Validates that the NFS Ganesha process receives a SIGHUP signal (reload) rather than a full restart
    when its certificate and key are updated via GKLM (Global Key Lifecycle Manager).
    This function performs the following steps:
    1. Retrieves the current Ganesha process PID from the NFS container.
    2. Generates a new certificate and key for the NFS node using GKLM.
    3. Deletes the old certificate from GKLM and assigns the new certificate to the GKLM client.
    4. Updates the NFS Ganesha instance with the new certificate and key.
    5. Retrieves the Ganesha process PID again to verify if the process was reloaded (SIGHUP) or restarted.
    6. Raises an OperationFailedError if the PID has changed, indicating a restart instead of a SIGHUP.
    Args:
        installer (list): List of installer objects used to interact with the NFS service.
        nfs_node (object): The NFS node object containing hostname and IP address information.
        nfs_name (str): The name of the NFS service instance.
        gklm_rest_client (object): GKLM REST client for certificate management operations.
        gkml_client_name (str): The GKLM client name to assign the new certificate.
        gklm_hostname (str): Hostname of the GKLM server.
        ca_cert (str): CA certificate used for secure communication.
    Returns:
        int: Returns 1 if the Ganesha PID could not be found before the update.
    Raises:
        OperationFailedError: If the Ganesha process PID changes after the update,
        indicating a restart instead of a SIGHUP.
    """
    log.info("getting ganesha PID before updating GKLM")
    ganesha_pid_berfore, container_info = get_ganesha_info_from_container(
        installer=installer, nfs_service_name=nfs_name, nfs_host_node=nfs_node
    )
    log.info(f"Ganesha PID: {ganesha_pid_berfore}, Container Info: {container_info}")

    if ganesha_pid_berfore is None:
        log.error("Ganesha PID not found")
        return 1

    log.info("Generating new certificate and key from GKLM")
    rsa_key_2, cert_2, _ = gklm_rest_client.certificates.get_certificates(
        subject={
            "common_name": nfs_node.hostname,
            "ip_address": nfs_node.ip_address,
        }
    )

    log.info("Deleting old certificate from GKLM and assigning new one")
    gklm_rest_client.certificates.delete_certificate(gklm_cert_alias_old)
    log.info(f"Successfully deleted GKLM client {gklm_cert_alias_old}")
    gklm_rest_client.clients.assign_users_to_generic_kmip_client(
        gkml_client_name, [gklm_user]
    )
    assign_cert_data = gklm_rest_client.clients.assign_client_certificate(
        client_name=gkml_client_name, cert_pem=cert_2, alias=gklm_cert_alias_new
    )

    log.info(f"Successfully assigned new certificate {assign_cert_data}")

    log.info("Updating NFS Ganesha instance with new cert and key")
    create_nfs_instance_for_byok(
        installer, nfs_node, nfs_name, gklm_hostname, rsa_key_2, cert_2, ca_cert
    )
    log.info("Updating NFS Ganesha instance with new cert and key - successful")

    ganesha_pid_after, container_info = get_ganesha_info_from_container(
        installer=installer, nfs_service_name=nfs_name, nfs_host_node=nfs_node
    )
    log.info(
        f"Ganesha PID after updating GKLM and NFS: {ganesha_pid_after}, Container Info: {container_info}"
    )

    if ganesha_pid_berfore != ganesha_pid_after:
        log.info("Ganesha PID has changed after updating GKLM and NFS")
        raise OperationFailedError(
            "Ganesha PID changed after Updating GKLM, Its a restart not a SIGHUP"
        )


def run(ceph_cluster, **kw):
    """
    Test BYOK (Bring Your Own Key) functionality for NFS Ganesha with GKLM integration.
    This test performs comprehensive validation of NFS Ganesha encryption using customer-managed
    keys from GKLM (Gemalto Key Lifecycle Manager). It supports both single-cluster and
    multi-cluster deployments with optional SIGHUP testing for certificate rotation.
    Args:
        ceph_cluster: Ceph cluster object containing node information
        **kw: Keyword arguments containing:
            - config: Test configuration parameters
            - test_data: Custom test data including GKLM connection details
    Configuration Parameters:
        - nfs_mount (str): NFS mount point path (default: "/mnt/nfs_byok")
        - nfs_export (str): NFS export path (default: "/export_byok")
        - nfs_instance_name (str): NFS service name (default: "nfs_byok")
        - total_export_num (int): Number of exports to create (default: 2)
        - clients (int): Number of client nodes to use (default: 1)
        - fs_name (str): CephFS filesystem name (default: "cephfs")
        - nfs_replication_number (int): Number of NFS clusters for multi-cluster test (default: 1)
        - nfs_port (str): NFS service port (default: "2049")
        - nfs_version (str): NFS protocol version (default: "4.0")
        - check_sighup (bool): Enable SIGHUP testing for certificate rotation (default: False)
        - spec: Multi-cluster specification for advanced deployments
    Test Workflow:
        1. Sets up GKLM infrastructure and REST client connection
        2. Generates certificates, keys, and CA certificate from GKLM
        3. Creates CephFS subvolume group for NFS exports
        4. Deploys NFS Ganesha instance(s) with BYOK encryption
        5. Creates and mounts NFS exports with encryption tags
        6. (Optional) Tests SIGHUP certificate rotation without service restart
        7. Performs I/O operations and validates encryption via FUSE mounts
        8. Cleans up all test resources including GKLM certificates and clients
    Single-Cluster Mode (nfs_replication_number=1):
        - Deploys one NFS Ganesha instance with BYOK
        - Optionally tests certificate rotation via SIGHUP
        - Validates that PID remains unchanged during certificate updates
    Multi-Cluster Mode (nfs_replication_number>1):
        - Deploys multiple NFS Ganesha instances with shared BYOK configuration
        - Distributes exports across available NFS nodes
        - Performs parallel I/O validation across all clusters
    SIGHUP Testing (check_sighup=True):
        - Works in single cluster mode
        - Captures Ganesha process PID before certificate rotation
        - Generates new certificate and key from GKLM
        - Updates NFS instance with new certificates
        - Verifies PID remains unchanged (confirming SIGHUP vs restart)
    Returns:
        int: 0 on success, 1 on failure
    Raises:
        ConfigError: When insufficient client nodes are available
        OperationFailedError: When critical operations fail (e.g., PID change during SIGHUP)
    Note:
        - Requires pre-configured GKLM server with appropriate credentials
        - Uses "ganeshagroup" as the default CephFS subvolume group
        - Performs comprehensive cleanup in finally block regardless of test outcome
        - Validates cluster health after cleanup to ensure system stability

    """
    config = kw.get("config", {})
    custom_data = kw.get("test_data", {})
    cephci_data = get_cephci_config()

    # Cluster nodes and setup
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    nfs_node = nfs_nodes[0]  # Use the first NFS node for certificate operations
    installer = ceph_cluster.get_nodes(role="installer")
    clients = ceph_cluster.get_nodes("client")

    # Configuration parameters
    nfs_mount = config.get("nfs_mount", "/mnt/nfs_byok")
    nfs_export = config.get("nfs_export", "/export_byok")
    nfs_name = config.get("nfs_instance_name", "nfs_byok")
    export_num = config.get("total_export_num", 2)
    no_clients = int(config.get("clients", 1))
    fs_name = config.get("fs_name", "cephfs")
    nfs_replication_number = int(config.get("nfs_replication_number", "1"))
    port = config.get("nfs_port", "2049")
    subvolume_group = "ganeshagroup"

    # Ensure sufficient clients
    if no_clients > len(clients):
        raise ConfigError(
            f"Test requires {no_clients} clients but only {len(clients)} available"
        )
    clients = clients[:no_clients]

    # Load GKLM configuration
    gklm_params = load_gklm_config(custom_data, config, cephci_data)
    gklm_ip = gklm_params["gklm_ip"]
    gklm_user = gklm_params["gklm_user"]
    gklm_password = gklm_params["gklm_password"]
    gklm_node_username = gklm_params["gklm_node_username"]
    gklm_node_password = gklm_params["gklm_node_password"]
    gklm_hostname = gklm_params["gklm_hostname"]

    gkml_client_name = "automation"
    gklm_cert_alias = "cert2"
    client_export_mount_dict = None

    try:
        log.info("Step 1: Setting up GKLM infrastructure")
        exe_node = setup_gklm_infrastructure(
            nfs_nodes=nfs_nodes,
            gklm_ip=gklm_ip,
            gklm_node_username=gklm_node_username,
            gklm_node_password=gklm_node_password,
            gklm_hostname=gklm_hostname,
        )

        log.info("Step 2: Initializing GKLM REST client")
        gklm_rest_client = GklmClient(
            ip=gklm_ip, user=gklm_user, password=gklm_password, verify=False
        )

        log.info("Step 3: generating certificates and keys, and CA_cert from GKLM")

        ca_cert = get_gklm_ca_certificate(
            gklm_ip=gklm_ip,
            gklm_node_username=gklm_node_username,
            gklm_node_password=gklm_node_password,
            exe_node=exe_node,
            gklm_rest_client=gklm_rest_client,
            gkml_servering_cert_name=gklm_hostname,
        )

        rsa_key, cert, _ = gklm_rest_client.certificates.get_certificates(
            subject={
                "common_name": nfs_node.hostname,
                "ip_address": nfs_node.ip_address,
            }
        )

        all_clients = gklm_rest_client.clients.list_clients()
        if gkml_client_name.upper() not in [x.get("clientName") for x in all_clients]:
            log.info(f"Not Found client name {gkml_client_name}, creating ")
            gklm_rest_client.clients.create_client(gkml_client_name)

        enctag = get_enctag(
            gklm_rest_client,
            gkml_client_name,
            gklm_cert_alias,
            gklm_user,
            cert,
        )

        log.info("Step 4: Configuring CephFS subvolume group for NFS")
        Ceph(clients[0]).fs.sub_volume_group.create(
            volume=fs_name, group=subvolume_group
        )

        # Single cluster BYOK flow
        if nfs_replication_number == 1:
            log.info(">>> Running single-cluster BYOK with Fuse validation")

            log.info("Step 5: Deploying NFS Ganesha instance with BYOK")
            create_nfs_instance_for_byok(
                installer, nfs_node, nfs_name, gklm_hostname, rsa_key, cert, ca_cert
            )

            log.info("Step 6: Creating and mounting NFS exports")
            client_export_mount_dict = create_export_and_mount_for_existing_nfs_cluster(
                clients,
                nfs_export,
                nfs_mount,
                export_num,
                fs_name,
                nfs_name,
                fs_name,
                port,
                version=config.get("nfs_version", "4.0"),
                enctag=enctag,
                nfs_server=nfs_node.hostname,
            )

            if config.get("check_sighup", False):
                log.info("SIGHUP Testing -- begins")
                gklm_cert_alias_new = "certsighup"
                validate_sighup(
                    installer=installer[0],
                    nfs_node=nfs_node,
                    nfs_name=nfs_name,
                    gklm_rest_client=gklm_rest_client,
                    gkml_client_name=gkml_client_name,
                    gklm_hostname=gklm_hostname,
                    ca_cert=ca_cert,
                    gklm_user=gklm_user,
                    gklm_cert_alias_old=gklm_cert_alias,
                    gklm_cert_alias_new=gklm_cert_alias_new,
                )

            log.info(
                "Step 7: Performing I/O validation on NFS exports \n"
                "Step 8: Validate encryption via FUSE mounts as well"
            )
            perform_io_operations_and_validate_fuse(
                client_export_mount_dict,
                clients,
                file_count=10,
                dd_command_size_in_M=5,
                nfs_name=nfs_name,
            )

        else:
            # Multi cluster BYOK flow
            log.info(
                f">>> Running multi-cluster BYOK: {nfs_replication_number} clusters"
            )

            multiclusters_dict = create_multiple_nfs_instance_for_byok(
                spec=config.get("spec"),
                replication_number=nfs_replication_number,
                installer=installer,
                cert=cert,
                rsa_key=rsa_key,
                ca_cert=ca_cert,
                kmip_host_list=gklm_hostname,
            )

            nfs_exports = [
                f"{nfs_export}_{x['service_id']}" for x in multiclusters_dict
            ]
            nfs_mounts = [f"{nfs_mount}_{x['service_id']}" for x in multiclusters_dict]
            nfs_names = [x["service_id"] for x in multiclusters_dict]

            # Ensure we have enough NFS servers for all clusters
            nfs_servers = [
                nfs_nodes[i % len(nfs_nodes)].hostname for i in range(len(nfs_names))
            ]
            nfs_ports = [x["spec"]["port"] for x in multiclusters_dict]

            log.info(
                f"NFS Clusters setup:"
                f"\n - Names: {nfs_names}"
                f"\n - Servers: {nfs_servers}"
                f"\n - Ports: {nfs_ports}"
            )

            log.info("Step 6: Creating and mounting NFS exports")
            client_export_mount_dict = create_export_and_mount_for_existing_nfs_cluster(
                clients,
                nfs_exports,
                nfs_mounts,
                export_num,
                fs_name,
                nfs_names,
                fs_name,
                nfs_ports,
                version=config.get("nfs_version", "4.0"),
                enctag=enctag,
                nfs_server=nfs_servers,
            )

            log.info("Step 7: Performing I/O validation on NFS exports")
            perform_io_operations_and_validate_fuse(
                client_export_mount_dict=client_export_mount_dict,
                clients=clients,
                file_count=5,
                dd_command_size_in_M=5,
                is_multicluster=True,
            )

        return 0

    finally:
        log.info("Cleanup: GKLM, NFS clusters, and mounts")
        if config.get("check_sighup", False):
            all_certs = [
                x.get("alias", None)
                for x in gklm_rest_client.certificates.list_certificates()
            ]
            if "certsighup" in all_certs:
                gklm_cert_alias = "certsighup"

        # Clean GKLM resources
        clean_up_gklm(
            gklm_rest_client=gklm_rest_client,
            gkml_client_name=gkml_client_name,
            gklm_cert_alias=gklm_cert_alias,
        )

        # Cleanup single cluster or multi cluster accordingly
        if nfs_replication_number == 1 and client_export_mount_dict is not None:
            log.info("Cleaning up single-cluster mounts and exports")
            cleanup_custom_nfs_cluster_multi_export_client(
                clients, nfs_mount, nfs_name, nfs_export, export_num
            )
            log.info("Cleaning up single-cluster FUSE mounts")
            for client in clients:
                for mount in [
                    m + "_fuse"
                    for m in client_export_mount_dict.get(client, {}).get("mount", [])
                ]:
                    client.exec_command(
                        sudo=True, cmd=f"rm -rf {mount}/*", long_running=True
                    )
                    if Unmount(client).unmount(mount):
                        raise OperationFailedError(
                            f"Failed to unmount FUSE mount {mount} on {client.hostname}"
                        )
        elif nfs_replication_number > 1:
            log.info("Cleaning up multi-cluster mounts and exports")
            dynamic_cleanup_common_names(
                clients,
                mounts_common_name=config.get("nfs_mount_common_name", "nfs_byok"),
            )

        if CephFSCommonUtils(ceph_cluster).wait_for_healthy_ceph(clients[0], 300):
            log.error("Cluster health is not OK even after waiting for 300secs")
            return 1
        log.info("Cleanup completed for all test resources.")
