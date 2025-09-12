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
)
from tests.nfs.test_nfs_io_operations_during_upgrade import (
    create_export_and_mount_for_existing_nfs_cluster,
)
from utility.gklm_client.gklm_client import GklmClient
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Unified BYOK (Bring Your Own Key) NFS test runner.

    Modes:
      - Single cluster with FUSE validation (default / nfs_replication_number=1)
      - Multi cluster with IO restart validation (when nfs_replication_number > 1)
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
        created_client_data = gklm_rest_client.clients.create_client(gkml_client_name)
        enctag = get_enctag(
            gklm_rest_client,
            created_client_data,
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

            log.info(
                "Step 7: Performing I/O validation on NFS exports \n"
                "Step 8: Validate encryption via FUSE mounts as well"
            )
            perform_io_operations_and_validate_fuse(
                client_export_mount_dict,
                clients,
                file_count=10,
                dd_command_size_in_M=20,
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
