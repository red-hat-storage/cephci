import time
import traceback

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import MountFailedError, Unmount
from tests.nfs.byok.byok_tools import (
    clean_up_gklm,
    create_nfs_instance_for_byok,
    get_enctag,
    load_gklm_config,
    perform_io_operations_and_validate_fuse,
    setup_gklm_infrastructure,
    wait_for_gklm_server_restart,
)
from tests.nfs.nfs_operations import cleanup_custom_nfs_cluster_multi_export_client
from tests.nfs.test_nfs_io_operations_during_upgrade import (
    create_export_and_mount_for_existing_nfs_cluster,
)
from utility.gklm_client.gklm_client import GklmClient
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


def run(ceph_cluster, **kw):
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
    port = config.get("nfs_port", "2049")
    subvolume_group = "ganeshagroup"
    operation = config.get("operation", None)

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
    gklm_hostname = gklm_params["gklm_hostname"]

    gkml_client_name = "automation"
    gklm_cert_alias = "cert2"
    new_ca_cert_alias = "custom_sni_host"
    client_export_mount_dict = None

    try:
        log.info("Step 1: Setting up GKLM infrastructure")
        setup_gklm_infrastructure(
            nfs_nodes=nfs_nodes,
            gklm_ip=gklm_ip,
            gklm_hostname=gklm_hostname,
        )

        log.info("Step 2: Initializing GKLM REST client")
        gklm_rest_client = GklmClient(
            ip=gklm_ip, user=gklm_user, password=gklm_password, verify=False
        )

        log.info("Step 3: generating certificates and keys, and CA_cert from GKLM")
        if gklm_hostname not in [
            x.get("alias") for x in gklm_rest_client.certificates.list_certificates()
        ]:
            log.info(f"Not Found CA cert alias {gklm_cert_alias}, creating ")
            gklm_rest_client.certificates.create_system_certificate(
                {
                    "type": "Self-signed",
                    "alias": gklm_hostname,
                    "validity": "3650",
                    "algorithm": "RSA",
                    "usageSubtype": "KEYSERVING_TLS",
                }
            )
            gklm_rest_client.server.restart_server()
            wait_for_gklm_server_restart(
                gklm_rest_client=gklm_rest_client,
                timeout=500,
                check_interval=10,
                initial_wait=10,
            )
        ca_cert = gklm_rest_client.certificates.get_system_certificate(
            cert_name=gklm_hostname
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

        gklm_rest_client.login()
        enctag = get_enctag(
            gklm_rest_client,
            gkml_client_name,
            gklm_cert_alias,
            gklm_user,
            cert,
        )

        log.info("Step 4: Configuring CephFS subvolume group for NFS")
        Ceph(nfs_node).execute("systemctl start rpcbind", sudo=True)
        Ceph(clients[0]).fs.sub_volume_group.create(
            volume=fs_name, group=subvolume_group
        )

        # Single cluster BYOK flow

        log.info(">>> Running single-cluster BYOK with Fuse validation")

        log.info("Step 5: Deploying NFS Ganesha instance with BYOK - with SNI settings")
        if operation == "Default Secure Case":
            log.info(
                "Using default secure case with matching servername and verify_hostname"
            )
            kmip_host_list = {
                "addr": gklm_hostname,
                "servername": gklm_hostname,
                "verify_hostname": gklm_hostname,
            }
        elif operation == "Mismatch Validation Hostname":
            log.info(
                "Using mismatch validation hostname with unmatching servername and verify_hostname"
            )
            kmip_host_list = {
                "addr": gklm_hostname,
                "servername": gklm_hostname,
                "verify_hostname": nfs_node.hostname,
            }
        elif operation == "Disable Validation":
            log.info(
                "Using disable validation with servername set but verify_hostname unset"
            )
            kmip_host_list = {
                "addr": gklm_hostname,
                "servername": gklm_hostname,
                "verify_hostname": "",
            }
        elif operation == "No SNI - Validate Other Hostname":
            log.info(
                "Using no SNI with empty servername and verify_hostname set to unmatching hostname"
            )
            kmip_host_list = {
                "addr": "",
                "servername": "",
                "verify_hostname": nfs_node.hostname,
            }
        elif operation in [
            "Custom SNI - Validation Hostname",
            "Different SNI and Validation",
            "No SNI - No Validation",
        ]:

            log.info("Creating new GKLM certificate for operation " + operation)

            all_ca_certs = gklm_rest_client.certificates.list_certificates()
            if new_ca_cert_alias in [x.get("alias") for x in all_ca_certs]:
                gklm_rest_client.certificates.delete_system_certificate(
                    new_ca_cert_alias
                )
                log.info(
                    f"Deleted existing custom SNI certificate with alias {new_ca_cert_alias} to recreate"
                )
                time.sleep(5)
            log.info(
                f"Creating new GKLM certificate with alias {new_ca_cert_alias} for custom SNI"
            )
            gklm_rest_client.certificates.create_system_certificate(
                {
                    "type": "Self-signed",
                    "alias": new_ca_cert_alias,
                    "cn": new_ca_cert_alias,
                    "validity": "3650",
                    "algorithm": "RSA",
                    "usageSubtype": "KEYSERVING_TLS",
                }
            )

            log.info(
                f"Created new GKLM certificate with alias {new_ca_cert_alias}, rebooting GKLM service to apply changes"
            )

            restart_data = gklm_rest_client.server.restart_server()
            log.info(f"GKLM server restart initiated: {restart_data}")
            wait_for_gklm_server_restart(
                gklm_rest_client=gklm_rest_client,
                timeout=500,
                check_interval=10,
                initial_wait=10,
            )

            ca_cert = gklm_rest_client.certificates.get_system_certificate(
                cert_name=new_ca_cert_alias
            )
            log.info("Using custom SNI with custom servername and verify_hostname")
            if operation == "Custom SNI - Validation Hostname":
                kmip_host_list = {
                    "addr": gklm_hostname,
                    "servername": new_ca_cert_alias,
                    "verify_hostname": new_ca_cert_alias,
                }
            elif operation == "Different SNI and Validation":
                kmip_host_list = {
                    "addr": gklm_hostname,
                    "servername": new_ca_cert_alias,
                    "verify_hostname": clients[0].hostname,
                }
            elif operation == "No SNI - No Validation":
                kmip_host_list = {
                    "addr": gklm_hostname,
                    "servername": "",
                    "verify_hostname": "",
                }

        create_nfs_instance_for_byok(
            installer=installer,
            nfs_node=nfs_node,
            nfs_name=nfs_name,
            kmip_host_list=kmip_host_list,
            rsa_key=rsa_key,
            cert=cert,
            ca_cert=ca_cert,
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
            dd_command_size_in_M=5,
            nfs_name=nfs_name,
        )

        if operation in [
            "Mismatch Validation Hostname",
            "No SNI - Validate Other Hostname",
            "Different SNI and Validation",
        ]:
            log.info(
                "Test failed: Mount succeeded despite hostname mismatch in GKLM certificate."
            )
            return 1

        return 0

    except MountFailedError:
        if operation in [
            "Mismatch Validation Hostname",
            "No SNI - Validate Other Hostname",
            "Different SNI and Validation",
        ]:
            log.info(
                "Mount failed as expected due to hostname mismatch in GKLM certificate."
            )
            return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Cleanup: GKLM, NFS clusters, and mounts")
        if operation not in [
            "Mismatch Validation Hostname",
            "No SNI - Validate Other Hostname",
            "Different SNI and Validation",
        ]:
            log.info("Cleaning up cluster FUSE mounts")
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
        log.info("Cleaning up cluster mounts and exports")
        cleanup_custom_nfs_cluster_multi_export_client(
            clients, nfs_mount, nfs_name, nfs_export, export_num, nfs_nodes=nfs_nodes
        )

        if operation in ["Custom SNI - Validation Hostname", "No SNI - No Validation"]:
            log.info(
                f"Updating GKLM system certificate to add KEYSERVING_TLS usage subtype for {gklm_hostname}"
            )
            gklm_rest_client.certificates.update_system_certificate(
                alias=gklm_hostname,
                add_usage_subtype="KEYSERVING_TLS",
            )
            log.info(
                f"Updated GKLM system certificate to add KEYSERVING_TLS usage subtype for old {gklm_hostname}"
            )

            gklm_rest_client.certificates.delete_system_certificate(new_ca_cert_alias)
            log.info(
                f"Deleted custom SNI certificate with alias {new_ca_cert_alias} to cleanup"
            )
            restart_data = gklm_rest_client.server.restart_server()
            log.info(f"GKLM server restart initiated: {restart_data}")
            wait_for_gklm_server_restart(
                gklm_rest_client=gklm_rest_client,
                timeout=500,
                check_interval=10,
                initial_wait=10,
            )

        # Clean GKLM resources
        clean_up_gklm(
            gklm_rest_client=gklm_rest_client,
            gkml_client_name=gkml_client_name,
            gklm_cert_alias=gklm_cert_alias,
        )
        log.info("Cleanup completed for all test resources.")
