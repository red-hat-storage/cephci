import json

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from tests.nfs.byok.byok_tools import (
    clean_up_gklm,
    create_nfs_instance_for_byok,
    get_enctag,
    get_gklm_ca_certificate,
    load_gklm_config,
    setup_gklm_infrastructure,
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
from utility.utils import get_cephci_config

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Main BYOK (Bring Your Own Key) NFS test runner.

    1. Load GKLM config (supports openstack & baremetal).
    2. Setup GKLM infrastructure (SSH, hosts).
    3. Register GKLM client, issue certs and enctag.
    4. Deploy and configure NFS Ganesha with BYOK.
    5. Create, mount, validate exports via FUSE.
    6. Cleanup all resources on success or failure.
    """
    config = kw.get("config", {})
    custom_data = kw.get("test_data", {})
    cephci_data = get_cephci_config()

    # Cluster settings
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    clients = ceph_cluster.get_nodes("client")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs_byok")
    nfs_export = config.get("nfs_export", "/export_byok")
    nfs_name = config.get("nfs_instance_name", "nfs_byok")
    export_num = config.get("total_export_num", 2)
    no_clients = int(config.get("clients", 1))
    fs_name = config.get("fs_name", "cephfs")
    port = config.get("nfs_port", "2049")
    subvolume_group = "ganeshagroup"
    nfs_node = nfs_nodes[0]

    # Load and validate GKLM parameters
    gklm_params = load_gklm_config(custom_data, config, cephci_data)
    gklm_ip = gklm_params["gklm_ip"]
    gklm_user = gklm_params["gklm_user"]
    gklm_password = gklm_params["gklm_password"]
    gklm_node_username = gklm_params["gklm_node_username"]
    gklm_node_password = gklm_params["gklm_node_password"]
    gklm_hostname = gklm_params["gklm_hostname"]

    # Check clients availability
    if no_clients > len(clients):
        log.error(
            "Test requires %d clients, but only %d available", no_clients, len(clients)
        )
        raise ConfigError("Insufficient clients for test")
    clients = clients[:no_clients]

    # Common variables
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

        log.info("Step 3: Fetching certificates and keys from GKLM")
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
        ca_cert = get_gklm_ca_certificate(
            gklm_ip=gklm_ip,
            gklm_node_username=gklm_node_username,
            gklm_node_password=gklm_node_password,
            exe_node=exe_node,
            gklm_rest_client=gklm_rest_client,
        )

        log.info("Step 4: Configuring CephFS subvolume group for NFS")
        Ceph(clients[0]).fs.sub_volume_group.create(
            volume=fs_name, group=subvolume_group
        )

        log.info("Step 5: Deploy NFS Ganesha instance with BYOK")
        create_nfs_instance_for_byok(
            installer, nfs_node, nfs_name, gklm_ip, rsa_key, cert, ca_cert
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

        log.info("Step 7: Performing I/O validation on NFS exports")
        perform_io_operations_in_loop(
            client_export_mount_dict=client_export_mount_dict,
            clients=clients,
            file_count=10,
            dd_command_size_in_M=20,
        )

        log.info("Step 8: Validating encryption via FUSE mounts")
        export_list = json.loads(Ceph(clients[0]).nfs.export.ls(nfs_name))
        exports_info = {
            e: json.loads(Ceph(clients[0]).nfs.export.get(nfs_name, e, format="json"))
            for e in export_list
        }
        for client in clients:
            mounts = client_export_mount_dict[client]["mount"]
            if mounts:
                fuse_mount = mounts[0] + "_fuse"
                fuse_mount_retry(
                    client=client,
                    mount=fuse_mount,
                    extra_params=f'-r {exports_info[client_export_mount_dict[client]["export"][0]]["path"]}',
                )
                validate_enc_for_nfs_export_via_fuse(
                    client=client,
                    fuse_mount=fuse_mount,
                    nfs_mount=mounts[0],
                )

        return 0

    except Exception as e:
        log.error("BYOK test failed: %s", e)
        raise

    finally:
        log.info("Cleanup: GKLM, NFS clusters, and mounts")

        # Clean up GKLM resources
        clean_up_gklm(
            gklm_rest_client=gklm_rest_client,
            gkml_client_name=gkml_client_name,
            gklm_cert_alias=gklm_cert_alias,
        )

        # Clean up NFS clusters and mounts
        cleanup_custom_nfs_cluster_multi_export_client(
            clients, nfs_mount, nfs_name, nfs_export, export_num
        )

        # Unmount and remove FUSE mounts
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

        log.info("Cleanup completed for all test resources.")
