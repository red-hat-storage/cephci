import re
from time import sleep

from ceph.parallel import parallel
from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import MountFailedError, Unmount
from tests.ceph_ansible.purge_cluster import reboot_node
from tests.nfs.byok.byok_tools import (
    clean_up_gklm,
    create_in_file_certs,
    get_enctag,
    get_gklm_ca_certificate,
    load_gklm_config,
    perform_io_operations_and_validate_fuse,
    setup_gklm_infrastructure,
)
from tests.nfs.nfs_operations import (
    dynamic_cleanup_common_names,
    setup_custom_nfs_cluster_multi_export_client,
)
from tests.nfs.test_nfs_io_operations_during_upgrade import (
    create_export_and_mount_for_existing_nfs_cluster,
    perform_io_operations_in_loop,
)
from utility.gklm_client.gklm_client import GklmClient
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


def reboot_node_with_wait_and_loops(node, wait=10, loops=1):
    """
    Reboot a node multiple times with configurable wait intervals between reboots.

    This function performs one or more reboots on the specified node. When multiple
    loops are requested, it waits for the specified duration between each reboot
    to allow the system to stabilize.

    Args:
        node: The node object to be rebooted
        wait (int, optional): Number of seconds to wait between reboots when
                             loops > 1. Defaults to 10.
        loops (int, optional): Number of times to reboot the node. Defaults to 1.

    Returns:
        None

    Note:
        - When loops=1, no wait time is applied as there's only one reboot
        - The wait parameter is only used when loops > 1
        - Uses the reboot_node() function to perform the actual reboot operation
    """
    for i in range(loops):
        log.info(
            "Starting reboot {0} of {1} for node {2}".format(
                i + 1, loops, node.hostname
            )
        )
        reboot_node(node)
        log.info("Reboot {0} completed for node {1}".format(i + 1, node.hostname))
        if loops > 1 and i < loops - 1:  # Don't wait after the last reboot
            log.info("Waiting {0} seconds before next reboot...".format(wait))
            sleep(wait)
            log.info("Wait period completed, proceeding with next reboot")

    log.info("All {0} reboots completed for node {1}".format(loops, node.hostname))


def run(ceph_cluster, **kw):
    """
    Execute BYOK (Bring Your Own Key) NFS high availability failover encryption test.

    This function tests NFS encryption with BYOK by setting up GKLM infrastructure,
    configuring NFS clusters with encryption, and performing failover testing with
    concurrent I/O operations and node reboots.

    Test Steps:
        1. Set up GKLM infrastructure and validate connectivity
        2. Initialize GKLM REST client for key management operations
        3. Generate certificates, keys, and CA certificate from GKLM
        4. Create client certificates and encryption tags
        5. Configure NFS cluster with BYOK encryption
        6. Identify active NFS node in HA setup
        7. Execute parallel operations (I/O + node failover)
        8. Cleanup all resources including GKLM, NFS clusters, and mounts

    Args:
        ceph_cluster: Ceph cluster object containing node information
        **kw: Keyword arguments containing:
            config (dict): Test configuration parameters including:
                - nfs_mount (str): NFS mount path (default: "/mnt/nfs_byok")
                - nfs_export (str): NFS export path (default: "/export_byok")
                - nfs_instance_name (str): NFS instance name (default: "nfs_byok")
                - total_export_num (int): Number of exports (default: 2)
                - clients (int): Number of client nodes (default: 1)
                - servers (int): Number of server nodes (default: 1)
                - fs_name (str): CephFS name (default: "cephfs")
                - nfs_replication_number (int): NFS replication number (default: 1)
                - nfs_port (str): NFS port (default: "2049")
                - ha (bool): Enable high availability (default: False)
                - active_standby (bool): Enable active-standby mode (default: False)
                - vip (str): Virtual IP address (optional)
                - nfs_version (str): NFS version (default: "4.2")
            test_data (dict): Custom test data containing GKLM configuration

    Returns:
        int: 0 on success, 1 on failure

    Raises:
        ConfigError: When insufficient clients or servers are available
        OperationFailedError: When critical operations fail during execution
    """
    config = kw.get("config", {})
    custom_data = kw.get("test_data", {})
    cephci_data = get_cephci_config()

    # Cluster nodes and setup
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    nfs_node = nfs_nodes[0]  # Use the first NFS node for certificate operations
    clients = ceph_cluster.get_nodes("client")

    # Configuration parameters
    nfs_mount = config.get("nfs_mount", "/mnt/nfs_byok")
    nfs_export = config.get("nfs_export", "/export_byok")
    nfs_name = config.get("nfs_instance_name", "nfs_byok")
    export_num = config.get("total_export_num", 2)
    no_clients = int(config.get("clients", 1))
    no_servers = int(config.get("servers", 1))
    fs_name = config.get("fs_name", "cephfs")
    export_num = int(config.get("total_num_exports", 1))
    port = config.get("nfs_port", "2049")
    version = config.get("nfs_version", "4.0")
    subvolume_group = "ganeshagroup"
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)
    operation = config.get("operation")
    incorrect_key = "KEY-3d4a967-58b7e921-d86c-4d0c-8824-81edba3c655d"

    # Ensure sufficient clients
    if no_clients > len(clients):
        raise ConfigError(
            "Test requires {0} clients but only {1} available".format(
                no_clients, len(clients)
            )
        )

    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")

    nfs_servers = nfs_nodes[:no_servers]
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
            nfs_nodes=nfs_servers,
            gklm_ip=gklm_ip,
            gklm_node_username=gklm_node_username,
            gklm_node_password=gklm_node_password,
            gklm_hostname=gklm_hostname,
        )

        log.info("Step 2: Initializing GKLM REST client")
        gklm_rest_client = GklmClient(
            ip=gklm_ip, user=gklm_user, password=gklm_password, verify=False
        )

        log.info(
            "Step 3: Generating certificates and keys, and CA certificate from GKLM"
        )
        ca_cert = get_gklm_ca_certificate(
            gklm_ip=gklm_ip,
            gklm_node_username=gklm_node_username,
            gklm_node_password=gklm_node_password,
            exe_node=exe_node,
            gklm_rest_client=gklm_rest_client,
            gkml_servering_cert_name=gklm_hostname,
        )

        log.info("Step 4: Creating client certificates and encryption tags")
        rsa_key, cert, _ = gklm_rest_client.certificates.get_certificates(
            subject={
                "common_name": nfs_node.hostname,
                "ip_address": nfs_node.ip_address,
            }
        )

        is_gklm_client_present = False
        clients_list = gklm_rest_client.clients.list_clients()
        for client in clients_list:
            if client.get("name", "") == gkml_client_name:
                is_gklm_client_present = True
                log.info(f"GKLM client {0} already exists".format(gkml_client_name))
                break

        if not is_gklm_client_present:
            log.info("Creating GKLM client {0}".format(gkml_client_name))
            created_client_data = gklm_rest_client.clients.create_client(
                gkml_client_name
            )

        enctag = get_enctag(
            gklm_client=gklm_rest_client,
            gkml_client_name=gkml_client_name,
            gklm_cert_alias=gklm_cert_alias,
            gklm_user=gklm_user,
            cert=cert,
            created_client_data=created_client_data,
        )

        kmip_config = {
            "in-file": create_in_file_certs(
                certs_dict={
                    "kmip_cert": "|\n" + cert.rstrip("\\n"),
                    "kmip_key": "|\n" + rsa_key.rstrip("\\n"),
                    "kmip_ca_cert": "|\n" + re.sub("\r", "", ca_cert.rstrip("\\n")),
                    "kmip_host_list": [gklm_hostname],
                },
                node=clients[0],
            )
        }

        log.info("Step 5: Configuring NFS cluster with BYOK encryption")
        Ceph(clients[0]).fs.sub_volume_group.create(
            volume=fs_name, group=subvolume_group
        )
        client_export_mount_dict = setup_custom_nfs_cluster_multi_export_client(
            clients=clients,
            nfs_server=[nfs_node.hostname for nfs_node in nfs_servers],
            port=port,
            nfs_name=nfs_name,
            fs_name=fs_name,
            fs=fs_name,
            nfs_mount=nfs_mount,
            nfs_export=nfs_export,
            ha=bool(config.get("ha", False)),
            version=version,
            ceph_cluster=ceph_cluster,
            export_num=export_num,
            active_standby=bool(config.get("active_standby", False)),
            vip=config.get("vip", None),
            enctag=enctag,
            **kmip_config,
        )

        # log.info("Step 6: Identifying active NFS node in HA setup")
        # active_nfs_node = get_active_node_ha(clients[0], nfs_name, nfs_servers)
        # log.info(f"Found active node as {active_nfs_node.hostname}")

        log.info(
            "Step 7: Executing parallel operations - I/O operations and node failover"
        )
        if operation == "correct_incorrect_key":
            perform_io_operations_and_validate_fuse(
                client_export_mount_dict,
                clients,
                file_count=50,
                dd_command_size_in_M=10,
                nfs_name=nfs_name,
            )
        else:
            with parallel() as p:
                log.info("Starting I/O operations with FUSE validation")
                p.spawn(
                    perform_io_operations_and_validate_fuse,
                    client_export_mount_dict,
                    clients,
                    file_count=50,
                    dd_command_size_in_M=10,
                    nfs_name=nfs_name,
                )
                log.info("Starting node reboot operations for failover testing")
                # rebooting while io is commented due to https://bugzilla.redhat.com/show_bug.cgi?id=2377090
                # p.spawn(reboot_node_with_wait_and_loops, active_nfs_node, loops=1)

        log.info("Step 8: All parallel operations completed successfully")

        client_export_mount_dict_2 = create_export_and_mount_for_existing_nfs_cluster(
            clients=clients,
            nfs_export=nfs_export + "part2",
            nfs_mount=nfs_mount + "part2",
            fs_name=fs_name,
            nfs_name=nfs_name,
            fs=fs_name,
            port=port,
            export_num=3,
            version=version,
            enctag=incorrect_key if operation == "correct_incorrect_key" else enctag,
            nfs_server=[x.hostname for x in nfs_servers],
            ha=ha,
            vip=vip,
        )

        log.info("Performing additional I/O operations on both mounts")
        perform_io_operations_in_loop(
            client_export_mount_dict=client_export_mount_dict_2,
            clients=clients,
            file_count=10,
            dd_command_size_in_M=10,
        )

        return 0
    except MountFailedError:
        if operation == "correct_incorrect_key":
            log.info("Expected failure occurred during I/O with incorrect key scenario")
            return 0

    finally:
        log.info("Step 9: Cleanup - GKLM, NFS clusters, and mounts")

        # Clean GKLM resources
        clean_up_gklm(
            gklm_rest_client=gklm_rest_client,
            gkml_client_name=gkml_client_name,
            gklm_cert_alias=gklm_cert_alias,
        )

        # Cleanup single cluster or multi cluster accordingly
        if client_export_mount_dict is not None:
            log.info("Cleaning up single-cluster FUSE mounts")
            for client in clients:
                for mount in [
                    m + "_fuse"
                    for m in client_export_mount_dict.get(client, {}).get("mount", [])
                ]:
                    client.exec_command(
                        sudo=True, cmd="rm -rf {0}/*".format(mount), long_running=True
                    )
                    if Unmount(client).unmount(mount):
                        raise OperationFailedError(
                            f"Failed to unmount FUSE mount {0} on {1}".format(
                                mount, client.hostname
                            )
                        )

        log.info("Cleaning up multi-cluster mounts and exports")
        dynamic_cleanup_common_names(
            clients,
            mounts_common_name=config.get("nfs_mount_common_name", "nfs_byok"),
            group_name=subvolume_group,
        )

        log.info("Cleanup completed for all test resources.")
