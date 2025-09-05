import random
import string
import traceback

from cli.exceptions import ConfigError
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.nfs.byok.byok_tools import (
    clean_up_gklm,
    create_nfs_instance_for_byok,
    get_enctag,
    get_gklm_ca_certificate,
    load_gklm_config,
    nfs_byok_test_setup,
    setup_gklm_infrastructure,
)
from tests.nfs.nfs_operations import (
    cleanup_custom_nfs_cluster_multi_export_client,
    nfs_log_parser,
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
    BYOK Negative tests:
    A. Incorrect enctag:
    1. Setup GKLM - SSH,GKLM client, certs and enctag
    2. Deploy and configure NFS Ganesha with BYOK from Spec file.
    3. Create NFS export with wrong enctag and verify mount access denied.
       Verify relevant error in nfs log for export create failure.
    4. Remove and recreate with correct enctag and verify mount suceeds
    B. Incorrect KMIP_cert:
    1. Setup GKLM - SSH,GKLM client, certs and enctag
    2. Deploy and configure NFS Ganesha with BYOK from Spec file, with invalid KMIP ca-cert.
    3. Create NFS export with correct enctag and verify mount access denied due to invalid kmip cert.
       Verify relevant error in nfs log for invalid kmip cert.
    4. Remove NFS export cluster and NFS cluster
    5. Repeat 2-4 for invalid KMIP cert
    Cleanup:
     - Cleanup all resources on success or failure.
    """
    try:
        global byok_setup_params, nfs_mount, nfs_export, port, clients, fs_name, config, client_export_mount_dict
        global setup_params, cephfs_common_utils
        global nfs_cleanup, incorrect_enctag_cleanup, incorrect_cert_cleanup
        config = kw.get("config", {})
        custom_data = kw.get("test_data", {})
        cephci_data = get_cephci_config()
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        # Cluster settings
        nfs_nodes = ceph_cluster.get_nodes("nfs")
        installer = ceph_cluster.get_nodes(role="installer")[0]
        clients = ceph_cluster.get_nodes("client")
        client_objs = ceph_cluster.get_ceph_objects("client")
        client = clients[0]
        nfs_mount = config.get("nfs_mount", "/mnt/nfs_byok")
        nfs_export = config.get("nfs_export", "/export_byok")
        nfs_name = config.get("nfs_instance_name", "nfs_byok")
        no_clients = int(config.get("clients", 1))
        fs_name = config.get("fs_name", "cephfs")
        port = config.get("nfs_port", "2049")
        nfs_cleanup, incorrect_enctag_cleanup, incorrect_cert_cleanup, setup_params = (
            None,
            None,
            None,
            None,
        )

        # Load and validate GKLM parameters
        gklm_params = load_gklm_config(custom_data, config, cephci_data)
        rand_str = "".join(
            [random.choice(string.ascii_lowercase + string.digits) for _ in range(3)]
        )
        gklm_client_name = f"byok_neg_client_{rand_str}"
        gklm_cert_alias = f"byok_neg_client_cert_{rand_str}"

        # Check clients availability
        if no_clients > len(clients):
            log.error(
                "Test requires %d clients, but only %d available",
                no_clients,
                len(clients),
            )
            raise ConfigError("Insufficient clients for test")
        clients = clients[:no_clients]

        # Common variables
        client_export_mount_dict = None
        byok_setup_params = {
            "gklm_ip": gklm_params["gklm_ip"],
            "gklm_user": gklm_params["gklm_user"],
            "gklm_password": gklm_params["gklm_password"],
            "gklm_node_user": gklm_params["gklm_node_username"],
            "gklm_node_password": gklm_params["gklm_node_password"],
            "gklm_hostname": gklm_params["gklm_hostname"],
            "gklm_client_name": gklm_client_name,
            "gklm_cert_alias": gklm_cert_alias,
            "nfs_nodes": nfs_nodes,
            "installer": installer,
            "nfs_name": nfs_name,
        }
        log.info("Verify Cluster is healthy before test")
        if cephfs_common_utils.wait_for_healthy_ceph(client, 300):
            log.error("Cluster health is not OK even after waiting for 300secs")
            return 1
        fs_details = cephfs_common_utils.get_fs_info(client, fs_name)
        if not fs_details:
            cephfs_common_utils.create_fs(client, fs_name)
        if byok_test_incorrect_enctag():
            log.error("FAIL:BYOK Test for Incorrect enctag")
            return 1
        log.info("PASS:BYOK Test for Incorrect enctag")
        if byok_test_incorrect_kmip_cert():
            log.error("FAIL:BYOK Test for Incorrect KMIP cert")
            return 1
        log.info("PASS:BYOK Test for Incorrect KMIP cert")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        wait_time_secs = 300
        if cephfs_common_utils.wait_for_healthy_ceph(client_objs[0], wait_time_secs):
            log.error(
                "Cluster health is not OK even after waiting for %s secs",
                wait_time_secs,
            )
        if incorrect_enctag_cleanup != 1 or incorrect_cert_cleanup != 1:
            clean_up_gklm(
                gklm_rest_client=byok_setup_params["gklm_rest_client"],
                gkml_client_name=byok_setup_params["gklm_client_name"],
                gklm_cert_alias=byok_setup_params["gklm_cert_alias"],
            )

        # Clean up NFS clusters and mounts
        if nfs_cleanup != 1:
            cleanup_custom_nfs_cluster_multi_export_client(
                clients, nfs_mount, nfs_name, nfs_export, 1
            )

        cephfs_common_utils.remove_fs(client_objs[0], fs_name)
        log.info("Cleanup completed")


def byok_test_incorrect_enctag():
    """
    Test workflow - BYOK Test for Incorrect enctag
    """
    global nfs_cleanup, incorrect_enctag_cleanup
    log.info("BYOK Test for Incorrect enctag")
    _, enctag, gklm_rest_client, _ = nfs_byok_test_setup(byok_setup_params)
    byok_setup_params.update({"gklm_rest_client": gklm_rest_client})
    log.info("BYOK Test params: Enctag - %s", enctag)
    enctag1 = list(enctag)
    enctag1[4:7] = "aaa"
    wrng_enctag = "".join(enctag1)
    nfs_name = byok_setup_params["nfs_name"]
    nfs_node = byok_setup_params["nfs_nodes"][0]

    try:
        create_export_and_mount_for_existing_nfs_cluster(
            clients,
            nfs_export,
            nfs_mount,
            1,
            fs_name,
            nfs_name,
            fs_name,
            port,
            version=config.get("nfs_version", "4.2"),
            enctag=wrng_enctag,
            nfs_server=nfs_node.hostname,
        )
    except Exception as ex:
        log.info(ex)
        if ("Nfs mount failed" not in str(ex)) or (nfs_mount in str(ex)):
            log.error(
                "Mount suceeds but is expected to fail due to incorrect enctag in export"
            )
            return 1
        else:
            expect_list = ["keyset callback: runt/missing key for kmip_key_id"]
            if nfs_log_parser(clients[0], nfs_node, nfs_name, expect_list):
                log.error("NFS Debug log doesn't contain %s", expect_list)
                return 1
    log.info("Cleanup GKLM Client")
    clean_up_gklm(
        gklm_rest_client=byok_setup_params["gklm_rest_client"],
        gkml_client_name=byok_setup_params["gklm_client_name"],
        gklm_cert_alias=byok_setup_params["gklm_cert_alias"],
    )
    incorrect_enctag_cleanup = 1
    log.info("Cleanup mountdir,export and nfs cluster")
    cleanup_custom_nfs_cluster_multi_export_client(
        clients, nfs_mount, nfs_name, nfs_export, 1
    )
    nfs_cleanup = 1
    return 0


def byok_test_incorrect_kmip_cert():
    """
    Test workflow - BYOK Test for Incorrect Kmip Cert
    """
    global nfs_cleanup, incorrect_cert_cleanup
    log.info("BYOK Test for Incorrect kmip cert")

    gklm_hostname = byok_setup_params["gklm_hostname"]

    nfs_nodes = byok_setup_params["nfs_nodes"]
    nfs_node = nfs_nodes[0]
    installer = byok_setup_params["installer"]
    nfs_name = byok_setup_params["nfs_name"]
    enctag, rsa_key, cert, ca_cert, gklm_rest_client = gklm_setup()
    byok_setup_params.update({"gklm_rest_client": gklm_rest_client})
    incorrect_cert_cleanup = 0
    log.info("Creating symmetric key encryption tag in GKLM")

    log.info("Tamper the kmip cert before nfs byok cluster create")
    wrng_cert = incorrect_kmip_cert(cert)
    # ------------------- NFS Ganesha Instance Creation with BYOK -------------------
    log.info("Creating BYOK NFS Ganesha instance with incorrect KMIP cert")
    create_nfs_instance_for_byok(
        installer, nfs_node, nfs_name, gklm_hostname, rsa_key, wrng_cert, ca_cert
    )
    nfs_cleanup = 0
    log.info("Verify NFS mount fails, validate export create failure in nfs log")
    try:
        create_export_and_mount_for_existing_nfs_cluster(
            clients,
            nfs_export,
            nfs_mount,
            1,
            fs_name,
            nfs_name,
            fs_name,
            port,
            version=config.get("nfs_version", "4.2"),
            enctag=enctag,
            nfs_server=nfs_node.hostname,
        )
    except Exception as ex:
        if ("Nfs mount failed" not in str(ex)) or (nfs_mount in str(ex)):
            log.error(
                "Mount suceeds but is expected to fail due to incorrect enctag in export"
            )
            return 1
        else:
            expect_list = ["kmip can't connect to"]
            if nfs_log_parser(clients[0], nfs_node, nfs_name, expect_list):
                log.error("NFS Debug log doesn't contain %s", expect_list)
                return 1
    log.info("Cleanup mountdir,export and nfs cluster")
    cleanup_custom_nfs_cluster_multi_export_client(
        clients, nfs_mount, nfs_name, nfs_export, 1
    )
    nfs_cleanup = 1
    log.info("Tamper the ca-cert before nfs byok cluster create")
    wrng_ca_cert = incorrect_kmip_cert(ca_cert)
    # ------------------- NFS Ganesha Instance Creation with BYOK -------------------
    log.info("Creating BYOK NFS Ganesha instance with incorrect KMIP CA cert")
    create_nfs_instance_for_byok(
        installer, nfs_node, nfs_name, gklm_hostname, rsa_key, cert, wrng_ca_cert
    )
    nfs_cleanup = 0
    log.info("Verify NFS mount fails, validate export create failure in nfs log")
    try:
        create_export_and_mount_for_existing_nfs_cluster(
            clients,
            nfs_export,
            nfs_mount,
            1,
            fs_name,
            nfs_name,
            fs_name,
            port,
            version=config.get("nfs_version", "4.2"),
            enctag=enctag,
            nfs_server=nfs_node.hostname,
        )
    except Exception as ex:
        if ("Nfs mount failed" not in str(ex)) or (nfs_mount in str(ex)):
            log.error(
                "Mount suceeds but is expected to fail due to incorrect enctag in export"
            )
            return 1
        else:
            expect_list = ["kmip can't connect to"]
            if nfs_log_parser(clients[0], nfs_node, nfs_name, expect_list):
                log.error("NFS Debug log doesn't contain %s", expect_list)
                return 1
    log.info("Cleanup GKLM Client")
    clean_up_gklm(
        gklm_rest_client=byok_setup_params["gklm_rest_client"],
        gkml_client_name=byok_setup_params["gklm_client_name"],
        gklm_cert_alias=byok_setup_params["gklm_cert_alias"],
    )
    incorrect_cert_cleanup = 1
    log.info("Cleanup mountdir,export and nfs cluster")
    cleanup_custom_nfs_cluster_multi_export_client(
        clients, nfs_mount, nfs_name, nfs_export, 1
    )
    nfs_cleanup = 1
    return 0


# HELPER ROUTINES


def gklm_setup():
    """
    This helper method does gklm setup and provides required certs for nfs byok cluster setup
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
        gklm_client_name,
        gklm_cert_alias,
        gklm_user,
        cert,
        created_client_data,
    )

    # ------------------- Prerequisites and Certificate Export -------------------
    # Ensure SSH access, hostname resolution, and certificate availability
    log.info("Setting up SSH and CA certificate prerequisites on NFS and GKLM nodes")
    ca_cert = get_gklm_ca_certificate(
        gklm_ip=gklm_ip,
        gklm_node_username=gklm_node_user,
        gklm_node_password=gklm_node_password,
        gklm_hostname=gklm_hostname,
        exe_node=exe_node,
        gklm_rest_client=gklm_rest_client,
    )
    log.info("CA certificate successfully retrieved \n %s", ca_cert)
    return enctag, rsa_key, cert, ca_cert, gklm_rest_client


def incorrect_kmip_cert(cert):
    """
    This is helper method to set incorrect kmip cert before nfs cluster create
    """
    cert1 = list(cert)
    cert1[31:34] = "AAA"
    wrng_cert = "".join(cert1)
    return wrng_cert
