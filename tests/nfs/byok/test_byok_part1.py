from ceph.ceph import CephNode
from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError
from cli.utilities.packages import Package
from tests.nfs.nfs_operations import cleanup_cluster, create_nfs_via_file_and_verify, exports_mounts_perclient
from tests.nfs.scripts_tools.gklm_client import GklmClient
from tests.nfs.test_nfs_io_operations_during_upgrade import create_export_and_mount_for_existing_nfs_cluster, perform_io_operations_in_loop
from utility.log import Log
from utility.utils import generate_self_signed_certificate

log = Log(__name__)

def get_enctag(gklm_client, gkml_client_name, gklm_cert_alias, gklm_user, cert): 
    try:
        client_data = gklm_client.create_client(gkml_client_name)
        assaign_cert_data = gklm_client.assign_client_certificate(client_name=gkml_client_name, cert_pem=cert, alias= gklm_cert_alias)
        log.info(f"Created GKLM client {client_data} and assigned certificate {assaign_cert_data}")
        symmetric_data = gklm_client.create_symmetric_key_object(number_of_objects= 1, client_name=gkml_client_name,alias_prefix_name="AUT",cryptoUsageMask= "Encrypt,Decrypt")
        enctag = symmetric_data['id']
        gklm_client.assign_users_to_generic_kmip_client(client_name=gkml_client_name, users=[gklm_user])
        log.info(f"Created symmetric key object with UUID {enctag} in GKLM")
    except Exception as e:
        log.error(f"Failed to create GKLM client {gkml_client_name} and assign certificate {gklm_cert_alias}: {e}")
        raise
    return enctag

def create_nfs_instance_for_byok(installer, nfs_node, nfs_name, kmip_host_list, rsa_key, cert, ca_cert):
    try:
        # create nfs ganesha
        nfs_cluster_dict_for_orch = {
            "service_type": "nfs",
            "service_id": nfs_name,
            "placement": {"host_pattern": nfs_node.hostname},
            "spec": {
            "kmip_cert": '|\n' + cert.rstrip('\\n'),
            "kmip_key": '|\n' + rsa_key.rstrip('\\n'),
            "kmip_ca_cert": '|\n' + ca_cert.rstrip('\\n'),
            "kmip_host_list": [kmip_host_list],}
        }

        create_nfs_via_file_and_verify(installer_node=installer,
                                       nfs_objects=[nfs_cluster_dict_for_orch],
                                       timeout=300)

        log.info("NFS Ganesha instance created successfully with BYOK configuration")
    except Exception as e:
        log.error(f"Failed to create NFS Ganesha instance with BYOK configuration: {e}")
        raise


# --------------Main run function ----------------
def run(ceph_cluster, **kw):
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    clients = ceph_cluster.get_nodes("client")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs_byok")
    nfs_export = config.get("nfs_export", "/export_byok")
    version = config.get("nfs_version", "4.0")
    gklm_ip = config.get("gklm_ip", None)
    gklm_user = config.get("gklm_user", "SKLMAdmin")
    gklm_password = config.get("gklm_password", "Db2@pass123")
    gklm_cert = config.get("gklm_cert", None)
    nfs_name = config.get("nfs_instance_name", "nfs_byok")
    export_num = config.get("total_export_num", 1)
    no_clients = int(config.get("clients", "3"))
    fs_name = config.get("fs_name", "cephfs")
    port = config.get("nfs_port", "2049")
    subvolume_group = "ganeshagroup"
    nfs_node = nfs_nodes[0]
    fs = fs_name
    kmip_host_list = gklm_ip
    ca_cert = gklm_cert

    # ------------------- Work in progress -------------------
    # client = clients[0]
    # client.exec_command('sshpass -p "passwd" ssh -t root@10.0.67.226 "ls /var/log; exec bash"')
    # Package(client).install("sshpass")
    # cmd = 'sshpass -p "passwd" ssh -tt -o StrictHostKeyChecking=no root@10.0.67.226 "cat /etc/hosts"'
    # Ceph(client).execute(cmd)

    # gklm_node = CephNode(username = "root",
    #     password = "passwd",
    #     root_password = "passwd",
    #     ip_address = "10.0.67.226",
    #     hostname = "ceph-gklm-servers-nfs-ttjxzl-node2",
    #     look_for_key = "/root/.ssh/known_hosts",
    #     private_key_path = "/root/.ssh/id_rsa.pub",
    #     root_login = True,
    #     private_ip = "10.0.67.226",
    #     subnet = '24',
    #     ceph_nodename = 'gklm',
    #     no_of_volumes = None,
    #     role = 'client',
    #     )


    # ------------------- GKLM Client Setup -------------------
    gkml_client_name = "automation"
    gklm_cert_alias = "cert2"
    gklm_client = GklmClient(gklm_ip, user=gklm_user, password=gklm_password, verify=False)   
    rsa_key, cert, _ = gklm_client.get_certificates(subject={"common_name": nfs_node.hostname, "ip_address": nfs_node.ip_address})
    enctag = get_enctag(gklm_client, gkml_client_name, gklm_cert_alias, gklm_user, cert)

    Ceph(clients[0]).fs.sub_volume_group.create(volume=fs_name, group=subvolume_group)
    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    try:
        create_nfs_instance_for_byok(installer, 
                                     nfs_node, 
                                     nfs_name, 
                                     kmip_host_list,
                                     rsa_key,
                                     cert,
                                     ca_cert)

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
        perform_io_operations_in_loop(client_export_mount_dict, clients, file_count=10, dd_command_size_in_M=20)
        return 0
    except Exception as e:
        log.error(f"BYOK  testcase failed : {e}")
    finally:
        log.info("Cleanup in progress...")
        log.info("cleanup GKLM client objects")
        # #cleanup_part
        ids = [id['uuid'] for id in gklm_client.list_client_objects(gkml_client_name)]
        for id in ids:
            log.info(f"Deleting GKLM symmetric key object {id}")
            gklm_client.delete_object(id)
        gklm_client.delete_certificate(gklm_cert_alias)
        gklm_client.delete_client(gkml_client_name)
        log.info("GKLM client cleanup successful")
        log.info("Cleanup in NFS cluster and mounts")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
