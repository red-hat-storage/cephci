import json

from nfs_operations import cleanup_cluster, setup_nfs_cluster
from smb_operations import (
    check_ctdb_health,
    check_rados_clustermeta,
    deploy_smb_service_declarative,
    smb_cleanup,
    smbclient_check_shares,
    verify_smb_service,
)

from ceph.ceph_admin import CephAdmin
from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


class SmbNfsColocation(Exception):
    pass


def verify_nfs_service(node, service_name):
    """
    Verifies service is up
    Args:
        node (str): monitor node object
        service_name (str): service name
    return: (bool)
    """
    # Get service status
    out = json.loads(
        CephAdm(node).ceph.orch.ls(service_type=service_name, format="json-pretty")
    )[0]
    # Check smb service is up
    timeout, interval = 300, 60
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = json.loads(
            CephAdm(node).ceph.orch.ls(service_type=service_name, format="json-pretty")
        )[0]
        if out["service_type"] == "nfs" and out["status"]["running"] > 0:
            return True
    if w.expired:
        raise SmbNfsColocation(f"Service {service_name} is not deployed")


def run(ceph_cluster, **kw):
    """Deploy samba with auth_mode 'user' using declarative style(Spec File)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get config
    config = kw.get("config")

    # Get cephadm obj
    cephadm = CephAdmin(cluster=ceph_cluster, **config)

    # Check mandatory parameter file_type
    if not config.get("file_type"):
        raise ConfigError("Mandatory config 'file_type' not provided")

    # Get spec file type
    file_type = config.get("file_type")

    # Check mandatory parameter spec
    if not config.get("spec"):
        raise ConfigError("Mandatory config 'spec' not provided")

    # Get smb spec
    smb_spec = config.get("spec")

    # Get smb spec file mount path
    file_mount = config.get("file_mount", "/tmp")

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get smb nodes
    smb_nodes = ceph_cluster.get_nodes("smb")

    # get client node
    client = ceph_cluster.get_nodes(role="client")[0]

    # Get smb subvolume mode
    smb_subvolume_mode = config.get("smb_subvolume_mode", "0777")

    # Get operations
    operations = config.get("operations")

    # Get nfs service details
    nfs_node = ceph_cluster.get_nodes("nfs")[0]
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.1")
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname
    clients = ceph_cluster.get_nodes(role="client")

    # Get smb service value from spec file
    smb_shares = []
    smb_subvols = []
    for spec in smb_spec:
        if spec["resource_type"] == "ceph.smb.cluster":
            smb_cluster_id = spec["cluster_id"]
            auth_mode = spec["auth_mode"]
            if "domain_settings" in spec:
                domain_realm = spec["domain_settings"]["realm"]
            else:
                domain_realm = None
            if "clustering" in spec:
                clustering = spec["clustering"]
            else:
                clustering = "default"
            if "public_addrs" in spec:
                public_addrs = [
                    public_addrs["address"].split("/")[0]
                    for public_addrs in spec["public_addrs"]
                ]
            else:
                public_addrs = None
            if "custom_ports" in spec:
                smb_port = spec["custom_ports"]["smb"]
            else:
                smb_port = "445"
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

    try:
        for operation in operations:
            if operation == "deploy_nfs":
                # Setup nfs cluster
                setup_nfs_cluster(
                    clients,
                    nfs_server_name,
                    port,
                    version,
                    nfs_name,
                    nfs_mount,
                    fs_name,
                    nfs_export,
                    fs,
                    ceph_cluster=ceph_cluster,
                )

            if operation == "deploy_smb":
                # Create volume and smb subvolume with case_sensitivity disable
                deploy_smb_service_declarative(
                    installer,
                    cephfs_vol,
                    smb_subvol_group,
                    smb_subvols,
                    smb_cluster_id,
                    smb_subvolume_mode,
                    file_type,
                    smb_spec,
                    file_mount,
                )

                # Verify ctdb clustering
                if clustering != "never":
                    # check samba clustermeta in rados
                    if not check_rados_clustermeta(cephadm, smb_cluster_id, smb_nodes):
                        log.error("rados clustermeta for samba not found")
                        return 1
                    # Verify CTDB health
                    if not check_ctdb_health(smb_nodes, smb_cluster_id):
                        log.error("ctdb health error")
                        return 1

                # Check smb share using smbclient
                smbclient_check_shares(
                    smb_nodes,
                    client,
                    smb_shares,
                    smb_user_name,
                    smb_user_password,
                    auth_mode,
                    domain_realm,
                    smb_port,
                    public_addrs,
                )

            if operation == "verify_service":
                # Verify smb services
                verify_smb_service(installer, service_name="smb")

                # Verify nfs services
                verify_nfs_service(installer, service_name="nfs")

            if operation == "cleanup_smb":
                smb_cleanup(installer, smb_shares, smb_cluster_id)

            if operation == "cleanup_nfs":
                cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)

    except Exception as e:
        log.error(f"Failed to deploy samba with auth_mode 'user' : {e}")
        return 1
    return 0
