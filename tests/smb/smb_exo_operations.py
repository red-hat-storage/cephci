from time import sleep

from smb_operations import (
    apply_smb_spec,
    check_ctdb_health,
    check_rados_clustermeta,
    create_vol_smb_subvol,
    enable_smb_module,
    generate_smb_spec,
    smbclient_check_shares,
    verify_smb_service,
)

from ceph.ceph_admin import CephAdmin
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def cleanup_smb(pri_installer, sec_installer, smb_spec):
    """Cleanup smb services"""
    try:
        # Get smb service value from spec file
        smb_shares = []
        smb_subvols = []
        for spec in smb_spec:
            if spec["resource_type"] == "ceph.smb.cluster":
                smb_cluster_id = spec["cluster_id"]
            elif spec["resource_type"] == "ceph.smb.share":
                cephfs_vol = spec["cephfs"]["volume"]
                smb_subvol_group = spec["cephfs"]["path"].split("/")[2]
                smb_subvols.append(spec["cephfs"]["path"].split("/")[3])
                smb_shares.append(spec["share_id"])

        # Remove smb shares
        for smb_share in smb_shares:
            CephAdm(sec_installer).ceph.smb.share.rm(smb_cluster_id, smb_share)

        # # Remove smb cluster
        CephAdm(sec_installer).ceph.smb.cluster.rm(smb_cluster_id)
        sleep(9)

        # Remove subvolume
        for smb_subvol in smb_subvols:
            pri_installer.exec_command(
                sudo=True,
                cmd=f"cephadm shell -- ceph fs subvolume rm {cephfs_vol} {smb_subvol} --group_name {smb_subvol_group}",
            )

        # Remove subvolumegroup
        cmd = f"cephadm shell -- ceph fs subvolumegroup rm {cephfs_vol} {smb_subvol_group} --force"
        pri_installer.exec_command(sudo=True, cmd=cmd)
        sleep(9)
    except Exception as e:
        raise ConfigError(f"Fail to clean up samba services {e}")


def verify_exo_client_connection(smb_nodes, smb_spec, sec_client):
    """Verify client connection with an external secondary cluster"""
    try:
        # Get smb service value from spec file
        smb_shares = []
        for spec in smb_spec:
            if spec["resource_type"] == "ceph.smb.cluster":
                auth_mode = spec["auth_mode"]
                if "domain_settings" in spec:
                    domain_realm = spec["domain_settings"]["realm"]
                else:
                    domain_realm = None
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
                smb_shares.append(spec["share_id"])

        # Check smb share using smbclient
        sleep(30)
        smbclient_check_shares(
            smb_nodes,
            sec_client,
            smb_shares,
            smb_user_name,
            smb_user_password,
            auth_mode,
            domain_realm,
            smb_port,
            public_addrs,
        )
    except Exception as e:
        raise ConfigError(f"Client connection failed {e}")


def verify_exo_ctdb(cephadm, smb_nodes, smb_spec):
    """Verify ctdb status with an external secondary cluster"""
    try:
        # Get smb service value from spec file
        for spec in smb_spec:
            if spec["resource_type"] == "ceph.smb.cluster":
                smb_cluster_id = spec["cluster_id"]
                if "clustering" in spec:
                    clustering = spec["clustering"]
                else:
                    clustering = "default"
        if clustering != "never":
            # check samba clustermeta in rados
            if not check_rados_clustermeta(cephadm, smb_cluster_id, smb_nodes):
                log.error("rados clustermeta for samba not found")
                return 1
            # Verify CTDB health
            if not check_ctdb_health(smb_nodes, smb_cluster_id):
                log.error("ctdb health error")
                return 1
    except Exception as e:
        raise ConfigError(f"CTDB health error {e}")


def deploy_exo_smb(
    pri_cephadm,
    sec_cephadm,
    pri_installer,
    sec_installer,
    file_type,
    file_mount,
    smb_spec,
    smb_nodes,
    pri_client,
    sec_client,
    smb_subvolume_mode,
):
    """Deploy samba services with an external secondary cluster"""
    try:
        # Get smb service value from spec file
        smb_shares = []
        smb_subvols = []
        external_ceph_cluster_clients_name = []

        for spec in smb_spec:
            if spec["resource_type"] == "ceph.smb.cluster":
                smb_cluster_id = spec["cluster_id"]
            elif spec["resource_type"] == "ceph.smb.share":
                cephfs_vol = spec["cephfs"]["volume"]
                smb_subvol_group = spec["cephfs"]["path"].split("/")[2]
                smb_subvols.append(spec["cephfs"]["path"].split("/")[3])
                smb_shares.append(spec["share_id"])
            elif spec["resource_type"] == "ceph.smb.ext.cluster":
                external_ceph_cluster_clients_name.append(
                    spec["cluster"]["cephfs_user"]["name"]
                )

        # Create cephfs volume and smb sub volume
        create_vol_smb_subvol(
            pri_installer,
            cephfs_vol,
            smb_subvol_group,
            smb_subvols,
            smb_subvolume_mode,
        )

        # Get subvolume path
        for subvol in smb_subvols:
            subvol_path = CephAdm(pri_installer).ceph.fs.sub_volume.getpath(
                cephfs_vol, subvol, group_name=smb_subvol_group
            )
            for resource in smb_spec:
                if (
                    resource.get("resource_type") == "ceph.smb.share"
                    and resource.get("cephfs").get("path").split("/")[3] == subvol
                ):
                    resource["cephfs"]["path"] = subvol_path

        # Get primary cluster details
        cmd = "cephadm shell -- ceph config generate-minimal-conf"
        out = pri_installer.exec_command(sudo=True, cmd=cmd)[0]
        lines = out.splitlines()
        fsid = next(line.split("=")[1].strip() for line in lines if "fsid" in line)
        mon_host = next(
            line.split("=")[1].strip() for line in lines if "mon_host" in line
        )
        for resource in smb_spec:
            if resource.get("resource_type") == "ceph.smb.ext.cluster":
                resource["cluster"]["fsid"] = fsid
                resource["cluster"]["mon_host"] = mon_host

        # Get client details
        for external_ceph_cluster_client_name in external_ceph_cluster_clients_name:
            cmd = (
                f"cephadm shell -- ceph auth caps {external_ceph_cluster_client_name} "
                "mon 'allow *' "
                "mgr 'allow *' "
                "mds 'allow *' "
                "osd 'allow *'"
            )
            pri_installer.exec_command(sudo=True, cmd=cmd)[0]
            cmd = f"cephadm shell -- ceph auth get {external_ceph_cluster_client_name}"
            out = pri_installer.exec_command(sudo=True, cmd=cmd)[0]
            lines = out.splitlines()
            key = next(
                line.split("=", 1)[1].strip()
                for line in out.splitlines()
                if line.strip().startswith("key")
            )
            for resource in smb_spec:
                if resource.get("resource_type") == "ceph.smb.ext.cluster":
                    resource["cluster"]["cephfs_user"][
                        "name"
                    ] = external_ceph_cluster_client_name
                    resource["cluster"]["cephfs_user"]["key"] = key

        # Enable smb module
        enable_smb_module(sec_installer, smb_cluster_id)
        sleep(30)

        # Create smb spec file
        smb_spec_file = generate_smb_spec(sec_installer, file_type, smb_spec)

        # Apply smb spec file
        apply_smb_spec(sec_installer, smb_spec_file, file_mount)

        # Check smb service
        verify_smb_service(sec_installer, service_name="smb")
    except Exception as e:
        raise ConfigError(f"Fail to deploy samba services {e}")


def run(**kw):
    """Deploy samba with auth_mode 'user' using declarative style(Spec File)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get config
    config = kw.get("config")

    # Get clusters
    clusters = kw.get("ceph_cluster_dict")

    # Get primary cluster
    pri_cluster = clusters.get("ceph-pri", clusters[list(clusters.keys())[0]])

    # Get secoundary cluster
    sec_cluster = clusters.get("ceph-sec", clusters[list(clusters.keys())[0]])

    # Get primary cluster cephadm obj
    pri_cephadm = CephAdmin(cluster=pri_cluster, **config)

    # Get secoundary cluster cephadm obj
    sec_cephadm = CephAdmin(cluster=sec_cluster, **config)

    # Get primary cluster installer node
    pri_installer = pri_cluster.get_nodes(role="installer")[0]

    # Get secoundary cluster installer node
    sec_installer = sec_cluster.get_nodes(role="installer")[0]

    # Get spec file type
    file_type = config.get("file_type", "yaml")

    # Get smb spec file mount path
    file_mount = config.get("file_mount", "/tmp")

    # Get smb spec
    smb_spec = config.get("spec")

    # Get smb nodes
    smb_nodes = sec_cluster.get_nodes("smb")

    # Get primary client node
    pri_client = pri_cluster.get_nodes(role="client")[0]

    # Get secoundary client node
    sec_client = sec_cluster.get_nodes(role="client")[0]

    # Get smb subvolume mode
    smb_subvolume_mode = config.get("smb_subvolume_mode", "0777")

    # Get opertaions
    operations = config.get("operations", ["deploy_smb"])

    for operation in operations:
        if operation == "deploy_smb":
            # deploy smb services with an external secondary cluster
            deploy_exo_smb(
                pri_cephadm,
                sec_cephadm,
                pri_installer,
                sec_installer,
                file_type,
                file_mount,
                smb_spec,
                smb_nodes,
                pri_client,
                sec_client,
                smb_subvolume_mode,
            )
        elif operation == "verify_ctdb":
            # Verify ctdb clustering
            verify_exo_ctdb(sec_cephadm, smb_nodes, smb_spec)
        elif operation == "client_connection":
            # Verify client connection
            verify_exo_client_connection(
                smb_nodes,
                smb_spec,
                sec_client,
            )
        elif operation == "cleanup_smb":
            # Cleanup smb services
            cleanup_smb(
                pri_installer,
                sec_installer,
                smb_spec,
            )
    return 0
