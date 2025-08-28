import json
from time import sleep

from smb_operations import (
    apply_smb_spec,
    check_ctdb_health,
    check_rados_clustermeta,
    enable_smb_module,
    generate_smb_spec,
    smb_cifs_mount,
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


class SmbCaseSensitivityError(Exception):
    pass


def perfrom_io(
    smb_nodes,
    client,
    smb_shares,
    smb_user_name,
    smb_user_password,
    auth_mode,
    domain_realm,
    cifs_mount_point,
    mount_type,
):
    try:
        if mount_type == "linux":
            # Mount smb share with cifs
            smb_cifs_mount(
                smb_nodes[0],
                client,
                smb_shares[0],
                smb_user_name,
                smb_user_password,
                auth_mode,
                domain_realm,
                cifs_mount_point,
            )

            # perfrom  io as per bug #2386474
            cmd = f"touch {cifs_mount_point}/a {cifs_mount_point}/b"
            client.exec_command(sudo=True, cmd=cmd)
            cmd = f"ln {cifs_mount_point}/a {cifs_mount_point}/c"
            client.exec_command(sudo=True, cmd=cmd)
            cmd = f"rm {cifs_mount_point}/a"
            client.exec_command(sudo=True, cmd=cmd)
            cmd = f"mv {cifs_mount_point}/b {cifs_mount_point}/d"
            client.exec_command(sudo=True, cmd=cmd)
    except Exception as e:
        raise SmbCaseSensitivityError(f"Fail to perfrom IO: {e}")


def verify_mds_service(node, service_name):
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
        if out["service_type"] == "mds" and out["status"]["running"] > 0:
            return True
    if w.expired:
        raise SmbCaseSensitivityError(f"Service {service_name} is not deployed")


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

    # Get casesensitive
    casesensitive = config.get("casesensitive", None)

    # Get operations
    operations = config.get("operations")

    # Get mount type
    mount_type = config.get("mount_type", "linux")

    # Get cifs mount point
    cifs_mount_point = config.get("cifs_mount_point", "/mnt/smb")

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
            if operation == "deploy_smb":
                # Create volume and smb subvolume with case_sensitivity disable
                CephAdm(installer).ceph.fs.volume.create(cephfs_vol)
                CephAdm(installer).ceph.fs.sub_volume_group.create(
                    cephfs_vol, smb_subvol_group
                )
                for subvol in smb_subvols:
                    cmd = (
                        f"cephadm shell -- ceph fs subvolume create {cephfs_vol} {subvol} "
                        f"--group_name {smb_subvol_group} --mode {smb_subvolume_mode} "
                        f"--casesensitive={casesensitive}"
                    )
                    installer.exec_command(sudo=True, cmd=cmd)
                # Enable smb module
                enable_smb_module(installer, smb_cluster_id)
                sleep(30)
                # Create smb spec file
                smb_spec_file = generate_smb_spec(installer, file_type, smb_spec)
                # Apply smb spec file
                apply_smb_spec(installer, smb_spec_file, file_mount)
                # Check smb service
                verify_smb_service(installer, service_name="smb")

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

            if operation == "perfrom_io":
                # Pefrom IO
                perfrom_io(
                    smb_nodes,
                    client,
                    smb_shares,
                    smb_user_name,
                    smb_user_password,
                    auth_mode,
                    domain_realm,
                    cifs_mount_point,
                    mount_type,
                )

            if operation == "verify_service":
                # Verify smb services
                verify_smb_service(installer, service_name="smb")

                # Verify mds services
                verify_mds_service(installer, service_name="mds")

            if operation == "cleanup_smb":
                client.exec_command(
                    sudo=True,
                    cmd=f"rm -rf {cifs_mount_point}/*",
                )
                client.exec_command(
                    sudo=True,
                    cmd=f"umount {cifs_mount_point}",
                )
                smb_cleanup(installer, smb_shares, smb_cluster_id)

    except Exception as e:
        log.error(f"Failed to deploy samba with auth_mode 'user' : {e}")
        return 1
    return 0
