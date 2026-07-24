from smb_operations import (
    check_ctdb_health,
    check_rados_clustermeta,
    deploy_smb_service_declarative,
    smb_cifs_mount,
    smb_cleanup,
)

from ceph.ceph_admin import CephAdmin
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Deploy samba with auth_mode 'user' using declarative style(Spec File)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get config
    config = kw.get("config")

    # Get cephadm object
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
        elif spec["resource_type"] == "ceph.smb.usersgroups":
            smb_user_name = spec["values"]["users"][0]["name"]
            smb_user_password = spec["values"]["users"][0]["password"]
        elif spec["resource_type"] == "ceph.smb.join.auth":
            smb_user_name = spec["auth"]["username"]
            smb_user_password = spec["auth"]["password"]
        elif spec["resource_type"] == "ceph.smb.share":
            cephfs_vol = spec["cephfs"]["volume"]
            smb_subvol_group = spec["cephfs"]["subvolumegroup"]
            smb_user_access = spec["login_control"][0]["access"]
            smb_subvols.append(spec["cephfs"]["subvolume"])
            smb_shares.append(spec["share_id"])

    try:
        # Deploy smb services
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
            public_addrs,
        )

        # Check read-write permission
        if smb_user_access == "read-write":
            cmd = f"touch {cifs_mount_point}/test_file && ls {cifs_mount_point}"
            _, err = client.exec_command(sudo=True, cmd=cmd)
            if "Permission denied" in err:
                raise OperationFailedError(
                    f"Read-Write access test failed: '{smb_user_name}' "
                    f"could not create a file in share '{smb_shares[0]}'. "
                    "Expected write permission, but got 'Permission denied'"
                )
            else:
                log.info(
                    f"User: '{smb_user_name}' has confirmed read-write access to share '{smb_shares[0]}'"
                )

        # Check read permission
        elif smb_user_access == "read":
            cmd = f"touch {cifs_mount_point}/test_file && ls {cifs_mount_point}"
            _, err = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
            if "Permission denied" not in err:
                raise OperationFailedError(
                    f"Read-only access test failed: '{smb_user_name}' "
                    f"was able to create a file in share '{smb_shares[0]}'. "
                    "Expected read-only permission, but write access is allowed."
                )
            else:
                log.info(
                    f"User: '{smb_user_name}' has confirmed read-only access to share '{smb_shares[0]}'"
                )

    except Exception as e:
        log.error(f"Failed to deploy samba with auth_mode 'user' : {e}")
        return 1
    finally:
        client.exec_command(
            sudo=True,
            cmd=f"umount {cifs_mount_point}",
        )
        client.exec_command(
            sudo=True,
            cmd=f"rm -rf {cifs_mount_point}",
        )
        smb_cleanup(installer, smb_shares, smb_cluster_id)
    return 0
