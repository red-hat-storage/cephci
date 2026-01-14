from smb_operations import (
    check_ctdb_health,
    check_rados_clustermeta,
    deploy_smb_service_declarative,
    smb_cleanup,
    smbclient_check_shares,
)

from ceph.ceph_admin import CephAdmin
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def add_smbtorture(client, cloud_type):
    try:
        if cloud_type != "ibmc":
            # Fetch os version
            cmd = '. /etc/os-release && echo "${VERSION_ID%%.*}"'
            os_version = client.exec_command(
                sudo=True,
                cmd=cmd,
            )[0].strip()

            # Enable repo
            cmd = f"subscription-manager repos --enable codeready-builder-for-rhel-{os_version}-x86_64-rpms"
            client.exec_command(
                sudo=True,
                cmd=cmd,
            )

        # Install samba-test
        cmd = "dnf install -y --nogpgcheck samba-test"
        client.exec_command(
            sudo=True,
            cmd=cmd,
        )
    except Exception as e:
        raise OperationFailedError(f"Fail to add smbtorture, Error {e}")


def run_smbtorture_tc(
    client, smb_node, smb_shares, smb_user_name, smb_user_password, smbtorture_test
):
    # Run Smbtorture tc
    for smb_share in smb_shares:
        cmd = f"smbtorture //{smb_node.ip_address}/{smb_share} -U {smb_user_name}%{smb_user_password} {smbtorture_test}"
        out = client.exec_command(sudo=True, long_running=True, cmd=cmd)
        if out and smbtorture_test not in [
            "smb2.maximum_allowed",
            "smb2.rename",
            "smb2.session",
            "smb2.create",
            "smb2.timestamps",
            "smb2.charset",
            "smb2.credits",
        ]:
            return "Fail"
    return "Pass"


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

    # Get smb deployment
    smb_service_deployment = config.get("smb_service_deployment", False)

    # Get smb cleanup
    smb_service_cleanup = config.get("smb_service_cleanup", False)

    # Get smbtorture setup
    setup_smbtorture = config.get("setup_smbtorture", False)

    # Get smbtorture test case
    smbtorture_test = config.get("smbtorture_test", False)

    # Get cloud type
    cloud_type = config.get("cloud-type")

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
        if smb_service_deployment:
            # deploy smb services
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

        if setup_smbtorture:
            # Setup smbtorture
            add_smbtorture(client, cloud_type)

        # Execute Smbtorture test
        out = run_smbtorture_tc(
            client,
            smb_nodes[0],
            smb_shares,
            smb_user_name,
            smb_user_password,
            smbtorture_test,
        )
        if out == "Fail":
            log.error(f"Smbtorture tc {smbtorture_test} failed")
            return 1

    except Exception as e:
        log.error(f"Failed to deploy samba with auth_mode 'user' : {e}")
        return 1
    finally:
        if smb_service_cleanup:
            smb_cleanup(installer, smb_shares, smb_cluster_id)
    return 0
