from smb_operations import (
    check_ctdb_health,
    check_rados_clustermeta,
    config_smb_images,
    deploy_smb_service_declarative,
    smb_cleanup,
    smbclient_check_shares,
)

from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.orch import Orch
from cephci.utils.build_info import CephTestManifest
from cli.exceptions import ConfigError
from cli.utilities.operations import wait_for_cluster_health
from utility.log import Log

log = Log(__name__)


class SmbUpgradeError(Exception):
    pass


def fetch_build_artifacts(product, release, build_type, platform):
    try:
        manifest_obj = CephTestManifest(
            product=product,
            release=release,
            build_type=build_type,
            platform=platform,
        )

        return manifest_obj
    except Exception as e:
        raise SmbUpgradeError(f"Fail to fetch build artifacts, Error {e}")


def deploy_smb(
    cephadm,
    installer,
    smb_subvolume_mode,
    file_type,
    smb_spec,
    file_mount,
    deployment_version,
    smb_nodes,
    client,
    product,
    platform,
    deployment_release,
):
    """Deploy smb services
    Args:
        cephadm (obj): cephadm obj
        installer (obj): Installer node obj
        smb_subvolume_mode (str): Smb subvolume mode
        file_type (str): Spec file type (yaml/json)
        smb_spec (dict): Smb spec details
        file_mount (str): File mount
        initial_ga_version(str): Intial cluster GA version (Before Upgrade)
        smb_nodes (list): List of smb nodes obj
        client (obj): Client node obj
        product (str): product type ibm or redhat
    """
    try:
        # Create manifest object
        manifest_obj = fetch_build_artifacts(
            product, deployment_version, deployment_release, platform
        )

        # Configure smb images
        samba_image = manifest_obj.images["samba_image"]
        samba_metrics_image = manifest_obj.images["samba_metrics_image"]
        config_smb_images(installer, samba_image, samba_metrics_image)

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
                smb_subvols.append(spec["cephfs"]["subvolume"])
                smb_shares.append(spec["share_id"])

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
            public_addrs,
        )
    except Exception as e:
        raise SmbUpgradeError(f"Fail to deploy smb services, Error {e}")


def upgrade(
    installer,
    orch,
    osd_flags,
    check_cluster_health,
    samba_image,
    samba_metrics_image,
    ceph_image,
):
    """Upgrade ceph cluster
    Args:
        installer (obj): Installer node obj
        orch (obj): Cephadm orch obj
        target_image (str): Target upgrade image
        upgrade_target_version (str): Upgrade version
        check_cluster_health (Bool): Cluster health check flag
    """
    try:
        # Check cluster health before upgrade
        if check_cluster_health:
            health = wait_for_cluster_health(installer, "HEALTH_OK", 300, 30)
            if not health:
                raise SmbUpgradeError("Cluster not in 'HEALTH_OK' state")

        # Add repo to all the nodes
        orch.set_tool_repo()

        # Update cephadm rpms
        # orch.install(**{"upgrade": True})
        orch.install()

        # Set osd flags
        for flag in osd_flags:
            cmd = f"cephadm shell -- ceph osd set {flag}"
            installer.exec_command(sudo=True, cmd=cmd)

        # Configure smb images
        config_smb_images(installer, samba_image, samba_metrics_image)

        # Check service versions vs available and target containers
        orch.upgrade_check(image=ceph_image)

        # Start Upgrade
        cmd = f"cephadm shell -- ceph orch upgrade start {ceph_image}"
        installer.exec_command(sudo=True, cmd=cmd)

        # Monitor upgrade status, till completion
        orch.monitor_upgrade_status()

        # Unset osd flags
        for flag in osd_flags:
            cmd = f"cephadm shell -- ceph osd unset {flag}"
            installer.exec_command(sudo=True, cmd=cmd)

        # Check cluster health after upgrade
        if check_cluster_health:
            health = wait_for_cluster_health(installer, "HEALTH_OK", 600, 60)
            if not health:
                raise SmbUpgradeError("Cluster not in 'HEALTH_OK' state")
    except Exception as e:
        raise SmbUpgradeError(f"Fail to upgrade ceph cluster, Error {e}")


def cleanup_smb(installer, smb_spec):
    """Cleanup smb services
    Args:
        installer (obj): Installer node obj
        smb_spec (dict): Smb spec details
    """
    try:
        # Get smb service value from spec file
        smb_shares = []
        for spec in smb_spec:
            if spec["resource_type"] == "ceph.smb.cluster":
                smb_cluster_id = spec["cluster_id"]
            elif spec["resource_type"] == "ceph.smb.share":
                smb_shares.append(spec["share_id"])
        # clean up smb services
        smb_cleanup(installer, smb_shares, smb_cluster_id)
    except Exception as e:
        raise SmbUpgradeError(f"Fail to cleanup smb services, Error {e}")


def run(ceph_cluster, **kw):
    """Perfrom SMB upgrade operations
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get config
    config = kw.get("config")

    # Get orch obj
    orch = Orch(cluster=ceph_cluster, **config)

    # Get operations
    operations = config.get("operations")

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

    # Get deployment version
    deployment_version = config.get("deployment_version", "8.1")

    # Get inital ga version
    deployment_version = config.get("deployment_version", "8.1")

    # Get deployment release
    deployment_release = config.get("deployment_release", "rc")

    # Get osd flags
    osd_flags = config.get("osd_flags", ["noout", "noscrub", "nodeep-scrub"])

    # Get check cluster health flag
    check_cluster_health = config.get("check_cluster_health", False)

    # Get product details
    product = config.get("product", "ibm")

    # Get platform details
    platform = config.get("platform", "rhel-9")

    # Get upgrade samba image
    manifest = config.get("manifest")
    samba_image = manifest.images["samba_image"]
    samba_metrics_image = manifest.images["samba_metrics_image"]

    # Get upgrade ceph image
    manifest = config.get("manifest")
    ceph_image = manifest.images["ceph-base"]

    for operation in operations:
        if operation == "deploy_smb":
            # deploy smb services
            deploy_smb(
                cephadm,
                installer,
                smb_subvolume_mode,
                file_type,
                smb_spec,
                file_mount,
                deployment_version,
                smb_nodes,
                client,
                product,
                platform,
                deployment_release,
            )
        elif operation == "upgrade":
            # upgrade ceph cluster
            upgrade(
                installer,
                orch,
                osd_flags,
                check_cluster_health,
                samba_image,
                samba_metrics_image,
                ceph_image,
            )
        elif operation == "cleanup_smb":
            # cleanup smb services
            cleanup_smb(installer, smb_spec)
    return 0
