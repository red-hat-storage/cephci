from smb_operations import deploy_smb_service_declarative, smbclient_check_shares

from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def _parse_spec(spec):
    """Extract values needed by the SMB deployment helper."""
    values = {"shares": [], "subvolumes": [], "domain_realm": None}
    for resource in spec:
        resource_type = resource["resource_type"]
        if resource_type == "ceph.smb.cluster":
            values.update(
                {
                    "cluster_id": resource["cluster_id"],
                    "auth_mode": resource["auth_mode"],
                    "domain_realm": resource.get("domain_settings", {}).get("realm"),
                }
            )
            if not resource.get("remote_control", {}).get("locally_enabled"):
                raise ConfigError(
                    "SMB spec must set remote_control.locally_enabled to true"
                )
        elif resource_type == "ceph.smb.usersgroups":
            user = resource["values"]["users"][0]
            values.update({"username": user["name"], "password": user["password"]})
        elif resource_type == "ceph.smb.join.auth":
            values.update(
                {
                    "username": resource["auth"]["username"],
                    "password": resource["auth"]["password"],
                }
            )
        elif resource_type == "ceph.smb.share":
            cephfs = resource["cephfs"]
            values.update(
                {
                    "volume": cephfs["volume"],
                    "subvolume_group": cephfs["subvolumegroup"],
                }
            )
            values["subvolumes"].append(cephfs["subvolume"])
            values["shares"].append(resource["share_id"])

    required = (
        "cluster_id",
        "auth_mode",
        "username",
        "password",
        "volume",
        "subvolume_group",
    )
    missing = [name for name in required if name not in values]
    if missing:
        raise ConfigError(f"SMB spec is missing required values: {', '.join(missing)}")
    return values


def run(ceph_cluster, **kw):
    """Deploy SMB with local Unix-socket remote-control support."""
    config = kw.get("config") or {}
    file_type = config.get("file_type")
    spec = config.get("spec")
    if not file_type:
        raise ConfigError("Mandatory config 'file_type' not provided")
    if not spec:
        raise ConfigError("Mandatory config 'spec' not provided")

    installer = ceph_cluster.get_nodes(role="installer")[0]
    smb_nodes = ceph_cluster.get_nodes("smb")
    clients = ceph_cluster.get_nodes(role="client")
    if not smb_nodes:
        raise ConfigError("No node with the 'smb' role is available")
    if not clients:
        raise ConfigError("No node with the 'client' role is available")

    values = _parse_spec(spec)

    try:
        deploy_smb_service_declarative(
            installer,
            values["volume"],
            values["subvolume_group"],
            values["subvolumes"],
            values["cluster_id"],
            config.get("smb_subvolume_mode", "0777"),
            file_type,
            spec,
            config.get("file_mount", "/tmp"),
        )
        smbclient_check_shares(
            smb_nodes,
            clients[0],
            values["shares"],
            values["username"],
            values["password"],
            values["auth_mode"],
            values["domain_realm"],
        )
    except Exception as error:
        log.error("Failed to set up SMB remote control: %s", error)
        return 1
    return 0
