import json

from smb_operations import (
    deploy_smb_service_imperative,
    generate_apply_smb_spec,
    smb_cleanup,
)

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


def verify_service(node, service_name):
    """
    Check service is up
    Args:
        node (str): monitor node object
        service_name (str): service name
    return: (bool)
    """
    # Check smb service is up
    service_name = service_name.split(".")[0]
    timeout, interval = 90, 30
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = json.loads(
            CephAdm(node).ceph.orch.ls(service_type=service_name, format="json-pretty")
        )[0]
        if out["service_type"] == service_name and out["status"]["running"] > 0:
            break
    if w.expired:
        raise OperationFailedError(f"Service {service_name} is not deployed")


def run(ceph_cluster, **kw):
    """Validate SMB share options are getting overwritten after restarting services
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get config
    config = kw.get("config")

    # Get smb spec
    smb_spec = config.get("spec")

    # Get spec file type
    file_type = config.get("file_type")

    # Get smb spec file mount path
    file_mount = config.get("file_mount", "/tmp")

    # Get service need to restart
    restart_service = config.get("restart_service")

    # Get service need to change_resource_names
    change_resource_names = config.get("change_resource_names")

    # Get service need to change_resource_option
    change_resource_option = config.get("change_resource_option")

    # Get service need to change_value
    change_value = config.get("change_value")

    # Get cephfs volume
    cephfs_vol = config.get("cephfs_volume", "cephfs")

    # Get smb subvloume group
    smb_subvol_group = config.get("smb_subvolume_group", "smb")

    # Get smb subvloumes
    smb_subvols = config.get("smb_subvolumes", ["sv1"])

    # Get smb subvolume mode
    smb_subvolume_mode = config.get("smb_subvolume_mode", "0777")

    # Get smb cluster id
    smb_cluster_id = config.get("smb_cluster_id", "smb1")

    # Get auth_mode
    auth_mode = config.get("auth_mode", "user")

    # Get domain_realm
    domain_realm = config.get("domain_realm", None)

    # Get custom_dns
    custom_dns = config.get("custom_dns", None)

    # Get smb user name
    smb_user_name = config.get("smb_user_name", "user1")

    # Get smb user password
    smb_user_password = config.get("smb_user_password", "passwd")

    # Get smb shares
    smb_shares = config.get("smb_shares", ["share1"])

    # Get smb path
    path = config.get("path", "/")

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    try:
        # deploy smb services
        deploy_smb_service_imperative(
            installer,
            cephfs_vol,
            smb_subvol_group,
            smb_subvols,
            smb_subvolume_mode,
            smb_cluster_id,
            auth_mode,
            smb_user_name,
            smb_user_password,
            smb_shares,
            path,
            domain_realm,
            custom_dns,
        )

        # Change smb share option using spec
        generate_apply_smb_spec(installer, file_type, smb_spec, file_mount)

        # Restart services
        out = CephAdm(installer).ceph.orch.restart(restart_service)
        if "Scheduled" not in out:
            raise OperationFailedError(f"Fail to restart {restart_service}")

        # Checking service status
        verify_service(installer, restart_service)

        # Check change option
        data = json.loads(CephAdm(installer).ceph.smb.show(change_resource_names))
        if data[change_resource_option] != change_value:
            raise OperationFailedError(
                f"Value {change_resource_option} not changed after service restart"
            )

    except Exception as e:
        log.error(f"Failed to deploy samba with auth_mode 'user' : {e}")
        return 1
    finally:
        smb_cleanup(installer, smb_shares, smb_cluster_id)
    return 0
