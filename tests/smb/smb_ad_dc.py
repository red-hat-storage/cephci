from json import loads

from smb_operations import (
    deploy_smb_service_imperative,
    smb_cleanup,
    smbclient_check_shares,
)

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.containers import Container
from utility.log import Log

log = Log(__name__)


def _container_exec(smb_node, container_id, cmd):
    """Container exec private method to avoid duplicate calls"""
    out = Container(smb_node).exec(
        container=str(container_id), interactive=True, tty=True, cmds=cmd
    )[0]

    return out


def run(ceph_cluster, **kw):
    """Deploy Active Directory and join domain to perform basic operations"""
    # Get config
    config = kw.get("config")

    # Get cephfs volume
    cephfs_vol = config.get("cephfs_volume", "cephfs")

    # Get smb subvloume group
    smb_subvol_group = config.get("smb_subvolume_group", "smb")

    # Get smb subvloumes
    smb_subvols = config.get("smb_subvolumes", ["sv1", "sv2"])

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
    smb_shares = config.get("smb_shares", ["share1", "share2"])

    # Get smb path
    path = config.get("path", "/")

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get smb nodes
    smb_nodes = ceph_cluster.get_nodes("smb")

    # get client node
    client = ceph_cluster.get_nodes(role="client")[0]

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

        # Check smb share using smbclient
        smbclient_check_shares(
            smb_nodes,
            client,
            smb_shares,
            smb_user_name,
            smb_user_password,
            auth_mode,
            domain_realm,
        )

        # Get smb container ID
        smb_node = smb_nodes[0].hostname
        CephAdm(installer).ceph.orch.ps(refresh=True)
        conf = {"daemon_type": "smb", "format": "json-pretty"}
        service_info = loads(CephAdm(nodes=installer).ceph.orch.ps(**conf))
        container_id = [
            c.get("container_id") for c in service_info if c.get("hostname") == smb_node
        ][0]

        # Check the smb testparm
        cmd = "testparm -s"
        out = _container_exec(smb_nodes[0], container_id, cmd)
        if "Loaded services file OK" not in out:
            raise OperationFailedError("Smb config file is not correct")

        # Join the domain server
        cmd = f"net ads join -U {smb_user_name}%{smb_user_password}"
        out = _container_exec(smb_nodes[0], container_id, cmd)
        if "Joined" not in out:
            raise OperationFailedError("Failed to join domain")

        # Test server join
        cmd = "net ads testjoin"
        out = _container_exec(smb_nodes[0], container_id, cmd)
        if "Join is OK" not in out:
            raise OperationFailedError("Domain join failed")

        # Check the ads status
        cmd = "net ads status -P"
        out = _container_exec(smb_nodes[0], container_id, cmd)
        if domain_realm not in out:
            raise OperationFailedError("Domain status is unavailable")

        # Check server info
        cmd = "net ads info"
        out = _container_exec(smb_nodes[0], container_id, cmd)
        if domain_realm not in out:
            raise OperationFailedError("Domain info ins unavailable")

        # Check the trust secret
        cmd = "wbinfo -t"
        out = _container_exec(smb_nodes[0], container_id, cmd)
        if "succeeded" not in out:
            raise OperationFailedError(
                f"Checking the trust secret for {domain_realm} failed"
            )

        # Check Adminstrator user SID
        cmd = f"wbinfo --name-to-sid 'SAMBA\b{smb_user_name}'"
        out = _container_exec(smb_nodes[0], container_id, cmd)
        if not out:
            raise OperationFailedError(f"{smb_user_name} SID not found")

        # Check domain users
        cmd = "wbinfo --domain-users"
        out = _container_exec(smb_nodes[0], container_id, cmd)
        if not out:
            raise OperationFailedError("Failed to list domain users")

        # Check domain groups
        cmd = "wbinfo --domain-groups"
        out = _container_exec(smb_nodes[0], container_id, cmd)
        if not out:
            raise OperationFailedError("Failed to list domain groups")

    except Exception as e:
        log.error(f"Failed to deploy samba with auth_mode 'user' : {e}")
        return 1
    finally:
        smb_cleanup(installer, smb_shares, smb_cluster_id)
    return 0
