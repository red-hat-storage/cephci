from smb_operations import (
    check_ctdb_health,
    check_rados_clustermeta,
    deploy_smb_service_imperative,
    smb_cleanup,
    smbclient_check_shares,
)

from ceph.ceph_admin import CephAdmin
from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import CephadmOpsExecutionError
from utility.log import Log

log = Log(__name__)


def check_smb_module(installer, smb_cluster_id):
    """Enable smb mgr module
    Args:
        installer (obj): Installer node obj
    """
    # check mgr module is enabled
    timeout, interval = 150, 30
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = CephAdm(installer).ceph.mgr.module.ls()
        module_status = {
            module.split()[0]: " ".join(module.split()[1:])
            for module in out.strip().split("\n")[1:]
        }
        if module_status["smb"] == "on":
            return True
    if w.expired:
        raise CephadmOpsExecutionError(f"Smb cluster {smb_cluster_id} not available")


def run(ceph_cluster, **kw):
    """Deploy samba with auth_mode 'user' using imperative style(CLI Commands)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get config
    config = kw.get("config")

    # Get cephadm obj
    cephadm = CephAdmin(cluster=ceph_cluster, **config)

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

    # Check ctdb clustering
    clustering = config.get("clustering", "default")

    try:
        # Check smb mgr module
        check_smb_module(installer, smb_cluster_id)

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
            clustering,
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
        )
    except Exception as e:
        log.error(f"Failed to deploy samba with auth_mode 'user' : {e}")
        return 1
    finally:
        smb_cleanup(installer, smb_shares, smb_cluster_id)
    return 0
