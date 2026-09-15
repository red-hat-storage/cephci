"""LDAP helpers for NFS-Ganesha cephci tests."""

import time
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph import CommandFailed
from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from cli.utilities.packages import Package
from tests.nfs.nfs_operations import (
    cleanup_cluster,
    fuse_mount_retry,
    mount_cleanup_retry,
    setup_nfs_cluster,
)
from tests.nfs.security.ldap_helper import (
    DEFAULT_LDAP_BASE_DN,
    DEFAULT_LDAP_CONTAINER,
    DEFAULT_LDAP_MOUNT,
    DEFAULT_LDAP_PORT,
    DEFAULT_NFS_LDAP_CLUSTER,
    LDAPSetup,
    get_default_ldap_admin_password,
    load_stashed_admin_password,
    stash_admin_password,
)
from tests.nfs.security.security_utils import _nfs_ls_to_cluster_names
from utility.log import Log
from utility.utils import setup_cluster_access

log = Log(__name__)

DEFAULT_LDAP_EXPORT = "/export_ldap"
DEFAULT_FS_NAME = "cephfs"
DEFAULT_SUBVOLUME_GROUP = "ganeshagroup"
DEFAULT_NFS_PORT = "2049"

_NFT_LDAP_BLOCK_TABLE = "cephci_ldap_block"


def ldap_config_get(config, *keys, default=None):
    for key in keys:
        if key in config and config[key] is not None:
            return config[key]
    return default


def ldap_node_from_cluster(ceph_cluster, clients, config=None):
    config = config or {}
    idx = int(config.get("ldap_client_index", 1))
    if len(clients) > idx:
        return clients[idx]
    return ceph_cluster.get_nodes(role="installer")[0]


def build_ldap_setup(config, ldap_node):
    admin_pass = ldap_config_get(config, "ldap_admin_password")
    if not admin_pass and ldap_node:
        admin_pass = load_stashed_admin_password(ldap_node)
    if not admin_pass:
        admin_pass = get_default_ldap_admin_password()
    return LDAPSetup(
        ldap_node,
        ldap_container_name=ldap_config_get(
            config, "ldap_container_name", default=DEFAULT_LDAP_CONTAINER
        ),
        ldap_port=int(ldap_config_get(config, "ldap_port", default=DEFAULT_LDAP_PORT)),
        ldap_admin_pass=admin_pass,
        ldap_base_dn=ldap_config_get(
            config, "ldap_base_dn", default=DEFAULT_LDAP_BASE_DN
        ),
        test_user=ldap_config_get(config, "ldap_test_user", default="tester"),
        test_uid=int(ldap_config_get(config, "ldap_test_uid", default=10005)),
        test_gid=int(ldap_config_get(config, "ldap_test_gid", default=10005)),
        test_user_2=ldap_config_get(config, "ldap_test_user_2", default="tester2"),
        test_uid_2=int(ldap_config_get(config, "ldap_test_uid_2", default=10006)),
        test_gid_2=int(ldap_config_get(config, "ldap_test_gid_2", default=10006)),
    )


def _ldap_egress_block_backend(nfs_node):
    out, _ = nfs_node.exec_command(sudo=True, cmd="command -v iptables", check_ec=False)
    if out and str(out).strip():
        return "iptables"
    out_nft, _ = nfs_node.exec_command(sudo=True, cmd="command -v nft", check_ec=False)
    if out_nft and str(out_nft).strip():
        return "nft"
    raise OperationFailedError(
        "Neither iptables nor nft found on NFS node {}; cannot simulate LDAP outage "
        "(install iptables-nft or nftables).".format(nfs_node.hostname)
    )


def _block_ldap_egress_to_ip(nfs_node, ldap_ip, backend):
    if backend == "iptables":
        nfs_node.exec_command(
            sudo=True, cmd="iptables -A OUTPUT -d {} -j DROP".format(ldap_ip)
        )
        return
    nfs_node.exec_command(
        sudo=True,
        cmd=(
            "nft delete table inet {tbl} 2>/dev/null; "
            "nft add table inet {tbl}; "
            "nft add chain inet {tbl} output "
            "'{{ type filter hook output priority 0; policy accept; }}'; "
            "nft add rule inet {tbl} output ip daddr {ip} drop"
        ).format(tbl=_NFT_LDAP_BLOCK_TABLE, ip=ldap_ip),
    )


def _unblock_ldap_egress(nfs_node, ldap_ip, backend):
    if backend == "iptables":
        nfs_node.exec_command(
            sudo=True,
            cmd="iptables -D OUTPUT -d {} -j DROP".format(ldap_ip),
            check_ec=False,
        )
        return
    nfs_node.exec_command(
        sudo=True,
        cmd="nft delete table inet {}".format(_NFT_LDAP_BLOCK_TABLE),
        check_ec=False,
    )


def configure_sssd(node, ldap_ip, ldap_setup, force=False):
    """Configure SSSD to use the LDAP server (idempotent when user resolves)."""
    if not force:
        try:
            out, _ = node.exec_command(
                sudo=True, cmd="id {}".format(ldap_setup.test_user), check_ec=False
            )
            if "uid={}".format(ldap_setup.test_uid) in (out or ""):
                log.info(
                    "SSSD already resolves %s on %s",
                    ldap_setup.test_user,
                    node.hostname,
                )
                return
        except Exception:
            pass

    log.info("Configuring SSSD on %s", node.hostname)
    Package(node).install("sssd sssd-ldap openldap-clients authselect nmap-ncat")

    log.info(
        "Checking network connectivity from %s to LDAP server %s:%s",
        node.hostname,
        ldap_ip,
        ldap_setup.ldap_port,
    )
    try:
        node.exec_command(
            sudo=True, cmd="nc -z -w5 {} {}".format(ldap_ip, ldap_setup.ldap_port)
        )
        log.info("Network connectivity to LDAP server is OK.")
    except Exception as exc:
        raise OperationFailedError(
            "Network connectivity check to LDAP server failed: {}".format(exc)
        )

    sssd_conf = """
[sssd]
services = nss, pam
domains = LDAP

[domain/LDAP]
id_provider = ldap
auth_provider = ldap
ldap_uri = ldap://{ip}
ldap_search_base = {base_dn}
ldap_id_use_start_tls = False
cache_credentials = True
ldap_tls_reqcert = never
ldap_schema = rfc2307
ldap_default_bind_dn = cn=admin,{base_dn}
ldap_default_authtok = {password}
""".format(
        ip=ldap_ip,
        base_dn=ldap_setup.ldap_base_dn,
        password=ldap_setup.ldap_admin_pass,
    )
    node.remote_file(sudo=True, file_name="/etc/sssd/sssd.conf", file_mode="w").write(
        sssd_conf
    )
    node.exec_command(sudo=True, cmd="chmod 600 /etc/sssd/sssd.conf")
    node.exec_command(sudo=True, cmd="authselect select sssd --force")
    node.exec_command(sudo=True, cmd="systemctl restart sssd")

    log.info(
        "Verifying user resolution for %s on %s",
        ldap_setup.test_user,
        node.hostname,
    )
    for _ in range(10):
        try:
            out, _ = node.exec_command(
                sudo=True, cmd="id {}".format(ldap_setup.test_user)
            )
            if (
                "uid={}".format(ldap_setup.test_uid) in out
                and "gid={}".format(ldap_setup.test_gid) in out
            ):
                log.info(
                    "User %s resolved successfully: %s",
                    ldap_setup.test_user,
                    out.strip(),
                )
                return
        except Exception:
            pass
        time.sleep(2)

    raise OperationFailedError(
        "Failed to resolve LDAP user {} on {}".format(
            ldap_setup.test_user, node.hostname
        )
    )


def _ensure_subvolume_group(client_node, fs_name, group_name):
    try:
        Ceph(client_node).fs.sub_volume_group.create(volume=fs_name, group=group_name)
    except (CommandFailed, OperationFailedError) as exc:
        if "already exists" in str(exc).lower():
            log.info("Subvolume group %s already exists", group_name)
        else:
            raise


def _nfs_cluster_exists(client_node, nfs_name):
    try:
        clusters = Ceph(client_node).nfs.cluster.ls()
        return nfs_name in _nfs_ls_to_cluster_names(clusters)
    except (CommandFailed, OperationFailedError, ValueError):
        return False


def _mount_matches_version(client_node, mount_path, version):
    out, _ = client_node.exec_command(
        sudo=True,
        cmd="findmnt -n -o FSTYPE,OPTIONS {} 2>/dev/null || true".format(mount_path),
        check_ec=False,
    )
    if not out or not str(out).strip():
        return False
    ver = str(version)
    opts = str(out)
    if ver.startswith("4"):
        return "nfs4" in opts or "vers=4" in opts or "nfsvers=4" in opts
    return "vers={}".format(ver) in opts or "nfsvers={}".format(ver) in opts


def remount_ldap_nfs_export(
    client_node,
    nfs_node,
    mount_path,
    nfs_export,
    version,
    port=DEFAULT_NFS_PORT,
):
    """Remount the LDAP NFS export with the requested NFS version."""
    mount_cleanup_retry(client_node, mount_path)
    Unmount(client_node).unmount(mount_path)
    Mount(client_node).nfs(
        mount=mount_path,
        version=version,
        port=str(port),
        server=nfs_node.hostname,
        export=nfs_export,
    )
    log.info(
        "Remounted %s:%s -> %s (vers=%s) on %s",
        nfs_node.hostname,
        nfs_export,
        mount_path,
        version,
        client_node.hostname,
    )


def ensure_ldap_nfs_stack(
    ceph_cluster,
    client_node,
    nfs_node,
    config,
    ldap_setup,
    nfs_name=None,
    nfs_export=None,
    nfs_mount=None,
    fs_name=None,
    subvolume_group=None,
):
    """Ensure NFS cluster exists and client mount matches requested version."""
    nfs_name = nfs_name or ldap_config_get(
        config, "nfs_cluster_name", default=DEFAULT_NFS_LDAP_CLUSTER
    )
    nfs_export = nfs_export or ldap_config_get(
        config, "ldap_export_path", "nfs_export", default=DEFAULT_LDAP_EXPORT
    )
    nfs_mount = nfs_mount or ldap_config_get(
        config, "ldap_client_mount", "nfs_mount", default=DEFAULT_LDAP_MOUNT
    )
    fs_name = fs_name or ldap_config_get(config, "fs_name", default=DEFAULT_FS_NAME)
    subvolume_group = subvolume_group or ldap_config_get(
        config, "subvolume_group", default=DEFAULT_SUBVOLUME_GROUP
    )
    version = ldap_config_get(config, "nfs_version", default="4.2")
    port = str(ldap_config_get(config, "nfs_port", default=DEFAULT_NFS_PORT))

    _ensure_subvolume_group(client_node, fs_name, subvolume_group)

    if not _nfs_cluster_exists(client_node, nfs_name):
        log.info("Deploying LDAP NFS cluster %s", nfs_name)
        setup_nfs_cluster(
            clients=[client_node],
            nfs_server=nfs_node.hostname,
            port=port,
            version=version,
            nfs_name=nfs_name,
            nfs_mount=nfs_mount,
            fs_name=fs_name,
            export=nfs_export,
            fs=fs_name,
            ceph_cluster=ceph_cluster,
            enable_rdma=config.get("enable_rdma", False),
            rdma_port=config.get("rdma_port"),
        )
    elif not _mount_matches_version(client_node, nfs_mount, version):
        remount_ldap_nfs_export(
            client_node,
            nfs_node,
            nfs_mount,
            nfs_export,
            version,
            port=port,
        )
    else:
        log.info(
            "LDAP NFS cluster %s and mount %s (vers=%s) already present",
            nfs_name,
            nfs_mount,
            version,
        )

    client_node.exec_command(sudo=True, cmd="chmod 777 {}".format(nfs_mount))
    return nfs_name, nfs_export, nfs_mount, version


def provision_ldap_environment(
    ldap_setup,
    ldap_node,
    nfs_node,
    client_node,
    ceph_cluster,
    config,
):
    """Stand up OpenLDAP, SSSD, and the shared LDAP NFS export."""
    ldap_setup.setup_ldap_container()
    stash_admin_password(ldap_node, ldap_setup.ldap_admin_pass)
    ldap_ip = ldap_node.ip_address
    log.info("LDAP server on %s (%s)", ldap_ip, ldap_node.hostname)

    configure_sssd(nfs_node, ldap_ip, ldap_setup)
    configure_sssd(client_node, ldap_ip, ldap_setup)

    nfs_name, nfs_export, nfs_mount, _version = ensure_ldap_nfs_stack(
        ceph_cluster,
        client_node,
        nfs_node,
        config,
        ldap_setup,
    )
    return ldap_ip, nfs_name, nfs_export, nfs_mount


def verify_mapping(client_node, nfs_node, nfs_mount, ceph_cluster, ldap_setup):
    """Perform basic LDAP user mapping verification."""
    log.info("Performing LDAP user file creation test")
    client_node.exec_command(sudo=True, cmd="chmod 777 {}".format(nfs_mount))

    test_file = "{}/ldap_test_file".format(nfs_mount)
    log.info("Creating file %s as user %s", test_file, ldap_setup.test_user)
    client_node.exec_command(
        sudo=True, cmd="sudo -u {} touch {}".format(ldap_setup.test_user, test_file)
    )

    log.info("Verifying ownership on Client...")
    out, _ = client_node.exec_command(sudo=True, cmd="ls -lart {}".format(nfs_mount))
    log.info("Client mount contents:\n%s", out)

    out, _ = client_node.exec_command(
        sudo=True, cmd="stat -c '%u:%g' {}".format(test_file)
    )
    if out.strip() != "{}:{}".format(ldap_setup.test_uid, ldap_setup.test_gid):
        raise OperationFailedError(
            "Client ownership mismatch: expected {}:{}, got {}".format(
                ldap_setup.test_uid, ldap_setup.test_gid, out.strip()
            )
        )
    log.info("Client-side ownership verified.")

    log.info("Verifying ownership on Backend (CephFS)...")
    setup_cluster_access(ceph_cluster, nfs_node)
    ceph_mount_point = "/mnt/ceph_direct"
    nfs_node.exec_command(sudo=True, cmd="mkdir -p {}".format(ceph_mount_point))
    nfs_node.exec_command(
        sudo=True, cmd="umount {}".format(ceph_mount_point), check_ec=False
    )
    Package(nfs_node).install("ceph-fuse")
    fuse_mount_retry(client=nfs_node, mount=ceph_mount_point)

    try:
        out, _ = nfs_node.exec_command(
            sudo=True, cmd="ls -lart {}".format(ceph_mount_point)
        )
        log.info("Backend mount contents:\n%s", out)

        cmd = "find {} -name ldap_test_file -printf '%U:%G'".format(ceph_mount_point)
        out, _ = nfs_node.exec_command(sudo=True, cmd=cmd)
        log.info("Backend file stats: %s", out.strip())

        if out.strip() != "{}:{}".format(ldap_setup.test_uid, ldap_setup.test_gid):
            raise OperationFailedError(
                "Backend ownership mismatch: expected {}:{}, got {}".format(
                    ldap_setup.test_uid, ldap_setup.test_gid, out.strip()
                )
            )
        log.info("Backend ownership verified.")
    finally:
        nfs_node.exec_command(sudo=True, cmd="umount {}".format(ceph_mount_point))


def verify_group_permissions(client_node, nfs_mount, ldap_setup):
    """Verify group-based access control."""
    log.info("Verifying group permissions...")
    test_dir = "{}/group_restricted".format(nfs_mount)
    client_node.exec_command(sudo=True, cmd="mkdir -p {}".format(test_dir))
    client_node.exec_command(
        sudo=True, cmd="chown root:{} {}".format(ldap_setup.test_gid, test_dir)
    )
    client_node.exec_command(sudo=True, cmd="chmod 770 {}".format(test_dir))

    log.info(
        "Attempting write as %s (member of group %s)",
        ldap_setup.test_user,
        ldap_setup.test_gid,
    )
    client_node.exec_command(
        sudo=True,
        cmd="sudo -u {} touch {}/file_allowed".format(ldap_setup.test_user, test_dir),
    )

    log.info(
        "Attempting write as %s (NOT member of group %s)",
        ldap_setup.test_user_2,
        ldap_setup.test_gid,
    )
    try:
        client_node.exec_command(
            sudo=True,
            cmd="sudo -u {} touch {}/file_denied".format(
                ldap_setup.test_user_2, test_dir
            ),
        )
        raise OperationFailedError(
            "User {} was able to write to restricted directory!".format(
                ldap_setup.test_user_2
            )
        )
    except CommandFailed:
        log.info("User %s correctly denied access.", ldap_setup.test_user_2)


def verify_user_change(client_node, nfs_node, nfs_mount, ldap_setup):
    """Verify behavior when user is removed from LDAP."""
    log.info("Verifying user removal handling...")
    test_file = "{}/user_change_test".format(nfs_mount)
    client_node.exec_command(sudo=True, cmd="touch {}".format(test_file))
    client_node.exec_command(
        sudo=True,
        cmd="chown {}:{} {}".format(
            ldap_setup.test_user, ldap_setup.test_gid, test_file
        ),
    )

    log.info("Removing user %s from LDAP...", ldap_setup.test_user)
    ldap_setup.delete_test_user()

    log.info("Invalidating SSSD cache...")
    nfs_node.exec_command(sudo=True, cmd="sss_cache -E", check_ec=False)
    client_node.exec_command(sudo=True, cmd="sss_cache -E", check_ec=False)

    out, _ = client_node.exec_command(sudo=True, cmd="ls -ln {}".format(test_file))
    if str(ldap_setup.test_uid) not in out:
        log.warning(
            "Expected numeric UID %s in output, got: %s",
            ldap_setup.test_uid,
            out,
        )
    else:
        log.info("File correctly shows numeric UID after user removal.")


def verify_ldap_outage(client_node, nfs_node, nfs_mount, ldap_setup):
    """Verify NFS behavior during LDAP outage."""
    ldap_ip = ldap_setup.node.ip_address
    backend = _ldap_egress_block_backend(nfs_node)
    log.info(
        "Simulating LDAP outage by blocking %s on NFS node %s (using %s)...",
        ldap_ip,
        nfs_node.hostname,
        backend,
    )
    _block_ldap_egress_to_ip(nfs_node, ldap_ip, backend)

    try:
        nfs_node.exec_command(sudo=True, cmd="sss_cache -E", check_ec=False)
        log.info("Attempting to access NFS mount as LDAP user during outage...")
        try:
            client_node.exec_command(
                sudo=True,
                cmd="sudo -u {} ls {}".format(ldap_setup.test_user, nfs_mount),
                timeout=10,
            )
            log.warning(
                "NFS operation succeeded despite LDAP outage "
                "(cache might still be active)"
            )
        except Exception:
            log.info("NFS operation failed/timed out as expected during LDAP outage.")
    finally:
        log.info("Restoring network connectivity...")
        _unblock_ldap_egress(nfs_node, ldap_ip, backend)

    time.sleep(5)
    log.info("Verifying recovery...")
    client_node.exec_command(
        sudo=True, cmd="sudo -u {} ls {}".format(ldap_setup.test_user, nfs_mount)
    )
    log.info("Recovery successful.")


def verify_performance(client_node, nfs_mount):
    """Basic performance test with concurrent IO."""
    log.info("Starting performance test with concurrent IO...")

    def _io_task(i):
        filename = "{}/perf_file_{}".format(nfs_mount, i)
        cmd = "dd if=/dev/zero of={} bs=1M count=50 conv=fdatasync".format(filename)
        client_node.exec_command(sudo=True, cmd=cmd)
        client_node.exec_command(sudo=True, cmd="rm -f {}".format(filename))

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(_io_task, i) for i in range(10)]
        for future in futures:
            future.result()

    log.info("Performance test completed successfully.")


def cleanup_ldap_nfs_stack(client_node, nfs_mount, nfs_name, nfs_export, nfs_nodes):
    """Remove LDAP NFS cluster and client mount."""
    cleanup_cluster(client_node, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_nodes)


def cleanup_ldap_environment(
    ldap_setup, client_node, nfs_mount, nfs_name, nfs_export, nfs_nodes
):
    """Remove LDAP container and NFS stack."""
    if ldap_setup:
        ldap_setup.cleanup_ldap()
    cleanup_ldap_nfs_stack(client_node, nfs_mount, nfs_name, nfs_export, nfs_nodes)
