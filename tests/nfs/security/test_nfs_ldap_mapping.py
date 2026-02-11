# Conf: conf/tentacle/nfs/1admin-7node-3client.yaml
import time
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph import CommandFailed
from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.utilities.packages import Package
from tests.nfs.nfs_operations import (
    cleanup_cluster,
    fuse_mount_retry,
    setup_nfs_cluster,
)
from tests.nfs.security.ldap_helper import LDAPSetup
from utility.log import Log
from utility.utils import setup_cluster_access

log = Log(__name__)


def configure_sssd(node, ldap_ip, ldap_setup):
    """Configure SSSD to use the LDAP server."""
    log.info("Configuring SSSD on {}".format(node.hostname))

    # Install necessary packages
    Package(node).install("sssd sssd-ldap openldap-clients authselect nmap-ncat")

    # Check network connectivity to LDAP server
    log.info(
        "Checking network connectivity from {} to LDAP server {}:{}".format(
            node.hostname, ldap_ip, ldap_setup.ldap_port
        )
    )
    try:
        node.exec_command(
            sudo=True, cmd="nc -z -w5 {} {}".format(ldap_ip, ldap_setup.ldap_port)
        )
        log.info("Network connectivity to LDAP server is OK.")
    except Exception as e:
        raise OperationFailedError(
            "Network connectivity check to LDAP server failed: {}".format(e)
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

    # Apply authselect and restart SSSD
    node.exec_command(sudo=True, cmd="authselect select sssd --force")
    node.exec_command(sudo=True, cmd="systemctl restart sssd")

    # Verify user resolution
    log.info(
        "Verifying user resolution for {} on {}".format(
            ldap_setup.test_user, node.hostname
        )
    )
    for i in range(10):
        try:
            out, _ = node.exec_command(
                sudo=True, cmd="id {}".format(ldap_setup.test_user)
            )
            if (
                "uid={}".format(ldap_setup.test_uid) in out
                and "gid={}".format(ldap_setup.test_gid) in out
            ):
                log.info(
                    "User {} resolved successfully: {}".format(
                        ldap_setup.test_user, out.strip()
                    )
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


def install_packages(client, pkgs):
    """Return the packages required for cthon tests"""
    try:
        for pkg in pkgs:
            Package(client).install(
                "git gcc nfs-utils time make libtirpc-devel",
            )
    except Exception as e:
        raise CommandFailed("Failed to install cthon packages \n error: {0}".format(e))


def verify_mapping(client_node, nfs_node, nfs_mount, ceph_cluster, ldap_setup):
    """Perform basic LDAP user mapping verification."""
    log.info("Performing LDAP user file creation test")

    # Allow writing to the mount point (chmod 777)
    client_node.exec_command(sudo=True, cmd="chmod 777 {}".format(nfs_mount))

    test_file = "{}/ldap_test_file".format(nfs_mount)

    # Create file as tester
    log.info("Creating file {} as user {}".format(test_file, ldap_setup.test_user))
    client_node.exec_command(
        sudo=True, cmd="sudo -u {} touch {}".format(ldap_setup.test_user, test_file)
    )

    # Verify Ownership on Client
    log.info("Verifying ownership on Client...")
    out, _ = client_node.exec_command(sudo=True, cmd="ls -lart {}".format(nfs_mount))
    log.info("Client mount contents:\n{}".format(out))

    out, _ = client_node.exec_command(
        sudo=True, cmd="stat -c '%u:%g' {}".format(test_file)
    )
    log.debug("stat output: {}".format(out.strip()))
    if out.strip() != "{}:{}".format(ldap_setup.test_uid, ldap_setup.test_gid):
        raise OperationFailedError(
            "Client ownership mismatch: expected {}:{}, got {}".format(
                ldap_setup.test_uid, ldap_setup.test_gid, out.strip()
            )
        )
    log.info("Client-side ownership verified.")

    # Verify Ownership on Backend (CephFS)
    log.info("Verifying ownership on Backend (CephFS)...")
    # Setup cluster access for ceph-fuse
    setup_cluster_access(ceph_cluster, nfs_node)

    # Mount CephFS on NFS node to verify backend storage
    ceph_mount_point = "/mnt/ceph_direct"
    nfs_node.exec_command(sudo=True, cmd="mkdir -p {}".format(ceph_mount_point))

    # Unmount if already mounted (cleanup safety)
    nfs_node.exec_command(
        sudo=True, cmd="umount {}".format(ceph_mount_point), check_ec=False
    )

    # Mount using ceph-fuse
    Package(nfs_node).install(
        "ceph-fuse",
    )
    fuse_mount_retry(client=nfs_node, mount=ceph_mount_point)

    try:
        # Find the file and check stats
        out, _ = nfs_node.exec_command(
            sudo=True, cmd="ls -lart {}".format(ceph_mount_point)
        )
        log.info("Backend mount contents:\n{}".format(out))

        cmd = "find {} -name ldap_test_file -printf '%U:%G'".format(ceph_mount_point)
        out, _ = nfs_node.exec_command(sudo=True, cmd=cmd)

        log.info("Backend file stats: {}".format(out.strip()))

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

    # Set ownership to root:ceph-users (GID 10005) and permissions to 770
    client_node.exec_command(
        sudo=True, cmd="chown root:{} {}".format(ldap_setup.test_gid, test_dir)
    )
    client_node.exec_command(sudo=True, cmd="chmod 770 {}".format(test_dir))
    log.info(
        "Set permissions 770 on {} (Owner: root, Group: {})".format(
            test_dir, ldap_setup.test_gid
        )
    )

    # TEST_USER (10005) should be able to write
    log.info(
        "Attempting write as {} (member of group {})".format(
            ldap_setup.test_user, ldap_setup.test_gid
        )
    )
    client_node.exec_command(
        sudo=True,
        cmd="sudo -u {} touch {}/file_allowed".format(ldap_setup.test_user, test_dir),
    )

    # TEST_USER_2 (10006) should NOT be able to write
    log.info(
        "Attempting write as {} (NOT member of group {})".format(
            ldap_setup.test_user_2, ldap_setup.test_gid
        )
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
        log.info("User {} correctly denied access.".format(ldap_setup.test_user_2))


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

    log.info("Removing user {} from LDAP...".format(ldap_setup.test_user))
    cmd = ("podman exec {} ldapdelete -x -D 'cn=admin,{}' " "-w {} 'uid={},{}'").format(
        ldap_setup.ldap_container_name,
        ldap_setup.ldap_base_dn,
        ldap_setup.ldap_admin_pass,
        ldap_setup.test_user,
        ldap_setup.ldap_base_dn,
    )
    ldap_setup.node.exec_command(sudo=True, cmd=cmd)

    # Invalidate SSSD cache
    log.info("Invalidating SSSD cache...")
    nfs_node.exec_command(sudo=True, cmd="sss_cache -E", check_ec=False)
    client_node.exec_command(sudo=True, cmd="sss_cache -E", check_ec=False)

    # Verify ls -ln shows numeric UID
    out, _ = client_node.exec_command(sudo=True, cmd="ls -ln {}".format(test_file))
    if str(ldap_setup.test_uid) not in out:
        log.warning(
            "Expected numeric UID {} in output, got: {}".format(
                ldap_setup.test_uid, out
            )
        )
    else:
        log.info("File correctly shows numeric UID after user removal.")


def verify_ldap_outage(client_node, nfs_node, nfs_mount, ldap_setup):
    """Verify NFS behavior during LDAP outage."""
    ldap_ip = ldap_setup.node.ip_address
    log.info("Simulating LDAP outage by blocking {} on NFS node...".format(ldap_ip))
    nfs_node.exec_command(
        sudo=True, cmd="iptables -A OUTPUT -d {} -j DROP".format(ldap_ip)
    )

    try:
        # Force cache clear to ensure lookup attempts
        nfs_node.exec_command(sudo=True, cmd="sss_cache -E", check_ec=False)

        log.info("Attempting to access NFS mount as LDAP user during outage...")
        # This command might hang or fail depending on configuration
        try:
            client_node.exec_command(
                sudo=True,
                cmd="sudo -u {} ls {}".format(ldap_setup.test_user, nfs_mount),
                timeout=10,
            )
            log.warning(
                "NFS operation succeeded despite LDAP outage (cache might still be active)"
            )
        except Exception:
            log.info("NFS operation failed/timed out as expected during LDAP outage.")

    finally:
        log.info("Restoring network connectivity...")
        nfs_node.exec_command(
            sudo=True, cmd="iptables -D OUTPUT -d {} -j DROP".format(ldap_ip)
        )

    # Verify recovery
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
        for f in futures:
            f.result()

    log.info("Performance test completed successfully.")


def run(ceph_cluster, **kw):
    """
    Verify that NFS-Ganesha correctly resolves LDAP users and maps UID/GID for CephFS NFS exports.
    """
    log.info("Starting NFS LDAP Mapping Test")
    config = kw.get("config")
    test_case = config.get("test_case", "verify_mapping")
    log.info("Running test case: {}".format(test_case))

    # Identify nodes
    installer = ceph_cluster.get_nodes(role="installer")[0]
    nfs_nodes = ceph_cluster.get_nodes(role="nfs")
    clients = ceph_cluster.get_nodes(role="client")

    if not nfs_nodes or not clients:
        raise OperationFailedError(
            "Test requires at least one NFS node and one Client node"
        )

    nfs_node = nfs_nodes[0]
    client_node = clients[0]

    # NFS Config
    nfs_name = "cephfs-nfs-ldap"
    fs_name = "cephfs"
    nfs_export = "/export_ldap"
    nfs_mount = "/mnt/nfs_ldap"
    port = "2049"
    version = config.get("nfs_version", "4.2")
    subvolume_group = "ganeshagroup"

    # Use Client 2 (node5) for LDAP server if available, otherwise fallback to installer
    ldap_node = clients[1] if len(clients) > 1 else installer

    try:
        Ceph(client_node).fs.sub_volume_group.create(
            volume=fs_name, group=subvolume_group
        )
        # 1. Setup LDAP Server on Client 2 (or installer)
        ldap_setup = LDAPSetup(ldap_node)
        ldap_setup.setup_ldap_container()
        ldap_ip = ldap_node.ip_address
        log.info("LDAP Server running on {} ({})".format(ldap_ip, ldap_node.hostname))

        # 2. Configure SSSD on NFS node and Client node
        configure_sssd(nfs_node, ldap_ip, ldap_setup)
        configure_sssd(client_node, ldap_ip, ldap_setup)

        # 3. Setup NFS Cluster and Export
        log.info("Setting up NFS Cluster and Export...")
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
        )

        # Allow writing to the mount point (chmod 777) for setup
        client_node.exec_command(sudo=True, cmd="chmod 777 {}".format(nfs_mount))

        # 4. Perform Test Operations
        if test_case == "verify_group_permissions":
            verify_group_permissions(client_node, nfs_mount, ldap_setup)
        elif test_case == "verify_user_change":
            verify_user_change(client_node, nfs_node, nfs_mount, ldap_setup)
        elif test_case == "verify_ldap_outage":
            verify_ldap_outage(client_node, nfs_node, nfs_mount, ldap_setup)
        elif test_case == "verify_performance":
            verify_performance(client_node, nfs_mount)
        else:
            # Default to basic mapping verification
            verify_mapping(client_node, nfs_node, nfs_mount, ceph_cluster, ldap_setup)

        return 0

    except Exception as e:
        log.error("Test failed: {}".format(e))
        return 1
    finally:
        log.info("Cleaning up")
        ldap_setup.cleanup_ldap()
        cleanup_cluster(
            client_node, nfs_mount, nfs_name, nfs_export, nfs_nodes=[nfs_node]
        )
