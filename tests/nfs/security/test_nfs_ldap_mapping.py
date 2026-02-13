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
from utility.log import Log
from utility.utils import setup_cluster_access

log = Log(__name__)

LDAP_CONTAINER_NAME = "ldap-server"
LDAP_PORT = 389
LDAP_ADMIN_PASS = "password"
LDAP_ORG = "Ceph"
LDAP_DOMAIN = "ceph.com"
LDAP_BASE_DN = "dc=ceph,dc=com"
TEST_USER = "tester"
TEST_UID = 10005
TEST_GID = 10005
TEST_USER_2 = "tester2"
TEST_UID_2 = 10006
TEST_GID_2 = 10006


def setup_ldap_container(node):
    """Deploy OpenLDAP container and populate it."""
    log.info(f"Setting up LDAP container on {node.hostname}")

    # Clean up any existing container
    node.exec_command(
        sudo=True, cmd=f"podman rm -f {LDAP_CONTAINER_NAME}", check_ec=False
    )

    # Run LDAP container
    cmd = (
        f"podman run -d -p {LDAP_PORT}:389 --name {LDAP_CONTAINER_NAME} "
        f"--env LDAP_ORGANISATION='{LDAP_ORG}' "
        f"--env LDAP_DOMAIN='{LDAP_DOMAIN}' "
        f"--env LDAP_ADMIN_PASSWORD='{LDAP_ADMIN_PASS}' "
        "docker.io/osixia/openldap:latest"
    )
    node.exec_command(sudo=True, cmd=cmd)

    # Open firewall port for LDAP
    log.info(f"Opening port {LDAP_PORT} on {node.hostname}")
    node.exec_command(
        sudo=True, cmd=f"firewall-cmd --add-port={LDAP_PORT}/tcp --permanent"
    )
    node.exec_command(sudo=True, cmd="firewall-cmd --reload")

    # Wait for LDAP to be ready
    log.info("Waiting for LDAP server to initialize...")
    time.sleep(15)

    # Create LDIF content
    ldif_content = f"""
dn: cn=ceph-users,{LDAP_BASE_DN}
objectClass: posixGroup
cn: ceph-users
gidNumber: {TEST_GID}

dn: uid={TEST_USER},{LDAP_BASE_DN}
objectClass: inetOrgPerson
objectClass: posixAccount
objectClass: top
cn: {TEST_USER}
sn: user
uid: {TEST_USER}
uidNumber: {TEST_UID}
gidNumber: {TEST_GID}
homeDirectory: /home/{TEST_USER}
loginShell: /bin/bash
userPassword: password123

dn: uid={TEST_USER_2},{LDAP_BASE_DN}
objectClass: inetOrgPerson
objectClass: posixAccount
objectClass: top
cn: {TEST_USER_2}
sn: user2
uid: {TEST_USER_2}
uidNumber: {TEST_UID_2}
gidNumber: {TEST_GID_2}
homeDirectory: /home/{TEST_USER_2}
loginShell: /bin/bash
userPassword: password123
"""
    ldif_path = "/tmp/users.ldif"
    node.remote_file(sudo=True, file_name=ldif_path, file_mode="w").write(ldif_content)

    # Add users to LDAP
    log.info("Adding users to LDAP...")
    cmd = (
        f"cat {ldif_path} | podman exec -i {LDAP_CONTAINER_NAME} ldapadd -x "
        f"-D 'cn=admin,{LDAP_BASE_DN}' -w {LDAP_ADMIN_PASS} -f /dev/stdin"
    )
    node.exec_command(sudo=True, cmd=cmd)
    log.info("LDAP user and group created.")


def configure_sssd(node, ldap_ip):
    """Configure SSSD to use the LDAP server."""
    log.info(f"Configuring SSSD on {node.hostname}")

    # Install necessary packages
    Package(node).install("sssd sssd-ldap openldap-clients authselect nmap-ncat")

    # Check network connectivity to LDAP server
    log.info(
        f"Checking network connectivity from {node.hostname} to LDAP server {ldap_ip}:{LDAP_PORT}"
    )
    try:
        node.exec_command(sudo=True, cmd=f"nc -z -w5 {ldap_ip} {LDAP_PORT}")
        log.info("Network connectivity to LDAP server is OK.")
    except Exception as e:
        raise OperationFailedError(
            f"Network connectivity check to LDAP server failed: {e}"
        )

    sssd_conf = f"""
[sssd]
services = nss, pam
domains = LDAP

[domain/LDAP]
id_provider = ldap
auth_provider = ldap
ldap_uri = ldap://{ldap_ip}
ldap_search_base = {LDAP_BASE_DN}
ldap_id_use_start_tls = False
cache_credentials = True
ldap_tls_reqcert = never
ldap_schema = rfc2307
ldap_default_bind_dn = cn=admin,{LDAP_BASE_DN}
ldap_default_authtok = {LDAP_ADMIN_PASS}
"""
    node.remote_file(sudo=True, file_name="/etc/sssd/sssd.conf", file_mode="w").write(
        sssd_conf
    )
    node.exec_command(sudo=True, cmd="chmod 600 /etc/sssd/sssd.conf")

    # Apply authselect and restart SSSD
    node.exec_command(sudo=True, cmd="authselect select sssd --force")
    node.exec_command(sudo=True, cmd="systemctl restart sssd")

    # Verify user resolution
    log.info(f"Verifying user resolution for {TEST_USER} on {node.hostname}")
    for i in range(10):
        try:
            out, _ = node.exec_command(sudo=True, cmd=f"id {TEST_USER}")
            if f"uid={TEST_UID}" in out and f"gid={TEST_GID}" in out:
                log.info(f"User {TEST_USER} resolved successfully: {out.strip()}")
                return
        except Exception:
            pass
        time.sleep(2)

    raise OperationFailedError(
        f"Failed to resolve LDAP user {TEST_USER} on {node.hostname}"
    )


def cleanup_ldap(node):
    """Cleanup LDAP container."""
    log.info("Cleaning up LDAP container...")
    node.exec_command(
        sudo=True, cmd=f"podman rm -f {LDAP_CONTAINER_NAME}", check_ec=False
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


def verify_mapping(client_node, nfs_node, nfs_mount, ceph_cluster):
    """Perform basic LDAP user mapping verification."""
    log.info("Performing LDAP user file creation test")

    # Allow writing to the mount point (chmod 777)
    client_node.exec_command(sudo=True, cmd=f"chmod 777 {nfs_mount}")

    test_file = f"{nfs_mount}/ldap_test_file"

    # Create file as tester
    log.info(f"Creating file {test_file} as user {TEST_USER}")
    client_node.exec_command(sudo=True, cmd=f"sudo -u {TEST_USER} touch {test_file}")

    # Verify Ownership on Client
    log.info("Verifying ownership on Client...")
    out, _ = client_node.exec_command(sudo=True, cmd=f"stat -c '%u:%g' {test_file}")
    if out.strip() != f"{TEST_UID}:{TEST_GID}":
        raise OperationFailedError(
            f"Client ownership mismatch: expected {TEST_UID}:{TEST_GID}, got {out.strip()}"
        )
    log.info("Client-side ownership verified.")

    # Verify Ownership on Backend (CephFS)
    log.info("Verifying ownership on Backend (CephFS)...")
    # Setup cluster access for ceph-fuse
    setup_cluster_access(ceph_cluster, nfs_node)

    # Mount CephFS on NFS node to verify backend storage
    ceph_mount_point = "/mnt/ceph_direct"
    nfs_node.exec_command(sudo=True, cmd=f"mkdir -p {ceph_mount_point}")

    # Unmount if already mounted (cleanup safety)
    nfs_node.exec_command(sudo=True, cmd=f"umount {ceph_mount_point}", check_ec=False)

    # Mount using ceph-fuse
    fuse_mount_retry(client=nfs_node, mount=ceph_mount_point)

    try:
        # Find the file and check stats
        cmd = f"find {ceph_mount_point} -name ldap_test_file -printf '%U:%G'"
        out, _ = nfs_node.exec_command(sudo=True, cmd=cmd)

        log.info(f"Backend file stats: {out.strip()}")

        if out.strip() != f"{TEST_UID}:{TEST_GID}":
            raise OperationFailedError(
                f"Backend ownership mismatch: expected {TEST_UID}:{TEST_GID}, got {out.strip()}"
            )

        log.info("Backend ownership verified.")
    finally:
        nfs_node.exec_command(sudo=True, cmd=f"umount {ceph_mount_point}")


def verify_group_permissions(client_node, nfs_mount):
    """Verify group-based access control."""
    log.info("Verifying group permissions...")
    test_dir = f"{nfs_mount}/group_restricted"
    client_node.exec_command(sudo=True, cmd=f"mkdir -p {test_dir}")

    # Set ownership to root:ceph-users (GID 10005) and permissions to 770
    client_node.exec_command(sudo=True, cmd=f"chown root:{TEST_GID} {test_dir}")
    client_node.exec_command(sudo=True, cmd=f"chmod 770 {test_dir}")

    # TEST_USER (10005) should be able to write
    log.info(f"Attempting write as {TEST_USER} (member of group {TEST_GID})")
    client_node.exec_command(
        sudo=True, cmd=f"sudo -u {TEST_USER} touch {test_dir}/file_allowed"
    )

    # TEST_USER_2 (10006) should NOT be able to write
    log.info(f"Attempting write as {TEST_USER_2} (NOT member of group {TEST_GID})")
    try:
        client_node.exec_command(
            sudo=True, cmd=f"sudo -u {TEST_USER_2} touch {test_dir}/file_denied"
        )
        raise OperationFailedError(
            f"User {TEST_USER_2} was able to write to restricted directory!"
        )
    except CommandFailed:
        log.info(f"User {TEST_USER_2} correctly denied access.")


def verify_user_change(client_node, nfs_node, nfs_mount, ldap_node):
    """Verify behavior when user is removed from LDAP."""
    log.info("Verifying user removal handling...")
    test_file = f"{nfs_mount}/user_change_test"
    client_node.exec_command(sudo=True, cmd=f"touch {test_file}")
    client_node.exec_command(sudo=True, cmd=f"chown {TEST_USER}:{TEST_GID} {test_file}")

    log.info(f"Removing user {TEST_USER} from LDAP...")
    cmd = (
        f"podman exec {LDAP_CONTAINER_NAME} ldapdelete -x -D 'cn=admin,{LDAP_BASE_DN}' "
        f"-w {LDAP_ADMIN_PASS} 'uid={TEST_USER},{LDAP_BASE_DN}'"
    )
    ldap_node.exec_command(sudo=True, cmd=cmd)

    # Invalidate SSSD cache
    log.info("Invalidating SSSD cache...")
    nfs_node.exec_command(sudo=True, cmd="sss_cache -E", check_ec=False)
    client_node.exec_command(sudo=True, cmd="sss_cache -E", check_ec=False)

    # Verify ls -ln shows numeric UID
    out, _ = client_node.exec_command(sudo=True, cmd=f"ls -ln {test_file}")
    if str(TEST_UID) not in out:
        log.warning(f"Expected numeric UID {TEST_UID} in output, got: {out}")
    else:
        log.info("File correctly shows numeric UID after user removal.")


def verify_ldap_outage(client_node, nfs_node, nfs_mount, ldap_ip):
    """Verify NFS behavior during LDAP outage."""
    log.info(f"Simulating LDAP outage by blocking {ldap_ip} on NFS node...")
    nfs_node.exec_command(sudo=True, cmd=f"iptables -A OUTPUT -d {ldap_ip} -j DROP")

    try:
        # Force cache clear to ensure lookup attempts
        nfs_node.exec_command(sudo=True, cmd="sss_cache -E", check_ec=False)

        log.info("Attempting to access NFS mount as LDAP user during outage...")
        # This command might hang or fail depending on configuration
        try:
            client_node.exec_command(
                sudo=True, cmd=f"sudo -u {TEST_USER} ls {nfs_mount}", timeout=10
            )
            log.warning(
                "NFS operation succeeded despite LDAP outage (cache might still be active)"
            )
        except Exception:
            log.info("NFS operation failed/timed out as expected during LDAP outage.")

    finally:
        log.info("Restoring network connectivity...")
        nfs_node.exec_command(sudo=True, cmd=f"iptables -D OUTPUT -d {ldap_ip} -j DROP")

    # Verify recovery
    time.sleep(5)
    log.info("Verifying recovery...")
    client_node.exec_command(sudo=True, cmd=f"sudo -u {TEST_USER} ls {nfs_mount}")
    log.info("Recovery successful.")


def verify_performance(client_node, nfs_mount):
    """Basic performance test with concurrent IO."""
    log.info("Starting performance test with concurrent IO...")

    def _io_task(i):
        filename = f"{nfs_mount}/perf_file_{i}"
        cmd = f"dd if=/dev/zero of={filename} bs=1M count=50 conv=fdatasync"
        client_node.exec_command(sudo=True, cmd=cmd)
        client_node.exec_command(sudo=True, cmd=f"rm -f {filename}")

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
    log.info(f"Running test case: {test_case}")

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
        setup_ldap_container(ldap_node)
        ldap_ip = ldap_node.ip_address
        log.info(f"LDAP Server running on {ldap_ip} ({ldap_node.hostname})")

        # 2. Configure SSSD on NFS node and Client node
        configure_sssd(nfs_node, ldap_ip)
        configure_sssd(client_node, ldap_ip)

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
        client_node.exec_command(sudo=True, cmd=f"chmod 777 {nfs_mount}")

        # 4. Perform Test Operations
        if test_case == "verify_group_permissions":
            verify_group_permissions(client_node, nfs_mount)
        elif test_case == "verify_user_change":
            verify_user_change(client_node, nfs_node, nfs_mount, ldap_node)
        elif test_case == "verify_ldap_outage":
            verify_ldap_outage(client_node, nfs_node, nfs_mount, ldap_ip)
        elif test_case == "verify_performance":
            verify_performance(client_node, nfs_mount)
        else:
            # Default to basic mapping verification
            verify_mapping(client_node, nfs_node, nfs_mount, ceph_cluster)

        return 0

    except Exception as e:
        log.error(f"Test failed: {e}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_ldap(ldap_node)
        cleanup_cluster(
            client_node, nfs_mount, nfs_name, nfs_export, nfs_nodes=[nfs_node]
        )
