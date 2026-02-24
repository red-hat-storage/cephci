"""
Create a non-root user on installer and client nodes for NFS tests.
When test config has 'user: <name>', this test creates that user, sets up
Ceph keyring/conf in the user's home, and prepares mount points. It then
stores the username in test_data["nfs_run_user"] so subsequent NFS tests
can run as that user (no run.py changes needed).
If 'user' is not set, the test no-ops (returns 0); root is used by default.
"""

from utility.log import Log

log = Log(__name__)

DEFAULT_NFS_USER_PASSWORD = "nfsuser123"


def _create_user_on_node(node, username, password=DEFAULT_NFS_USER_PASSWORD):
    """Create user and home dir on node. Idempotent."""
    node.exec_command(
        sudo=True,
        cmd=f"id -u {username} 2>/dev/null || "
        f"(useradd -m -s /bin/bash {username} && echo '{username}:{password}' | chpasswd)",
        check_ec=False,
    )
    node.exec_command(sudo=True, cmd=f"mkdir -p /home/{username}/.ceph")
    node.exec_command(sudo=True, cmd=f"chown -R {username}:{username} /home/{username}")

    # Grant targeted passwordless sudo for mount and umount commands
    sudoers_rule = f"{username} ALL=(ALL) NOPASSWD: /usr/bin/mount, /usr/bin/umount, /bin/mount, /bin/umount"
    node.exec_command(
        sudo=True,
        cmd=f"echo '{sudoers_rule}' > /etc/sudoers.d/{username}_mount && chmod 440 /etc/sudoers.d/{username}_mount"
    )


def _setup_ceph_for_user(node, username, installer_node):
    """Copy ceph.conf and admin keyring to user's .ceph and chown."""
    keyring_out, keyring_err = installer_node.exec_command(
        sudo=True,
        cmd="cephadm shell -- ceph auth get client.admin",
        long_running=False,
    )
    if not keyring_out or "[client.admin]" not in keyring_out:
        raise RuntimeError(
            f"Failed to get admin keyring: out={keyring_out!r} err={keyring_err!r}"
        )
    keyring_path = f"/home/{username}/.ceph/ceph.client.admin.keyring"
    f = node.remote_file(sudo=True, file_name=keyring_path, file_mode="w")
    f.write(keyring_out)
    f.flush()
    f.close()
    node.exec_command(sudo=True, cmd=f"chown {username}:{username} {keyring_path}")

    conf_out, conf_err = installer_node.exec_command(
        sudo=True,
        cmd="cephadm shell -- ceph config generate-minimal-conf",
        long_running=False,
    )
    conf_content = conf_out if conf_out else ""
    conf_path = f"/home/{username}/.ceph/ceph.conf"
    cf = node.remote_file(sudo=True, file_name=conf_path, file_mode="w")
    cf.write(conf_content)
    cf.flush()
    cf.close()
    node.exec_command(sudo=True, cmd=f"chown {username}:{username} {conf_path}")
    node.exec_command(sudo=True, cmd=f"chown -R {username}:{username} /home/{username}/.ceph")


def _prepare_mount_point(node, username, nfs_mount="/mnt/nfs"):
    """Create mount point and chown to user."""
    node.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mount}")
    node.exec_command(sudo=True, cmd=f"chown {username}:{username} {nfs_mount}")


def run(ceph_cluster, **kw):
    """
    Create non-root user for NFS when config has 'user'.
    Stores the username in test_data["nfs_run_user"] so other tests
    can pass run_user to setup_nfs_cluster without run.py changes.
    """
    config = kw.get("config", {})
    username = config.get("user")
    if not username:
        log.info("No 'user' in config; NFS tests will run as root (default).")
        return 0

    installer = ceph_cluster.get_nodes(role="installer")[0]
    all_nodes = ceph_cluster.get_nodes()
    clients = ceph_cluster.get_nodes(role="client")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs")

    log.info("Creating NFS run user '%s' on all nodes.", username)

    for node in all_nodes:
        _create_user_on_node(node, username)
        _setup_ceph_for_user(node, username, installer)

    for client in clients:
        _prepare_mount_point(client, username, nfs_mount)

    # So other NFS tests get run_user without run.py merging default_config
    test_data = kw.get("test_data")
    if test_data is not None:
        test_data["nfs_run_user"] = username

    log.info("NFS run user '%s' created; test_data['nfs_run_user'] set for later tests.", username)
    return 0
