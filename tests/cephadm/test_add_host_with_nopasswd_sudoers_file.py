from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError


def update_sudoers_file(node, file_content):
    """
    Updates the /etc/sudoers.d/cephuser file content
    Args:
        node(Ceph): Installer node
        file_content(str): The content to be updated
    """
    cmd = f"echo '{file_content}' > /etc/sudoers.d/*"
    node.exec_command(cmd=cmd, sudo=True)

    # Verify the update was successful
    out, _ = node.exec_command(cmd="cat /etc/sudoers.d/*", sudo=True)
    if file_content not in out:
        raise OperationFailedError("Failed to update sudoers.d file")


def run(ceph_cluster, **kw):
    """Verify host add Localhost with NOPASSWD removed in sudoers file
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    node = ceph_cluster.get_nodes("installer")[0]
    try:
        # Update the sudoers file without NOPASSWD
        content = "cephuser ALL=(ALL) ALL"
        update_sudoers_file(node, content)

        # Now verify the host (local host) addition
        out = CephAdm(node).ceph.orch.host.add(
            hostname=node.hostname, ip_address=node.ip_address, label="_admin"
        )
        if "Can't communicate with remote host " in out:
            raise OperationFailedError(
                "Failed to add host(localhost) with NOPASSWD removed in sudoers file. "
                "Possible regression of #2115462"
            )
    except Exception as e:
        raise OperationFailedError(
            f"Failure while validating the host addition with NOPASSWD removed in sudoers file. {e}"
        )
    finally:
        # Reset the sudoers file to the original configuration
        content = "$cephuser ALL=NOPASSWD(ALL) ALL"
        update_sudoers_file(node, content)
    return 0
