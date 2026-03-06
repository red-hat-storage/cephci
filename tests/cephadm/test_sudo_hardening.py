import json

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError


def run(ceph_cluster, **kw):
    """Verify ceph cephadm prepare-host-and-enable-sudo-hardening command

    This test validates the sudo hardening feature by:
    1. Getting list of all cluster hosts
    2. Running prepare-host-and-enable-sudo-hardening command
    3. Verifying command output contains expected message
    4. Checking sudoers.d file creation and content
    """
    # Get test configs
    config = kw.get("config", {})
    user = config.get("user", "cephuser")

    # Get installer node
    installer = ceph_cluster.get_nodes("installer")[0]

    # Step 1: Get list of all hostnames in the cluster
    cephadm = CephAdm(installer)
    host_list_out, _ = cephadm.ceph.orch.host.ls(format="json")

    # Parse hostnames from output
    hosts_data = json.loads(host_list_out)
    cluster_hostnames = [host.get("hostname") for host in hosts_data]

    if not cluster_hostnames:
        raise OperationFailedError("No hosts found in cluster")

    # Step 2: Run prepare-host-and-enable-sudo-hardening command
    cmd = f"ceph cephadm prepare-host-and-enable-sudo-hardening {user}"
    out, _ = installer.exec_command(cmd=cmd, sudo=True)

    # Step 3: Verify command output
    expected_message = "Sudo hardening is now active for all cluster operations."
    if expected_message not in out:
        raise OperationFailedError(
            f"Expected message '{expected_message}' not found in command output"
        )

    # Verify affected hosts message
    if "Affected hosts:" not in out:
        raise OperationFailedError(
            "Command output does not contain 'Affected hosts:' message"
        )

    # Verify all cluster hostnames are mentioned in output
    for hostname in cluster_hostnames:
        if hostname not in out:
            raise OperationFailedError(
                f"Hostname '{hostname}' not found in affected hosts list"
            )

    # Step 4: Verify sudoers.d file creation and content
    sudoers_file = f"/etc/sudoers.d/{user}"
    expected_content = f"{user} ALL=(ALL) NOPASSWD: /usr/libexec/cephadm_invoker.py"

    # Check if file exists and read content
    try:
        file_content, _ = installer.exec_command(cmd=f"cat {sudoers_file}", sudo=True)

        if expected_content not in file_content:
            raise OperationFailedError(
                f"Expected content '{expected_content}' not found in {sudoers_file}. "
                f"Actual content: {file_content}"
            )
    except Exception as e:
        raise OperationFailedError(f"Failed to verify sudoers file {sudoers_file}: {e}")

    return 0
