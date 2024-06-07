from cli.exceptions import CephadmOpsExecutionError
from cli.utilities.utils import reboot_node


def run(ceph_cluster, **kwargs):
    """Check ceph 'conf' and 'keyring' files after upgrade and node reboot"""
    # Get config
    config = kwargs.get("config")

    # Get file path
    path = config.get("path")

    # Get files
    files = config.get("files")

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Reboot installer node
    reboot_node(installer)

    # Check files after reboot
    out = installer.exec_command(cmd=f"ls {path}", sudo=True)[0].split("\n")

    for file in files:
        if file not in out:
            raise CephadmOpsExecutionError(f"File {file} not present in {path}")
            return 1
    return 0
