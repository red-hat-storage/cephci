from cli.exceptions import OperationFailedError

CEPHADM_LOG_PATH = "/var/log/ceph/cephadm.log"


def run(ceph_cluster, **kw):
    """Verify spam logs not present in cephadm.log
    Args:
        **kw: Key/value pairs of configuration information
              to be used in the test.
    """
    node = ceph_cluster.get_nodes(role="_admin")[0]
    admin = node.ssh
    with admin().open_sftp().open(CEPHADM_LOG_PATH, "r") as file:
        content = file.read()
        # Spam logs were being generated as part of 'gather-facts'
        # Validate 'gather-facts' related logs are present in the log file
        if b"gather-facts" not in content:
            OperationFailedError(
                f"Failed: Expected log 'gather-facts' not found in {CEPHADM_LOG_PATH}"
            )
        # Spam logs had the string 'DEBUG sestatus' in them
        # Validate spam logs are not present in the log file
        if b"DEBUG sestatus:" in content:
            OperationFailedError(
                f"Failed: Spam log 'DEBUG sestatus' found in {CEPHADM_LOG_PATH}"
            )

    return 0
