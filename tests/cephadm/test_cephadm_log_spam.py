from json import loads

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError

CEPHADM_LOG_PATH = "/var/log/ceph/cephadm.log"


def validate_spam_log_using_file(node):
    """Verify spam logs not present in cephadm.log file
    Args:
        **kw: Key/value pairs of configuration information
              to be used in the test.
    """
    admin = node.ssh

    # Validate log spam under cephadm.log file
    with admin().open_sftp().open(CEPHADM_LOG_PATH, "r") as file:
        content = file.read()
        # Spam logs were being generated as part of 'gather-facts'
        # Validate 'gather-facts' related logs are present in the log file
        if b"gather-facts" not in content:
            raise OperationFailedError(
                f"Failed: Expected log 'gather-facts' not found in {CEPHADM_LOG_PATH}"
            )
        # Spam logs had the string 'DEBUG sestatus' in them
        # Validate spam logs are not present in the log file
        if b"DEBUG sestatus:" in content:
            raise OperationFailedError(
                f"Failed: Spam log 'DEBUG sestatus' found in {CEPHADM_LOG_PATH}"
            )


def validate_spam_log_using_cmd(node, installer):
    """Verify spam logs not present under cephadm logs command
    Args:
        **kw: Key/value pairs of configuration information
              to be used in the test.
    """
    # Validate log spam under "cephadm logs" for mgr daemon
    spam = "log [INF] : Detected new or changed devices"
    fsid = CephAdm(node).ceph.fsid()
    if not fsid:
        raise OperationFailedError("Failed to get cluster FSID")

    mgr_ps = loads(CephAdm(node).ceph.orch.ps(daemon_type="mgr", format="json"))
    if not mgr_ps:
        raise OperationFailedError("Failed to get mgr ps")
    daemon_name = [
        key["daemon_name"] for key in mgr_ps if "installer" in key["daemon_name"]
    ]

    # Spam logs had the string "Detected new or changed devices" in them
    # These logs were being over-populated and getting triggered whenever
    # a device refresh operation was performed
    # Validate spam logs are not flooding cephadm logs after device refresh
    timeout, interval = 120, 10
    for w in WaitUntil(timeout=timeout, interval=interval):
        conf = {"refresh": True}
        refresh_devs = CephAdm(installer).ceph.orch.device.ls(**conf)
        if not refresh_devs:
            raise OperationFailedError("Failed to refresh devices")
        content = CephAdm(installer).logs(fsid, daemon_name[0])
        if not content:
            raise OperationFailedError("Failed to get cephadm logs")
        before_count = content.count(spam)
        refresh_devs = CephAdm(installer).ceph.orch.device.ls(**conf)
        if not refresh_devs:
            raise OperationFailedError("Failed to refresh devices")
        content = CephAdm(installer).logs(fsid, daemon_name[0])
        if not content:
            raise OperationFailedError("Failed to get cephadm logs")
        after_count = content.count(spam)
        # Compare spam log count after device refresh
        if before_count != after_count:
            raise OperationFailedError(
                f"Failed: Spam log '{spam}' is flooding cephadm logs"
            )


def run(ceph_cluster, **kw):
    """Validate spam logs not present in the logs based on the config option
    If type is "file" then validate spam under the cephadm.log file
    If type is "command" then validate spam under cephadm logs command

    Args:
        **kw: Key/value pairs of configuration information
              to be used in the test.
    """
    node = ceph_cluster.get_nodes(role="_admin")[0]
    installer = ceph_cluster.get_nodes(role="installer")[0]
    type = kw.get("config").get("type")

    if type == "file":
        validate_spam_log_using_file(node)

    if type == "command":
        validate_spam_log_using_cmd(node, installer)

    return 0
