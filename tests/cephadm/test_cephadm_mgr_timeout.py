from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError, UnexpectedStateError
from cli.utilities.utils import get_process_info
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify cephadm command timeout
    Args:
        **kw: Key/value pairs of configuration information
              to be used in the test.
    """
    admin = ceph_cluster.get_nodes(role="_admin")[0]

    # Set default_cephadm_command_timeout to 120 seconds
    CephAdm(admin).ceph.config.set(
        key="mgr/cephadm/default_cephadm_command_timeout", value="120", daemon="mgr"
    )
    log.info("Default cephadm command timeout set to 120 seconds")

    # Upload the 'cephadm-hold-lock-utility.py' script to the admin node
    admin.upload_file(
        "cli/utilities/cephadm-hold-lock-utility.py",
        "/root/cephadm-hold-lock-utility.py",
        sudo=True,
    )
    log.info("File 'cephadm-hold-lock-utility.py' uploaded successfully")

    # Execute the 'cephadm-hold-lock-utility.py' script
    fsid = CephAdm(admin).ceph.fsid()
    if not fsid:
        raise OperationFailedError("Failed to get cluster FSID")
    cmd = f"python3 /root/cephadm-hold-lock-utility.py hold-lock --fsid {fsid} & > /tmp/tmp.txt"
    admin.exec_command(sudo=True, long_running=True, cmd=cmd)

    # Check that hold-lock process is running under 'ps aux'
    out = get_process_info(admin, process="cephadm", awk="13")
    if "hold-lock" not in out:
        raise OperationFailedError("failed to list the lock task in 'ps aux'")
    log.info("hold-lock process is running")

    # Refresh devices
    conf = {"refresh": True}
    if not CephAdm(admin).ceph.orch.device.ls(**conf):
        raise OperationFailedError("Failed to refresh devices")

    # Check that the ceph-volume inventory process is running under 'ps aux'
    timeout, interval = 60, 10
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = get_process_info(admin, process="cephadm", awk="17")
        if "ceph-volume" in out:
            log.info("ceph-volume inventory process is running")
            break
    if w.expired:
        raise OperationFailedError(
            "failed to list the timeout ceph-volume task in 'ps aux'"
        )

    # Wait for 120 seconds for the timeout to trigger and check if the command timed out
    timeout, interval = 150, 10
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = CephAdm(admin).ceph.health(detail=True)
        if (
            'Command "cephadm ceph-volume -- inventory" timed out'
            and "HEALTH_WARN" in out
        ):
            log.info("ceph-volume inventory command timed out successfully")
            break
    if w.expired:
        raise UnexpectedStateError(
            "ceph-volume inventory command is not being timed out"
        )
    return 0
