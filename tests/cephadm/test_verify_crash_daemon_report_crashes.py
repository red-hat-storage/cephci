from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.utils import get_all_running_pids, kill_process
from utility.log import Log

log = Log(__name__)


def get_crash_dir(node):
    fsid = CephAdm(node).ceph.fsid()
    cmd = f"ls /var/lib/ceph/{fsid}/crash"
    out, _ = node.exec_command(cmd=cmd, sudo=True)
    return out


def run(ceph_cluster, **kw):
    """Verify the crash daemon report any crashes that happened
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get contents of the crash dir before generating a crash
    crash_dir = get_crash_dir(installer)

    # Generate a crash by killing an osd pid
    osd_pids = get_all_running_pids(installer, "osd")
    kill_process(node=installer, pid_to_kill=osd_pids[0])

    # Check if the crash is posted inside the crash dir
    for w in WaitUntil(timeout=120, interval=10):
        crash_dir_post_kill = get_crash_dir(installer)
        if crash_dir != crash_dir_post_kill:
            log.info("Success!. Crash is reported.")
            break
    if w.expired:
        raise OperationFailedError("Failed to list the crash even after 120 seconds")

    return 0
