from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.utils import get_all_running_pids, kill_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify redeploy for a specific service
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    # Check for mgr nodes
    installer = ceph_cluster.get_nodes(role="installer")[0]
    fsid = CephAdm(installer).ceph.fsid()

    # Identify the osd pid
    out = get_all_running_pids(installer, "osd")
    if not out:
        raise OperationFailedError("No osd daemons running")

    # Introduce a crash in OSD daemon
    kill_process(installer, out)

    # Get crash stats
    crash_stat = CephAdm(installer).ceph.crash.stat()
    log.info(f"Crash stats after stopping container are '{crash_stat}'")

    # Validate if the crash is posted in /var/lib/ceph/<fsid>/crash, /var/lib/ceph/<fsid>/crash/posted directories.
    folders = [f"/var/lib/ceph/{fsid}/crash", f"/var/lib/ceph/{fsid}/crash/posted"]
    for folder in folders:
        dirs = installer.get_dir_list(dir_path=folder, sudo=True)
        if not dirs:
            # Fail test if the crash stat return the crash details
            if "0 crashes recorded" not in crash_stat:
                raise OperationFailedError(
                    f"Unexpected! Crash status not reflected in {folder}"
                )
            else:
                log.info("No crashes were triggered during the test")
    log.info("Crash stat and crash directories returns the same status")
    return 0
