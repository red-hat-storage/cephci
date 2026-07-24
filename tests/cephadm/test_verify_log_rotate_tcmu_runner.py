from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Verify the logrotate config created by cephadm take into account tcmu-runner

    Args:
        ceph_cluster: Ceph cluster object
        **kw :     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).
    """
    installer = ceph_cluster.get_nodes("installer")[0]

    # Get the logrotate configuration
    fsid = CephAdm(installer).ceph.fsid()
    path = f"/etc/logrotate.d/ceph-{fsid}"
    cmd = f"cat {path}"
    out, _ = installer.exec_command(cmd=cmd, sudo=True)
    rotate_conf = [entry for entry in out.split("\n") if "killall" in entry]

    # Verify tcmu-runner is part of killall and pkill
    for item in rotate_conf[0].split(" || ")[:-1]:
        if "tcmu-runner" not in item:
            raise OperationFailedError(
                "Tcmu-runner is not in killall/pkill entries. Ref : #2204505"
            )
    log.info("the logrotate config created by cephadm take into account tcmu-runner")

    return 0
