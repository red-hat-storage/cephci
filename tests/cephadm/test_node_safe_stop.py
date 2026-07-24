from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from utility.log import Log

log = Log(__name__)


class IncorrectAlertWarningError(Exception):
    pass


def run(ceph_cluster, **kw):
    """
    Verify if a host be safely stopped without reducing the availability
     using ceph orch ok-to-stop command

    Args:
        ceph_cluster: Ceph cluster object
        **kw :     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).
    """

    config = kw.get("config")

    cephadm = CephAdmin(cluster=ceph_cluster, **config)

    ok_to_stop_cmd = "cephadm shell ceph orch host ok-to-stop"

    roles = ["installer", "rgw", "mgr"]
    # - When trying to stop the installer_node, it should raise an Alert as
    #   there is an active mgr running
    # - Validate no ALERT/WARNING messages are shown when trying to remove a safe node
    # - When trying to stop RGW node, it should raise a WARNING saying removing
    #    RGW daemon cause clients to lose connectivity (based on the conf, else no warnings)
    # -. Validate no ALERT/WARNING messages are shown when trying to remove a safe node

    for role in roles:
        node = ceph_cluster.get_nodes(role=role)[0]
        _shortname = node.shortname
        try:
            _, err = cephadm.installer.exec_command(
                cmd=f"{ok_to_stop_cmd} {_shortname}", sudo=True
            )
            if "WARNING" in err or "ALERT" in err:
                if role == "rgw":
                    raise IncorrectAlertWarningError(
                        f"Unexpected WARNING/ALERT message shown for {role} : {_shortname}"
                    )
            else:
                if role != "rgw":
                    raise IncorrectAlertWarningError(
                        f"No expected ALERT/WARNING message shown for {role} : {_shortname}"
                    )
        except CommandFailed:
            log.error(f"Failed to execute ok-to-stop cmd on {role} node : {_shortname}")
            return 1

    return 0
