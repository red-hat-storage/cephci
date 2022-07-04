"""Module to test ceph health alerts"""
import traceback
from json import loads
from time import sleep

from ceph.ceph_admin import CephAdmin
from utility.log import Log

log = Log(__name__)


def execute_commands(node, commands):
    for cmd in commands:
        out, err = node.exec_command(sudo=True, cmd=cmd)
        log.info(f"Output:\n {out}")
        log.error(f"Error:\n {err}")


def run(ceph_cluster, **kw):
    """
    Prepares the cluster & runs alerts test Scenarios.
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
        kw: Args that need to be passed to the test for initialization
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    alert = config.get("alert")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    if alert == "HEALTH_ERR":
        if not generate_health_error_alert(alert, cephadm):
            return 1
    elif alert == "HEALTH_WARN":
        if not generate_health_warn_alert(alert, cephadm):
            return 1
    return 0


def validate_alerts(alert: str, node: CephAdmin):
    """
    Method to validate generated alerts
    Args:
        alert: name of the alert
        node: name of the cephadmin node
    Returns: True -> pass, False -> failure
    """
    cmd = "ceph health detail"
    out, err = node.shell([cmd])
    if alert in out:
        log.info(f"alert {alert} is generated successfully")
        return True
    else:
        raise Exception(f"alert {alert} is not generated.")
        return False


def generate_health_error_alert(alert: str, node: CephAdmin) -> bool:
    """
    Method to generate health error alert
    Args:
        alert: name of the alert to be generated
        node: name of the installer node
    Returns: True -> pass, False -> failure

    """
    # Ratio .25 to generate alert and .95 to reset default
    for ratio in [0.25, 0.95]:
        cmd = f"ceph osd set-full-ratio {ratio}"
        try:
            out, err = node.shell([cmd])
            log.debug("changed the osd set-full-ratio")
        except Exception:
            log.error("Failed to change the osd set-full-ratio")
            log.error(traceback.format_exc())
            return False

        if ratio != 0.95 and not validate_alerts(alert, node):
            log.error(f"alert : {alert} has failed")
            return False
    return True


def generate_health_warn_alert(alert: str, admin_node: CephAdmin) -> bool:
    """
    Method to generate health warn alert
    Args:
        alert: name of the alert to be generated
        node: name of the installer node
    Returns: True -> pass, False -> failure

    """
    out, _ = admin_node.shell(
        args=["ceph", "orch", "ps", "--daemon_type", "mon", "-f", "json"]
    )
    daemon_obj = loads(out)

    if daemon_obj:
        # To get hostname in which daemon got deployed
        for daemon in daemon_obj:
            host = daemon["hostname"]
        # To get last mon on all mon host
        for node in admin_node.cluster.get_nodes():
            if node.hostname == host:
                mon_node = node
                break
        WARN_ENABLE_COOMANDS = [
            "touch grepout",
            "systemctl -l | grep ceph > /root/grepout",
        ]
        execute_commands(mon_node, WARN_ENABLE_COOMANDS)
        command = "cat /root/grepout | grep mon"
        out, err = mon_node.exec_command(sudo=True, cmd=command)
        mon_service = out.split()[0]
        # Stoping mon service to get health warn
        cmd = f"systemctl stop {mon_service}"
        out, err = mon_node.exec_command(sudo=True, cmd=cmd)
        sleep(10)  # waiting for ceph status to reflect on the fail mon

        if not validate_alerts(alert, admin_node):
            log.error(f"alert : {alert} has failed")
            return False

        # Activating mon again to get back to original state
        cmd = f"systemctl start {mon_service}"
        out, err = mon_node.exec_command(sudo=True, cmd=cmd)
        return True
    raise Exception("Daemon Mon does not exists")
