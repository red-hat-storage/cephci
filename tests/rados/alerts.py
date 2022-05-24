"""Module to test ceph health alerts"""
import traceback

from ceph.ceph_admin import CephAdmin
from utility.log import Log

log = Log(__name__)


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
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    # List of alerts to be tested
    alert_list = ["HEALTH_ERR", "HEALTH_WARN"]
    for alert in alert_list:
        if alert == "HEALTH_ERR":
            if not generate_health_error_alert(alert, cephadm):
                return 1
    return 0


def validate_alerts(alert: str, node: CephAdmin):
    """
    Method to validate genearted alerts
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
    cmd = "ceph osd set-full-ratio .25"
    try:
        out, err = node.shell([cmd])
        log.debug("changed the osd set-full-ratio")
    except Exception:
        log.error("Failed to change the osd set-full-ratio")
        log.error(traceback.format_exc())
        return False

    if not validate_alerts(alert, node):
        log.error(f"alert : {alert} has failed")
        return False
    # Resetting to default
    cmd = "ceph osd set-full-ratio .95"
    try:
        out, err = node.shell([cmd])
        log.debug("changed the osd set-full-ratio")
    except Exception:
        log.error("Failed to change the osd set-full-ratio")
        log.error(traceback.format_exc())
        return False
    return True
