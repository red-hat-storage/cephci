import logging
import re
import time
import traceback

from ceph.ceph_admin import CephAdmin

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Polarion ID: https://polarion.engineering.redhat.com/polarion/#/project/CEPH/workitem?id=CEPH-83573854
    Feature : Mute Health alerts : https://trello.com/c/AU8FT6Qp/27-mute-health-warnings
    RFE Bug : https://bugzilla.redhat.com/show_bug.cgi?id=1821508
    upstream doc : https://docs.ceph.com/en/octopus/rados/operations/monitoring/#muting-health-checks

    1. Check the cluster health status and simulate various failures.
    2. Verify the health warning/ error generated due to the failures.
    3. Mute the failures, test the various arguments and verify that the alerts are muted successfully
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """
    log.info("Running test for feature : Mute Health Alerts")
    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    all_alerts = get_alerts(cephadm)
    alert_list = ["MON_DISK_BIG", "OSDMAP_FLAGS"]
    if all_alerts["active_alerts"]:
        log.info(
            f"There are health alerts generated on the cluster. Alerts : {all_alerts}"
        )
    else:
        log.info("Cluster Health is ok. \n Generating a alert to verify feature")

    # Scenario 1 : Verify the auto-unmute of alert after TTL
    log.info("Scenario 1: Verify the auto-unmute of alert after TTL")
    alert = alert_list[0]
    if not verify_alert_with_ttl(node=cephadm, alert=alert, ttl=8):
        log.error(f"Scenario 1 for alert : {alert} has failed")
        return 1

    # Scenario 2 : Verify the auto-unmute of alert if the health alert is generated again. ( without sticky )
    log.info(
        "Scenario 2: Verify the auto-unmute of alert if the health alert is generated again."
    )
    alert = alert_list[1]
    if not verify_alert_unmute(node=cephadm, alert=alert):
        log.error(f"Scenario 2 for alert : {alert} has failed")
        return 1

    # Scenario 3 : Verify the auto-unmute of alert if the health alert is generated again. ( with sticky )
    log.info(
        "Scenario 3: Verify the auto-unmute of alert if the health alert is generated again."
    )
    alert = alert_list[1]
    if not verify_alert_unmute(node=cephadm, alert=alert, sticky=True):
        log.error(f"Scenario 3 for alert : {alert} has failed")
        return 1

    log.info(f"All the current alerts : {get_alerts(cephadm)}")
    # Scenario 4 : Verify the unmute command with sticky
    log.info("Scenario 4 : Verify the unmute command")
    alert = alert_list[0]
    if not verify_unmute_cli(node=cephadm, alert=alert, sticky=True):
        log.error(f"Scenario 4 for alert : {alert} has failed")
        return 1

    log.info("All the scenarios have passed")
    return 0


def verify_alert_with_ttl(node: CephAdmin, alert: str, ttl: int, **kwargs) -> bool:
    """
    Mutes the alert with TTL and checkes if the alert is auto un-muted
    Args:
        alert: Name of the alert to be muted
        node: node on which command should be executed
        ttl: True -> pass, False -> failure
        **kwargs: any other params that need to be passed

    Returns: True -> pass, False -> failure

    """
    alert = alert
    dur = f"{ttl}m"
    if not verify_alert(node=node, alert=alert, duration=dur, **kwargs):
        log.error(f"Alert : {alert} is not Muted")
        return False
    log.info(
        f"The alert was muted for {ttl} minutes. Checking if the alert is un-muted after the TTL"
    )

    # The alert is muted for ttl minutes, after which it will be auto un-muted.
    # Sleeping for ttl minutes to verify the auto un-mute after TTL
    time.sleep((ttl * 60) + 2)
    all_alerts = get_alerts(node)
    if alert not in all_alerts["active_alerts"] and alert in all_alerts["muted_alerts"]:
        log.error(f"Alert : {alert} is still Muted")
        return False
    log.info(f"The alert {alert} was unmuted after TTL. Scenario pass")

    # Clearing the alert generated
    generate_health_alert(alert=alert, node=node, clear=True, **kwargs)
    return True


def verify_alert_unmute(node: CephAdmin, alert: str, **kwargs) -> bool:
    """
    Verifies the behaviour of mute on an alert when the same alert is generated again
    Args:
        alert: Name of the alert to be muted
        node: node on which command should be executed
        **kwargs: any other params that need to be passed
    Returns: True -> pass, False -> failure

    """
    if alert == "OSDMAP_FLAGS":
        sticky = kwargs.get("sticky")
        if not verify_alert(node=node, alert=alert, flag="noscrub", sticky=sticky):
            log.error(f"Alert : {alert} is not Muted")
            return False

        # Adding another flag to worsen the health alert, so that the mute will be removed automatically.
        # If the alert was added without sticky option, so the mute should be removed.
        if not generate_health_alert(alert=alert, node=node, flag="nodeep-scrub"):
            log.error(f"could not generate the {alert}")
            return False

        # Checking if alert is un-muted again
        all_alerts = get_alerts(node)
        if not sticky:
            if (
                alert in all_alerts["muted_alerts"]
                and alert not in all_alerts["active_alerts"]
            ):
                log.error(f"Alert : {alert} is still Muted")
                return False
        else:
            unmute_health_alert(alert=alert, node=node)
            if (
                alert not in all_alerts["muted_alerts"]
                and alert in all_alerts["active_alerts"]
            ):
                log.error(f"Alert : {alert} is still Muted")
                return False
        log.info(f"Auto un-mute for alert : {alert} is verified. Scenario Pass")

        # clearing the flags set
        generate_health_alert(alert=alert, node=node, flag="noscrub", clear=True)
        generate_health_alert(alert=alert, node=node, flag="nodeep-scrub", clear=True)
        log.info(f"Auto-unmute was verified for the alert : {alert}. Scenario pass")
        return True

    log.error(f"the scenario not available for the alert : {alert}")
    return True


def verify_unmute_cli(node: CephAdmin, alert: str, **kwargs) -> bool:
    """
    Verifies the behaviour of the "ceph health unmute <alert>" command
    Args:
        alert: Name of the alert to be muted
        node: node on which command should be executed
        **kwargs: any other params that need to be passed

    Returns: True -> pass, False -> failure

    """
    if not verify_alert(node=node, alert=alert, **kwargs):
        log.error(f"Unable to unmute the alert : {alert}")
        return False

    if not unmute_health_alert(alert=alert, node=node):
        log.error(f"Unable to unmute the alert : {alert}")
        return False

    log.info(f"The alert : {alert} is un-muted. Scenario Pass")
    generate_health_alert(alert=alert, node=node, clear=True, **kwargs)
    return True


def verify_alert(node: CephAdmin, alert: str, **kwargs) -> bool:
    """
    Creates health alerts and mutes the provided alert with the args given
    Args:
        alert: Name of the alert to be muted
        node: node on which command should be executed
        **kwargs: any other params that need to be passed

    Returns: True -> pass, False -> failure

    """

    if not generate_health_alert(alert=alert, node=node, **kwargs):
        log.error("could not generate the error")
        return False

    duration = kwargs.get("duration")
    sticky = kwargs.get("sticky")

    if not mute_health_alert(alert=alert, node=node, duration=duration, sticky=sticky):
        log.error(f"could not mute the alert : {alert}")
        return False

    return True


def get_alerts(node: CephAdmin) -> dict:
    """
    Fetches all the current health alerts codes that are generated on the ceph cluster
    Args:
            node: node on which command should be executed

    Returns: list of the alerts present
            alert dictionary :
            { "active_alerts" : ['CEPHADM_REFRESH_FAILED', 'OSDMAP_FLAGS'],
             "muted_alerts" : ['MON_DISK_BIG'] }
    """
    cmd = "ceph health detail"
    all_alerts = {}
    out, err = node.shell([cmd])
    regex = r"(\(MUTED[\w\s,-]*\))?\s*\[\w{3}\]\s([\w_]*):"
    alerts = re.findall(regex, out)
    all_alerts["active_alerts"] = [alert[1] for alert in alerts if not alert[0]]
    all_alerts["muted_alerts"] = [alert[1] for alert in alerts if alert[0]]
    return all_alerts


def mute_health_alert(
    alert: str, node: CephAdmin, duration: str = None, sticky: bool = False
) -> bool:
    """
    Mutes the health alert generated on the cluster
    Args:
        alert: Name of the alert to be muted
        node: node on which command should be executed
        duration: duration for which the alert should be muted.
                Allowed Values: None -> mutes the specified alert indefinitely until the same alert is raised again
                                5m, 1h -> mutes the specified alert for specified duration
        sticky: makes use of the "--sticky" param to mute specified alert indefinitely

    Returns: True -> pass, False -> failure

    """
    all_alerts = get_alerts(node)
    if alert not in all_alerts["active_alerts"] + all_alerts["muted_alerts"]:
        log.info(f"the alert: {alert} not generated on the cluster, Cannot mute")
        return True
    if alert in all_alerts["muted_alerts"]:
        log.info(f"the alert: {alert} is already muted")
        return True

    # Muting the given alert along with specified duration
    cmd = f"ceph health mute {alert}"
    if duration:
        cmd += f" {duration}"
    if sticky:
        cmd += " --sticky"
    node.shell([cmd])

    # Sleeping for 5 sec for the alert to be logged
    time.sleep(5)
    all_alerts = get_alerts(node)
    log.info(
        f"Muted the alert : {alert}. All the muted alerts : {all_alerts['muted_alerts']}"
    )
    return True if alert in all_alerts["muted_alerts"] else False


def unmute_health_alert(alert: str, node: CephAdmin) -> bool:
    """
    Un-Mutes the health alert on the cluster
    Args:
        alert: Name of the alert to be muted
        node: node on which command should be executed

    Returns: True -> pass, False -> failure

    """
    all_alerts = get_alerts(node)
    if alert not in all_alerts["muted_alerts"] + all_alerts["active_alerts"]:
        log.info(f"the alert: {alert} not generated on the cluster, Cannot mute")
        return True
    if alert in all_alerts["active_alerts"]:
        log.info(f"the alert: {alert} is already un-muted")
        return True

    # Un-Muting the given alert
    cmd = f"ceph health unmute {alert}"
    node.shell([cmd])
    # Sleeping for 2 sec for the alert to be logged
    time.sleep(2)
    all_alerts = get_alerts(node)
    log.info(
        f"Un-Muted the alert : {alert}. All the Un-muted alerts : {all_alerts['active_alerts']}"
    )
    return True if alert in all_alerts["active_alerts"] else False


def generate_health_alert(alert: str, node: CephAdmin, **kwargs) -> bool:
    """
    Method to generate various health alerts
    Args:
        alert: name of the alert to be generated
        node: name of the installer node
        clear: Bool value which specifies if the given alert should be cleared
        kwargs: any other params that need to be sent for a particular alert

    Returns: True -> pass, False -> failure

    """
    clear = kwargs.get("clear")
    if alert == "OSDMAP_FLAGS":
        try:
            flag = kwargs.get("flag")
        except KeyError:
            log.error(f"Flag not provided to generate health alert : {alert}")
            return False
        cmd = f"ceph osd set {flag}"
        if clear:
            cmd = f"ceph osd unset {flag}"
        try:
            node.shell([cmd])
            log.debug(f"{flag} set")
        except Exception:
            log.error(f"Failed to set the osd flag {flag}")
            log.error(traceback.format_exc())
            return False
        # Sleeping for 5 seconds for the error to logged by cluster
        time.sleep(5)
        return True

    if alert == "MON_DISK_BIG":
        cmd = "ceph config set global mon_data_size_warn 2500000"
        if clear:
            cmd = "ceph config set global mon_data_size_warn 16106127360"
        try:
            node.shell([cmd])
            log.debug("changed the mon data warn size param")
        except Exception:
            log.error("Failed to change the mon data warn size")
            log.error(traceback.format_exc())
            return False
        # Sleeping for 5 seconds for the error to logged by cluster
        time.sleep(5)
        return True

    log.error(f"method not implemented to generate the alert : {alert}")
    return False
