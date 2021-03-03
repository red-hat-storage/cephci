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
            f"There are health alerts generated on the cluster. Alerts : {all_alerts['active_alerts']}"
        )
    else:
        log.info("Cluster Health is ok. \n Generating a alert to verify feature")

    # Scenario 1 : Verify the auto-unmute of alert after TTL
    alert = alert_list[0]
    log.info(f"generating alert : {alert}")
    if not generate_health_alert(alert=alert, node=cephadm):
        log.error("could not generate the error")
        return 1
    log.info(f"Proceeding to mute the alert: {alert}")
    if not mute_health_alert(alert=alert, node=cephadm, duration="6m"):
        log.error(f"could not mute the alert : {alert}")
        return 1
    log.info(
        "The alert was muted for 6 minutes. Checking if the alert is un-muted after the TTL"
    )

    # The alert is muted for 6 minutes, after which it will be auto un-muted.
    # Sleeping for 6 minutes to verify the auto unmute after TTL
    time.sleep(362)

    all_alerts = get_alerts(cephadm)
    if alert not in all_alerts["active_alerts"] and alert in all_alerts["muted_alerts"]:
        log.error(f"Alert : {alert} is still Muted")
        return 1
    log.info(f"The alert {alert} was unmuted after TTL")
    # Clearing the alert generated
    generate_health_alert(alert=alert, node=cephadm, clear=True)

    # Scenario 2 : Verify the auto-unmute of alert if the health alert is generated again. ( without sticky )
    alert = alert_list[1]
    log.debug(f"Generating alerts : {alert}")
    if not generate_health_alert(alert=alert, node=cephadm, flag="noscrub"):
        log.error(f"could not generate the {alert}")
        return 1

    if not mute_health_alert(alert=alert, node=cephadm):
        log.error(f"could not Mute the alert : {alert}")
        return 1
    log.info(f"muted health alert : {alert}")
    # adding another flag to worsen the health alert, so that the mute will be removed automatically.
    # the alert was added without sticky option, so the mute should be removed
    if not generate_health_alert(alert=alert, node=cephadm, flag="nodeep-scrub"):
        log.error(f"could not generate the {alert}")
        return 1

    # Checking if alert is un-muted again
    all_alerts = get_alerts(cephadm)
    if alert in all_alerts["muted_alerts"] and alert not in all_alerts["active_alerts"]:
        log.error(f"Alert : {alert} is still Muted")
        return 1
    log.info(
        f"The alert : {alert} is un-muted after when the same alert was generated again"
    )

    # Scenario 3 : Verify the auto-unmute of alert if the health alert is generated again. ( with sticky )
    # removing one of the flags set
    if not generate_health_alert(alert=alert, node=cephadm, flag="noscrub", clear=True):
        log.error(f"could not remove the alert : {alert}")
        return 1
    if not mute_health_alert(alert=alert, node=cephadm, sticky=True):
        log.error(f"could not Mute the alert : {alert}")
        return 1

    # adding another flag to worsen the health alert, so that the mute will be removed automatically.
    # the alert was added without sticky option, so the mute should be removed
    if not generate_health_alert(alert=alert, node=cephadm, flag="noscrub"):
        log.error(f"could not generate the {alert}")
        return 1
    # Checking that the alert is still muted
    all_alerts = get_alerts(cephadm)
    if alert in all_alerts["active_alerts"] and alert not in all_alerts["muted_alerts"]:
        log.error(f"Alert : {alert} is un-Muted")
        return 1

    log.info(f"The alert : {alert} is still muted since it was used with --sticky")

    # Scenario 4 : Verify the unmute command with sticky
    if not unmute_health_alert(alert=alert, node=cephadm):
        log.error(f"Unable to unmute the alert : {alert}")
        return 1
    # removing the flags set
    generate_health_alert(alert=alert, node=cephadm, flag="noscrub", clear=True)
    generate_health_alert(alert=alert, node=cephadm, flag="nodeep-scrub", clear=True)
    return 0


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
    log.debug("Collecting all the alerts generated on the cluster")
    cmd = "ceph health detail"
    all_alerts = {}
    out, err = node.shell([cmd])
    log.debug(f"the o/p of {cmd} is {out}")
    regex_active = r"\s*[^(MUTED)]\s+\[\w{3}\]\s([\w_]*):"
    regex_muted = r"\s*[(MUTED)]\s+\[\w{3}\]\s([\w_]*):"
    all_alerts["active_alerts"] = re.findall(regex_active, out)
    all_alerts["muted_alerts"] = re.findall(regex_muted, out)
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
    log.debug("Collecting all the alerts generated on the cluster")
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
    time.sleep(1)
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

    Returns: True -> pass, 1 -> failure

    """
    log.debug("Collecting all the alerts generated on the cluster")
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
    time.sleep(1)
    all_alerts = get_alerts(node)
    log.info(
        f"Un-Muted the alert : {alert}. All the Un-muted alerts : {all_alerts['active_alerts']}"
    )
    return True if alert in all_alerts["active_alerts"] else False


def generate_health_alert(
    alert: str, node: CephAdmin, clear: bool = False, **kwargs
) -> bool:
    """
    Method to generate various health alerts
    Args:
        alert: name of the alert to be generated
        node: name of the installer node
        clear: Bool value which specifies if the given alert should be cleared
        kwargs: any other params that need to be sent for a particular alert

    Returns: 0 -> pass, 1 -> failure

    """
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
        time.sleep(5)
        return True
