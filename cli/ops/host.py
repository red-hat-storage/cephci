from json import loads

from cli.cephadm.cephadm import CephAdm
from cli.utilities.waiter import WaitUntil
from utility.log import Log

log = Log(__name__)


def check_host_status(node, hostname, status=None):
    """
    Checks the status of host(offline or online) using
    ceph orch host ls and return boolean
    Args:
        node (ceph): Node in which the cmd is executed
        hostname: hostname of host to be checked
        status: custom status check for the host
    """
    conf = {"host_pattern": hostname, "format": "json-pretty"}
    out = loads(CephAdm(node).ceph.orch.host.ls(**conf))
    host_status = out[0]["status"].lower().strip()
    log.info(f"Status of the host is {host_status}")
    if status.lower() == host_status:
        return True
    return False


def host_maintenance_enter(
    node, hostname, timeout=300, interval=10, force=False, yes_i_really_mean_it=False
):
    """
    Adds the specified host into maintenance mode
    Args:
        node (ceph): Node in which the cmd is executed
        hostname: name of the host which needs to be added into maintenance mode
        timeout (int): Operation waiting time in sec
        interval (int): Operation retry time in sec
        force (bool): Whether to append force with maintenance enter command
        yes-i-really-mean-it (bool) : Whether to append --yes-i-really-mean-it with maintenance enter command
    """
    log.debug(f"Passed host : {hostname} to be added into maintenance mode")
    for w in WaitUntil(timeout, interval):
        CephAdm(node).ceph.orch.host.maintenance(
            hostname=hostname,
            operation="enter",
            force=force,
            yes_i_really_mean_it=yes_i_really_mean_it,
        )
        if check_host_status(node, hostname, status="Maintenance"):
            log.info(f"Host: {hostname},moved to maintenance mode.")
            return True
        log.info("Host is not moved to maintenance, retrying")
    if w.expired:
        return False


def host_maintenance_exit(node, hostname, timeout=300, interval=10):
    """
    Removes the specified host from maintenance mode
    Args:
        node (ceph): Node in which the cmd is executed
        hostname(str): name of the host which needs to be removed from maintenance mode
        timeout (int): Operation waiting time in sec
        interval (int): Operation retry time in sec
    """
    log.debug(f"Passed host : {hostname} to be removed from maintenance mode")
    for w in WaitUntil(timeout, interval):
        CephAdm(node).ceph.orch.host.maintenance(hostname, operation="exit")
        if not check_host_status(node, hostname, status="Maintenance"):
            log.info(f"Host:{hostname}, moved out from maintenance mode.")
            return True
        log.info("Host is not moved out of maintenance, retrying")
    if w.expired:
        return False
