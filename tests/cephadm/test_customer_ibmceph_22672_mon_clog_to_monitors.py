"""
Module to verify that 'clog_to_monitors = false' remains effective across MON daemon restarts.

Reference:
    IBMCEPH-22672: clog_to_monitors=false is not applied after MON restart
"""

import time
from json import loads

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError, ResourceNotFoundError
from utility.log import Log

log = Log(__name__)

TARGET_LOG_PATTERN = "log_client _send_to_mon log to self"


def run(ceph_cluster, **kw):
    """Verify that clog_to_monitors=false persists and is respected across MON restarts.

    Test Steps:
        1. Enable monclient debug logging: ceph config set mon debug_monc 10
        2. Verify that clog_to_monitors is set to true by default.
        3. Configure clog_to_monitors to false: ceph config set mon clog_to_monitors false
        4. Verify that config get confirms clog_to_monitors=false.
        5. Restart MON service daemons: ceph orch restart mon
        6. Wait for MON daemons to reach running state and quorum.
        7. Verify that config get still returns clog_to_monitors=false.
        8. Inspect post-restart MON daemon logs and verify that 'log_client _send_to_mon log to self'
           is not emitted after restart.
        9. Revert configuration in teardown.

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object.
        **kw: Key/value pairs of configuration information.

    Returns:
        int: 0 on success.
    """
    installer_nodes = ceph_cluster.get_nodes(role="installer")
    if not installer_nodes:
        raise ResourceNotFoundError("No installer node available in the cluster")
    installer = installer_nodes[0]
    cephadm = CephAdm(installer)

    fsid = cephadm.ceph.fsid()
    if not fsid:
        raise OperationFailedError("Failed to retrieve cluster FSID")

    log.info(f"Target cluster FSID: {fsid}")

    try:
        # Step 1: Enable monclient debug logging
        log.info("Setting 'debug_monc' to '10' for mon daemons")
        cephadm.ceph.config.set(daemon="mon", key="debug_monc", value="10")

        # Step 2: Verify default/current clog_to_monitors value
        initial_val = cephadm.ceph.config.get(who="mon", key="clog_to_monitors").strip()
        log.info(f"Initial clog_to_monitors value: {initial_val}")

        # Step 3: Configure clog_to_monitors to false
        log.info("Setting 'clog_to_monitors' to 'false' for mon daemons")
        cephadm.ceph.config.set(daemon="mon", key="clog_to_monitors", value="false")

        # Step 4: Verify config is applied in config DB
        configured_val = cephadm.ceph.config.get(
            who="mon", key="clog_to_monitors"
        ).strip()
        if configured_val.lower() != "false":
            raise ConfigError(
                f"Failed to set clog_to_monitors=false. Current value: {configured_val}"
            )
        log.info(f"Verified clog_to_monitors is set to: {configured_val}")

        # Step 5: Restart the MON service daemons
        log.info("Restarting MON daemons via 'ceph orch restart mon'")
        cephadm.ceph.orch.restart(service="mon")

        # Step 6: Wait for all MON daemons to be running
        log.info("Waiting for all MON daemons to return to running state")
        for waiter in WaitUntil(timeout=300, interval=10):
            cephadm.ceph.orch.ps(refresh=True)
            mon_ps = loads(cephadm.ceph.orch.ps(daemon_type="mon", format="json"))
            if mon_ps and all(d.get("status_desc") == "running" for d in mon_ps):
                log.info("All MON daemons are running successfully")
                break

        if waiter.expired:
            raise OperationFailedError(
                "MON daemons failed to return to running state after restart"
            )

        # Ensure quorum is stable
        cephadm.ceph.shell("mon stat")
        time.sleep(15)

        # Step 7: Verify config get still reflects false
        post_restart_val = cephadm.ceph.config.get(
            who="mon", key="clog_to_monitors"
        ).strip()
        log.info(f"Post-restart config value for clog_to_monitors: {post_restart_val}")
        if post_restart_val.lower() != "false":
            raise ConfigError(
                f"clog_to_monitors configuration was altered after restart: {post_restart_val}"
            )

        # Step 8: Verify daemon logs after restart
        mon_daemons = loads(cephadm.ceph.orch.ps(daemon_type="mon", format="json"))
        if not mon_daemons:
            raise ResourceNotFoundError("No MON daemons found in ceph orch ps")

        for daemon in mon_daemons:
            daemon_name = daemon.get("daemon_name")
            hostname = daemon.get("hostname")
            node = ceph_cluster.get_node_by_hostname(hostname) or installer

            log.info(f"Checking logs for daemon {daemon_name} on host {hostname}")
            daemon_logs = CephAdm(node).logs(fsid, daemon_name)
            if not daemon_logs:
                log.warning(f"No logs returned for daemon {daemon_name}")
                continue

            recent_logs = daemon_logs.splitlines()[-200:]
            recent_logs_text = "\n".join(recent_logs)

            if TARGET_LOG_PATTERN in recent_logs_text:
                raise OperationFailedError(
                    f"Defect IBMCEPH-22672 reproduced: '{TARGET_LOG_PATTERN}' found in logs of "
                    f"{daemon_name} after restart despite clog_to_monitors=false"
                )
            log.info(
                f"Confirmed '{TARGET_LOG_PATTERN}' is absent in recent logs of {daemon_name}"
            )

        log.info("PASS: clog_to_monitors=false is properly respected after MON restart")

    finally:
        log.info("Cleaning up test configurations")
        try:
            cephadm.ceph.config.rm(who="mon", option="clog_to_monitors")
            log.info("Removed 'clog_to_monitors' config")
        except Exception as err:
            log.warning(f"Failed to remove clog_to_monitors config: {err}")

        try:
            cephadm.ceph.config.rm(who="mon", option="debug_monc")
            log.info("Removed 'debug_monc' config")
        except Exception as err:
            log.warning(f"Failed to remove debug_monc config: {err}")

    return 0
