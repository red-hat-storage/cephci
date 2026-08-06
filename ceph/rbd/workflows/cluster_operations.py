import json
import re
import time
from importlib import import_module

from ceph.rados import utils
from tests.rados.rados_test_util import get_device_path, wait_for_device
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def restart_ceph_target(admin_node):
    """
    Restart the Ceph target service.

    Args:
        admin_node (Node): The cephadm admin node.

    Raises:
        Exception: If the Ceph target service restart fails.
    """
    log.info("Restarting Ceph target service")

    cmd = "systemctl restart ceph.target"
    out, err = admin_node.exec_command(cmd=cmd, sudo=True)

    # Check for errors
    if err:
        log.error(err)
        raise Exception("Failed to restart ceph target service")


def restart_osd(client_node):
    """
    Method to restart ceph osd service.

    Args:
        client_node (Node): The ceph client node.

    Raises:
        Exception: If Ceph health check fails.
    """
    log.info("Restarting Ceph osd service")
    try:
        cmd = "ceph orch ls osd"
        out, err = client_node.exec_command(cmd=cmd, sudo=True)

        # Check for errors
        if err:
            raise Exception("Listing OSD daemon failed")

        # Regular expression to extract the OSD name
        osd_daemon = re.search(r"osd\.(\S+)", str(out))
        cmd = f"ceph orch restart {osd_daemon[0]}"

        out, err = client_node.exec_command(cmd=cmd, sudo=True)

        # Check for errors
        if err:
            raise Exception("Failed to restart ceph osd service")
    except Exception as err:
        log.error(err)
        raise Exception("Failed to restart ceph osd service")


def check_health(client_node):
    """
    Check Ceph health status.

    Args:
        client_node (Node): The ceph client node.

    Raises:
        Exception: If Ceph health check fails.
    """
    log.info("Checking Ceph health status")
    try:
        out, err = client_node.exec_command(cmd="ceph -s", sudo=True)

        if "HEALTH_ERR" in out:
            log.debug(out)
            raise Exception("Cluster went to error health state")

        log.info(f"Ceph health status: {out}")

        # Check for errors
        if err:
            raise Exception(f"ceph health check error out as {err}")

    except Exception as err:
        log.error(err)
        raise Exception("Failed to check Ceph health status")


def osd_remove_and_add_back(
    ceph_cluster,
    rados_obj,
    pool,
):
    """
    Method to remove osd and add back the removed osd.

    Args:
        ceph_cluster: The Ceph cluster object.
        rados_obj: RadosObject for interacting with Ceph.
        pool: The name of the Ceph pool.

    Raises:
        Exception: If any ceph operation fails.
    """
    try:
        client_node = ceph_cluster.get_nodes(role="client")[0]
        pg_set = rados_obj.get_pg_acting_set(pool_name=pool)
        log.info(f"Acting set for removal and addition of OSDs {pg_set}")
        target_osd = pg_set[0]
        host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=target_osd)

        timeout = 300
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                dev_path = get_device_path(host, target_osd)
                if dev_path:
                    # Device path exist, break the loop
                    break
                time.sleep(10)
            except Exception as e:
                log.info(e)
                time.sleep(10)

        else:
            raise Exception("Timeout: Unable to get device path within timeout")

        log.info(
            f"osd device path  : {dev_path}, osd_id : {target_osd}, host.hostname : {host.hostname}"
        )

        utils.set_osd_devices_unmanaged(ceph_cluster, target_osd, unmanaged=True)
        method_should_succeed(utils.set_osd_out, ceph_cluster, target_osd)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        utils.osd_remove(ceph_cluster, target_osd)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        method_should_succeed(utils.zap_device, ceph_cluster, host.hostname, dev_path)
        method_should_succeed(wait_for_device, host, target_osd, action="remove")

        # Checking cluster health after OSD removal
        method_should_succeed(rados_obj.run_pool_sanity_check)
        log.info(
            f"Removal of OSD : {target_osd} is successful. Proceeding to add back the OSD daemon."
        )

        # Adding the removed OSD back and checking the cluster status
        utils.add_osd(ceph_cluster, host.hostname, dev_path, target_osd)

        # Checking cluster health after OSD removal
        method_should_succeed(rados_obj.run_pool_sanity_check)
        cmd = f"ceph osd tree | grep {target_osd}"
        timeout = 300
        start_time = time.time()

        while time.time() - start_time < timeout:
            osd_tree_out = client_node.exec_command(cmd=cmd, sudo=True)[0]
            if "up" in osd_tree_out:
                log.info(
                    f"Addition of OSD : {target_osd} back into the cluster is successful"
                )
                break

            time.sleep(10)

        else:
            raise Exception(
                f"Timeout: OSD {target_osd} did not become 'up' within {timeout} seconds."
            )

        utils.set_osd_devices_unmanaged(ceph_cluster, target_osd, unmanaged=False)

    except Exception as err:
        log.error(err)
        raise Exception("Failed to perform osd remove and add back the removed osd")


def operation(obj, test, **kw):
    """
    Executes the test specified in test parameter with inputs args and returns results
    Args:
        obj: rbd object or module to be imported
        test: test to be executed
        **kwargs: input args required for the test
    """
    if isinstance(obj, str):
        obj = import_module(obj)
    method = getattr(obj, test)
    rc = method(**kw)
    if (type(rc) is bool and rc is False) or (type(rc) is int and rc == 1):
        raise Exception(f"method {test} failed")


def validate_cluster_health_and_daemons(client, cluster_label, allow_warn=True):
    """Validate that a Ceph cluster is usable and core daemons are running.

    Checks:
    1. ``ceph health`` is ``HEALTH_OK``, or ``HEALTH_WARN`` when *allow_warn*
       is True. ``HEALTH_ERR`` always fails.
    2. ``ceph -s`` status is captured in the logs for triage.
    3. All ``mon``, ``mgr``, and ``osd`` daemons are in running state.

    Args:
        client: CephNode client to run commands on.
        cluster_label: Human-readable label (e.g. ``source``, ``destination``)
            used in log messages.
        allow_warn: When True (default), ``HEALTH_WARN`` does not fail the
            check; status is still logged via ``ceph -s``.

    Returns:
        0 if cluster health is acceptable with all daemons running.
        1 if any check fails.
    """
    try:
        # Always capture cluster status for logs (OK or WARN)
        status, _ = client.exec_command(cmd="ceph -s", sudo=True, check_ec=False)
        log.info(f"{cluster_label} cluster ceph -s output:\n{status}")

        health, _ = client.exec_command(cmd="ceph health", sudo=True)
        health = health.strip()
        # ``ceph health`` may append detail text, e.g.
        # ``HEALTH_WARN 1 failed cephadm daemon(s)``
        health_status = health.split(None, 1)[0] if health else ""

        if health_status == "HEALTH_ERR" or (
            health_status == "HEALTH_WARN" and not allow_warn
        ):
            log.error(f"{cluster_label} cluster is not healthy: {health}")
            client.exec_command(cmd="ceph health detail", sudo=True, check_ec=False)
            return 1

        if health_status not in ("HEALTH_OK", "HEALTH_WARN"):
            log.error(
                f"{cluster_label} cluster returned unexpected health status: {health}"
            )
            client.exec_command(cmd="ceph health detail", sudo=True, check_ec=False)
            return 1

        if health_status == "HEALTH_WARN":
            log.info(
                f"{cluster_label} cluster health is HEALTH_WARN; continuing test "
                f"(ceph -s captured above): {health}"
            )
            client.exec_command(cmd="ceph health detail", sudo=True, check_ec=False)
        else:
            log.info(f"{cluster_label} cluster health: {health}")

        for daemon_type in ("mon", "mgr", "osd"):
            out, _ = client.exec_command(
                cmd=f"ceph orch ps --daemon-type {daemon_type} --format json",
                sudo=True,
            )
            daemons = json.loads(out)
            if not daemons:
                log.error(f"{cluster_label} cluster has no {daemon_type} daemons")
                return 1

            failed = [
                d
                for d in daemons
                if d.get("status_desc") != "running" and d.get("status") != 1
            ]
            if failed:
                log.error(
                    f"{cluster_label} cluster has non-running {daemon_type} "
                    f"daemons: {failed}"
                )
                return 1

            log.info(
                f"{cluster_label} cluster: {len(daemons)} {daemon_type} "
                f"daemon(s) running"
            )

    except Exception as err:
        log.error(
            f"Failed to validate {cluster_label} cluster health and daemons: {err}"
        )
        return 1

    return 0


def get_ceph_major_version(config):
    """Extract the major Ceph version number from the rhbuild config key.

    Useful for code paths that branch on the major release (e.g. >= 3 for
    EC pool init, >= 9 for native import features).

    Args:
        config (dict): Test configuration dict containing an optional
            ``rhbuild`` key (e.g. ``"9.2"`` or ``"8.0"``).

    Returns:
        int: Major Ceph version integer. Defaults to 9 when rhbuild is absent.
    """
    rhbuild = str(config.get("rhbuild", "9"))
    match = re.search(r"\d+", rhbuild)
    return int(match.group(0)) if match else 9


def validate_min_ceph_version(config, min_major, min_minor, *clients):
    """Validate that all supplied cluster clients meet a minimum Ceph version.

    Logs the live ``ceph -v`` output for every client and then checks the
    ``rhbuild`` config key against the requested minimum.  When ``rhbuild``
    is absent the check is skipped (returns 0) so CI can still run against
    dev builds where the build string is unavailable.

    Args:
        config (dict): Test configuration dict containing an optional
            ``rhbuild`` key (e.g. ``"9.2"``).
        min_major (int): Minimum required major version (e.g. 9).
        min_minor (int): Minimum required minor version (e.g. 2).
        *clients: One or more ``(label, CephNode)`` tuples, e.g.
            ``("source", src_client), ("destination", dst_client)``.

    Returns:
        int: 0 if the version requirement is met or rhbuild is unavailable,
             1 if the deployed version is below the minimum.

    Example::

        rc = validate_min_ceph_version(
            config, 9, 2,
            ("source", source_client),
            ("destination", destination_client),
        )
        if rc:
            return 1
    """
    from ceph.rbd.utils import exec_cmd

    for label, client in clients:
        ceph_ver = exec_cmd(node=client, cmd="ceph -v", output=True)
        log.info(f"{label} cluster ceph version: {ceph_ver}")

    rhbuild = str(config.get("rhbuild", ""))
    match = re.search(r"(\d+)(?:\.(\d+))?", rhbuild)
    if not match:
        log.info("rhbuild is unavailable; skipping minimum version check")
        return 0

    major = int(match.group(1))
    minor = int(match.group(2) or 0)
    if (major, minor) < (min_major, min_minor):
        log.error(
            f"This test requires Ceph/RHCS {min_major}.{min_minor} or later, "
            f"found rhbuild={rhbuild}"
        )
        return 1
    return 0
