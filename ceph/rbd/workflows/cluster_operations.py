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
