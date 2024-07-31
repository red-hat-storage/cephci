import json
import time
import traceback

from pip._internal.exceptions import CommandError

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83575628 - Perform active mds failures while upgrading
    Steps Performed:
    1. Check if upgrade in progress
    2. get active mds
    3. Fail active mds with interval for 2 min each
    4. Perform this till upgrade in progress
    5. Check if there are any crash occurred

    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")

        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        client1 = clients[0]
        fs_name = "cephfs"
        log.info("Wait for Upgrade to start")
        time.sleep(120)
        # while True:
        start_time = time.time()
        while time.time() - start_time < 1800:
            cmd = "ceph orch upgrade status --format json"
            out, rc = client1.exec_command(cmd=cmd, sudo=True)
            output = json.loads(out)
            if not output["in_progress"]:
                log.info("Upgrade Complete...")
                break
            mds_ls = fs_util.get_active_mdss(client1, fs_name=fs_name)
            for mds in mds_ls:
                out, rc = client1.exec_command(
                    cmd=f"ceph mds fail {mds}", client_exec=True
                )
                log.info(out)

                if not wait_for_two_active_mds(client1, fs_name):
                    raise CommandError(
                        "2 Active MDS did not start after failing one MDS"
                    )
                time.sleep(120)
                out, rc = client1.exec_command(
                    cmd=f"ceph fs status {fs_name}", client_exec=True
                )
                log.info(f"Status of {fs_name}:\n {out}")
                out, rc = client1.exec_command(cmd="ceph -s -f json", client_exec=True)
                ceph_status = json.loads(out)
                log.info(f"Ceph status: {json.dumps(ceph_status, indent=4)}")
                if ceph_status["health"]["status"] == "HEALTH_ERR":
                    log.error("Ceph Health is NOT OK")
                    return 1

        out, rc = client1.exec_command(sudo=True, cmd="ceph crash ls")
        if out:
            raise CommandError(f"Found Crash while Upgrade {out}")
        return 0
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        pass


def wait_for_two_active_mds(client1, fs_name, max_wait_time=180, retry_interval=10):
    """
    Wait until two active MDS (Metadata Servers) are found or the maximum wait time is reached.

    Args:
        data (str): JSON data containing MDS information.
        max_wait_time (int): Maximum wait time in seconds (default: 180 seconds).
        retry_interval (int): Interval between retry attempts in seconds (default: 5 seconds).

    Returns:
        bool: True if two active MDS are found within the specified time, False if not.

    Example usage:
    ```
    data = '...'  # JSON data
    if wait_for_two_active_mds(data):
        print("Two active MDS found.")
    else:
        print("Timeout: Two active MDS not found within the specified time.")
    ```
    """

    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        out, rc = client1.exec_command(
            cmd=f"ceph fs status {fs_name} -f json", client_exec=True
        )
        log.info(out)
        parsed_data = json.loads(out)
        active_mds = [
            mds
            for mds in parsed_data.get("mdsmap", [])
            if mds.get("rank", -1) in [0, 1] and mds.get("state") == "active"
        ]
        if len(active_mds) == 2:
            return True  # Two active MDS found
        else:
            time.sleep(retry_interval)  # Retry after the specified interval

    return False
