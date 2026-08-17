import json
import time
import traceback

from pip._internal.exceptions import CommandError

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

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
        config = kw.get("config", {})
        min_active_mds = config.get("min_active_mds", 2)
        mds_recovery_timeout = config.get("mds_recovery_timeout", 900)
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
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
        retry_exec_command = retry(CommandFailed, tries=10, delay=30, backoff=1)(
            client1.exec_command
        )
        # while True:
        start_time = time.time()
        while time.time() - start_time < 1800:
            cmd = "ceph orch upgrade status"
            out, rc = client1.exec_command(cmd=cmd, sudo=True)
            exp_msg = "There are no upgrades in progress currently."
            if exp_msg in out:
                log.info("Upgrade Complete...")
                break
            mds_ls = fs_util.get_active_mdss(client1, fs_name=fs_name)
            for mds in mds_ls:
                out, rc = retry_exec_command(
                    cmd=f"ceph mds fail {mds}", client_exec=True
                )
                log.info(out)

                if not wait_for_active_mds(
                    client1,
                    fs_name,
                    min_active=min_active_mds,
                    max_wait_time=mds_recovery_timeout,
                ):
                    raise CommandError(
                        f"{min_active_mds} active MDS did not recover after failing one MDS"
                    )
                time.sleep(120)
                out, rc = retry_exec_command(
                    cmd=f"ceph fs status {fs_name}", client_exec=True
                )
                log.info(f"Status of {fs_name}:\n {out}")
                out, rc = retry_exec_command(cmd="ceph -s -f json", client_exec=True)
                ceph_status = json.loads(out)
                log.info(f"Ceph status: {json.dumps(ceph_status, indent=4)}")
                if ceph_status["health"]["status"] == "HEALTH_ERR":
                    log.error("Ceph Health is NOT OK")
                    return 1

        out, rc = retry_exec_command(sudo=True, cmd="ceph crash ls")
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


def wait_for_active_mds(
    client1, fs_name, min_active=2, max_wait_time=900, retry_interval=20
):
    """
    Wait until the required number of active MDS ranks are found.

    Args:
        client1: Ceph client node used to run commands.
        fs_name (str): Filesystem name.
        min_active (int): Minimum active MDS count required to continue.
        max_wait_time (int): Maximum wait time in seconds.
        retry_interval (int): Interval between retry attempts in seconds.

    Returns:
        bool: True if the required active MDS count is met within the wait time.
    """
    retry_exec_command = retry(CommandFailed, tries=10, delay=30, backoff=1)(
        client1.exec_command
    )
    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        out, rc = retry_exec_command(
            cmd=f"ceph fs status {fs_name} -f json", client_exec=True
        )
        log.info(out)
        parsed_data = json.loads(out)
        active_mds = [
            mds
            for mds in parsed_data.get("mdsmap", [])
            if mds.get("state") == "active"
        ]
        if len(active_mds) >= min_active:
            log.info(
                "Found %s active MDS rank(s); required minimum is %s",
                len(active_mds),
                min_active,
            )
            return True
        time.sleep(retry_interval)

    return False


def wait_for_two_active_mds(client1, fs_name, max_wait_time=600, retry_interval=20):
    """Backward-compatible wrapper for callers expecting two active MDS."""
    return wait_for_active_mds(
        client1,
        fs_name,
        min_active=2,
        max_wait_time=max_wait_time,
        retry_interval=retry_interval,
    )
