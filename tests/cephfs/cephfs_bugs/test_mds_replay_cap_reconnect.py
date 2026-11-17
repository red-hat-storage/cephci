"""
CEPH-83633043 - Multi-client MDS replay cap reconnect (cap_imports assert).

Port of upstream qa TestReplayCapReconnect.test_multi_client_replay_cap_reconnect
(tracker #60014 / ceph PR #69920 / #72205) for CephCI / IBMCEPH-16365.
"""

import json
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)

ASSERT_NEEDLE = "cap_imports[ino].size() == 1"
TEST_FILE = "reconnect_test"
HOLD_SCRIPT = "/root/mds_replay_cap_hold_open.py"
TRIM_KEY = "mds_log_trim_upkeep_interval"
PAUSE_KEY = "mds_log_pause"


def _get_mds_config(client, key):
    out, _ = client.exec_command(sudo=True, cmd=f"ceph config get mds {key}")
    return out.strip()


def _set_mds_config(client, key, value):
    client.exec_command(sudo=True, cmd=f"ceph config set mds {key} {value}")
    got = _get_mds_config(client, key)
    if got != str(value):
        raise CommandFailed(f"Failed to set mds {key}={value}, got {got}")


def _tell_active_mds_config(client, fs_util, fs_name, key, value):
    """Apply runtime config on each active MDS (best-effort)."""
    for mds in fs_util.get_active_mdss(client, fs_name=fs_name):
        client.exec_command(
            sudo=True,
            cmd=f"ceph tell mds.{mds} config set {key} {value}",
            check_ec=False,
        )


def _start_hold(client, path, marker):
    client.exec_command(sudo=True, cmd=f"rm -f -- '{marker}'", check_ec=False)
    safe = marker.replace("/", "_")
    cmd = (
        f"nohup python3 {HOLD_SCRIPT} '{path}' '{marker}' "
        f">/tmp/hold_{safe}.log 2>&1 & echo $!"
    )
    out, _ = client.exec_command(sudo=True, cmd=cmd)
    pid = out.strip().splitlines()[-1].strip()
    deadline = time.time() + 30
    while time.time() < deadline:
        check, _ = client.exec_command(
            sudo=True,
            cmd=f"test -f '{marker}' && cat '{marker}' || true",
            check_ec=False,
        )
        if "opened" in (check or ""):
            log.info("Hold open ready pid=%s path=%s", pid, path)
            return pid
        time.sleep(1)
    raise CommandFailed(f"Hold open did not signal opened for {path} (pid={pid})")


def _stop_pid(client, pid):
    if not pid:
        return
    client.exec_command(
        sudo=True,
        cmd=(
            f"kill -TERM {pid} 2>/dev/null || true; sleep 1; "
            f"kill -KILL {pid} 2>/dev/null || true"
        ),
        check_ec=False,
    )


def _umount_keep_client(client, mount_point, fuse=False):
    """Unmount without deleting CephFS contents or the client keyring.

    FsUtils.client_clean_up("umount") removes the keyring and runs
    ``ceph auth del``, so a later remount fails cephx. It also deletes
    files under the mount, which drops the inode this test must reopen.
    """
    mount = mount_point.rstrip("/")
    client.exec_command(sudo=True, cmd="sync", check_ec=False)
    if fuse:
        cmd = f"fusermount -u '{mount}' || umount '{mount}'"
    else:
        cmd = f"umount '{mount}'"
    client.exec_command(sudo=True, cmd=cmd, timeout=180)


def _log_crash_details(client, crashes):
    if not isinstance(crashes, list):
        return
    for item in crashes[:5]:
        cid = item.get("id") if isinstance(item, dict) else item
        if not cid:
            continue
        info, _ = client.exec_command(
            sudo=True, cmd=f"ceph crash info {cid}", check_ec=False
        )
        log.error("crash info %s:\n%s", cid, (info or "")[:2000])
        if ASSERT_NEEDLE in (info or ""):
            log.error("Matched customer assert: %s", ASSERT_NEEDLE)


def run(ceph_cluster, **kw):
    """
    CEPH-83633043 - Multi-client replay cap reconnect — no MDS abort on
    ceph_assert(cap_imports[ino].size() == 1).

    Steps:
      1. Archive crashes; ensure CephFS; set max_mds=1 (upstream recipe)
      2. Kernel + fuse mount same FS; create reconnect_test; umount
         without deleting the inode or the client keyring
      3. Stretch mds_log_trim_upkeep_interval; flush journal; remount
      4. Hold O_RDONLY fds on both mounts; pause MDS log; start chmod
      5. Fail active MDS; wait for active again
      6. Assert no new crash (esp. cap_imports); mode is 600; R/W OK

    Config (optional):
      trim_interval (default 3600000), chmod_wait_secs (default 10)
    """
    client1 = None
    fs_util = None
    fuse_mounting_dir = None
    kernel_mounting_dir = None
    default_fs = None
    saved_configs = {}
    log_paused = False
    hold_pids = []
    chmod_pid = None
    original_max_mds = None

    try:
        tc = "CEPH-83633043"
        log.info("Running cephfs %s multi-client replay cap reconnect test", tc)
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config") or {}
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        if len(clients) < 1:
            log.error("This test requires at least 1 client")
            return 1

        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        default_fs = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, default_fs)
        if not fs_details:
            fs_util.create_fs(client1, default_fs)

        trim_interval = str(config.get("trim_interval", "3600000"))
        chmod_wait_secs = int(config.get("chmod_wait_secs", 10))

        log.info("Archiving pre-existing crash records")
        client1.exec_command(sudo=True, cmd="ceph crash archive-all", check_ec=False)

        out, _ = client1.exec_command(
            sudo=True, cmd=f"ceph fs get {default_fs} -f json"
        )
        try:
            original_max_mds = json.loads(out).get("mdsmap", {}).get("max_mds")
        except Exception:
            original_max_mds = None
        log.info("Setting max_mds=1 for %s (was %s)", default_fs, original_max_mds)
        client1.exec_command(sudo=True, cmd=f"ceph fs set {default_fs} max_mds 1")
        retry_mds_status = retry(CommandFailed, tries=12, delay=15)(
            fs_util.get_mds_status
        )
        retry_mds_status(client1, 1, vol_name=default_fs)

        for key in (TRIM_KEY, PAUSE_KEY):
            try:
                saved_configs[key] = _get_mds_config(client1, key)
            except Exception as exc:
                log.warning("Could not read mds %s default: %s", key, exc)
                saved_configs[key] = None

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(8)
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse_{mounting_dir}_replay/"
        kernel_mounting_dir = f"/mnt/cephfs_kernel_{mounting_dir}_replay/"
        mon_node_ips = fs_util.get_mon_node_ips()

        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir,
            extra_params=f" --client_fs {default_fs}",
        )
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=f",fs={default_fs}",
        )

        fuse_file = f"{fuse_mounting_dir.rstrip('/')}/{TEST_FILE}"
        kernel_file = f"{kernel_mounting_dir.rstrip('/')}/{TEST_FILE}"
        client1.exec_command(sudo=True, cmd=f"touch '{fuse_file}'")

        log.info("Umount both clients without deleting the inode or client keyring")
        _umount_keep_client(client1, fuse_mounting_dir, fuse=True)
        _umount_keep_client(client1, kernel_mounting_dir, fuse=False)

        log.info(
            "Stretch %s=%s so opens stay out of on-disk open-file table",
            TRIM_KEY,
            trim_interval,
        )
        _set_mds_config(client1, TRIM_KEY, trim_interval)
        _tell_active_mds_config(client1, fs_util, default_fs, TRIM_KEY, trim_interval)
        time.sleep(3)

        active = fs_util.get_active_mdss(client1, fs_name=default_fs)
        if not active:
            raise CommandFailed("No active MDS before journal flush")
        client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{active[0]} flush journal", timeout=120
        )

        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir,
            extra_params=f" --client_fs {default_fs}",
        )
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=f",fs={default_fs}",
        )

        client1.upload_file(
            sudo=True,
            src="tests/cephfs/lib/mds_replay_cap_hold_open.py",
            dst=HOLD_SCRIPT,
        )
        client1.exec_command(sudo=True, cmd=f"chmod +x {HOLD_SCRIPT}")

        hold_a = _start_hold(client1, fuse_file, "/tmp/hold_fuse_opened")
        hold_b = _start_hold(client1, kernel_file, "/tmp/hold_kernel_opened")
        hold_pids = [hold_a, hold_b]

        log.info("Pause MDS journal so chmod early-replies then stays unsafe")
        _set_mds_config(client1, PAUSE_KEY, "true")
        _tell_active_mds_config(client1, fs_util, default_fs, PAUSE_KEY, "true")
        log_paused = True

        out, _ = client1.exec_command(
            sudo=True,
            cmd=(
                f"nohup timeout 180 chmod 600 '{fuse_file}' "
                f">/tmp/chmod_reconnect.log 2>&1 & echo $!"
            ),
        )
        chmod_pid = out.strip().splitlines()[-1].strip()
        log.info(
            "Started background chmod pid=%s; waiting %ss", chmod_pid, chmod_wait_secs
        )
        time.sleep(chmod_wait_secs)

        active = fs_util.get_active_mdss(client1, fs_name=default_fs)
        if not active:
            raise CommandFailed("No active MDS to fail")
        fail_target = active[0]
        log.info("Failing MDS %s during deferred reconnect window", fail_target)
        client1.exec_command(sudo=True, cmd=f"ceph mds fail {fail_target}")

        try:
            _set_mds_config(client1, PAUSE_KEY, "false")
            log_paused = False
        except Exception:
            log.error(
                "Failed to clear %s after MDS fail; finally will retry", PAUSE_KEY
            )

        log.info("Waiting for MDS to become active again")
        retry_mds_status(client1, 1, vol_name=default_fs)
        time.sleep(15)

        crashes = fs_util.get_crash_ls_new(client1)
        log.info("crash ls-new after failover: %s", crashes)
        if crashes:
            _log_crash_details(client1, crashes)
            raise CommandFailed(
                f"Unexpected ceph crash(es) after replay cap reconnect: {crashes}"
            )

        mode, _ = client1.exec_command(sudo=True, cmd=f"stat -c '%a' '{fuse_file}'")
        mode = (mode or "").strip()
        log.info("Mode after reconnect: %s", mode)
        if mode != "600":
            log.error("Expected mode 600 after replayed chmod, got %s", mode)
            return 1

        probe = f"{fuse_mounting_dir.rstrip('/')}/post_reconnect_probe"
        client1.exec_command(
            sudo=True,
            cmd=f"echo ok > '{probe}' && cat '{probe}' && rm -f '{probe}'",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"echo ok >> '{kernel_file}' && cat '{kernel_file}' >/dev/null",
        )

        log.info(
            "TEST PASSED | %s | multi-client replay cap reconnect | no MDS crash",
            tc,
        )
        return 0
    except CommandFailed as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        if client1 and chmod_pid:
            _stop_pid(client1, chmod_pid)
        for pid in hold_pids:
            try:
                _stop_pid(client1, pid)
            except Exception:
                pass
        if client1:
            client1.exec_command(
                sudo=True,
                cmd=(
                    f"pkill -f '{HOLD_SCRIPT}' || true; "
                    'pkill -f "chmod 600" || true; sleep 1'
                ),
                check_ec=False,
            )
        if log_paused and client1:
            try:
                _set_mds_config(client1, PAUSE_KEY, "false")
            except Exception:
                client1.exec_command(
                    sudo=True,
                    cmd=f"ceph config set mds {PAUSE_KEY} false",
                    check_ec=False,
                )
        if client1:
            for key, value in saved_configs.items():
                if value is None:
                    client1.exec_command(
                        sudo=True,
                        cmd=f"ceph config rm mds {key}",
                        check_ec=False,
                    )
                else:
                    client1.exec_command(
                        sudo=True,
                        cmd=f"ceph config set mds {key} {value}",
                        check_ec=False,
                    )
        if client1 and default_fs and original_max_mds is not None:
            try:
                client1.exec_command(
                    sudo=True,
                    cmd=f"ceph fs set {default_fs} max_mds {original_max_mds}",
                    check_ec=False,
                )
            except Exception:
                pass
        if fs_util and client1 and fuse_mounting_dir:
            try:
                client1.exec_command(
                    sudo=True,
                    cmd=(
                        f"rm -f -- '{fuse_mounting_dir.rstrip('/')}/{TEST_FILE}' "
                        f"'{fuse_mounting_dir.rstrip('/')}/post_reconnect_probe' "
                        f"2>/dev/null || true"
                    ),
                    check_ec=False,
                )
                fs_util.client_clean_up(
                    "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir
                )
            except Exception as e:
                log.warning("Cleanup fuse umount warning: %s", e)
        if fs_util and client1 and kernel_mounting_dir:
            try:
                fs_util.client_clean_up(
                    "umount", kernel_clients=[client1], mounting_dir=kernel_mounting_dir
                )
            except Exception as e:
                log.warning("Cleanup kernel umount warning: %s", e)
