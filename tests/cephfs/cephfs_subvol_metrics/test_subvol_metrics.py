import random
import string
import threading
import time
import traceback
from typing import Any, Dict, List, Optional

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_subvol_metric_utils import MDSMetricsHelper
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Create subvolume_1, mount via ceph-fuse, run fio for 5 minutes,
    and collect mds_subvolume_metrics for that subvolume during the run.

    Also: parse fio summary and compare with MDS metrics using helper methods.

    Returns 0 on success, 1 on failure.
    """
    helper = MDSMetricsHelper(ceph_cluster)
    fs_util = helper.fs_util

    test_data = kw.get("test_data")
    erasure = (
        FsUtils.get_custom_config_value(test_data, "erasure") if test_data else False
    )
    config: Dict[str, Any] = kw.get("config", {})
    build = config.get("build", config.get("rhbuild"))

    clients = ceph_cluster.get_ceph_objects("client")
    if not clients:
        log.error("This test requires at least 1 client node")
        return 1

    client = clients[0]

    # variables for cleanup
    subvol_created = False
    mounting_dir = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
    )
    fuse_mount_dir = f"/mnt/cephfs_fuse{mounting_dir}/"
    subvol_name = "subvolume_1"
    pid: Optional[str] = None
    fio_log_path = ""
    default_fs = "cephfs" if not erasure else "cephfs-ec"

    try:
        # Prep client
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        fs_created = False
        # Ensure FS exists
        if not fs_util.get_fs_info(client, default_fs):
            fs_util.create_fs(client, default_fs)
            fs_created = True
        # Create subvolume_1 in default group (_nogroup)
        log.info("Creating subvolume %s in fs %s", subvol_name, default_fs)
        fs_util.create_subvolume(
            client,
            vol_name=default_fs,
            subvol_name=subvol_name,
            size="5368709120",  # 5 GiB
        )
        subvol_created = True

        # Resolve path of subvolume for fuse -r and for metrics filtering
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} {subvol_name}",
        )
        subvol_path = out.strip()  # e.g., /volumes/_nogroup/subvolume_1
        log.info(f"Subvolume path is {subvol_path}")

        # Mount via ceph-fuse (pointing to subvol path)
        fs_util.fuse_mount(
            [client],
            fuse_mount_dir,
            extra_params=f" -r {subvol_path} --client_fs {default_fs}",
        )

        # Enable quota on fuse_mount_dir (set quota to match subvolume size: 5 GiB = 5368709120 bytes)
        quota_bytes_value = 2147483648  # 2 GiB
        log.info("Setting quota on %s: %s bytes", fuse_mount_dir, quota_bytes_value)
        fs_util.set_quota_attrs(client, "1000", quota_bytes_value, fuse_mount_dir)

        # Decide which MDS to query: active rank 0 (adjust to [0, 1] if needed)
        ranks = [0]

        # Start fio in background (5 minutes) -> get PID and /root log path
        jobname = "fio_subvol_1"
        rw = config.get("rw", "randwrite")  # e.g. "randrw" for mixed
        rwmixread = config.get("rwmixread", 70)  # only used when "rw" in rw
        pid, fio_log_path = MDSMetricsHelper.run_fio_background(
            client,
            fuse_mount_dir,
            runtime_sec=int(config.get("runtime_sec", 60)),
            jobname=jobname,
            rw=rw,
            rwmixread=rwmixread,
        )

        # Collect expected values (quota_bytes and used_bytes) in parallel with metrics collection
        expected_values_snapshots: List[Dict[str, Any]] = []
        expected_values_lock = threading.Lock()
        stop_collection = threading.Event()

        def collect_expected_values():
            """Collect expected quota_bytes and used_bytes at same intervals as metrics"""
            duration_sec = int(config.get("runtime_sec", 60))
            interval_sec = int(config.get("interval_sec", 10))
            start = time.time()
            next_t = start
            end = start + duration_sec

            while not stop_collection.is_set():
                now = time.time()
                if now >= next_t:
                    try:
                        # Get expected quota_bytes from getattr
                        quota_attrs = fs_util.get_quota_attrs(client, fuse_mount_dir)
                        expected_quota_bytes = quota_attrs.get(
                            "bytes", quota_bytes_value
                        )

                        # Get expected used_bytes from subvolume info
                        subvol_info = fs_util.get_subvolume_info(
                            client, vol_name=default_fs, subvol_name=subvol_name
                        )
                        expected_used_bytes = subvol_info.get("bytes_used", 0)

                        with expected_values_lock:
                            expected_values_snapshots.append(
                                {
                                    "t": int(now),
                                    "quota_bytes": expected_quota_bytes,
                                    "used_bytes": expected_used_bytes,
                                }
                            )
                        log.info(
                            "[t=%s] Collected expected values: "
                            "quota_bytes=%s, used_bytes=%s",
                            int(now),
                            expected_quota_bytes,
                            expected_used_bytes,
                        )
                    except Exception as e:
                        log.error(
                            "Failed to collect expected values at t=%s: %s", int(now), e
                        )
                        # Use fallback values
                        with expected_values_lock:
                            expected_values_snapshots.append(
                                {
                                    "t": int(now),
                                    "quota_bytes": quota_bytes_value,
                                    "used_bytes": 0,
                                }
                            )
                    next_t += interval_sec
                if now >= end:
                    break
                time.sleep(1)

        # Start collecting expected values in parallel thread
        expected_values_thread = threading.Thread(
            target=collect_expected_values, daemon=True
        )
        expected_values_thread.start()

        # Collect metrics DURING the fio run (every 10s)
        during_snapshots = helper.poll_metrics_during_run(
            client=client,
            fs_name=default_fs,
            subvol_path=subvol_path,
            ranks=ranks,
            role="active",  # or "both" to include standby-replay too
            duration_sec=int(config.get("runtime_sec", 60)),
            interval_sec=int(config.get("interval_sec", 10)),
        )

        # Stop expected values collection
        stop_collection.set()
        expected_values_thread.join(timeout=5)

        # Wait for fio to finish (be tolerant if it misbehaves)
        try:
            MDSMetricsHelper.wait_pid(
                client, pid, timeout=int(config.get("wait_timeout_sec", 120))
            )
        except Exception as e:
            log.error(
                "fio pid %s did not exit within timeout; forcing kill. Error: %s",
                pid,
                e,
            )
            client.exec_command(
                sudo=True,
                cmd=f"pkill -9 -P {pid} || true; kill -9 {pid} || true",
                check_ec=False,
            )

        # Collect one more snapshot AFTER the run
        after_snapshot = helper.collect_subvolume_metrics(
            client=client,
            fs_name=default_fs,
            role="active",
            ranks=ranks,
            path_prefix=subvol_path,
        )

        if not during_snapshots:
            log.error("Metrics not recorded while IO was in progress")
            return 1
        log.info("After snapshot of metrics : %s", after_snapshot)

        # Create a lookup dict for expected values by timestamp (with tolerance for matching)
        expected_values_by_t: Dict[int, Dict[str, int]] = {}
        with expected_values_lock:
            for ev_snap in expected_values_snapshots:
                expected_values_by_t[ev_snap["t"]] = {
                    "quota_bytes": ev_snap["quota_bytes"],
                    "used_bytes": ev_snap["used_bytes"],
                }

        def find_expected_values(timestamp: int) -> Optional[Dict[str, int]]:
            """Find expected values for a given timestamp (with tolerance)"""
            # Try exact match first
            if timestamp in expected_values_by_t:
                return expected_values_by_t[timestamp]
            # Try to find closest match within 5 seconds
            for t, values in expected_values_by_t.items():
                if abs(t - timestamp) <= 5:
                    return values
            return None

        # Log a concise summary (timeline)
        log.info("=== Metrics timeline (active ranks) for subvolume_1 ===")
        for snap in during_snapshots:
            t = snap["t"]
            # Get expected values for this timestamp
            expected_vals = find_expected_values(t)
            if not expected_vals:
                log.warning(
                    "[t=%s] No expected values found for this timestamp, "
                    "using fallback values",
                    t,
                )
                expected_quota_bytes = quota_bytes_value
                expected_used_bytes = 0
            else:
                expected_quota_bytes = expected_vals["quota_bytes"]
                expected_used_bytes = expected_vals["used_bytes"]

            for mds_name, items in snap["samples"].items():
                for it in items:
                    # Validate required fields exist
                    if "quota_bytes" not in it:
                        log.error(
                            "[t=%s] %s %s " "Missing required field: quota_bytes",
                            t,
                            mds_name,
                            it["subvolume_path"],
                        )
                        return 1
                    if "used_bytes" not in it:
                        log.error(
                            "[t=%s] %s %s " "Missing required field: used_bytes",
                            t,
                            mds_name,
                            it["subvolume_path"],
                        )
                        return 1

                    # Validate quota_bytes value matches expected value from getattr
                    actual_quota_bytes = it.get("quota_bytes")
                    if actual_quota_bytes != expected_quota_bytes:
                        log.error(
                            "[t=%s] %s %s "
                            "quota_bytes mismatch: expected=%s, "
                            "actual=%s",
                            t,
                            mds_name,
                            it["subvolume_path"],
                            expected_quota_bytes,
                            actual_quota_bytes,
                        )
                        return 1

                    # Validate used_bytes value matches expected value from subvolume info
                    actual_used_bytes = it.get("used_bytes")
                    if actual_used_bytes != expected_used_bytes:
                        log.error(
                            "[t=%s] %s %s "
                            "used_bytes mismatch: expected=%s, "
                            "actual=%s",
                            t,
                            mds_name,
                            it["subvolume_path"],
                            expected_used_bytes,
                            actual_used_bytes,
                        )
                        return 1

                    log.info(
                        "[t=%s] %s %s "
                        "w_iops=%s w_tp_Bps=%s "
                        "w_lat_ms=%s "
                        "quota_bytes=%s(exp=%s) "
                        "used_bytes=%s(exp=%s)",
                        t,
                        mds_name,
                        it["subvolume_path"],
                        it["avg_write_iops"],
                        it["avg_write_tp_Bps"],
                        it["avg_write_lat_msec"],
                        it["quota_bytes"],
                        expected_quota_bytes,
                        it["used_bytes"],
                        expected_used_bytes,
                    )
        log.info("=== FIO output (full file) ===")

        try:
            fio_out, _ = client.exec_command(
                sudo=True,
                cmd=f"cat '{fio_log_path}' 2>/dev/null",
                check_ec=False,
            )
            for line in fio_out.splitlines():
                log.info(line)
        except Exception as e:
            log.error("Failed to read fio log at %s: %s", fio_log_path, e)
        # Remove the fio log after printing
        try:
            client.exec_command(
                sudo=True, cmd=f"rm -f '{fio_log_path}'", check_ec=False
            )
            log.info("Deleted fio log: %s", fio_log_path)
        except Exception as e:
            log.error("Failed to delete fio log %s: %s", fio_log_path, e)

        return 0

    except Exception as e:
        log.error("Test failed: %s", e)
        log.error(traceback.format_exc())
        return 1

    finally:
        # Cleanup
        try:
            if pid:
                client.exec_command(sudo=True, cmd=f"kill -9 {pid}", check_ec=False)
        except Exception:
            pass

        cleanup_fail = False
        try:
            client.exec_command(
                sudo=True, cmd=f"umount -l {fuse_mount_dir}", check_ec=False
            )
            client.exec_command(
                sudo=True, cmd=f"rm -rf {fuse_mount_dir}", check_ec=False
            )
        except Exception as e:
            log.error("Unmount/cleanup fuse mount failed: %s", e)
            cleanup_fail = True

        try:
            if subvol_created:
                fs_util.remove_subvolume(
                    client, vol_name=default_fs, subvol_name=subvol_name
                )
        except Exception as e:
            log.error("Subvolume cleanup failed: %s", e)
            cleanup_fail = True

        try:
            if fs_created:
                fs_util.remove_fs(client, fs_name=default_fs)
        except Exception as e:
            log.error("Filesystem cleanup failed: %s", e)
            cleanup_fail = True
        if cleanup_fail:
            return 1

        try:
            if fio_log_path:
                client.exec_command(
                    sudo=True, cmd=f"rm -f '{fio_log_path}'", check_ec=False
                )
        except Exception:
            pass
