import random
import string
import traceback
from typing import Any, Dict, Optional

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

        # Ensure FS exists
        if not fs_util.get_fs_info(client, default_fs):
            fs_util.create_fs(client, default_fs)

        # Create subvolume_1 in default group (_nogroup)
        log.info(f"Creating subvolume {subvol_name} in fs {default_fs}")
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

        # Decide which MDS to query: active rank 0 (adjust to [0, 1] if needed)
        ranks = [0]

        # Start fio in background (5 minutes) -> get PID and /root log path
        jobname = "fio_subvol_1"
        rw = config.get("rw", "randwrite")  # e.g. "randrw" for mixed
        rwmixread = config.get("rwmixread", 70)  # only used when "rw" in rw
        pid, fio_log_path = MDSMetricsHelper.run_fio_background(
            client,
            fuse_mount_dir,
            runtime_sec=int(config.get("runtime_sec", 300)),
            jobname=jobname,
            rw=rw,
            rwmixread=rwmixread,
        )

        # Collect metrics DURING the fio run (every 30s)
        during_snapshots = helper.poll_metrics_during_run(
            client=client,
            fs_name=default_fs,
            subvol_path=subvol_path,
            ranks=ranks,
            role="active",  # or "both" to include standby-replay too
            duration_sec=int(config.get("runtime_sec", 300)),
            interval_sec=int(config.get("interval_sec", 30)),
        )

        # Wait for fio to finish (be tolerant if it misbehaves)
        try:
            MDSMetricsHelper.wait_pid(
                client, pid, timeout=int(config.get("wait_timeout_sec", 360))
            )
        except Exception as e:
            log.error(
                f"fio pid {pid} did not exit within timeout; forcing kill. Error: {e}"
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
        # Log a concise summary (timeline)
        log.info("=== Metrics timeline (active ranks) for subvolume_1 ===")
        for snap in during_snapshots:
            t = snap["t"]
            for mds_name, items in snap["samples"].items():
                for it in items:
                    log.info(
                        f"[t={t}] {mds_name} {it['subvolume_path']} "
                        f"w_iops={it['avg_write_iops']} w_tp_Bps={it['avg_write_tp_Bps']} "
                        f"w_lat_ms={it['avg_write_lat_msec']}"
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
            log.error(f"Failed to read fio log at {fio_log_path}: {e}")
        # Remove the fio log after printing
        try:
            client.exec_command(
                sudo=True, cmd=f"rm -f '{fio_log_path}'", check_ec=False
            )
            log.info(f"Deleted fio log: {fio_log_path}")
        except Exception as e:
            log.error(f"Failed to delete fio log {fio_log_path}: {e}")

        return 0

    except Exception as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        return 1

    finally:
        # Cleanup
        try:
            if pid:
                client.exec_command(sudo=True, cmd=f"kill -9 {pid}", check_ec=False)
        except Exception:
            pass

        try:
            client.exec_command(
                sudo=True, cmd=f"umount -l {fuse_mount_dir}", check_ec=False
            )
            client.exec_command(
                sudo=True, cmd=f"rm -rf {fuse_mount_dir}", check_ec=False
            )
        except Exception as e:
            log.error(f"Unmount/cleanup fuse mount failed: {e}")

        try:
            if subvol_created:
                fs_util.remove_subvolume(
                    client, vol_name=default_fs, subvol_name=subvol_name
                )
        except Exception as e:
            log.error(f"Subvolume cleanup failed: {e}")

        try:
            if fio_log_path:
                client.exec_command(
                    sudo=True, cmd=f"rm -f '{fio_log_path}'", check_ec=False
                )
        except Exception:
            pass
