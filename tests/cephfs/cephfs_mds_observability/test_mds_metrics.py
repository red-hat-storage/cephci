import json
import random
import re
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
    Polarion TC ID: CEPH-83632240
    Test Steps:
    1. Create subvolume, mount via fuse, run stress-ng IO
    2. Collect mds_rank_perf metrics during IO.
    3. Compare CPU usage from top command with metrics counters and verify variance within 30%.
    Return 0 on success, 1 on failure.
    """
    helper = MDSMetricsHelper(ceph_cluster)
    fs_util = helper.fs_util

    test_data = kw.get("test_data")
    erasure = (
        FsUtils.get_custom_config_value(test_data, "erasure") if test_data else False
    )
    config: Dict[str, Any] = kw.get("config", {})
    build = config.get("build", config.get("rhbuild"))
    skip_cpu_validation = config.get("skip_cpu_validation", False)
    clients = ceph_cluster.get_ceph_objects("client")
    if not clients:
        log.error("This test requires at least 1 client node")
        return 1

    client = clients[0]
    default_fs = "cephfs" if not erasure else "cephfs-ec"

    # Variables for cleanup
    subvol_created = False
    mounting_dir = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
    )
    fuse_mount_dir = f"/mnt/cephfs_fuse{mounting_dir}/"
    subvol_name = "subvolume_1"
    stress_ng_pid: Optional[str] = None

    try:
        # Prep client
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        # Verify only one CephFS volume exists, remove others if present
        log.info("Checking for existing filesystems")
        out, _ = client.exec_command(sudo=True, cmd="ceph fs ls --format json")
        fs_list = json.loads(out)

        # Remove any filesystems other than default_fs
        for fs in fs_list:
            fs_name = fs.get("name", "")
            if fs_name and fs_name != default_fs:
                log.info("Removing filesystem: %s", fs_name)
                try:
                    client.exec_command(
                        sudo=True,
                        cmd="ceph config set mon mon_allow_pool_delete true",
                        check_ec=False,
                    )
                    client.exec_command(
                        sudo=True,
                        cmd=f"ceph fs volume rm {fs_name} --yes-i-really-mean-it",
                        check_ec=False,
                    )
                    log.info("Successfully removed filesystem: %s", fs_name)
                    time.sleep(5)  # Wait for cleanup
                except Exception as e:
                    log.error("Failed to remove filesystem %s: %s", fs_name, e)
                    return 1

        # Ensure FS exists
        fs_created = False
        if not fs_util.get_fs_info(client, default_fs):
            fs_util.create_fs(client, default_fs)
            fs_created = True

        # Set max_mds to 2
        log.info("Setting max_mds to 2 for filesystem %s", default_fs)
        client.exec_command(sudo=True, cmd=f"ceph fs set {default_fs} max_mds 2")
        # Validate max_mds is set correctly
        out, _ = client.exec_command(sudo=True, cmd=f"ceph fs get {default_fs} -f json")
        fs_info = json.loads(out)
        current_max_mds = fs_info.get("mdsmap", {}).get("max_mds", 1)
        if current_max_mds != 2:
            log.error("Failed to set max_mds to 2. Current value: %s", current_max_mds)
            return 1
        log.info("Successfully set max_mds to %s", current_max_mds)

        # Create subvolume_1 in default group (_nogroup)
        log.info("Creating subvolume %s in fs %s", subvol_name, default_fs)
        fs_util.create_subvolume(
            client,
            vol_name=default_fs,
            subvol_name=subvol_name,
            size="5368709120",  # 5 GiB
        )
        subvol_created = True

        # Resolve path of subvolume for fuse mount
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} {subvol_name}",
        )
        subvol_path = out.strip()
        log.info("Subvolume path is %s", subvol_path)

        # Mount via ceph-fuse
        fs_util.fuse_mount(
            [client],
            fuse_mount_dir,
            extra_params=f" -r {subvol_path} --client_fs {default_fs}",
        )

        # Enable distributed pinning
        log.info("Enabling distributed pinning on MDS")
        client.exec_command(
            sudo=True,
            cmd="ceph config set mds mds_export_ephemeral_distributed true",
        )
        log.info("Setting distributed pinning attribute on %s", fuse_mount_dir)
        client.exec_command(
            sudo=True,
            cmd=f"setfattr -n ceph.dir.pin.distributed -v 1 {fuse_mount_dir}",
        )
        log.info("Distributed pinning enabled successfully")

        # Get ranks to query (default to all active ranks)
        ranks = config.get("ranks", None)  # None means all ranks
        mds_nodes = {}
        mds_names = fs_util.get_active_mdss(client, default_fs)
        log.info("mds_names: %s", mds_names)
        for mds_name in mds_names:
            mds_nodes[mds_name] = ceph_cluster.get_node_by_hostname(
                mds_name.split(".")[1]
            )
        # Install stress-ng if not present
        log.info("Checking for stress-ng installation")
        out, rc = client.exec_command(sudo=True, cmd="stress-ng", check_ec=False)
        if rc != 0 or not out.strip():
            log.info("stress-ng not found, installing...")
            try:
                client.exec_command(
                    sudo=True, cmd="yum install -y stress-ng", timeout=300
                )
                log.info("stress-ng installed successfully")
            except Exception as e:
                log.error("Failed to install stress-ng: %s", e)
                return 1
        else:
            log.info("stress-ng is already installed")

        # Prepare for parallel collection
        mds_metrics_snapshots: List[Dict[str, Any]] = []
        cpu_usage_snapshots: List[Dict[str, Any]] = []
        collection_lock = threading.Lock()
        stop_collection = threading.Event()

        duration_sec = int(config.get("duration_sec", 60))
        interval_sec = int(config.get("interval_sec", 10))

        def collect_mds_metrics_loop():
            """Collect MDS metrics at intervals"""
            start = time.time()
            next_t = start
            end = start + duration_sec

            while not stop_collection.is_set():
                now = time.time()
                if now >= next_t:
                    try:
                        metrics = helper.collect_mds_metrics(
                            client=client,
                            fs_name=default_fs,
                            role="active",
                            ranks=ranks,
                        )
                        with collection_lock:
                            mds_metrics_snapshots.append(
                                {"t": int(now), "metrics": metrics}
                            )
                        log.info("[t=%s] Collected MDS metrics : %s", int(now), metrics)
                    except Exception as e:
                        log.error(
                            "Failed to collect MDS metrics at t=%s: %s", int(now), e
                        )
                    next_t += interval_sec
                if now >= end:
                    break
                time.sleep(1)

        # Note Pids
        mds_pids = {}
        for mds_name in mds_names:
            pid_cmd = f"pgrep -f '^/usr/bin/ceph-mds.*{mds_name}' | head -1"
            pid_out, _ = mds_nodes[mds_name].exec_command(
                sudo=True, cmd=pid_cmd, check_ec=False
            )
            pid = pid_out.strip()
            mds_pids[mds_name] = pid

        def collect_cpu_usage_loop(mds_name: str):
            """Collect CPU usage from top command on MDS node"""
            start = time.time()
            next_t = start
            end = start + duration_sec
            while not stop_collection.is_set():
                now = time.time()
                if now >= next_t:
                    try:

                        # Get CPU usage using top
                        cmd = f"top -b -n 1 -p {mds_pids[mds_name]} | tail -1 | awk '{{print $9}}'"
                        out, _ = mds_nodes[mds_name].exec_command(
                            sudo=True, cmd=cmd, check_ec=False
                        )
                        cpu_str = out.strip()
                        log.info(cpu_str)
                        # Remove % if present and convert
                        cpu_str = cpu_str.replace("%", "").strip()
                        if cpu_str and (
                            cpu_str.replace(".", "").isdigit()
                            or re.match(r"^\d+\.?\d*$", cpu_str)
                        ):
                            cpu_usage = float(cpu_str)
                        with collection_lock:
                            cpu_usage_snapshots.append(
                                {
                                    "t": int(now),
                                    "mds_name": mds_name,
                                    "cpu_usage": float(cpu_str),
                                }
                            )
                        log.info("[t=%s] Collected CPU usage: %s", int(now), cpu_usage)
                    except Exception as e:
                        log.error(
                            "Failed to collect CPU usage at t=%s: %s", int(now), e
                        )
                        with collection_lock:
                            cpu_usage_snapshots.append(
                                {"t": int(now), "mds_name": mds_name, "cpu_usage": 0.0}
                            )
                    next_t += interval_sec
                if now >= end:
                    break
                time.sleep(1)

        # Start stress-ng IO
        log.info("Starting stress-ng IO on %s", fuse_mount_dir)
        stress_cmd = (
            f"stress-ng --iomix 4 --open 4 --timeout {duration_sec}s "
            f"--metrics-brief --temp-path {fuse_mount_dir} > /tmp/stress_ng.log 2>&1 & echo $!"
        )
        out, _ = client.exec_command(sudo=True, cmd=stress_cmd)
        stress_ng_pid = out.strip()
        log.info("Started stress-ng (pid=%s)", stress_ng_pid)
        time.sleep(5)
        # Start parallel collection threads

        cpu_thread1 = threading.Thread(
            target=collect_cpu_usage_loop, args=(mds_names[0],)
        )
        cpu_thread2 = threading.Thread(
            target=collect_cpu_usage_loop, args=(mds_names[1],)
        )
        metrics_thread = threading.Thread(target=collect_mds_metrics_loop)
        metrics_thread.start()
        time.sleep(2)
        cpu_thread1.start()
        cpu_thread2.start()
        # Wait for collection duration
        time.sleep(duration_sec + 5)

        # Stop collection
        stop_collection.set()

        metrics_thread.join(timeout=10)
        cpu_thread1.join(timeout=10)
        cpu_thread2.join(timeout=10)
        # Stop stress-ng
        try:
            client.exec_command(
                sudo=True, cmd=f"kill -9 {stress_ng_pid} 2>/dev/null", check_ec=False
            )
            log.info("Stopped stress-ng (pid=%s)", stress_ng_pid)
        except Exception:
            pass

        def find_expected_value(
            timestamp: int,
            mds_name: str,
            lookup_list: List[Dict[str, Any]],
            default: Any = 0,
        ) -> Any:
            """Find expected value for a given timestamp (with tolerance)"""
            for snap in lookup_list:
                if snap["t"] == timestamp and snap["mds_name"] == mds_name:
                    return snap["cpu_usage"]
            for snap in lookup_list:
                if abs(snap["t"] - timestamp) <= 5 and snap["mds_name"] == mds_name:
                    return snap["cpu_usage"]
            return default

        # Validate metrics
        log.info("=== Validating MDS rank performance metrics ===")
        test_fail = 0
        for snap in mds_metrics_snapshots:
            t = snap["t"]
            metrics = snap["metrics"]

            if not metrics:
                log.error("No metrics collected for [t=%s]", t)
                return 1

            log.info("Metrics from counter dump - [t=%s] metrics: %s", t, metrics)

            for mds_name, items in metrics.items():
                for i in cpu_usage_snapshots:
                    if i["t"] == t and i["mds_name"] == mds_name:
                        log.info("cpu_usage from top cmd - %s", i)
                        break
                if not isinstance(items, list):
                    continue
                item = items[0]
                labels = item.get("labels", {})
                counters = item.get("counters", {})
                rank = labels.get("rank")

                if rank is None:
                    log.error("mds_name:%s rank is None", mds_name)
                    return 1
                if "cpu_usage" not in counters or "open_requests" not in counters:
                    log.error(
                        "mds_name:%s rank:%s"
                        "- Missing required field: cpu_usage or open_requests, check %s",
                        mds_name,
                        rank,
                        counters,
                    )
                    return 1
                # Get expected values for this timestamp
                expected_cpu_usage = find_expected_value(
                    t, mds_name, cpu_usage_snapshots, 0.0
                )
                # Validate cpu_usage
                actual_cpu_usage = counters.get("cpu_usage")
                actual_open_requests = counters.get("open_requests")
                log.info(
                    "[t=%s] %s rank=%s "
                    "cpu_usage: expected=%s, actual=%s "
                    "open_requests: actual=%s",
                    t,
                    mds_name,
                    rank,
                    expected_cpu_usage,
                    actual_cpu_usage,
                    actual_open_requests,
                )
                if expected_cpu_usage > 0:
                    variance = (
                        abs(actual_cpu_usage - expected_cpu_usage)
                        / expected_cpu_usage
                        * 100
                    )
                    if variance > 30:
                        log.error(
                            "[t=%s] %s rank=%s cpu_usage variance exceeds 30 "
                            "expected=%s, actual=%s, variance=%s",
                            t,
                            mds_name,
                            rank,
                            expected_cpu_usage,
                            actual_cpu_usage,
                            variance,
                        )
                        test_fail += 1
        if test_fail > 3 and not skip_cpu_validation:
            log.error(
                "Test failed: %s MDS rank performance metrics validation failed",
                test_fail,
            )
            return 1
        log.info("=== All MDS rank performance metrics validated successfully ===")
        return 0

    except Exception as e:
        log.error("Test failed: %s", e)
        log.error(traceback.format_exc())
        return 1

    finally:
        # Cleanup
        try:
            if stress_ng_pid:
                client.exec_command(
                    sudo=True,
                    cmd=f"kill -9 {stress_ng_pid} 2>/dev/null",
                    check_ec=False,
                )
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
        # Disable distributed pinning
        log.info("Disabling distributed pinning on MDS")
        client.exec_command(
            sudo=True,
            cmd="ceph config set mds mds_export_ephemeral_distributed false",
        )
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
