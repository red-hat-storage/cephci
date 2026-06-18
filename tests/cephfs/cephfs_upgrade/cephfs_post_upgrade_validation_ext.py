import json
import random
import string
import threading
import time
import traceback
from typing import Any, Dict, List, Optional, Tuple

from looseversion import LooseVersion

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.lib.cephfs_subvol_metric_utils import MDSMetricsHelper
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log

log = Log(__name__)


def _get_quota_and_used_from_metrics(
    helper: MDSMetricsHelper,
    client,
    vol_name: str,
    subvol_path: str,
    ranks: Optional[List[int]] = None,
) -> Optional[Tuple[int, int]]:
    """Return (quota_bytes, used_bytes) for the subvolume from MDS metrics."""
    results = helper.collect_subvolume_metrics(
        client=client,
        fs_name=vol_name,
        role="active",
        ranks=ranks or [0],
        path_prefix=subvol_path,
    )
    for _mds_name, items in results.items():
        for it in items:
            if "quota_bytes" in it and "used_bytes" in it:
                return (int(it["quota_bytes"]), int(it["used_bytes"]))
    return None


def _get_expected_used_bytes(
    client, vol_name: str, subvol_name: str, svg: str
) -> Optional[int]:
    """Get bytes_used from subvolume info."""
    subvol_info = fs_util.get_subvolume_info(
        client, vol_name=vol_name, subvol_name=subvol_name, group_name=svg
    )
    return subvol_info.get("bytes_used", 0)


def snap_visibility_test():
    """
    Test Steps:
    a) Verify CLI option default behavior on existing subvolume, new subvolume.
    b) Verify CLI option modify on existing and new subvolume

    Args:
        snap_req_params data type is dict with below params,
        snap_req_params = {
            "config" : pre_upgrade_config,
            "fs_util" : fs_util,
            "clients" : clients,
            "vol_name" : default_fs
        }
        param data types:
        Required -
        pre_upgrade_config - dict data having pre upgrade snapshot configuration
        generated from cephfs_upgrade/upgrade_pre_req.py
        fsutil - fsutil testlib object
        clients - list type, having client objects
        Optional -
        vol_name - cephfs volume name, default is 'cephfs'

    Returns:
        None
    Raises:
        BaseException
    """
    config = test_reqs["config"]
    vol_name = test_reqs.get("vol_name", "cephfs")
    log.info("Get snapshot configuration from pre-upgrade config")
    sv_snap = {}
    for svg in config["CephFS"][vol_name]:
        if "svg" in svg:
            for sv in config["CephFS"][vol_name][svg]:
                sv_data = config["CephFS"][vol_name][svg][sv]
                if sv_data.get("snap_list"):
                    sv_snap.update(
                        {
                            sv: {
                                "snap_list": sv_data["snap_list"],
                                "svg": svg,
                                "mnt_pt": sv_data["mnt_pt"],
                                "mnt_client": sv_data["mnt_client"],
                            }
                        }
                    )
    clients = test_reqs["clients"]
    client = clients[0]
    log.info(
        "Get default CLI option for a existing subvolume snapshot visibility and validate value"
    )
    sv = random.choice(list(sv_snap.keys()))
    sv_old = {
        "vol_name": vol_name,
        "sub_name": sv,
        "group_name": sv_snap[sv]["svg"],
    }
    mnt_client_name = sv_snap[sv]["mnt_client"]
    mnt_client = [i for i in clients if i.node.hostname == mnt_client_name][0]
    sv_new = {
        "vol_name": vol_name,
        "subvol_name": "new_svt_post_upgrade",
        "group_name": sv_snap[sv]["svg"],
    }
    fs_util.create_subvolume(client, **sv_new)
    sv_new.update({"sub_name": "new_svt_post_upgrade"})
    log.info(
        "Get default CLI option for a existing and new subvolume snapshot visibility and validate value"
    )
    for kwargs in [sv_old, sv_new]:
        actual_snap_visibility = snap_util.snapshot_visibility(client, "get", **kwargs)
        if actual_snap_visibility != "1":
            str1 = f"Expected : 1,Actual:{actual_snap_visibility}"
            log.error(
                "Snapshot Visibility on existing subvolume post upgrade is not as Expected.%s",
                str1,
            )
            return 1

    def get_nfs_details():
        nfs_config = config["NFS"]
        for i in nfs_config:
            nfs_name = i
            break
        nfs_config = config["NFS"][nfs_name]
        for i in nfs_config:
            nfs_export_name = i
            break
        nfs_server_name = nfs_config[nfs_export_name]["nfs_server"]
        return nfs_name, nfs_server_name

    mnt_list = ["fuse"]
    if ibm_build:
        nfs_name, nfs_server_name = get_nfs_details()
        mnt_list.append("nfs")
    for sv_iter in [sv_old, sv_new]:
        sv_iter.update(
            {
                "value": "false",
                "mnt_type": random.choice(mnt_list),
                "client_respect_snapshot_visibility": "true",
            }
        )
        if ibm_build:
            sv_iter.update(
                {
                    "nfs_server": nfs_server_name,
                    "nfs_name": nfs_name,
                }
            )
    log.info("Toggle CLI option on existing and new subvolume")
    for kwargs in [sv_old, sv_new]:
        actual_snap_visibility = snap_util.snapshot_visibility(client, "set", **kwargs)
        snap_util.snapshot_visibility(client, "set", **kwargs)
        snap_util.snapshot_visibility_client_mgr(client, "client", "set", **kwargs)
        snap_util.snapshot_visibility_client_mgr(client, "mgr", "set", **kwargs)
        log.info("Validate snapshot visibility on mountpoint")
        if snap_util.validate_snapshot_visibility(client, mnt_client, **kwargs):
            return 1

    return 0


def subvolume_metrics_quota_used_test():
    """
    Validate 9.1 subvolume metrics quota_bytes and used_bytes post-upgrade.

    1. Get existing subvolumes from pre_upgrade_config that have fuse mount
       (and optionally quota enabled via get_quota_attrs).
    2. For those subvolumes: get quota_bytes and used_bytes from subvolume
       metrics; compare with get_quota_attrs(mnt_pt) and du -sb(mnt_pt).
       test_status = 0 if match, else 1.
    3. Create new directory on existing fuse mount, add dataset, set quota
       via set_quota_attrs; run subvolume metrics and verify quota_bytes and
       used_bytes with get_quota_attrs and du -sb. test_status = 0 if match, else 1.
    4. Return 0 on success, 1 on failure.

    Uses test_reqs: config, clients, fs_util, helper, vol_name (optional).
    """
    config = test_reqs["config"]
    clients = test_reqs["clients"]
    fs_util = test_reqs["fs_util"]
    helper = test_reqs["helper"]
    vol_name = test_reqs.get("vol_name", "cephfs")
    ranks = [0]

    # Collect fuse-mounted subvolumes from pre_upgrade_config
    fuse_sv_list: List[Dict[str, Any]] = []
    if "CephFS" not in config or vol_name not in config["CephFS"]:
        log.warning(
            "No CephFS config for vol %s; skipping subvolume metrics test", vol_name
        )
        return 0

    for svg in config["CephFS"][vol_name]:
        if not isinstance(config["CephFS"][vol_name][svg], dict):
            continue
        for sv in config["CephFS"][vol_name][svg]:
            sv_data = config["CephFS"][vol_name][svg].get(sv)
            if not isinstance(sv_data, dict):
                continue
            mnt_pt = sv_data.get("mnt_pt")
            mnt_client_name = sv_data.get("mnt_client")
            if not mnt_pt or not mnt_client_name or "fuse" not in mnt_pt:
                continue
            mnt_client = next(
                (c for c in clients if c.node.hostname == mnt_client_name), None
            )
            if not mnt_client:
                continue
            try:
                quota_attrs = fs_util.get_quota_attrs(mnt_client, mnt_pt)
                quota_enabled = quota_attrs.get("bytes", 0) > 0
            except Exception as e:
                log.warning("get_quota_attrs failed for %s: %s", mnt_pt, e)
                quota_enabled = False
            fuse_sv_list.append(
                {
                    "vol_name": vol_name,
                    "svg": svg,
                    "sv": sv,
                    "mnt_pt": mnt_pt,
                    "mnt_client": mnt_client,
                    "quota_enabled": quota_enabled,
                }
            )

    if not fuse_sv_list:
        log.warning(
            "No fuse-mounted subvolumes in pre_upgrade_config; skipping subvolume metrics test"
        )
        return 0

    # Prefer subvolumes with quota enabled;
    candidates = [e for e in fuse_sv_list if e["quota_enabled"]]
    if not candidates:
        log.warning("No subvolumes with quota enabled; skipping subvolume metrics test")
        return 0
    # Step 2: Validate metrics vs get_quota_attrs and used_bytes for existing subvolume
    for entry in candidates:
        mnt_client = entry["mnt_client"]
        mnt_pt = entry["mnt_pt"]
        out, _ = mnt_client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {entry['vol_name']} {entry['sv']} {entry['svg']}",
        )
        subvol_path = (out or "").strip()
        if not subvol_path:
            log.error(
                "Failed to get subvolume path for %s/%s", entry["svg"], entry["sv"]
            )
            continue

        rand_str = "".join(random.choices(string.ascii_letters + string.digits, k=3))
        quota_attrs = fs_util.get_quota_attrs(mnt_client, mnt_pt)
        expected_quota = int(quota_attrs.get("bytes", 0))
        # Add small dataset (e.g. 10MB)
        dd_cmd = f"dd if=/dev/urandom of={mnt_pt}/data_{rand_str}.bin bs=1M count=10 conv=fsync 2>/dev/null"
        mnt_client.exec_command(sudo=True, cmd=dd_cmd)
        time.sleep(2)
        retry_count = 0
        test_fail = 0
        while retry_count < 5:
            test_fail = 0
            result = _get_quota_and_used_from_metrics(
                helper, mnt_client, entry["vol_name"], subvol_path, ranks
            )
            if result is None:
                log.error("No subvolume metrics for %s", subvol_path)
                retry_count += 1
                test_fail += 1
                time.sleep(5)
                continue
            metrics_quota, metrics_used = result

            expected_used_bytes = _get_expected_used_bytes(
                mnt_client, entry["vol_name"], entry["sv"], entry["svg"]
            )

            if metrics_quota != expected_quota:
                log.error(
                    "Step 2: quota_bytes mismatch: expected %s got %s",
                    expected_quota,
                    metrics_quota,
                )
                test_fail += 1
            else:
                log.info(
                    "Step 2: quota_bytes matched: expected %s got %s",
                    expected_quota,
                    metrics_quota,
                )

            if metrics_used != expected_used_bytes:
                log.error(
                    "Step 2: used_bytes mismatch: expected %s got %s",
                    expected_used_bytes,
                    metrics_used,
                )
                test_fail += 1
            else:
                log.info(
                    "Step 2: used_bytes matched: expected %s got %s",
                    expected_used_bytes,
                    metrics_used,
                )
            if test_fail == 0:
                log.info(
                    "Subvolume %s/%s: quota_bytes=%s used_bytes=%s (match get_quota_attrs and expected_used_bytes)",
                    entry["svg"],
                    entry["sv"],
                    metrics_quota,
                    metrics_used,
                )
                break
            else:
                retry_count += 1
                time.sleep(5)
        if test_fail != 0:
            log.error(
                "Failed to validate subvolume metrics for %s/%s",
                entry["svg"],
                entry["sv"],
            )
            return 1

    # Step 3: On first candidate's mount, add dataset, set quota, verify

    entry = candidates[0]
    mnt_client = entry["mnt_client"]
    mnt_pt = entry["mnt_pt"]
    out, _ = mnt_client.exec_command(
        sudo=True,
        cmd=f"ceph fs subvolume getpath {entry['vol_name']} {entry['sv']} {entry['svg']}",
    )
    subvol_path = (out or "").strip()
    # Remove existing quota on mount path before creating new dir and applying quota
    log.info("Removing existing quota on mount path %s", mnt_pt)
    fs_util.set_quota_attrs(mnt_client, "0", "0", mnt_pt)
    time.sleep(1)

    rand_str = "".join(random.choices(string.ascii_letters + string.digits, k=3))
    # Existing dataset is ~10GB, so new quota is 50GB considering new IOs post upgrade
    quota_bytes_new = 50 * 1024 * 1024 * 1024  # 50GB
    fs_util.set_quota_attrs(mnt_client, "1000", quota_bytes_new, mnt_pt)
    time.sleep(2)
    dd_cmd = f"dd if=/dev/urandom of={mnt_pt}/data_{rand_str}.bin bs=1M count=10 conv=fsync 2>/dev/null"
    mnt_client.exec_command(sudo=True, cmd=dd_cmd)
    retry_count = 0
    test_fail = 0
    while retry_count < 5:
        test_fail = 0
        result = _get_quota_and_used_from_metrics(
            helper, mnt_client, entry["vol_name"], subvol_path, ranks
        )
        if result is None:
            log.error("Step 3: No subvolume metrics after setting new quota")
            retry_count += 1
            test_fail += 1
            time.sleep(5)
            continue

        metrics_quota, metrics_used = result
        expected_used_bytes = _get_expected_used_bytes(
            mnt_client, entry["vol_name"], entry["sv"], entry["svg"]
        )
        if metrics_used != expected_used_bytes:
            log.error(
                "Step 3: new used_bytes mismatch: expected %s got %s",
                expected_used_bytes,
                metrics_used,
            )
            test_fail += 1
        else:
            log.info(
                "Step 3: new used_bytes matched: expected %s got %s",
                expected_used_bytes,
                metrics_used,
            )

        # New dir: verify get_quota_attrs

        expected_quota_attrs = fs_util.get_quota_attrs(mnt_client, mnt_pt)
        expected_quota_bytes = int(expected_quota_attrs.get("bytes", 0))

        if expected_quota_bytes != metrics_quota:
            log.error(
                "Step 3: new quota mismatch: expected %s got %s",
                expected_quota_bytes,
                metrics_quota,
            )
            test_fail += 1
        else:
            log.info(
                "Step 3: new quota matched: expected %s got %s",
                expected_quota_bytes,
                metrics_quota,
            )
        if test_fail == 0:
            log.info(
                "Subvolume %s/%s: quota_bytes=%s used_bytes=%s (match get_quota_attrs and expected_used_bytes)",
                entry["svg"],
                entry["sv"],
                metrics_quota,
                metrics_used,
            )
            break
        else:
            retry_count += 1
            time.sleep(5)
    if test_fail != 0:
        log.error(
            "Failed to validate subvolume metrics for %s/%s", entry["svg"], entry["sv"]
        )
        return 1
    return 0


def _ensure_stress_ng_installed(client) -> int:
    """Install stress-ng if missing. Return 0 on success, 1 on failure."""
    out, rc = client.exec_command(sudo=True, cmd="stress-ng", check_ec=False)
    if rc == 0 and out.strip():
        log.info("stress-ng is already installed")
        return 0
    log.info("stress-ng not found, installing...")
    try:
        client.exec_command(sudo=True, cmd="yum install -y stress-ng", timeout=300)
        log.info("stress-ng installed successfully")
        return 0
    except Exception as e:
        log.error("Failed to install stress-ng: %s", e)
        return 1


def _run_mds_observability_basic_test(
    helper: MDSMetricsHelper,
    client,
    vol_name: str,
    fuse_mount_dir: str,
    duration_sec: int = 60,
    interval_sec: int = 10,
) -> int:
    """
    Run MDS observability basic test: stress-ng IO and collect mds_rank_perf metrics.
    When validate_metrics is True, verify cpu_usage and open_requests fields exist.
    """
    if _ensure_stress_ng_installed(client) != 0:
        return 1

    mds_metrics_snapshots: List[Dict[str, Any]] = []
    collection_lock = threading.Lock()
    stop_collection = threading.Event()
    stress_ng_pid: Optional[str] = None

    def collect_mds_metrics_loop():
        start = time.time()
        next_t = start
        end = start + duration_sec
        while not stop_collection.is_set():
            now = time.time()
            if now >= next_t:
                try:
                    metrics = helper.collect_mds_metrics(
                        client=client,
                        fs_name=vol_name,
                        role="active",
                        ranks=None,
                    )
                    with collection_lock:
                        mds_metrics_snapshots.append(
                            {"t": int(now), "metrics": metrics}
                        )
                    log.info("[t=%s] Collected MDS metrics: %s", int(now), metrics)
                except Exception as e:
                    log.error("Failed to collect MDS metrics at t=%s: %s", int(now), e)
                next_t += interval_sec
            if now >= end:
                break
            time.sleep(1)

    log.info("Starting stress-ng IO on %s", fuse_mount_dir)
    stress_cmd = (
        f"stress-ng --iomix 4 --open 4 --timeout {duration_sec}s "
        f"--metrics-brief --temp-path {fuse_mount_dir} > /tmp/stress_ng.log 2>&1 & echo $!"
    )
    out, _ = client.exec_command(sudo=True, cmd=stress_cmd)
    stress_ng_pid = out.strip()
    log.info("Started stress-ng (pid=%s)", stress_ng_pid)
    time.sleep(5)

    metrics_thread = threading.Thread(target=collect_mds_metrics_loop)
    metrics_thread.start()
    time.sleep(duration_sec + 5)
    stop_collection.set()
    metrics_thread.join(timeout=10)

    try:
        client.exec_command(
            sudo=True,
            cmd=f"kill -9 {stress_ng_pid} 2>/dev/null",
            check_ec=False,
        )
        log.info("Stopped stress-ng (pid=%s)", stress_ng_pid)
    except Exception:
        pass

    if not mds_metrics_snapshots:
        log.error("No MDS metrics snapshots collected for fs %s", vol_name)
        return 1

    log.info("=== Validating MDS rank performance metrics for fs %s ===", vol_name)
    for snap in mds_metrics_snapshots:
        t = snap["t"]
        metrics = snap["metrics"]
        if not metrics:
            log.error("No metrics collected for [t=%s]", t)
            return 1
        log.info("Metrics from counter dump - [t=%s] metrics: %s", t, metrics)
        for mds_name, items in metrics.items():
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
                    "mds_name:%s rank:%s - Missing required field: "
                    "cpu_usage or open_requests, check %s",
                    mds_name,
                    rank,
                    counters,
                )
                return 1
            log.info(
                "[t=%s] %s rank=%s cpu_usage=%s open_requests=%s",
                t,
                mds_name,
                rank,
                counters.get("cpu_usage"),
                counters.get("open_requests"),
            )
    log.info("=== MDS rank performance metrics validated successfully ===")
    return 0


def _get_fuse_mount_from_config(
    config: Dict[str, Any], vol_name: str, clients: List
) -> Optional[Dict[str, Any]]:
    """Return first fuse-mounted subvolume entry from pre_upgrade_config."""
    vol_name = list(config["CephFS"].keys())[0]
    for svg in config["CephFS"][vol_name]:
        if not isinstance(config["CephFS"][vol_name][svg], dict):
            continue
        for sv in config["CephFS"][vol_name][svg]:
            sv_data = config["CephFS"][vol_name][svg].get(sv)
            if not isinstance(sv_data, dict):
                continue
            mnt_pt = sv_data.get("mnt_pt")
            mnt_client_name = sv_data.get("mnt_client")
            if not mnt_pt or not mnt_client_name or "fuse" not in mnt_pt:
                continue
            mnt_client = next(
                (c for c in clients if c.node.hostname == mnt_client_name), None
            )
            if mnt_client:
                return {
                    "vol_name": vol_name,
                    "svg": svg,
                    "sv": sv,
                    "mnt_pt": mnt_pt,
                    "mnt_client": mnt_client,
                }
    return None


def mds_observability_test():
    """
    Post-upgrade MDS observability validation.

    1. On existing CephFS volume from pre-upgrade config: run MDS observability
       basic test on a fuse-mounted subvolume. Skip top comparison and validation.
    2. Create a new FS volume, run MDS observability basic test with CPU
       validation skipped, then remove the new FS volume.

    Uses test_reqs: config, clients, fs_util, helper, ceph_cluster, vol_name (optional).
    """
    config = test_reqs["config"]
    clients = test_reqs["clients"]
    fs_util = test_reqs["fs_util"]
    helper = test_reqs["helper"]
    vol_name = list(config["CephFS"].keys())[0]
    client = clients[0]

    # Step 1: existing volume — collect metrics only, no validation
    fuse_entry = _get_fuse_mount_from_config(config, vol_name, clients)
    if not fuse_entry:
        log.warning(
            "No fuse-mounted subvolumes in pre_upgrade_config for vol %s; "
            "skipping existing-volume MDS observability test",
            vol_name,
        )
    else:
        log.info(
            "Step 1: MDS observability on existing volume %s mount %s",
            vol_name,
            fuse_entry["mnt_pt"],
        )
        if (
            _run_mds_observability_basic_test(
                helper,
                fuse_entry["mnt_client"],
                fuse_entry["vol_name"],
                fuse_entry["mnt_pt"],
            )
            != 0
        ):
            log.error("Step 1: MDS observability on existing volume failed")
            return 1
        log.info("Step 1: MDS observability on existing volume succeeded")

    # Step 2: new FS volume — validate metric fields, skip CPU comparison
    new_fs_name = "cephfs_mds_obs_upgrade"
    mounting_dir = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
    )
    fuse_mount_dir = f"/mnt/cephfs_fuse{mounting_dir}/"
    subvol_name = "subvolume_mds_obs"
    fs_created = False
    subvol_created = False
    test_status = 0

    try:
        log.info("Step 2: Creating new FS volume %s", new_fs_name)
        fs_util.create_fs(client, new_fs_name)
        fs_created = True

        log.info("Setting max_mds to 2 for filesystem %s", new_fs_name)
        client.exec_command(sudo=True, cmd=f"ceph fs set {new_fs_name} max_mds 2")
        time.sleep(5)

        fs_util.create_subvolume(
            client,
            vol_name=new_fs_name,
            subvol_name=subvol_name,
            size="5368709120",
        )
        subvol_created = True

        subvol_path = common_util.subvolume_get_path(
            client,
            new_fs_name,
            subvolume_name=subvol_name,
        )
        log.info("Subvolume path is %s", subvol_path)

        fs_util.fuse_mount(
            [client],
            fuse_mount_dir,
            extra_params=f" -r {subvol_path} --client_fs {new_fs_name}",
        )

        client.exec_command(
            sudo=True,
            cmd="ceph config set mds mds_export_ephemeral_distributed true",
        )
        client.exec_command(
            sudo=True,
            cmd=f"setfattr -n ceph.dir.pin.distributed -v 1 {fuse_mount_dir}",
        )

        if (
            _run_mds_observability_basic_test(
                helper, client, new_fs_name, fuse_mount_dir
            )
            != 0
        ):
            log.error("Step 2: MDS observability on new FS volume failed")
            test_status = 1
        else:
            log.info("Step 2: MDS observability on new FS volume succeeded")

    finally:
        client.exec_command(
            sudo=True,
            cmd="ceph config set mds mds_export_ephemeral_distributed false",
        )
        try:
            client.exec_command(
                sudo=True, cmd=f"umount -l {fuse_mount_dir}", check_ec=False
            )
            client.exec_command(
                sudo=True, cmd=f"rm -rf {fuse_mount_dir}", check_ec=False
            )
        except Exception as e:
            log.error("Unmount/cleanup fuse mount failed: %s", e)
        if subvol_created:
            try:
                fs_util.remove_subvolume(
                    client, vol_name=new_fs_name, subvol_name=subvol_name
                )
            except Exception as e:
                log.error("Subvolume cleanup failed: %s", e)
        if fs_created:
            try:
                fs_util.remove_fs(client, vol_name=new_fs_name)
                log.info("Removed new FS volume %s", new_fs_name)
            except Exception as e:
                log.error("Filesystem cleanup failed: %s", e)
                test_status = 1
    return test_status


def run(ceph_cluster, **kw):
    """
    Test Details:
    1. Toggle Snapshot Visibility :
        CEPH-83621389 : Verify CLI option default behavior and option modify on existing subvolume, new subvolume
    2. Validate 9.1 subvolume metrics quota_bytes and used_bytes post-upgrade.
    3. MDS observability post-upgrade validation on existing and new FS volumes.

    """
    try:
        global ibm_build, common_util, fs_util, snap_util, test_reqs
        fs_util = FsUtils(ceph_cluster)
        snap_util = SnapUtils(ceph_cluster)
        helper = MDSMetricsHelper(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        common_util = CephFSCommonUtils(ceph_cluster)
        test_data = kw.get("test_data")
        config_kw = kw.get("config", {})
        build = config_kw.get("build", config_kw.get("rhbuild", ""))
        log.info("Get the Ceph pre-upgrade config data from cephfs_upgrade_config.json")
        f = clients[0].remote_file(
            sudo=True,
            file_name="/home/cephuser/cephfs_upgrade_config.json",
            file_mode="r",
        )
        pre_upgrade_config = json.load(f)
        test_reqs = {
            "config": pre_upgrade_config,
            "clients": clients,
            "nfs_servers": nfs_servers,
            "fs_util": fs_util,
            "helper": helper,
        }
        f.close()
        ibm_build = fs_util.get_custom_config_value(test_data, "ibm-build")
        space_str = "\t\t\t\t\t\t\t\t\t"

        log.info(
            f"\n\n {space_str}Test1 : Post-upgrade Snapshot Visibility Toggle Validation\n"
        )
        test_status = snap_visibility_test()
        if test_status == 1:
            log.error("Test 1 : Post upgrade Snapshot Visibility validation failed")
            return 1
        log.info("Post upgrade Snapshot Visibility validation succeeded \n")

        log.info(
            f"\n\n {space_str}Test2 : Post-upgrade Subvolume Metrics (quota_bytes / used_bytes) Validation\n"
        )
        if build and LooseVersion(build) >= LooseVersion("9.1"):
            test_status = subvolume_metrics_quota_used_test()
            if test_status == 1:
                log.error(
                    "Test 2 : Post upgrade Subvolume metrics quota_bytes/used_bytes validation failed"
                )
                return 1
            log.info("Post upgrade Subvolume metrics validation succeeded \n")
        else:
            log.info("Skipping Test 2 : requires Ceph version >= 9.1 (build=%s)", build)

        log.info(f"\n\n {space_str}Test3 : Post-upgrade MDS Observability Validation\n")
        if build and LooseVersion(build) >= LooseVersion("9.1"):
            test_status = mds_observability_test()
            if test_status == 1:
                log.error("Test 3 : Post upgrade MDS observability validation failed")
                return 1
            log.info("Post upgrade MDS observability validation succeeded \n")
        else:
            log.info("Skipping Test 3 : requires Ceph version >= 9.1 (build=%s)", build)

        return 0

    except Exception as e:
        log.info(traceback.format_exc())
        log.error(e)
        return 1


# HELPER ROUTINES
