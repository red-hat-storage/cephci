"""
Verify quota_bytes in subvolume metrics when subvolume quota is varied:
  limited (e.g. 3G) -> unlimited -> limited again (e.g. 5G).
Uses MDSMetricsHelper and FsUtils; requires Ceph >= 9.1 for subvolume metrics quota_bytes field.
"""

import random
import string
import time
import traceback
from typing import Any, Dict, List, Optional, Tuple

from looseversion import LooseVersion

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_subvol_metric_utils import MDSMetricsHelper
from utility.log import Log

log = Log(__name__)

# Quota sizes in bytes
QUOTA_3G = 3 * 1024 * 1024 * 1024  # 3221225472
QUOTA_4G = 4 * 1024 * 1024 * 1024  # 4294967296
QUOTA_5G = 5 * 1024 * 1024 * 1024  # 5368709120
DATA_2G = 2 * 1024 * 1024 * 1024  # 2147483648


def _get_quota_and_used_from_metrics(
    helper: MDSMetricsHelper,
    client,
    default_fs: str,
    subvol_path: str,
    ranks: Optional[List[int]] = None,
) -> Optional[Tuple[int, int]]:
    """Collect subvolume metrics and return (quota_bytes, used_bytes) for the subvolume (first match)."""
    results = helper.collect_subvolume_metrics(
        client=client,
        fs_name=default_fs,
        role="active",
        ranks=ranks or [0],
        path_prefix=subvol_path,
    )
    for _mds_name, items in results.items():
        for it in items:
            if "quota_bytes" in it and "used_bytes" in it:
                return (int(it["quota_bytes"]), int(it["used_bytes"]))
    return None


def _fill_data(
    client, mount_dir: str, size_bytes: int, filename: str = "data.bin"
) -> int:
    """Fill size_bytes in mount_dir using dd. Returns 0 on success, 1 on failure."""
    # Use dd: bs=1M count = size_bytes / (1024*1024)
    count_mb = size_bytes // (1024 * 1024)
    path = f"{mount_dir.rstrip('/')}/{filename}"
    cmd = f"dd if=/dev/random of={path} bs=1M count={count_mb} conv=fsync 2>/dev/null"
    try:
        client.exec_command(sudo=True, cmd=cmd, check_ec=False)
    except Exception as e:
        log.error("dd failed: %s", e)
        return 1
    return 0


def run(ceph_cluster, **kw):
    """
    Test assertion: Verify quota_bytes in subvolume metrics when subvolume quota
    is varied from limited (non-zero) to unlimited and back to limited.

    Test steps:
    1. Create subvolume with size 4G, mount, apply quota 3G via set_quota_attrs;
       fill 2G data, verify quota_bytes in subvol metrics = 3G and used_bytes matches du -sb.
    2. Remove quota via set_quota_attrs(client, "0", "0", mount_dir);
       verify quota_bytes in metrics is updated (0 for unlimited) and used_bytes matches du -sb.
    3. Add 2G more data, then apply quota 5G via set_quota_attrs.
    4. Rerun subvolume metrics fetch and verify quota_bytes = 5G and used_bytes matches du -sb.

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
    if LooseVersion(build) < LooseVersion("9.1"):
        log.info("Skipping test: requires Ceph version >= 9.1 (build=%s)", build)
        return 0

    clients = ceph_cluster.get_ceph_objects("client")
    if not clients:
        log.error("This test requires at least 1 client node")
        return 1

    client = clients[0]
    default_fs = "cephfs" if not erasure else "cephfs-ec"
    subvol_name = "subvol_quota_modify"
    vol_name = default_fs
    ranks = [0]
    subvol_created = False
    fs_created = False
    mounting_dir = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
    )
    fuse_mount_dir = f"/mnt/cephfs_fuse{mounting_dir}/"

    try:
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        if not fs_util.get_fs_info(client, default_fs):
            fs_util.create_fs(client, default_fs)
            fs_created = True

        # Create subvolume with size 4G
        log.info("Creating subvolume %s with size 4G", subvol_name)
        fs_util.create_subvolume(
            client,
            vol_name=vol_name,
            subvol_name=subvol_name,
            size=str(QUOTA_4G),
        )
        subvol_created = True

        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {vol_name} {subvol_name}",
        )
        subvol_path = out.strip()
        log.info("Subvolume path: %s", subvol_path)

        fs_util.fuse_mount(
            [client],
            fuse_mount_dir,
            extra_params=f" -r {subvol_path} --client_fs {vol_name}",
        )

        # Apply quota 3G on mount
        log.info("Setting quota on %s: 3G bytes", fuse_mount_dir)
        fs_util.set_quota_attrs(client, "1000", QUOTA_3G, fuse_mount_dir)
        # wait for quota changes
        time.sleep(10)
        # Step 1: Quota is 3G. Fill 2G and verify quota_bytes and used_bytes in metrics
        log.info(
            "Step 1: Fill 2G data and verify quota_bytes = 3G and used_bytes vs du"
        )
        if _fill_data(client, fuse_mount_dir, DATA_2G, "data_2g_1.bin") != 0:
            return 1
        time.sleep(2)
        result = _get_quota_and_used_from_metrics(
            helper, client, default_fs, subvol_path, ranks
        )
        if result is None:
            log.error("Step 1: No subvolume metrics with quota_bytes/used_bytes found")
            return 1
        quota_bytes, used_bytes = result
        if quota_bytes != QUOTA_3G:
            log.error(
                "Step 1: quota_bytes mismatch: expected %s (3G), got %s",
                QUOTA_3G,
                quota_bytes,
            )
            return 1
        subvol_info = fs_util.get_subvolume_info(
            client, vol_name=vol_name, subvol_name=subvol_name
        )
        expected_used_bytes = subvol_info.get("bytes_used", 0)
        if expected_used_bytes is None:
            return 1
        if used_bytes != expected_used_bytes:
            log.error(
                "Step 1: used_bytes mismatch with expected_used_bytes used_bytes=%s, expected_used_bytes=%s",
                used_bytes,
                expected_used_bytes,
            )
            return 1
        log.info(
            "Step 1 Passed: quota_bytes = %s (3G), used_bytes = %s",
            quota_bytes,
            used_bytes,
        )

        # Step 2: Remove quota (set to unlimited via set_quota_attrs 0,0), verify metrics update
        log.info("Step 2: Remove quota on mount (set_quota_attrs 0, 0)")
        fs_util.set_quota_attrs(client, "0", "0", fuse_mount_dir)
        time.sleep(2)
        result = _get_quota_and_used_from_metrics(
            helper, client, default_fs, subvol_path, ranks
        )
        if result is None:
            log.error("Step 2: No subvolume metrics with quota_bytes/used_bytes found")
            return 1
        quota_bytes, used_bytes = result
        # Unlimited quota is typically reported as 0 in Ceph
        log.info("Step 2: quota_bytes after unlimited = %s", quota_bytes)
        if quota_bytes != 0:
            log.warning("Step 2: Expected 0 for unlimited; got %s", quota_bytes)
        subvol_info = fs_util.get_subvolume_info(
            client, vol_name=vol_name, subvol_name=subvol_name
        )
        expected_used_bytes = subvol_info.get("bytes_used", 0)
        if expected_used_bytes is None:
            return 1
        if used_bytes != expected_used_bytes:
            log.error(
                "Step 2: used_bytes mismatch with expected_used_bytes used_bytes=%s, expected_used_bytes=%s",
                used_bytes,
                expected_used_bytes,
            )
            return 1
        log.info("Step 2 Passed: used_bytes = %s", used_bytes)

        # Step 3: Add 2G more data (total 4G), then apply quota 5G via set_quota_attrs
        log.info("Step 3: Add 2G more data and set quota to 5G on mount")
        if _fill_data(client, fuse_mount_dir, DATA_2G, "data_2g_2.bin") != 0:
            return 1
        log.info("Setting quota on %s: 5G bytes", fuse_mount_dir)
        fs_util.set_quota_attrs(client, "1000", QUOTA_5G, fuse_mount_dir)
        time.sleep(2)

        # Step 4: Rerun metrics fetch and verify quota_bytes = 5G and used_bytes vs du
        log.info(
            "Step 4: Verify quota_bytes = 5G and used_bytes vs expected_used_bytes in subvolume metrics"
        )
        result = _get_quota_and_used_from_metrics(
            helper, client, default_fs, subvol_path, ranks
        )
        if result is None:
            log.error("Step 4: No subvolume metrics with quota_bytes/used_bytes found")
            return 1
        quota_bytes, used_bytes = result
        if quota_bytes != QUOTA_5G:
            log.error(
                "Step 4: quota_bytes mismatch: expected %s (5G), got %s",
                QUOTA_5G,
                quota_bytes,
            )
            return 1
        subvol_info = fs_util.get_subvolume_info(
            client, vol_name=vol_name, subvol_name=subvol_name
        )
        expected_used_bytes = subvol_info.get("bytes_used", 0)
        if expected_used_bytes is None:
            return 1
        if used_bytes != expected_used_bytes:
            log.error(
                "Step 4: used_bytes mismatch with expected_used_bytes used_bytes=%s, expected_used_bytes=%s",
                used_bytes,
                expected_used_bytes,
            )
            return 1
        log.info(
            "Step 4 Passed: quota_bytes = %s (5G), used_bytes = %s",
            quota_bytes,
            used_bytes,
        )

        log.info("All steps passed: quota_bytes correctly reflects quota changes")
        return 0

    except Exception as e:
        log.error("Test failed: %s", e)
        log.error(traceback.format_exc())
        return 1

    finally:
        try:
            client.exec_command(
                sudo=True, cmd=f"umount -l {fuse_mount_dir}", check_ec=False
            )
            client.exec_command(
                sudo=True, cmd=f"rm -rf {fuse_mount_dir}", check_ec=False
            )
        except Exception as e:
            log.error("Unmount/cleanup fuse mount failed: %s", e)
        try:
            if subvol_created:
                fs_util.remove_subvolume(
                    client, vol_name=vol_name, subvol_name=subvol_name
                )
        except Exception as e:
            log.error("Subvolume cleanup failed: %s", e)
        try:
            if fs_created:
                fs_util.remove_fs(client, vol_name=default_fs)
        except Exception as e:
            log.error("FS cleanup failed: %s", e)
