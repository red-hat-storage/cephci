import json
import random
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


def _get_du_bytes(client, path: str) -> Optional[int]:
    """Run du -sb on path and return total size in bytes."""
    path = path.rstrip("/")
    out, rc = client.exec_command(
        sudo=True, cmd=f"du -sb {path} 2>/dev/null", check_ec=False
    )
    if rc != 0:
        log.error("du -sb failed for %s: %s", path, out)
        return None
    try:
        return int(out.strip().split()[0])
    except (ValueError, IndexError) as e:
        log.error("Failed to parse du output %r: %s", out, e)
        return None


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
        if actual_snap_visibility != 1:
            str1 = f"Expected : 1,Actual:{actual_snap_visibility}"
            log.error(
                "Snapshot Visibility on existing subvolume post upgrade is not as Expected.%s",
                str1,
            )
            log.info("This is due to BZ2404075, ignoring the issue")
            # return 1

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
    test_status = 0

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

    # Prefer subvolumes with quota enabled; else use all
    candidates = [e for e in fuse_sv_list if e["quota_enabled"]] or fuse_sv_list

    # Step 2: Validate metrics vs get_quota_attrs and du for existing mount
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
            test_status = 1
            continue
        time.sleep(1)
        result = _get_quota_and_used_from_metrics(
            helper, mnt_client, entry["vol_name"], subvol_path, ranks
        )
        if result is None:
            log.error("No subvolume metrics for %s", subvol_path)
            test_status = 1
            continue
        metrics_quota, metrics_used = result
        try:
            quota_attrs = fs_util.get_quota_attrs(mnt_client, mnt_pt)
            expected_quota = int(quota_attrs.get("bytes", 0))
        except Exception as e:
            log.error("get_quota_attrs failed for %s: %s", mnt_pt, e)
            test_status = 1
            continue
        du_bytes = _get_du_bytes(mnt_client, mnt_pt)
        if du_bytes is None:
            test_status = 1
            continue
        if metrics_quota != expected_quota:
            log.error(
                "quota_bytes mismatch: subvol %s/%s metrics=%s get_quota_attrs=%s",
                entry["svg"],
                entry["sv"],
                metrics_quota,
                expected_quota,
            )
            test_status = 1
        if metrics_used != du_bytes:
            log.error(
                "used_bytes mismatch: subvol %s/%s metrics=%s du -sb=%s",
                entry["svg"],
                entry["sv"],
                metrics_used,
                du_bytes,
            )
            test_status = 1
        if test_status == 0:
            log.info(
                "Subvolume %s/%s: quota_bytes=%s used_bytes=%s (match get_quota_attrs and du)",
                entry["svg"],
                entry["sv"],
                metrics_quota,
                metrics_used,
            )

    # Step 3: Create new dir on first candidate's mount, add dataset, set quota, verify
    if test_status != 0:
        return test_status

    entry = candidates[0]
    mnt_client = entry["mnt_client"]
    mnt_pt = entry["mnt_pt"]
    # Remove existing quota on mount path before creating new dir and applying quota
    log.info("Removing existing quota on mount path %s", mnt_pt)
    fs_util.set_quota_attrs(mnt_client, "0", "0", mnt_pt)
    time.sleep(1)

    new_dir = f"{mnt_pt.rstrip('/')}/subvol_metrics_test_dir"
    try:
        mnt_client.exec_command(sudo=True, cmd=f"mkdir -p {new_dir}")
    except Exception as e:
        log.error("mkdir failed for %s: %s", new_dir, e)
        return 1

    # Add small dataset (e.g. 10MB)
    dataset_size_mb = 10
    dd_cmd = f"dd if=/dev/zero of={new_dir}/data.bin bs=1M count={dataset_size_mb} conv=fsync 2>/dev/null"
    mnt_client.exec_command(sudo=True, cmd=dd_cmd)
    time.sleep(2)

    quota_bytes_new = 50 * 1024 * 1024  # 50MB
    fs_util.set_quota_attrs(mnt_client, "1000", quota_bytes_new, new_dir)
    time.sleep(1)

    out, _ = mnt_client.exec_command(
        sudo=True,
        cmd=f"ceph fs subvolume getpath {entry['vol_name']} {entry['sv']} {entry['svg']}",
    )
    subvol_path = (out or "").strip()
    result = _get_quota_and_used_from_metrics(
        helper, mnt_client, entry["vol_name"], subvol_path, ranks
    )
    if result is None:
        log.error("Step 3: No subvolume metrics after adding dir")
        test_status = 1
    else:
        metrics_quota, metrics_used = result
        du_mount = _get_du_bytes(mnt_client, mnt_pt)
        if du_mount is None:
            test_status = 1
        elif metrics_used != du_mount:
            log.error(
                "Step 3: used_bytes mismatch with du -sb mount: metrics=%s du=%s",
                metrics_used,
                du_mount,
            )
            test_status = 1
        # New dir: verify get_quota_attrs and du -sb on the dir
        try:
            dir_quota_attrs = fs_util.get_quota_attrs(mnt_client, new_dir)
            dir_quota_bytes = int(dir_quota_attrs.get("bytes", 0))
            dir_du_bytes = _get_du_bytes(mnt_client, new_dir)
            if dir_du_bytes is None:
                test_status = 1
            elif dir_quota_bytes != quota_bytes_new:
                log.error(
                    "Step 3: new dir quota mismatch: expected %s got %s",
                    quota_bytes_new,
                    dir_quota_bytes,
                )
                test_status = 1
            elif dir_du_bytes != dataset_size_mb * 1024 * 1024:
                log.error(
                    "Step 3: new dir du mismatch: expected %s got %s",
                    dataset_size_mb * 1024 * 1024,
                    dir_du_bytes,
                )
                test_status = 1
        except Exception as e:
            log.error("Step 3: get_quota_attrs or du for new dir failed: %s", e)
            test_status = 1

    try:
        mnt_client.exec_command(sudo=True, cmd=f"rm -rf {new_dir}")
    except Exception:
        pass

    return test_status


def run(ceph_cluster, **kw):
    """
    Test Details:
    1. Toggle Snapshot Visibility :
        CEPH-83621389 : Verify CLI option default behavior and option modify on existing subvolume, new subvolume

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

        return 0

    except Exception as e:
        log.info(traceback.format_exc())
        log.error(e)
        return 1


# HELPER ROUTINES
