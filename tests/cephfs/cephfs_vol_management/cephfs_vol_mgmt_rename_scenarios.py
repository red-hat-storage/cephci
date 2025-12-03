import json
import random
import re
import secrets
import string
import time
import traceback

from looseversion import LooseVersion

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from cli.cephadm.cephadm import CephAdm
from cli.utilities.utils import get_service_id
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.lib.fscrypt_utils import FscryptUtils
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log
from utility.utils import get_ceph_version_from_cluster

log = Log(__name__)

subvol_path = []
subvol_fuse_mount = []
subvol_kernel_mount = []
subvol_nfs_mount = []
io_list = []
fs_name = "cephfs_1"
nfs_name = "cephfs-nfs"
nfs_server = "cephfs-nfs"
fuse_mount_dir = []
kernel_mount_dir = []
nfs_mount_dir = []
export_list = []
pool_name = ""


def install_tools(client1):
    client1.exec_command(sudo=True, cmd="dnf install jq -y", check_ec=False)
    client1.exec_command(
        sudo=True,
        cmd="git clone https://github.com/distributed-system-analysis/smallfile.git",
        check_ec=False,
    )
    client1.exec_command(sudo=True, cmd="dnf install cephfs-top -y", check_ec=False)


def mount_subvolumes(ceph_cluster):
    global subvol_path, subvol_fuse_mount, subvol_kernel_mount, subvol_nfs_mount, io_list
    global fuse_mount_dir, kernel_mount_dir, nfs_mount_dir, nfs_name, nfs_server

    client1 = ceph_cluster.get_ceph_objects("client")[0]
    fs_util = FsUtils(ceph_cluster)
    clients = ceph_cluster.get_ceph_objects("client")
    client1 = clients[0]

    # Add NFS server to /etc/hosts
    nfs_servers = ceph_cluster.get_ceph_objects("nfs")
    if nfs_servers and hasattr(nfs_servers[0].node, "ip_address"):
        nfs_server_ip = nfs_servers[0].node.ip_address
        client1.exec_command(
            sudo=True,
            cmd=f"grep -q {nfs_server} /etc/hosts || echo '{nfs_server_ip} {nfs_server}' >> /etc/hosts",
            check_ec=False,
        )
        log.info(f"Added {nfs_server} ({nfs_server_ip}) to /etc/hosts")

    client1.exec_command(sudo=True, cmd="ceph fs volume create %s" % fs_name)
    fs_util.create_subvolumegroup(client1, fs_name, "subvol_group_name_1")
    for i in range(12):
        fs_util.create_subvolume(
            client1, fs_name, "subvol_%d" % i, group_name="subvol_group_name_1"
        )
        out, ec = client1.exec_command(
            sudo=True,
            cmd="ceph fs subvolume getpath %s subvol_%d --group_name=subvol_group_name_1"
            % (fs_name, i),
        )
        subvol_path.append(out.strip())
    log.info("Subvolumes are created")
    log.info("Mount the subvolumes")
    subvol_fuse_mount = [
        subvol_path[0],
        subvol_path[3],
        subvol_path[6],
        subvol_path[9],
    ]
    subvol_kernel_mount = [
        subvol_path[1],
        subvol_path[4],
        subvol_path[7],
        subvol_path[10],
    ]
    subvol_nfs_mount = [
        subvol_path[2],
        subvol_path[5],
        subvol_path[8],
        subvol_path[11],
    ]
    log.info("Fuse Mount")
    rand = "".join(
        secrets.choice(string.ascii_lowercase + string.digits) for _ in range(5)
    )
    for i in range(4):
        fuse_mount_dir.append("/mnt/fuse_%s_%d" % (rand, i))
        kernel_mount_dir.append("/mnt/kernel_%s_%d" % (rand, i))
        nfs_mount_dir.append("/mnt/nfs_%s_%d" % (rand, i))
    mon_node_ips = fs_util.get_mon_node_ips()
    mount_dic = {}
    for i in range(len(subvol_fuse_mount)):
        fs_util.fuse_mount(
            [client1],
            fuse_mount_dir[i],
            extra_params=" -r %s --client_fs %s" % (subvol_fuse_mount[i], fs_name),
        )
        mount_dic[subvol_fuse_mount[i]] = fuse_mount_dir[i]
    for i in range(len(subvol_kernel_mount)):
        fs_util.kernel_mount(
            [client1],
            kernel_mount_dir[i],
            ",".join(mon_node_ips),
            sub_dir=subvol_kernel_mount[i],
            extra_params=",fs=%s" % fs_name,
        )
        mount_dic[subvol_kernel_mount[i]] = kernel_mount_dir[i]
    for i in range(len(subvol_nfs_mount)):
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_list.append(nfs_export_name)
        cmd = "ceph nfs export create cephfs %s %s %s --path=%s" % (
            nfs_name,
            nfs_export_name,
            fs_name,
            subvol_nfs_mount[i],
        )
        log.info(cmd)
        client1.exec_command(sudo=True, cmd=cmd)
        fs_util.cephfs_nfs_mount(client1, nfs_server, nfs_export_name, nfs_mount_dir[i])
        mount_dic[subvol_nfs_mount[i]] = nfs_mount_dir[i]
    io_list = fuse_mount_dir + kernel_mount_dir + nfs_mount_dir


def workflow1(ceph_cluster):
    """
    Workflow 1: Validate volume rename functionality with snapshots, clones, and quotas

    Tests creation of snapshots and clones, setting quotas, and volume renaming
    while ensuring all features continue to work after the rename operation.

    Args:
        ceph_cluster: Ceph cluster object
    """
    global fs_name, nfs_name, nfs_server

    # Basic setup
    fs_util = FsUtils(ceph_cluster)
    clients = ceph_cluster.get_ceph_objects("client")
    client1 = clients[0]
    # 1. Create snapshots
    log.info("Starting snapshot creation")
    snap_list1 = [
        {
            "vol_name": fs_name,
            "subvol_name": "subvol_1",
            "snap_name": "snap_1",
            "group_name": "subvol_group_name_1",
        },
        {
            "vol_name": fs_name,
            "subvol_name": "subvol_2",
            "snap_name": "snap_2",
            "group_name": "subvol_group_name_1",
        },
        {
            "vol_name": fs_name,
            "subvol_name": "subvol_3",
            "snap_name": "snap_3",
            "group_name": "subvol_group_name_1",
        },
    ]

    for snapshot in snap_list1:
        fs_util.create_snapshot(client1, **snapshot, validate=False)

    # 2. Create additional snapshots and prepare for clones
    snap_list2 = [
        {
            "vol_name": fs_name,
            "subvol_name": "subvol_4",
            "snap_name": "snap_4",
            "group_name": "subvol_group_name_1",
        },
        {
            "vol_name": fs_name,
            "subvol_name": "subvol_5",
            "snap_name": "snap_5",
            "group_name": "subvol_group_name_1",
        },
        {
            "vol_name": fs_name,
            "subvol_name": "subvol_6",
            "snap_name": "snap_6",
            "group_name": "subvol_group_name_1",
        },
    ]

    # Clone creation parameters
    clone_list = [
        {
            "vol_name": fs_name,
            "subvol_name": "subvol_4",
            "snap_name": "snap_4",
            "target_subvol_name": "clone4",
            "group_name": "subvol_group_name_1",
            "target_group_name": "subvol_group_name_1",
        },
        {
            "vol_name": fs_name,
            "subvol_name": "subvol_5",
            "snap_name": "snap_5",
            "target_subvol_name": "clone5",
            "group_name": "subvol_group_name_1",
            "target_group_name": "subvol_group_name_1",
        },
        {
            "vol_name": fs_name,
            "subvol_name": "subvol_6",
            "snap_name": "snap_6",
            "target_subvol_name": "clone6",
            "group_name": "subvol_group_name_1",
            "target_group_name": "subvol_group_name_1",
        },
    ]

    # Create snapshots then clones
    for snapshot in snap_list2:
        fs_util.create_snapshot(client1, **snapshot, validate=False)

    for clone in clone_list:
        fs_util.create_clone(client1, **clone, validate=False, timeout=0)

    # 3. Set quotas
    log.info("Starting quota setup")
    file_quota = 200
    byte_quota = 10000000

    # Access global mount directories
    try:
        file_quota_list = [fuse_mount_dir[2], kernel_mount_dir[2]]
        byte_quota_list = [fuse_mount_dir[3], kernel_mount_dir[3]]

        for directory in file_quota_list:
            log.info("Setting file quota on: %s" % directory)
            fs_util.set_quota_attrs(client1, file_quota, "", directory)

        for directory in byte_quota_list:
            log.info("Setting byte quota on: %s" % directory)
            fs_util.set_quota_attrs(client1, "", byte_quota, directory)

        log.info("Quotas before rename:")
        log.info("File quota: %s" % file_quota)
        log.info("Byte quota: %s" % byte_quota)
    except (IndexError, NameError):
        log.warning("Mount directory information not available, skipping quota setup")
        file_quota_list = []
        byte_quota_list = []

    # 4. Rename volume
    new_fs_name = "cephfs_2"
    log.info("Renaming volume: %s -> %s" % (fs_name, new_fs_name))
    fs_util.rename_volume(client1, fs_name, new_fs_name)
    fs_name = new_fs_name  # Update global variable

    # 5. Verify quotas
    if file_quota_list and byte_quota_list:
        file_quotas = []
        byte_quotas = []

        for directory in file_quota_list:
            log.info("Checking file quota on: %s" % directory)
            out = fs_util.get_quota_attrs(client1, directory)
            log.info(out)
            file_quotas.append(out)

        for directory in byte_quota_list:
            log.info("Checking byte quota on: %s" % directory)
            out = fs_util.get_quota_attrs(client1, directory)
            log.info(out)
            byte_quotas.append(out)

        # Validate quotas
        for i in range(len(file_quotas)):
            if file_quotas[i]["files"] != file_quota:
                log.error("File quota has changed")
                raise Exception("File quota not set")

            if byte_quotas[i]["bytes"] != byte_quota:
                log.error("Byte quota has changed")
                raise Exception("Byte quota not set")
    # 6. Verify clones and snapshots
    clone_list_to_check = ["clone4", "clone5", "clone6"]
    # Create additional snapshots
    snap_list3 = [
        {
            "vol_name": fs_name,
            "subvol_name": "subvol_7",
            "snap_name": "snap_7",
            "group_name": "subvol_group_name_1",
        },
        {
            "vol_name": fs_name,
            "subvol_name": "subvol_8",
            "snap_name": "snap_8",
            "group_name": "subvol_group_name_1",
        },
        {
            "vol_name": fs_name,
            "subvol_name": "subvol_9",
            "snap_name": "snap_9",
            "group_name": "subvol_group_name_1",
        },
    ]

    log.info("Creating and verifying snapshots after rename")
    snap_list_to_check = [
        "snap_1",
        "snap_2",
        "snap_3",
        "snap_4",
        "snap_5",
        "snap_6",
        "snap_7",
        "snap_8",
        "snap_9",
    ]

    subvol_name_list = [
        "subvol_1",
        "subvol_2",
        "subvol_3",
        "subvol_4",
        "subvol_5",
        "subvol_6",
        "subvol_7",
        "subvol_8",
        "subvol_9",
    ]

    log.info("###########################################")
    log.info("############## Snapshot check ###############")
    log.info("###########################################")

    # Create new snapshots
    for snapshot in snap_list3:
        fs_util.create_snapshot(client1, **snapshot, validate=False)

    # Check all snapshots
    for subv, snap in zip(subvol_name_list, snap_list_to_check):
        out, ec = client1.exec_command(
            sudo=True,
            cmd="ceph fs subvolume snapshot ls %s %s --group_name subvol_group_name_1"
            % (fs_name, subv),
        )
        if snap not in out:
            log.error("%s is not present" % snap)
            raise Exception("%s is not present" % snap)

    # Check clones
    log.info("#### Clone check ####")
    for clone in clone_list_to_check:
        out, ec = client1.exec_command(
            sudo=True,
            cmd="ceph fs subvolume ls %s --group_name subvol_group_name_1" % fs_name,
        )
        log.info(out)
        if clone not in out:
            log.error("%s is not present" % clone)
            raise Exception("%s is not present" % clone)

    log.info("#### Workflow 1 completed successfully ####")


def workflow2(ceph_cluster):
    """
    Workflow 2: Validate metrics and snapshot scheduling after volume renaming

    Tests that metrics information and snapshot schedules are maintained after renaming a CephFS volume.
    Collects and compares metrics before and after the rename operation to verify consistency.

    Args:
        ceph_cluster: Ceph cluster object
    """
    global fs_name
    fs_util = FsUtils(ceph_cluster)
    clients = ceph_cluster.get_ceph_objects("client")
    client1 = clients[0]
    snaputil = SnapUtils(ceph_cluster)
    snaputil.enable_snap_schedule(client1)
    metric_dir = [fuse_mount_dir[0], kernel_mount_dir[0], nfs_mount_dir[0]]
    daemons_value = {"mds": 10}
    fs_util.enable_logs(client1, daemons_value)
    client1.exec_command(sudo=True, cmd="ceph config set mds debug_mds 20")
    client1.exec_command(sudo=True, cmd="ceph config set mds debug_ms 1")
    metrics_before = fs_util.get_mds_metrics(client1, 0, metric_dir[0], fs_name=fs_name)
    fs_util.rename_volume(client1, fs_name, "cephfs_3")
    fs_name = "cephfs_3"
    time.sleep(10)
    metrics_after = fs_util.get_mds_metrics(client1, 0, metric_dir[0], fs_name=fs_name)
    rw_list = [
        "total_read_ops",
        "total_read_size",
        "total_write_ops",
        "total_write_size",
    ]
    for read in rw_list:
        b_metric = (
            metrics_before["counters"][read]
            if isinstance(metrics_before, dict) and "counters" in metrics_before
            else 0
        )
        a_metric = (
            metrics_after["counters"][read]
            if isinstance(metrics_after, dict) and "counters" in metrics_after
            else 0
        )
        log.info("Metrics before rename %s : %s" % (read, b_metric))
        log.info("Metrics after rename %s : %s" % (read, a_metric))
        if b_metric > a_metric:
            log.error("%s is not same" % read)
            raise Exception("%s is not same" % read)
    log.info("####Metrics are validated after rename####")


def workflow3(ceph_cluster):
    """
    Workflow 3: Validate data pools and configuration settings after volume renaming

    Tests the addition of new data pools to a CephFS volume followed by volume renaming.
    Verifies that client and MDS configurations are preserved after the rename operation.
    Uses various configuration modifications and validations to test the functionality.

    Args:
        ceph_cluster: Ceph cluster object
    """
    global fs_name
    fs_util = FsUtils(ceph_cluster)
    clients = ceph_cluster.get_ceph_objects("client")
    client1 = clients[0]
    log.info("Add additional Data-pool")
    pool_name = "cephfs-new-data-pool"
    fs_util.create_osd_pool(client1, pool_name=pool_name, pg_num=32, pgp_num=32)
    cmd = f"ceph fs add_data_pool {fs_name} {pool_name}"
    client1.exec_command(sudo=True, cmd=cmd)
    log.info("####Data pool is added####")
    log.info("####Checking if the data-pool name is added####")
    out, ec = client1.exec_command(sudo=True, cmd="ceph fs ls")
    if pool_name not in out:
        log.error("%s is not present" % pool_name)
        raise Exception("%s is not present" % pool_name)
    out, ec = client1.exec_command(sudo=True, cmd="ceph osd lspools")
    if pool_name not in out:
        log.error("%s is not present" % pool_name)
        raise Exception("%s is not present" % pool_name)
    fs_util.rename_volume(client1, fs_name, "cephfs_4")
    fs_name = "cephfs_4"
    conf_set = "ceph config set client"
    conf_get = "ceph config get client"
    conf_default1 = [
        ["debug_client", ""],
        ["client_acl_type", ""],
        ["client_cache_mid", ""],
        ["client_cache_size", ""],
        ["client_caps_release_delay", ""],
        ["client_debug_force_sync_read", ""],
        ["client_dirsize_rbytes", ""],
        ["client_max_inline_size", ""],
        ["client_metadata", ""],
        ["client_mount_gid", ""],
        ["client_mount_timeout", ""],
        ["client_mountpoint", ""],
        ["client_oc", ""],
        ["client_oc_max_dirty", ""],
        ["client_oc_max_dirty_age", ""],
        ["client_oc_max_objects", ""],
        ["client_oc_size", ""],
        ["client_oc_target_dirty", ""],
        ["client_permissions", ""],
        ["client_quota_df", ""],
        ["client_readahead_max_bytes", ""],
        ["client_readahead_max_periods", ""],
        ["client_readahead_min", ""],
        ["client_reconnect_stale", ""],
        ["client_snapdir", ""],
        ["client_tick_interval", ""],
        ["client_use_random_mds", ""],
        ["fuse_default_permissions", ""],
        ["fuse_max_write", ""],
        ["fuse_disable_pagecache", ""],
        ["client_debug_getattr_caps", ""],
        ["client_debug_inject_tick_delay", ""],
        ["client_inject_fixed_oldest_tid", ""],
        ["client_inject_release_failure", ""],
        ["client_trace", ""],
    ]
    for conf in conf_default1:
        output = client1.exec_command(sudo=True, cmd=f"{conf_get} {conf[0]}")
        conf[1] = output[0].strip()
    log.info("\n conf default values: %s" % conf_default1)
    conf_target1 = [
        ("debug_client", "20/20"),
        ("client_acl_type", "posix_acl"),
        ("client_cache_mid", "0.76"),
        ("client_cache_size", "16Ki"),
        ("client_caps_release_delay", "5"),
        ("client_debug_force_sync_read", "false"),
        ("client_dirsize_rbytes", "true"),
        ("client_max_inline_size", "4Ki"),
        ("client_metadata", "test"),
        ("client_mount_gid", "-1"),
        ("client_mount_timeout", "301"),
        ("client_mountpoint", "/"),
        ("client_oc", "true"),
        ("client_oc_max_dirty", "100Mi"),
        ("client_oc_max_dirty_age", "5.0"),
        ("client_oc_max_objects", "1000"),
        ("client_oc_size", "200Mi"),
        ("client_oc_target_dirty", "8Mi"),
        ("client_permissions", "true"),
        ("client_quota_df", "true"),
        ("client_readahead_max_bytes", "0"),
        ("client_readahead_max_periods", "4"),
        ("client_readahead_min", "128Ki"),
        ("client_reconnect_stale", "false"),
        ("client_snapdir", ".snap"),
        ("client_tick_interval", "1"),
        ("client_use_random_mds", "false"),
        ("fuse_default_permissions", "false"),
        ("fuse_max_write", "0B"),
        ("fuse_disable_pagecache", "false"),
        ("client_debug_getattr_caps", "true"),
        ("client_debug_inject_tick_delay", "10"),
        ("client_inject_fixed_oldest_tid", "true"),
        ("client_inject_release_failure", "true"),
        ("client_trace", "test"),
    ]
    conf_expected1 = [
        ("debug_client", "20/20"),
        ("client_acl_type", "posix_acl"),
        ("client_cache_mid", "0.760000"),
        ("client_cache_size", "16384"),
        ("client_caps_release_delay", "5"),
        ("client_debug_force_sync_read", "false"),
        ("client_dirsize_rbytes", "true"),
        ("client_max_inline_size", "4096"),
        ("client_metadata", "test"),
        ("client_mount_gid", "-1"),
        ("client_mount_timeout", "301"),
        ("client_mountpoint", "/"),
        ("client_oc", "true"),
        ("client_oc_max_dirty", "104857600"),
        ("client_oc_max_dirty_age", "5.000000"),
        ("client_oc_max_objects", "1000"),
        ("client_oc_size", "209715200"),
        ("client_oc_target_dirty", "8388608"),
        ("client_permissions", "true"),
        ("client_quota_df", "true"),
        ("client_readahead_max_bytes", "0"),
        ("client_readahead_max_periods", "4"),
        ("client_readahead_min", "131072"),
        ("client_reconnect_stale", "false"),
        ("client_snapdir", ".snap"),
        ("client_tick_interval", "1"),
        ("client_use_random_mds", "false"),
        ("fuse_default_permissions", "false"),
        ("fuse_max_write", "0"),
        ("fuse_disable_pagecache", "false"),
        ("client_debug_getattr_caps", "true"),
        ("client_debug_inject_tick_delay", "10"),
        ("client_inject_fixed_oldest_tid", "true"),
        ("client_inject_release_failure", "true"),
        ("client_trace", "test"),
    ]
    for conf in conf_default1:
        client_conf = conf[0]
        output = client1.exec_command(
            sudo=True, cmd=f"ceph config get client {client_conf}"
        )
        conf[1] = output[0].strip()

    for target, expected in zip(conf_target1, conf_expected1):
        client_conf = target[0]
        value = target[1]
        client1.exec_command(sudo=True, cmd=f"{conf_set} {client_conf} {value}")
    fs_util.rename_volume(client1, fs_name, "cephfs_5")
    fs_name = "cephfs_5"
    for target, expected in zip(conf_target1, conf_expected1):
        client_conf = target[0]
        output = client1.exec_command(
            sudo=True, cmd=f"ceph config get client {client_conf}"
        )
        if output[0].rstrip() != expected[1]:
            raise Exception("Failed to set %s to %s" % (client_conf, value))
    for conf in conf_default1:
        client_conf = conf[0]
        output = client1.exec_command(
            sudo=True,
            cmd=f"ceph config set client {client_conf} {conf[1]}",
            check_ec=False,
        )
    log.info("Successfully set all the client config values to defaults")
    ceph_version = get_ceph_version_from_cluster(clients[0])
    if LooseVersion(ceph_version) >= LooseVersion("18.1"):
        output = client1.exec_command(
            sudo=True, cmd=f"{conf_get} defer_client_eviction_on_laggy_osds"
        )
        defer_client_eviction_on_laggy_osds = output[0].strip()
        if defer_client_eviction_on_laggy_osds.lower() != "false":
            log.error(
                f"defer_client_eviction_on_laggy_osds values is expected to be disabled by default.\n "
                f"but the value is {defer_client_eviction_on_laggy_osds}"
            )

            return 1
    if LooseVersion(ceph_version) >= LooseVersion("18.2"):
        set_balance_automate_and_validate(client1, fs_name=fs_name)
    conf_default = [
        ["mds_cache_mid", ""],
        ["mds_dir_max_commit_size", ""],
        ["mds_dir_max_entries", ""],
        ["mds_decay_halflife", ""],
        ["mds_beacon_interval", ""],
        ["mds_beacon_grace", ""],
        ["mon_mds_blocklist_interval", ""],
        ["mds_reconnect_timeout", ""],
        ["mds_tick_interval", ""],
        ["mds_dirstat_min_interval", ""],
        ["mds_scatter_nudge_interval", ""],
        ["mds_client_prealloc_inos", ""],
        ["mds_early_reply", ""],
        ["mds_default_dir_hash", ""],
        ["mds_log_skip_corrupt_events", ""],
        ["mds_log_max_events", ""],
        ["mds_log_max_segments", ""],
        ["mds_bal_sample_interval", ""],
        ["mds_bal_replicate_threshold", ""],
        ["mds_bal_unreplicate_threshold", ""],
        ["mds_bal_split_size", ""],
        ["mds_bal_split_rd", ""],
        ["mds_bal_split_wr", ""],
        ["mds_bal_split_bits", ""],
        ["mds_bal_merge_size", ""],
        ["mds_bal_interval", ""],
        ["mds_bal_fragment_interval", ""],
        ["mds_bal_fragment_fast_factor", ""],
        ["mds_bal_fragment_size_max", ""],
        ["mds_bal_idle_threshold", ""],
        ["mds_bal_max", ""],
        ["mds_bal_max_until", ""],
        ["mds_bal_mode", ""],
        ["mds_bal_min_rebalance", ""],
        ["mds_bal_min_start", ""],
        ["mds_bal_need_min", ""],
        ["mds_bal_need_max", ""],
        ["mds_bal_midchunk", ""],
        ["mds_bal_minchunk", ""],
        ["mds_replay_interval", ""],
        ["mds_shutdown_check", ""],
        ["mds_thrash_exports", ""],
        ["mds_thrash_fragments", ""],
        ["mds_dump_cache_on_map", ""],
        ["mds_dump_cache_after_rejoin", ""],
        ["mds_verify_scatter", ""],
        ["mds_debug_scatterstat", ""],
        ["mds_debug_frag", ""],
        ["mds_debug_auth_pins", ""],
        ["mds_debug_subtrees", ""],
        ["mds_kill_mdstable_at", ""],
        ["mds_kill_export_at", ""],
        ["mds_kill_import_at", ""],
        ["mds_kill_link_at", ""],
        ["mds_kill_rename_at", ""],
        ["mds_inject_skip_replaying_inotable", ""],
        (
            ["mds_kill_skip_replaying_inotable", ""]
            if LooseVersion(ceph_version) <= LooseVersion("19.1.1")
            else ["mds_kill_after_journal_logs_flushed", ""]
        ),
        ["mds_wipe_sessions", ""],
        ["mds_wipe_ino_prealloc", ""],
        ["mds_skip_ino", ""],
        ["mds_min_caps_per_client", ""],
    ]
    conf_target = [
        ("mds_cache_mid", "0.800000"),
        ("mds_dir_max_commit_size", "11"),
        ("mds_dir_max_entries", "1"),
        ("mds_decay_halflife", "6.000000"),
        ("mds_beacon_interval", "5.000000"),
        ("mds_beacon_grace", "16.000000"),
        ("mon_mds_blocklist_interval", "3600.000000"),
        ("mds_reconnect_timeout", "46.000000"),
        ("mds_tick_interval", "4.000000"),
        ("mds_dirstat_min_interval", "2.000000"),
        ("mds_scatter_nudge_interval", "6.000000"),
        ("mds_client_prealloc_inos", "1001"),
        ("mds_early_reply", "false"),
        ("mds_default_dir_hash", "3"),
        ("mds_log_skip_corrupt_events", "true"),
        ("mds_log_max_events", "0"),
        ("mds_log_max_segments", "256"),
        ("mds_bal_sample_interval", "4.000000"),
        ("mds_bal_replicate_threshold", "8001.000000"),
        ("mds_bal_unreplicate_threshold", "0.100000"),
        ("mds_bal_split_size", "10001"),
        ("mds_bal_split_rd", "25001.000000"),
        ("mds_bal_split_wr", "10001.000000"),
        ("mds_bal_split_bits", "6"),
        ("mds_bal_merge_size", "51"),
        ("mds_bal_interval", "11"),
        ("mds_bal_fragment_interval", "6"),
        ("mds_bal_fragment_fast_factor", "1.700000"),
        ("mds_bal_fragment_size_max", "100001"),
        ("mds_bal_idle_threshold", "0.100000"),
        ("mds_bal_max", "1"),
        ("mds_bal_max_until", "1"),
        ("mds_bal_mode", "1"),
        ("mds_bal_min_rebalance", "0.200000"),
        ("mds_bal_min_start", "0.300000"),
        ("mds_bal_need_min", "0.900000"),
        ("mds_bal_need_max", "1.500000"),
        ("mds_bal_midchunk", "0.500000"),
        ("mds_bal_minchunk", "0.005000"),
        ("mds_replay_interval", "1.500000"),
        ("mds_shutdown_check", "1"),
        ("mds_thrash_exports", "1"),
        ("mds_thrash_fragments", "1"),
        ("mds_dump_cache_on_map", "true"),
        ("mds_dump_cache_after_rejoin", "true"),
        ("mds_verify_scatter", "true"),
        ("mds_debug_scatterstat", "true"),
        ("mds_debug_frag", "true"),
        ("mds_debug_auth_pins", "true"),
        ("mds_debug_subtrees", "true"),
        ("mds_kill_mdstable_at", "1"),
        ("mds_kill_export_at", "1"),
        ("mds_kill_import_at", "1"),
        ("mds_kill_link_at", "1"),
        ("mds_kill_rename_at", "1"),
        ("mds_inject_skip_replaying_inotable", "true"),
        (
            ("mds_kill_skip_replaying_inotable", "true")
            if LooseVersion(ceph_version) <= LooseVersion("19.1.1")
            else ("mds_kill_after_journal_logs_flushed", "true")
        ),
        ("mds_wipe_sessions", "true"),
        ("mds_wipe_ino_prealloc", "true"),
        ("mds_skip_ino", "1"),
        ("mds_min_caps_per_client", "101"),
    ]
    conf_set = "ceph config set mds"
    conf_get = "ceph config get mds"
    for conf in conf_default:
        output = client1.exec_command(sudo=True, cmd=f"{conf_get} {conf[0]}")
        conf[1] = output[0].strip()
    for target in conf_target:
        client_conf = target[0]
        value = target[1]
        client1.exec_command(sudo=True, cmd=f"{conf_set} {client_conf} {value}")
    for target in conf_target:
        client_conf = target[0]
        expected_value = target[1]
        output = client1.exec_command(
            sudo=True, cmd=f"ceph config get mds {client_conf}"
        )
        log.info("%s = %s" % (client_conf, output[0].rstrip()))
        log.info("Expected: %s" % expected_value)
        if output[0].rstrip() != expected_value:
            raise CommandFailed(
                "Failed to set %s to %s" % (client_conf, expected_value)
            )
    for conf in conf_default:
        client_conf = conf[0]
        value = conf[1]
        client1.exec_command(sudo=True, cmd=f"{conf_set} {client_conf} {value}")


def workflow4(ceph_cluster):
    """
    Workflow 4: Validate volume renaming and failover in a multi-filesystem environment

    Tests volume rename functionality in an environment with multiple CephFS volumes.
    Simulates MGR failover scenarios and verifies that system operations remain normal after renaming.
    Also tests standby-replay MDS configuration and MDS failover to validate high availability features.

    Args:
        ceph_cluster: Ceph cluster object
    """
    fs_util = FsUtils(ceph_cluster)
    clients = ceph_cluster.get_ceph_objects("client")
    client1 = clients[0]
    log.info("###########################################")
    log.info("####Volume Rename in multi fs scenario####")
    log.info("###########################################")
    new_fs_name = "cephfs_multi"
    fs_util.create_fs(client1, new_fs_name)
    rand = "".join(
        secrets.choice(string.ascii_lowercase + string.digits) for _ in range(5)
    )
    multi_mount_dir = "/mnt/fuse_%s_%s" % (rand, rand)
    fs_util.fuse_mount(
        [client1], multi_mount_dir, extra_params=f" --client_fs {new_fs_name}"
    )
    log.info("####Fail mgr scenario####")
    node = ceph_cluster.get_nodes(role="mgr")[0]
    services = get_service_id(node, "mgr")
    regex, mgr_service = r"(?<=@mgr\.)[\w.-]+(?=\.service)", None
    mgr_service = None

    if services is None:
        services = []

    for service in services:
        mgr_service = re.findall(regex, service)
        if mgr_service:
            break
    # Fail mgr service
    if (
        mgr_service
        and len(mgr_service) > 0
        and CephAdm(node).ceph.mgr.fail(mgr=mgr_service[0])
    ):
        raise Exception("Failed to fail mgr service")
    mgrs = ceph_cluster.get_ceph_objects("mgr")
    for node in mgrs:
        for service in services:
            node.exec_command(
                sudo=True, cmd=f"systemctl restart {service}", check_ec=False
            )
    time.sleep(10)
    log.info("####Waiting for mgr daemon is active####")
    mgr_out, ec = client1.exec_command(
        sudo=True, cmd="ceph orch ps --daemon_type=mgr -f json-pretty"
    )
    mgr_out = json.loads(mgr_out)
    log.info(mgr_out[0])
    mgr_active = {}
    log.info(f"mgr type: {type(mgr_out)}")
    for item in mgr_out:
        log.info(item)
        mgr_active[item["daemon_name"]] = item["is_active"]
    for key, value in mgr_active.items():
        if value == "False":
            log.error(f"{key} is not active")
            raise Exception(f"{key} is not active")
    log.info("####Mgr is active####")
    log.info("####Volume Rename####")
    names_after_mgr_fail, _ = client1.exec_command(sudo=True, cmd="ceph fs ls")
    if new_fs_name not in names_after_mgr_fail:
        log.error(f"{new_fs_name} is not present")
        raise Exception(f"{new_fs_name} is not present")
    log.info("####Volume Rename is validated when mgr fail over case####")
    log.info(
        "####Configuring the standby-replay on primary volume and after renaming "
        "the standby-replay should function properly####"
    )
    log.info("setting standby-replay on renamed volume")
    client1.exec_command(
        sudo=True, cmd=f"ceph fs set {fs_name} allow_standby_replay true"
    )
    time.sleep(10)
    log.info("Check if the standby-replay is set")
    out, ec = client1.exec_command(sudo=True, cmd=f"ceph fs status {fs_name}")
    log.info(out)
    if "standby-replay" not in out:
        log.error("standby-replay is not set")
        raise Exception("standby-replay is not set")
    log.info("####Fail rank 0 mds and check if the standby mds replace it####")
    mds_status = json.loads(
        client1.exec_command(sudo=True, cmd=f"ceph fs status {fs_name} -f json-pretty")[
            0
        ]
    )
    log.info(mds_status)
    rank0_mds = next(
        (mds["name"] for mds in mds_status["mdsmap"] if mds["rank"] == 0), None
    )
    if not rank0_mds:
        raise Exception("No active rank 0 MDS found")
    log.info("Failing rank 0 MDS: %s", rank0_mds)
    client1.exec_command(sudo=True, cmd="ceph mds fail %s" % rank0_mds)
    log.info(
        "####Fail the rank0 mds and ensure that the standby-replay node becomes active and takes over rank0.####"
    )
    # Verify failover
    time.sleep(30)  # Allow time for failover
    new_status = json.loads(
        client1.exec_command(
            sudo=True, cmd="ceph fs status %s -f json-pretty" % fs_name
        )[0]
    )
    new_rank0 = next(
        (mds["name"] for mds in new_status["mdsmap"] if mds["rank"] == 0), None
    )
    new_mds_info = next(
        (mds for mds in new_status["mdsmap"] if mds["name"] == new_rank0), None
    )
    log.info("failed mds info %s", rank0_mds)
    log.info("new mds info %s", new_rank0)
    if new_rank0 == rank0_mds:
        log.error("[FAIL] MDS failover - Same rank 0 active: %s" % rank0_mds)
        raise Exception("MDS failover failed - same rank 0 MDS still active")

    if new_mds_info is not None:
        log.info(
            "[PASS] New active MDS: %s (State: %s)",
            new_rank0,
            new_mds_info["state"].upper(),
        )
    else:
        log.info("[PASS] New active MDS: %s (State unknown)", new_rank0)

    log.info("Standby-replay successfully replaced failed MDS")


def add_fscrypt_config(client, ceph_cluster):
    """
    This method adds fscrypt config to a fuse mountpoint
    """
    global fscrypt_util, fscrypt_params
    test_status = 0
    fscrypt_util = FscryptUtils(ceph_cluster)
    mnt_pt = random.choice(fuse_mount_dir)
    mnt_info = {}
    mnt_info.update({"any_sv": {"fuse": {"mnt_client": client, "mountpoint": mnt_pt}}})
    encrypt_info = fscrypt_util.encrypt_dir_setup(mnt_info)
    encrypt_path = encrypt_info["any_sv"]["path"]
    encrypt_params = encrypt_info["any_sv"]["encrypt_params"]
    fscrypt_util.add_dataset(client, encrypt_path)
    test_status = fscrypt_util.lock(client, encrypt_path)
    protector_id = encrypt_params["protector_id"]
    unlock_args = {"key": encrypt_params["key"]}
    test_status += fscrypt_util.unlock(
        client, encrypt_path, mnt_pt, protector_id, **unlock_args
    )
    fscrypt_params = {
        "encrypt_path": encrypt_path,
        "encrypt_params": encrypt_params,
        "mountpoint": mnt_pt,
    }
    return test_status


def run(ceph_cluster, **kw):
    """
    pre-requisites:
    1. Create a Volume (cephfs_x)
    2.Create 1 Subvolume Group
    3.Create few Subvolumes when needed as part of each workflows.
    4.Mount Subvolume on all clients types Kernel, Fuse, NFS
    5.Continue IO's on all 3 mounts while all of the below operations.
    workflow1:
    1.Create 3 + 3 + 3 + 3 subvolumes - 12 - mount each subvolume to a client type
    2.Configure 3 subvolumes with Manual Snapshots, 3 -
     Clones of all subvolumes mount, and quotas[ 3 - bytes and 3 - Files]
      of Subvolumes. Continue IOs on all subvolumes for some time and stop it.
    3.Rename the Volume.
    4.ensure all the snapshots are present, the file and byte quota that was set are intact.
    5.Create more new manual Snapshots, create more Clones, modify the byte level
     and file level quotas, and continue the IOs for some time.
    workflow2:
    1.Capture metrics using cephfs-top
    2.Create a snapshot schedule on the 3 subvolumes mounted across all 3 clients.
     Schedule it for 5 minutes at an interval of 1 min.
    3.Stop IO's here and Rename the Volume
    4.Once remounted - validate the schedules remain active [check the status].
    5.If required create new schedules using the new volume name.
    6.Recapture metrics using cephfs-top on newly renamed volumes and ensure the metrics are intact.
     - it should be incremented values from the previous top output.
    workflow3:
    1.Adding additional DataPool to the newly renamed Volumes.
    2.Validate the newly added datapools in osd lspools, ceph fs status, ceph fs dump and all other places.
    3.Continue the IOs
    4.Run these tests tests/cephfs/clients/client_option_value_verification.py
     & tests/cephfs/clients/mds_conf_modifying.py  and capture the values set
    5.Stop the IOs
    6.Rename the Volume and validate that all the values set are intact.
    7.Rerun the tests - tests tests/cephfs/clients/client_option_value_verification.py
     & tests/cephfs/clients/mds_conf_modifying.py
    workflow4:
    1. Multifs scenario - Create a new Volume along with the existing one. Total 2 subvolumes
    2. Run IOs on both Volumes.
    3. rename 1 of the Volume
    4. Rename with the name of another volume that already exists.[Negative]
    5. Failover Scenarios before and after renaming should behave the same
    6. configuring the standby-replay on primary volume and after renaming the standby-replay should function properly.
    """
    global export_list, pool_name
    try:
        tc = "CEPH-83607848"
        log.info(f"Running CephFS tests: {tc}")
        fs_util = FsUtils(ceph_cluster)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fscrypt_test = False
        ceph_version = get_ceph_version_from_cluster(client1)
        if LooseVersion(ceph_version) >= LooseVersion("19.2.1"):
            fscrypt_test = True
        if fscrypt_test:
            log.info("Cleanup stale mounts, required for fscrypt test")
            for client_tmp in clients:
                for mnt_prefix in [
                    "/mnt/cephfs_",
                    "/mnt/fuse",
                    "/mnt/kernel",
                    "/mnt/nfs",
                ]:
                    if cephfs_common_utils.client_mount_cleanup(
                        client_tmp, mount_path_prefix=mnt_prefix
                    ):
                        log.error("Client mount cleanup didn't suceed")
                        fs_util.reboot_node(client_tmp)
        if not fs_util.get_fs_info(client1):
            fs_util.create_fs(client1, "cephfs_1")
        fs_util.auth_list([client1])
        nfs_name = "cephfs-nfs"
        config = kw.get("config", {})
        build = config.get("build") if config else None
        if not build and config:
            build = config.get("rhbuild")
        # fs_util.prepare_clients(clients, build)
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        fs_util.create_nfs(
            client1,
            nfs_cluster_name=nfs_name,
            nfs_server_name=nfs_server,
        )
        install_tools(client1)
        mount_subvolumes(ceph_cluster)
        if fscrypt_test:
            fscrypt_status = True
            if add_fscrypt_config(client1, ceph_cluster):
                log.error("FScrypt config create failed")
                fscrypt_status = False
        with parallel() as p:
            for i in range(len(io_list)):
                p.spawn(fs_util.run_ios(client1, f"{io_list[i]}/", ["dd"]))
        """
        ceph config set mds debug_mds 20
        ceph config set mds debug_ms 1
        """

        workflow1(ceph_cluster)
        workflow2(ceph_cluster)
        workflow3(ceph_cluster)
        workflow4(ceph_cluster)
        if fscrypt_test:
            if fscrypt_status:
                mnt_pt = fscrypt_params["mountpoint"]
                encrypt_path = fscrypt_params["encrypt_path"]
                encrypt_params = fscrypt_params["encrypt_params"]
                if fscrypt_util.validate_fscrypt_with_lock_unlock(
                    client1, mnt_pt, encrypt_path, encrypt_params
                ):
                    log.error(
                        "FAIL:FScrypt config validation after Vol rename scenarios Failed"
                    )
                    return 1
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        new_fs_name = "cephfs_multi"
        client1.exec_command(
            sudo=True,
            cmd="ceph config set mon mon_allow_pool_delete true",
            check_ec=False,
        )
        log.info("####cleanup####")
        for mnt_pt in io_list:
            client1.exec_command(sudo=True, cmd=f"umount -l {mnt_pt}", check_ec=False)
        client1.exec_command(
            sudo=True, cmd="ceph nfs cluster delete cephfs-nfs", check_ec=False
        )
        fs_util.remove_fs(client1, new_fs_name, check_ec=False)
        # clean up data pool
        client1.exec_command(
            sudo=True,
            cmd="ceph osd pool rm %s %s --yes-i-really-really-mean-it"
            % (pool_name, pool_name),
            check_ec=False,
        )
        fs_util.remove_snapshot(
            client1,
            fs_name,
            "subvol_4",
            "snap_4",
            group_name="subvol_group_name_1",
            check_ec=False,
        )
        fs_util.remove_snapshot(
            client1,
            fs_name,
            "subvol_5",
            "snap_5",
            group_name="subvol_group_name_1",
            check_ec=False,
        )
        fs_util.remove_snapshot(
            client1,
            fs_name,
            "subvol_6",
            "snap_6",
            group_name="subvol_group_name_1",
            check_ec=False,
        )
        fs_util.remove_fs(client1, fs_name, check_ec=False)
        for export in export_list:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export delete {nfs_name} {export}",
                check_ec=False,
            )
        client1.exec_command(
            sudo=True,
            cmd="ceph nfs export delete {}".format(nfs_name),
            check_ec=False,
        )


def set_balance_automate_and_validate(client, fs_name="cephfs"):
    """
    Validates and manipulates the balance_automate flag state for a CephFS filesystem.
    This function checks the default state, toggles the value, and verifies the changes.

    Args:
        client: Ceph client node used to execute commands
        fs_name: Name of the CephFS filesystem. Defaults to "cephfs"

    Raises:
        CommandFailed: If any of the following conditions occur:
            - Default balance_automate value is not False
            - Unable to set balance_automate to True
            - Unable to set balance_automate back to False (default)

    Returns:
        None
    """
    log.info(
        "Validate if the Balance Automate is set to False by default and reset after toggling the values"
    )
    out, rc = client.exec_command(sudo=True, cmd=f"ceph fs get {fs_name} -f json")
    json_data = json.loads(out)
    default_balance_automate_value = json_data["mdsmap"]["flags_state"][
        "balance_automate"
    ]

    if str(default_balance_automate_value).lower() != "false":
        log.error(
            "balance automate values is expected to be disabled by default.\n "
            "but the value is %s",
            default_balance_automate_value,
        )
        raise CommandFailed("Set Balance Automate value is not set to default value")

    log.info("Set Balance Automate to True and Validate")
    client.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} balance_automate true")
    out, rc = client.exec_command(sudo=True, cmd=f"ceph fs get {fs_name} -f json")
    json_data = json.loads(out)
    balance_automate = json_data["mdsmap"]["flags_state"]["balance_automate"]

    if str(balance_automate).lower() != "true":
        log.error(
            "balance automate values is unable to set to True.\n "
            "and the current value is %s",
            balance_automate,
        )
        raise CommandFailed("Unable to set Balance Automate value to True")

    log.info("Set Balance Automate back to Default Value")
    client.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} balance_automate false")

    out, rc = client.exec_command(sudo=True, cmd=f"ceph fs get {fs_name} -f json")
    json_data = json.loads(out)
    balance_automate = json_data["mdsmap"]["flags_state"]["balance_automate"]
    if str(balance_automate).lower() != "false":
        log.error(
            "balance automate values is unable to set to default value.\n "
            "but the value is %s",
            balance_automate,
        )
        raise CommandFailed("Unable to set Balance Automate value to default value")


def validate_renamed_volume(client, renamed):
    """
    Validates that a renamed CephFS volume appears correctly in the output of various Ceph commands.

    Args:
        client (ceph.ceph.CephNode): Ceph client node
        renamed (str): New volume name to validate

    Raises:
        Exception: If the volume is not found in the output of any of these commands:
            - ceph fs ls
            - ceph fs status
            - ceph fs dump
            - ceph fs volume info
            - cephfs-top --dumpfs

    Returns:
        None
    """
    log.info("####ceph fs ls#####")
    out, ec = client.exec_command(sudo=True, cmd="ceph fs ls")
    if renamed not in out:
        log.error("%s is not present" % renamed)
        raise Exception("%s is not present" % renamed)
    log.info("####ceph fs status#####")
    out, ec = client.exec_command(sudo=True, cmd=f"ceph fs status {renamed}")
    if renamed not in out:
        log.error("%s is not ceph fs status" % renamed)
        raise Exception("%s is not status" % renamed)
    log.info("####ceph fs dump#####")
    out, ec = client.exec_command(sudo=True, cmd="ceph fs dump")
    if renamed not in out:
        log.error("%s is not present in ceph fs dump" % renamed)
        raise Exception("%s is not present in ceph fs dump" % renamed)
    log.info("####ceph fs volume info#####")
    out, ec = client.exec_command(sudo=True, cmd="ceph fs volume info %s" % renamed)
    if not out:
        log.error("%s is not present in ceph fs volume info" % renamed)
        raise Exception("%s is not present in ceph fs volume info" % renamed)
    log.info("####cephfs-top --dumpfs#####")
    out, ec = client.exec_command(sudo=True, cmd="cephfs-top --dumpfs %s" % renamed)
    if renamed not in out:
        log.error("%s is not present in cephfs-top --dumpfs" % renamed)
        raise Exception("%s is not present in cephfs-top --dumpfs" % renamed)
