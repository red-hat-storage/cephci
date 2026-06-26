import json
import random
import string
import threading
import time
import traceback
from typing import Any, Dict, List

from looseversion import LooseVersion

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.lib.cephfs_subvol_metric_utils import MDSMetricsHelper
from utility.log import Log

log = Log(__name__)


def _get_unique_mds_hosts(ceph_cluster) -> List[str]:
    hosts = []
    for mds in ceph_cluster.get_ceph_objects("mds"):
        host = mds.node.hostname
        if host not in hosts:
            hosts.append(host)
    return hosts


def _wait_for_mds_daemon_count(
    client, fs_name: str, expected_count: int, timeout: int = 240
):
    end_time = time.time() + timeout
    while time.time() < end_time:
        out, _ = client.exec_command(
            sudo=True,
            cmd="ceph orch ps --daemon_type=mds --format json",
            check_ec=False,
        )
        daemons = json.loads(out)
        running = [
            d
            for d in daemons
            if d.get("daemon_id", "").startswith(f"{fs_name}.")
            and d.get("status_desc") == "running"
        ]
        if len(running) >= expected_count:
            return 0
        time.sleep(10)
    return 1


def _validate_metrics(
    stage: str,
    helper: MDSMetricsHelper,
    fs_util: FsUtils,
    client,
    fs_name: str,
    subvol_path: str,
    fuse_mount_dir: str,
):
    log.info("Collecting metrics for stage: %s", stage)
    quota_attrs = fs_util.get_quota_attrs(client, fuse_mount_dir)
    expected_quota_bytes = int(quota_attrs.get("bytes", 0))
    subvol_metrics = helper.collect_subvolume_metrics(
        client=client,
        fs_name=fs_name,
        role="active",
        ranks=None,
        path_prefix=subvol_path,
    )
    log.info(
        "%s: expected values from CLI quota_bytes=%s",
        stage,
        expected_quota_bytes,
    )
    if not subvol_metrics:
        raise RuntimeError(f"{stage}: subvolume metrics are empty")

    found_subvol_item = False
    for mds_name, items in subvol_metrics.items():
        for item in items:
            item_subvol_path = item.get("subvolume_path", "")
            if item_subvol_path.startswith(subvol_path) or subvol_path.startswith(
                item_subvol_path
            ):
                found_subvol_item = True
                if "quota_bytes" not in item or "used_bytes" not in item:
                    raise RuntimeError(
                        f"{stage}: missing quota_bytes/used_bytes in subvolume metrics from {mds_name}"
                    )
                actual_quota_bytes = int(item.get("quota_bytes", -1))
                actual_used_bytes = int(item.get("used_bytes", -1))
                if actual_quota_bytes != expected_quota_bytes:
                    raise RuntimeError(
                        f"{stage}: quota_bytes mismatch for {mds_name} {item_subvol_path}. "
                        f"expected={expected_quota_bytes}, actual={actual_quota_bytes}"
                    )
                log.info(
                    "%s: %s %s quota_bytes=%s used_bytes=%s (validated)",
                    stage,
                    mds_name,
                    item_subvol_path,
                    actual_quota_bytes,
                    actual_used_bytes,
                )

    if not found_subvol_item:
        raise RuntimeError(
            f"{stage}: no subvolume metrics found for path prefix {subvol_path}"
        )

    mds_metrics = helper.collect_mds_metrics(
        client=client,
        fs_name=fs_name,
        role="active",
        ranks=None,
    )
    if not mds_metrics:
        raise RuntimeError(f"{stage}: mds_rank_perf metrics are empty")

    found_mds_item = False
    for mds_name, items in mds_metrics.items():
        if not isinstance(items, list):
            continue
        for item in items:
            counters = item.get("counters", {})
            if "cpu_usage" in counters and "open_requests" in counters:
                found_mds_item = True
                log.info(
                    "%s: %s rank_perf cpu_usage=%s open_requests=%s",
                    stage,
                    mds_name,
                    counters.get("cpu_usage"),
                    counters.get("open_requests"),
                )
    if not found_mds_item:
        raise RuntimeError(
            f"{stage}: required cpu_usage/open_requests fields missing in mds metrics"
        )

    log.info("Metrics validation passed for stage: %s", stage)


def _start_continuous_smallfile_io(fs_util: FsUtils, client, mount_path: str):
    stop_event = threading.Event()
    io_state: Dict[str, Any] = {"iterations": 0, "last_error": None}

    def _runner():
        while not stop_event.is_set():
            io_dir = f"{mount_path}/smallfile_io_dir_{io_state['iterations']}"
            client.exec_command(sudo=True, cmd=f"mkdir -p {io_dir}")
            try:
                fs_util.run_ios(client, io_dir, io_tools=["smallfile"])
                io_state["iterations"] += 1
            except Exception as ex:
                # Smallfile can intermittently fail around daemon disruptions; tolerate known transient.
                if "threads reached starting gate within" in str(ex):
                    log.warning("Transient smallfile issue during disruptions: %s", ex)
                    io_state["iterations"] += 1
                    continue
                io_state["last_error"] = str(ex)
                log.error("Continuous smallfile IO thread failed: %s", ex)
                break

    io_thread = threading.Thread(target=_runner, daemon=True)
    io_thread.start()
    return stop_event, io_state, io_thread


def run(ceph_cluster, **kw):
    """
    Polarion TC ID: CEPH-83632428
    Test Steps:
    1. Create FS volume, subvolumegroup and subvolume.
    2. Fuse mount the subvolume and set max_files/max_bytes quota.
    3. Start smallfile IO, run for 60 seconds, stop IO, then verify subvolume and MDS metrics
       (baseline before disruptive ops).
    4. For each disruptive op: start continuous smallfile IO, run the op while IO runs,
       stop IO, then verify subvolume metrics and MDS metrics (stable snapshots).
    5. Ops run serially:
       - Active MDS daemon restart
       - MGR daemon restart
       - MDS node reboot
       - MDS service remove and add
    """
    fs_util = FsUtils(ceph_cluster, test_data=kw.get("test_data"))
    helper = MDSMetricsHelper(ceph_cluster)
    common_utils = CephFSCommonUtils(ceph_cluster)

    config: Dict[str, Any] = kw.get("config", {})
    build = config.get("build", config.get("rhbuild"))
    clients = ceph_cluster.get_ceph_objects("client")
    if not clients:
        log.error("This test requires at least one client node")
        return 1
    build = config.get("build", config.get("rhbuild"))
    if LooseVersion(build) < LooseVersion("9.1"):
        log.info("Skipping test: requires Ceph version >= 9.1 (build=%s)", build)
        return 0
    client = clients[0]
    fs_name = config.get("fs_name", "cephfs-metrics-ops")
    subvol_group = config.get("subvol_group", "subvol_metrics_grp")
    subvol_name = config.get("subvol_name", "subvol_metrics_ops")
    quota_max_bytes = int(config.get("quota_max_bytes", 2147483648))
    quota_max_files = int(config.get("quota_max_files", 2000))
    mount_suffix = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(8)
    )
    fuse_mount_dir = f"/mnt/cephfs_fuse_{mount_suffix}/"

    fs_created = False
    subvol_group_created = False
    subvol_created = False
    stop_event = None
    io_thread = None
    io_state = {"iterations": 0, "last_error": None}

    def _check_io_health(stage: str):
        if io_state.get("last_error"):
            raise RuntimeError(
                f"{stage}: IO thread failed - {io_state.get('last_error')}"
            )
        if io_thread and not io_thread.is_alive():
            raise RuntimeError(f"{stage}: IO thread is not running")

    try:
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        if not fs_util.get_fs_info(client, fs_name):
            fs_util.create_fs(client, fs_name)
            fs_created = True
            log.info("Created filesystem %s", fs_name)

        mds_hosts = _get_unique_mds_hosts(ceph_cluster)
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        if len(mds_hosts) >= 2:
            selected_hosts = mds_hosts[: min(3, len(mds_hosts))]
            placement = " ".join(selected_hosts)
            client.exec_command(
                sudo=True,
                cmd=f"ceph orch apply mds {fs_name} --placement='{len(selected_hosts)} {placement}'",
            )
            if _wait_for_mds_daemon_count(client, fs_name, expected_count=2):
                raise RuntimeError(
                    "MDS daemons did not reach expected count after placement apply"
                )

        fs_util.create_subvolumegroup(
            client, vol_name=fs_name, group_name=subvol_group, validate=True
        )
        subvol_group_created = True

        fs_util.create_subvolume(
            client,
            vol_name=fs_name,
            subvol_name=subvol_name,
            group_name=subvol_group,
            size="5368709120",
        )
        subvol_created = True

        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} {subvol_name} {subvol_group}",
        )
        subvol_path = out.strip()
        log.info("Subvolume path: %s", subvol_path)

        fs_util.fuse_mount(
            [client],
            fuse_mount_dir,
            extra_params=f" -r {subvol_path} --client_fs {fs_name}",
        )

        fs_util.set_quota_attrs(
            client=client,
            file=quota_max_files,
            bytes=quota_max_bytes,
            directory=fuse_mount_dir,
        )
        log.info(
            "Set quota on %s max_files=%s max_bytes=%s",
            fuse_mount_dir,
            quota_max_files,
            quota_max_bytes,
        )

        log.info(
            "Starting smallfile IO for 15s before baseline metrics (before disruptive ops)"
        )
        stop_event, io_state, io_thread = _start_continuous_smallfile_io(
            fs_util, client, fuse_mount_dir.rstrip("/")
        )
        time.sleep(15)  # Wait for I/O to generate data and metrics to update
        _validate_metrics(
            stage="before disruptive ops",
            helper=helper,
            fs_util=fs_util,
            client=client,
            fs_name=fs_name,
            subvol_path=subvol_path,
            fuse_mount_dir=fuse_mount_dir,
        )

        def _restart_active_mds_daemon():
            active_mds = FsUtils.get_active_mdss(client, fs_name=fs_name)
            if not active_mds:
                raise RuntimeError("No active MDS found for restart operation")
            active_host = active_mds[0].split(".")[1]
            active_mds_node = ceph_cluster.get_node_by_hostname(active_host)
            fs_util.deamon_op(active_mds_node, rf"mds\.{fs_name}\.", "restart")
            log.info("Restarted active MDS daemon on host %s", active_host)

        def _restart_active_mgr_daemon():
            client.exec_command(sudo=True, cmd="ceph orch restart mgr")
            log.info("Restarted active MGR daemon: mgr")
            time.sleep(10)

        def _reboot_active_mds_node():
            active_mds = FsUtils.get_active_mdss(client, fs_name=fs_name)
            if not active_mds:
                raise RuntimeError("No active MDS found for node reboot operation")
            active_host = active_mds[0].split(".")[1]
            active_mds_node = None
            for mds in mds_nodes:
                log.info(
                    "MDS node: %s , active_host: %s", mds.node.hostname, active_host
                )
                if mds.node.hostname == active_host:
                    active_mds_node = mds
                    break
            if not active_mds_node:
                raise RuntimeError("Active MDS node not found for reboot operation")
            fs_util.reboot_node(ceph_node=active_mds_node)
            log.info("Rebooted active MDS node: %s", active_host)

        def _remove_add_mds_service():
            out, _ = client.exec_command(
                sudo=True, cmd="ceph orch ps --daemon_type=mds --format json"
            )
            daemons = json.loads(out)
            fs_hosts = []
            for daemon in daemons:
                daemon_id = daemon.get("daemon_id", "")
                host = daemon.get("hostname")
                if (
                    daemon_id.startswith(f"{fs_name}.")
                    and host
                    and host not in fs_hosts
                ):
                    fs_hosts.append(host)
            if len(fs_hosts) < 2:
                raise RuntimeError(
                    "Need at least 2 MDS hosts for remove/add operation, found "
                    f"{len(fs_hosts)}"
                )

            reduced_hosts = fs_hosts[:-1]
            reduced_placement = " ".join(reduced_hosts)
            full_placement = " ".join(fs_hosts)

            client.exec_command(
                sudo=True,
                cmd=f"ceph orch apply mds {fs_name} --placement='{len(reduced_hosts)} {reduced_placement}'",
            )
            if _wait_for_mds_daemon_count(
                client, fs_name, expected_count=len(reduced_hosts)
            ):
                raise RuntimeError("MDS count did not converge after remove operation")

            client.exec_command(
                sudo=True,
                cmd=f"ceph orch apply mds {fs_name} --placement='{len(fs_hosts)} {full_placement}'",
            )
            if _wait_for_mds_daemon_count(
                client, fs_name, expected_count=len(fs_hosts)
            ):
                raise RuntimeError("MDS count did not converge after add operation")

            log.info("Completed MDS service remove/add for fs %s", fs_name)

        disruptive_ops = [
            ("after active MDS daemon restart", _restart_active_mds_daemon),
            ("after MGR daemon restart", _restart_active_mgr_daemon),
            ("after MDS node reboot", _reboot_active_mds_node),
            ("after MDS service remove-add", _remove_add_mds_service),
        ]
        for stage, op in disruptive_ops:
            _check_io_health(f"{stage} pre-check")
            log.info("Checking cluster health before disruptive op: %s", stage)
            if common_utils.wait_for_healthy_ceph(client, wait_time=300):
                raise RuntimeError(f"{stage}: cluster did not become healthy in time")
            op()
            if common_utils.wait_for_healthy_ceph(client, wait_time=1200):
                raise RuntimeError(f"{stage}: cluster did not become healthy in time")
            _validate_metrics(
                stage=stage,
                helper=helper,
                fs_util=fs_util,
                client=client,
                fs_name=fs_name,
                subvol_path=subvol_path,
                fuse_mount_dir=fuse_mount_dir,
            )

        log.info("All disruptive operations completed.")
        if stop_event:
            stop_event.set()
        if io_thread and io_thread.is_alive():
            io_thread.join(timeout=90)
        stop_event, io_thread = None, None
        return 0
    except Exception as ex:
        log.error("Test failed: %s", ex)
        log.error(traceback.format_exc())
        return 1

    finally:
        try:
            if stop_event:
                stop_event.set()
            if io_thread and io_thread.is_alive():
                io_thread.join(timeout=90)
        except Exception:
            pass

        try:
            client.exec_command(
                sudo=True, cmd=f"umount -l {fuse_mount_dir}", check_ec=False
            )
            client.exec_command(
                sudo=True, cmd=f"rm -rf {fuse_mount_dir}", check_ec=False
            )
        except Exception as ex:
            log.error("Failed cleanup for fuse mount path %s: %s", fuse_mount_dir, ex)

        try:
            if subvol_created:
                fs_util.remove_subvolume(
                    client,
                    vol_name=fs_name,
                    subvol_name=subvol_name,
                    group_name=subvol_group,
                    check_ec=False,
                )
        except Exception as ex:
            log.error("Failed to remove subvolume %s: %s", subvol_name, ex)

        try:
            if subvol_group_created:
                fs_util.remove_subvolumegroup(
                    client,
                    vol_name=fs_name,
                    group_name=subvol_group,
                    force=True,
                    check_ec=False,
                )
        except Exception as ex:
            log.error("Failed to remove subvolume group %s: %s", subvol_group, ex)

        try:
            if fs_created:
                fs_util.remove_fs(client, vol_name=fs_name, check_ec=False)
        except Exception as ex:
            log.error("Failed to remove filesystem %s: %s", fs_name, ex)
