import json
import random
import string
import threading
import time
import traceback
from typing import Any, Dict

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.lib.cephfs_recovery_lib import FSRecovery
from utility.log import Log

log = Log(__name__)

# 200MB MDS Log costs 1.5hrs journal tool runtime,plan accordingly for higher limits
# 3mins of metadata ops fills 200MB MDS Log
# Plan accordingly for higher limits
LOAD_RUNTIME_SECS = 300
JOURNAL_TOOL_WAIT_SECS = 2 * 60 * 60
RUNTIME_SECS = LOAD_RUNTIME_SECS + JOURNAL_TOOL_WAIT_SECS + 3600
MDS_LOG_LIMIT_GB = 0.2
MOUNT_BLOCKED_CONFIRM_SECS = 60
MOUNT_CHECK_TIMEOUT_SECS = 5
MONITOR_LOOP_INTERVAL_SECS = 30

MDS_CONFIG_KEYS = (
    "mds_log_max_segments",
    "mds_cache_memory_limit",
    # "mds_log_trim_upkeep_interval",
)

MDS_CONFIG_TEST_VALUES = {
    "mds_log_max_segments": "100000",
    "mds_cache_memory_limit": "34359738368",
    # "mds_log_trim_upkeep_interval": "86400",
}

NUM_METADATA_WRAPPERS = 100
NUM_METADATA_ITER = 100000


def metadata_ops(client, mount_dir, end_time):
    metadata_ops_script = """#!/usr/bin/env python3\n
import argparse\n
import os\n
import subprocess\n
import time\n
from concurrent.futures import ThreadPoolExecutor, as_completed\n

RUNTIME_SECS = 8 * 60 * 60\n

NUM_METADATA_WRAPPERS = 100\n
NUM_METADATA_ITER = 100000\n

def _run_cmd(cmd: str) -> int:\n
    return os.system(cmd)\n

def metadata_ops(mount_dir: str, end_time: float) -> None:\n
    A200 = "A" * 200\n
    X400 = "X" * 400\n

    def worker(w_index: int) -> None:\n
        d = f"{mount_dir}/w{w_index}"\n
        a_dir = f"{d}/a"\n
        b_dir = f"{d}/b"\n
        c_dir = f"{d}/c"\n

        _run_cmd(f"mkdir -p {a_dir} {b_dir} {c_dir}")\n

        for i in range(NUM_METADATA_ITER):\n
            a_path = f"{a_dir}/{A200}_f{i}"\n
            b_link = f"{b_dir}/{A200}_l{i}"\n
            c_r = f"{c_dir}/{A200}_r{i}"\n
            c_h = f"{c_dir}/{A200}_h{i}"\n
            b_m = f"{b_dir}/{A200}_m{i}"\n

            if subprocess.call(["test", "-f", a_path]) != 0:\n
                _run_cmd(f"touch {a_path}")\n

            _run_cmd(f"ln {a_path} {b_link}")\n
            _run_cmd(f"mv {b_link} {c_r}")\n
            _run_cmd(f"ln {a_path} {c_h}")\n
            _run_cmd(f"mv {a_path} {b_m}")\n

            _run_cmd(f"setfattr -n user.chaos -v {X400!r} {b_m}")\n

    def clean_w_dirs() -> None:\n
        _run_cmd(f"rm -rf {mount_dir}/w*")\n
    clean_w_dirs()\n
    while time.time() < end_time:\n
        with ThreadPoolExecutor(max_workers=NUM_METADATA_WRAPPERS) as executor:\n
            futures = [\n
                executor.submit(worker, idx)\n
                for idx in range(NUM_METADATA_WRAPPERS)\n
            ]\n
            for future in as_completed(futures):\n
                future.result()\n

        if time.time() >= end_time:\n
            break\n
        clean_w_dirs()\n

    print("Metadata ops completed")\n


def parse_args() -> argparse.Namespace:\n
    parser = argparse.ArgumentParser(\n
        description="Run CephFS metadata chaos ops until end_time."\n
    )\n
    parser.add_argument(\n
        "--mount-path",\n
        required=True,\n
        help="Fuse/kernel mount path to run metadata ops on.",\n
    )\n
    time_group = parser.add_mutually_exclusive_group(required=True)\n
    time_group.add_argument(\n
        "--end-time",\n
        type=float,\n
        help="Unix epoch timestamp when metadata ops should stop.",\n
    )\n
    return parser.parse_args()\n

args = parse_args()\n
mount_path = args.mount_path.rstrip("/")\n
end_time = args.end_time\n

metadata_ops(mount_path, end_time)\n
    """
    # copy metadata_ops_script to /home/cephuser/metadata_ops.py on client node and
    # run with input params as mount_dir and end_time
    cmd = f"echo -e '{metadata_ops_script}' > /home/cephuser/metadata_ops.py"
    client.exec_command(sudo=True, cmd=cmd)
    cmd = "chmod +x /home/cephuser/metadata_ops.py"
    client.exec_command(sudo=True, cmd=cmd)
    cmd = f"python3 /home/cephuser/metadata_ops.py --mount-path {mount_dir} --end-time {end_time}"
    client.exec_command(
        sudo=True, cmd=cmd, long_running=True, timeout=LOAD_RUNTIME_SECS
    )
    log.info("Metadata ops completed")


def _get_mds_config_defaults(client) -> Dict[str, str]:
    defaults = {}
    for key in MDS_CONFIG_KEYS:
        out, _ = client.exec_command(sudo=True, cmd=f"ceph config get mds {key}")
        defaults[key] = out.strip()
        log.info("Default %s=%s", key, defaults[key])
    return defaults


def _set_mds_configs(client, values: Dict[str, str]):
    for key, value in values.items():
        fs_util_cmd = f"ceph config set mds {key} {value}"
        client.exec_command(sudo=True, cmd=fs_util_cmd)
        out, _ = client.exec_command(sudo=True, cmd=f"ceph config get mds {key}")
        if out.strip() != str(value):
            raise CommandFailed(
                f"Failed to set mds {key} to {value}, got {out.strip()}"
            )


def _restore_mds_configs(client, defaults: Dict[str, str]):
    for key, value in defaults.items():
        client.exec_command(
            sudo=True, cmd=f"ceph config set mds {key} {value}", check_ec=False
        )


def _add_ceph_common_package(journal_node):
    cmd = "rpm -q ceph-common"
    out, _ = journal_node.exec_command(sudo=True, cmd=cmd)
    if "ceph-common" in out:
        cmd = "yum upgrade -y --nogpgcheck ceph-common"
        journal_node.exec_command(sudo=True, cmd=cmd)
    else:
        cmd = "yum install -y --nogpgcheck ceph-common"
        journal_node.exec_command(sudo=True, cmd=cmd)
    # copy /etc/ceph contents from installer node to journal node
    cmd = f"scp -r /etc/ceph/* {journal_node.hostname}:/etc/ceph/"
    installer_node.exec_command(sudo=True, cmd=cmd)
    return True


def _get_mds_log_wrpos(client, mds_name: str) -> int:
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"ceph tell mds.{mds_name} counter dump -f json",
        check_ec=False,
    )
    if not (out or "").strip():
        log.warning("Empty counter dump from MDS %s", mds_name)
        return False
    try:
        dump = json.loads(out)
    except json.JSONDecodeError as ex:
        log.error("Failed to parse counter dump from MDS %s: %s", mds_name, ex)
        return False
    mds_log = dump.get("mds_log", {})[0]
    if not mds_log:
        log.warning("No mds_log counters in dump from MDS %s", mds_name)
        return False
    wrpos = int(mds_log["counters"].get("wrpos", 0))
    return wrpos


def _mds_log_limit_exceeded(client, fs_name: str, mds_log_limit: int) -> bool:
    """Return True if any active MDS mds_log wrpos exceeds mds_log_max_segments."""

    log.info("MDS log wrpos limit : %s", mds_log_limit)
    active_mdss = FsUtils.get_active_mdss(client, fs_name)
    for mds_name in active_mdss:
        wrpos = _get_mds_log_wrpos(client, mds_name)
        if not wrpos:
            log.error("Failed to get mds_log wrpos for MDS %s", mds_name)
            return False
        log.info("MDS %s mds_log wrpos=%s limit=%s", mds_name, wrpos, mds_log_limit)
        if wrpos > mds_log_limit:
            log.info(
                "MDS %s wrpos %s exceeded limit %s",
                mds_name,
                wrpos,
                mds_log_limit,
            )
            return True
    return False


def _fail_active_mdss(client, fs_name: str):
    active_mds = FsUtils.get_active_mdss(client, fs_name=fs_name)
    log.info("Active MDSs: %s", active_mds)
    for mds in active_mds:
        log.info("Failing active MDS %s due to trimming lag", mds)
        try:
            client.exec_command(cmd=f"ceph mds fail {mds}")
        except Exception as ex:
            log.info(ex)

    log.info("Exiting _fail_active_mdss")


def _is_mount_accessible(client, mount_dir: str) -> bool:
    try:
        client.exec_command(
            sudo=True,
            cmd=f"timeout {MOUNT_CHECK_TIMEOUT_SECS} ls {mount_dir} >/dev/null 2>&1",
        )
        return True
    except CommandFailed:
        return False


def _journal_tool(
    client, fs_name: str, journal_tool_wait_secs: int, mds_rank: int, args: list
):
    cmd = f"cephfs-journal-tool --rank {fs_name}:{mds_rank} {' '.join(args)}"
    out = client.exec_command(
        sudo=True, cmd=cmd, long_running=True, timeout=journal_tool_wait_secs
    )
    if out:
        log.info("Journal tool output: %s", out)
    return True


def _wait_for_active_fs(client, fs_name: str, timeout: int = 600):
    end_time = time.time() + timeout
    while time.time() < end_time:
        out, _ = client.exec_command(
            sudo=True, cmd=f"ceph fs status {fs_name} -f json", check_ec=False
        )
        try:
            parsed = json.loads(out)
        except json.JSONDecodeError:
            time.sleep(10)
            continue
        active = [
            mds for mds in parsed.get("mdsmap", []) if mds.get("state") == "active"
        ]
        if active:
            log.info("Filesystem %s has %s active MDS rank(s)", fs_name, len(active))
            return
        time.sleep(10)
    raise RuntimeError(f"Filesystem {fs_name} did not become active within {timeout}s")


def _monitor_journal_tool(client, fs_name, journal_tool_wait_secs):
    log.info("Monitoring journal tool for %s on node: %s", fs_name, client)

    # Run below cmds on MDS node in loop untill journal tool command completes
    def get_journal_tool_pid():
        cmd = "ps -aef|grep cephfs-journal-tool"
        out, _ = client.exec_command(sudo=True, cmd=cmd)
        log.info(out)
        pid_out = out.split("\n")
        if len(pid_out) > 3:
            for pid_str in pid_out:
                if "event" in pid_str or "reset" in pid_str:
                    pid = pid_str.split()[1]
                    return pid
        return False

    # time.sleep(30)
    pid_out = get_journal_tool_pid()
    if not pid_out:
        log.error("Failed to get journal tool process id")
        return False
    pid = pid_out.strip()
    end_time = time.time() + journal_tool_wait_secs

    while pid and end_time > time.time():
        cmd = f"ps -p {pid} -o rss= >> /home/cephuser/track_journal_tool_rss.log"
        client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        time.sleep(60)
        pid = get_journal_tool_pid()
        log.info("PID: %s", pid)

    return True


def _verify_journal_tool_rss(client):
    # Verify journal tool RSS is less than 24GB
    cmd = "cat /home/cephuser/track_journal_tool_rss.log"
    out, _ = client.exec_command(sudo=True, cmd=cmd)
    lines = out.splitlines()
    # Get the max rss
    max_rss = max(int(line.strip()) for line in lines)
    if max_rss > 24 * 1024 * 1024:
        log.error("Journal tool RSS is greater than 24GB: %s KB", max_rss)
        return False
    log.info("Journal tool Max RSS: %s KB", max_rss)
    return True


def _get_file_and_dir_count(client, mount_dir):
    cmd = f"find {mount_dir} -type f | wc -l"
    out, _ = client.exec_command(sudo=True, cmd=cmd)
    file_count = int(out.strip())
    cmd = f"find {mount_dir} -type d | wc -l"
    out, _ = client.exec_command(sudo=True, cmd=cmd)
    dir_count = int(out.strip())
    return file_count, dir_count


def _run_recovery_workflow(
    client,
    fs_util: FsUtils,
    fs_recovery: FSRecovery,
    fs_name: str,
    journal_node: str,
    journal_tool_wait_secs: int,
    sv_data: Dict[str, Dict[str, Any]],
):
    def get_metadata_ops_pid(mount_dir, mnt_client):
        cmd = "ps -aef|grep metadata_ops.py"
        out, _ = mnt_client.exec_command(sudo=True, cmd=cmd)
        log.info(out)
        pid_out = out.split("\n")
        log.info("PID out: %s", pid_out)

        for pid_str in pid_out:
            if mount_dir in pid_str:
                pid = pid_str.split()[1]
                return pid
        return False

    mds_rank = 0
    # umount all subvolumes
    for subvol_name in sv_data:
        mnt_dir = sv_data[subvol_name]["fuse_mount_dir"]
        mnt_dir = mnt_dir.rstrip("/")
        mnt_client = sv_data[subvol_name]["client"]
        pid = get_metadata_ops_pid(mnt_dir, mnt_client)
        if pid:
            mnt_client.exec_command(sudo=True, cmd=f"kill -9 {pid}")
            log.info("Killed metadata_ops.py process with pid: %s", pid)
        else:
            log.info("No metadata_ops.py process found for %s", mnt_dir)

        mnt_client.exec_command(sudo=True, cmd=f"umount -l {mnt_dir}")
    active_mdss = FsUtils.get_active_mdss(client, fs_name)
    for mds_name in active_mdss:
        wrpos = _get_mds_log_wrpos(client, mds_name)
        log.info(
            "MDS %s mds_log wrpos=%s",
            mds_name,
            wrpos,
        )
    client.exec_command(sudo=True, cmd=f"ceph fs fail {fs_name}")
    cmd = "rm -f /home/cephuser/track_journal_tool_rss.log;touch /home/cephuser/track_journal_tool_rss.log"
    try:
        journal_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    except Exception as ex:
        log.info(ex)
    # start monitoring journal tool MEM usage in background thread

    journal_thread = threading.Thread(
        target=_monitor_journal_tool,
        args=(journal_node, fs_name, journal_tool_wait_secs),
    )
    journal_thread.start()
    # use another arg 'max-rss <allowed_mem_size_in_bytes>' to limit journal tool MEM usage,once arg is available
    # allowed_mem_size_in_bytes should not be greater than 70% of total memory in journal node
    _journal_tool(
        journal_node,
        fs_name,
        journal_tool_wait_secs,
        mds_rank,
        ["event", "recover_dentries", "summary"],
    )
    _journal_tool(
        journal_node,
        fs_name,
        journal_tool_wait_secs,
        mds_rank,
        ["journal", "reset", "--yes-i-really-really-mean-it"],
    )
    # End monitoring journal tool
    journal_thread.join(timeout=120)
    if not _verify_journal_tool_rss(journal_node):
        log.error("Journal tool RSS is greater than 24GB")
        return False
    log.info("Start data scan for filesystem %s", fs_name)
    fs_recovery.data_scan(client, fs_name=fs_name)

    fs_util.config_set_runtime(client, "mds", "mds_verify_scatter", "false")
    fs_util.config_set_runtime(client, "mds", "mds_debug_scatterstat", "false")

    client.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} joinable true")
    client.exec_command(sudo=True, cmd=f"ceph mds repaired {fs_name}:{mds_rank}")
    try:
        _wait_for_active_fs(client, fs_name)
    except Exception as ex:
        log.error("Failed to wait for active FS: %s", ex)
        return False
    fs_util.wait_for_mds_process(client, fs_name, timeout=600)
    # get active mds list
    active_mds_list = FsUtils.get_active_mdss(client, fs_name)
    # Run FS scrub
    fs_recovery.scrub_ops(client, active_mds_list)
    try:
        _wait_for_active_fs(client, fs_name)
    except Exception as ex:
        log.error("Failed to wait for active FS: %s", ex)
        return False
    for subvol_name in sv_data:
        fs_util.fuse_mount(
            [sv_data[subvol_name]["client"]],
            sv_data[subvol_name]["fuse_mount_dir"],
            extra_params=sv_data[subvol_name]["extra_mount_params"],
        )
        if not _is_mount_accessible(
            sv_data[subvol_name]["client"], sv_data[subvol_name]["fuse_mount_dir"]
        ):
            log.error(
                "Mount %s is not accessible after recovery",
                sv_data[subvol_name]["fuse_mount_dir"],
            )
            return False

    # get file and dir count  on fuse mount directory
    for subvol_name in sv_data:
        file_count, dir_count = _get_file_and_dir_count(
            sv_data[subvol_name]["client"], sv_data[subvol_name]["fuse_mount_dir"]
        )
        log.info(
            "After recovery - %s: File count: %s, Dir count: %s",
            subvol_name,
            file_count,
            dir_count,
        )
        log.info(
            "Before recovery - %s: File count: %s, Dir count: %s",
            subvol_name,
            sv_data[subvol_name]["before_file_count"],
            sv_data[subvol_name]["before_dir_count"],
        )
        if (
            file_count < sv_data[subvol_name]["before_file_count"]
            or dir_count < sv_data[subvol_name]["before_dir_count"]
        ):
            log.error(
                "File or Dir count after recovery is less than before recovery for %s",
                sv_data[subvol_name]["fuse_mount_dir"],
            )
            return False
    return True


def _monitor_loop(
    client,
    fs_util: FsUtils,
    fs_recovery: FSRecovery,
    fs_name: str,
    journal_node: str,
    end_time: float,
    journal_tool_wait_secs: int,
    mds_log_limit: int,
    sv_data: Dict[str, Dict[str, Any]],
):
    try:
        while time.time() < end_time:
            if _mds_log_limit_exceeded(client, fs_name, mds_log_limit):
                log.info(
                    "Detected 'mds log limit exceeded' given limit: %s", mds_log_limit
                )
                # get file and dir count  on fuse mount directory
                for subvol_name in sv_data:
                    file_count, dir_count = _get_file_and_dir_count(
                        sv_data[subvol_name]["client"],
                        sv_data[subvol_name]["fuse_mount_dir"],
                    )
                    log.info(
                        "Before recovery - %s: File count: %s, Dir count: %s",
                        subvol_name,
                        file_count,
                        dir_count,
                    )
                    sv_data[subvol_name]["before_file_count"] = file_count
                    sv_data[subvol_name]["before_dir_count"] = dir_count
                _fail_active_mdss(client, fs_name)
                if not _run_recovery_workflow(
                    client,
                    fs_util,
                    fs_recovery,
                    fs_name,
                    journal_node,
                    journal_tool_wait_secs,
                    sv_data,
                ):
                    log.error("Failed to run recovery workflow")
                    return False
                else:
                    log.info("Recovery workflow completed successfully")
                    return True

            time.sleep(MONITOR_LOOP_INTERVAL_SECS)
        if time.time() > end_time:
            for subvol_name in sv_data:
                file_count, dir_count = _get_file_and_dir_count(
                    sv_data[subvol_name]["client"],
                    sv_data[subvol_name]["fuse_mount_dir"],
                )
                log.info(
                    "Before recovery - %s: File count: %s, Dir count: %s",
                    subvol_name,
                    file_count,
                    dir_count,
                )
                sv_data[subvol_name]["before_file_count"] = file_count
                sv_data[subvol_name]["before_dir_count"] = dir_count
            _fail_active_mdss(client, fs_name)
            if not _run_recovery_workflow(
                client,
                fs_util,
                fs_recovery,
                fs_name,
                journal_node,
                journal_tool_wait_secs,
                sv_data,
            ):
                log.error("Failed to run recovery workflow")
                return False
            else:
                log.info("Recovery workflow completed successfully")
                return True
    except Exception as ex:
        log.error("Monitor loop failed: %s", ex)
        log.error(traceback.format_exc())


def run(ceph_cluster, **kw):
    """
    CEPH-83632603 : MDS log trim stress with parallel metadata ops and trimming-lag recovery.

    Test steps:
    1. Save default values for mds_log_max_segments, mds_cache_memory_limit and
       mds_log_trim_upkeep_interval.
    2. Set mds_log_max_segments=100000, mds_cache_memory_limit=34359738368 and
       mds_log_trim_upkeep_interval=86400.
       Set FS joinable as false
    3. Create a subvolume and fuse-mount it at the subvolume path.
    4. In parallel for up to given runtime:
       - Run metadata chaos ops on the fuse-mounted subvolume.
       - Monitor journal tool RSS and MDS log wrpos, if MDS log wrpos exceeds mds_log_limit,
         run journal recover_dentries/reset, disable scatter debug configs, set FS joinable,
         remount and verify access; otherwise continue monitoring.
    """
    fs_util = FsUtils(ceph_cluster, test_data=kw.get("test_data"))
    common_utils = CephFSCommonUtils(ceph_cluster)
    fs_recovery = FSRecovery(ceph_cluster)
    global installer_node
    installer_node = ceph_cluster.get_nodes(role="installer")[0]
    config = kw.get("config", {})
    build = config.get("build", config.get("rhbuild"))
    mds_nodes = ceph_cluster.get_ceph_objects("mds")
    ceph_nodes = ceph_cluster.get_nodes()
    clients = ceph_cluster.get_ceph_objects("client")
    if not clients:
        log.error("This test requires at least one client node")
        return 1
    cluster_nodes = []
    for ceph_node in ceph_nodes:
        if ceph_node.hostname not in [client.node.hostname for client in clients]:
            cluster_nodes.append(ceph_node)
    journal_node = cluster_nodes[-1]
    log.info("Journal node: %s", journal_node.hostname)
    client = clients[0]
    fs_name = config.get("fs_name", "cephfs_log_trim")

    mount_suffix = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(8)
    )
    fuse_mount_dir = f"/mnt/cephfs_fuse_{mount_suffix}"
    mds_log_limit = config.get("mds_log_limit_gb", MDS_LOG_LIMIT_GB)
    load_runtime_secs = int(config.get("load_runtime_secs", LOAD_RUNTIME_SECS))
    journal_tool_wait_secs = config.get(
        "journal_tool_wait_secs", JOURNAL_TOOL_WAIT_SECS
    )
    # runtime_secs = load_runtime_secs + journal_tool_wait_secs
    runtime_secs = int(config.get("runtime_secs", RUNTIME_SECS))
    subvol_group = config.get("subvol_group", "mds_log_trim_grp")
    subvol_prefix = config.get("subvol_prefix", "mds_log_trim_sv")
    subvol_cnt = config.get("subvol_cnt", 6)
    subvol_size = config.get("subvol_size", "53687091200")

    mds_log_limit = int(mds_log_limit * 1024 * 1024 * 1024)
    config_defaults: Dict[str, str] = {}
    fs_created = False
    subvol_group_created = False
    subvol_created = False
    subvol_path = None
    extra_mount_params = None

    try:
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        if common_utils.wait_for_healthy_ceph(client, wait_time=120):
            log.error("Cluster health is not OK before starting the test")
            return 1

        if not fs_util.get_fs_info(client, fs_name):
            fs_util.create_fs(client, fs_name)
            fs_created = True
            fs_util.wait_for_mds_process(client, fs_name)

        config_defaults = _get_mds_config_defaults(client)
        _set_mds_configs(client, MDS_CONFIG_TEST_VALUES)
        client.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} joinable false")
        cmd = f"ceph fs status {fs_name} -f json"
        out, _ = client.exec_command(sudo=True, cmd=cmd)
        parsed = json.loads(out)
        log.info("Parsed: %s", parsed)
        mds_name = [
            mds["name"]
            for mds in parsed["mdsmap"]
            if "active" in mds["state"] and mds["rank"] == 0
        ]
        for mds_node in mds_nodes:
            if mds_node.node.hostname in mds_name:
                mds_node = mds_node.node.hostname
                break
        log.info("mds_node: %s", mds_node)

        fs_util.create_subvolumegroup(
            client, vol_name=fs_name, group_name=subvol_group, validate=True
        )
        subvol_group_created = True
        for i in range(subvol_cnt):
            subvol_name = f"{subvol_prefix}_{i}"
            fs_util.create_subvolume(
                client,
                vol_name=fs_name,
                subvol_name=subvol_name,
                group_name=subvol_group,
                size=subvol_size,
            )
        subvol_created = True
        sv_data = {}
        for i in range(subvol_cnt):
            subvol_name = f"{subvol_prefix}_{i}"
            client1 = random.choice(clients)
            out, _ = client.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {fs_name} {subvol_name} {subvol_group}",
            )
            subvol_path = out.strip()
            log.info("Subvolume path: %s", subvol_path)
            extra_mount_params = config.get(
                "extra_mount_params", f" -r {subvol_path} --client_fs {fs_name}"
            )
            fuse_mount_dir_tmp = f"{fuse_mount_dir}_{i}/"
            fs_util.fuse_mount(
                [client1], fuse_mount_dir_tmp, extra_params=extra_mount_params
            )
            sv_data.update(
                {
                    subvol_name: {
                        "extra_mount_params": extra_mount_params,
                        "client": client1,
                        "fuse_mount_dir": fuse_mount_dir_tmp,
                    }
                }
            )
            if not _is_mount_accessible(client1, fuse_mount_dir_tmp):
                raise RuntimeError(
                    f"Initial mount verification failed for {fuse_mount_dir_tmp}"
                )
        log.info("SV data: %s", sv_data)

        # Check if ceph-common is installed on journal node
        _add_ceph_common_package(journal_node)

        end_time = time.time() + load_runtime_secs
        metadata_threads = []
        for i in range(subvol_cnt):
            subvol_name = f"{subvol_prefix}_{i}"
            metadata_thread = threading.Thread(
                target=metadata_ops,
                args=(
                    sv_data[subvol_name]["client"],
                    sv_data[subvol_name]["fuse_mount_dir"].rstrip("/"),
                    end_time,
                ),
                daemon=True,
            )
            metadata_thread.start()
            metadata_threads.append(metadata_thread)
        end_time1 = time.time() + runtime_secs
        monitor_thread = threading.Thread(
            target=_monitor_loop,
            args=(
                client,
                fs_util,
                fs_recovery,
                fs_name,
                journal_node,
                end_time1,
                journal_tool_wait_secs,
                mds_log_limit,
                sv_data,
            ),
            daemon=True,
        )
        monitor_thread.start()

        end_time2 = time.time() + runtime_secs + 3600
        while time.time() < end_time2:
            completed_threads = 0
            for metadata_thread in metadata_threads:
                if metadata_thread.is_alive():
                    time.sleep(MONITOR_LOOP_INTERVAL_SECS)
                else:
                    completed_threads += 1
            if completed_threads == len(metadata_threads):
                break
        while time.time() < end_time2:
            if monitor_thread.is_alive():
                time.sleep(MONITOR_LOOP_INTERVAL_SECS)
            else:
                break

        log.info("Metadata ops and monitor threads completed")
        for metadata_thread in metadata_threads:
            if metadata_thread.is_alive():
                metadata_thread.join(timeout=120)
        if monitor_thread.is_alive():
            monitor_thread.join(timeout=120)
        return 0

    except Exception as ex:
        log.error(ex)
        log.error(traceback.format_exc())
        return 1

    finally:

        if monitor_thread and monitor_thread.is_alive():
            monitor_thread.join(timeout=30)
        if metadata_thread and metadata_thread.is_alive():
            metadata_thread.join(timeout=30)
        if config_defaults:
            log.info("Restoring MDS config defaults")
            _restore_mds_configs(client, config_defaults)
        client.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} joinable true")
        client.exec_command(sudo=True, cmd=f"ceph mds repaired {fs_name}:0")
        try:
            _wait_for_active_fs(client, fs_name)
        except Exception as ex:
            log.error("Failed to wait for active FS: %s", ex)
        if sv_data:
            if subvol_created:
                for subvol_name in sv_data:
                    mnt_dir = sv_data[subvol_name]["fuse_mount_dir"]
                    sv_data[subvol_name]["client"].exec_command(
                        sudo=True, cmd=f"umount -l {mnt_dir}", check_ec=False
                    )
                    sv_data[subvol_name]["client"].exec_command(
                        sudo=True, cmd=f"rm -rf {mnt_dir}", check_ec=False
                    )
                    subvol = {
                        "vol_name": fs_name,
                        "subvol_name": subvol_name,
                        "group_name": subvol_group,
                    }
                    fs_util.remove_subvolume(
                        client, **subvol, validate=False, check_ec=False
                    )
        if subvol_group_created:
            fs_util.remove_subvolumegroup(
                client, fs_name, subvol_group, validate=False, check_ec=False
            )
        if fs_created:
            fs_util.remove_fs(client, fs_name, validate=False)
