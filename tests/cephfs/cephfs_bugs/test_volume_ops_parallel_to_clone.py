import json
import random
import string
import time
import traceback
from threading import Thread

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log

log = Log(__name__)


def add_data_to_subvolume(client, mount_path, target_size_gb=1):
    """
    Add data to subvolume using truncate command pattern
    Creates files of various sizes to reach approximately target_size_gb
    """
    log.info(
        "Adding data to subvolume at %s to reach ~%sGB", mount_path, target_size_gb
    )

    # Define file sizes and suffixes
    file_sizes = ["1k", "4k", "7k", "100k", "1M", "10M", "100M", "1G"]
    suffixes = ["dat", "bin", "txt", "log"]

    # Calculate approximate number of iterations needed
    # Rough estimate: each iteration creates ~8 files of varying sizes
    # Average size per iteration ~= 1.1GB (rough estimate)
    iterations = max(5, int(target_size_gb / 1.1))
    for i in range(iterations):
        for size in file_sizes:
            file_name = f"{mount_path}/file_{i}_{size}.{suffixes[i % len(suffixes)]}"
            client.exec_command(sudo=True, cmd=f"truncate -s {size} {file_name}")
    # Verify data was created
    out, rc = client.exec_command(
        sudo=True, cmd=f"du -sh {mount_path} | awk '{{print $1}}'"
    )
    log.info("Total data size in mount path: %s", out.strip())

    return 0


def capture_operation_time(operation_name, operation_func, *args, **kwargs):
    """
    Capture time taken for an operation
    Returns: (result, time_taken_seconds)
    """
    start_time = time.time()
    try:
        result = operation_func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        log.info("[TIMING] %s completed in %s seconds", operation_name, elapsed_time)
        return (result, elapsed_time)
    except Exception as e:
        elapsed_time = time.time() - start_time
        log.error(
            "[TIMING] %s failed after %s seconds: %s",
            operation_name,
            elapsed_time,
            str(e),
        )
        return (None, elapsed_time)


def verify_clone_in_progress(client, fs_util, default_fs, clone_name, max_retries=10):
    """
    Verify that clone is in-progress state
    Returns: True if clone is in-progress, False otherwise
    """
    for attempt in range(max_retries):
        try:
            cmd_out, _ = fs_util.get_clone_status(client, default_fs, clone_name)
            status_json = json.loads(cmd_out)
            state = status_json.get("status", {}).get("state", "")
            log.info("Clone status check attempt %s: state = %s", attempt + 1, state)

            if state == "in-progress":
                log.info("Clone %s is in-progress state", clone_name)
                return True
            elif state == "complete":
                log.warning("Clone %s has already completed", clone_name)
                return False
            elif state == "pending":
                log.info("Clone %s is in pending state, waiting...", clone_name)
            else:
                log.info("Clone %s state: %s, waiting...", clone_name, state)
        except Exception as e:
            log.info(
                "Clone status check attempt %s failed: %s, retrying...",
                attempt + 1,
                str(e),
            )

    log.error(
        "Could not verify clone %s is in-progress after %s attempts",
        clone_name,
        max_retries,
    )
    return False


def wait_for_clone_complete(
    client, fs_util, default_fs, clone_name, max_wait_time=1800, check_interval=5
):
    """
    Wait for clone to complete
    Returns: True if clone completed, False if timeout
    """
    start_time = time.time()
    log.info("Waiting for clone %s to complete...", clone_name)

    while time.time() - start_time < max_wait_time:
        try:
            cmd_out, _ = fs_util.get_clone_status(client, default_fs, clone_name)
            status_json = json.loads(cmd_out)
            state = status_json.get("status", {}).get("state", "")

            if state == "complete":
                elapsed = time.time() - start_time
                log.info("Clone %s completed in %s seconds", clone_name, elapsed)
                return True
            elif state == "failed":
                log.error("Clone %s failed", clone_name)
                return False
            else:
                log.info("Clone %s state: %s, waiting...", clone_name, state)
                time.sleep(check_interval)
        except Exception as e:
            log.info("Clone status check failed: %s, retrying...", str(e))
            time.sleep(check_interval)

    log.error("Clone %s did not complete within %s seconds", clone_name, max_wait_time)
    return False


def run_parallel_volume_ops(
    client,
    fs_util,
    snap_util,
    default_fs,
    sv_obj,
    skip_clone_create="false",
    baseline_times=None,
):
    """
    Run various volume operations one at a time in parallel while clone is in progress.
    For each operation:
    1. Create and start a clone
    2. Wait for clone to be in-progress
    3. Run the operation in parallel to clone
    4. Wait for clone to complete
    5. Remove the clone
    6. Repeat for next operation
    """
    operation_times = {}
    baseline_times = baseline_times or {}
    log.info("Baseline times: %s", baseline_times)

    def op_manual_snapshot_create():
        """Manual snapshot create operation"""
        snap_name = "manual_snap_clone_test"
        snapshot = {
            "vol_name": default_fs,
            "subvol_name": sv_obj["subvol_name"],
            "snap_name": snap_name,
        }
        if sv_obj.get("group_name"):
            snapshot.update({"group_name": sv_obj["group_name"]})
        fs_util.create_snapshot(client, **snapshot, validate=False)
        return snap_name

    def op_snap_schedule_add():
        """Create snap-schedule"""
        snap_params = {
            "client": client,
            "fs_name": default_fs,
            "path": "/",
            "sched": "5m",
            "subvol_name": sv_obj["subvol_name"],
            "validate": False,
        }
        if sv_obj.get("group_name"):
            snap_params.update({"group_name": sv_obj["group_name"]})
        snap_util.create_snap_schedule(snap_params)
        return "snap_schedule_created"

    def op_snap_schedule_remove():
        """Remove snap-schedule"""
        snap_util.remove_snap_schedule(
            client,
            "/",
            fs_name=default_fs,
            subvol_name=sv_obj["subvol_name"],
            group_name=sv_obj.get("group_name"),
        )
        return "snap_schedule_removed"

    def op_other_clone_create():
        """Create another clone"""
        other_clone = {
            "vol_name": default_fs,
            "subvol_name": sv_obj["subvol_name"],
            "snap_name": sv_obj["snap_name"],
            "target_subvol_name": "parallel_clone_create_test",
        }
        if sv_obj.get("group_name"):
            other_clone.update({"group_name": sv_obj["group_name"]})
        fs_util.create_clone(client, **other_clone, validate=False)
        return other_clone["target_subvol_name"]

    def op_subvolumegroup_create():
        """Create subvolumegroup"""
        group_name = "parallel_group_create_test"
        subvolumegroup = {
            "vol_name": default_fs,
            "group_name": group_name,
        }
        fs_util.create_subvolumegroup(client, **subvolumegroup)
        return group_name

    def op_subvolume_create():
        """Create subvolume"""
        subvol_name = "parallel_sv_create_test"
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": subvol_name,
            "size": "5368706371",
        }
        fs_util.create_subvolume(client, **subvolume, validate=False)
        return subvol_name

    def op_subvolume_info():
        """Get subvolume info"""
        fs_util.get_subvolume_info(
            client,
            default_fs,
            sv_obj["subvol_name"],
            group_name=sv_obj.get("group_name"),
        )
        return "info_retrieved"

    def op_subvolume_delete():
        """Delete a subvolume (create one first, then delete)"""
        temp_sv_name = "parallel_clone_create_test"
        temp_sv = {
            "vol_name": default_fs,
            "subvol_name": temp_sv_name,
        }
        fs_util.remove_subvolume(client, **temp_sv, validate=False, check_ec=False)
        return temp_sv_name

    def op_subvolume_delete_with_retain_snapshot():
        """Delete subvolume with retain-snapshot"""
        # Delete with retain-snapshot
        cmd = f"ceph fs subvolume rm {default_fs} parallel_sv_delete_test --retain-snapshots"
        client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        return "subvolume_deleted_with_retain_snapshot"

    def op_subvolume_modify():
        """Modify subvolume (resize)"""
        new_size = "10737418240"  # 10GB
        cmd = (
            f"ceph fs subvolume resize {default_fs} {sv_obj['subvol_name']} {new_size}"
        )
        if sv_obj.get("group_name"):
            cmd += f" --group_name {sv_obj['group_name']}"
        client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        return "resize_done"

    def op_subvolume_metadata_set():
        """Subvolume metadata set operation"""
        metadata_key = f"test_key_{random.randint(1000, 9999)}"
        metadata_value = f"test_value_{random.randint(1000, 9999)}"

        # Set metadata
        cmd = f"ceph fs subvolume metadata set {default_fs} {sv_obj['subvol_name']} {metadata_key} {metadata_value}"
        if sv_obj.get("group_name"):
            cmd += f" --group_name {sv_obj['group_name']}"
        client.exec_command(sudo=True, cmd=cmd, check_ec=False)

        return "set_metadata_done"

    def op_subvolume_metadata_get():
        """Subvolume metadata get operation"""
        metadata_key = f"test_key_{random.randint(1000, 9999)}"

        # Get metadata
        cmd = f"ceph fs subvolume metadata get {default_fs} {sv_obj['subvol_name']} {metadata_key}"
        if sv_obj.get("group_name"):
            cmd += f" --group_name {sv_obj['group_name']}"
        client.exec_command(sudo=True, cmd=cmd, check_ec=False)

        return "get_metadata_done"

    def op_snapshot_metadata_set():
        """Snapshot metadata operations"""
        metadata_key = "snap_key_clone_test"
        metadata_value = f"snap_value_{random.randint(1000, 9999)}"

        # Set snapshot metadata
        metadata_dict = {metadata_key: metadata_value}
        fs_util.set_snapshot_metadata(
            client,
            default_fs,
            sv_obj["subvol_name"],
            sv_obj["snap_name"],
            metadata_dict,
            group_name=sv_obj.get("group_name"),
        )
        return "set_snapshot_metadata_done"

    def op_snapshot_metadata_get():
        """Snapshot metadata get operation"""
        metadata_key = "snap_key_clone_test"

        # Get snapshot metadata
        fs_util.get_snapshot_metadata(
            client,
            default_fs,
            sv_obj["subvol_name"],
            sv_obj["snap_name"],
            metadata_key,
            group_name=sv_obj.get("group_name"),
        )

        return "get_snapshot_metadata_done"

    def op_ceph_config_get():
        """Ceph config get operation"""
        config_key = "mgr/volumes/max_concurrent_clones"
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph config get mgr {config_key}"
        )
        return out.strip()

    def op_ceph_config_set():
        """Ceph config set operation"""
        # Set a temporary config and then restore
        config_key = "mgr/volumes/snapshot_clone_delay"
        original_value = "0"
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph config get mgr {config_key}"
        )
        original_value = out.strip()

        # Set new value
        client.exec_command(sudo=True, cmd=f"ceph config set mgr {config_key} 1")

        # Restore original value
        client.exec_command(
            sudo=True, cmd=f"ceph config set mgr {config_key} {original_value}"
        )

        return "config_set_done"

    def subvolume_snapshot_create():
        """Create a subvolume and snapshot for the subvolume"""
        subvol_name = "parallel_sv_delete_test"
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": subvol_name,
            "size": "5368706371",
        }
        fs_util.create_subvolume(client, **subvolume, validate=False)
        snapshot = {
            "vol_name": default_fs,
            "subvol_name": subvol_name,
            "snap_name": "parallel_snap_sv_delete_test",
        }
        fs_util.create_snapshot(client, **snapshot, validate=False)

    # Define all operations
    operations = [
        ("Manual Snapshot Create", op_manual_snapshot_create),
        ("Snap-Schedule Add", op_snap_schedule_add),
        ("Snap-Schedule Remove", op_snap_schedule_remove),
        ("Other Clone Create", op_other_clone_create),
        ("Subvolumegroup Create", op_subvolumegroup_create),
        ("Subvolume Create", op_subvolume_create),
        ("Subvolume Info", op_subvolume_info),
        ("Subvolume Delete", op_subvolume_delete),
        (
            "Subvolume Delete with Retain-Snapshot",
            op_subvolume_delete_with_retain_snapshot,
        ),
        ("Subvolume Modify Resize", op_subvolume_modify),
        ("Subvolume Metadata Set", op_subvolume_metadata_set),
        ("Subvolume Metadata Get", op_subvolume_metadata_get),
        ("Snapshot Metadata Set", op_snapshot_metadata_set),
        ("Snapshot Metadata Get", op_snapshot_metadata_get),
        ("Ceph Config Get", op_ceph_config_get),
        ("Ceph Config Set", op_ceph_config_set),
    ]
    log.info("Creating subvolume and snapshot in subvolume,required for the test")
    subvolume_snapshot_create()
    # Execute operations one at a time in parallel to clone
    log.info("Starting volume operations one at a time in parallel to clone...")
    log.info("Total operations to run: %s", len(operations))

    for idx, (op_name, op_func) in enumerate(operations, 1):
        log.info("=" * 80)
        log.info("Operation %s/%s: %s", idx, len(operations), op_name)
        log.info("=" * 80)

        # Create a unique clone name for this operation
        clone_name = f"clone_for_{op_name.lower().replace(' ', '_')}_{random.randint(1000, 9999)}"
        clone_obj = {
            "vol_name": default_fs,
            "subvol_name": sv_obj["subvol_name"],
            "snap_name": sv_obj["snap_name"],
            "target_subvol_name": clone_name,
        }

        # Step 1: Start clone creation in a separate thread
        clone_start_time = time.time()
        log.info("Starting clone creation: %s", clone_name)

        clone_result = {"status": None, "error": None}

        def create_clone():
            try:
                fs_util.create_clone(client, **clone_obj, validate=True)
                clone_result["status"] = "success"
            except Exception as e:
                log.error("Clone creation failed: %s", str(e))
                clone_result["status"] = "failed"
                clone_result["error"] = str(e)
                return 1

        def remove_clone():
            try:
                fs_util.remove_subvolume(
                    client,
                    vol_name=default_fs,
                    subvol_name=clone_name,
                    validate=True,
                    check_ec=False,
                )
            except Exception as e:
                log.error("Failed to remove clone %s: %s", clone_name, str(e))
                return 1

        if skip_clone_create == "false":
            clone_thread = Thread(target=create_clone)
            clone_thread.start()

            # Step 2: Wait for clone to start and verify it's in-progress
            log.info("Waiting for clone to start...")
            time.sleep(3)

            log.info(
                "Verifying clone %s is in-progress before running %s...",
                clone_name,
                op_name,
            )
            if not verify_clone_in_progress(
                client, fs_util, default_fs, clone_name, max_retries=15
            ):
                log.error(
                    "Clone %s did not enter in-progress state. Skipping %s.",
                    clone_name,
                    op_name,
                )
                operation_times[op_name] = {
                    "time_taken": 0,
                    "status": "skipped",
                    "baseline_time": baseline_times.get(op_name, "N/A"),
                }
                fs_util.clone_cancel(client, default_fs, clone_name, check_ec=False)
                log.info("Clone %s cancelled successfully", clone_name)
                # Try to clean up clone if it exists
                remove_clone()
                return 1

        # Step 3: Run this operation in parallel to clone (clone continues in background)
        log.info("Running %s in parallel to clone %s...", op_name, clone_name)
        try:
            result, elapsed_time = capture_operation_time(op_name, op_func)
            operation_times[op_name] = {
                "time_taken": elapsed_time,
                "status": "success" if result is not None else "failed",
                "baseline_time": baseline_times.get(op_name, "N/A"),
            }
            log.info("%s completed in %s seconds", op_name, elapsed_time)
        except Exception as e:
            log.error("Operation %s raised exception: %s", op_name, str(e))
            operation_times[op_name] = {
                "time_taken": 0,
                "status": "exception",
                "baseline_time": baseline_times.get(op_name, "N/A"),
            }
        if skip_clone_create == "false":
            # Step 4: Wait for clone to complete
            log.info("Waiting for clone %s to complete...", clone_name)
            clone_thread.join(timeout=1800)  # 30 minutes timeout
            wait_for_clone_complete(client, fs_util, default_fs, clone_name)
            if clone_result["status"] == "failed":
                log.error(
                    "Clone %s creation failed: %s",
                    clone_name,
                    clone_result.get("error", "Unknown error"),
                )
                return 1
            elif clone_result["status"] is None or clone_thread.is_alive():
                # Clone is still running or timed out
                log.error(
                    "Clone %s creation taking more than 30mins, cancelling and exiting test...",
                    clone_name,
                )
                # Cancel clone operation
                log.info("Cancelling clone %s operation...", clone_name)
                try:
                    fs_util.clone_cancel(client, default_fs, clone_name, check_ec=False)
                    log.info("Clone %s cancelled successfully", clone_name)
                except Exception as e:
                    log.error("Failed to cancel clone %s: %s", clone_name, str(e))
                    remove_clone()
                    return 1
                # Wait a bit for cancel to take effect
                time.sleep(2)

        log.info("Completed operation %s/%s: %s", idx, len(operations), op_name)
        if skip_clone_create == "false":
            clone_end_time = time.time()
            clone_total_time = clone_end_time - clone_start_time
            log.info("Clone %s total time: %s seconds", clone_name, clone_total_time)

            if remove_clone():
                return 1
        if "Other Clone Create" in op_name:
            wait_for_clone_complete(
                client, fs_util, default_fs, "parallel_clone_create_test"
            )
        if "Subvolume Delete" in op_name:
            fs_util.remove_subvolume(
                client,
                vol_name=default_fs,
                subvol_name="parallel_sv_create_test",
                validate=True,
                check_ec=False,
            )
        if "Subvolume Delete with Retain-Snapshot" in op_name:
            snapshot = {
                "vol_name": default_fs,
                "subvol_name": "parallel_sv_delete_test",
                "snap_name": "parallel_snap_sv_delete_test",
            }
            fs_util.remove_snapshot(client, **snapshot, validate=False, check_ec=False)
        out, rc = client.exec_command(sudo=True, cmd="ceph df")
        log.info("Ceph df output: %s", out)
        log.info("Completed operation %s/%s: %s", idx, len(operations), op_name)
    test_fail = 0
    # Log timing results
    log.info("=" * 80)
    log.info("OPERATION TIMING RESULTS")
    log.info("=" * 80)
    log.info("Operation:<50> Time (s):<15> Baseline (s):<15> Status:<10>")
    log.info("-" * 80)

    for op_name, timing_info in operation_times.items():
        baseline = timing_info["baseline_time"]
        baseline_str = (
            "%s" % baseline if isinstance(baseline, (int, float)) else str(baseline)
        )
        log.info(
            "%s:<50> %s:<15> %s:<15> %s:<10>",
            op_name,
            timing_info["time_taken"],
            baseline_str,
            timing_info["status"],
        )
        # Compare with baseline if available
        if isinstance(baseline, (int, float)) and baseline > 0:
            diff = timing_info["time_taken"] - baseline
            diff_pct = (diff / baseline) * 100
            log.info("  -> Difference: %s seconds (%s%%)", diff, diff_pct)
            if diff_pct > 10:
                log.error(
                    "Operation %s took more than 10%% longer than baseline", op_name
                )
                test_fail += 1

    log.info("=" * 80)
    if test_fail > 0:
        return 1
    return 0


def run(ceph_cluster, **kw):
    """
    Test Case: Clone create with parallel volume operations
    Polarion TC : CEPH-83593402
    Test Steps:
    1. Set max_concurrent_clones to 3
    2. Create subvolume and add data worth 10G using truncate pattern
    3. Create a Snapshot
    4. Perform clone create and while cloning in-progress, run volume ops in parallel:
       a. Snapshots create - Both manual and add snap-schedule with retention
       b. Other Clone create
       c. Subvolumegroup create, Subvolume create, Subvolume info, Subvolume delete
          and subvolume delete with retain-snapshot
       d. Subvolume modify, Subvolume and Snapshot metadata ops
       e. Ceph config get/set
    5. Capture time taken for each operation and compare with baseline time

    Clean-up:
    1. Remove snap-schedules
    2. Remove snapshots
    3. Remove clones
    4. Remove subvolumes
    5. Remove subvolumegroups
    6. Reset max_concurrent_clones to default
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        snap_util = SnapUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        skip_clone_create = config.get("skip_clone_create", "false")
        log.info("Checking Pre-requisites")
        if len(clients) < 1:
            log.error(
                "This test requires minimum 1 client node. This has only %s clients",
                len(clients),
            )
            return 1

        client1 = clients[0]
        default_fs = config.get("fs_name", "cephfs_clone_test")

        # Verify/Create filesystem
        log.info("Create fs volume if not present, otherwise remove and create again")
        fs_details = fs_util.get_fs_info(client1, default_fs)
        if fs_details:
            fs_util.remove_fs(client1, default_fs)
        fs_util.create_fs(client1, default_fs)
        fs_util.wait_for_mds_process(client1, default_fs)
        log.info("FS volume %s created", default_fs)

        # Set Baseline times
        log.info("Setting baseline times")
        baseline_times = {
            "Manual Snapshot Create": 1,
            "Snap-Schedule Add": 1,
            "Snap-Schedule Remove": 1,
            "Other Clone Create": 1,
            "Subvolumegroup Create": 2,
            "Subvolume Create": 1,
            "Subvolume Info": 1,
            "Subvolume Delete": 1,
            "Subvolume Delete with Retain-Snapshot": 1,
            "Subvolume Modify (Resize)": 1,
            "Subvolume Metadata Set": 1,
            "Subvolume Metadata Get": 1,
            "Snapshot Metadata Set": 1,
            "Snapshot Metadata Get": 1,
            "Ceph Config Get": 1,
            "Ceph Config Set": 3,
        }
        # Step 1: Set max_concurrent_clones to 3
        log.info("Step 1: Set max_concurrent_clones to 3")
        concurrent_limit = "3"
        out, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph config set mgr mgr/volumes/max_concurrent_clones {concurrent_limit}",
        )
        log.info("Set max_concurrent_clones to %s", concurrent_limit)

        # Verify the setting
        out, rc = client1.exec_command(
            sudo=True,
            cmd="ceph config get mgr mgr/volumes/max_concurrent_clones",
        )
        if concurrent_limit not in out:
            log.error("Failed to set max_concurrent_clones to %s", concurrent_limit)
            return 1

        # Step 2: Create subvolume and add data worth 10G
        log.info("Step 2: Create subvolume and add data worth 10G")
        subvol_name = f"test_sv_{''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(5))}"
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": subvol_name,
            "size": "16106127360",  # 15GB to accommodate 10G data
        }
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Created subvolume: %s", subvol_name)

        # Get subvolume path and mount
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} {subvol_name}",
        )
        subvol_path = subvol_path.strip()

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}/"

        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir,
            extra_params=f"--client_fs {default_fs}",
        )

        # Add data using truncate pattern
        mount_path = f"{fuse_mounting_dir}{subvol_path.lstrip('/')}"
        add_data_to_subvolume(client1, mount_path, target_size_gb=9)

        # Step 3: Create a Snapshot
        log.info("Step 3: Create a Snapshot")
        snap_name = "clone_snap"
        snapshot = {
            "vol_name": default_fs,
            "subvol_name": subvol_name,
            "snap_name": snap_name,
        }
        fs_util.create_snapshot(client1, **snapshot)
        log.info("Created snapshot: %s", snap_name)

        # Prepare subvolume object for operations
        sv_obj = {
            "vol_name": default_fs,
            "subvol_name": subvol_name,
            "snap_name": snap_name,
        }

        # Step 4: Perform clone create and run parallel operations
        log.info("Step 4: Perform clone create and run parallel volume operations")

        # Enable snap-schedule module if needed
        try:
            snap_util.enable_snap_schedule(client1)
            snap_util.allow_minutely_schedule(client1, allow=True)
        except Exception as e:
            log.error("Could not enable snap-schedule module, exiting... %s", str(e))
            return 1

        # Run parallel volume operations (one at a time, each in parallel to clone)
        test_status = run_parallel_volume_ops(
            client1,
            fs_util,
            snap_util,
            default_fs,
            sv_obj,
            skip_clone_create,
            baseline_times,
        )
        return test_status

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Starting Clean Up")
        client1 = clients[0] if clients else None

        if client1:
            # Note: Clones are cleaned up within run_parallel_volume_ops for each operation

            # Clean up snapshot

            if "snap_name" in locals() and "subvol_name" in locals():
                fs_util.remove_snapshot(
                    client1,
                    vol_name=default_fs,
                    subvol_name=subvol_name,
                    snap_name=snap_name,
                    validate=False,
                    check_ec=False,
                )

            # Clean up subvolume

            if "subvol_name" in locals():
                fs_util.remove_subvolume(
                    client1,
                    vol_name=default_fs,
                    subvol_name=subvol_name,
                    validate=True,
                    check_ec=False,
                )

            # Unmount

            if "fuse_mounting_dir" in locals():
                client1.exec_command(
                    sudo=True,
                    cmd=f"umount {fuse_mounting_dir}",
                    timeout=300,
                    check_ec=False,
                )
                client1.exec_command(
                    sudo=True, cmd=f"rm -rf {fuse_mounting_dir}", check_ec=False
                )

            # Reset max_concurrent_clones to default (4)

            client1.exec_command(
                sudo=True,
                cmd="ceph config set mgr mgr/volumes/max_concurrent_clones 4",
                check_ec=False,
            )
            log.info("Reset max_concurrent_clones to default value 4")

            # Remove FS volume
            fs_util.remove_fs(client1, default_fs)
            log.info("FS volume removed")
        log.info("Clean Up completed")
