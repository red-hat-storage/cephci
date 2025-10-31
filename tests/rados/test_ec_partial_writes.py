"""
Module to test Erasure Code Partial Write and Read Optimization (Fast EC)

This test verifies that EC pools with allow_ec_optimizations enabled properly
utilize partial write optimization to reduce write amplification by skipping
unnecessary shard updates, and validates EC partial read operations.

Test workflow:
1. Create EC pool (k=2, m=2 configurable) with optimizations enabled
2. Create replicated metadata pool for RBD
3. Create TWO RBD images with EC data pool:
   - Image 1: For direct device I/O (unmounted)
   - Image 2: For filesystem I/O (mounted)
4. Identify target PGs/OSDs and enable debug logging
5. Run direct RADOS writes with partial overwrites at various offsets
6. Workflow 1 - Direct device writes (Image 1, unmounted):
   - Block device writes (direct I/O): full stripe, single chunk, sub-chunk, overwrites
7. Workflow 2 - Filesystem writes (Image 2, mounted):
   - Filesystem writes (buffered I/O): chunk-aligned, sub-chunk, overwrites
8. Run various read I/O patterns with offsets:
   - Full object reads from offset 0
   - Reads from stripe boundaries
   - Reads from chunk boundaries
   - Reads from arbitrary offsets (aligned and unaligned)
   - Single byte reads from EOF
9. Parse OSD logs to verify partial write patterns
10. Verify optimization statistics
11. Cleanup resources
"""

import re
import time
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Main test function to verify EC partial write optimization via logging

    Args:
        ceph_cluster: Ceph cluster object
        **kw: Test configuration parameters

    Returns:
        0 on success, 1 on failure
    """
    log.info(run.__doc__)
    config = kw.get("config", {})
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_config_obj = MonConfigMethods(rados_obj=rados_obj)

    # Test configuration
    image_size = config.get("image_size", "1G")
    ec_k = config.get("ec_k", 2)
    ec_m = config.get("ec_m", 2)
    plugin = config.get("plugin", "isa")
    technique = config.get("technique", "reed_sol_van")
    chunk_size = config.get("chunk_size", 16384)  # 16K
    profile_name = config.get("profile_name", f"ec_k{ec_k}_m{ec_m}_profile")
    ec_pool_name = config.get("ec_pool_name", f"ec_k{ec_k}_m{ec_m}_data_pool")
    metadata_pool_name = config.get(
        "metadata_pool_name", f"re_k{ec_k}_m{ec_m}_metadata_pool"
    )
    # Two images: one for device I/O, one for filesystem I/O
    device_image_name = config.get(
        "device_image_name", f"ec_k{ec_k}_m{ec_m}_device_img"
    )
    fs_image_name = config.get("fs_image_name", f"ec_k{ec_k}_m{ec_m}_fs_img")
    num_target_pgs = config.get("num_target_pgs", 6)

    target_osds = []
    target_pgs = []
    device_path_device = None
    device_path_fs = None
    mount_path = None

    try:
        log.info(
            "Module to test Erasure Code Partial Write Optimization (Fast EC). Testing started"
        )

        if not rados_obj.enable_file_logging():
            log.error("Failed to enable file logging")
            return 1

        log.info("Create EC pool with Fast EC optimizations")
        if not rados_obj.create_erasure_pool(
            pool_name=ec_pool_name,
            profile_name=profile_name,
            k=ec_k,
            m=ec_m,
            plugin=plugin,
            technique=technique,
            enable_fast_ec_features=True,
            app_name="rbd",
        ):
            log.error("Failed to create EC data pool with Fast EC optimizations")
            return 1

        log.info(
            "Created EC pool: %s (k=%s, m=%s, chunk=%s)",
            ec_pool_name,
            ec_k,
            ec_m,
            chunk_size,
        )

        log.debug("Create replicated metadata pool for Metadata purposes")
        if not rados_obj.create_pool(pool_name=metadata_pool_name, app_name="rbd"):
            log.error("Failed to create metadata pool")
            return 1

        log.debug(
            "Proceeding Identify target PGs and OSDs by mapping object placement via object names. \n"
            "This is for enabling debug logs on the OSDs for verifying the Partial writes in OSD logs"
        )
        time.sleep(20)
        pgs = rados_obj.get_pgid(pool_name=ec_pool_name)
        log.debug("Pool '%s' has %s total PGs", ec_pool_name, len(pgs))

        if num_target_pgs > len(pgs):
            log.warning(
                "Requested %s PGs but pool only has %s. Using only %s for testing.",
                num_target_pgs,
                len(pgs),
                len(pgs),
            )
            num_target_pgs = len(pgs)

        log.debug("Num PGs selected for testing : %s", num_target_pgs)

        target_pgs, target_osds, planned_objects = identify_target_pgs(
            rados_obj=rados_obj,
            pool_name=ec_pool_name,
            num_pgs=num_target_pgs,
        )

        if not target_osds:
            log.error("Failed to identify target OSDs")
            return 1

        log.debug("Enable debug logging on target OSDs - debug_osd 30")
        for osd_id in target_osds:
            mon_config_obj.set_config(
                section=f"osd.{osd_id}",
                name="debug_osd",
                value="30/30",
            )
        log.debug("Enabled debug logging on %s OSDs", len(target_osds))
        time.sleep(5)
        start_time, _ = cephadm.shell(
            args=["date", "-u", "'+%Y-%m-%dT%H:%M:%S.%3N+0000'"]
        )
        start_time = start_time.strip().strip("'")
        log.info("Test start time : %s", start_time)

        stripe_width = ec_k * chunk_size  # e.g., 2 * 16KB = 32KB
        chunk_kb = chunk_size // 1024
        stripe_kb = stripe_width // 1024
        half_chunk_kb = chunk_kb // 2

        log.info("EC Configuration: k=%s, m=%s, chunk_size=%sKB", ec_k, ec_m, chunk_kb)
        log.info("Calculated stripe width: %sKB (%s x %sKB)", stripe_kb, ec_k, chunk_kb)

        # Test 0: Direct RADOS writes to EC pool
        log.info("Test 0: Direct RADOS writes to EC pool with pre-planned object names")
        if not run_direct_rados_writes(
            rados_obj=rados_obj,
            pool_name=ec_pool_name,
            chunk_size=chunk_size,
            stripe_width=stripe_width,
            planned_objects=planned_objects,
        ):
            log.error("Direct RADOS writes failed")
            return 1
        time.sleep(2)

        log.info("Starting writes via RBD as client")

        # Workflow 1: Create RBD image for direct device I/O (unmounted)
        log.info("Creating Image 1 for direct device I/O (unmounted)")
        cmd = f"rbd create --size {image_size} --data-pool {ec_pool_name} {metadata_pool_name}/{device_image_name}"
        try:
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            log.info(
                "Created device I/O image: %s/%s", metadata_pool_name, device_image_name
            )
        except Exception as e:
            log.error("Failed to create device I/O image: %s", e)
            return 1

        # Map the device I/O image (but don't mount)
        cmd = f"rbd map {metadata_pool_name}/{device_image_name}"
        try:
            device_path_device, _ = rados_obj.client.exec_command(cmd=cmd, sudo=True)
            device_path_device = device_path_device.strip()
            log.info("Mapped device I/O image to %s (unmounted)", device_path_device)
        except Exception as e:
            log.error("Failed to map device I/O image: %s", e)
            return 1

        # Workflow 2: Create RBD image for filesystem I/O (mounted)
        log.info("Creating Image 2 for filesystem I/O (mounted)")
        cmd = f"rbd create --size {image_size} --data-pool {ec_pool_name} {metadata_pool_name}/{fs_image_name}"
        try:
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            log.info(
                "Created filesystem image: %s/%s", metadata_pool_name, fs_image_name
            )
        except Exception as e:
            log.error("Failed to create filesystem image: %s", e)
            return 1

        # Mount the filesystem image
        mount_result = rados_obj.mount_image_on_client(
            pool_name=metadata_pool_name,
            img_name=fs_image_name,
            client_obj=rados_obj.client,
            mount_path="/tmp/ec_test_fs_mount/",
        )

        if not mount_result:
            log.error("Failed to mount filesystem image")
            return 1

        mount_path, device_path_fs = mount_result
        log.info("Mounted filesystem image %s at %s", device_path_fs, mount_path)

        # Test 1: Run device writes on unmounted device image
        log.info("Test 1: Running direct device write tests (Image 1, unmounted)")
        if not run_device_writes(
            rados_obj=rados_obj,
            device_path=device_path_device,
            chunk_size=chunk_size,
            stripe_width=stripe_width,
        ):
            log.error("Device write tests failed")
            return 1
        time.sleep(2)

        # Test 2: Run filesystem writes on mounted filesystem image
        log.info("Test 2: Running filesystem write tests (Image 2, mounted)")
        if not run_filesystem_writes(
            rados_obj=rados_obj,
            mount_path=mount_path,
            chunk_size=chunk_size,
        ):
            log.error("Filesystem write tests failed")
            return 1
        time.sleep(2)

        end_time, _ = cephadm.shell(
            args=["date", "-u", "'+%Y-%m-%dT%H:%M:%S.%3N+0000'"]
        )
        end_time = end_time.strip().strip("'")
        log.info("Write Test end time (from installer): %s", end_time)

        # Removing debug configs set
        for osd_id in target_osds:
            mon_config_obj.remove_config(section=f"osd.{osd_id}", name="debug_osd")

        # Test 3: Read patterns with various offsets (rados get)
        log.info("Test 3: Read patterns with various offsets")
        if not run_read_offset_tests(
            rados_obj=rados_obj,
            pool_name=ec_pool_name,
            chunk_size=chunk_size,
            ec_k=ec_k,
        ):
            log.error("Read offset tests failed")
            return 1
        time.sleep(2)

        log.info("Waiting 10 seconds after tests (buffer for log completion)...")
        time.sleep(10)

        log.info("Analyze OSD logs for partial write evidence")
        # Parse logs from each target OSD
        results = parse_ec_optimization_logs(
            rados_obj=rados_obj,
            target_osds=target_osds,
            start_time=start_time,
            end_time=end_time,
        )

        if not results:
            log.error("Failed to parse OSD logs")
            return 1
        log.info("Log analysis of OSD logs for partial write evidence Successful.")

        log.info("Verification of optimization results")
        if not verify_partial_write_optimization(results, ec_k=ec_k, ec_m=ec_m):
            log.error("Partial write optimization verification failed")
            return 1

        if not rados_obj.run_pool_sanity_check():
            log.error("Pool sanity checks failed on cluster")
            return 1

    except Exception as err:
        log.error("Test failed with exception: %s", err)
        log.exception(err)
        return 1

    finally:
        log.info("\n\nExecution of finally block\n\n")
        if "target_osds" in locals() and target_osds:
            for osd_id in target_osds:
                mon_config_obj.remove_config(section=f"osd.{osd_id}", name="debug_osd")

        # Unmount filesystem image (Image 2)
        try:
            if "mount_path" in locals() and mount_path:
                clean_mount_path = mount_path.rstrip("/")
                rados_obj.client.exec_command(
                    cmd=f"umount {clean_mount_path}", sudo=True, check_ec=False
                )
                # Remove mount directory
                rados_obj.client.exec_command(
                    cmd=f"rm -rf {clean_mount_path}", sudo=True, check_ec=False
                )
                log.debug("Unmounted and cleaned up %s", clean_mount_path)
        except Exception as e:
            log.warning("Failed to unmount filesystem: %s", e)

        # Unmap filesystem RBD device (Image 2)
        try:
            if "device_path_fs" in locals() and device_path_fs:
                rados_obj.client.exec_command(
                    cmd=f"rbd unmap {device_path_fs}", sudo=True, check_ec=False
                )
                log.debug("Unmapped filesystem RBD device %s", device_path_fs)
        except Exception as e:
            log.warning("Failed to unmap filesystem RBD device: %s", e)

        # Unmap device I/O RBD device (Image 1)
        try:
            if "device_path_device" in locals() and device_path_device:
                rados_obj.client.exec_command(
                    cmd=f"rbd unmap {device_path_device}", sudo=True, check_ec=False
                )
                log.debug("Unmapped device I/O RBD device %s", device_path_device)
        except Exception as e:
            log.warning("Failed to unmap device I/O RBD device: %s", e)

        # Delete pools
        rados_obj.delete_pool(pool=ec_pool_name)
        rados_obj.delete_pool(pool=metadata_pool_name)

        # Check for crashes
        if rados_obj.check_crash_status():
            log.error("Crashes detected after test execution")
            return 1
        rados_obj.log_cluster_health()

    log.info("TEST PASSED: EC Partial Write Optimization Verified")
    return 0


def run_direct_rados_writes(
    rados_obj,
    pool_name: str,
    chunk_size: int,
    stripe_width: int,
    planned_objects: List[str],
) -> bool:
    """
    Perform direct RADOS writes to EC pool using pre-planned object names.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        chunk_size: EC chunk size in bytes
        stripe_width: Stripe width (k * chunk_size) in bytes
        planned_objects: List of object names mapped to land on target OSDs

    Returns:
        True on success, False on failure
    """
    try:
        log.info("Direct RADOS writes with partial overwrites using --offset")
        log.info(
            "Will write to %s pre-mapped objects: %s",
            len(planned_objects),
            planned_objects,
        )

        # Create a large base object size (multiple stripes)
        base_object_size = stripe_width * 4
        log.info(
            "Base object size: %s bytes (%s stripes of %s bytes each)",
            base_object_size,
            4,
            stripe_width,
        )

        # Define partial overwrite patterns at different offsets
        overwrite_patterns = [
            {
                "size": chunk_size,
                "offset": 0,
                "desc": "single chunk at start (offset 0)",
            },
            {
                "size": chunk_size // 2,
                "offset": chunk_size,
                "desc": "sub-chunk at chunk boundary",
            },
            {"size": 4096, "offset": stripe_width, "desc": "4KB at stripe boundary"},
            {
                "size": 8192,
                "offset": stripe_width + chunk_size // 2,
                "desc": "8KB mid-stripe",
            },
            {
                "size": chunk_size,
                "offset": stripe_width * 2,
                "desc": "chunk at 2nd stripe",
            },
            {
                "size": chunk_size + (chunk_size // 2),
                "offset": 512,
                "desc": "1.5 chunks unaligned",
            },
            {
                "size": stripe_width,
                "offset": chunk_size,
                "desc": "full stripe at chunk offset",
            },
            {"size": chunk_size * 2, "offset": 1024, "desc": "2 chunks at 1KB offset"},
        ]

        total_operations = len(planned_objects) * (1 + len(overwrite_patterns))
        log.info(
            "Will perform %s operations: %s initial writes + %s overwrites",
            total_operations,
            len(planned_objects),
            len(planned_objects) * len(overwrite_patterns),
        )

        operation_count = 0

        for obj_name in planned_objects:
            # Step 1: Create base object (large, multiple stripes)
            base_file = f"/tmp/{obj_name}_base.bin"
            cmd = f"dd if=/dev/urandom of={base_file} bs={base_object_size} count=1 2>/dev/null"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            cmd = f"rados -p {pool_name} put {obj_name} {base_file}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            operation_count += 1
            log.debug(
                "Created base object %s (%s bytes, %s stripes)",
                obj_name,
                base_object_size,
                4,
            )

            # Step 2: Perform in-place partial overwrites using rados put --offset
            for pattern_idx, pattern in enumerate(overwrite_patterns):
                # Create partial data to write
                partial_file = f"/tmp/{obj_name}_partial{pattern_idx}.bin"
                cmd = f"dd if=/dev/urandom of={partial_file} bs={pattern['size']} count=1 2>/dev/null"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)

                # Write at specific offset to trigger partial write optimization
                cmd = f"rados -p {pool_name} put {obj_name} {partial_file} --offset {pattern['offset']}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
                log.debug(
                    "Overwrote %s: %s bytes at offset %s (%s)",
                    obj_name,
                    pattern["size"],
                    pattern["offset"],
                    pattern["desc"],
                )

                operation_count += 1
            log.debug(
                "Completed %s partial overwrites on %s",
                len(overwrite_patterns),
                obj_name,
            )

        # Cleanup temp files
        rados_obj.client.exec_command(
            cmd="rm -f /tmp/*_base.bin /tmp/*_partial*.bin",
            sudo=True,
            check_ec=False,
        )

        log.info(
            "Completed %s operations: %s base objects + %s partial overwrites",
            operation_count,
            len(planned_objects),
            len(planned_objects) * len(overwrite_patterns),
        )
        return True

    except Exception as e:
        log.error("Direct RADOS writes failed: %s", e)
        return False


def run_device_writes(
    rados_obj,
    device_path: str,
    chunk_size: int,
    stripe_width: int,
) -> bool:
    """
    Run direct device write tests (unmounted device)

    Args:
        rados_obj: RadosOrchestrator object
        device_path: RBD block device path (e.g., /dev/rbd0) - UNMOUNTED
        chunk_size: EC chunk size in bytes
        stripe_width: EC stripe width (k * chunk_size) in bytes

    Returns:
        True on success, False on failure
    """
    try:
        chunk_kb = chunk_size // 1024
        stripe_kb = stripe_width // 1024
        half_chunk_kb = chunk_kb // 2
        cross_chunk_kb = chunk_kb + half_chunk_kb

        # Define device write test patterns
        device_patterns = [
            {
                "block_size": f"{stripe_kb}K",
                "count": 100,
                "seek": None,
                "desc": f"Full stripe writes ({stripe_kb}KB)",
            },
            {
                "block_size": f"{chunk_kb}K",
                "count": 200,
                "seek": None,
                "desc": f"Single chunk writes ({chunk_kb}KB)",
            },
            {
                "block_size": f"{half_chunk_kb}K",
                "count": 500,
                "seek": None,
                "desc": f"Sub-chunk writes ({half_chunk_kb}KB)",
            },
            {
                "block_size": f"{cross_chunk_kb}K",
                "count": 100,
                "seek": None,
                "desc": f"Cross-chunk writes ({cross_chunk_kb}KB)",
            },
            {
                "block_size": f"{chunk_kb}K",
                "count": 100,
                "seek": 0,
                "desc": f"Overwrite at offset 0 ({chunk_kb}KB)",
            },
            {
                "block_size": f"{half_chunk_kb}K",
                "count": 50,
                "seek": 100,
                "desc": f"Overwrite at offset 100 ({half_chunk_kb}KB)",
            },
            {
                "block_size": f"{chunk_kb}K",
                "count": 50,
                "seek": 200,
                "desc": f"Overwrite at offset 200 ({chunk_kb}KB)",
            },
        ]

        log.info(
            "Running %s direct device write patterns on %s (unmounted)",
            len(device_patterns),
            device_path,
        )

        for idx, pattern in enumerate(device_patterns, 1):
            log.info(
                "Device Test %s/%s: %s", idx, len(device_patterns), pattern["desc"]
            )

            seek_param = (
                f"seek={pattern['seek']}" if pattern["seek"] is not None else ""
            )
            cmd = (
                f"dd if=/dev/urandom of={device_path} "
                f"bs={pattern['block_size']} count={pattern['count']} {seek_param} "
                f"oflag=direct conv=notrunc"
            )

            try:
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
                log.info("Device Test %s/%s PASSED", idx, len(device_patterns))
            except Exception as e:
                log.error("Device Test %s/%s FAILED: %s", idx, len(device_patterns), e)
                return False

            time.sleep(1)

        log.info(
            "Completed all %s device write patterns successfully", len(device_patterns)
        )
        return True

    except Exception as e:
        log.error("Device write tests failed: %s", e)
        return False


def run_filesystem_writes(
    rados_obj,
    mount_path: str,
    chunk_size: int,
) -> bool:
    """
    Run filesystem write tests (mounted device)

    Tests buffered I/O through filesystem layer.
    Device must be mounted with a filesystem.

    Args:
        rados_obj: RadosOrchestrator object
        mount_path: Mounted filesystem path (e.g., /tmp/ec_test_mount/)
        chunk_size: EC chunk size in bytes

    Returns:
        True on success, False on failure
    """
    try:
        chunk_kb = chunk_size // 1024
        half_chunk_kb = chunk_kb // 2

        # Define all RBD write test patterns
        write_patterns = [
            # Filesystem writes (buffered I/O) - Initial writes
            {
                "target": "mount",
                "block_size": f"{chunk_kb}K",
                "count": 200,
                "seek": None,
                "test_name": "Filesystem: Chunk Writes",
                "desc": f"Filesystem: Chunk-aligned writes ({chunk_kb}KB)",
            },
            {
                "target": "mount",
                "block_size": f"{half_chunk_kb}K",
                "count": 300,
                "seek": None,
                "test_name": "Filesystem: Sub-chunk Writes",
                "desc": f"Filesystem: Sub-chunk writes ({half_chunk_kb}KB)",
            },
            # Filesystem overwrites (testing buffered overwrites)
            {
                "target": "mount",
                "block_size": f"{chunk_kb}K",
                "count": 50,
                "seek": 0,
                "test_name": "Filesystem: Overwrite at offset 0",
                "desc": f"Filesystem: Overwrite at offset 0 ({chunk_kb}KB)",
            },
            {
                "target": "mount",
                "block_size": f"{half_chunk_kb}K",
                "count": 50,
                "seek": 100,
                "test_name": "Filesystem: Overwrite at offset 100",
                "desc": f"Filesystem: Overwrite at block 100 ({half_chunk_kb}KB)",
            },
        ]

        log.info(
            "Running %s filesystem write test patterns on mounted device",
            len(write_patterns),
        )

        # Create base filesystem test file for overwrites
        log.debug("Creating base filesystem test file for overwrites")
        clean_mount_path = mount_path.rstrip("/")
        base_file = f"{clean_mount_path}/rbd_test_file.dat"
        cmd = f"dd if=/dev/urandom of={base_file} bs=1M count=50 conv=fsync"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.debug("Created base file: %s (50MB)", base_file)

        # Execute all write patterns (filesystem only)
        failed_tests = []
        for idx, pattern in enumerate(write_patterns, 1):
            log.info("Test %s/%s: %s", idx, len(write_patterns), pattern["desc"])

            result = write_io_data(
                rados_obj=rados_obj,
                mount_path=mount_path,
                block_size=pattern["block_size"],
                count=pattern["count"],
                seek=pattern["seek"],
                test_name=pattern["test_name"],
            )

            if not result:
                log.error(
                    "Test %s/%s FAILED: %s",
                    idx,
                    len(write_patterns),
                    pattern["test_name"],
                )
                failed_tests.append(pattern["test_name"])
                log.error("Filesystem write failed.")
            else:
                log.info(
                    "Test %s/%s PASSED: %s",
                    idx,
                    len(write_patterns),
                    pattern["test_name"],
                )
            time.sleep(2)

        if failed_tests:
            log.error("Some tests failed: %s", failed_tests)
            return False

        log.info(
            "Completed all %s filesystem write patterns successfully",
            len(write_patterns),
        )
        return True

    except Exception as e:
        log.error("Filesystem write tests failed: %s", e)
        return False


def write_io_data(
    rados_obj,
    device: str = None,
    mount_path: str = None,
    block_size: str = "1M",
    count: int = 10,
    seek: int = None,
    test_name: str = "Write I/O",
) -> bool:
    """
    Write data to RBD device (direct I/O) or filesystem (buffered I/O)

    Args:
        rados_obj: RadosOrchestrator object
        device: Device path for direct block writes (e.g., /dev/rbd0)
        mount_path: Mount path for filesystem writes (e.g., /mnt/rbd)
        block_size: Block size (e.g., "4K", "16K", "32K", "1M"). Default: "1M"
        count: Number of blocks to write
        seek: Seek position in blocks (for overwrites on both device and filesystem)
        test_name: Test name for logging

    Returns:
        True on success, False on failure
    """
    try:
        if mount_path:
            clean_mount_path = mount_path.rstrip("/")

            # Use fixed filename for overwrites (seek != None), unique for new writes
            if seek is not None:
                test_file = f"{clean_mount_path}/rbd_test_file.dat"
                seek_param = f"seek={seek}"
                conv_param = "conv=notrunc,fsync"  # Don't truncate + sync data
            else:
                test_file = f"{clean_mount_path}/testfile_{int(time.time())}.dat"
                seek_param = ""
                conv_param = "conv=fsync"  # Sync data to disk

            cmd = (
                f"dd if=/dev/urandom of={test_file} "
                f"bs={block_size} count={count} {seek_param} {conv_param}"
            )
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            log.debug(
                "%s: Wrote %s x %s to mounted filesystem", test_name, count, block_size
            )
        elif device:
            seek_param = f"seek={seek}" if seek is not None else ""
            cmd = (
                f"dd if=/dev/urandom of={device} "
                f"bs={block_size} count={count} {seek_param} "
                f"oflag=direct conv=notrunc"
            )
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            log.debug("%s: Wrote %s x %s to block device", test_name, count, block_size)

        else:
            log.error("Either device or mount_path must be specified")
            return False

        log.info("Completed RBD writes on the image. Test %s", test_name)
        return True

    except Exception as e:
        log.error("%s: Failed to write data: %s", test_name, e)
        return False


def identify_target_pgs(
    rados_obj, pool_name: str, num_pgs: int = 3
) -> Tuple[List[str], List[int], List[str]]:
    """
    Identify target PGs and OSDs by mapping object placement BEFORE creating them
    Uses 'ceph osd map' with planned object names to map where objects will land.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: Name of the pool
        num_pgs: Number of PGs to target

    Returns:
        Tuple of (list of PG IDs, list of primary OSD IDs, list of planned object names)
    """
    try:
        log.debug("Mapping object placement for target PG/OSD selection")
        base_prefixes = [
            "ec_test_full_stripe",
            "partial_write_test",
            "fast_ec_verification",
            "optimization_check",
            "shard_skip_test",
        ]

        # Generate candidate object names
        candidate_objects = base_prefixes.copy()
        for prefix in base_prefixes:
            for i in range(10):
                candidate_objects.append(f"{prefix}_{i:04d}")

        log.debug("Generated %s candidate object names", len(candidate_objects))

        # Map predicted placement for each candidate object
        target_pgs = []
        target_osds = []
        planned_objects = []
        pg_to_objects = defaultdict(list)  # Track which objects map to which PG

        for obj_name in candidate_objects:
            try:
                osd_map_output = rados_obj.get_osd_map(pool=pool_name, obj=obj_name)
                pg_id = osd_map_output.get("pgid")
                acting_set = osd_map_output.get("acting", [])

                if not pg_id or not acting_set:
                    log.warning(
                        "Could not map placement for %s: pg_id=%s, acting=%s",
                        obj_name,
                        pg_id,
                        acting_set,
                    )
                    continue
                primary_osd = acting_set[0]

                if pg_id not in target_pgs:
                    if len(target_pgs) < num_pgs:
                        target_pgs.append(pg_id)
                        log.info(
                            "New PG Added to tracking and test further: %s (Primary: OSD.%s, Acting: %s)",
                            pg_id,
                            primary_osd,
                            acting_set,
                        )
                    else:
                        log.debug(
                            "Not selecting PG : %s, Required no of PGs %s already selected",
                            pg_id,
                            num_pgs,
                        )
                        continue

                # Add ALL objects that map to our target PGs
                if pg_id in target_pgs:
                    planned_objects.append(obj_name)
                    pg_to_objects[pg_id].append(obj_name)

                    if primary_osd not in target_osds:
                        target_osds.append(primary_osd)

                    log.debug(
                        "Mapped: %s -> PG %s (object #%s for this PG)",
                        obj_name,
                        pg_id,
                        len(pg_to_objects[pg_id]),
                    )

            except Exception as e:
                log.warning("Failed to map placement for %s: %s", obj_name, e)
                continue

        if not target_osds:
            log.error("No OSDs identified from placement prediction")
            return [], [], []

        log.info("  Selected PGs: %s", target_pgs)
        log.info("  Selected OSDs: %s", target_osds)
        log.info("  Objects mapped: %s", planned_objects)
        log.info("\nPG to Objects Mapping:")
        for pg in target_pgs:
            objects = pg_to_objects[pg]
            log.info("  PG %s: %s objects -> %s", pg, len(objects), objects)
        return target_pgs, target_osds, planned_objects

    except Exception as e:
        log.error("Failed to map target PGs: %s", e)
        return [], [], []


def run_read_offset_tests(
    rados_obj,
    pool_name: str,
    chunk_size: int,
    ec_k: int,
) -> bool:
    """
    Test EC partial reads with various offset patterns using rados get

    Tests stripe-aligned and cross-stripe reads to verify EC read optimization.
    Note: rados get --offset X reads from offset X to END of object

    Validates:
    1. Read size correctness
    2. Data integrity (byte-by-byte comparison)

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        chunk_size: EC chunk size in bytes
        ec_k: Number of data chunks

    Returns:
        True on success, False on failure
    """
    try:
        log.info("EC Partial Read Tests - Testing offset-based reads")

        # Calculate stripe width
        stripe_width = ec_k * chunk_size
        test_data_size = 2 * stripe_width  # 2 stripes

        log.info(
            "EC config: k=%s, chunk=%sKB, stripe=%sKB",
            ec_k,
            chunk_size // 1024,
            stripe_width // 1024,
        )

        # Create unique test object name (avoid conflicts with previous runs)
        obj_name = f"ec_read_test_{int(time.time())}"
        test_data_file = "/tmp/ec_test_read_data.bin"

        log.debug(
            "Creating test object with %s bytes (%sKB)",
            test_data_size,
            test_data_size // 1024,
        )

        # Generate test data
        cmd = f"dd if=/dev/urandom of={test_data_file} bs={test_data_size} count=1 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

        # Upload to pool
        cmd = f"rados -p {pool_name} put {obj_name} {test_data_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.debug("Created object '%s' in pool '%s'", obj_name, pool_name)

        # Define test patterns - covers key EC read scenarios
        test_cases = [
            {
                "name": "Full object",
                "offset": 0,
                "desc": "Read entire object (2 stripes)",
            },
            {
                "name": "Stripe boundary",
                "offset": stripe_width,
                "desc": "Read from stripe alignment",
            },
            {
                "name": "Chunk boundary",
                "offset": chunk_size,
                "desc": "Read from chunk alignment",
            },
            {
                "name": "Mid-chunk",
                "offset": chunk_size // 2,
                "desc": "Read from mid-chunk (unaligned)",
            },
            {
                "name": "Cross-stripe",
                "offset": stripe_width - 100,
                "desc": "Read crossing stripe boundary",
            },
            {
                "name": "Last byte",
                "offset": test_data_size - 1,
                "desc": "Read single last byte",
            },
        ]

        # Calculate expected lengths for each test
        for test in test_cases:
            test["expected_length"] = test_data_size - test["offset"]

        passed = 0
        failed = 0

        log.info("Running %s read offset test patterns", len(test_cases))

        for i, test in enumerate(test_cases, 1):
            test_passed = True  # Track if this specific test passed both validations

            try:
                log.info("Test %s/%s: %s", i, len(test_cases), test["name"])
                log.debug("  %s (offset=%s)", test["desc"], test["offset"])

                outfile = f"/tmp/ec_read_test_{i}.bin"

                # Perform rados get with offset
                cmd = f"rados -p {pool_name} get {obj_name} {outfile} --offset {test['offset']}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)

                # Validation 1: Check size
                check_cmd = f"wc -c < {outfile}"
                result, _ = rados_obj.client.exec_command(cmd=check_cmd, sudo=True)
                actual_size = int(result.strip())
                expected_size = test["expected_length"]

                if actual_size != expected_size:
                    log.error(
                        " Size mismatch: got %s bytes, expected %s",
                        actual_size,
                        expected_size,
                    )
                    test_passed = False
                else:
                    log.debug(" Size correct: %s bytes", actual_size)

                # Validation 2: Data integrity (only if size is correct and non-zero)
                if test_passed and actual_size > 0:
                    """
                    Original file (64KB):
                    [0────────16KB─────────32KB─────────48KB─────────64KB]
                              ↑                                         ↑
                           offset                                     EOF
                              └──────────────────────────────────────┘
                                       Compare this portion (48KB)

                    Read file (48KB):
                    [0─────────────────────32KB─────────────────────48KB]
                     ↑                                                 ↑
                     start                                           EOF
                    """
                    cmd = f"cmp -n {actual_size} -i {test['offset']}:0 {test_data_file} {outfile}"
                    result, _ = rados_obj.client.exec_command(
                        cmd=cmd, sudo=True, check_ec=False
                    )

                    if result.strip() == "":
                        log.debug(" Data integrity verified")
                    else:
                        log.error(" Data mismatch: content doesn't match original")
                        test_passed = False

                # Update counters based on overall test result
                if test_passed:
                    passed += 1
                    log.info("  PASS: %s", test["name"])
                else:
                    failed += 1
                    log.error("  FAIL: %s", test["name"])

                # Cleanup
                rados_obj.client.exec_command(
                    cmd=f"rm -f {outfile}", sudo=True, check_ec=False
                )

            except Exception as e:
                log.error("  FAIL: %s - %s", test["name"], e)
                failed += 1

        # Cleanup test object and files
        log.debug("Cleaning up test data")
        rados_obj.client.exec_command(
            cmd=f"rados -p {pool_name} rm {obj_name}", sudo=True, check_ec=False
        )
        rados_obj.client.exec_command(
            cmd=f"rm -f {test_data_file}", sudo=True, check_ec=False
        )

        log.info("EC Read Offset Tests Summary:")
        log.info(
            "  Total: %s | Passed: %s | Failed: %s", len(test_cases), passed, failed
        )

        if failed == 0:
            log.info("All EC read offset tests PASSED")
            return True
        else:
            log.error(" %s/%s test(s) FAILED", failed, len(test_cases))
            return False

    except Exception as e:
        log.error("EC read offset tests failed with exception: %s", e)
        log.exception(e)
        return False


def parse_ec_optimization_logs(
    rados_obj,
    target_osds: List[int],
    start_time: str,
    end_time: str,
) -> Dict[int, Dict[str, Any]]:
    """
    Parse OSD logs to extract EC optimization evidence

    Args:
        rados_obj: RadosOrchestrator object
        target_osds: List of OSD IDs to analyze
        start_time: Start time for log analysis
        end_time: End time for log analysis

    Returns:
        Dictionary with OSD statistics
    """
    results = {}

    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    for osd_id in target_osds:
        try:
            log.info("Analyzing logs for OSD.%s", osd_id)

            # Find which node hosts this OSD
            cmd = f"ceph osd find {osd_id}"
            osd_info = rados_obj.run_ceph_command(cmd=cmd)
            osd_host = osd_info.get("host")
            host_obj = rados_obj.get_host_object(hostname=osd_host)
            log_path = f"/var/log/ceph/{fsid}/ceph-osd.{osd_id}.log"
            temp_log = f"/tmp/osd_{osd_id}_ec_test.log"

            # Extract date portion for filtering
            start_date = start_time.split("T")[0]
            end_date = end_time.split("T")[0]

            log.debug(
                "Extracting ALL log entries for OSD.%s in time range %s to %s",
                osd_id,
                start_time,
                end_time,
            )
            time_filter_cmd = f"grep -E '{start_date}|{end_date}' {log_path}"
            cmd = (
                f"{time_filter_cmd} | "
                f"awk -v start='{start_time}' -v end='{end_time}' "
                f"'$1 >= start && $1 <= end' "
                f"> {temp_log} 2>/dev/null || true"
            )
            host_obj.exec_command(cmd=cmd, sudo=True, check_ec=False)

            try:
                count_filtered, _ = host_obj.exec_command(
                    cmd=f"wc -l {temp_log}", sudo=True, check_ec=False
                )
                lines_captured = int(count_filtered.strip().split()[0])
                log.info(
                    "Captured %s total log lines in time range for OSD.%s",
                    lines_captured,
                    osd_id,
                )
            except Exception as e:
                log.warning("Could not count filtered lines: %s", e)
                lines_captured = 0

            log.debug("Proceeding to Read and parse log")
            log_content, _ = host_obj.exec_command(
                cmd=f"sudo cat {temp_log}", sudo=True, check_ec=False
            )

            # Check if we captured any logs
            if not log_content or len(log_content.strip()) == 0 or lines_captured == 0:
                log.error(
                    "No log entries found for OSD.%s in time range %s to %s",
                    osd_id,
                    start_time,
                    end_time,
                )
                continue

            # Parse for EC optimization patterns
            written_patterns = defaultdict(int)
            shard_version_count = 0
            partial_write_count = 0

            # Count total log lines captured
            log_lines_list = log_content.splitlines() if log_content else []
            log_lines_captured = len(log_lines_list)

            log.debug("Parsing %s lines for OSD.%s", log_lines_captured, osd_id)

            for line in log_lines_list:
                # Look for written={...} patterns
                written_match = re.search(r"written=\{([0-9,]*)\}", line)
                if written_match:
                    written_set = written_match.group(1)
                    if written_set:  # Non-empty
                        # Normalize the pattern
                        shards = sorted([int(s) for s in written_set.split(",")])
                        pattern = "{" + ",".join(str(s) for s in shards) + "}"
                        written_patterns[pattern] += 1

                # Look for shard_versions - tracks stale shards
                if "shard_versions={" in line and "shard_versions={}" not in line:
                    shard_version_count += 1

                # Look for partial_write - PWLC tracking
                if "partial_write" in line:
                    partial_write_count += 1

            results[osd_id] = {
                "written_patterns": dict(written_patterns),
                "shard_version_count": shard_version_count,
                "total_written_entries": sum(written_patterns.values()),
                "partial_write_count": partial_write_count,
                "log_lines_captured": log_lines_captured,
            }

            # Cleanup temp files
            host_obj.exec_command(cmd=f"rm -f {temp_log}", sudo=True, check_ec=False)

            # Log summary
            log.info("OSD.%s Analysis Results:", osd_id)
            log.info("  Log lines captured: %s", log_lines_captured)
            log.info(
                "  written= patterns: %s", results[osd_id]["total_written_entries"]
            )
            log.info("  shard_versions entries: %s", shard_version_count)
            log.info("  partial_write entries: %s", partial_write_count)

            if written_patterns:
                log.debug("  Written patterns breakdown:")
                for pattern, count in sorted(
                    written_patterns.items(), key=lambda x: -x[1]
                ):
                    log.debug("    %s: %s occurrences", pattern, count)

            if results[osd_id]["total_written_entries"] > 0:
                log.info(" Found EC optimization patterns")
            else:
                log.error(" No EC optimization patterns found")
        except Exception as e:
            log.error("Failed to parse logs for OSD.%s: %s", osd_id, e)
            continue
    return results


def verify_partial_write_optimization(
    results: Dict[int, Dict[str, Any]], ec_k: int, ec_m: int
) -> bool:
    """
    Verify that partial write optimization is working

    Args:
        results: Dictionary of OSD analysis results
        ec_k: Number of data chunks
        ec_m: Number of parity chunks

    Returns:
        True if optimization is verified, False otherwise
    """
    if not results:
        log.error("No OSD log data to verify")
        return False

    total_shards = ec_k + ec_m
    log.info("Verification: EC k=%s, m=%s, total_shards=%s", ec_k, ec_m, total_shards)

    # Collect statistics across all OSDs
    total_partial_writes = 0
    total_full_writes = 0

    for osd_id, stats in results.items():
        written_patterns = stats.get("written_patterns", {})

        for pattern, count in written_patterns.items():
            # Count number of shards in this pattern
            num_shards = (
                pattern.count(",") + 1
                if "," in pattern
                else (1 if pattern != "{}" else 0)
            )

            if num_shards < total_shards:
                total_partial_writes += count
            elif num_shards == total_shards:
                total_full_writes += count

    log.info("Verification Results:")
    log.info("  Partial write patterns: %s", total_partial_writes)
    log.info("  Full write patterns: %s", total_full_writes)

    if total_partial_writes > 0:
        log.info("EC PARTIAL WRITE OPTIMIZATION VERIFIED - TEST PASSED")
        return True
    elif total_full_writes > 0:
        log.error("Only full writes detected - Partial write optimization NOT working")
        return False
    else:
        log.error("No EC write patterns found in logs")
        return False
