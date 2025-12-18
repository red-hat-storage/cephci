"""
Module to test Erasure Code Partial Write and Read Optimization (Fast EC)

This test verifies that EC pools with allow_ec_optimizations enabled properly
utilize partial write optimization to reduce write amplification by skipping
unnecessary shard updates, and partial read optimization to reduce read
amplification by reading only necessary shards and byte ranges.

Test workflow:
1. Create EC pool (k=2, m=2 configurable) with optimizations enabled
2. Create replicated metadata pool for RBD
3. Create TWO RBD images with EC data pool:
   - Image 1: For direct device I/O (unmounted)
   - Image 2: For filesystem I/O (mounted)
4. Identify target PGs/OSDs and enable debug logging for write tests
5. Run direct RADOS writes with partial overwrites at various offsets
6. Run object TRUNCATE tests with various boundary conditions:
   - Truncate to exact stripe boundary (N * K * stripe_unit)
   - Truncate to chunk boundaries
   - Truncate to non-aligned sizes
   - Truncate after partial overwrites
   - Sequential truncates on same object
   - Truncate to zero
   - Data integrity verification after truncate
7. Run object APPEND tests with various boundary conditions:
   - Append to reach exact stripe boundary (N * K * stripe_unit)
   - Append crossing stripe boundary
   - Sequential appends on same object
   - Append after truncate (shard version interaction)
   - Append after partial overwrite
   - Small appends (sub-chunk size)
   - Data integrity verification after append
8. Workflow 1 - Direct device writes (Image 1, unmounted):
   - Block device writes (direct I/O): full stripe, single chunk, sub-chunk, overwrites
9. Workflow 2 - Filesystem writes (Image 2, mounted):
   - Filesystem writes (buffered I/O): chunk-aligned, sub-chunk, overwrites
10. Parse OSD logs to verify partial WRITE patterns (written={...} with subset of shards)
11. Verify partial WRITE optimization statistics
12. Enable debug logging for read tests
13. Run various read I/O patterns with offsets:
    - Full object reads from offset 0
    - Reads from stripe boundaries
    - Reads from chunk boundaries
    - Reads from arbitrary offsets (aligned and unaligned)
    - Single byte reads from EOF
14. Parse OSD logs to verify partial READ patterns:
    - shard_want_to_read={...} with single shard reads
    - extent-based reads with specific byte ranges
    - Reduced read amplification
15. Verify partial READ optimization statistics
16. Cleanup resources
"""

import re
import time
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
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
    # stripe_unit: chunk size per data shard (default 16KB, recommended for Fast EC)
    stripe_unit = config.get("stripe_unit", 16384)
    # For k+m > 4, use OSD failure domain; otherwise use host (default)
    crush_failure_domain = config.get("crush_failure_domain", None)
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
    test_start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {test_start_time}")
    try:
        log.info(
            "Module to test Erasure Code Partial Write Optimization (Fast EC). Testing started"
        )

        if not rados_obj.enable_file_logging():
            log.error("Failed to enable file logging")
            return 1

        # Calculate derived EC values
        stripe_width = ec_k * stripe_unit  # Total data bytes per stripe
        full_stripe = (ec_k + ec_m) * stripe_unit  # Total bytes including parity

        # Print EC Configuration Banner
        log.info("=" * 70)
        log.info("EC POOL CONFIGURATION PASSED FROM CONFIG FILE")
        log.info("=" * 70)
        log.info("  Pool Name       : %s", ec_pool_name)
        log.info("  Profile Name    : %s", profile_name)
        log.info("  Plugin          : %s", plugin)
        log.info("  Technique       : %s", technique)
        log.info("  k (data shards) : %s", ec_k)
        log.info("  m (parity shards): %s", ec_m)
        log.info(
            "  stripe_unit     : %s bytes (%sKB) - chunk size per data shard",
            stripe_unit,
            stripe_unit // 1024,
        )
        log.info(
            "  stripe_width    : %s bytes (%sKB) - k * stripe_unit",
            stripe_width,
            stripe_width // 1024,
        )
        log.info(
            "  full_stripe     : %s bytes (%sKB) - (k+m) * stripe_unit",
            full_stripe,
            full_stripe // 1024,
        )
        log.info("  Fast EC         : Enabled (allow_ec_optimizations)")
        log.info("=" * 70)

        log.info("Creating EC pool with Fast EC optimizations...")
        ec_pool_params = {
            "pool_name": ec_pool_name,
            "profile_name": profile_name,
            "k": ec_k,
            "m": ec_m,
            "plugin": plugin,
            "technique": technique,
            "stripe_unit": stripe_unit,
            "enable_fast_ec_features": True,
            "app_name": "rbd",
        }
        # Add crush failure domain if specified (required for k+m > 4)
        if crush_failure_domain:
            ec_pool_params["crush-failure-domain"] = crush_failure_domain
            log.info("  CRUSH failure domain: %s", crush_failure_domain)

        if not rados_obj.create_erasure_pool(**ec_pool_params):
            log.error("Failed to create EC data pool with Fast EC optimizations")
            return 1

        log.info("EC pool created successfully: %s", ec_pool_name)

        # Verify EC profile configuration matches expected values
        log.info("Verifying EC profile configuration...")
        ec_profile_details = rados_obj.get_ec_profile_detail(profile=profile_name)
        if not ec_profile_details:
            log.error("Failed to retrieve EC profile details for %s", profile_name)
            return 1

        # Verify stripe_unit from EC profile
        profile_stripe_unit = int(ec_profile_details.get("stripe_unit", 0))
        profile_k = int(ec_profile_details.get("k", 0))
        profile_m = int(ec_profile_details.get("m", 0))
        profile_plugin = ec_profile_details.get("plugin", "")

        log.info("-" * 70)
        log.info("EC PROFILE VERIFICATION (from cluster)")
        log.info("-" * 70)
        log.info("  Profile Name    : %s", profile_name)
        log.info("  k               : %s (expected: %s)", profile_k, ec_k)
        log.info("  m               : %s (expected: %s)", profile_m, ec_m)
        log.info("  plugin          : %s (expected: %s)", profile_plugin, plugin)
        log.info(
            "  stripe_unit     : %s bytes (%sKB) (expected: %s bytes)",
            profile_stripe_unit,
            profile_stripe_unit // 1024,
            stripe_unit,
        )
        log.info(
            "  stripe_width    : %sKB (k * stripe_unit = %s * %sKB)",
            (profile_k * profile_stripe_unit) // 1024,
            profile_k,
            profile_stripe_unit // 1024,
        )
        log.info("-" * 70)

        # Fail if stripe_unit doesn't match
        if profile_stripe_unit != stripe_unit:
            log.error(
                "EC profile stripe_unit mismatch! Expected: %s bytes, Got: %s bytes",
                stripe_unit,
                profile_stripe_unit,
            )
            return 1
        log.info(
            "EC profile verification passed - stripe_unit correctly set to %sKB",
            stripe_unit // 1024,
        )

        # Verify pool details from cluster
        log.info("Verifying EC pool details...")
        pool_details = rados_obj.get_pool_details(pool=ec_pool_name)
        if not pool_details:
            log.error("Failed to retrieve pool details for %s", ec_pool_name)
            return 1

        # Extract pool attributes
        pool_size = pool_details.get("size", 0)  # k + m
        pool_min_size = pool_details.get("min_size", 0)
        pool_ec_profile = pool_details.get("erasure_code_profile", "")
        pool_stripe_width = pool_details.get("stripe_width", 0)
        pool_type = pool_details.get("type", "")
        pool_ec_overwrites = pool_details.get("flags_names", "")

        # Expected values
        expected_size = ec_k + ec_m
        expected_stripe_width = ec_k * stripe_unit

        log.info("-" * 70)
        log.info("EC POOL VERIFICATION (from cluster)")
        log.info("-" * 70)
        log.info("  Pool Name       : %s", ec_pool_name)
        log.info("  Pool Type       : %s", pool_type)
        log.info("  EC Profile      : %s (expected: %s)", pool_ec_profile, profile_name)
        log.info("  Size (k+m)      : %s (expected: %s)", pool_size, expected_size)
        log.info("  Min Size        : %s", pool_min_size)
        log.info(
            "  stripe_width    : %s bytes (%sKB) (expected: %s bytes / %sKB)",
            pool_stripe_width,
            pool_stripe_width // 1024 if pool_stripe_width else 0,
            expected_stripe_width,
            expected_stripe_width // 1024,
        )
        log.info("  Flags           : %s", pool_ec_overwrites)
        log.info("-" * 70)

        # Verify stripe_width matches expected
        if pool_stripe_width != expected_stripe_width:
            log.error(
                "Pool stripe_width mismatch! Expected: %s bytes (%sKB), Got: %s bytes (%sKB)",
                expected_stripe_width,
                expected_stripe_width // 1024,
                pool_stripe_width,
                pool_stripe_width // 1024 if pool_stripe_width else 0,
            )
            return 1
        log.info(
            "EC pool verification passed - stripe_width correctly set to %sKB",
            pool_stripe_width // 1024,
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

        # Test 0: Direct RADOS writes to EC pool
        log.info("Test 0: Direct RADOS writes to EC pool with pre-planned object names")
        if not run_direct_rados_writes(
            rados_obj=rados_obj,
            pool_name=ec_pool_name,
            stripe_unit=stripe_unit,
            stripe_width=stripe_width,
            planned_objects=planned_objects,
        ):
            log.error("Direct RADOS writes failed")
            return 1
        time.sleep(2)

        # Test 0.5: EC Object Truncation Tests
        log.info("Test 0.5: EC Object Truncation tests with boundary conditions")
        if not run_truncate_tests(
            rados_obj=rados_obj,
            pool_name=ec_pool_name,
            stripe_unit=stripe_unit,
            ec_k=ec_k,
            ec_m=ec_m,
        ):
            log.error("EC Truncation tests failed")
            return 1
        time.sleep(2)

        # Test 0.6: EC Object Append Tests
        log.info("Test 0.6: EC Object Append tests with boundary conditions")
        if not run_append_tests(
            rados_obj=rados_obj,
            pool_name=ec_pool_name,
            stripe_unit=stripe_unit,
            ec_k=ec_k,
            ec_m=ec_m,
        ):
            log.error("EC Append tests failed")
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
            stripe_unit=stripe_unit,
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
            stripe_unit=stripe_unit,
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

        # Run read tests with OSD identification and debug logging
        read_test_results = run_read_offset_tests(
            rados_obj=rados_obj,
            pool_name=ec_pool_name,
            stripe_unit=stripe_unit,
            ec_k=ec_k,
            mon_config_obj=mon_config_obj,
            cephadm=cephadm,
        )

        if not read_test_results or not read_test_results.get("success"):
            log.error("Read offset tests failed")
            return 1

        read_target_osds = read_test_results.get("target_osds", [])
        read_start_time = read_test_results.get("start_time", "")
        read_end_time = read_test_results.get("end_time", "")

        if not read_target_osds:
            log.error("Failed to identify primary OSD for read tests")
            return 1

        log.info(
            "Read tests completed successfully. Will analyze primary OSD.%s",
            read_target_osds[0],
        )

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

        log.info("Verification of partial write optimization results")
        if not verify_partial_write_optimization(results, ec_k=ec_k, ec_m=ec_m):
            log.error("Partial write optimization verification failed")
            return 1

        log.info("Analyze OSD logs for partial READ evidence")
        # Parse logs for read optimization patterns from READ test OSDs
        read_results = parse_ec_read_optimization_logs(
            rados_obj=rados_obj,
            target_osds=read_target_osds,
            start_time=read_start_time,
            end_time=read_end_time,
        )

        if not read_results:
            log.error("Failed to parse OSD logs for read patterns")
            return 1
        log.info("Log analysis of OSD logs for partial read evidence Successful.")

        log.info("Verification of partial READ optimization results")
        if not verify_partial_read_optimization(read_results, ec_k=ec_k, ec_m=ec_m):
            log.error("Partial read optimization verification failed")
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
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {test_start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(
            start_time=test_start_time, end_time=test_end_time
        ):
            log.error("Test failed due to crash at the end of test")
            return 1
        rados_obj.log_cluster_health()

    log.info("TEST PASSED: EC Partial Write Optimization Verified")
    return 0


def run_direct_rados_writes(
    rados_obj,
    pool_name: str,
    stripe_unit: int,
    stripe_width: int,
    planned_objects: List[str],
) -> bool:
    """
    Perform direct RADOS writes to EC pool using pre-planned object names.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        stripe_unit: EC stripe_unit in bytes (chunk size per data shard)
        stripe_width: Stripe width (k * stripe_unit) in bytes
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
                "size": stripe_unit,
                "offset": 0,
                "desc": "single chunk at start (offset 0)",
            },
            {
                "size": stripe_unit // 2,
                "offset": stripe_unit,
                "desc": "sub-chunk at chunk boundary",
            },
            {"size": 4096, "offset": stripe_width, "desc": "4KB at stripe boundary"},
            {
                "size": 8192,
                "offset": stripe_width + stripe_unit // 2,
                "desc": "8KB mid-stripe",
            },
            {
                "size": stripe_unit,
                "offset": stripe_width * 2,
                "desc": "chunk at 2nd stripe",
            },
            {
                "size": stripe_unit + (stripe_unit // 2),
                "offset": 512,
                "desc": "1.5 chunks unaligned",
            },
            {
                "size": stripe_width,
                "offset": stripe_unit,
                "desc": "full stripe at chunk offset",
            },
            {"size": stripe_unit * 2, "offset": 1024, "desc": "2 chunks at 1KB offset"},
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


def run_truncate_tests(
    rados_obj,
    pool_name: str,
    stripe_unit: int,
    ec_k: int,
    ec_m: int,
) -> bool:
    """
    Test EC object truncation with various boundary conditions.

    Test covers:
    1. Truncate to exact stripe boundaries (N * K * stripe_unit)
    2. Truncate to chunk boundaries (N * stripe_unit)
    3. Truncate to non-aligned sizes
    4. Truncate after partial overwrites
    5. Sequential truncates on same object
    6. Truncate to zero (complete truncation)
    7. Data integrity verification after truncate

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        stripe_unit: EC chunk size in bytes
        ec_k: Number of data chunks
        ec_m: Number of parity chunks

    Returns:
        True on success, False on failure
    """
    try:
        stripe_width = ec_k * stripe_unit
        log.info("=" * 70)
        log.info("EC TRUNCATE TESTS - Testing object truncation edge cases")
        log.info("=" * 70)
        log.info(
            "EC config: k=%s, m=%s, stripe_unit=%sKB, stripe_width=%sKB",
            ec_k,
            ec_m,
            stripe_unit // 1024,
            stripe_width // 1024,
        )
        log.info(
            "BUG SCENARIO: Truncate to N * K * stripe_unit = N * %s bytes",
            stripe_width,
        )

        passed = 0
        failed = 0
        test_results = []

        # =====================================================================
        # TEST GROUP 1: Truncate to exact stripe boundaries
        # This is the critical test - truncating to N * K * stripe_unit
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 1: Truncate to exact stripe boundaries")
        log.info("-" * 50)

        stripe_boundary_tests = [
            {
                "name": "Truncate 1.5 stripes -> 1 stripe",
                "initial_size": int(stripe_width * 1.5),
                "truncate_to": stripe_width,
                "desc": "12KB -> 8KB for k=2,chunk=4K (N=1 * K * chunk)",
            },
            {
                "name": "Truncate 3 stripes -> 2 stripes",
                "initial_size": stripe_width * 3,
                "truncate_to": stripe_width * 2,
                "desc": "Truncate to N=2 stripe boundary",
            },
            {
                "name": "Truncate 2.5 stripes -> 1 stripe",
                "initial_size": int(stripe_width * 2.5),
                "truncate_to": stripe_width,
                "desc": "Larger reduction to N=1 stripe boundary",
            },
            {
                "name": "Truncate 4 stripes -> 3 stripes",
                "initial_size": stripe_width * 4,
                "truncate_to": stripe_width * 3,
                "desc": "Truncate to N=3 stripe boundary",
            },
            {
                "name": "Truncate 1.25 stripes -> 1 stripe",
                "initial_size": int(stripe_width * 1.25),
                "truncate_to": stripe_width,
                "desc": "Small reduction to N=1 stripe boundary",
            },
        ]

        for test in stripe_boundary_tests:
            result = _run_single_truncate_test(
                rados_obj=rados_obj,
                pool_name=pool_name,
                test_name=test["name"],
                initial_size=test["initial_size"],
                truncate_to=test["truncate_to"],
                description=test["desc"],
                verify_data=True,
            )
            test_results.append(result)
            if result["passed"]:
                passed += 1
            else:
                failed += 1

        # =====================================================================
        # TEST GROUP 2: Truncate to chunk boundaries
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 2: Truncate to chunk boundaries")
        log.info("-" * 50)

        chunk_boundary_tests = [
            {
                "name": "Truncate 2.5 chunks -> 2 chunks",
                "initial_size": int(stripe_unit * 2.5),
                "truncate_to": stripe_unit * 2,
                "desc": "Truncate to 2 chunk boundary",
            },
            {
                "name": "Truncate 3 chunks -> 1 chunk",
                "initial_size": stripe_unit * 3,
                "truncate_to": stripe_unit,
                "desc": "Truncate to 1 chunk boundary",
            },
            {
                "name": "Truncate 1.5 chunks -> 1 chunk",
                "initial_size": int(stripe_unit * 1.5),
                "truncate_to": stripe_unit,
                "desc": "Sub-stripe truncation",
            },
            {
                "name": "Truncate 4 chunks -> 3 chunks",
                "initial_size": stripe_unit * 4,
                "truncate_to": stripe_unit * 3,
                "desc": "Truncate crossing stripe boundary",
            },
        ]

        for test in chunk_boundary_tests:
            result = _run_single_truncate_test(
                rados_obj=rados_obj,
                pool_name=pool_name,
                test_name=test["name"],
                initial_size=test["initial_size"],
                truncate_to=test["truncate_to"],
                description=test["desc"],
                verify_data=True,
            )
            test_results.append(result)
            if result["passed"]:
                passed += 1
            else:
                failed += 1

        # =====================================================================
        # TEST GROUP 3: Truncate to non-aligned sizes
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 3: Truncate to non-aligned sizes")
        log.info("-" * 50)

        non_aligned_tests = [
            {
                "name": "Truncate to mid-chunk (unaligned)",
                "initial_size": stripe_width * 2,
                "truncate_to": stripe_unit + (stripe_unit // 2),
                "desc": "Truncate to 1.5 chunks (unaligned)",
            },
            {
                "name": "Truncate to arbitrary offset",
                "initial_size": stripe_width * 2,
                "truncate_to": stripe_unit + 1000,
                "desc": "Truncate to chunk + 1000 bytes",
            },
            {
                "name": "Truncate to 1 byte less than stripe",
                "initial_size": stripe_width * 2,
                "truncate_to": stripe_width - 1,
                "desc": "Just under stripe boundary",
            },
            {
                "name": "Truncate to 1 byte more than stripe",
                "initial_size": stripe_width * 2,
                "truncate_to": stripe_width + 1,
                "desc": "Just over stripe boundary",
            },
            {
                "name": "Truncate to small size (512 bytes)",
                "initial_size": stripe_width * 2,
                "truncate_to": 512,
                "desc": "Truncate to sub-block size",
            },
        ]

        for test in non_aligned_tests:
            result = _run_single_truncate_test(
                rados_obj=rados_obj,
                pool_name=pool_name,
                test_name=test["name"],
                initial_size=test["initial_size"],
                truncate_to=test["truncate_to"],
                description=test["desc"],
                verify_data=True,
            )
            test_results.append(result)
            if result["passed"]:
                passed += 1
            else:
                failed += 1

        # =====================================================================
        # TEST GROUP 4: Truncate to zero (complete truncation)
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 4: Truncate to zero")
        log.info("-" * 50)

        zero_truncate_tests = [
            {
                "name": "Truncate stripe-sized object to zero",
                "initial_size": stripe_width,
                "truncate_to": 0,
                "desc": "Complete truncation of 1-stripe object",
            },
            {
                "name": "Truncate multi-stripe object to zero",
                "initial_size": stripe_width * 3,
                "truncate_to": 0,
                "desc": "Complete truncation of 3-stripe object",
            },
            {
                "name": "Truncate chunk-sized object to zero",
                "initial_size": stripe_unit,
                "truncate_to": 0,
                "desc": "Complete truncation of single chunk",
            },
        ]

        for test in zero_truncate_tests:
            result = _run_single_truncate_test(
                rados_obj=rados_obj,
                pool_name=pool_name,
                test_name=test["name"],
                initial_size=test["initial_size"],
                truncate_to=test["truncate_to"],
                description=test["desc"],
                verify_data=False,  # Can't verify data for zero-size
            )
            test_results.append(result)
            if result["passed"]:
                passed += 1
            else:
                failed += 1

        # =====================================================================
        # TEST GROUP 5: Sequential truncates on same object
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 5: Sequential truncates on same object")
        log.info("-" * 50)

        result = _run_sequential_truncate_test(
            rados_obj=rados_obj,
            pool_name=pool_name,
            stripe_width=stripe_width,
        )
        test_results.append(result)
        if result["passed"]:
            passed += 1
        else:
            failed += 1

        # =====================================================================
        # TEST GROUP 6: Truncate after partial overwrite + Sequential truncates on same object
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 6: Truncate after partial overwrite")
        log.info("-" * 50)

        result = _run_truncate_after_partial_write_test(
            rados_obj=rados_obj,
            pool_name=pool_name,
            stripe_unit=stripe_unit,
            stripe_width=stripe_width,
        )
        test_results.append(result)
        if result["passed"]:
            passed += 1
        else:
            failed += 1

        # =====================================================================
        # SUMMARY
        # =====================================================================
        log.info("=" * 70)
        log.info("EC TRUNCATE TESTS SUMMARY")
        log.info("=" * 70)
        log.info(
            "Total tests: %s | Passed: %s | Failed: %s",
            len(test_results),
            passed,
            failed,
        )

        # Log failed tests
        failed_tests = [t for t in test_results if not t["passed"]]
        if failed_tests:
            log.error("FAILED TESTS:")
            for ft in failed_tests:
                log.error("  - %s: %s", ft["name"], ft.get("error", "Unknown error"))

        if failed > 0:
            log.error(
                "EC TRUNCATE TESTS FAILED: %s/%s tests failed",
                failed,
                len(test_results),
            )
            return False

        log.info("EC TRUNCATE TESTS PASSED: All %s tests passed", len(test_results))
        return True

    except Exception as e:
        log.error("EC truncate tests failed with exception: %s", e)
        log.exception(e)
        return False


def _run_single_truncate_test(
    rados_obj,
    pool_name: str,
    test_name: str,
    initial_size: int,
    truncate_to: int,
    description: str,
    verify_data: bool = True,
) -> Dict[str, Any]:
    """
    Run a single truncate test case.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        test_name: Name of the test
        initial_size: Initial object size in bytes
        truncate_to: Size to truncate to in bytes
        description: Test description
        verify_data: Whether to verify data integrity after truncate

    Returns:
        Dictionary with test results: {"name", "passed", "error"}
    """
    result = {"name": test_name, "passed": False, "error": None}
    obj_name = f"truncate_test_{int(time.time() * 1000)}"
    data_file = f"/tmp/{obj_name}_data.bin"
    read_file = f"/tmp/{obj_name}_read.bin"

    try:
        log.info("Test: %s", test_name)
        log.info("  Description: %s", description)
        log.info(
            "  Initial size: %s bytes, Truncate to: %s bytes", initial_size, truncate_to
        )

        # Step 1: Create test data file
        cmd = f"dd if=/dev/urandom of={data_file} bs={initial_size} count=1 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

        # Step 2: Upload object to pool
        cmd = f"rados -p {pool_name} put {obj_name} {data_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.debug("  Created object %s with %s bytes", obj_name, initial_size)

        # Step 3: print initial object size
        initial_stat = rados_obj.get_object_stat(pool_name=pool_name, obj_name=obj_name)
        log.debug(
            "  Initial stat: %s",
            initial_stat.get("raw_output") if initial_stat else "N/A",
        )

        # Step 4: Perform truncate
        log.info(
            "  Executing: rados -p %s truncate %s %s", pool_name, obj_name, truncate_to
        )
        try:
            cmd = f"rados -p {pool_name} truncate {obj_name} {truncate_to}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            log.info("  Truncate command succeeded")
        except Exception as truncate_err:
            error_str = str(truncate_err)
            log.error("  Truncate failed with: %s", truncate_err)
            result["error"] = f"Truncate failed: {error_str}"
            return result

        # Step 5: Verify truncated object size
        size_check = validate_object_size(
            rados_obj=rados_obj,
            pool_name=pool_name,
            obj_name=obj_name,
            expected_size=truncate_to,
            operation="truncate",
        )
        if not size_check["success"]:
            result["error"] = size_check["error"]
            return result

        # Step 6: Verify data integrity (for non-zero truncates)
        if verify_data and truncate_to > 0:
            # Read truncated object
            cmd = f"rados -p {pool_name} get {obj_name} {read_file}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            # Compare first truncate_to bytes of original with read data
            cmd = f"cmp -n {truncate_to} {data_file} {read_file}"
            cmp_output, _ = rados_obj.client.exec_command(
                cmd=cmd, sudo=True, check_ec=False
            )

            if cmp_output.strip() == "":
                log.info("  Data integrity verified: first %s bytes match", truncate_to)
            else:
                log.error("  Data integrity FAILED: content mismatch")
                result["error"] = "Data integrity verification failed"
                return result

        result["passed"] = True
        log.info("  PASSED: %s", test_name)

    except Exception as e:
        log.error("  FAILED: %s - %s", test_name, e)
        result["error"] = str(e)

    finally:
        # Cleanup
        try:
            rados_obj.client.exec_command(
                cmd=f"rm -f {data_file} {read_file}", sudo=True, check_ec=False
            )
        except Exception:
            pass

    return result


def _run_sequential_truncate_test(
    rados_obj,
    pool_name: str,
    stripe_width: int,
) -> Dict[str, Any]:
    """
    Test sequential truncates on the same object.

    Creates object and performs multiple truncates:
    4 stripes -> 3 stripes -> 2 stripes -> 1 stripe -> 0

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        stripe_width: Stripe width (k * stripe_unit)

    Returns:
        Dictionary with test results
    """
    result = {"name": "Sequential truncates", "passed": False, "error": None}
    obj_name = f"seq_truncate_test_{int(time.time() * 1000)}"
    data_file = f"/tmp/{obj_name}_data.bin"

    try:
        log.info("Test: Sequential truncates on same object")
        initial_size = stripe_width * 4
        log.info("  Initial size: %s bytes (4 stripes)", initial_size)

        # Create initial object
        cmd = f"dd if=/dev/urandom of={data_file} bs={initial_size} count=1 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        cmd = f"rados -p {pool_name} put {obj_name} {data_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

        # Sequential truncates to stripe boundaries
        truncate_sequence = [
            stripe_width * 3,  # 3 stripes
            stripe_width * 2,  # 2 stripes
            stripe_width * 1,  # 1 stripe
            0,  # zero
        ]

        for i, truncate_to in enumerate(truncate_sequence, 1):
            log.info(
                "  Step %s: Truncate to %s bytes (%s stripe(s))",
                i,
                truncate_to,
                truncate_to // stripe_width if stripe_width else 0,
            )

            cmd = f"rados -p {pool_name} truncate {obj_name} {truncate_to}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            log.info("    Truncate succeeded")

            # Verify size after truncate
            size_check = validate_object_size(
                rados_obj=rados_obj,
                pool_name=pool_name,
                obj_name=obj_name,
                expected_size=truncate_to,
                operation="truncate",
                step_info=f"Step {i}",
            )
            if not size_check["success"]:
                result["error"] = size_check["error"]
                return result

        result["passed"] = True
        log.info("  PASSED: Sequential truncates")

    except Exception as e:
        log.error("  FAILED: Sequential truncates - %s", e)
        result["error"] = str(e)

    finally:
        # Cleanup
        try:
            rados_obj.client.exec_command(
                cmd=f"rm -f {data_file}", sudo=True, check_ec=False
            )
        except Exception:
            pass

    return result


def _run_truncate_after_partial_write_test(
    rados_obj,
    pool_name: str,
    stripe_unit: int,
    stripe_width: int,
) -> Dict[str, Any]:
    """
    Test truncate after partial overwrite operations.

    This tests the interaction between partial writes (which may leave
    shards with different versions) and truncation.

    Workflow:
    1. Create 3-stripe object
    2. Partial overwrite at offset stripe_unit (single chunk)
    3. Truncate to 1 stripe boundary

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        stripe_unit: EC chunk size
        stripe_width: Stripe width (k * stripe_unit)

    Returns:
        Dictionary with test results
    """
    result = {"name": "Truncate after partial write", "passed": False, "error": None}
    obj_name = f"partial_trunc_test_{int(time.time() * 1000)}"
    data_file = f"/tmp/{obj_name}_data.bin"
    partial_file = f"/tmp/{obj_name}_partial.bin"

    try:
        log.info("Test: Truncate after partial overwrite")
        initial_size = stripe_width * 3
        log.info("  Initial size: %s bytes (3 stripes)", initial_size)

        # Step 1: Create initial object
        cmd = f"dd if=/dev/urandom of={data_file} bs={initial_size} count=1 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        cmd = f"rados -p {pool_name} put {obj_name} {data_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  Created base object")

        # Step 2: Partial overwrite at chunk boundary
        partial_size = stripe_unit
        partial_offset = stripe_unit
        cmd = f"dd if=/dev/urandom of={partial_file} bs={partial_size} count=1 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        cmd = f"rados -p {pool_name} put {obj_name} {partial_file} --offset {partial_offset}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info(
            "  Performed partial overwrite: %s bytes at offset %s",
            partial_size,
            partial_offset,
        )

        # Step 3: Sequential truncates to various boundaries after partial write
        truncate_sequence = [
            (stripe_width * 2, "2 stripes"),
            (stripe_width + stripe_unit, "1 stripe + 1 chunk"),
            (stripe_width, "1 stripe"),
            (stripe_unit, "1 chunk"),
        ]

        for i, (truncate_to, desc) in enumerate(truncate_sequence, 1):
            log.info("  Step %s: Truncating to %s bytes (%s)", i, truncate_to, desc)
            cmd = f"rados -p {pool_name} truncate {obj_name} {truncate_to}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            log.info("    Truncate succeeded")

            # Verify size after truncate
            size_check = validate_object_size(
                rados_obj=rados_obj,
                pool_name=pool_name,
                obj_name=obj_name,
                expected_size=truncate_to,
                operation="truncate after partial write",
                step_info=f"Step {i}",
            )
            if not size_check["success"]:
                result["error"] = size_check["error"]
                return result

        # Step 4: Verify we can still read the object
        read_file = f"/tmp/{obj_name}_read.bin"
        cmd = f"rados -p {pool_name} get {obj_name} {read_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  Successfully read truncated object")

        result["passed"] = True
        log.info("  PASSED: Truncate after partial write")

    except Exception as e:
        log.error("  FAILED: Truncate after partial write - %s", e)
        result["error"] = str(e)

    finally:
        # Cleanup
        try:
            rados_obj.client.exec_command(
                cmd=f"rm -f {data_file} {partial_file} /tmp/{obj_name}_read.bin",
                sudo=True,
                check_ec=False,
            )
        except Exception:
            pass

    return result


def run_append_tests(
    rados_obj,
    pool_name: str,
    stripe_unit: int,
    ec_k: int,
    ec_m: int,
) -> bool:
    """
    Test EC object append operations with various boundary conditions.

    Append operations extend an object by adding data to the end, which
    can trigger different EC shard handling than overwrites. This is
    particularly important for Fast EC where shard versions and partial
    updates need to be handled correctly.

    Test covers:
    1. Append to reach exact stripe boundary (N * K * stripe_unit)
    2. Append crossing stripe boundary
    3. Multiple sequential appends
    4. Append after truncate (interaction with shard versions)
    5. Small appends (sub-chunk size)
    6. Append to reach chunk boundary

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        stripe_unit: EC chunk size in bytes
        ec_k: Number of data chunks
        ec_m: Number of parity chunks

    Returns:
        True on success, False on failure
    """
    try:
        stripe_width = ec_k * stripe_unit
        log.info("=" * 70)
        log.info("EC APPEND TESTS - Testing object append operations")
        log.info("=" * 70)
        log.info(
            "EC config: k=%s, m=%s, stripe_unit=%sKB, stripe_width=%sKB",
            ec_k,
            ec_m,
            stripe_unit // 1024,
            stripe_width // 1024,
        )

        passed = 0
        failed = 0
        test_results = []

        # =====================================================================
        # TEST GROUP 1: Append to reach exact stripe boundary
        # Similar to truncate bug - final size = N * K * stripe_unit
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 1: Append to reach exact stripe boundary")
        log.info("-" * 50)

        stripe_boundary_append_tests = [
            {
                "name": "Append to reach 1 stripe",
                "initial_size": int(stripe_width * 0.5),
                "append_size": int(stripe_width * 0.5),
                "desc": "Half stripe + half stripe = 1 stripe boundary",
            },
            {
                "name": "Append to reach 2 stripes",
                "initial_size": int(stripe_width * 1.5),
                "append_size": int(stripe_width * 0.5),
                "desc": "1.5 stripes + 0.5 stripe = 2 stripe boundary",
            },
            {
                "name": "Append small to reach stripe boundary",
                "initial_size": stripe_width - 1024,
                "append_size": 1024,
                "desc": "Just under stripe + 1KB = exact stripe boundary",
            },
            {
                "name": "Append chunk to reach 2 stripes",
                "initial_size": stripe_width * 2 - stripe_unit,
                "append_size": stripe_unit,
                "desc": "(2 stripes - 1 chunk) + 1 chunk = 2 stripes",
            },
        ]

        for test in stripe_boundary_append_tests:
            result = _run_single_append_test(
                rados_obj=rados_obj,
                pool_name=pool_name,
                test_name=test["name"],
                initial_size=test["initial_size"],
                append_size=test["append_size"],
                description=test["desc"],
                verify_data=True,
            )
            test_results.append(result)
            if result["passed"]:
                passed += 1
            else:
                failed += 1

        # =====================================================================
        # TEST GROUP 2: Append crossing stripe boundary
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 2: Append crossing stripe boundary")
        log.info("-" * 50)

        cross_stripe_append_tests = [
            {
                "name": "Append crossing from stripe 1 to 2",
                "initial_size": stripe_width - (stripe_unit // 2),
                "append_size": stripe_unit,
                "desc": "Start near end of stripe 1, cross into stripe 2",
            },
            {
                "name": "Large append crossing multiple stripes",
                "initial_size": stripe_unit,
                "append_size": stripe_width * 2,
                "desc": "1 chunk + 2 stripes = crosses 2 stripe boundaries",
            },
            {
                "name": "Append crossing at unaligned position",
                "initial_size": stripe_width + 500,
                "append_size": stripe_width,
                "desc": "Unaligned start + full stripe append",
            },
        ]

        for test in cross_stripe_append_tests:
            result = _run_single_append_test(
                rados_obj=rados_obj,
                pool_name=pool_name,
                test_name=test["name"],
                initial_size=test["initial_size"],
                append_size=test["append_size"],
                description=test["desc"],
                verify_data=True,
            )
            test_results.append(result)
            if result["passed"]:
                passed += 1
            else:
                failed += 1

        # =====================================================================
        # TEST GROUP 3: Append to reach chunk boundary
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 3: Append to reach chunk boundary")
        log.info("-" * 50)

        chunk_boundary_append_tests = [
            {
                "name": "Append to reach 1 chunk",
                "initial_size": stripe_unit // 2,
                "append_size": stripe_unit // 2,
                "desc": "Half chunk + half chunk = 1 chunk",
            },
            {
                "name": "Append to reach 2 chunks",
                "initial_size": stripe_unit + (stripe_unit // 2),
                "append_size": stripe_unit // 2,
                "desc": "1.5 chunks + 0.5 chunk = 2 chunks",
            },
            {
                "name": "Append 1 byte to reach chunk boundary",
                "initial_size": stripe_unit - 1,
                "append_size": 1,
                "desc": "stripe_unit - 1 + 1 byte = exact chunk",
            },
        ]

        for test in chunk_boundary_append_tests:
            result = _run_single_append_test(
                rados_obj=rados_obj,
                pool_name=pool_name,
                test_name=test["name"],
                initial_size=test["initial_size"],
                append_size=test["append_size"],
                description=test["desc"],
                verify_data=True,
            )
            test_results.append(result)
            if result["passed"]:
                passed += 1
            else:
                failed += 1

        # =====================================================================
        # TEST GROUP 4: Small appends (sub-chunk)
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 4: Small appends (sub-chunk size)")
        log.info("-" * 50)

        small_append_tests = [
            {
                "name": "Append 512 bytes",
                "initial_size": stripe_width,
                "append_size": 512,
                "desc": "Sub-block append",
            },
            {
                "name": "Append 4KB",
                "initial_size": stripe_width,
                "append_size": 4096,
                "desc": "4KB append (common block size)",
            },
            {
                "name": "Append 1 byte",
                "initial_size": stripe_width,
                "append_size": 1,
                "desc": "Single byte append",
            },
        ]

        for test in small_append_tests:
            result = _run_single_append_test(
                rados_obj=rados_obj,
                pool_name=pool_name,
                test_name=test["name"],
                initial_size=test["initial_size"],
                append_size=test["append_size"],
                description=test["desc"],
                verify_data=True,
            )
            test_results.append(result)
            if result["passed"]:
                passed += 1
            else:
                failed += 1

        # =====================================================================
        # TEST GROUP 5: Sequential appends
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 5: Sequential appends on same object")
        log.info("-" * 50)

        result = _run_sequential_append_test(
            rados_obj=rados_obj,
            pool_name=pool_name,
            stripe_unit=stripe_unit,
            stripe_width=stripe_width,
        )
        test_results.append(result)
        if result["passed"]:
            passed += 1
        else:
            failed += 1

        # =====================================================================
        # TEST GROUP 6: Append after truncate
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 6: Append after truncate (shard version interaction)")
        log.info("-" * 50)

        result = _run_append_after_truncate_test(
            rados_obj=rados_obj,
            pool_name=pool_name,
            stripe_unit=stripe_unit,
            stripe_width=stripe_width,
        )
        test_results.append(result)
        if result["passed"]:
            passed += 1
        else:
            failed += 1

        # =====================================================================
        # TEST GROUP 7: Append after partial overwrite
        # =====================================================================
        log.info("-" * 50)
        log.info("TEST GROUP 7: Append after partial overwrite")
        log.info("-" * 50)

        result = _run_append_after_overwrite_test(
            rados_obj=rados_obj,
            pool_name=pool_name,
            stripe_unit=stripe_unit,
            stripe_width=stripe_width,
        )
        test_results.append(result)
        if result["passed"]:
            passed += 1
        else:
            failed += 1

        # =====================================================================
        # SUMMARY
        # =====================================================================
        log.info("=" * 70)
        log.info("EC APPEND TESTS SUMMARY")
        log.info("=" * 70)
        log.info(
            "Total tests: %s | Passed: %s | Failed: %s",
            len(test_results),
            passed,
            failed,
        )

        # Log failed tests
        failed_tests = [t for t in test_results if not t["passed"]]
        if failed_tests:
            log.error("FAILED TESTS:")
            for ft in failed_tests:
                log.error("  - %s: %s", ft["name"], ft.get("error", "Unknown error"))

        if failed > 0:
            log.error(
                "EC APPEND TESTS FAILED: %s/%s tests failed", failed, len(test_results)
            )
            return False

        log.info("EC APPEND TESTS PASSED: All %s tests passed", len(test_results))
        return True

    except Exception as e:
        log.error("EC append tests failed with exception: %s", e)
        log.exception(e)
        return False


def _run_single_append_test(
    rados_obj,
    pool_name: str,
    test_name: str,
    initial_size: int,
    append_size: int,
    description: str,
    verify_data: bool = True,
) -> Dict[str, Any]:
    """
    Run a single append test case.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        test_name: Name of the test
        initial_size: Initial object size in bytes
        append_size: Size of data to append in bytes
        description: Test description
        verify_data: Whether to verify data integrity after append

    Returns:
        Dictionary with test results: {"name", "passed", "error"}
    """
    result = {"name": test_name, "passed": False, "error": None}
    obj_name = f"append_test_{int(time.time() * 1000)}"
    initial_file = f"/tmp/{obj_name}_initial.bin"
    append_file = f"/tmp/{obj_name}_append.bin"
    read_file = f"/tmp/{obj_name}_read.bin"
    combined_file = f"/tmp/{obj_name}_combined.bin"

    try:
        log.info("Test: %s", test_name)
        log.info("  Description: %s", description)
        log.info(
            "  Initial size: %s bytes, Append size: %s bytes, Final size: %s bytes",
            initial_size,
            append_size,
            initial_size + append_size,
        )

        # Step 1: Create initial data file
        cmd = f"dd if=/dev/urandom of={initial_file} bs={initial_size} count=1 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

        # Step 2: Create append data file
        cmd = (
            f"dd if=/dev/urandom of={append_file} bs={append_size} count=1 2>/dev/null"
        )
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

        # Step 3: Create combined reference file for verification
        if verify_data:
            cmd = f"cat {initial_file} {append_file} > {combined_file}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

        # Step 4: Upload initial object to pool
        cmd = f"rados -p {pool_name} put {obj_name} {initial_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.debug("  Created initial object %s with %s bytes", obj_name, initial_size)

        # Step 5: Perform append - THIS IS THE CRITICAL OPERATION
        log.info(
            "  Executing: rados -p %s append %s %s", pool_name, obj_name, append_file
        )
        cmd = f"rados -p {pool_name} append {obj_name} {append_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  Append command succeeded")

        # Step 6: Verify final object size
        expected_size = initial_size + append_size
        size_check = validate_object_size(
            rados_obj=rados_obj,
            pool_name=pool_name,
            obj_name=obj_name,
            expected_size=expected_size,
            operation="append",
        )
        if not size_check["success"]:
            result["error"] = size_check["error"]
            return result

        # Step 7: Verify data integrity
        if verify_data:
            cmd = f"rados -p {pool_name} get {obj_name} {read_file}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            # Compare with combined reference
            cmd = f"cmp {combined_file} {read_file}"
            cmp_output, _ = rados_obj.client.exec_command(
                cmd=cmd, sudo=True, check_ec=False
            )

            if cmp_output.strip() == "":
                log.info("  Data integrity verified: content matches")
            else:
                log.error("  Data integrity FAILED: content mismatch")
                result["error"] = "Data integrity verification failed"
                return result

        result["passed"] = True
        log.info("  PASSED: %s", test_name)

    except Exception as e:
        log.error("  FAILED: %s - %s", test_name, e)
        result["error"] = str(e)

    finally:
        # Cleanup
        try:
            rados_obj.client.exec_command(
                cmd=f"rm -f {initial_file} {append_file} {read_file} {combined_file}",
                sudo=True,
                check_ec=False,
            )
        except Exception:
            pass

    return result


def _run_sequential_append_test(
    rados_obj,
    pool_name: str,
    stripe_unit: int,
    stripe_width: int,
) -> Dict[str, Any]:
    """
    Test sequential appends on the same object.

    Creates object and performs multiple appends:
    Initial (chunk/2) -> +chunk/2 -> +chunk -> +chunk -> +stripe
    Tests cumulative growth through various boundaries.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        stripe_unit: EC chunk size
        stripe_width: Stripe width (k * stripe_unit)

    Returns:
        Dictionary with test results
    """
    result = {"name": "Sequential appends", "passed": False, "error": None}
    obj_name = f"seq_append_test_{int(time.time() * 1000)}"
    data_file = f"/tmp/{obj_name}_data.bin"

    try:
        log.info("Test: Sequential appends on same object")

        # Initial object size
        initial_size = stripe_unit // 2
        log.info("  Initial size: %s bytes (half chunk)", initial_size)

        # Create and upload initial object
        cmd = f"dd if=/dev/urandom of={data_file} bs={initial_size} count=1 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        cmd = f"rados -p {pool_name} put {obj_name} {data_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

        current_size = initial_size

        # Sequential appends with different sizes
        # Note: For k=2, 1 stripe = 2 chunks = stripe_width = 2 * stripe_unit
        append_sequence = [
            (stripe_unit // 2, "reach 1 chunk boundary"),
            (stripe_unit, "reach 1 stripe boundary (2 chunks for k=2)"),
            (stripe_unit, "cross stripe boundary (3 chunks = 1.5 stripes for k=2)"),
            (stripe_width, "reach complete stripes"),
        ]

        for i, (append_size, desc) in enumerate(append_sequence, 1):
            # Create append data
            append_file = f"/tmp/{obj_name}_append_{i}.bin"
            cmd = f"dd if=/dev/urandom of={append_file} bs={append_size} count=1 2>/dev/null"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            log.info(
                "  Step %s: Append %s bytes (%s) - current: %s -> new: %s",
                i,
                append_size,
                desc,
                current_size,
                current_size + append_size,
            )

            cmd = f"rados -p {pool_name} append {obj_name} {append_file}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            current_size += append_size
            log.info("    Append succeeded, new size: %s bytes", current_size)

            # Verify size after each append
            size_check = validate_object_size(
                rados_obj=rados_obj,
                pool_name=pool_name,
                obj_name=obj_name,
                expected_size=current_size,
                operation="append",
                step_info=f"Step {i}",
            )
            if not size_check["success"]:
                result["error"] = size_check["error"]
                return result

            # Cleanup temp append file
            rados_obj.client.exec_command(
                cmd=f"rm -f {append_file}", sudo=True, check_ec=False
            )

        result["passed"] = True
        log.info("  PASSED: Sequential appends")

    except Exception as e:
        log.error("  FAILED: Sequential appends - %s", e)
        result["error"] = str(e)

    finally:
        # Cleanup
        try:
            rados_obj.client.exec_command(
                cmd=f"rm -f {data_file} /tmp/{obj_name}_append_*.bin",
                sudo=True,
                check_ec=False,
            )
        except Exception:
            pass

    return result


def _run_append_after_truncate_test(
    rados_obj,
    pool_name: str,
    stripe_unit: int,
    stripe_width: int,
) -> Dict[str, Any]:
    """
    Test append after truncate operations.

    This tests the interaction between truncation (which may affect shard
    versions) and subsequent append operations.

    Workflow:
    1. Create 2-stripe object
    2. Truncate to 1 stripe
    3. Append to reach 1.5 stripes
    4. Append to reach 2 stripes (exact boundary)

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        stripe_unit: EC chunk size
        stripe_width: Stripe width (k * stripe_unit)

    Returns:
        Dictionary with test results
    """
    result = {"name": "Append after truncate", "passed": False, "error": None}
    obj_name = f"append_trunc_test_{int(time.time() * 1000)}"
    data_file = f"/tmp/{obj_name}_data.bin"
    append_file = f"/tmp/{obj_name}_append.bin"

    try:
        log.info("Test: Append after truncate")
        initial_size = stripe_width * 2
        log.info("  Initial size: %s bytes (2 stripes)", initial_size)

        # Step 1: Create initial object
        cmd = f"dd if=/dev/urandom of={data_file} bs={initial_size} count=1 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        cmd = f"rados -p {pool_name} put {obj_name} {data_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  Created base object (2 stripes)")

        # Step 2: Truncate to 1 stripe
        truncate_to = stripe_width
        log.info("  Truncating to %s bytes (1 stripe)", truncate_to)
        cmd = f"rados -p {pool_name} truncate {obj_name} {truncate_to}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  Truncate succeeded")

        # Step 3: Append to reach 1.5 stripes
        append_size_1 = stripe_width // 2
        cmd = f"dd if=/dev/urandom of={append_file} bs={append_size_1} count=1 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  Appending %s bytes (to reach 1.5 stripes)", append_size_1)

        cmd = f"rados -p {pool_name} append {obj_name} {append_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  First append after truncate succeeded")

        # Step 4: Append to reach exactly 2 stripes (boundary)
        append_size_2 = stripe_width // 2
        cmd = f"dd if=/dev/urandom of={append_file} bs={append_size_2} count=1 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  Appending %s bytes (to reach 2 stripe boundary)", append_size_2)

        cmd = f"rados -p {pool_name} append {obj_name} {append_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  Second append succeeded (reached 2 stripe boundary)")

        # Verify final size
        expected_final = stripe_width + append_size_1 + append_size_2
        size_check = validate_object_size(
            rados_obj=rados_obj,
            pool_name=pool_name,
            obj_name=obj_name,
            expected_size=expected_final,
            operation="append after truncate",
        )
        if not size_check["success"]:
            result["error"] = size_check["error"]
            return result

        result["passed"] = True
        log.info("  PASSED: Append after truncate")

    except Exception as e:
        log.error("  FAILED: Append after truncate - %s", e)
        result["error"] = str(e)

    finally:
        # Cleanup
        try:
            rados_obj.client.exec_command(
                cmd=f"rm -f {data_file} {append_file}",
                sudo=True,
                check_ec=False,
            )
        except Exception:
            pass

    return result


def _run_append_after_overwrite_test(
    rados_obj,
    pool_name: str,
    stripe_unit: int,
    stripe_width: int,
) -> Dict[str, Any]:
    """
    Test append after partial overwrite operations.

    This tests the interaction between partial writes (which may leave
    shards with different versions via PWLC) and subsequent append operations.

    Workflow:
    1. Create 2-stripe object
    2. Partial overwrite at chunk boundary (updates subset of shards)
    3. Append to reach 3 stripes (exact boundary)

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        stripe_unit: EC chunk size
        stripe_width: Stripe width (k * stripe_unit)

    Returns:
        Dictionary with test results
    """
    result = {"name": "Append after partial overwrite", "passed": False, "error": None}
    obj_name = f"append_ow_test_{int(time.time() * 1000)}"
    data_file = f"/tmp/{obj_name}_data.bin"
    partial_file = f"/tmp/{obj_name}_partial.bin"
    append_file = f"/tmp/{obj_name}_append.bin"

    try:
        log.info("Test: Append after partial overwrite")
        initial_size = stripe_width * 2
        log.info("  Initial size: %s bytes (2 stripes)", initial_size)

        # Step 1: Create initial object
        cmd = f"dd if=/dev/urandom of={data_file} bs={initial_size} count=1 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        cmd = f"rados -p {pool_name} put {obj_name} {data_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  Created base object")

        # Step 2: Partial overwrite at chunk boundary
        partial_size = stripe_unit
        partial_offset = stripe_unit
        cmd = f"dd if=/dev/urandom of={partial_file} bs={partial_size} count=1 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        cmd = f"rados -p {pool_name} put {obj_name} {partial_file} --offset {partial_offset}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info(
            "  Performed partial overwrite: %s bytes at offset %s",
            partial_size,
            partial_offset,
        )

        # Step 3: Append to reach 3 stripe boundary
        append_size = stripe_width
        cmd = (
            f"dd if=/dev/urandom of={append_file} bs={append_size} count=1 2>/dev/null"
        )
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  Appending %s bytes (to reach 3 stripe boundary)", append_size)

        cmd = f"rados -p {pool_name} append {obj_name} {append_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  Append after partial overwrite succeeded")

        # Verify final size
        expected_final = initial_size + append_size
        size_check = validate_object_size(
            rados_obj=rados_obj,
            pool_name=pool_name,
            obj_name=obj_name,
            expected_size=expected_final,
            operation="append after overwrite",
        )
        if not size_check["success"]:
            result["error"] = size_check["error"]
            return result

        # Verify we can read the object
        read_file = f"/tmp/{obj_name}_read.bin"
        cmd = f"rados -p {pool_name} get {obj_name} {read_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("  Successfully read object after partial overwrite + append")

        result["passed"] = True
        log.info("  PASSED: Append after partial overwrite")

    except Exception as e:
        log.error("  FAILED: Append after partial overwrite - %s", e)
        result["error"] = str(e)

    finally:
        # Cleanup
        try:
            rados_obj.client.exec_command(
                cmd=f"rm -f {data_file} {partial_file} {append_file} /tmp/{obj_name}_read.bin",
                sudo=True,
                check_ec=False,
            )
        except Exception:
            pass

    return result


def validate_object_size(
    rados_obj,
    pool_name: str,
    obj_name: str,
    expected_size: int,
    operation: str = "operation",
    step_info: str = None,
) -> Dict[str, Any]:
    """
    Validate object size matches expected value using rados stat.

    This helper function fetches object stats and validates the size
    against the expected value, providing consistent error handling
    and logging across all truncate/append tests.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: Name of the pool containing the object
        obj_name: Name of the object to validate
        expected_size: Expected object size in bytes
        operation: Description of the operation (for logging), e.g., "truncate", "append"
        step_info: Optional step information for sequential operations, e.g., "step 1"

    Returns:
        Dictionary containing:
        - success (bool): True if size matches expected, False otherwise
        - actual_size (int or None): Actual object size, None if stat failed
        - error (str or None): Error message if validation failed, None on success
        - raw_output (str or None): Raw stat output for debugging
    """
    prefix = f"  {step_info}: " if step_info else "  "

    result = {
        "success": False,
        "actual_size": None,
        "error": None,
        "raw_output": None,
    }

    try:
        stat = rados_obj.get_object_stat(pool_name=pool_name, obj_name=obj_name)

        if not stat:
            error_msg = f"Failed to get object stat for {obj_name} in pool {pool_name}"
            log.error(f"{prefix}{error_msg}")
            result["error"] = error_msg
            return result

        result["raw_output"] = stat.get("raw_output")
        result["actual_size"] = stat.get("size")

        if result["actual_size"] is None:
            error_msg = f"Could not parse size from stat output after {operation}"
            log.error(f"{prefix}{error_msg}")
            result["error"] = error_msg
            return result

        if result["actual_size"] != expected_size:
            error_msg = (
                f"Size mismatch after {operation}: "
                f"expected {expected_size} bytes, got {result['actual_size']} bytes"
            )
            log.error(f"{prefix}{error_msg}")
            result["error"] = error_msg
            return result

        # Success
        log.info(
            f"{prefix}Size verified after {operation}: {result['actual_size']} bytes"
        )
        result["success"] = True
        return result

    except Exception as e:
        error_msg = f"Exception while validating object size: {e}"
        log.error(f"{prefix}{error_msg}")
        result["error"] = error_msg
        return result


def run_device_writes(
    rados_obj,
    device_path: str,
    stripe_unit: int,
    stripe_width: int,
) -> bool:
    """
    Run direct device write tests (unmounted device)

    Args:
        rados_obj: RadosOrchestrator object
        device_path: RBD block device path (e.g., /dev/rbd0) - UNMOUNTED
        stripe_unit: EC stripe_unit in bytes (chunk size per data shard)
        stripe_width: EC stripe width (k * stripe_unit) in bytes

    Returns:
        True on success, False on failure
    """
    try:
        stripe_unit_kb = stripe_unit // 1024
        stripe_kb = stripe_width // 1024
        half_stripe_unit_kb = stripe_unit_kb // 2
        cross_stripe_unit_kb = stripe_unit_kb + half_stripe_unit_kb

        # Define device write test patterns
        device_patterns = [
            {
                "block_size": f"{stripe_kb}K",
                "count": 100,
                "seek": None,
                "desc": f"Full stripe writes ({stripe_kb}KB)",
            },
            {
                "block_size": f"{stripe_unit_kb}K",
                "count": 200,
                "seek": None,
                "desc": f"Single chunk writes ({stripe_unit_kb}KB)",
            },
            {
                "block_size": f"{half_stripe_unit_kb}K",
                "count": 500,
                "seek": None,
                "desc": f"Sub-chunk writes ({half_stripe_unit_kb}KB)",
            },
            {
                "block_size": f"{cross_stripe_unit_kb}K",
                "count": 100,
                "seek": None,
                "desc": f"Cross-chunk writes ({cross_stripe_unit_kb}KB)",
            },
            {
                "block_size": f"{stripe_unit_kb}K",
                "count": 100,
                "seek": 0,
                "desc": f"Overwrite at offset 0 ({stripe_unit_kb}KB)",
            },
            {
                "block_size": f"{half_stripe_unit_kb}K",
                "count": 50,
                "seek": 100,
                "desc": f"Overwrite at offset 100 ({half_stripe_unit_kb}KB)",
            },
            {
                "block_size": f"{stripe_unit_kb}K",
                "count": 50,
                "seek": 200,
                "desc": f"Overwrite at offset 200 ({stripe_unit_kb}KB)",
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
    stripe_unit: int,
) -> bool:
    """
    Run filesystem write tests (mounted device)

    Tests buffered I/O through filesystem layer.
    Device must be mounted with a filesystem.

    Args:
        rados_obj: RadosOrchestrator object
        mount_path: Mounted filesystem path (e.g., /tmp/ec_test_mount/)
        stripe_unit: EC stripe_unit in bytes (chunk size per data shard)

    Returns:
        True on success, False on failure
    """
    try:
        stripe_unit_kb = stripe_unit // 1024
        half_stripe_unit_kb = stripe_unit_kb // 2

        # Define all RBD write test patterns
        write_patterns = [
            # Filesystem writes (buffered I/O) - Initial writes
            {
                "target": "mount",
                "block_size": f"{stripe_unit_kb}K",
                "count": 200,
                "seek": None,
                "test_name": "Filesystem: Chunk Writes",
                "desc": f"Filesystem: Chunk-aligned writes ({stripe_unit_kb}KB)",
            },
            {
                "target": "mount",
                "block_size": f"{half_stripe_unit_kb}K",
                "count": 300,
                "seek": None,
                "test_name": "Filesystem: Sub-chunk Writes",
                "desc": f"Filesystem: Sub-chunk writes ({half_stripe_unit_kb}KB)",
            },
            # Filesystem overwrites (testing buffered overwrites)
            {
                "target": "mount",
                "block_size": f"{stripe_unit_kb}K",
                "count": 50,
                "seek": 0,
                "test_name": "Filesystem: Overwrite at offset 0",
                "desc": f"Filesystem: Overwrite at offset 0 ({stripe_unit_kb}KB)",
            },
            {
                "target": "mount",
                "block_size": f"{half_stripe_unit_kb}K",
                "count": 50,
                "seek": 100,
                "test_name": "Filesystem: Overwrite at offset 100",
                "desc": f"Filesystem: Overwrite at block 100 ({half_stripe_unit_kb}KB)",
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
    stripe_unit: int,
    ec_k: int,
    mon_config_obj=None,
    cephadm=None,
) -> Dict[str, Any]:
    """
    Test EC partial reads with various offset patterns using rados get

    Tests stripe-aligned and cross-stripe reads to verify EC read optimization.
    Note: rados get --offset X reads from offset X to END of object

    Validates:
    1. Read size correctness
    2. Data integrity (byte-by-byte comparison)

    This function also:
    - Identifies which PG/OSDs the read test object maps to
    - Enables debug logging on the PRIMARY OSD only (where read optimization happens)
    - Captures timestamps for log analysis
    - Disables debug logging after tests

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        stripe_unit: EC stripe_unit in bytes (chunk size per data shard)
        ec_k: Number of data chunks
        mon_config_obj: MonConfigMethods object for debug logging (optional)
        cephadm: CephAdmin object for timestamps (optional)

    Returns:
        Dictionary with keys:
        - success: bool
        - target_osds: List[int] (contains only primary OSD)
        - start_time: str
        - end_time: str
    """
    result = {
        "success": False,
        "target_osds": [],
        "start_time": "",
        "end_time": "",
    }

    try:
        log.info("EC Partial Read Tests - Testing offset-based reads")

        # Calculate stripe width
        stripe_width = ec_k * stripe_unit
        test_data_size = 2 * stripe_width  # 2 stripes

        log.info(
            "EC config: k=%s, stripe_unit=%sKB, stripe_width=%sKB",
            ec_k,
            stripe_unit // 1024,
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

        # Identify which OSDs this object maps to
        log.info("Identifying target OSDs for read test object '%s'", obj_name)
        osd_map_output = rados_obj.get_osd_map(pool=pool_name, obj=obj_name)
        pg_id = osd_map_output.get("pgid")
        acting_set = osd_map_output.get("acting", [])

        if not acting_set:
            log.error("Failed to identify acting set for read test object")
            return result

        primary_osd = acting_set[0]
        read_target_osds = [primary_osd]  # Only check primary OSD for read optimization

        log.info(
            "Read test object '%s' maps to PG %s, Acting set: %s (Primary: OSD.%s)",
            obj_name,
            pg_id,
            acting_set,
            primary_osd,
        )
        log.info(
            "Will analyze read optimization logs from PRIMARY OSD.%s only",
            primary_osd,
        )
        result["target_osds"] = read_target_osds

        # Enable debug logging
        log.info(
            "Enabling debug logging (debug_osd=20) on primary OSD: %s",
            primary_osd,
        )
        for osd_id in read_target_osds:
            mon_config_obj.set_config(
                section=f"osd.{osd_id}",
                name="debug_osd",
                value="20/20",
            )
        time.sleep(5)

        # Capture start time
        start_time_out, _ = cephadm.shell(
            args=["date", "-u", "'+%Y-%m-%dT%H:%M:%S.%3N+0000'"]
        )
        result["start_time"] = start_time_out.strip().strip("'")
        log.info("Read test start time: %s", result["start_time"])

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
                "offset": stripe_unit,
                "desc": "Read from chunk alignment",
            },
            {
                "name": "Mid-chunk",
                "offset": stripe_unit // 2,
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
                size_output, _ = rados_obj.client.exec_command(cmd=check_cmd, sudo=True)
                actual_size = int(size_output.strip())
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
                    cmp_output, _ = rados_obj.client.exec_command(
                        cmd=cmd, sudo=True, check_ec=False
                    )

                    if cmp_output.strip() == "":
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

        # Capture end time
        end_time_out, _ = cephadm.shell(
            args=["date", "-u", "'+%Y-%m-%dT%H:%M:%S.%3N+0000'"]
        )
        result["end_time"] = end_time_out.strip().strip("'")
        log.info("Read test end time: %s", result["end_time"])

        # Disable debug logging
        if result["target_osds"]:
            log.info(
                "Disabling debug logging on primary OSD: %s", result["target_osds"][0]
            )
            for osd_id in result["target_osds"]:
                mon_config_obj.remove_config(section=f"osd.{osd_id}", name="debug_osd")
        time.sleep(2)

        # Cleanup temp files only (keep test object in pool until final cleanup)
        log.debug("Cleaning up temp files")
        rados_obj.client.exec_command(
            cmd=f"rm -f {test_data_file}", sudo=True, check_ec=False
        )

        log.info("EC Read Offset Tests Summary:")
        log.info(
            "  Total: %s | Passed: %s | Failed: %s", len(test_cases), passed, failed
        )

        if failed == 0:
            log.info("All EC read offset tests PASSED")
            result["success"] = True
            return result
        else:
            log.error(" %s/%s test(s) FAILED", failed, len(test_cases))
            result["success"] = False
            return result

    except Exception as e:
        log.error("EC read offset tests failed with exception: %s", e)
        log.exception(e)
        # Clean up debug logging on exception
        if mon_config_obj and result.get("target_osds"):
            for osd_id in result["target_osds"]:
                mon_config_obj.remove_config(section=f"osd.{osd_id}", name="debug_osd")
        return result


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
            matched_write_lines = []  # Collect all matched lines

            # Count total log lines captured
            log_lines_list = log_content.splitlines() if log_content else []
            log_lines_captured = len(log_lines_list)

            log.debug("Parsing %s lines for OSD.%s", log_lines_captured, osd_id)

            for line in log_lines_list:
                # Look for written={...} patterns
                written_match = re.search(r"written=\{([0-9,]*)\}", line)
                if written_match:
                    matched_write_lines.append(line)  # Collect matched line
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

            # Print all matched write lines
            if matched_write_lines:
                log.debug("Matched WRITE lines in OSD.%s logs:", osd_id)
                for matched_line in matched_write_lines:
                    log.debug("  %s", matched_line)

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


def parse_ec_read_optimization_logs(
    rados_obj,
    target_osds: List[int],
    start_time: str,
    end_time: str,
) -> Dict[int, Dict[str, Any]]:
    """
    Parse OSD logs to extract EC read optimization evidence

    Looks for patterns indicating partial reads:
    - shard_want_to_read={...} - which shards were requested
    - shard_reads={...} - actual shard read operations
    - to_read=[offset,length,flags] - read request details
    - extents=[[...]] - byte ranges read from shards

    Args:
        rados_obj: RadosOrchestrator object
        target_osds: List of OSD IDs to analyze
        start_time: Start time for log analysis
        end_time: End time for log analysis

    Returns:
        Dictionary with OSD read statistics
    """
    results = {}

    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    for osd_id in target_osds:
        try:
            log.info("Analyzing READ logs for OSD.%s", osd_id)

            # Find which node hosts this OSD
            cmd = f"ceph osd find {osd_id}"
            osd_info = rados_obj.run_ceph_command(cmd=cmd)
            osd_host = osd_info.get("host")
            host_obj = rados_obj.get_host_object(hostname=osd_host)
            log_path = f"/var/log/ceph/{fsid}/ceph-osd.{osd_id}.log"
            temp_log = f"/tmp/osd_{osd_id}_ec_read_test.log"

            # Extract date portion for filtering
            start_date = start_time.split("T")[0]
            end_date = end_time.split("T")[0]

            log.debug(
                "Extracting READ log entries for OSD.%s in time range %s to %s",
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

            log.debug("Proceeding to Read and parse READ log")
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

            # Parse for EC read optimization patterns
            shard_want_to_read_patterns = defaultdict(int)
            partial_read_count = 0
            full_read_count = 0
            extent_based_reads = 0
            read_op_count = 0
            matched_read_lines = []  # Collect all matched lines

            # Count total log lines captured
            log_lines_list = log_content.splitlines() if log_content else []
            log_lines_captured = len(log_lines_list)

            log.debug(
                "Parsing %s lines for READ patterns on OSD.%s",
                log_lines_captured,
                osd_id,
            )

            for line in log_lines_list:
                # Look for ReadOp or start_read_op to count total read operations
                if "start_read_op" in line or "ReadOp(tid=" in line:
                    read_op_count += 1

                # Look for shard_want_to_read patterns (indicates which shards are requested)
                # Example: shard_want_to_read={0:[0~4194304]}
                shard_want_match = re.search(r"shard_want_to_read=\{([^}]*)\}", line)
                if shard_want_match:
                    matched_read_lines.append(line)  # Collect matched line
                    shard_want_str = shard_want_match.group(1)
                    if shard_want_str:  # Non-empty
                        # Count number of shards in the pattern
                        # Format: 0:[0~4194304],1:[0~4194304] or just 0:[0~4194304]
                        num_shards = len(re.findall(r"\d+:\[", shard_want_str))
                        shard_want_to_read_patterns[num_shards] += 1

                        if num_shards == 1:
                            partial_read_count += 1
                        else:
                            full_read_count += 1

                # Look for extent-based reads in shard_reads
                # Example: shard_reads={0:shard_read_t(extents=[[0~4194304]], subchunk=--, pg_shard=12(0))}
                if "extents=[[" in line:
                    extent_based_reads += 1

            results[osd_id] = {
                "shard_want_to_read_patterns": dict(shard_want_to_read_patterns),
                "partial_read_count": partial_read_count,  # Reading from single shard
                "full_read_count": full_read_count,  # Reading from multiple shards
                "extent_based_reads": extent_based_reads,
                "total_read_ops": read_op_count,
                "log_lines_captured": log_lines_captured,
            }

            # Cleanup temp files
            host_obj.exec_command(cmd=f"rm -f {temp_log}", sudo=True, check_ec=False)

            # Print all matched read lines
            if matched_read_lines:
                log.debug("Matched READ lines in OSD.%s logs:", osd_id)
                for matched_line in matched_read_lines:
                    log.debug("  %s", matched_line)

            # Log summary
            log.info("OSD.%s READ Analysis Results:", osd_id)
            log.info("  Log lines captured: %s", log_lines_captured)
            log.info("  Total read operations: %s", read_op_count)
            log.info("  Partial reads (single shard): %s", partial_read_count)
            log.info("  Full reads (multiple shards): %s", full_read_count)
            log.info("  Extent-based reads: %s", extent_based_reads)

            if shard_want_to_read_patterns:
                log.debug("  Shard read patterns breakdown:")
                for num_shards, count in sorted(shard_want_to_read_patterns.items()):
                    log.debug("    %s shard(s) read: %s occurrences", num_shards, count)

        except Exception as e:
            log.error("Failed to parse READ logs for OSD.%s: %s", osd_id, e)
            continue

    return results


def verify_partial_read_optimization(
    results: Dict[int, Dict[str, Any]], ec_k: int, ec_m: int
) -> bool:
    """
    Verify that partial read optimization is working

    With EC read optimization enabled:
    - Partial reads should request data from fewer shards (ideally 1 shard for aligned reads)
    - Should see extent-based reads with specific byte ranges
    - Should NOT always read from all k data shards for small reads

    Without optimization:
    - Reads would request full chunks from multiple shards
    - More read amplification

    Args:
        results: Dictionary of OSD read analysis results
        ec_k: Number of data chunks
        ec_m: Number of parity chunks

    Returns:
        True if optimization is verified, False otherwise
    """
    if not results:
        log.error("No OSD read log data to verify")
        return False

    total_shards = ec_k + ec_m
    log.info(
        "READ Verification: EC k=%s, m=%s, total_shards=%s", ec_k, ec_m, total_shards
    )

    # Collect statistics across all OSDs
    total_partial_reads = 0
    total_full_reads = 0
    total_extent_reads = 0
    total_read_ops = 0

    for osd_id, stats in results.items():
        total_partial_reads += stats.get("partial_read_count", 0)
        total_full_reads += stats.get("full_read_count", 0)
        total_extent_reads += stats.get("extent_based_reads", 0)
        total_read_ops += stats.get("total_read_ops", 0)

    log.info("READ Verification Results:")
    log.info("  Total read operations: %s", total_read_ops)
    log.info("  Partial reads (single shard): %s", total_partial_reads)
    log.info("  Full reads (multiple shards): %s", total_full_reads)
    log.info("  Extent-based reads: %s", total_extent_reads)

    # Calculate optimization ratio
    if total_partial_reads + total_full_reads > 0:
        optimization_ratio = (
            total_partial_reads / (total_partial_reads + total_full_reads) * 100
        )
        log.info("  Optimization ratio: %.2f%% (partial/total)", optimization_ratio)

    # Verification criteria
    verification_passed = False
    reasons = []

    if total_partial_reads > 0:
        verification_passed = True
        reasons.append(
            f"Found {total_partial_reads} partial read operations (single shard reads)"
        )

    if total_extent_reads > 0:
        verification_passed = True
        reasons.append(
            f"Found {total_extent_reads} extent-based reads with specific byte ranges"
        )

    if verification_passed:
        log.info("EC PARTIAL READ OPTIMIZATION VERIFIED - TEST PASSED")
        for reason in reasons:
            log.info("  %s", reason)
        return True
    else:
        if total_full_reads > 0:
            log.error(
                "Only full shard reads detected (%s) - Partial read optimization NOT working",
                total_full_reads,
            )
        else:
            log.error("No EC read patterns found in logs - Cannot verify optimization")
        return False
