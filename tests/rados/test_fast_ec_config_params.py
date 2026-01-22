"""
Module to verify scenarios related to Fast EC (Erasure Coding) configuration parameters and optimizations

This module tests the behavior of erasure-coded pools with various configurations including:
- Default EC optimization settings (osd_pool_default_flag_ec_optimizations)
- RBD images on EC pools with data operations
- Enabling/disabling allow_ec_optimizations at pool level
- Compression with EC pools
- Read/write operations with different EC optimization states
- Space savings verification for objects smaller than stripe_unit (zero-padding prevention)
"""

import json
import random
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test suite for Fast EC (Erasure Coding) configuration parameters and optimizations

    This test suite verifies the behavior of erasure-coded pools with various configurations
    and validates read/write operations with RBD images.

    Args:
        ceph_cluster: Ceph cluster object
        kw: Test configuration parameters

    Supported test scenarios:
        - test_fast_ec_optimization_params: Comprehensive verification of EC pool optimization
          parameters with default configurations, runtime changes, and RBD data operations
        - test_fast_ec_space_gain: Verifies space savings when EC optimizations prevent
          zero-padding for objects smaller than stripe_unit. Compares pool-level bytes_used
          and shard-level sizes using ceph-objectstore-tool.

    Test Requirements:
        - Ceph version 9.0 or above (squid+)
        - At least one client node
        - Sufficient OSDs for EC pool creation (k=2, m=2)
    """

    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config.get("rhbuild")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    cot_obj = objectstoreToolWorkflows(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]

    if config.get("test_fast_ec_optimization_params"):
        doc = (
            "\n# Test: Fast EC Optimizations"
            "\n Verify EC pool optimizations with default config and RBD operations"
            "\n Note: All write operations include read verification of all objects written so far"
            "\n\t 1. Verify osd_pool_default_flag_ec_optimizations is true by default at OSD and mon level"
            "\n\t 2. Create EC pool with RBD app, verify allow_ec_optimizations is true, "
            "attempt to set false (should fail)"
            "\n\t 3. Set osd_pool_default_flag_ec_optimizations to false at global level, verify at OSD and mon level"
            "\n\t 4. Create second EC pool, verify allow_ec_optimizations is false (due to global config)"
            "\n\t 5. Create RBD images on both pools and mount them on client"
            "\n\t 6. Write 10 files on both mounted filesystems and verify all reads"
            "\n\t 7. Enable fast EC features (allow_ec_optimizations) on 2nd pool"
            "\n\t 8. Write 10 more files (20 total) and verify all reads - validates EC optimization works"
            "\n\t 10. Enable compression (force mode with snappy) on both pools"
            "\n\t 11. Write 10 more files (30 total) and verify all reads - validates compression works"
            "\n\t     Verify compression stats show data is actually compressed and store baseline"
            "\n\t 12. Remove compression from both pools"
            "\n\t 13. Write 10 more files (40 total) and verify all reads - validates post-compression works"
            "\n\t     Compare compression stats: verify compress_bytes_used didn't increase by >25%"
            "\n\t     (ensures new data is NOT compressed while old compressed data remains)"
            "\n\t 14. Run pool sanity check (ignoring OSD_SCRUB_ERRORS and PG_DAMAGED)"
            "\n\t 15. Cleanup: Set osd_pool_default_flag_ec_optimizations to true, unmount/unmap images, delete pools"
        )

        log.info(doc)
        log.info("Running test to verify Fast EC optimizations")

        # Version check - test only applicable for 9.0 and above
        if float(rhbuild.split("-")[0]) < 9.0:
            log.info(
                "Test skipped: Fast EC optimizations feature is only available in 9.0 and above"
            )
            log.info("Current version: %s", rhbuild)
            return 0

        pool_name_1 = "test_ec_pool_1"
        pool_name_2 = "test_ec_pool_2"
        metadata_pool_name_1 = "test_metadata_pool_1"
        metadata_pool_name_2 = "test_metadata_pool_2"
        image_name_1 = "test_image_1"
        image_name_2 = "test_image_2"
        device_map_1 = None
        device_map_2 = None
        mount_path_1 = "/tmp/ec_test_mount_1/"
        mount_path_2 = "/tmp/ec_test_mount_2/"

        ec_config = {
            "pool_name": pool_name_1,
            "profile_name": "ec_profile_test",
            "k": 2,
            "m": 2,
            "app_name": "rbd",
            "erasure_code_use_overwrites": "true",
        }
        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        try:
            # Step 1: Check default value of osd_pool_default_flag_ec_optimizations
            log.info(
                "Step 1: Checking default value of osd_pool_default_flag_ec_optimizations"
            )

            # Verify config at both DB and runtime levels
            verify_config_db_and_runtime(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="osd_pool_default_flag_ec_optimizations",
                expected_value="true",
            )

            # Step 2: Create first EC pool with optimizations enabled (default)
            log.info("Step 2: Creating first EC pool and verifying optimizations")
            ec_config["pool_name"] = pool_name_1
            assert rados_obj.create_erasure_pool(**ec_config), (
                "Failed to create pool %s" % pool_name_1
            )
            log.info("Created EC pool: %s", pool_name_1)
            time.sleep(2)

            # Create metadata pool for first EC pool
            metadata_pool_name_1 = "test_metadata_pool_1"
            assert rados_obj.create_pool(pool_name=metadata_pool_name_1, app_name="rbd")
            log.info("Created metadata pool: %s", metadata_pool_name_1)

            # Check allow_ec_optimizations
            ec_opt_value_dict = rados_obj.get_pool_property(
                pool=pool_name_1, props="allow_ec_optimizations"
            )
            ec_opt_value = ec_opt_value_dict.get("allow_ec_optimizations")
            log.info("Pool %s - allow_ec_optimizations: %s ", pool_name_1, ec_opt_value)

            if ec_opt_value is not True:
                log.error("allow_ec_optimizations is not True for %s", pool_name_1)
                raise Exception("EC optimizations check failed for %s" % pool_name_1)

            # Try to set allow_ec_optimizations to false (should fail)
            log.info("Attempting to set allow_ec_optimizations to false (should fail)")
            try:
                rados_obj.set_pool_property(
                    pool=pool_name_1, props="allow_ec_optimizations", value="false"
                )
                log.error(
                    "Setting allow_ec_optimizations to false did not fail as expected!"
                )
                raise Exception(
                    "Expected failure did not occur when setting allow_ec_optimizations to false"
                )
            except Exception as e:
                if "Invalid argument" in str(e) or "EINVAL" in str(e):
                    log.info(
                        "Successfully verified: Cannot set allow_ec_optimizations to false on existing pool"
                    )
                else:
                    raise e

            # Step 3: Set osd_pool_default_flag_ec_optimizations to false at global level
            log.info(
                "Step 3: Setting osd_pool_default_flag_ec_optimizations to false at global level"
            )

            mon_obj.set_config(
                section="global",
                name="osd_pool_default_flag_ec_optimizations",
                value="false",
            )
            log.info(
                "Set osd_pool_default_flag_ec_optimizations to false at global level"
            )

            time.sleep(5)

            # Verify config at both DB and runtime levels
            verify_config_db_and_runtime(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="osd_pool_default_flag_ec_optimizations",
                expected_value="false",
            )

            # Step 4: Create second EC pool (should have optimizations disabled)
            log.info(
                "Step 4: Creating second EC pool with optimizations disabled at global level"
            )
            ec_config["pool_name"] = pool_name_2
            assert rados_obj.create_erasure_pool(**ec_config), (
                "Failed to create pool %s" % pool_name_2
            )
            log.info("Created EC pool: %s", pool_name_2)

            # Create metadata pool for second EC pool
            metadata_pool_name_2 = "test_metadata_pool_2"
            assert rados_obj.create_pool(pool_name=metadata_pool_name_2, app_name="rbd")
            log.info("Created metadata pool: %s", metadata_pool_name_2)

            time.sleep(2)

            # Check allow_ec_optimizations (should be false)
            ec_opt_value_2_dict = rados_obj.get_pool_property(
                pool=pool_name_2, props="allow_ec_optimizations"
            )
            ec_opt_value_2 = ec_opt_value_2_dict.get("allow_ec_optimizations")
            log.info(
                "Pool %s - allow_ec_optimizations: %s (type: %s)",
                pool_name_2,
                ec_opt_value_2,
                type(ec_opt_value_2),
            )

            if ec_opt_value_2 is not False:
                log.error(
                    "allow_ec_optimizations is not False for %s, even when it's set to false at global level",
                    pool_name_2,
                )
                raise Exception("EC optimizations should be False for %s" % pool_name_2)

            # Step 5: Create RBD images on both pools and write objects
            log.info("Step 5: Creating RBD images on both pools and writing objects")

            # Create RBD image on first EC pool with metadata pool
            image_create_cmd_1 = (
                f"rbd create --size 1G --data-pool {pool_name_1} "
                f"{metadata_pool_name_1}/{image_name_1}"
            )
            client_node.exec_command(cmd=image_create_cmd_1, sudo=True)
            log.info(
                "Created RBD image %s with data-pool %s and metadata-pool %s",
                image_name_1,
                pool_name_1,
                metadata_pool_name_1,
            )

            # Create RBD image on second EC pool with metadata pool
            image_create_cmd_2 = (
                f"rbd create --size 1G --data-pool {pool_name_2} "
                f"{metadata_pool_name_2}/{image_name_2}"
            )
            client_node.exec_command(cmd=image_create_cmd_2, sudo=True)
            log.info(
                "Created RBD image %s with data-pool %s and metadata-pool %s",
                image_name_2,
                pool_name_2,
                metadata_pool_name_2,
            )

            # Mount first image
            mount_path_1, device_map_1 = rados_obj.mount_image_on_client(
                pool_name=metadata_pool_name_1,
                img_name=image_name_1,
                client_obj=client_node,
                mount_path="/tmp/ec_test_mount_1/",
            )
            log.info(
                "Mapped and mounted image %s to device %s at %s",
                image_name_1,
                device_map_1,
                mount_path_1,
            )

            # Mount second image
            mount_path_2, device_map_2 = rados_obj.mount_image_on_client(
                pool_name=metadata_pool_name_2,
                img_name=image_name_2,
                client_obj=client_node,
                mount_path="/tmp/ec_test_mount_2/",
            )
            log.info(
                "Mapped and mounted image %s to device %s at %s",
                image_name_2,
                device_map_2,
                mount_path_2,
            )

            # Step 6: Write 10 objects and verify read
            log.info("Step 6: Writing 10 objects to both RBD images and verifying")
            write_and_verify_objects(
                client_node, mount_path_1, mount_path_2, 0, 10, "Initial write"
            )

            # Step 7: Enable fast EC features on 2nd pool
            log.info("Step 7: Enabling fast EC features on 2nd pool")

            rados_obj.set_pool_property(
                pool=pool_name_2, props="allow_ec_optimizations", value="true"
            )
            log.info("Enabled allow_ec_optimizations on %s", pool_name_2)

            time.sleep(5)

            # Verify the change
            ec_opt_value_2_new_dict = rados_obj.get_pool_property(
                pool=pool_name_2, props="allow_ec_optimizations"
            )
            ec_opt_value_2_new = ec_opt_value_2_new_dict.get("allow_ec_optimizations")
            log.info(
                "Pool %s - allow_ec_optimizations after enabling: %s (type: %s)",
                pool_name_2,
                ec_opt_value_2_new,
                type(ec_opt_value_2_new),
            )

            if ec_opt_value_2_new is not True:
                log.error("Failed to enable allow_ec_optimizations for %s", pool_name_2)
                raise Exception("EC optimizations not enabled for %s" % pool_name_2)

            # Step 8: Write 10 more objects and verify all 20
            log.info(
                "Step 8: Writing 10 more objects and verifying all 20 "
                "(includes read verification after EC optimization)"
            )
            write_and_verify_objects(
                client_node, mount_path_1, mount_path_2, 10, 10, "Second write"
            )

            # Step 10: Enable compression on both pools
            log.info("Step 10: Enabling compression on both pools")

            rados_obj.set_pool_property(
                pool=pool_name_1, props="compression_mode", value="force"
            )
            rados_obj.set_pool_property(
                pool=pool_name_1, props="compression_algorithm", value="snappy"
            )
            log.info("Enabled compression on %s", pool_name_1)

            rados_obj.set_pool_property(
                pool=pool_name_2, props="compression_mode", value="force"
            )
            rados_obj.set_pool_property(
                pool=pool_name_2, props="compression_algorithm", value="snappy"
            )
            log.info("Enabled compression on %s", pool_name_2)

            time.sleep(5)

            # Step 11: Write 10 more objects with compression and verify all 30
            # This also verifies reading after enabling compression
            log.info(
                "Step 11: Writing 10 more objects with compression and verifying all 30 "
                "(includes read verification after compression enabled)"
            )
            write_and_verify_objects(
                client_node,
                mount_path_1,
                mount_path_2,
                20,
                10,
                "Third write with compression",
            )

            # Verify compression is working and store baseline stats
            log.info(
                "Verifying that data is actually compressed on both pools,"
                " sleeping for 20 seconds for stats to be updated"
            )
            time.sleep(20)  # Allow compression to complete

            # Store compression stats before disabling compression
            compression_stats_before = {}

            for pool_name in [pool_name_1, pool_name_2]:
                pool_stats = rados_obj.get_cephdf_stats(
                    pool_name=pool_name, detail=True
                )

                if not pool_stats:
                    log.error("Could not retrieve stats for pool %s", pool_name)
                    raise Exception("Failed to get stats for %s" % pool_name)

                stats = pool_stats.get("stats", {})
                compress_bytes = stats.get("compress_bytes_used", 0)
                compress_under_bytes = stats.get("compress_under_bytes", 0)

                # Store for later comparison
                compression_stats_before[pool_name] = {
                    "compress_bytes_used": compress_bytes,
                    "compress_under_bytes": compress_under_bytes,
                }

                log.info(
                    "Pool %s compression stats (baseline): compress_bytes_used=%s, compress_under_bytes=%s",
                    pool_name,
                    compress_bytes,
                    compress_under_bytes,
                )

                if compress_bytes <= 0:
                    log.error("No compressed data found in pool %s", pool_name)
                    raise Exception("Compression not working on %s" % pool_name)

            # Step 12: Remove compression
            log.info("Step 12: Removing compression from both pools")

            rados_obj.set_pool_property(
                pool=pool_name_1, props="compression_mode", value="none"
            )
            log.info("Disabled compression on %s", pool_name_1)

            rados_obj.set_pool_property(
                pool=pool_name_2, props="compression_mode", value="none"
            )
            log.info("Disabled compression on %s", pool_name_2)

            time.sleep(5)

            # Step 13: Write final 10 objects and verify all 40
            # This also verifies reading after removing compression
            log.info(
                "Step 13: Writing final 10 objects and verifying all 40 "
                "(includes read verification after compression removed)"
            )
            write_and_verify_objects(
                client_node, mount_path_1, mount_path_2, 30, 10, "Final write"
            )
            log.info("All verification steps completed successfully!")

            # Step 14: Cleanup preparation - pool sanity check
            # scrub errors seen on Pools where RBD init is performed before enabling EC enhancements
            if not rados_obj.run_pool_sanity_check(
                ignore_list=["OSD_SCRUB_ERRORS", "PG_DAMAGED"]
            ):
                log.error("Pool sanity checks failed on cluster")
                return 1

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )

            # Step 15: Cleanup
            log.info(
                "Step 15: Cleanup - Unmounting, unmapping images and deleting pools"
            )
            existing_pools = rados_obj.list_pools()

            # Unmount and unmap images
            if device_map_1:
                try:
                    # Unmount filesystem
                    umount_cmd_1 = f"umount {mount_path_1}"
                    client_node.exec_command(cmd=umount_cmd_1, sudo=True)
                    log.info("Unmounted %s from %s", image_name_1, mount_path_1)
                except Exception as e:
                    log.warning("Failed to unmount %s: %s", image_name_1, e)

                try:
                    # Unmap device
                    unmap_cmd_1 = f"rbd device unmap {device_map_1}"
                    client_node.exec_command(cmd=unmap_cmd_1, sudo=True)
                    log.info("Unmapped image %s from %s", image_name_1, device_map_1)
                except Exception as e:
                    log.warning("Failed to unmap %s: %s", device_map_1, e)

                try:
                    # Remove mount directory
                    rmdir_cmd_1 = f"rm -rf {mount_path_1}"
                    client_node.exec_command(cmd=rmdir_cmd_1, sudo=True)
                    log.info("Removed mount directory %s", mount_path_1)
                except Exception as e:
                    log.warning(
                        "Failed to remove mount directory %s: %s", mount_path_1, e
                    )

            if device_map_2:
                try:
                    # Unmount filesystem
                    umount_cmd_2 = f"umount {mount_path_2}"
                    client_node.exec_command(cmd=umount_cmd_2, sudo=True)
                    log.info("Unmounted %s from %s", image_name_2, mount_path_2)
                except Exception as e:
                    log.warning("Failed to unmount %s: %s", image_name_2, e)

                try:
                    # Unmap device
                    unmap_cmd_2 = f"rbd device unmap {device_map_2}"
                    client_node.exec_command(cmd=unmap_cmd_2, sudo=True)
                    log.info("Unmapped image %s from %s", image_name_2, device_map_2)
                except Exception as e:
                    log.warning("Failed to unmap %s: %s", device_map_2, e)

                try:
                    # Remove mount directory
                    rmdir_cmd_2 = f"rm -rf {mount_path_2}"
                    client_node.exec_command(cmd=rmdir_cmd_2, sudo=True)
                    log.info("Removed mount directory %s", mount_path_2)
                except Exception as e:
                    log.warning(
                        "Failed to remove mount directory %s: %s", mount_path_2, e
                    )

            if pool_name_1 in existing_pools:
                try:
                    rados_obj.delete_pool(pool=pool_name_1)
                    log.info("Deleted EC pool %s", pool_name_1)
                except Exception as e:
                    log.warning("Failed to delete pool %s: %s", pool_name_1, e)

            if pool_name_2 in existing_pools:
                try:
                    rados_obj.delete_pool(pool=pool_name_2)
                    log.info("Deleted EC pool %s", pool_name_2)
                except Exception as e:
                    log.warning("Failed to delete pool %s: %s", pool_name_2, e)

            if metadata_pool_name_1 in existing_pools:
                try:
                    rados_obj.delete_pool(pool=metadata_pool_name_1)
                    log.info("Deleted metadata pool %s", metadata_pool_name_1)
                except Exception as e:
                    log.warning("Failed to delete pool %s: %s", metadata_pool_name_1, e)

            if metadata_pool_name_2 in existing_pools:
                try:
                    rados_obj.delete_pool(pool=metadata_pool_name_2)
                    log.info("Deleted metadata pool %s", metadata_pool_name_2)
                except Exception as e:
                    log.warning("Failed to delete pool %s: %s", metadata_pool_name_2, e)

            # Reset osd_pool_default_flag_ec_optimizations to true
            try:
                mon_obj.remove_config(
                    section="global", name="osd_pool_default_flag_ec_optimizations"
                )
                log.info(
                    "Reset osd_pool_default_flag_ec_optimizations to default (true)"
                )
            except Exception as e:
                log.warning(
                    "Failed to reset osd_pool_default_flag_ec_optimizations: %s", e
                )

            # Log cluster health
            rados_obj.log_cluster_health()

            # Check for crashes after test execution
            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.debug(
                f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
            )
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("Verification of Fast EC optimizations completed successfully")
        return 0

    if config.get("test_fast_ec_optimization_params_partial_upgrade"):
        """
        Test suite for Fast EC optimizations during partial cluster upgrades

        This test verifies the behavior of EC optimization parameters when the cluster
        is in a partially upgraded state with mixed daemon versions.

        Test covers 5 upgrade scenarios:
        1. Few or all Mgr's in 9.x only
        2. Few or Mons in 9.x + Few or all Mgr's in 9.x only
        3. Few OSDs in 9.x + Few or Mons in 9.x + Few or all Mgr's in 9.x only
        4. All core daemons (Mon, Mgr, OSD) in 9.x - Tests EC pool with optimizations
        5. All core daemons (Mon, Mgr, OSD) in 9.x + NO MDS upgraded - Tests backward compatibility:
           NEW OSDs (9.x) with EC optimizations work with older MDS clients

        Note: When all core daemons are upgraded:
        - Case 4 ALWAYS runs to test EC pool functionality
        - Case 5 ONLY runs if NO MDS are upgraded to 9.x (validates backward compatibility)
        """
        log.info("Running test to verify Fast EC optimizations during partial upgrade")
        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        try:
            version_info = get_daemon_versions(rados_obj)
            log.info("Daemon version distribution:")
            for daemon_type, versions in version_info.items():
                log.info("  %s: %s", daemon_type, versions)

            # Identify the upgrade case
            upgrade_case = identify_upgrade_case(version_info)
            log.info("Identified upgrade case: %s", upgrade_case)

            # Execute tests based on upgrade case
            if upgrade_case == "case_1_mgr_only":
                log.info("\n=== Case 1: Few or all Mgr's in 9.x only ===")
                log.info("Expected: EC optimizations unavailable (only Mgrs in 9.x)")

                # Run common setup
                (
                    actual_osd_release,
                    expected_osd_release,
                    test_pool_name,
                    pool_result,
                ) = run_common_case_setup(rados_obj, version_info, upgrade_case)

                # Verify EC optimizations status
                verify_ec_optimizations_status(pool_result, None, "Case 1")

                # Verify require_osd_release
                verify_require_osd_release_match(
                    actual_osd_release,
                    expected_osd_release,
                    "Case 1",
                    "expected since OSDs not upgraded",
                )

                # Print summary
                print_case_summary(
                    "Case 1",
                    pool_result,
                    actual_osd_release,
                    expected_osd_release,
                    expected_ec_opt=None,
                )

                # Create staggered upgrade pools that will accumulate across upgrades
                create_staggered_upgrade_pools(rados_obj, client_node, upgrade_case)

            elif upgrade_case in ["case_2_mon_and_mgr", "case_3_osd_mon_mgr"]:
                # Case 2 and Case 3 have identical behavior
                case_desc = (
                    "Case 2" if upgrade_case == "case_2_mon_and_mgr" else "Case 3"
                )
                case_title = (
                    "Few or all Mons in 9.x + Few or all Mgr's in 9.x only"
                    if case_desc == "Case 2"
                    else "Few OSDs in 9.x + Few or Mons in 9.x + Few or all Mgr's in 9.x only"
                )

                log.info(f"\n=== {case_desc}: {case_title} ===")
                log.info(
                    "Expected: EC optimizations false (Mons upgraded but not all OSDs)"
                )

                # Run common setup
                (
                    actual_osd_release,
                    expected_osd_release,
                    test_pool_name,
                    pool_result,
                ) = run_common_case_setup(rados_obj, version_info, upgrade_case)

                # Verify EC optimizations status
                verify_ec_optimizations_status(pool_result, False, case_desc)

                log.info(f"{case_desc}: Verifying manual EC optimization enable fails")
                try:
                    rados_obj.set_pool_property(
                        pool=test_pool_name,
                        props="allow_ec_optimizations",
                        value="true",
                    )
                    raise Exception(
                        f"{case_desc}: ERROR - allow_ec_optimizations was enabled "
                        f"(should fail when OSDs not fully upgraded)"
                    )
                except Exception as e:
                    error_msg = str(e)
                    if "EINVAL" in error_msg or "Invalid argument" in error_msg:
                        log.info(
                            f"{case_desc}: Manual enable failed as expected - {error_msg}"
                        )
                    else:
                        log.error(f"{case_desc}: Unexpected error: {error_msg}")
                        raise

                # Verify require_osd_release
                verify_require_osd_release_match(
                    actual_osd_release,
                    expected_osd_release,
                    case_desc,
                    "expected since OSDs not upgraded",
                )

                # Verify osd_pool_default_flag_ec_optimizations (OSDs not upgraded)
                verify_osd_pool_default_flag(
                    mon_obj, rados_obj, case_desc, osds_upgraded=False
                )

                # Print summary
                print_case_summary(
                    case_desc,
                    pool_result,
                    actual_osd_release,
                    expected_osd_release,
                    expected_ec_opt=False,
                    additional_info=[
                        "Manual enable failed as expected (OSDs not fully upgraded)",
                        "osd_pool_default_flag_ec_optimizations: true (verified)",
                    ],
                )

                # Create staggered upgrade pools that will accumulate across upgrades
                create_staggered_upgrade_pools(rados_obj, client_node, upgrade_case)

            elif upgrade_case == "case_4_and_5_all_core_daemons":
                log.info("\n" + "=" * 80)
                log.info("All core daemons (Mon, Mgr, OSD) upgraded to 9.x")
                log.info("Running both:")
                log.info("  - Case 4: EC pool functionality test")
                log.info(
                    "  - Case 5: Backward compatibility test (NEW OSDs + Older MDS)"
                )
                log.info("=" * 80)

                # ===== CASE 4: EC Pool Test =====
                log.info("\n=== Case 4: Standard EC Pool Test ===")
                log.info("Expected: EC optimizations true (all core daemons upgraded)")

                # Run common setup
                (
                    actual_osd_release,
                    expected_osd_release,
                    test_pool_name,
                    pool_result,
                ) = run_common_case_setup(rados_obj, version_info, "case_4")

                # Verify EC optimizations status
                verify_ec_optimizations_status(pool_result, True, "Case 4")

                # Verify require_osd_release
                verify_require_osd_release_match(
                    actual_osd_release,
                    expected_osd_release,
                    "Case 4",
                    "expected since all OSDs upgraded to 9.x",
                )

                # Verify osd_pool_default_flag_ec_optimizations
                verify_osd_pool_default_flag(mon_obj, rados_obj, "Case 4")

                # Print summary
                print_case_summary(
                    "Case 4",
                    pool_result,
                    actual_osd_release,
                    expected_osd_release,
                    expected_ec_opt=True,
                    additional_info=[
                        "osd_pool_default_flag_ec_optimizations: true (verified)"
                    ],
                )

                # Create staggered upgrade pools that will accumulate across upgrades
                create_staggered_upgrade_pools(
                    rados_obj, client_node, "case_4_all_core_daemons"
                )

                # ===== CASE 5: Backward Compatibility Test =====
                log.info("\n=== Case 5: Backward Compatibility Test ===")

                # Check if any MDS are upgraded to 9.x - if yes, skip Case 5
                mds_9x = version_info.get("mds", {}).get("9.x", 0)
                mds_total = version_info.get("mds", {}).get("total", 0)

                if mds_9x > 0 and mds_total > 0:
                    log.info(
                        f"Skipping Case 5: {mds_9x}/{mds_total} MDS daemons are upgraded to 9.x"
                    )
                    log.info(
                        "Case 5 requires NO MDS to be upgraded (tests backward compatibility with older MDS)"
                    )
                    log.info("Case 5 test will not run in this configuration")
                else:
                    log.info(
                        "Testing NEW OSDs (9.x) with EC optimizations + Older MDS Clients"
                    )
                    log.info(
                        "Validates that EC optimizations in upgraded OSDs work correctly with older MDS versions"
                    )

                    # Define pool and filesystem names
                    ec_data_pool = "test_ec_data_pool_case5"
                    metadata_pool = "test_ec_meta_pool_case5"
                    fs_name = "cephfs_ec_case5"

                    try:
                        # Create EC pool with cephfs app
                        log.info("Creating EC data pool with cephfs app...")
                        pool_result = create_ec_pool_and_check_optimizations(
                            rados_obj=rados_obj,
                            pool_name=ec_data_pool,
                            app_name="cephfs",
                        )

                        # Verify pool creation and object writes
                        if not pool_result["pool_created"]:
                            raise Exception(
                                f"Case 5: Pool creation failed - {pool_result.get('error')}"
                            )
                        if not pool_result["can_write_objects"]:
                            raise Exception(
                                f"Case 5: Object write failed - {pool_result.get('error')}"
                            )
                        log.info("EC pool created and verified successfully")

                        # Set bulk flag and create metadata pool
                        log.info("Setting bulk flag and creating metadata pool...")
                        rados_obj.set_pool_property(
                            pool=ec_data_pool, props="bulk", value="true"
                        )
                        if not rados_obj.create_pool(
                            pool_name=metadata_pool, app_name="cephfs"
                        ):
                            raise Exception(
                                f"Failed to create metadata pool {metadata_pool}"
                            )

                        # Create CephFS
                        log.info("Creating CephFS...")
                        client_node.exec_command(
                            cmd=f"ceph fs new {fs_name} {metadata_pool} {ec_data_pool} --force",
                            sudo=True,
                        )
                        log.info(f"CephFS {fs_name} created successfully")

                        # # no need to deploy MDS daemons, Existing MDS daemons will be used
                        # log.info("Deploying MDS daemons...")
                        # all_nodes = ceph_cluster.get_nodes()
                        # mds_placement = f"1 {all_nodes[0].hostname}"

                        # client_node.exec_command(
                        #     cmd=f"ceph orch apply mds {fs_name} --placement='{mds_placement}'",
                        #     sudo=True,
                        # )
                        # time.sleep(30)  # Wait for MDS

                        mds_stat_out, _ = client_node.exec_command(
                            cmd=f"ceph fs status {fs_name}", sudo=True
                        )
                        log.info(f"MDS status:\n{mds_stat_out}")

                        # Test CephFS operations
                        log.info("Testing CephFS operations...")
                        cephfs_test_success = test_cephfs_with_ec_pool(
                            client_node, fs_name
                        )
                        if not cephfs_test_success:
                            raise Exception(
                                "CephFS operations failed - see logs for details"
                            )

                        # Print summary
                        log.info(
                            "\n=== Case 5: Backward Compatibility Test - PASSED ==="
                        )
                        log.info("Summary:")
                        log.info(f"  - EC data pool: {ec_data_pool}")
                        log.info(f"  - Metadata pool: {metadata_pool}")
                        log.info(f"  - CephFS: {fs_name}")
                        log.info(
                            "  - BACKWARD COMPATIBILITY VERIFIED: Older MDS clients successfully"
                        )
                        log.info(
                            "    operated on EC pools with new optimizations from upgraded OSDs"
                        )

                    except Exception as e:
                        log.error(f"Error in Case 5: {e}")
                        raise
                    finally:
                        # Cleanup Case 5 resources
                        log.info("Cleaning up Case 5 resources...")

                        # Fail and remove filesystem
                        try:
                            log.info(f"Failing filesystem {fs_name}...")
                            fs_fail_cmd = f"ceph fs fail {fs_name}"
                            client_node.exec_command(
                                cmd=fs_fail_cmd, sudo=True, check_ec=False
                            )
                            time.sleep(5)

                            log.info(f"Removing filesystem {fs_name}...")
                            fs_rm_cmd = f"ceph fs rm {fs_name} --yes-i-really-mean-it"
                            client_node.exec_command(
                                cmd=fs_rm_cmd, sudo=True, check_ec=False
                            )
                            log.info(f"Filesystem {fs_name} removed")
                        except Exception as e:
                            log.warning(f"Failed to remove filesystem: {e}")

                        # Delete pools
                        try:
                            log.info(f"Deleting EC data pool {ec_data_pool}...")
                            rados_obj.delete_pool(pool=ec_data_pool)
                            log.info(f"EC data pool {ec_data_pool} deleted")
                        except Exception as e:
                            log.warning(f"Failed to delete EC data pool: {e}")

                        try:
                            log.info(f"Deleting metadata pool {metadata_pool}...")
                            rados_obj.delete_pool(pool=metadata_pool)
                            log.info(f"Metadata pool {metadata_pool} deleted")
                        except Exception as e:
                            log.warning(f"Failed to delete metadata pool: {e}")

                        log.info("Case 5 cleanup completed")
            else:
                log.info(
                    "Cluster not in a partial upgrade state or unsupported configuration"
                )
                log.info(
                    "Test requires cluster to be in one of the 5 defined upgrade cases"
                )
                return 0

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Cleanup for partial upgrade test *************** \n \n"
            )

            rados_obj.rados_pool_cleanup()
            # Log cluster health
            rados_obj.log_cluster_health()

            # Check for crashes after test execution
            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.debug(
                f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
            )
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info(
            "Verification of Fast EC optimizations during partial upgrade completed successfully"
        )
        return 0

    if config.get("test_fast_ec_space_gain"):
        """
        Test: Fast EC Space Gain Verification

        Verifies that EC optimizations prevent zero-padding for objects smaller than
        the stripe_unit, resulting in significant space savings at both pool and shard levels.

        Test Steps:
        1. Disable global EC optimizations flag
        2. Create EC pool WITHOUT optimizations (test_ec_pool_without_optimizations)
        3. Create EC pool WITH optimizations enabled (test_ec_pool_with_optimizations)
        4. Write 100 identical 4KB objects to both pools using 'rados put'
        5. Wait for stats to stabilize and trigger deep-scrub
        6. Compare pool-level space usage (bytes_used from 'ceph df detail')
        7. Verify shard-level sizes using:
           - rados stat: Confirms logical object size (4KB) is same for both pools
           - ceph-objectstore-tool: Confirms actual shard size differs (16KB vs 4KB)

        EC Configuration (k=2, m=2, stripe_unit=16KB):
        - stripe_unit = 16KB (chunk size per data shard)
        - stripe_width = k * stripe_unit = 32KB
        - full stripe = (k + m) * stripe_unit = 64KB

        Expected Results:
        - Without optimizations: 4KB object padded to stripe_unit → shard = 16KB
        - With optimizations: 4KB object stored as-is → shard = 4KB
        - Pool-level space savings: ~80% (4x reduction)
        - Shard-level size ratio: 4x (16KB vs 4KB)

        Failure Conditions:
        - Pool with optimizations doesn't use less bytes than pool without
        - Space savings < 50% threshold
        - rados stat object size != 4096 bytes
        - ceph-objectstore-tool shard size: pool1 <= pool2
        """
        pool_name_1 = "test_ec_pool_without_optimizations"
        pool_name_2 = "test_ec_pool_with_optimizations"
        stripe_unit = (
            "16384"  # 16KB chunk size per data shard (recommended for Fast EC pools)
        )
        test_failed = False
        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        try:
            # Step 1: Disable global optimizations to create pool without EC optimizations
            log.info("Step 1: Disabling global EC optimizations")
            mon_obj.set_config(
                section="global",
                name="osd_pool_default_flag_ec_optimizations",
                value="false",
            )
            log.info(
                "Set osd_pool_default_flag_ec_optimizations to false at global level"
            )
            time.sleep(2)

            # Step 2: Create first EC pool (WITHOUT optimizations) with 16K stripe_unit
            log.info(
                "Step 2: Creating EC pool WITHOUT optimizations: %s (stripe_unit=%s)",
                pool_name_1,
                stripe_unit,
            )
            create_ec_pool_with_optimization_flag(
                rados_obj=rados_obj,
                pool_name=pool_name_1,
                stripe_unit=stripe_unit,
                allow_ec_optimizations=False,
            )

            # Step 3: Create second EC pool with 16K stripe_unit and enable optimizations
            log.info(
                "Step 3: Creating EC pool WITH optimizations: %s (stripe_unit=%s)",
                pool_name_2,
                stripe_unit,
            )
            create_ec_pool_with_optimization_flag(
                rados_obj=rados_obj,
                pool_name=pool_name_2,
                stripe_unit=stripe_unit,
                allow_ec_optimizations=True,
            )

            # Step 4: Write objects smaller than stripe width (16KB) to both pools
            # Writing 4KB objects to demonstrate zero-padding difference
            # Note: We use 'rados put' instead of 'rados bench' because rados bench
            # rounds up object sizes to stripe_width for EC pools.
            # Without optimizations: 4KB padded to 16KB stripe_unit = 4x storage overhead
            # With optimizations: 4KB stored as-is = normal 2x EC overhead
            log.info(
                "Step 4: Writing objects (4KB each, smaller than 16KB stripe width) to both pools"
            )
            num_objects = 100
            object_size_bytes = 4096  # 4KB objects, smaller than 16KB stripe_unit

            # Create a temporary 4KB file for rados put
            temp_file = "/tmp/test_4k_object.bin"
            log.info(
                "Creating temporary %d byte file: %s", object_size_bytes, temp_file
            )
            client_node.exec_command(
                cmd=f"dd if=/dev/urandom of={temp_file} bs={object_size_bytes} count=1",
                sudo=True,
            )

            # Write objects to pool_name_1 (WITHOUT optimizations)
            log.info(
                "Writing %d objects of size %d bytes to pool %s (WITHOUT optimizations) using rados put",
                num_objects,
                object_size_bytes,
                pool_name_1,
            )
            for i in range(num_objects):
                obj_name = f"test_object_{i:04d}"
                out, err = client_node.exec_command(
                    cmd=f"rados -p {pool_name_1} put {obj_name} {temp_file}",
                    sudo=True,
                    check_ec=False,
                )
            log.info("Completed writing %d objects to %s", num_objects, pool_name_1)

            # Write objects to pool_name_2 (WITH optimizations)
            log.info(
                "Writing %d objects of size %d bytes to pool %s (WITH optimizations) using rados put",
                num_objects,
                object_size_bytes,
                pool_name_2,
            )
            for i in range(num_objects):
                obj_name = f"test_object_{i:04d}"
                out, err = client_node.exec_command(
                    cmd=f"rados -p {pool_name_2} put {obj_name} {temp_file}",
                    sudo=True,
                    check_ec=False,
                )
            log.info("Completed writing %d objects to %s", num_objects, pool_name_2)

            # Step 5: Wait for stats to be updated
            log.info(
                "Step 5: Initiating scrubs/deep-scrubs and sleeping for pool stats to be updated..."
            )
            rados_obj.run_scrub(pool=pool_name_2)
            rados_obj.run_deep_scrub(pool=pool_name_2)
            rados_obj.run_scrub(pool=pool_name_1)
            rados_obj.run_deep_scrub(pool=pool_name_1)
            time.sleep(120)

            # Step 6: Get pool stats and compare space usage
            log.info("Step 6: Comparing space usage between pools")

            pool1_stats = rados_obj.get_cephdf_stats(pool_name=pool_name_1, detail=True)
            pool2_stats = rados_obj.get_cephdf_stats(pool_name=pool_name_2, detail=True)

            if not pool1_stats or not pool2_stats:
                log.error("Could not retrieve stats for pools")
                raise Exception("Failed to get pool stats for comparison")

            # Log full stats for debugging
            log.info("Pool 1 full stats: %s", pool1_stats.get("stats", {}))
            log.info("Pool 2 full stats: %s", pool2_stats.get("stats", {}))

            # Extract all relevant fields from ceph df detail
            # STORED = logical object size (what user wrote)
            # USED (bytes_used) = actual bytes on OSDs after EC encoding
            pools_info = {
                pool_name_1: {
                    "stats": pool1_stats.get("stats", {}),
                    "description": "WITHOUT optimizations - zeros appended",
                },
                pool_name_2: {
                    "stats": pool2_stats.get("stats", {}),
                    "description": "WITH optimizations - zeros NOT appended",
                },
            }

            # Extract stats for each pool
            for pool_name, pool_info in pools_info.items():
                stats = pool_info["stats"]
                pool_info["stored"] = stats.get("stored", 0)
                pool_info["stored_data"] = stats.get("stored_data", 0)
                pool_info["bytes_used"] = stats.get("bytes_used", 0)
                pool_info["data_bytes_used"] = stats.get("data_bytes_used", 0)
                pool_info["objects"] = stats.get("objects", 0)

            log.info("=" * 80)
            log.info("SPACE USAGE COMPARISON RESULTS")
            log.info("=" * 80)

            # Log stats for each pool
            for pool_name, pool_info in pools_info.items():
                log.info("Pool %s (%s):", pool_name, pool_info["description"])
                log.info("  - Objects: %s", pool_info["objects"])
                log.info("  - STORED (logical): %s bytes", pool_info["stored"])
                log.info("  - STORED DATA: %s bytes", pool_info["stored_data"])
                log.info("  - USED (actual on OSDs): %s bytes", pool_info["bytes_used"])
                log.info("  - DATA USED: %s bytes", pool_info["data_bytes_used"])
                log.info(
                    "  - Average USED bytes per object: %.2f",
                    (
                        pool_info["bytes_used"] / pool_info["objects"]
                        if pool_info["objects"] > 0
                        else 0
                    ),
                )
                log.info("")

            # Get values for comparison - access by pool name directly
            pool1_bytes_used = pools_info[pool_name_1]["bytes_used"]
            pool2_bytes_used = pools_info[pool_name_2]["bytes_used"]
            pool1_stored = pools_info[pool_name_1]["stored"]
            pool2_stored = pools_info[pool_name_2]["stored"]

            # Calculate space savings based on USED (actual OSD bytes)
            if pool1_bytes_used > 0 and pool2_bytes_used > 0:
                space_saved = pool1_bytes_used - pool2_bytes_used
                space_saved_percent = (space_saved / pool1_bytes_used) * 100

                log.info("SPACE SAVINGS ANALYSIS:")
                log.info(
                    "  - STORED (logical) is same: Pool1=%s, Pool2=%s",
                    pool1_stored,
                    pool2_stored,
                )
                log.info("  - USED bytes saved: %s bytes", space_saved)
                log.info("  - Percentage saved: %.2f%%", space_saved_percent)
                log.info("=" * 80)

                # note that this is not a officially provided formula, it is a guess based on the behavior of the code
                # And we just take into account the object size, not omap or metadata size associated with the object
                # Calculate expected space savings dynamically based on stripe_unit and object_size
                # Formula: expected_savings = (stripe_unit - object_size) / stripe_unit * 100
                # Without optimizations: object padded to stripe_unit (16KB)
                # With optimizations: object stored as-is (4KB)
                #
                # Example calculation:
                #   stripe_unit = 16KB (16384 bytes)
                #   object_size = 4KB (4096 bytes)
                #   expected_savings = (16384 - 4096) / 16384 * 100 = 75%
                #   savings_tolerance = 0.25 (25% leeway)
                #   min_savings_threshold = 75 * (1 - 0.25) = 75 * 0.75 = 56.25%
                #
                # Test passes if space_saved_percent >= min_savings_threshold (56.25%)
                stripe_unit_int = int(stripe_unit)
                expected_savings = (
                    (stripe_unit_int - object_size_bytes) / stripe_unit_int
                ) * 100
                savings_tolerance = 0.25  # 25% leeway
                min_savings_threshold = expected_savings * (1 - savings_tolerance)

                log.info("Expected space savings calculation:")
                log.info("  - stripe_unit: %d bytes", stripe_unit_int)
                log.info("  - object_size: %d bytes", object_size_bytes)
                log.info("  - Expected savings: %.2f%%", expected_savings)
                log.info(
                    "  - Minimum threshold (with %.0f%% leeway): %.2f%%",
                    savings_tolerance * 100,
                    min_savings_threshold,
                )

                if space_saved_percent < min_savings_threshold:
                    log.error(
                        "Space savings verification FAILED: %.2f%% (threshold: %.2f%%)",
                        space_saved_percent,
                        min_savings_threshold,
                    )
                    log.error(
                        "Pool1 (without optimizations) USED: %s bytes",
                        pool1_bytes_used,
                    )
                    log.error(
                        "Pool2 (with optimizations) USED: %s bytes",
                        pool2_bytes_used,
                    )
                    raise Exception(
                        f"Space savings verification failed: {space_saved_percent:.2f}% "
                        f"is below minimum threshold of {min_savings_threshold}%"
                    )

                log.info(
                    "SUCCESS: Pool with EC optimizations uses %s less bytes (%.2f%% savings)",
                    space_saved,
                    space_saved_percent,
                )
                log.info(
                    "This confirms that zeros are NOT appended to objects smaller than "
                    "stripe width when EC optimizations are enabled"
                )
            else:
                log.error(
                    "Pool stats show zero bytes_used - pool1: %s, pool2: %s",
                    pool1_bytes_used,
                    pool2_bytes_used,
                )
                raise Exception("Invalid pool stats - bytes_used is zero")

            # Step 7: Verify shard sizes using ceph-objectstore-tool
            # This provides direct evidence of zero-padding at the OSD shard level
            log.info("=" * 80)
            log.info("Step 7: Verifying shard sizes using ceph-objectstore-tool")
            log.info("=" * 80)

            shard_sizes = {}

            for pool_name in [pool_name_1, pool_name_2]:
                log.info("Checking shard size for pool: %s", pool_name)

                # Get list of objects in the pool using run_ceph_command (returns JSON)
                obj_list = rados_obj.run_ceph_command(cmd=f"rados -p {pool_name} ls")
                objects = [obj["name"] for obj in obj_list] if obj_list else []

                if not objects:
                    log.warning("No objects found in pool %s", pool_name)
                    continue

                # Select a random object for verification
                test_obj = random.choice(objects)
                log.info("Selected object for verification: %s", test_obj)
                log.info("Total objects in pool %s: %d", pool_name, len(objects))

                # Step 7a: Use rados stat to check object size
                try:
                    stat_out, _ = client_node.exec_command(
                        cmd=f"rados -p {pool_name} stat {test_obj}", sudo=True
                    )
                    log.debug("rados stat output for %s in %s:", test_obj, pool_name)
                    log.debug("  %s", stat_out.strip())
                    # Parse rados stat output: "pool_name/obj_name mtime timestamp, size N"
                    rados_stat_match = re.search(r"size\s+(\d+)", stat_out)
                    if rados_stat_match:
                        rados_obj_size = int(rados_stat_match.group(1))
                        log.info("  - rados stat object size: %d bytes", rados_obj_size)
                        if pool_name not in shard_sizes:
                            shard_sizes[pool_name] = {}
                        shard_sizes[pool_name]["rados_stat_size"] = rados_obj_size
                except Exception as stat_err:
                    log.warning(
                        "Could not run rados stat for %s: %s", test_obj, stat_err
                    )

                try:
                    # Get the object's location using get_osd_map()
                    log.info(
                        "Getting OSD map for object %s in pool %s", test_obj, pool_name
                    )
                    osd_map = rados_obj.get_osd_map(pool=pool_name, obj=test_obj)
                    log.info("OSD map result: %s", osd_map)

                    if not osd_map:
                        log.error("Could not get OSD map for object %s", test_obj)
                        continue

                    pgid = osd_map.get("pgid")
                    acting_osds = osd_map.get("acting", [])

                    log.info("OSD map: pgid=%s, acting OSDs=%s", pgid, acting_osds)

                    if not pgid or not acting_osds:
                        log.error("Missing pgid or acting OSDs for object %s", test_obj)
                        continue

                    # Use the FIRST DATA shard to check for zero-padding
                    # For EC k=2, m=2: acting_osds = [data0, data1, parity0, parity1]
                    # With EC optimizations, small objects may only have data on shard 0
                    # (other shards might be empty or have "No data available")
                    # So we check shard 0 for consistent comparison between both pools
                    shard_index = 0  # First data shard
                    osd_id = acting_osds[shard_index]
                    log.info(
                        "Using OSD %d (shard %d - first data shard) for ceph-objectstore-tool operations",
                        osd_id,
                        shard_index,
                    )

                    # List objects in the PG to get the full object ID
                    log.info("Listing objects in PG %s on OSD %d", pgid, osd_id)
                    obj_list_output = cot_obj.list_objects(osd_id=osd_id, pgid=pgid)
                    # Print all objects present on the OSD and PG
                    if obj_list_output:
                        obj_lines = obj_list_output.strip().split("\n")
                        log.info(
                            "Objects in PG %s on OSD %d (total: %d):",
                            pgid,
                            osd_id,
                            len(obj_lines),
                        )
                        for obj_line in obj_lines:
                            log.info("  %s", obj_line)
                    else:
                        log.warning("No objects found in PG %s on OSD %d", pgid, osd_id)

                    # Find the test object in the list and extract pgid with shard
                    obj_id = None
                    pgid_with_shard = None
                    for line in obj_list_output.strip().split("\n"):
                        if test_obj in line:
                            obj_id = line.strip()
                            # Extract pgid with shard from object listing
                            # Format: ["157.15s0",{"oid":"..."}]
                            pgid_shard_match = re.search(
                                r'\["(\d+\.[a-f0-9]+s\d+)"', line
                            )
                            if pgid_shard_match:
                                pgid_with_shard = pgid_shard_match.group(1)
                            break

                    if not obj_id:
                        log.error(
                            "Object %s not found in cot list for PG %s", test_obj, pgid
                        )
                        continue

                    log.info(
                        "Found object: %s (pgid_with_shard: %s)",
                        test_obj,
                        pgid_with_shard,
                    )

                    # Get object dump using ceph-objectstore-tool (returns JSON)
                    obj_dump = cot_obj.fetch_object_dump(osd_id=osd_id, obj=obj_id)
                    log.debug("Object dump for %s:\n%s", test_obj, obj_dump)

                    # Parse JSON and extract stat.size (actual shard size on disk)
                    obj_data = json.loads(obj_dump)
                    shard_size = obj_data.get("stat", {}).get("size")

                    if shard_size is not None:
                        log.info(
                            "Pool %s, object %s - stat.size (shard size on disk): %d bytes",
                            pool_name,
                            test_obj,
                            shard_size,
                        )
                        if pool_name not in shard_sizes:
                            shard_sizes[pool_name] = {}
                        shard_sizes[pool_name]["cot_stat_size"] = shard_size
                    else:
                        log.error("stat.size not found in object dump for %s", test_obj)

                except Exception as e:
                    log.error("Failed to get shard info for pool %s: %s", pool_name, e)
                    log.exception("Full exception traceback:")
                    continue

            # Explicitly fail if no objects/shard data found for either pool
            missing_objects_in_pools = [
                p for p in [pool_name_1, pool_name_2] if p not in shard_sizes
            ]
            if missing_objects_in_pools:
                log.error(
                    "No objects found or shard data unavailable for pools: %s",
                    missing_objects_in_pools,
                )
                raise Exception(
                    f"Test failed: No objects found or unable to get shard data for pools: {missing_objects_in_pools}"
                )

            # Compare shard sizes - verify the zero-padding difference
            log.info("=" * 80)
            log.info("SHARD/OBJECT SIZE COMPARISON RESULTS")
            log.info("=" * 80)

            # Verify rados stat results - logical object size should be same for both pools
            if pool_name_1 in shard_sizes and pool_name_2 in shard_sizes:
                pool1_rados = shard_sizes.get(pool_name_1, {}).get("rados_stat_size")
                pool2_rados = shard_sizes.get(pool_name_2, {}).get("rados_stat_size")

                if pool1_rados and pool2_rados:
                    log.info("RADOS STAT (logical object size):")
                    log.info(
                        "  Pool %s (WITHOUT optimizations): %s bytes",
                        pool_name_1,
                        pool1_rados,
                    )
                    log.info(
                        "  Pool %s (WITH optimizations): %s bytes",
                        pool_name_2,
                        pool2_rados,
                    )
                    # Verify object size matches expected (4KB = 4096 bytes)
                    if (
                        pool1_rados != object_size_bytes
                        or pool2_rados != object_size_bytes
                    ):
                        log.error(
                            "rados stat object size mismatch! Expected %d, got pool1=%d, pool2=%d",
                            object_size_bytes,
                            pool1_rados,
                            pool2_rados,
                        )
                        raise Exception(
                            f"rados stat verification failed: expected {object_size_bytes} bytes"
                        )
                    log.info(
                        "  SUCCESS: Both pools show correct logical object size (%d bytes)",
                        object_size_bytes,
                    )
                else:
                    log.error("Could not get rados stat sizes for both pools")
                    raise Exception("rados stat verification failed: missing data")
            else:
                log.error("shard_sizes missing data for one or both pools")
                raise Exception("Shard size verification failed: missing pool data")

            # Verify ceph-objectstore-tool results - shard sizes should differ
            pool1_shard = shard_sizes.get(pool_name_1, {}).get("cot_stat_size")
            pool2_shard = shard_sizes.get(pool_name_2, {}).get("cot_stat_size")

            if pool1_shard and pool2_shard:
                log.info("")
                log.info("CEPH-OBJECTSTORE-TOOL (first data shard stat.size):")
                log.info(
                    "  Pool %s (WITHOUT optimizations): %s bytes",
                    pool_name_1,
                    pool1_shard,
                )
                log.info(
                    "  Pool %s (WITH optimizations): %s bytes",
                    pool_name_2,
                    pool2_shard,
                )

                # Pre-compute expected shard sizes and ratio based on stripe_unit and object_size
                # Without optimizations: object is padded to stripe_unit
                # With optimizations: object is stored as-is (no padding)
                expected_pool1_shard = int(stripe_unit)  # 16KB (padded to stripe_unit)
                expected_pool2_shard = object_size_bytes  # 4KB (no padding)
                expected_ratio = expected_pool1_shard / expected_pool2_shard  # 4.0x

                log.info("  Expected shard sizes:")
                log.info(
                    "    - Pool1 (padded to stripe_unit): %d bytes",
                    expected_pool1_shard,
                )
                log.info("    - Pool2 (no padding): %d bytes", expected_pool2_shard)
                log.info("    - Expected ratio: %.2fx", expected_ratio)

                actual_ratio = pool1_shard / pool2_shard
                log.info("  Actual shard sizes:")
                log.info("    - Pool1: %d bytes", pool1_shard)
                log.info("    - Pool2: %d bytes", pool2_shard)
                log.info("    - Actual ratio: %.2fx", actual_ratio)

                # Verify actual ratio is close to expected (within 25% tolerance)
                ratio_tolerance = 0.25
                if (
                    abs(actual_ratio - expected_ratio) / expected_ratio
                    <= ratio_tolerance
                ):
                    log.info(
                        "  SUCCESS: Shard ratio %.2fx matches expected %.2fx!",
                        actual_ratio,
                        expected_ratio,
                    )
                    shard_ratio = actual_ratio
                else:
                    log.error(
                        "Shard ratio mismatch: actual=%.2fx, expected=%.2fx (tolerance=%.0f%%)",
                        actual_ratio,
                        expected_ratio,
                        ratio_tolerance * 100,
                    )
                    raise Exception(
                        f"ceph-objectstore-tool verification failed: "
                        f"shard ratio {actual_ratio:.2f}x doesn't match expected {expected_ratio:.2f}x"
                    )
            else:
                log.error(
                    "Could not get ceph-objectstore-tool shard sizes: pool1=%s, pool2=%s",
                    pool1_shard,
                    pool2_shard,
                )
                raise Exception(
                    "ceph-objectstore-tool verification failed: missing shard data"
                )

            log.info("=" * 80)
            log.info("ALL VERIFICATIONS PASSED!")
            log.info("  - Pool-level space savings: %.2f%%", space_saved_percent)
            log.info("  - Shard-level size ratio: %.2fx", shard_ratio)
            log.info("=" * 80)

            # Run pool sanity check
            if not rados_obj.run_pool_sanity_check():
                log.error("Pool sanity checks failed on cluster")
                return 1

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            test_failed = True
        finally:
            log.info(
                "\n \n ************** In Finally block for fast EC space gain test *************** \n \n"
            )

            # Clean up temporary file
            try:
                temp_file = "/tmp/test_4k_object.bin"
                client_node.exec_command(cmd=f"rm -f {temp_file}", sudo=True)
                log.info("Cleaned up temporary file: %s", temp_file)
            except Exception as e:
                log.warning("Failed to clean up temporary file: %s", e)

            # Reset osd_pool_default_flag_ec_optimizations to true
            try:
                mon_obj.remove_config(
                    section="global", name="osd_pool_default_flag_ec_optimizations"
                )
                log.info(
                    "Reset osd_pool_default_flag_ec_optimizations to default (true)"
                )
            except Exception as e:
                log.warning(
                    "Failed to reset osd_pool_default_flag_ec_optimizations: %s", e
                )

            # Delete pools individually
            for pool_to_delete in [pool_name_1, pool_name_2]:
                try:
                    log.info("Deleting pool: %s", pool_to_delete)
                    if rados_obj.delete_pool(pool=pool_to_delete):
                        log.info("Successfully deleted pool: %s", pool_to_delete)
                    else:
                        log.warning("Failed to delete pool: %s", pool_to_delete)
                except Exception as e:
                    log.warning("Error deleting pool %s: %s", pool_to_delete, e)

            # Log cluster health
            rados_obj.log_cluster_health()

            # Check for crashes after test execution
            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.debug(
                f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
            )
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash at the end of test")
                test_failed = True

        if test_failed:
            return 1

        log.info("Verification of Fast EC space gain completed successfully")
        return 0

    if config.get("test_staggered_upgrade_accumulated_pools"):
        """
        Test suite for verifying all pools accumulated during staggered upgrades.

        This test runs AFTER the entire cluster is upgraded.

        Workflow:
        1. Discover all staggered upgrade pools
        2. Enable Fast EC features on all EC pools
        3. Run I/O tests on EC pools
        4. Test RBD images (existing + create new)
        5. Run scrub & deep-scrub, verify PGs are active+clean
        6. Summary and cleanup (if cleanup_pools=True)
        """
        log.info("=" * 80)
        log.info("TEST: STAGGERED UPGRADE ACCUMULATED POOLS VERIFICATION")
        log.info("=" * 80)

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        test_failed = False
        cleanup_pools = config.get("cleanup_pools", True)
        staggered_pools = None  # Initialize for finally block access

        try:
            # Step 1: Discover all staggered upgrade pools
            log.info("\nStep 1: Discovering staggered upgrade pools...")
            staggered_pools = get_staggered_upgrade_pools(rados_obj)

            if not staggered_pools["all_pools"]:
                log.warning(
                    "No staggered upgrade pools found! "
                    "Ensure test_fast_ec_optimization_params_partial_upgrade was run "
                    "during each staggered upgrade phase."
                )
                log.info("Test skipped: No pools to verify")
                return 0

            log.info(
                "Found %d staggered upgrade pools:", len(staggered_pools["all_pools"])
            )
            log.info("  EC pools: %d", len(staggered_pools["ec_pools"]))
            log.info("  RBD data pools: %d", len(staggered_pools["rbd_data_pools"]))
            log.info("  RBD meta pools: %d", len(staggered_pools["rbd_meta_pools"]))
            log.info("  Upgrade cases found: %s", staggered_pools["upgrade_cases"])

            # Step 2: Enable Fast EC features on all EC pools if not already enabled
            log.info("\nStep 2: Enabling Fast EC features on all EC pools...")
            all_ec_pools = (
                staggered_pools["ec_pools"] + staggered_pools["rbd_data_pools"]
            )
            for pool_name in all_ec_pools:
                ec_opt_dict = rados_obj.get_pool_property(
                    pool=pool_name, props="allow_ec_optimizations"
                )
                if ec_opt_dict.get("allow_ec_optimizations") is True:
                    log.info("  %s: Fast EC already enabled", pool_name)
                else:
                    if not rados_obj.enable_fast_ec_feature_on_pool(
                        pool_name=pool_name
                    ):
                        raise Exception(f"Failed to enable Fast EC on pool {pool_name}")
                    log.info("  %s: Fast EC enabled", pool_name)

            # Step 3: Run I/O tests on accumulated pools
            log.info("\nStep 3: Running I/O tests on accumulated pools...")

            # Test EC pools
            for pool_name in staggered_pools["ec_pools"]:
                log.info("  Testing I/O on EC pool: %s", pool_name)
                assert rados_obj.bench_write(
                    pool_name=pool_name,
                    byte_size="8K",
                    max_objs=50,
                    verify_stats=True,
                ), f"Failed to write data to EC pool {pool_name}"
                log.info("    Write test: PASSED")

            # Step 4: Test RBD images - write on existing and create new images
            log.info("\nStep 4: Testing RBD images (existing + new)...")
            for meta_pool in staggered_pools["rbd_meta_pools"]:
                data_pool = meta_pool.replace(
                    STAGGERED_UPGRADE_RBD_META_POOL_PREFIX,
                    STAGGERED_UPGRADE_RBD_DATA_POOL_PREFIX,
                )
                log.info("  Pool: %s (data: %s)", meta_pool, data_pool)

                # List and test existing images
                images_out, _ = client_node.exec_command(
                    cmd=f"rbd ls {meta_pool}", sudo=True
                )
                existing_images = (
                    [img for img in images_out.strip().split("\n") if img]
                    if images_out
                    else []
                )
                log.info("    Existing images: %s", existing_images)

                # Create new image
                new_image = f"post-upgrade-image-{meta_pool.split('-')[-1]}"
                client_node.exec_command(
                    cmd=f"rbd create {new_image} --size 256M --data-pool {data_pool} --pool {meta_pool}",
                    sudo=True,
                )
                log.info("    Created new image: %s", new_image)

                # Run I/O on all images (existing + new)
                all_images = existing_images + [new_image]
                try:
                    for image in all_images:
                        client_node.exec_command(
                            cmd=f"rbd bench {meta_pool}/{image} --io-type write --io-size 4K --io-total 512K",
                            sudo=True,
                        )
                        log.info("    I/O on %s: PASSED", image)
                except Exception as e:
                    log.error("    I/O test failed: %s", e)
                    raise Exception(f"RBD I/O test failed in pool {meta_pool}: {e}")

            # Step 5: Run scrub and deep-scrub on all pools, verify PGs are active+clean
            log.info("\nStep 5: Running scrub & deep-scrub on all pools...")

            # Initiate scrub and deep-scrub on all pools
            for pool_name in staggered_pools["all_pools"]:
                rados_obj.run_scrub(pool=pool_name)
                rados_obj.run_deep_scrub(pool=pool_name)
                log.info("  Initiated scrub/deep-scrub on: %s", pool_name)

            # Wait for scrubs to complete
            log.info("  Waiting for scrubs to complete...")
            time.sleep(120)

            # Verify PG states
            disallowed_states = [
                "remapped",
                "backfilling",
                "degraded",
                "incomplete",
                "peering",
                "recovering",
                "recovery_wait",
                "undersized",
                "backfilling_wait",
                "scrubbing",
            ]
            for pool_name in staggered_pools["all_pools"]:
                if not rados_obj.check_pool_pg_states(
                    pool=pool_name, disallowed_states=disallowed_states
                ):
                    raise Exception(f"Pool {pool_name} PGs not active+clean")
                log.info("  %s: PGs active+clean", pool_name)

            # Step 6: Summary
            log.info("\n" + "=" * 80)
            log.info("STAGGERED UPGRADE ACCUMULATED POOLS TEST SUMMARY")
            log.info("=" * 80)
            log.info("Total pools tested: %d", len(staggered_pools["all_pools"]))
            log.info(
                "  EC pools (app=ec-upgrade): %d", len(staggered_pools["ec_pools"])
            )
            log.info(
                "  RBD data pools (EC): %d", len(staggered_pools["rbd_data_pools"])
            )
            log.info(
                "  RBD meta pools (replicated): %d",
                len(staggered_pools["rbd_meta_pools"]),
            )
            log.info("Upgrade cases covered: %s", staggered_pools["upgrade_cases"])
            log.info("=" * 80)

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            test_failed = True
        finally:
            log.info("\n \n ************** in finally block *************** \n \n")

            # Cleanup staggered upgrade pools if configured
            if cleanup_pools and staggered_pools and staggered_pools["all_pools"]:
                log.info(
                    "Cleaning up %d staggered upgrade pools...",
                    len(staggered_pools["all_pools"]),
                )
                for pool_name in (
                    staggered_pools["rbd_meta_pools"]
                    + staggered_pools["rbd_data_pools"]
                    + staggered_pools["ec_pools"]
                ):
                    rados_obj.delete_pool(pool=pool_name)
                    log.info("  Deleted: %s", pool_name)
            elif not cleanup_pools:
                log.info("Skipping pool cleanup (cleanup_pools=False)")

            # Log cluster health
            rados_obj.log_cluster_health()

            # Check for crashes after test execution
            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.debug(
                f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
            )
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash at the end of test")
                test_failed = True

        if test_failed:
            return 1

        log.info(
            "Staggered upgrade accumulated pools verification completed successfully"
        )
        return 0

    if config.get("test_stripe_unit_pool_creation"):
        """
        Test: Stripe Unit and Fast EC Default Verification

        Verifies that Fast EC is enabled by default only for supported configurations,
        and that stripe_unit defaults are correct.

        Expected Defaults:
        - Fast EC pools: stripe_unit = 16K, allow_ec_optimizations = True
        - Non-Fast EC pools: stripe_unit = 4K, allow_ec_optimizations = False

        Workflow:
        1. Create EC pools with various plugin/technique/k/m combinations
        2. Verify allow_ec_optimizations matches expected
        3. Verify stripe_unit matches expected (16K for Fast EC, 4K otherwise)
        4. Write data to verify pool functionality
        5. Cleanup all test pools
        """
        log.info("=" * 80)
        log.info("TEST: STRIPE UNIT AND FAST EC DEFAULT VERIFICATION")
        log.info("=" * 80)

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        test_failed = False
        created_pools = []
        pool_fast_ec_expected = {}  # pool_name -> fast_ec_expected

        # Pool configurations: (k, m, plugin, technique, fast_ec_expected, d, description)
        # Fast EC supported: Jerasure+reed_sol_van, ISA+reed_sol_van, ISA+cauchy
        pool_configs = [
            # Jerasure with reed_sol_van - Fast EC SUPPORTED
            (2, 2, "jerasure", "reed_sol_van", True, None, "jerasure-rsv-k2m2"),
            (4, 2, "jerasure", "reed_sol_van", True, None, "jerasure-rsv-k4m2"),
            (4, 3, "jerasure", "reed_sol_van", True, None, "jerasure-rsv-k4m3"),
            (5, 2, "jerasure", "reed_sol_van", True, None, "jerasure-rsv-k5m2"),
            (6, 2, "jerasure", "reed_sol_van", True, None, "jerasure-rsv-k6m2"),
            (8, 4, "jerasure", "reed_sol_van", True, None, "jerasure-rsv-k8m4"),
            (8, 6, "jerasure", "reed_sol_van", True, None, "jerasure-rsv-k8m6"),
            # ISA with reed_sol_van - Fast EC SUPPORTED
            (2, 2, "isa", "reed_sol_van", True, None, "isa-rsv-k2m2"),
            (4, 2, "isa", "reed_sol_van", True, None, "isa-rsv-k4m2"),
            (4, 3, "isa", "reed_sol_van", True, None, "isa-rsv-k4m3"),
            (5, 2, "isa", "reed_sol_van", True, None, "isa-rsv-k5m2"),
            (6, 2, "isa", "reed_sol_van", True, None, "isa-rsv-k6m2"),
            (8, 4, "isa", "reed_sol_van", True, None, "isa-rsv-k8m4"),
            (8, 6, "isa", "reed_sol_van", True, None, "isa-rsv-k8m6"),
            # ISA with cauchy - Fast EC SUPPORTED
            (2, 2, "isa", "cauchy", True, None, "isa-cauchy-k2m2"),
            (4, 2, "isa", "cauchy", True, None, "isa-cauchy-k4m2"),
            (4, 3, "isa", "cauchy", True, None, "isa-cauchy-k4m3"),
            (5, 2, "isa", "cauchy", True, None, "isa-cauchy-k5m2"),
            (6, 2, "isa", "cauchy", True, None, "isa-cauchy-k6m2"),
            (8, 4, "isa", "cauchy", True, None, "isa-cauchy-k8m4"),
            (8, 6, "isa", "cauchy", True, None, "isa-cauchy-k8m6"),
            # ISA with reed_sol_van but m > 4 - falls back to cauchy - Fast EC supported
            (4, 5, "isa", "reed_sol_van", True, None, "isa-rsv-m5-fallback-k4m5"),
            (5, 5, "isa", "reed_sol_van", True, None, "isa-rsv-m5-fallback-k5m5"),
            (6, 5, "isa", "reed_sol_van", True, None, "isa-rsv-m5-fallback-k6m5"),
            (4, 6, "isa", "reed_sol_van", True, None, "isa-rsv-m6-fallback-k4m6"),
            (6, 6, "isa", "reed_sol_van", True, None, "isa-rsv-m6-fallback-k6m6"),
            # Clay plugin - Fast EC NOT supported
            (2, 2, "clay", None, False, 3, "clay-k2m2d3"),
            (3, 2, "clay", None, False, 4, "clay-k3m2d4"),
            (4, 2, "clay", None, False, 5, "clay-k4m2d5"),
        ]

        try:
            log.info("\nStep 1: Creating EC pools with various configurations...")

            # Get number of OSD hosts to determine failure domain
            num_osd_hosts = len(rados_obj.get_osd_hosts())
            log.info("  Number of OSD hosts: %d", num_osd_hosts)

            for k, m, plugin, technique, fast_ec_expected, d, desc in pool_configs:
                pool_name = f"stripe-unit-test-{desc}"
                profile_name = f"ec_profile_{pool_name}"

                # Determine failure domain: host if enough hosts, else osd
                required_shards = k + m if d is None else max(k + m, d)
                failure_domain = "host" if num_osd_hosts >= required_shards else "osd"

                log.info(
                    "  Creating: %s (k=%d, m=%d, plugin=%s, technique=%s, d=%s, "
                    "fast_ec=%s, failure_domain=%s)",
                    pool_name,
                    k,
                    m,
                    plugin,
                    technique or "default",
                    d,
                    fast_ec_expected,
                    failure_domain,
                )

                try:
                    # Build kwargs for create_erasure_pool
                    pool_kwargs = {
                        "pool_name": pool_name,
                        "profile_name": profile_name,
                        "k": k,
                        "m": m,
                        "plugin": plugin,
                        "crush-failure-domain": failure_domain,
                    }
                    if technique:
                        pool_kwargs["technique"] = technique
                    if d is not None:
                        pool_kwargs["d"] = d

                    if not rados_obj.create_erasure_pool(**pool_kwargs):
                        raise Exception(f"Failed to create pool {pool_name}")

                    created_pools.append(pool_name)
                    pool_fast_ec_expected[pool_name] = fast_ec_expected
                    log.info("    Created successfully")

                except Exception as e:
                    log.error("    Failed to create pool %s: %s", pool_name, e)
                    raise

            log.info("\nStep 2: Verifying Fast EC, stripe_unit and stripe_width...")
            verification_passed = True
            fast_ec_count, non_fast_ec_count = 0, 0

            # Fetch all pool details and convert to dict for O(1) lookups
            pool_details_map = {
                p.get("pool_name"): p
                for p in rados_obj.run_ceph_command(cmd="ceph osd pool ls detail")
            }

            # Cache for EC profiles (fetched on-demand, deduplicated)
            ec_profiles_cache = {}

            for pool_name in created_pools:
                fast_ec_expected = pool_fast_ec_expected[pool_name]
                pool_detail = pool_details_map.get(pool_name, {})
                ec_profile_name = pool_detail.get("erasure_code_profile", "")

                # Fetch EC profile if not cached
                if ec_profile_name and ec_profile_name not in ec_profiles_cache:
                    ec_profiles_cache[ec_profile_name] = rados_obj.run_ceph_command(
                        cmd=f"ceph osd erasure-code-profile get {ec_profile_name}"
                    )

                # Extract allow_ec_optimizations from flags_names
                ec_opt_actual = "ec_optimizations" in pool_detail.get("flags_names", "")

                # Get EC profile details
                profile = ec_profiles_cache.get(ec_profile_name, {})
                stripe_unit_actual = int(profile.get("stripe_unit", 0))
                k_val = int(profile.get("k", 0))

                # Get stripe_width from pool details
                stripe_width_actual = pool_detail.get("stripe_width", 0)

                # Some profiles (e.g., clay) may not expose stripe_unit; derive from stripe_width
                if stripe_unit_actual == 0 and stripe_width_actual and k_val:
                    stripe_unit_actual = int(stripe_width_actual / k_val)

                # Expected values
                expected_stripe_unit = 16384 if fast_ec_expected else 4096
                expected_stripe_width = k_val * stripe_unit_actual

                # Verify
                ec_opt_ok = ec_opt_actual == fast_ec_expected
                stripe_unit_ok = stripe_unit_actual == expected_stripe_unit
                stripe_width_ok = stripe_width_actual == expected_stripe_width

                all_ok = ec_opt_ok and stripe_unit_ok and stripe_width_ok
                status = "PASS" if all_ok else "FAIL"
                if not all_ok:
                    verification_passed = False

                # Count for summary
                if fast_ec_expected:
                    fast_ec_count += 1
                else:
                    non_fast_ec_count += 1

                log.info(
                    "  %s: ec_opt=%s (exp=%s), stripe_unit=%d (exp=%d), "
                    "stripe_width=%d (exp=k*stripe_unit=%d) - %s",
                    pool_name,
                    ec_opt_actual,
                    fast_ec_expected,
                    stripe_unit_actual,
                    expected_stripe_unit,
                    stripe_width_actual,
                    expected_stripe_width,
                    status,
                )

            if not verification_passed:
                raise Exception("Pool property verification failed")

            log.info("\nStep 3: Writing data to verify pool functionality...")

            for pool_name in created_pools:
                log.info("  Writing to: %s", pool_name)
                assert rados_obj.bench_write(
                    pool_name=pool_name,
                    byte_size="4K",
                    max_objs=20,
                    verify_stats=True,
                ), f"Failed to write to pool {pool_name}"
                log.info("    Write: PASSED")

            # Summary
            log.info("\n" + "=" * 80)
            log.info("STRIPE UNIT AND FAST EC VERIFICATION SUMMARY")
            log.info("=" * 80)
            log.info("Total pools tested: %d", len(created_pools))
            log.info("Fast EC enabled pools: %d", fast_ec_count)
            log.info("Non-Fast EC pools: %d", non_fast_ec_count)
            log.info("All verifications: PASSED")
            log.info("=" * 80)

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            test_failed = True
        finally:
            log.info("\n \n ************** in finally block *************** \n \n")

            # Cleanup created pools
            log.info("\nCleaning up %d test pools...", len(created_pools))
            for pool_name in created_pools:
                rados_obj.delete_pool(pool=pool_name)
            log.info("Cleanup completed")

            rados_obj.log_cluster_health()

            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.debug(f"Test completed. Start: {start_time}, End: {test_end_time}")
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash at the end of test")
                test_failed = True

        if test_failed:
            return 1

        log.info("Stripe unit pool creation verification completed successfully")
        return 0


def run_common_case_setup(rados_obj, version_info, upgrade_case):
    """
    Common setup for all test cases: check releases and create pool

    Args:
        rados_obj: RadosOrchestrator object
        version_info: Version info dictionary
        upgrade_case: Case identifier string

    Returns:
        tuple: (actual_osd_release, expected_osd_release, test_pool_name, pool_result)
    """
    # Check require_osd_release
    actual_osd_release = get_require_osd_release(rados_obj)
    expected_osd_release = get_expected_osd_release(version_info)
    log.info(
        "OSD Release - Actual: %s, Expected: %s",
        actual_osd_release,
        expected_osd_release,
    )

    # Create pool
    test_pool_name = f"test_ec_pool_partial_upgrade-{upgrade_case}"
    pool_result = create_ec_pool_and_check_optimizations(
        rados_obj=rados_obj,
        pool_name=test_pool_name,
    )

    # Verify pool creation and object writes
    log.info("Pool creation results: %s", pool_result)

    if not pool_result["pool_created"]:
        log.error(f"Failed to create EC pool in {upgrade_case}")
        raise Exception("Pool creation failed: %s" % pool_result.get("error"))
    log.info("EC pool created successfully")

    if not pool_result["can_write_objects"]:
        log.error(f"Failed to write objects to EC pool in {upgrade_case}")
        raise Exception("Object write failed: %s" % pool_result.get("error"))
    log.info("Objects can be written to the pool")

    return actual_osd_release, expected_osd_release, test_pool_name, pool_result


def verify_ec_optimizations_status(pool_result, expected_value, case_desc):
    """
    Verify EC optimizations status matches expected value

    Args:
        pool_result: Dictionary with pool creation results
        expected_value: Expected value (True, False, or None)
        case_desc: Case description for logging

    Raises:
        Exception: If status doesn't match expected value
    """
    actual = pool_result["ec_optimizations_enabled"]

    if actual == expected_value:
        if expected_value is None:
            log.info(f"{case_desc}: EC optimizations unavailable as expected")
        elif expected_value is False:
            log.info(f"{case_desc}: EC optimizations disabled (false) as expected")
        else:
            log.info(f"{case_desc}: EC optimizations enabled (true) as expected")
    else:
        raise Exception(
            f"{case_desc}: EC optimizations mismatch - Expected: {expected_value}, Got: {actual}"
        )


def print_ec_pool_profile_details(rados_obj, pool_name):
    """
    Print detailed EC pool and erasure code profile information

    Fetches and logs pool attributes and associated EC profile settings
    in a structured format for debugging and verification.

    Args:
        rados_obj: RadosOrchestrator instance for executing ceph commands
        pool_name: Name of the EC pool to inspect

    Returns:
        dict: Dictionary containing 'pool' and 'profile' details
    """
    result = {"pool": None, "profile": None}

    try:
        # Get pool details using 'ceph osd pool get <pool> all'
        pool_details = rados_obj.run_ceph_command(
            cmd=f"ceph osd pool get {pool_name} all"
        )
        result["pool"] = pool_details

        log.info("=" * 60)
        log.info("=== EC Pool Details: %s ===", pool_name)
        log.info("=" * 60)

        # Print pool attributes
        log.info("Pool Attributes:")
        log.info("  - pool: %s", pool_details.get("pool"))
        log.info("  - pool_id: %s", pool_details.get("pool_id"))
        log.info("  - size (k+m): %s", pool_details.get("size"))
        log.info("  - min_size: %s", pool_details.get("min_size"))
        log.info("  - pg_num: %s", pool_details.get("pg_num"))
        log.info("  - pgp_num: %s", pool_details.get("pgp_num"))
        log.info("  - crush_rule: %s", pool_details.get("crush_rule"))
        log.info(
            "  - erasure_code_profile: %s", pool_details.get("erasure_code_profile")
        )
        log.info("  - allow_ec_overwrites: %s", pool_details.get("allow_ec_overwrites"))
        log.info(
            "  - allow_ec_optimizations: %s", pool_details.get("allow_ec_optimizations")
        )
        log.info("  - pg_autoscale_mode: %s", pool_details.get("pg_autoscale_mode"))

        # Get EC profile details
        ec_profile_name = pool_details.get("erasure_code_profile")
        if ec_profile_name:
            profile_details = rados_obj.run_ceph_command(
                cmd=f"ceph osd erasure-code-profile get {ec_profile_name}"
            )
            result["profile"] = profile_details

            log.info("-" * 60)
            log.info("EC Profile Details (%s):", ec_profile_name)
            log.info("  - k: %s", profile_details.get("k"))
            log.info("  - m: %s", profile_details.get("m"))
            log.info("  - plugin: %s", profile_details.get("plugin"))
            log.info("  - stripe_unit: %s", profile_details.get("stripe_unit"))
            log.info("  - technique: %s", profile_details.get("technique"))
            log.info(
                "  - crush-failure-domain: %s",
                profile_details.get("crush-failure-domain"),
            )
            log.info(
                "  - crush-device-class: %s", profile_details.get("crush-device-class")
            )

        log.info("=" * 60)

    except Exception as e:
        log.error("Failed to get EC pool/profile details for %s: %s", pool_name, str(e))

    return result


def create_ec_pool_with_optimization_flag(
    rados_obj, pool_name, stripe_unit, allow_ec_optimizations
):
    """
    Create an EC pool with specified stripe_unit and verify the allow_ec_optimizations flag.

    This helper method:
    1. Creates an erasure-coded pool with k=2, m=2 and specified stripe_unit
    2. Optionally enables allow_ec_optimizations if requested
    3. Prints pool and profile details
    4. Verifies the allow_ec_optimizations flag matches the expected value

    Args:
        rados_obj: RadosOrchestrator instance for executing ceph commands
        pool_name: Name of the EC pool to create
        stripe_unit: Stripe unit size for the EC profile (e.g., "16384" for 16KB)
        allow_ec_optimizations: Boolean - True to enable optimizations, False to keep disabled

    Raises:
        AssertionError: If pool creation fails
        Exception: If allow_ec_optimizations verification fails
    """
    # Create the EC pool
    assert rados_obj.create_erasure_pool(
        pool_name=pool_name,
        profile_name=f"ec_profile_{pool_name}",
        k=2,
        m=2,
        stripe_unit=stripe_unit,
        app_name="rados",
    ), f"Failed to create EC pool {pool_name}"

    # Enable EC optimizations if requested
    if allow_ec_optimizations:
        rados_obj.set_pool_property(
            pool=pool_name, props="allow_ec_optimizations", value="true"
        )
        log.info("Enabled allow_ec_optimizations on pool %s", pool_name)
        time.sleep(2)

    # Print EC pool and profile details
    print_ec_pool_profile_details(rados_obj, pool_name)

    # Verify allow_ec_optimizations flag
    ec_opt_value_dict = rados_obj.get_pool_property(
        pool=pool_name, props="allow_ec_optimizations"
    )
    ec_opt_value = ec_opt_value_dict.get("allow_ec_optimizations")
    log.info("Pool %s - allow_ec_optimizations: %s", pool_name, ec_opt_value)

    if ec_opt_value is not allow_ec_optimizations:
        log.error(
            "allow_ec_optimizations is not %s for %s", allow_ec_optimizations, pool_name
        )
        raise Exception(
            f"EC optimizations check failed for {pool_name}: "
            f"expected {allow_ec_optimizations}, got {ec_opt_value}"
        )

    log.info(
        "Successfully created EC pool %s with allow_ec_optimizations=%s",
        pool_name,
        allow_ec_optimizations,
    )


def print_case_summary(
    case_desc,
    pool_result,
    actual_osd_release,
    expected_osd_release,
    expected_ec_opt=None,
    additional_info=None,
):
    """
    Print standardized summary for test cases

    Args:
        case_desc: Case description
        pool_result: Pool creation results
        actual_osd_release: Actual OSD release
        expected_osd_release: Expected OSD release
        expected_ec_opt: Expected EC optimization value
        additional_info: List of additional summary items
    """
    log.info(f"\n=== {case_desc} verification completed successfully ===")
    log.info("Summary:")
    log.info("  - Pool created: %s", pool_result["pool_created"])
    log.info("  - Can write objects: %s", pool_result["can_write_objects"])
    log.info(
        "  - EC optimizations: %s (expected: %s)",
        pool_result["ec_optimizations_enabled"],
        expected_ec_opt,
    )
    log.info(
        "  - require_osd_release: %s (expected: %s)",
        actual_osd_release,
        expected_osd_release,
    )

    if additional_info:
        for info in additional_info:
            log.info(f"  - {info}")


def test_cephfs_with_ec_pool(client_node, fs_name):
    """
    Test CephFS operations on EC pool with optimizations enabled

    Performs simple test operations:
    - Create subvolume group and subvolume
    - Mount subvolume
    - Write test files
    - Create and delete snapshots
    - Cleanup

    Args:
        client_node: Client node to execute commands
        fs_name: CephFS filesystem name

    Returns:
        bool: True if all operations succeeded, False otherwise
    """
    volume_group = "test_vgroup"
    subvol_name = "test_subvol"
    mount_path = "/mnt/case5_test"

    try:
        # Create subvolume group and subvolume
        log.info("Creating subvolume group and subvolume...")
        client_node.exec_command(
            cmd=f"ceph fs subvolumegroup create {fs_name} {volume_group}", sudo=True
        )
        client_node.exec_command(
            cmd=f"ceph fs subvolume create {fs_name} {subvol_name} --group_name {volume_group}",
            sudo=True,
        )

        # Get subvolume path and mount
        log.info("Mounting subvolume...")
        subvol_path, _ = client_node.exec_command(
            cmd=f"ceph fs subvolume getpath {fs_name} {subvol_name} --group_name {volume_group}",
            sudo=True,
        )
        client_node.exec_command(cmd=f"mkdir -p {mount_path}", sudo=True)
        client_node.exec_command(
            cmd=f"ceph-fuse --client_fs {fs_name} -r {subvol_path.strip()} {mount_path}",
            sudo=True,
        )

        # Write test files
        log.info("Writing test files...")
        for i in range(3):
            client_node.exec_command(
                cmd=f"dd if=/dev/urandom of={mount_path}/testfile{i}.dat bs=1M count=5 conv=fsync",
                sudo=True,
            )

        # Create and delete snapshots
        log.info("Creating and deleting snapshots...")
        snap1 = f"snap1_{int(time.time())}"
        snap2 = f"snap2_{int(time.time())}"

        client_node.exec_command(
            cmd=f"ceph fs subvolume snapshot create {fs_name} {subvol_name} {snap1} --group_name {volume_group}",
            sudo=True,
        )
        client_node.exec_command(
            cmd=f"ceph fs subvolume snapshot create {fs_name} {subvol_name} {snap2} --group_name {volume_group}",
            sudo=True,
        )
        client_node.exec_command(
            cmd=f"ceph fs subvolume snapshot rm {fs_name} {subvol_name} {snap1} --group_name {volume_group}",
            sudo=True,
        )
        client_node.exec_command(
            cmd=f"ceph fs subvolume snapshot rm {fs_name} {subvol_name} {snap2} --group_name {volume_group}",
            sudo=True,
        )

        log.info("CephFS operations completed successfully")
        return True

    except Exception as e:
        log.error(f"CephFS operations failed: {e}")
        log.exception(e)
        return False

    finally:
        # Cleanup
        log.info("Cleaning up CephFS test resources...")
        client_node.exec_command(cmd=f"umount {mount_path}", sudo=True, check_ec=False)
        client_node.exec_command(
            cmd=f"ceph fs subvolume rm {fs_name} {subvol_name} --group_name {volume_group}",
            sudo=True,
            check_ec=False,
        )
        client_node.exec_command(
            cmd=f"ceph fs subvolumegroup rm {fs_name} {volume_group}",
            sudo=True,
            check_ec=False,
        )


def verify_require_osd_release_match(
    actual_osd_release, expected_osd_release, case_desc, reason=""
):
    """
    Helper function to verify require_osd_release matches expected value

    Args:
        actual_osd_release: Actual require_osd_release from cluster
        expected_osd_release: Expected require_osd_release value
        case_desc: Description of the test case (e.g., "Case 1", "Case 2")
        reason: Optional reason for the expected value

    Raises:
        Exception: If actual doesn't match expected
    """
    log.info(f"Verifying require_osd_release for {case_desc}...")
    if actual_osd_release != expected_osd_release:
        log.error(
            "require_osd_release mismatch - Actual: %s, Expected: %s",
            actual_osd_release,
            expected_osd_release,
        )
        raise Exception(
            f"require_osd_release should be '{expected_osd_release}' but is '{actual_osd_release}' "
            f"({reason or 'OSDs not fully upgraded to update require_osd_release'})"
        )
    log.info(
        f"require_osd_release is correct: {actual_osd_release} {('(' + reason + ')') if reason else ''}",
    )


def verify_osd_pool_default_flag(mon_obj, rados_obj, case_desc, osds_upgraded=True):
    """
    Helper function to verify osd_pool_default_flag_ec_optimizations configuration

    Args:
        mon_obj: MonConfigMethods object
        rados_obj: RadosOrchestrator object
        case_desc: Description of the test case (e.g., "Case 2", "Case 3")
        osds_upgraded: Whether OSDs are upgraded to 9.x (default: True)

    Raises:
        Exception: If verification fails
    """
    log.info(
        "Verifying osd_pool_default_flag_ec_optimizations after Mon upgrade to 9.x..."
    )
    try:
        if osds_upgraded:
            # Full verification: Mon DB and OSD runtime should match
            verify_config_db_and_runtime(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="osd_pool_default_flag_ec_optimizations",
                expected_value="true",
            )
            log.info(
                "osd_pool_default_flag_ec_optimizations is correctly set to 'true' "
                "on both Mon and OSD (after Mon upgrade to 9.x)"
            )
        else:
            # Partial upgrade: Only verify Mon DB, OSDs don't understand the parameter yet
            log.info("OSDs not fully upgraded - verifying Mon config DB only")
            mon_db_value = mon_obj.get_config(
                section="mon", param="osd_pool_default_flag_ec_optimizations"
            )
            log.info(f"Mon config DB value: {mon_db_value}")
            if mon_db_value != "true":
                raise Exception(
                    f"Expected Mon config DB value 'true', got '{mon_db_value}'"
                )
            log.info(
                "osd_pool_default_flag_ec_optimizations is correctly set to 'true' "
                "on Mon (OSDs not yet upgraded, so runtime value is null - expected)"
            )
    except Exception as e:
        log.error("Failed to verify osd_pool_default_flag_ec_optimizations: %s", e)
        raise Exception(
            f"osd_pool_default_flag_ec_optimizations verification failed in {case_desc}: {e}"
        )


def read_and_verify_objects(client_node, mount_path_1, mount_path_2, count, step_desc):
    """
    Helper function to read and verify all objects from both mounted filesystems

    Args:
        client_node: Client node to execute commands
        mount_path_1: First mount path
        mount_path_2: Second mount path
        count: Total number of objects to read (0 to count-1)
        step_desc: Description of the read step for logging
    """
    log.info(
        "Reading and verifying ALL %s objects (range: 0-%s) - %s",
        count,
        count - 1,
        step_desc,
    )

    try:
        # Read all objects from mount path 1 (from offset 0 to count-1)
        log.info("Starting read verification for mount %s", mount_path_1)
        failed_reads_1 = []
        for i in range(count):
            file_path = f"{mount_path_1}file_{i}"
            # Try to read the file
            read_cmd_1 = f"cat {file_path} > /dev/null"
            out, err, exit_code, _ = client_node.exec_command(
                cmd=read_cmd_1, sudo=True, verbose=True
            )

            # Check exit code
            if exit_code != 0:
                log.error(
                    "Failed to read file %s from %s: exit_code=%s, err=%s",
                    i,
                    mount_path_1,
                    exit_code,
                    err,
                )
                failed_reads_1.append(i)

        if failed_reads_1:
            raise Exception(
                "Failed to read %s files from %s: %s"
                % (len(failed_reads_1), mount_path_1, failed_reads_1)
            )

        log.info(
            " Successfully read ALL %s files (0-%s) from %s",
            count,
            count - 1,
            mount_path_1,
        )

        # Read all objects from mount path 2 (from offset 0 to count-1)
        log.info("Starting read verification for mount %s", mount_path_2)
        failed_reads_2 = []
        for i in range(count):
            file_path = f"{mount_path_2}file_{i}"
            # Try to read the file
            read_cmd_2 = f"cat {file_path} > /dev/null"
            out, err, exit_code, _ = client_node.exec_command(
                cmd=read_cmd_2, sudo=True, verbose=True
            )

            # Check exit code
            if exit_code != 0:
                log.error(
                    "Failed to read file %s from %s: exit_code=%s, err=%s",
                    i,
                    mount_path_2,
                    exit_code,
                    err,
                )
                failed_reads_2.append(i)

        if failed_reads_2:
            raise Exception(
                "Failed to read %s files from %s: %s"
                % (len(failed_reads_2), mount_path_2, failed_reads_2)
            )

        log.info(
            " Successfully read ALL %s files (0-%s) from %s",
            count,
            count - 1,
            mount_path_2,
        )
        log.info(
            " %s - All %s files verified successfully from both mounts (Total: %s reads completed)",
            step_desc,
            count,
            count * 2,
        )

    except Exception as e:
        log.error("Failed during %s: %s", step_desc, str(e))
        raise


def verify_config_db_and_runtime(
    mon_obj, rados_obj, param_name, expected_value, osd_id=None, mon_id=None
):
    """
    Helper method to verify config parameter at both DB and runtime levels

    Args:
        mon_obj: MonConfigMethods object
        rados_obj: RadosOrchestrator object
        param_name: Name of the configuration parameter
        expected_value: Expected value of the parameter
        osd_id: Optional OSD ID to check (if None, will fetch first available)
        mon_id: Optional Mon ID to check (if None, will fetch first available)

    Returns: True if all checks pass, else Raises exception
    """
    log.info("Verifying config parameter '%s' = '%s'", param_name, expected_value)

    # Get OSD and Mon IDs if not provided
    if osd_id is None:
        osd_list = rados_obj.get_osd_list(status="up")
        osd_id = osd_list[0]
        log.debug("Picked OSD : %s for checking config at runtime", osd_id)

    if mon_id is None:
        mon_list = rados_obj.run_ceph_command(cmd="ceph mon dump", client_exec=True)[
            "mons"
        ]
        mon_id = mon_list[0]["name"]
        log.debug("Picked Mon : %s for checking config at runtime", mon_id)

    # Check at OSD level - both DB and runtime
    log.debug("Checking OSD level for '%s'", param_name)

    # Check runtime value (ceph config show)
    osd_runtime = mon_obj.show_config(daemon="osd", id=osd_id, param=param_name)
    osd_runtime = str(osd_runtime).strip()
    log.debug("  OSD %s - Runtime (config show): %s", osd_id, osd_runtime)

    # Check DB value (ceph config get)
    osd_db = mon_obj.get_config(section="osd", param=param_name)
    osd_db = str(osd_db).strip()
    log.debug("  OSD - DB (config get): %s", osd_db)

    # Check at mon level - both DB and runtime
    log.debug("Checking Mon level for '%s'", param_name)

    # Check runtime value (ceph config show)
    mon_runtime = mon_obj.show_config(daemon="mon", id=mon_id, param=param_name)
    mon_runtime = str(mon_runtime).strip()
    log.debug("  Mon %s - Runtime (config show): %s", mon_id, mon_runtime)

    # Check DB value (ceph config get)
    mon_db = mon_obj.get_config(section="mon", param=param_name)
    mon_db = str(mon_db).strip()
    log.debug("  Mon - DB (config get): %s", mon_db)

    # Verify all values match expected
    errors = []

    if osd_runtime != expected_value:
        errors.append(
            "OSD runtime value mismatch: %s != %s" % (osd_runtime, expected_value)
        )
    if osd_db != expected_value:
        errors.append("OSD DB value mismatch: %s != %s" % (osd_db, expected_value))
    if mon_runtime != expected_value:
        errors.append(
            "Mon runtime value mismatch: %s != %s" % (mon_runtime, expected_value)
        )
    if mon_db != expected_value:
        errors.append("Mon DB value mismatch: %s != %s" % (mon_db, expected_value))

    if errors:
        error_msg = "Config verification failed for '%s':\n%s" % (
            param_name,
            "\n".join(errors),
        )
        log.error(error_msg)
        raise Exception(error_msg)

    log.info(
        " Verified: %s = %s (both DB and runtime for OSD and Mon)",
        param_name,
        expected_value,
    )
    return True


def write_and_verify_objects(
    client_node, mount_path_1, mount_path_2, start_offset, count, step_desc
):
    """
    Helper function to write objects to both mounted filesystems and verify read

    Args:
        client_node: Client node to execute commands
        mount_path_1: First mount path
        mount_path_2: Second mount path
        start_offset: Starting offset for writing objects
        count: Number of objects to write
        step_desc: Description of the write step for logging

    The function writes 'count' objects starting from 'start_offset',
    then verifies all objects from 0 to (start_offset + count - 1)
    """
    end_offset = start_offset + count
    log.info(
        "Writing %s objects (offset %s-%s) - %s",
        count,
        start_offset,
        end_offset - 1,
        step_desc,
    )

    try:
        # Write objects to both mounted filesystems
        for i in range(start_offset, end_offset):
            file_path_1 = f"{mount_path_1}file_{i}"
            write_cmd_1 = f"dd if=/dev/urandom of={file_path_1} bs=4M count=1"
            out, err, exit_code, _ = client_node.exec_command(
                cmd=write_cmd_1, sudo=True, verbose=True
            )
            if exit_code != 0:
                log.error(
                    "Failed to write file %s to %s: exit_code=%s, err=%s",
                    i,
                    mount_path_1,
                    exit_code,
                    err,
                )
                raise Exception("Write failed for file %s at %s" % (i, mount_path_1))

            file_path_2 = f"{mount_path_2}file_{i}"
            write_cmd_2 = f"dd if=/dev/urandom of={file_path_2} bs=4M count=1"
            out, err, exit_code, _ = client_node.exec_command(
                cmd=write_cmd_2, sudo=True, verbose=True
            )
            if exit_code != 0:
                log.error(
                    "Failed to write file %s to %s: exit_code=%s, err=%s",
                    i,
                    mount_path_2,
                    exit_code,
                    err,
                )
                raise Exception("Write failed for file %s at %s" % (i, mount_path_2))

        # Sync data
        client_node.exec_command(cmd="sync", sudo=True)
        log.info("Completed writing %s files to both mounts", count)

        # Verify all objects written so far
        total_objects = end_offset
        log.info(
            "Reading and verifying ALL %s files (0-%s) after %s",
            total_objects,
            total_objects - 1,
            step_desc,
        )
        read_and_verify_objects(
            client_node,
            mount_path_1,
            mount_path_2,
            total_objects,
            f"Verify after {step_desc}",
        )

    except Exception as e:
        log.error("Failed during write and verify: %s", str(e))
        raise


def get_daemon_versions(rados_obj):
    """
    Fetch and parse daemon versions from the cluster using 'ceph versions'

    Maps version numbers to Ceph releases:
    - 20.x -> 9.x (Tentacle)
    - 19.x -> 8.x (Squid)
    - 18.x -> 7.x (Reef)

    Args:
        rados_obj: RadosOrchestrator object

    Returns:
        Dictionary with daemon version distribution:
        {
            'mon': {'9.x': 4, '8.x': 0, '7.x': 0, 'total': 4},
            'mgr': {'9.x': 4, '8.x': 0, '7.x': 0, 'total': 4},
            'osd': {'9.x': 52, '8.x': 0, '7.x': 0, 'total': 52},
            ...
        }
    """
    log.info("Fetching daemon versions from cluster")
    versions_output = rados_obj.run_ceph_command(cmd="ceph versions", client_exec=True)
    log.debug("versions output from cluster: %s", versions_output)
    version_info = {}
    daemon_types = ["mon", "mgr", "osd", "mds", "rgw"]

    for daemon_type in daemon_types:
        if daemon_type not in versions_output:
            continue

        version_info[daemon_type] = {
            "9.x": 0,
            "8.x": 0,
            "7.x": 0,
            "other": 0,
            "total": 0,
        }

        for version_string, count in versions_output[daemon_type].items():
            # Extract major version number (e.g., "ceph version 20.1.0-64..." -> "20")
            parts = version_string.split()
            if len(parts) >= 3 and parts[0] == "ceph" and parts[1] == "version":
                version_num = parts[2].split(".")[0]

                if version_num == "20":
                    version_info[daemon_type]["9.x"] += count
                elif version_num == "19":
                    version_info[daemon_type]["8.x"] += count
                elif version_num == "18":
                    version_info[daemon_type]["7.x"] += count
                else:
                    version_info[daemon_type]["other"] += count
                    log.warning(
                        "version older than reef for %s: %s",
                        daemon_type,
                        version_string,
                    )
                version_info[daemon_type]["total"] += count
    log.info(f"All the versions collected from the cluster are : {version_info}")
    return version_info


def get_require_osd_release(rados_obj):
    """
    Get the require_osd_release value from the cluster

    Args:
        rados_obj: RadosOrchestrator object

    Returns:
        String with the release name (e.g., 'reef', 'squid', 'tentacle')
    """
    log.info("Fetching require_osd_release from cluster")
    cluster_dump = rados_obj.run_ceph_command(cmd="ceph osd dump")
    osd_release = cluster_dump.get("require_osd_release")
    log.info("Current require_osd_release: %s", osd_release)
    return osd_release


def get_expected_osd_release(version_info):
    """
    Determine the expected require_osd_release based on daemon versions

    The require_osd_release should remain at the lower version until ALL OSDs are upgraded.

    Version mapping:
    - 7.x (18.x) -> reef
    - 8.x (19.x) -> squid
    - 9.x (20.x) -> tentacle

    Args:
        version_info: Dictionary from get_daemon_versions()

    Returns:
        String with expected release name (e.g., 'reef', 'squid', 'tentacle')
    """
    osd_9x = version_info.get("osd", {}).get("9.x", 0)
    osd_8x = version_info.get("osd", {}).get("8.x", 0)
    osd_7x = version_info.get("osd", {}).get("7.x", 0)
    osd_total = version_info.get("osd", {}).get("total", 0)

    # If there are any 7.x OSDs, expect reef
    if osd_7x > 0:
        return "reef"
    # If there are any 8.x OSDs, expect squid
    elif osd_8x > 0:
        return "squid"
    # If all OSDs are in 9.x, expect tentacle
    elif osd_9x == osd_total and osd_total > 0:
        return "tentacle"
    else:
        # Fallback: shouldn't happen in normal scenarios
        log.warning(
            "Unable to determine expected OSD release from version info: %s",
            version_info.get("osd", {}),
        )
        return "unknown"


def identify_upgrade_case(version_info):
    """
    Identify which upgrade case the cluster is in based on daemon versions

    Cases:
    1. case_1_mgr_only: Few or all Mgr's in 9.x only (Mons and OSDs not in 9.x)
    2. case_2_mon_and_mgr: Few or Mons in 9.x + Few or all Mgr's in 9.x only (OSDs not in 9.x)
    3. case_3_osd_mon_mgr: Few OSDs in 9.x + Few or Mons in 9.x + Few or all Mgr's in 9.x only (not all OSDs)
    4. case_4_and_5_all_core_daemons: All core daemons (Mon, Mgr, OSD) in 9.x
       - Case 4: Tests EC pool functionality with optimizations enabled (runs always)
       - Case 5: Tests backward compatibility - NEW OSDs with EC optimizations work with older MDS clients
                 (runs ONLY if NO MDS are upgraded to 9.x)

    Note: When all Mon, Mgr, and OSD are upgraded to 9.x:
    1. Case 4 always runs to validate EC optimizations work correctly
    2. Case 5 only runs if NO MDS are upgraded (tests backward compatibility with older MDS)

    Args:
        version_info: Dictionary from get_daemon_versions()

    Returns:
        String indicating the upgrade case, or None if not in a recognized state
    """
    log.info("Identifying upgrade case from version distribution")

    # Extract counts for key daemon types
    mgr_9x = version_info.get("mgr", {}).get("9.x", 0)
    mgr_total = version_info.get("mgr", {}).get("total", 0)

    mon_9x = version_info.get("mon", {}).get("9.x", 0)
    mon_total = version_info.get("mon", {}).get("total", 0)

    osd_9x = version_info.get("osd", {}).get("9.x", 0)
    osd_total = version_info.get("osd", {}).get("total", 0)

    mds_9x = version_info.get("mds", {}).get("9.x", 0)
    mds_total = version_info.get("mds", {}).get("total", 0)

    log.debug(
        "Daemon counts - Mgr: %s/%s in 9.x, Mon: %s/%s in 9.x, OSD: %s/%s in 9.x, MDS: %s/%s in 9.x",
        mgr_9x,
        mgr_total,
        mon_9x,
        mon_total,
        osd_9x,
        osd_total,
        mds_9x,
        mds_total,
    )

    # Check if cluster is fully upgraded (all in 9.x, including MDS if present)
    if (
        mgr_9x == mgr_total
        and mon_9x == mon_total
        and osd_9x == osd_total
        and mgr_9x > 0
        and mon_9x > 0
        and osd_9x > 0
        and (mds_9x == mds_total or mds_total == 0)  # MDS also upgraded or no MDS
    ):
        log.info("Cluster is fully upgraded to 9.x (including MDS if present)")
        return None

    # Check if cluster hasn't started upgrade (none in 9.x)
    if mgr_9x == 0 and mon_9x == 0 and osd_9x == 0:
        log.info("Cluster has not started upgrade (no daemons in 9.x)")
        return None

    # Cases 4 & 5: All OSDs, Mgrs, Mons in 9.x (MDS status irrelevant)
    # Both cases run under the same cluster conditions but test different things
    if (
        mgr_9x == mgr_total
        and mgr_total > 0
        and mon_9x == mon_total
        and mon_total > 0
        and osd_9x == osd_total
        and osd_total > 0
    ):
        log.info(
            "All core daemons (Mgr, Mon, OSD) upgraded to 9.x "
            "(MDS status: %d/%d in 9.x)",
            mds_9x,
            mds_total,
        )
        # Return both cases to indicate both tests can run
        # The calling code will decide which tests to execute
        return "case_4_and_5_all_core_daemons"

    # Case 3: Some OSDs in 9.x + Some Mons in 9.x + Some/All Mgrs in 9.x
    if osd_9x > 0 and osd_9x < osd_total and mon_9x > 0 and mgr_9x > 0:
        log.info("Case 3 identified: Partial OSDs, Mons, and Mgrs in 9.x")
        return "case_3_osd_mon_mgr"

    # Case 2: Some Mons in 9.x + Some/All Mgrs in 9.x (but no OSDs in 9.x)
    if mon_9x > 0 and mgr_9x > 0 and osd_9x == 0:
        log.info("Case 2 identified: Mons and Mgrs in 9.x, OSDs not upgraded")
        return "case_2_mon_and_mgr"

    # Case 1: Only Mgrs in 9.x (Mons and OSDs not in 9.x)
    if mgr_9x > 0 and mon_9x == 0 and osd_9x == 0:
        log.info("Case 1 identified: Only Mgrs in 9.x")
        return "case_1_mgr_only"

    # Unknown/unhandled case
    log.warning("Unrecognized upgrade state")
    return None


def create_ec_pool_and_check_optimizations(rados_obj, pool_name, app_name="rados"):
    """
    Helper method to create an EC pool and check if EC optimizations are enabled

    This method:
    1. Creates an erasure-coded pool with k=2, m=2
    2. Checks the allow_ec_optimizations flag on the EC pool
    3. Writes test objects to verify pool functionality

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: Name of the EC pool to create
        app_name: Application name for the pool (default: "rados", use "cephfs" for CephFS data pools)

    Returns:
        Dictionary with pool creation results:
        {
            'pool_created': bool,
            'ec_optimizations_enabled': bool,
            'can_write_objects': bool,
            'error': str (if any)
        }
    """
    log.info(
        "Creating EC pool '%s' with app '%s' and checking optimizations",
        pool_name,
        app_name,
    )

    result = {
        "pool_created": False,
        "ec_optimizations_enabled": None,
        "can_write_objects": False,
        "error": None,
    }

    try:
        ec_config = {
            "pool_name": pool_name,
            "profile_name": f"ec_profile_{pool_name}",
            "k": 2,
            "m": 2,
            "app_name": app_name,
            "erasure_code_use_overwrites": "true",
        }

        log.info("Creating EC pool with config: %s", ec_config)
        if not rados_obj.create_erasure_pool(**ec_config):
            result["error"] = f"Failed to create EC pool {pool_name}"
            log.error(result["error"])
            return result

        result["pool_created"] = True
        log.info("Successfully created EC pool: %s", pool_name)

        log.info("Checking allow_ec_optimizations flag on pool %s", pool_name)
        try:
            ec_opt_dict = rados_obj.get_pool_property(
                pool=pool_name, props="allow_ec_optimizations"
            )
            ec_opt_value = ec_opt_dict.get("allow_ec_optimizations")
            result["ec_optimizations_enabled"] = ec_opt_value

            log.info(
                "Pool %s - allow_ec_optimizations: %s (type: %s)",
                pool_name,
                ec_opt_value,
                type(ec_opt_value),
            )
        except Exception as e:
            # Expected on partially upgraded clusters where allow_ec_optimizations is not available
            log.warning(
                "Could not get allow_ec_optimizations for pool %s (expected on partial upgrade): %s",
                pool_name,
                str(e),
            )
            result["ec_optimizations_enabled"] = None
            log.info(
                "Pool %s - allow_ec_optimizations: Not available (partial upgrade scenario)",
                pool_name,
            )

        if not rados_obj.bench_write(pool_name=pool_name, byte_size="5K", max_objs=200):
            log.warning("Failed to write test object to pool %s", pool_name)
            result["error"] = f"Object write failed to pool {pool_name} "
        else:
            result["can_write_objects"] = True
            log.info("Successfully wrote test object to pool %s", pool_name)

    except Exception as e:
        result["error"] = str(e)
        log.error("Error in create_ec_pool_and_check_optimizations: %s", e)
        log.exception(e)

    return result


# Pool name prefixes for staggered upgrade pools - used for identification and cleanup
STAGGERED_UPGRADE_EC_POOL_PREFIX = "staggered-upgrade-ec-"
STAGGERED_UPGRADE_RBD_DATA_POOL_PREFIX = "staggered-upgrade-rbd-data-"
STAGGERED_UPGRADE_RBD_META_POOL_PREFIX = "staggered-upgrade-rbd-meta-"


def create_staggered_upgrade_pools(rados_obj, client_node, upgrade_case):
    """
    Create pools that will accumulate during staggered upgrades.

    Creates three pools per upgrade case:
    1. EC pool with application "ec-upgrade" for tracking upgrade state
    2. EC RBD data pool for RBD workloads
    3. Replicated metadata pool for RBD

    Writes test data to both EC and RBD pools to ensure they have content.
    Pools are discovered later using get_staggered_upgrade_pools().

    Args:
        rados_obj: RadosOrchestrator object
        client_node: Client node for executing commands
        upgrade_case: String identifying the upgrade case (e.g., "case_1_mgr_only")

    Returns:
        str or None: Error message if any step failed, None if all succeeded
    """
    # Generate unique pool names based on upgrade case
    ec_pool_name = f"{STAGGERED_UPGRADE_EC_POOL_PREFIX}{upgrade_case}"
    rbd_data_pool_name = f"{STAGGERED_UPGRADE_RBD_DATA_POOL_PREFIX}{upgrade_case}"
    rbd_meta_pool_name = f"{STAGGERED_UPGRADE_RBD_META_POOL_PREFIX}{upgrade_case}"
    rbd_image_name = f"staggered-upgrade-image-{upgrade_case}"

    log.info(
        "Creating staggered upgrade pools for %s: EC=%s, RBD-Data=%s, RBD-Meta=%s",
        upgrade_case,
        ec_pool_name,
        rbd_data_pool_name,
        rbd_meta_pool_name,
    )

    try:
        # Step 1: Create EC pool with "ec-upgrade" application and write test data
        if not rados_obj.create_erasure_pool(
            pool_name=ec_pool_name,
            profile_name=f"ec_profile_{ec_pool_name}",
            k=2,
            m=2,
            app_name="ec-upgrade",
        ):
            return f"Failed to create EC pool {ec_pool_name}"
        log.info("  Created EC pool: %s", ec_pool_name)

        # Write test data to EC pool
        assert rados_obj.bench_write(
            pool_name=ec_pool_name, byte_size="4K", max_objs=100, verify_stats=True
        ), f"Failed to write data to EC pool {ec_pool_name}"

        # Step 2: Create EC RBD data pool
        if not rados_obj.create_erasure_pool(
            pool_name=rbd_data_pool_name,
            profile_name=f"ec_profile_{rbd_data_pool_name}",
            k=2,
            m=2,
            app_name="rbd",
        ):
            return f"Failed to create RBD EC data pool {rbd_data_pool_name}"
        log.info("  Created RBD EC data pool: %s", rbd_data_pool_name)

        # Step 3: Create replicated metadata pool
        if not rados_obj.create_pool(pool_name=rbd_meta_pool_name, app_name="rbd"):
            return f"Failed to create RBD metadata pool {rbd_meta_pool_name}"
        log.info("  Created RBD metadata pool: %s", rbd_meta_pool_name)

        # Step 4: Create RBD image and write data (without mounting)
        create_cmd = (
            f"rbd create {rbd_image_name} --size 512M "
            f"--data-pool {rbd_data_pool_name} --pool {rbd_meta_pool_name}"
        )
        client_node.exec_command(cmd=create_cmd, sudo=True)
        log.info("  Created RBD image: %s", rbd_image_name)

        # Write data using rbd bench
        bench_cmd = (
            f"rbd bench {rbd_meta_pool_name}/{rbd_image_name} "
            f"--io-type write --io-size 4K --io-total 1M"
        )
        client_node.exec_command(cmd=bench_cmd, sudo=True, check_ec=False)
        log.info("  Test data written to RBD image")

        log.info("Staggered upgrade pools created successfully for %s", upgrade_case)
        return None  # Success

    except Exception as e:
        log.error("Error creating staggered upgrade pools: %s", e)
        return str(e)


def get_staggered_upgrade_pools(rados_obj):
    """
    Get all staggered upgrade pools created during upgrade process.

    Args:
        rados_obj: RadosOrchestrator object

    Returns:
        dict with ec_pools, rbd_data_pools, rbd_meta_pools, all_pools, upgrade_cases
    """
    result = {
        "ec_pools": [],
        "rbd_data_pools": [],
        "rbd_meta_pools": [],
        "all_pools": [],
        "upgrade_cases": [],
    }

    all_pools = rados_obj.list_pools()
    for pool_name in all_pools:
        if pool_name.startswith(STAGGERED_UPGRADE_EC_POOL_PREFIX):
            result["ec_pools"].append(pool_name)
            result["all_pools"].append(pool_name)
            result["upgrade_cases"].append(
                pool_name[len(STAGGERED_UPGRADE_EC_POOL_PREFIX) :]
            )
        elif pool_name.startswith(STAGGERED_UPGRADE_RBD_DATA_POOL_PREFIX):
            result["rbd_data_pools"].append(pool_name)
            result["all_pools"].append(pool_name)
        elif pool_name.startswith(STAGGERED_UPGRADE_RBD_META_POOL_PREFIX):
            result["rbd_meta_pools"].append(pool_name)
            result["all_pools"].append(pool_name)

    log.info(
        "Found staggered upgrade pools: EC=%d, RBD-data=%d, RBD-meta=%d",
        len(result["ec_pools"]),
        len(result["rbd_data_pools"]),
        len(result["rbd_meta_pools"]),
    )

    return result
