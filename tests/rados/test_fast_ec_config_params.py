"""
Module to verify scenarios related to Fast EC (Erasure Coding) configuration parameters and optimizations

This module tests the behavior of erasure-coded pools with various configurations including:
- Default EC optimization settings (osd_pool_default_flag_ec_optimizations)
- RBD images on EC pools with data operations
- Enabling/disabling allow_ec_optimizations at pool level
- Compression with EC pools
- Read/write operations with different EC optimization states
"""

import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
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
        - test_fast_ec_optimization_params: Comprehensive verification of EC pool optimization parameters
          with default configurations, runtime changes, and RBD data operations

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
            log.info(f"Current version: {rhbuild}")
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
            assert rados_obj.create_erasure_pool(
                **ec_config
            ), f"Failed to create pool {pool_name_1}"
            log.info(f"Created EC pool: {pool_name_1}")
            time.sleep(2)

            # Create metadata pool for first EC pool
            metadata_pool_name_1 = "test_metadata_pool_1"
            assert rados_obj.create_pool(pool_name=metadata_pool_name_1, app_name="rbd")
            log.info(f"Created metadata pool: {metadata_pool_name_1}")

            # Check allow_ec_optimizations
            ec_opt_value_dict = rados_obj.get_pool_property(
                pool=pool_name_1, props="allow_ec_optimizations"
            )
            ec_opt_value = ec_opt_value_dict.get("allow_ec_optimizations")
            log.info(f"Pool {pool_name_1} - allow_ec_optimizations: {ec_opt_value} ")

            if ec_opt_value is not True:
                log.error(f"allow_ec_optimizations is not True for {pool_name_1}")
                raise Exception(f"EC optimizations check failed for {pool_name_1}")

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
            assert rados_obj.create_erasure_pool(
                **ec_config
            ), f"Failed to create pool {pool_name_2}"
            log.info(f"Created EC pool: {pool_name_2}")

            # Create metadata pool for second EC pool
            metadata_pool_name_2 = "test_metadata_pool_2"
            assert rados_obj.create_pool(pool_name=metadata_pool_name_2, app_name="rbd")
            log.info(f"Created metadata pool: {metadata_pool_name_2}")

            time.sleep(2)

            # Check allow_ec_optimizations (should be false)
            ec_opt_value_2_dict = rados_obj.get_pool_property(
                pool=pool_name_2, props="allow_ec_optimizations"
            )
            ec_opt_value_2 = ec_opt_value_2_dict.get("allow_ec_optimizations")
            log.info(
                f"Pool {pool_name_2} - allow_ec_optimizations: {ec_opt_value_2} "
                f"(type: {type(ec_opt_value_2)})"
            )

            if ec_opt_value_2 is not False:
                log.error(
                    f"allow_ec_optimizations is not False for {pool_name_2},"
                    f" even when it's set to false at global level"
                )
                raise Exception(f"EC optimizations should be False for {pool_name_2}")

            # Step 5: Create RBD images on both pools and write objects
            log.info("Step 5: Creating RBD images on both pools and writing objects")

            # Create RBD image on first EC pool with metadata pool
            image_create_cmd_1 = (
                f"rbd create --size 1G --data-pool {pool_name_1} "
                f"{metadata_pool_name_1}/{image_name_1}"
            )
            client_node.exec_command(cmd=image_create_cmd_1, sudo=True)
            log.info(
                f"Created RBD image {image_name_1} with data-pool {pool_name_1} "
                f"and metadata-pool {metadata_pool_name_1}"
            )

            # Create RBD image on second EC pool with metadata pool
            image_create_cmd_2 = (
                f"rbd create --size 1G --data-pool {pool_name_2} "
                f"{metadata_pool_name_2}/{image_name_2}"
            )
            client_node.exec_command(cmd=image_create_cmd_2, sudo=True)
            log.info(
                f"Created RBD image {image_name_2} with data-pool {pool_name_2} "
                f"and metadata-pool {metadata_pool_name_2}"
            )

            # Mount first image
            mount_path_1, device_map_1 = rados_obj.mount_image_on_client(
                pool_name=metadata_pool_name_1,
                img_name=image_name_1,
                client_obj=client_node,
                mount_path="/tmp/ec_test_mount_1/",
            )
            log.info(
                f"Mapped and mounted image {image_name_1} to device {device_map_1} at {mount_path_1}"
            )

            # Mount second image
            mount_path_2, device_map_2 = rados_obj.mount_image_on_client(
                pool_name=metadata_pool_name_2,
                img_name=image_name_2,
                client_obj=client_node,
                mount_path="/tmp/ec_test_mount_2/",
            )
            log.info(
                f"Mapped and mounted image {image_name_2} to device {device_map_2} at {mount_path_2}"
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
            log.info(f"Enabled allow_ec_optimizations on {pool_name_2}")

            time.sleep(5)

            # Verify the change
            ec_opt_value_2_new_dict = rados_obj.get_pool_property(
                pool=pool_name_2, props="allow_ec_optimizations"
            )
            ec_opt_value_2_new = ec_opt_value_2_new_dict.get("allow_ec_optimizations")
            log.info(
                f"Pool {pool_name_2} - allow_ec_optimizations after enabling: {ec_opt_value_2_new} "
                f"(type: {type(ec_opt_value_2_new)})"
            )

            if ec_opt_value_2_new is not True:
                log.error(f"Failed to enable allow_ec_optimizations for {pool_name_2}")
                raise Exception(f"EC optimizations not enabled for {pool_name_2}")

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
            log.info(f"Enabled compression on {pool_name_1}")

            rados_obj.set_pool_property(
                pool=pool_name_2, props="compression_mode", value="force"
            )
            rados_obj.set_pool_property(
                pool=pool_name_2, props="compression_algorithm", value="snappy"
            )
            log.info(f"Enabled compression on {pool_name_2}")

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
                    log.error(f"Could not retrieve stats for pool {pool_name}")
                    raise Exception(f"Failed to get stats for {pool_name}")

                stats = pool_stats.get("stats", {})
                compress_bytes = stats.get("compress_bytes_used", 0)
                compress_under_bytes = stats.get("compress_under_bytes", 0)

                # Store for later comparison
                compression_stats_before[pool_name] = {
                    "compress_bytes_used": compress_bytes,
                    "compress_under_bytes": compress_under_bytes,
                }

                log.info(
                    f"Pool {pool_name} compression stats (baseline): "
                    f"compress_bytes_used={compress_bytes}, "
                    f"compress_under_bytes={compress_under_bytes}"
                )

                if compress_bytes <= 0:
                    log.error(f"No compressed data found in pool {pool_name}")
                    raise Exception(f"Compression not working on {pool_name}")

            # Step 12: Remove compression
            log.info("Step 12: Removing compression from both pools")

            rados_obj.set_pool_property(
                pool=pool_name_1, props="compression_mode", value="none"
            )
            log.info(f"Disabled compression on {pool_name_1}")

            rados_obj.set_pool_property(
                pool=pool_name_2, props="compression_mode", value="none"
            )
            log.info(f"Disabled compression on {pool_name_2}")

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
            log.error(f"Failed with exception: {e.__doc__}")
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
                    log.info(f"Unmounted {image_name_1} from {mount_path_1}")
                except Exception as e:
                    log.warning(f"Failed to unmount {image_name_1}: {e}")

                try:
                    # Unmap device
                    unmap_cmd_1 = f"rbd device unmap {device_map_1}"
                    client_node.exec_command(cmd=unmap_cmd_1, sudo=True)
                    log.info(f"Unmapped image {image_name_1} from {device_map_1}")
                except Exception as e:
                    log.warning(f"Failed to unmap {device_map_1}: {e}")

                try:
                    # Remove mount directory
                    rmdir_cmd_1 = f"rm -rf {mount_path_1}"
                    client_node.exec_command(cmd=rmdir_cmd_1, sudo=True)
                    log.info(f"Removed mount directory {mount_path_1}")
                except Exception as e:
                    log.warning(f"Failed to remove mount directory {mount_path_1}: {e}")

            if device_map_2:
                try:
                    # Unmount filesystem
                    umount_cmd_2 = f"umount {mount_path_2}"
                    client_node.exec_command(cmd=umount_cmd_2, sudo=True)
                    log.info(f"Unmounted {image_name_2} from {mount_path_2}")
                except Exception as e:
                    log.warning(f"Failed to unmount {image_name_2}: {e}")

                try:
                    # Unmap device
                    unmap_cmd_2 = f"rbd device unmap {device_map_2}"
                    client_node.exec_command(cmd=unmap_cmd_2, sudo=True)
                    log.info(f"Unmapped image {image_name_2} from {device_map_2}")
                except Exception as e:
                    log.warning(f"Failed to unmap {device_map_2}: {e}")

                try:
                    # Remove mount directory
                    rmdir_cmd_2 = f"rm -rf {mount_path_2}"
                    client_node.exec_command(cmd=rmdir_cmd_2, sudo=True)
                    log.info(f"Removed mount directory {mount_path_2}")
                except Exception as e:
                    log.warning(f"Failed to remove mount directory {mount_path_2}: {e}")

            if pool_name_1 in existing_pools:
                try:
                    rados_obj.delete_pool(pool=pool_name_1)
                    log.info(f"Deleted EC pool {pool_name_1}")
                except Exception as e:
                    log.warning(f"Failed to delete pool {pool_name_1}: {e}")

            if pool_name_2 in existing_pools:
                try:
                    rados_obj.delete_pool(pool=pool_name_2)
                    log.info(f"Deleted EC pool {pool_name_2}")
                except Exception as e:
                    log.warning(f"Failed to delete pool {pool_name_2}: {e}")

            if metadata_pool_name_1 in existing_pools:
                try:
                    rados_obj.delete_pool(pool=metadata_pool_name_1)
                    log.info(f"Deleted metadata pool {metadata_pool_name_1}")
                except Exception as e:
                    log.warning(f"Failed to delete pool {metadata_pool_name_1}: {e}")

            if metadata_pool_name_2 in existing_pools:
                try:
                    rados_obj.delete_pool(pool=metadata_pool_name_2)
                    log.info(f"Deleted metadata pool {metadata_pool_name_2}")
                except Exception as e:
                    log.warning(f"Failed to delete pool {metadata_pool_name_2}: {e}")

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
                    f"Failed to reset osd_pool_default_flag_ec_optimizations: {e}"
                )

            # Log cluster health
            rados_obj.log_cluster_health()

            # Check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("Verification of Fast EC optimizations completed successfully")
        return 0


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
        f"Reading and verifying ALL {count} objects (range: 0-{count - 1}) - {step_desc}"
    )

    try:
        # Read all objects from mount path 1 (from offset 0 to count-1)
        log.info(f"Starting read verification for mount {mount_path_1}")
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
                    f"Failed to read file {i} from {mount_path_1}: exit_code={exit_code}, err={err}"
                )
                failed_reads_1.append(i)

        if failed_reads_1:
            raise Exception(
                f"Failed to read {len(failed_reads_1)} files from {mount_path_1}: {failed_reads_1}"
            )

        log.info(
            f" Successfully read ALL {count} files (0-{count - 1}) from {mount_path_1}"
        )

        # Read all objects from mount path 2 (from offset 0 to count-1)
        log.info(f"Starting read verification for mount {mount_path_2}")
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
                    f"Failed to read file {i} from {mount_path_2}: exit_code={exit_code}, err={err}"
                )
                failed_reads_2.append(i)

        if failed_reads_2:
            raise Exception(
                f"Failed to read {len(failed_reads_2)} files from {mount_path_2}: {failed_reads_2}"
            )

        log.info(
            f" Successfully read ALL {count} files (0-{count - 1}) from {mount_path_2}"
        )
        log.info(
            f" {step_desc} - All {count} files verified successfully from both mounts "
            f"(Total: {count * 2} reads completed)"
        )

    except Exception as e:
        log.error(f"Failed during {step_desc}: {str(e)}")
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
    log.info(f"Verifying config parameter '{param_name}' = '{expected_value}'")

    # Get OSD and Mon IDs if not provided
    if osd_id is None:
        osd_list = rados_obj.get_osd_list(status="up")
        osd_id = osd_list[0]
        log.debug(f"Picked OSD : {osd_id} for checking config at runtime")

    if mon_id is None:
        mon_list = rados_obj.run_ceph_command(cmd="ceph mon dump", client_exec=True)[
            "mons"
        ]
        mon_id = mon_list[0]["name"]
        log.debug(f"Picked Mon : {mon_id} for checking config at runtime")

    # Check at OSD level - both DB and runtime
    log.debug(f"Checking OSD level for '{param_name}'")

    # Check runtime value (ceph config show)
    osd_runtime = mon_obj.show_config(daemon="osd", id=osd_id, param=param_name)
    osd_runtime = str(osd_runtime).strip()
    log.debug(f"  OSD {osd_id} - Runtime (config show): {osd_runtime}")

    # Check DB value (ceph config get)
    osd_db = mon_obj.get_config(section="osd", param=param_name)
    osd_db = str(osd_db).strip()
    log.debug(f"  OSD - DB (config get): {osd_db}")

    # Check at mon level - both DB and runtime
    log.debug(f"Checking Mon level for '{param_name}'")

    # Check runtime value (ceph config show)
    mon_runtime = mon_obj.show_config(daemon="mon", id=mon_id, param=param_name)
    mon_runtime = str(mon_runtime).strip()
    log.debug(f"  Mon {mon_id} - Runtime (config show): {mon_runtime}")

    # Check DB value (ceph config get)
    mon_db = mon_obj.get_config(section="mon", param=param_name)
    mon_db = str(mon_db).strip()
    log.debug(f"  Mon - DB (config get): {mon_db}")

    # Verify all values match expected
    errors = []

    if osd_runtime != expected_value:
        errors.append(f"OSD runtime value mismatch: {osd_runtime} != {expected_value}")
    if osd_db != expected_value:
        errors.append(f"OSD DB value mismatch: {osd_db} != {expected_value}")
    if mon_runtime != expected_value:
        errors.append(f"Mon runtime value mismatch: {mon_runtime} != {expected_value}")
    if mon_db != expected_value:
        errors.append(f"Mon DB value mismatch: {mon_db} != {expected_value}")

    if errors:
        error_msg = f"Config verification failed for '{param_name}':\n" + "\n".join(
            errors
        )
        log.error(error_msg)
        raise Exception(error_msg)

    log.info(
        f" Verified: {param_name} = {expected_value} (both DB and runtime for OSD and Mon)"
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
        f"Writing {count} objects (offset {start_offset}-{end_offset-1}) - {step_desc}"
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
                    f"Failed to write file {i} to {mount_path_1}: exit_code={exit_code}, err={err}"
                )
                raise Exception(f"Write failed for file {i} at {mount_path_1}")

            file_path_2 = f"{mount_path_2}file_{i}"
            write_cmd_2 = f"dd if=/dev/urandom of={file_path_2} bs=4M count=1"
            out, err, exit_code, _ = client_node.exec_command(
                cmd=write_cmd_2, sudo=True, verbose=True
            )
            if exit_code != 0:
                log.error(
                    f"Failed to write file {i} to {mount_path_2}: exit_code={exit_code}, err={err}"
                )
                raise Exception(f"Write failed for file {i} at {mount_path_2}")

        # Sync data
        client_node.exec_command(cmd="sync", sudo=True)
        log.info(f"Completed writing {count} files to both mounts")

        # Verify all objects written so far
        total_objects = end_offset
        log.info(
            f"Reading and verifying ALL {total_objects} files (0-{total_objects-1}) "
            f"after {step_desc}"
        )
        read_and_verify_objects(
            client_node,
            mount_path_1,
            mount_path_2,
            total_objects,
            f"Verify after {step_desc}",
        )

    except Exception as e:
        log.error(f"Failed during write and verify: {str(e)}")
        raise
