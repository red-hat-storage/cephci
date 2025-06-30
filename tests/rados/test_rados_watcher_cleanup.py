"""
Test Case: Verify RBD Watchers are Properly Cleaned Up

Workflow:
1. Create Watchers:
   - Input: List of image names, client node object(s)
   - Action: Map images to clients (single or multiple)
   - Validation: Confirm watchers are created

2. Delete Watchers:
   - Input: Cleanup method, mapping info from previous step
   - Supported Methods:
       a. unmap
       b. umount
       c. blacklist
       d. restart
       e. crash
   - Validation: Confirm watchers are removed

Approach 1: Single client
Approach 2: Multiple clients
"""

import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83616882: Verify watchers are properly cleaned up for RBD images.

    This test case focuses on the 'Single_rados_watcher' approach:
    Tests various cleanup methods with one client and one RBD image.
    Includes comprehensive cleanup and process handling.

    """

    log.info(run.__doc__)
    config = kw.get("config", {})
    log.info(f"DEBUG: Received config dictionary: {config}")

    single_rados_watcher_enabled = config.get("Single_rados_watcher")
    multiple_rados_watcher_enabled = config.get("Multiple_rados_watcher")
    if "Muliple_rados_watcher" in config and not multiple_rados_watcher_enabled:
        log.warning(
            "Assuming 'Multiple_rados_watcher' was intended and setting it to True."
        )
        multiple_rados_watcher_enabled = True

    log.info(f"DEBUG: Is Single_rados_watcher enabled? {single_rados_watcher_enabled}")
    log.info(
        f"DEBUG: Is Multiple_rados_watcher enabled? {multiple_rados_watcher_enabled}"
    )
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    client_nodes = ceph_cluster.get_nodes(role="client")

    # Define common test parameters
    # Retrieve from config, with default values if not provided
    pool = config.get("pool_name", "watcher_test_pool")
    single_client_image = config.get(
        "single_client_image_name", "img_single_client_test"
    )
    multi_client_images = config.get(
        "multi_client_image_names",
        ["img_multi_client_1", "img_multi_client_2", "img_multi_client_3"],
    )

    def create_watchers(image_name_param, client_param):
        """
        Maps an RBD image to a client node and returns the device path.
        """
        try:
            log.info(
                f"Mapping image {image_name_param} to client {client_param.hostname}"
            )
            map_cmd = f"rbd map {pool}/{image_name_param}"
            out, _ = client_param.exec_command(cmd=map_cmd, sudo=True)
            device = out.strip()  # The output of rbd map is the device path
            if not device.startswith("/dev/rbd"):
                raise Exception(
                    f"rbd map did not return a valid device path: {device}. Output: {out.strip()}"
                )
            log.info(f"Image {image_name_param} mapped to device {device}")
            return device
        except Exception as e:
            log.error(f"Failed to map image {image_name_param}: {e}")
            raise

    def _force_cleanup_device(client_param, device_param, image_name_param):
        """
        Attempts to forcefully unmount, kill processes, and unmap an RBD device.
        This is a common helper for 'unmap', 'restart', and 'crash' methods.
        """
        mount_path = f"/mnt/{image_name_param}"

        log.info(f"Attempting to unmount {mount_path} if it's mounted.")
        client_param.exec_command(cmd=f"umount {mount_path}", sudo=True, check_ec=False)
        time.sleep(1)

        if device_param:
            log.info(f"Checking for processes holding {device_param}.")
            lsof_cmd = f"lsof -t {device_param}"
            pids_out, _ = client_param.exec_command(
                cmd=lsof_cmd, sudo=True, check_ec=False
            )
            pids = [p.strip() for p in pids_out.splitlines() if p.strip()]

            if pids:
                log.warning(
                    f"Found PIDs holding {device_param}: {', '.join(pids)}. Attempting to kill them."
                )
                for pid in pids:
                    client_param.exec_command(
                        cmd=f"kill -9 {pid}", sudo=True, check_ec=False
                    )
                time.sleep(5)

                # Verify if PIDs are gone
                pids_after_kill_out, _ = client_param.exec_command(
                    cmd=lsof_cmd, sudo=True, check_ec=False
                )
                pids_after_kill = [
                    p.strip() for p in pids_after_kill_out.splitlines() if p.strip()
                ]
                if pids_after_kill:
                    log.error(
                        f"CRITICAL: PIDs still holding {device_param} after "
                        "kill -9: {', '.join(pids_after_kill)}. This is unexpected."
                    )
                else:
                    log.info(
                        f"All identified PIDs holding {device_param} terminated successfully."
                    )
            else:
                log.info(
                    f"No specific processes found holding {device_param} via lsof."
                )

            # Also try pkill for common RBD client processes as a fallback/additional measure
            log.info(
                "Attempting to pkill common RBD client processes (rbd-nbd, qemu-system-x86_64, etc.)."
            )
            client_param.exec_command(cmd="pkill -9 rbd-nbd", sudo=True, check_ec=False)
            client_param.exec_command(
                cmd="pkill -9 qemu-system-x86_64", sudo=True, check_ec=False
            )
            time.sleep(5)

            log.info(f"Attempting final rbd unmap {device_param}.")
            unmap_out, unmap_err = client_param.exec_command(
                cmd=f"rbd unmap {device_param}", sudo=True, check_ec=False
            )
            if unmap_err:
                log.warning(
                    f"Rbd unmap for {device_param} resulted in stderr: {unmap_err.strip()}"
                )
            if unmap_out:
                log.debug(f"Rbd unmap stdout: {unmap_out.strip()}")
            time.sleep(
                10
            )  # Extended wait for unmap to take effect and watcher to clear

            # Final check if still mapped
            check_mapped_cmd = "rbd showmapped"
            mapped_output, _ = client_param.exec_command(
                cmd=check_mapped_cmd, sudo=True, check_ec=False
            )
            if device_param in mapped_output:
                log.error(
                    f"CRITICAL: Failed to unmap {device_param}. It is still listed in rbd "
                    "showmapped output:\n{mapped_output.strip()}"
                )
                raise Exception(
                    f"Device {device_param} still mapped after forceful cleanup attempt."
                )
            else:
                log.info(
                    f"Successfully unmapped {device_param}. Not found in rbd showmapped output."
                )
        client_param.exec_command(cmd=f"rm -rf {mount_path}", sudo=True)
        log.info(f"Cleaned up mount point {mount_path}.")

    def _verify_watcher_cleanup(
        rados_obj_param, pool_param, image_name_param, method_param
    ):
        """
        Verifies if watchers for a given image have been cleaned up.
        """
        max_retries = 5
        retry_interval = 5
        watcher_cleaned = False
        for i in range(max_retries):
            log.info(
                f"Verifying watchers after '{method_param}' cleanup (Attempt {i+1}/{max_retries})..."
            )
            time.sleep(retry_interval)
            watchers = rados_obj_param.get_rbd_client_ips(pool_param, image_name_param)
            if not watchers:
                log.info(
                    f"Watchers successfully cleaned up after '{method_param}' method."
                )
                watcher_cleaned = True
                break
            else:
                log.warning(
                    f"Watchers still present after '{method_param}' cleanup (Attempt {i+1}): {watchers}. Retrying..."
                )

        if not watcher_cleaned:
            raise Exception(
                f"Watchers were NOT cleaned using method: {method_param}. Remaining: {watchers}"
            )

    def delete_watchers(method_param, client_param, image_name_param, device_param):
        """
        Deletes RBD image watchers using various specified methods.
        Includes enhanced logging and verification.
        """
        try:
            log.info(
                f"Attempting to delete watcher using '{method_param}' for image: {image_name_param}"
            )

            if (
                method_param == "unmap"
                or method_param == "restart"
                or method_param == "crash"
            ):
                log.info(
                    f"--- Entering '{method_param}' method cleanup for watcher:"
                    "{image_name_param} on {client_param.hostname} ---"
                )
                _force_cleanup_device(client_param, device_param, image_name_param)
                log.info(
                    f"--- Exited '{method_param}' method cleanup for watcher: {image_name_param} ---"
                )

            elif method_param == "umount":
                # Umount and unmap are handled by _force_cleanup_device now for robustness.
                _force_cleanup_device(client_param, device_param, image_name_param)
                log.info("Cleanup for umount method completed.")
            elif method_param == "blacklist":
                log.info(
                    f"Attempting to blacklist and un-blacklist client IPs for image: {image_name_param}"
                )
                watchers = rados_obj.get_rbd_client_ips(pool, image_name_param)
                if not watchers:
                    log.info(
                        f"No active watchers found for image {image_name_param} to blacklist."
                    )
                for ip in watchers:
                    log.info(f"Adding {ip} to Ceph OSD blacklist.")
                    client_param.exec_command(
                        cmd=f"ceph osd blacklist add {ip}", sudo=True
                    )
                time.sleep(10)
                for ip in watchers:
                    log.info(f"Removing {ip} from Ceph OSD blacklist.")
                    client_param.exec_command(
                        cmd=f"ceph osd blacklist rm {ip}", sudo=True
                    )
                    log.info(f"Un-blacklisted IP {ip}")

            # Verification for watcher cleanup (common to all methods)
            _verify_watcher_cleanup(rados_obj, pool, image_name_param, method_param)

        except Exception as e:
            log.error(
                f"An error occurred while trying to delete watcher "
                f"using {method_param} for image {image_name_param}: {e}"
            )
            raise Exception(
                f"Watcher cleanup failed for method '{method_param}'. Details: {e}"
            )

    try:
        log.info(f"Creating test pool: {pool}")
        rados_obj.create_pool(pool_name=pool)  # Create the test pool

        # ---------------- SINGLE CLIENT MODE ----------------
        if single_rados_watcher_enabled:
            doc = (
                "\n# CEPH-83616882"
                "\n Verify watchers are properly cleaned up for RBD images (Single Client)."
                "\n Tests various cleanup methods with one client and one RBD image."
                "\n Includes comprehensive cleanup and process handling."
                "\n\t 1. Create a fresh RBD image for the test."
                "\n\t 2. Map the RBD image to a single client to create a watcher."
                "\n\t 3. For each cleanup method (unmap, umount, blacklist, restart, crash):"
                "\n\t\t a. Apply the specified cleanup method."
                "\n\t\t b. Verify that the watcher is properly removed."
                "\n\t\t c. Clean up the RBD image and any lingering mount points."
            )
            log.info(doc)
            log.info(
                "Running Single Rados Watcher Test: Verifying watcher cleanup for a single client."
            )
            if not client_nodes:
                raise Exception(
                    "No client nodes found. Cannot run Single Rados Watcher Test."
                )

            client = client_nodes[0]
            methods = ["unmap", "umount", "blacklist", "restart", "crash"]

            for method in methods:
                log.info(
                    f"--- Single Client Test: Starting test for method: {method} ---"
                )
                device = None
                try:
                    # Create a fresh RBD image for each method test to ensure isolation.
                    log.info(
                        f"Creating image {single_client_image} for method {method}"
                    )
                    rados_obj.create_rbd_image(
                        pool_name=pool, img_name=single_client_image, size="1G"
                    )
                    # Create watcher by mapping the image
                    device = create_watchers(single_client_image, client)

                    # Special handling for 'umount' method: format and mount the image
                    if method == "umount":
                        log.info(
                            f"Formatting {device} and mounting to /mnt/{single_client_image}"
                        )
                        client.exec_command(cmd=f"mkfs.ext4 {device}", sudo=True)
                        client.exec_command(
                            cmd=f"mkdir -p /mnt/{single_client_image}", sudo=True
                        )
                        client.exec_command(
                            cmd=f"mount {device} /mnt/{single_client_image}", sudo=True
                        )
                        log.info(f"Mounted {device} to /mnt/{single_client_image}")

                    # Attempt to delete the watcher using the current method
                    delete_watchers(method, client, single_client_image, device)

                except Exception as e:
                    log.error(f"Single Client Test FAILED for method '{method}': {e}")
                    log.exception(e)
                    raise  # Re-raise to ensure the main try-except block catches this
                finally:
                    # Comprehensive cleanup after each method test, regardless of success/failure
                    log.info(
                        f"Cleaning up image {single_client_image} and its mount point after '{method}' test."
                    )
                    try:
                        # Force cleanup for the specific device and image if it's still mapped/mounted
                        _force_cleanup_device(client, device, single_client_image)
                        rados_obj.delete_rbd_image(
                            pool_name=pool, img_name=single_client_image
                        )
                    except Exception as cleanup_e:
                        log.warning(
                            f"Error during per-method cleanup for image {single_client_image}: {cleanup_e}"
                        )
                        log.exception(cleanup_e)
        # ---------------- MULTIPLE CLIENT MODE ----------------
        if multiple_rados_watcher_enabled:
            doc = (
                "\n# CEPH-83616882"
                "\n Verify watchers are properly cleaned up for RBD images (Multiple Clients)."
                "\n Tests various cleanup methods with multiple clients and multiple RBD images."
                "\n Includes comprehensive cleanup and process handling for concurrent scenarios."
                "\n\t 1. Create fresh RBD images for each client participating in the test."
                "\n\t 2. Map different RBD images to multiple clients to create concurrent watchers."
                "\n\t 3. For each client and each cleanup method (unmap, umount, blacklist, restart, crash):"
                "\n\t\t a. Apply the specified cleanup method for that client's image."
                "\n\t\t b. Verify that the watcher is properly removed for that specific image."
                "\n\t\t c. Clean up the RBD image and any lingering mount points on the respective client."
            )
            log.info(doc)
            log.info(
                "Running Multiple Rados Watcher Test: Verifying watcher cleanup across multiple clients."
            )
            methods = ["unmap", "umount", "blacklist", "restart", "crash"]
            clients = client_nodes[:3]

            for i, client in enumerate(clients):
                image = multi_client_images[i % len(multi_client_images)]
                log.info(
                    f"--- Multiple Client Test: Client {client.hostname}, Image: {image} ---"
                )
                for method in methods:
                    log.info(f"--- Method: {method} ---")
                    device = None  # Reset device for each method
                    try:
                        log.info(f"Creating image {image} for client {client.hostname}")
                        rados_obj.create_rbd_image(
                            pool_name=pool, img_name=image, size="1G"
                        )
                        device = create_watchers(image, client)

                        if method == "umount":
                            log.info(
                                f"Formatting {device} and mounting to /mnt/{image} on {client.hostname}"
                            )
                            client.exec_command(cmd=f"mkfs.ext4 {device}", sudo=True)
                            client.exec_command(cmd=f"mkdir -p /mnt/{image}", sudo=True)
                            client.exec_command(
                                cmd=f"mount {device} /mnt/{image}", sudo=True
                            )

                        delete_watchers(method, client, image, device)

                    except Exception as e:
                        log.error(
                            f"Multiple Client Test FAILED for client {client.hostname},"
                            f"image {image}, method {method}: {e}"
                        )
                        log.exception(e)
                        raise  # Re-raise to ensure main try-except block catches this

                    finally:
                        log.info(
                            f"Cleaning up after method {method} on client {client.hostname}"
                        )
                        try:
                            _force_cleanup_device(client, device, image)
                            rados_obj.delete_rbd_image(pool_name=pool, img_name=image)
                        except Exception as cleanup_e:
                            log.warning(
                                f"Error during cleanup after method {method} on "
                                f"client {client.hostname}: {cleanup_e}"
                            )
                            log.exception(cleanup_e)
        log.info("RBD watcher cleanup verification completed successfully.")
        return 0  # Return 0 for success if all tests pass

    except Exception as e:
        log.error(f"Test failed: {e}")
        log.exception(e)
        rados_obj.log_cluster_health()  # Log cluster health on failure
        return 1

    finally:
        log.info("\n************** Execution of finally block begins **************")
        rados_obj.log_cluster_health()  # Log cluster health at start of final cleanup

        # Final comprehensive cleanup to ensure no resources are left behind
        try:
            log.info(
                "Attempting final cleanup of any remaining mapped devices or images across all clients."
            )
            for client in client_nodes:  # Iterate through all clients
                try:
                    mapped_out, _ = client.exec_command(
                        cmd="rbd showmapped", sudo=True, check_ec=False
                    )
                    # Regex to match typical rbd showmapped output: ID POOL IMAGE SNAP DEVICE
                    # It's more robust than splitting by space directly
                    rbd_mapped_pattern = re.compile(
                        r"^\s*\d+\s+(\S+)\s+(\S+)\s+\S+\s+(\/dev\/rbd\d+)"
                    )
                    for line in mapped_out.splitlines():
                        match = rbd_mapped_pattern.match(line.strip())
                        if match:
                            mapped_pool = match.group(1)
                            mapped_image = match.group(2)
                            device_to_unmap = match.group(3)

                            if (
                                mapped_pool == pool
                            ):  # Only clean up images from our test pool
                                log.info(
                                    f"Found mapped device {device_to_unmap} (image: {mapped_image}) "
                                    f"from test pool on {client.hostname}. Attempting unmap and unmount."
                                )
                                # Use the centralized force cleanup helper
                                _force_cleanup_device(
                                    client, device_to_unmap, mapped_image
                                )
                                # Consider also deleting the image if it's from our test pool and still mapped
                                try:
                                    rados_obj.delete_rbd_image(
                                        pool_name=mapped_pool, img_name=mapped_image
                                    )
                                except Exception as e:
                                    log.warning(
                                        f"Could not delete remaining image "
                                        f"{mapped_pool}/{mapped_image} during final cleanup: {e}"
                                    )
                                    log.exception(e)

                except Exception as client_cleanup_e:
                    log.warning(
                        f"Error during client-specific final cleanup on {client.hostname}: {client_cleanup_e}"
                    )
                    log.exception(client_cleanup_e)

            # Delete the specific images used for the single and multiple client tests
            # These loops are needed as _force_cleanup_device only
            # handles mapping/unmapping, not image deletion directly.
            all_test_images = [single_client_image] + multi_client_images
            for image_to_delete in set(all_test_images):
                try:

                    log.info(
                        f"Attempting to delete image {pool}/{image_to_delete} in final cleanup block."
                    )
                    rados_obj.delete_rbd_image(pool_name=pool, img_name=image_to_delete)
                except Exception as img_del_e:
                    log.warning(
                        f"Failed to delete image {pool}/{image_to_delete} in final cleanup block: {img_del_e}"
                    )
                    log.exception(img_del_e)

            # Finally, delete the test pool
            log.info(f"Attempting to delete pool: {pool} in final cleanup block.")
            rados_obj.delete_pool(pool=pool)
            log.info(f"Successfully deleted pool: {pool} in final cleanup block.")
        except Exception as e:
            log.error(f"Critical error during final cleanup of pool: {e}")
            log.exception(e)

        # Check for crashes at the very end
        if rados_obj.check_crash_status():
            log.error(
                "Test finished, but crash detected during or after test execution."
            )
            pass
