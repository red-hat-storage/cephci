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
from typing import Dict, List, Optional

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83616882: Verify RBD Watchers are Properly Cleaned Up.
    https://bugzilla.redhat.com/bugzilla/show_bug.cgi?id=2336352

    This test case validates comprehensive watcher cleanup for RBD images
    across single and multiple client scenarios using various methods.

    Workflow:
    1.  Create Pool: Establishes a dedicated test pool.
    2.  Create RBD Images: Generates images for single and multi-client tests.
    3.  Create Watchers: Maps images to clients, validating watcher creation.
    4.  Delete Watchers (Phase 2): Applies various cleanup methods (unmap, umount,
        restart, crash) and confirms watcher removal for specific images/clients.
    5.  Delete Watchers (Phase 3 - Blacklist): Dedicated test for blacklist method.
    6.  Final Cleanup: Ensures all test-related resources are removed.

    Args:
        ceph_cluster (CephCluster): The Ceph cluster object.
        **kw: Keyword arguments for test configuration.
              Examples:
                - config:
                    Single_Client_watcher: true
                    pool_name: my_test_pool
                - config:
                    Multiple_Client_watcher: true
                    pool_name: another_pool

    Returns:
        int: 0 if the test passes, 1 if it fails.
    """
    log.info(run.__doc__)
    config: Dict = kw.get("config", {})
    log.info("Debug: Received config dictionary: %s", config)
    cephadm: CephAdmin = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj: RadosOrchestrator = RadosOrchestrator(node=cephadm)
    client_nodes: List = ceph_cluster.get_nodes(role="client")
    pool: str = config.get("pool_name", "watcher_test_pool")

    # We need one image for each cleanup method (unmap, umount, restart, crash)
    # plus one extra image specifically for the blacklist test.

    cleanup_methods: List[str] = [
        "unmap",
        "umount",
        "restart",
        "crash",
    ]
    num_images_to_create_per_client: int = 5

    # Tracks mapped devices for robust cleanup: {client_hostname: {image_name: device_path}}
    mapped_devices: Dict[str, Dict[str, str]] = {}
    # List to keep track of all images created for final cleanup
    all_created_images: List[str] = []

    try:
        log.info("Creating test pool: %s", pool)
        # Passing app_name="rbd" here, so create_pool handles the rbd pool init
        if not rados_obj.create_pool(pool_name=pool, app_name="rbd"):
            raise Exception(f"Failed to create and initialize pool {pool}")
        log.info(f"RBD pool {pool} created and initialized.")

        # --- Phase 1: Create, Map, Mount Images and Verify Watchers ---
        log.info(
            "\n--- Phase 1: Creating, Mapping, Mounting Images and Verifying Watchers ---"
        )

        # Handle Single Client scenario if enabled
        if config.get("Single_Client_watcher"):
            log.info("Preparing images for Single Client Watcher Test.")
            if not client_nodes:
                raise Exception(
                    "No client nodes found. Cannot run Single Rados Watcher Test."
                )

            client = client_nodes[0]
            log.info("Processing client: %s", client.hostname)

            for i in range(num_images_to_create_per_client):
                image_name: str = f"img_single_client_test_{i}"
                all_created_images.append(
                    image_name
                )  # Add to the list for final cleanup
                try:
                    _add_watchers(
                        client_obj=client,
                        pool=pool,
                        rados_orchestrator_obj=rados_obj,
                        image_name=image_name,
                        mapped_devices=mapped_devices,
                    )
                except Exception as e:
                    log.error(e)
                    log.error("Failed to create image and watcher for {image_name}")
                    return 1  # Fail test if watcher creation fails

        # Handle Multiple Client scenario if enabled
        if config.get("Multiple_Client_watcher"):
            log.info("Preparing images for Multiple Rados Watcher Test.")
            clients_for_multi_test: List = client_nodes

            if len(clients_for_multi_test) < 2:
                raise Exception(
                    "Not enough client nodes (need at least 2) for Multiple Rados Watcher Test."
                )

            for client_idx, client in enumerate(clients_for_multi_test):
                log.info("Processing client: %s", client.hostname)
                for i in range(num_images_to_create_per_client):
                    image_name: str = f"img_multi_client_{client_idx}_{i}"
                    all_created_images.append(
                        image_name
                    )  # Add to the list for final cleanup
                    try:
                        _add_watchers(
                            client_obj=client,
                            pool=pool,
                            rados_orchestrator_obj=rados_obj,
                            image_name=image_name,
                            mapped_devices=mapped_devices,
                        )
                    except Exception as e:
                        log.error(e)
                        log.error(
                            f"Failed to create image and watcher for {image_name}"
                        )
                        return 1  # Fail test if watcher creation fails

        log.info(
            "\n--- Phase 1: All images created, mapped, mounted, and watchers verified. Proceeding to cleanup. ---"
        )

        # --- Phase 2: Delete Watchers based on various methods (excluding blacklist) ---
        log.info(
            "\n--- Phase 2: Initiating Watcher Cleanup Verification (Excluding Blacklist) ---"
        )
        cleanup_methods: List[str] = [
            "unmap",
            "umount",
            "restart",
            "crash",
        ]
        # Ensure we have enough images for each method per client
        if num_images_to_create_per_client < len(cleanup_methods):
            raise Exception(
                f"Not enough images created ({num_images_to_create_per_client}) for all"
                "cleanup methods ({len(cleanup_methods)}). Increase num_images_to_create_per_client."
            )

        # Iterate through mapped devices to apply cleanup methods
        for client_hostname, images_info in list(mapped_devices.items()):
            # Replaced get_host_obj_from_hostname with ceph_cluster.get_node_by_hostname
            client = ceph_cluster.get_node_by_hostname(client_hostname)

            if not client:
                log.warning(
                    "Client %s not found, skipping cleanup for its images.",
                    client_hostname,
                )
                continue

            # Associate images with methods directly for this phase
            # Assuming images are named img_*_0, img_*_1, etc.
            images_for_method_test = sorted(
                images_info.keys()
            )  # Ensure consistent order

            # Adjusting cleanup_methods to match available images
            cleanup_methods_to_run = cleanup_methods[: len(images_for_method_test)]

            for method_idx, method in enumerate(cleanup_methods_to_run):
                image_name = images_for_method_test[method_idx]
                device_path = images_info[image_name]

                log.info(
                    "Testing cleanup for Image: %s on Client: %s using method '%s'",
                    image_name,
                    client.hostname,
                    method,
                )

                try:
                    # Execute the specific watcher deletion method and verify
                    _delete_watcher_and_verify(
                        method,
                        client,
                        image_name,
                        device_path,
                        rados_obj,
                        pool,
                        client.ip_address,
                    )
                    log.info(
                        "Cleanup method '%s' verified successfully for %s.",
                        method,
                        image_name,
                    )
                except Exception as e:
                    log.error(
                        "Cleanup method '%s' FAILED for image '%s': %s",
                        method,
                        image_name,
                        e,
                    )
                    log.exception(e)
                    return 1
                finally:
                    # Ensure cleanup after each method test iteration
                    log.info(
                        "Ensuring clean state after '%s' test for %s.",
                        method,
                        image_name,
                    )
                    # The image should ideally be fully unmapped here
                    _perform_individual_cleanup(
                        client,
                        device_path,  # Use the device path for the current image
                        image_name,
                        rados_obj,
                        pool,
                        mapped_devices,
                    )
                    # Small delay to allow cluster state to settle
                    time.sleep(5)

        log.info(
            "\n--- Phase 2: Non-Blacklist Watcher Cleanup Verification Completed. ---"
        )

        # --- Phase 3: Dedicated Blacklist Cleanup Verification ---
        log.info(
            "\n--- Phase 3: Initiating Dedicated Blacklist Cleanup Verification ---"
        )

        # Only run blacklist test
        blacklist_test_passed = False
        blacklist_image_name = None  # Will be determined dynamically
        blacklist_device = None  # Initialize device for blacklist test

        if client_nodes:
            client = client_nodes[0]  # Using the first client for the blacklist test
            client_hostname = client.hostname  # Get hostname

            # Finding an existing mapped image for the blacklist test
            # Iterate through mapped_devices for the current client
            if client_hostname in mapped_devices and mapped_devices[client_hostname]:
                for img_name, dev_path in mapped_devices[client_hostname].items():
                    blacklist_image_name = img_name
                    blacklist_device = dev_path
                    log.info(
                        "Found existing image {blacklist_image_name} ({blacklist_device}) "
                        "on {client_hostname} for blacklist test."
                    )
                    break

            # If no existing image is found, create a new one
            if not blacklist_image_name:
                log.info(
                    "No existing mapped image found for blacklist test. Creating a new one."
                )
                # Construct a unique name for the new image
                new_image_index = len(all_created_images)  # to get a unique index
                blacklist_image_name = f"img_blacklist_new_{new_image_index}"

                try:
                    # _add_watchers handles image creation, mapping, mounting, and initial watcher verification
                    _add_watchers(
                        client_obj=client,
                        pool=pool,
                        rados_orchestrator_obj=rados_obj,
                        image_name=blacklist_image_name,
                        mapped_devices=mapped_devices,
                    )
                    # After _add_watchers, the image should be in mapped_devices
                    if (
                        client_hostname in mapped_devices
                        and blacklist_image_name in mapped_devices[client_hostname]
                    ):
                        blacklist_device = mapped_devices[client_hostname][
                            blacklist_image_name
                        ]
                    else:
                        raise Exception(
                            f"Failed to retrieve device path for newly created image {blacklist_image_name}."
                        )

                    all_created_images.append(
                        blacklist_image_name
                    )  # Track the newly created image
                    log.info(
                        "Successfully created new image {blacklist_image_name} ({blacklist_device}) "
                        "on {client_hostname} for blacklist test."
                    )
                except Exception as e:
                    log.error("Failed to create new image for blacklist test: {e}")
                    log.exception(e)
                    return 1

            # Proceed only if we have a valid image and device (either existing or newly created)
            if blacklist_image_name and blacklist_device:
                try:
                    log.info(
                        "--- Applying cleanup method: 'blacklist' for %s on %s ---",
                        blacklist_image_name,
                        client.hostname,
                    )
                    _delete_watcher_and_verify(
                        "blacklist",
                        client,
                        blacklist_image_name,
                        blacklist_device,
                        rados_obj,
                        pool,
                        client.ip_address,
                    )
                    log.info(
                        "Cleanup method 'blacklist' verified successfully for %s.",
                        blacklist_image_name,
                    )
                    blacklist_test_passed = True
                except Exception as e:
                    log.error(
                        "Cleanup method 'blacklist' FAILED for image '%s': %s",
                        blacklist_image_name,
                        e,
                    )
                    log.exception(e)
                    blacklist_test_passed = False
                finally:
                    log.info(
                        "Ensuring clean state after 'blacklist' test for %s.",
                        blacklist_image_name,
                    )
                    # Ensure the blacklist is removed and image is unmapped/deleted
                    log.info(
                        "Attempting to remove client %s from Ceph OSD blacklist.",
                        client.ip_address,
                    )
                    try:
                        out, err, rc = client.exec_command(
                            cmd=f"ceph osd blacklist rm {client.ip_address}", sudo=True
                        )
                        if rc != 0 and "No such entry" not in err:
                            log.error(
                                "Failed to remove {client.ip_address} from blacklist: {err}"
                            )
                            raise Exception(
                                f"Blacklist removal failed with error: {err}"
                            )
                        log.info(
                            "Successfully removed {client.ip_address} from blacklist."
                        )
                    except Exception as e:
                        log.warning(
                            f"Could not remove {client.ip_address} from blacklist"
                            f"(might already be removed or another error): {e}"
                        )
                    time.sleep(5)

                    if (
                        blacklist_device
                    ):  # cleanup if the device was successfully mapped
                        _perform_individual_cleanup(
                            client,
                            blacklist_device,
                            blacklist_image_name,
                            rados_obj,
                            pool,
                            mapped_devices,
                        )
                    else:
                        log.info(
                            "Blacklist test image %s was not mapped, skipping individual cleanup for device.",
                            blacklist_image_name,
                        )
            else:
                log.error(
                    "Blacklist test could not proceed: no image or device available."
                )
                return 1
        else:
            log.info("Skipping blacklist test: No client nodes available.")

        if not blacklist_test_passed:
            return 1

        log.info("RBD watcher cleanup verification completed successfully.")
        return 0
    except Exception as e:
        log.error("Test failed: %s", e)
        log.exception(e)
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info("\n--- Final Cleanup Begins ---")
        rados_obj.log_cluster_health()
        try:
            log.info(
                "Attempting final cleanup of any remaining mapped devices or images."
            )
            _final_cleanup_mapped_devices(client_nodes, pool, rados_obj, mapped_devices)
            _final_cleanup_rbd_images(pool, rados_obj, all_created_images)
            log.info("Attempting to delete pool: %s.", pool)
            if not rados_obj.delete_pool(pool=pool):
                log.error("Failed to delete pool {pool} during final cleanup.")
        except Exception as e:
            log.error("Critical error during final cleanup of pool: %s", e)
            log.exception(e)
        if rados_obj.check_crash_status():
            log.error(
                "Test finished, but crash detected during or after test execution."
            )


# e.g., ("/mnt/img_single_client_test_0", "/dev/rbd0")
def _add_watchers(
    client_obj,
    pool: str,
    rados_orchestrator_obj: RadosOrchestrator,
    image_name: str,
    mapped_devices: Dict[str, Dict[str, str]],
) -> None:
    """
    Creates an RBD image, maps and mounts it on the client, and verifies watcher presence.
    Updates the mapped_devices dictionary.

    Args:
        client_obj (Node): The client node object.
        pool (str): The name of the pool.
        rados_orchestrator_obj (RadosOrchestrator): The RadosOrchestrator object.
        image_name (str): The name of the RBD image to create and map.
        mapped_devices (Dict): Dictionary to track mapped devices for cleanup.

    Raises:
        Exception: If image creation, mapping, mounting, or watcher verification fails.
    """
    log.info("Creating image '%s' for client %s.", image_name, client_obj.hostname)

    rados_orchestrator_obj.create_rbd_image(
        pool_name=pool, img_name=image_name, size="1G"
    )
    log.info("The {image_name} created.")

    desired_mount_path: str = f"/mnt/{image_name}"

    # Correctly assign the returned values from mount_image_on_client
    # The first returned value is the mount directory (e.g., /mnt/img_name)
    # The second returned value is the RBD device path (e.g., /dev/rbd0)
    actual_mount_directory, rbd_block_device_path = (
        rados_orchestrator_obj.mount_image_on_client(
            pool_name=pool,
            img_name=image_name,
            client_obj=client_obj,
            mount_path=desired_mount_path,
        )
    )

    # Check if mounting was successful by verifying actual_mount_directory and rbd_block_device_path are not None
    if actual_mount_directory and rbd_block_device_path:
        log.info(
            "Image %s mapped to %s and mounted to %s on %s",
            image_name,
            rbd_block_device_path,  # Log the actual device path (e.g., /dev/rbd0)
            actual_mount_directory,  # Log the actual mount directory (e.g., /mnt/img_name)
            client_obj.hostname,
        )
        # Store the actual rbd_block_device_path for cleanup
        mapped_devices.setdefault(client_obj.hostname, {})[
            image_name
        ] = rbd_block_device_path
        log.debug(
            "Added %s to mapped_devices: %s",
            image_name,
            mapped_devices[client_obj.hostname][image_name],
        )

        # Now verify the watcher
        _verify_watcher_presence(
            image_name,
            client_obj,
            pool,
            rados_orchestrator_obj,  # client_obj.ip_address is now fetched inside
        )
    else:
        raise Exception(
            f"Failed to map and mount image {image_name} on client {client_obj.hostname}. "
            f"Actual mount dir: {actual_mount_directory}, RBD device: {rbd_block_device_path}"
        )


def _verify_watcher_presence(
    image_name: str,
    client_obj,
    pool: str,
    rados_orchestrator_obj: RadosOrchestrator,
) -> str:
    """
    Verifies the presence of a specific RBD watcher for an image.

    Args:
        image_name (str): The name of the RBD image.
            Example: "my_rbd_image"
        client_obj (Node): The client node object where the image is mapped.
            Example: ceph_cluster.get_nodes(role="client")[0]
        pool (str): The name of the pool where the image resides.
            Example: "watcher_test_pool"
        rados_orchestrator_obj (RadosOrchestrator): The RadosOrchestrator object for
        executing Ceph commands and getting watcher info.
            Example: rados_obj (instance created in run function)

    Returns:
        str: The client IP if the watcher is successfully found.

    Raises:
        Exception: If the watcher is not found after multiple retries.
    """
    client_ip = client_obj.ip_address  # Fetch client_ip directly from client_obj
    log.info("Verifying watcher created for %s from %s", image_name, client_ip)
    max_retries: int = 10
    for i in range(max_retries):
        watchers: List[str] = rados_orchestrator_obj.get_rbd_client_ips(
            pool, image_name
        )
        if watchers and client_ip in watchers:
            log.info(
                "Watcher successfully created for %s by %s.", image_name, client_ip
            )
            return True
        log.debug(
            "Watcher not yet seen for %s by %s. Retrying... (Attempt %d)",
            image_name,
            client_ip,
            i + 1,
        )
        time.sleep(2)
    raise Exception(
        "Watcher for %s not found after mapping from %s. Current watchers: %s"
        % (image_name, client_ip, watchers)
    )


def _verify_watcher_cleanup(
    rados_orchestrator_obj: RadosOrchestrator,
    pool: str,
    image_name: str,
    method: str,
    client_ip: Optional[str] = None,
) -> None:
    """
    Verifies that RBD watchers for a given image are properly cleaned up.

    Args:
        rados_orchestrator_obj (RadosOrchestrator): The RadosOrchestrator object for
        executing Ceph commands and getting watcher info.
            Example: rados_obj (instance created in run function)
        pool (str): The name of the pool.
            Example: "watcher_test_pool"
        image_name (str): The name of the RBD image.
            Example: "img_single_client_test_0"
        method (str): The cleanup method being tested (for logging purposes).
            Example: "unmap", "blacklist"
        client_ip (str, optional): The specific client IP to check for removal.
        If None, checks if all watchers are removed.
            Example: "192.168.1.100" (or None)

    Returns:
        None

    Raises:
        Exception: If watchers are still present after multiple retries.
    """
    max_retries: int = 10
    retry_interval: int = 5
    watcher_cleaned: bool = False
    for i in range(max_retries):
        log.info(
            "Verifying watchers after '%s' cleanup (Attempt %d/%d)...",
            method,
            i + 1,
            max_retries,
        )
        time.sleep(retry_interval)
        watchers: List[str] = rados_orchestrator_obj.get_rbd_client_ips(
            pool, image_name
        )
        if client_ip:
            if client_ip not in watchers:
                log.info(
                    "Watcher from %s successfully removed for %s.",
                    client_ip,
                    image_name,
                )
                watcher_cleaned = True
                break
            else:
                log.warning(
                    "Watcher from %s still present for %s after '%s' cleanup. Current watchers: %s. Retrying...",
                    client_ip,
                    image_name,
                    method,
                    watchers,
                )
        else:  # Case where we expect ALL watchers to be gone
            if not watchers:
                log.info("All watchers successfully cleaned up for %s.", image_name)
                watcher_cleaned = True
                break
            else:
                log.warning(
                    "Watchers still present for %s after '%s' cleanup: %s. Retrying...",
                    image_name,
                    method,
                    watchers,
                )
    if not watcher_cleaned:
        if client_ip:
            raise Exception(
                "Watcher from %s was NOT cleaned using method: '%s' for image %s. Remaining: %s"
                % (client_ip, method, image_name, watchers)
            )
        else:
            raise Exception(
                "Watchers were NOT cleaned using method: '%s' for image %s. Remaining: %s"
                % (method, image_name, watchers)
            )


def _delete_watcher_and_verify(
    method: str,
    client,
    image_name: str,
    device: str,
    rados_orchestrator_obj: RadosOrchestrator,
    pool: str,
    client_ip: str,
) -> None:
    """
    Deletes an RBD watcher using a specified method and verifies its removal.

    Args:
        method (str): The cleanup method to apply (e.g., "unmap", "blacklist", "restart", "crash", "umount").
            Example: "unmap"
        client (Node): The client node object where the watcher resides.
            Example: ceph_cluster.get_nodes(role="client")[0]
        image_name (str): The name of the RBD image.
            Example: "img_single_client_test_0"
        device (str): The device path (e.g., /dev/rbd0) if applicable for unmap/umount.
            Example: "/dev/rbd0"
        rados_orchestrator_obj (RadosOrchestrator): The RadosOrchestrator object for
        executing Ceph commands and getting watcher info.
            Example: rados_obj (instance created in run function)
        pool (str): The name of the pool.
            Example: "watcher_test_pool"
        client_ip (str): The IP address of the client whose watcher is being deleted.
            Example: "192.168.1.100"

    Returns:
        None
    """
    log.info(
        "Attempting to delete watcher using '%s' for image: %s on client %s",
        method,
        image_name,
        client.hostname,
    )

    mount_path: str = f"/mnt/{image_name}"

    if method == "unmap":
        log.info("Performing rbd unmap of device %s.", device)
        try:
            out, err, rc = client.exec_command(cmd=f"rbd unmap {device}", sudo=True)
            log.debug("rbd unmap stdout: %s", out.strip())
        except Exception as e:
            if "is not mapped" in str(e):
                log.warning(
                    f"rbd unmap of {device} failed because it was not mapped. This is acceptable for cleanup."
                )
            else:
                raise
        time.sleep(5)
        # After unmap, the device is no longer associated. Clean up mount point.
        client.exec_command(cmd=f"rm -rf {mount_path}", sudo=True)

    elif method == "umount":
        log.info("Attempting to unmount %s from mount path %s.", device, mount_path)
        umount_cmd = f"umount {mount_path}"
        try:
            out, err, rc = client.exec_command(cmd=umount_cmd, sudo=True)
            log.info(f"Successfully unmounted {mount_path}.")
            log.debug("umount stdout: %s", out.strip())
        except Exception as e:
            if "not mounted" in str(e):
                log.warning(
                    f"Mount path {mount_path} was not mounted. Skipping umount. This is acceptable for cleanup."
                )
            else:
                raise
        time.sleep(5)

        # Important: For umount test, we only umount. The image is still mapped.
        # The `_perform_individual_cleanup` in the finally block will unmap it later.

    elif method == "blacklist":
        log.info(
            "Attempting to blacklist client IP %s for %s",
            client_ip,
            image_name,
        )
        # Get current watchers before blacklisting to ensure the client IP is actually there
        current_watchers: List[str] = rados_orchestrator_obj.get_rbd_client_ips(
            pool, image_name
        )
        if client_ip not in current_watchers:
            log.warning(
                "Client IP %s is not an active watcher for %s. Skipping blacklist operation.",
                client_ip,
                image_name,
            )
        else:
            log.info("Adding %s to Ceph OSD blacklist.", client_ip)
            try:
                client.exec_command(
                    cmd=f"ceph osd blacklist add {client_ip}", sudo=True
                )
            except Exception as e:
                if "already exists" in str(e):
                    log.warning(
                        f"Client IP {client_ip} already in blacklist. Continuing."
                    )
                else:
                    raise
            time.sleep(10)

    elif method == "restart":
        log.info("Attempting to restart rbd-nbd service on client %s.", client.hostname)
        try:
            is_active_out, _, _ = client.exec_command(
                cmd="systemctl is-active rbd-nbd",
                sudo=True,
            )
            if "active" in is_active_out:
                client.exec_command(cmd="systemctl restart rbd-nbd", sudo=True)
                log.info("Rbd-nbd service restarted on client %s.", client.hostname)
                time.sleep(15)
            else:
                log.warning(
                    "Rbd-nbd service not active on %s. Skipping restart action.",
                    client.hostname,
                )
        except Exception as e:
            log.warning(
                "Failed to restart rbd-nbd service on %s: %s", client.hostname, e
            )

    elif method == "crash":
        log.info("Simulating client crash by rebooting client %s.", client.hostname)
        try:
            # Reboot will cause SSH connection to drop, which will raise an exception.
            client.exec_command(cmd="reboot -f", sudo=True)
            log.info(
                "Client %s initiated reboot. Waiting for client to come back up.",
                client.hostname,
            )
            # Re-initialize the client connection after reboot
            client.wait_for_ssh()  # This should wait for the client to become reachable via SSH again
            log.info("Client %s is back online after reboot.", client.hostname)
            # After reboot, the device will be unmapped and unmounted naturally.
        except Exception as e:
            if "Disconnected from remote host" in str(
                e
            ) or "SSH session not active" in str(e):
                log.info(
                    f"Expected SSH connection loss during reboot of {client.hostname}."
                )
            else:
                log.error(
                    "Failed to reboot client %s to simulate crash: %s",
                    client.hostname,
                    e,
                )
                raise
    else:
        log.warning(
            "Unsupported cleanup method: %s. No specific action taken for watcher deletion.",
            method,
        )

    _verify_watcher_cleanup(rados_orchestrator_obj, pool, image_name, method, client_ip)


def _perform_individual_cleanup(
    client,
    device: str,
    image_name: str,
    rados_obj: RadosOrchestrator,
    pool: str,
    mapped_devices: Dict[str, Dict[str, str]],
) -> None:
    """
    Performs cleanup for a single image, including unmapping and deleting the RBD image.
    This is often used within loops to ensure a clean state after individual test iterations.

    Args:
        client (Node): The client node object.
            Example: ceph_cluster.get_nodes(role="client")[0]
        device (str): The device path (e.g., /dev/rbd0).
            Example: "/dev/rbd0"
        image_name (str): The name of the RBD image.
            Example: "img_single_client_test_0"
        rados_obj (RadosOrchestrator): The RadosOrchestrator object for Ceph operations.
            Example: rados_obj (instance created in run function)
        pool (str): The name of the pool.
            Example: "watcher_test_pool"
        mapped_devices (Dict[str, Dict[str, str]]): Dictionary tracking currently mapped devices.
            Used to remove the entry for the cleaned-up image.
            Example: {"client1_hostname": {"img1": "/dev/rbd0"}}

    Returns:
        None
    """
    try:
        log.info(
            "Performing individual cleanup for image %s using device %s.",
            image_name,
            device,
        )
        mount_path = f"/mnt/{image_name}"
        log.info(
            "Attempting to unmount %s if mounted for individual cleanup.", mount_path
        )
        try:
            client.exec_command(cmd=f"umount {mount_path}", sudo=True)
        except Exception as e:
            if "not mounted" not in str(e):
                log.warning(
                    f"Umount of {mount_path} failed unexpectedly during individual cleanup: {e}"
                )
        time.sleep(1)

        log.info("Attempting rbd unmap %s for individual cleanup.", device)
        try:
            client.exec_command(cmd=f"rbd unmap {device}", sudo=True)
        except Exception as e:
            if "is not mapped" not in str(e):
                log.warning(
                    f"Rbd unmap for {device} failed unexpectedly during individual cleanup: {e}"
                )
        time.sleep(10)
        client.exec_command(cmd=f"rm -rf {mount_path}", sudo=True)

        rados_obj.delete_rbd_image(pool_name=pool, img_name=image_name)
        if (
            client.hostname in mapped_devices
            and image_name in mapped_devices[client.hostname]
        ):
            del mapped_devices[client.hostname][image_name]
            log.info("Removed %s from mapped_devices tracking.", image_name)
    except Exception as cleanup_e:
        log.warning(
            "Error during individual cleanup for image %s: %s", image_name, cleanup_e
        )
        log.exception(cleanup_e)


def _final_cleanup_mapped_devices(
    client_nodes: List,
    pool: str,
    rados_obj: RadosOrchestrator,
    mapped_devices: Dict[str, Dict[str, str]],
) -> None:
    """
    Performs a final cleanup of any remaining mapped RBD devices across all client nodes
    associated with the test pool.

    Args:
        client_nodes (List[Node]): List of client node objects.
            Example: [client_node_1, client_node_2]
        pool (str): The name of the test pool.
            Example: "watcher_test_pool"
        rados_obj (RadosOrchestrator): The RadosOrchestrator object for Ceph operations.
            Example: rados_obj (instance created in run function)
        mapped_devices (Dict[str, Dict[str, str]]): Dictionary tracking currently mapped devices
            from the test execution. This is updated as devices are unmapped.
            Example: {"client1_hostname": {"img1": "/dev/rbd0"}}

    Returns:
        None
    """
    rbd_mapped_pattern = re.compile(r"^\s*\d+\s+(\S+)\s+(\S+)\s+\S+\s+(\/dev\/rbd\d+)")
    for client in client_nodes:
        try:
            # rbd showmapped is expected to succeed (return 0).
            # If it fails (e.g., client is down), an exception will be raised.
            mapped_out, _ = client.exec_command(cmd="rbd showmapped", sudo=True)
            for line in mapped_out.splitlines():
                match = rbd_mapped_pattern.match(line.strip())
                if match:
                    mapped_pool: str = match.group(1)
                    mapped_image: str = match.group(2)
                    device_to_unmap: str = match.group(3)
                    if mapped_pool == pool:
                        log.info(
                            "Found mapped device %s (image: %s) from test pool on %s. Attempting unmap and unmount.",
                            device_to_unmap,
                            mapped_image,
                            client.hostname,
                        )
                        try:
                            mount_path_final = f"/mnt/{mapped_image}"
                            log.info(
                                "Attempting to unmount %s if mounted for final cleanup.",
                                mount_path_final,
                            )
                            try:
                                client.exec_command(
                                    cmd=f"umount {mount_path_final}", sudo=True
                                )
                            except Exception as e:
                                if "not mounted" not in str(e):
                                    log.warning(
                                        f"Umount of {mount_path_final} failed unexpectedly during final cleanup: {e}"
                                    )
                            time.sleep(1)

                            log.info(
                                "Attempting rbd unmap %s for final cleanup.",
                                device_to_unmap,
                            )
                            try:
                                client.exec_command(
                                    cmd=f"rbd unmap {device_to_unmap}", sudo=True
                                )
                            except Exception as e:
                                if "is not mapped" not in str(e):
                                    log.warning(
                                        f"Rbd unmap for {device_to_unmap} failed unexpectedly during final cleanup: {e}"
                                    )
                            time.sleep(10)
                            client.exec_command(
                                cmd=f"rm -rf {mount_path_final}", sudo=True
                            )

                            if (
                                client.hostname in mapped_devices
                                and mapped_image in mapped_devices[client.hostname]
                            ):
                                del mapped_devices[client.hostname][mapped_image]
                                log.info(
                                    "Removed %s from mapped_devices tracking during final cleanup.",
                                    mapped_image,
                                )
                        except Exception as inner_cleanup_e:
                            log.warning(
                                "Failed to clean up mapped device %s for image %s on %s during final cleanup: %s",
                                device_to_unmap,
                                mapped_image,
                                client.hostname,
                                inner_cleanup_e,
                            )
                            log.exception(inner_cleanup_e)
        except Exception as client_cleanup_e:
            log.warning(
                "Error during client-specific final cleanup on %s: %s",
                client.hostname,
                client_cleanup_e,
            )
            log.exception(client_cleanup_e)


def _final_cleanup_rbd_images(
    pool: str, rados_obj: RadosOrchestrator, images_to_delete: List[str]
) -> None:
    """
    Performs a final cleanup of all created RBD images within the specified test pool.

    Args:
        pool (str): The name of the test pool.
            Example: "watcher_test_pool"
        rados_obj (RadosOrchestrator): The RadosOrchestrator object for Ceph operations.
            Example: rados_obj (instance created in run function)
        images_to_delete (List[str]): A list of RBD image names to be deleted.
            Example: ["img_single_client_test_0", "img_multi_client_0_1"]

    Returns:
        None
    """
    for image_to_delete in images_to_delete:
        try:
            log.info(
                "Attempting to delete image %s/%s in final cleanup block.",
                pool,
                image_to_delete,
            )
            # Deleting an image is expected to succeed if it exists and is not in use.
            # If it's still in use, the delete_rbd_image method will handle it
            rados_obj.delete_rbd_image(pool_name=pool, img_name=image_to_delete)
        except Exception as img_del_e:
            log.warning(
                "Failed to delete image %s/%s in final cleanup block: %s",
                pool,
                image_to_delete,
                img_del_e,
            )
            log.exception(img_del_e)
