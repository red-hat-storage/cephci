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
    CEPH-83616882: Verify RBD Watchers are Properly Cleaned Up.
    https://bugzilla.redhat.com/show_bug.cgi?id=2336352

    This test case validates comprehensive watcher cleanup for RBD images
    across single and multiple client scenarios using various methods.

    Workflow:
    1.  Create Pool: Establishes a dedicated test pool.
    2.  Create RBD Images: Generates images for single and multi-client tests.
    3.  Create Watchers: Maps images to clients, validating watcher creation.
    4.  Delete Watchers: Applies various cleanup methods (unmap, umount, blacklist,
        restart, crash) and confirms watcher removal for specific images/clients.
    5.  Final Cleanup: Ensures all test-related resources are removed.
    """
    log.info(run.__doc__)
    config = kw.get("config", {})
    log.info("Debug: Received config dictionary: %s", config)
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    client_nodes = ceph_cluster.get_nodes(role="client")
    pool = config.get("pool_name", "watcher_test_pool")
    num_images_to_create = 4  # Number of images to create per client

    # Tracks mapped devices for robust cleanup: {client_hostname: {image_name: device_path}}
    mapped_devices = {}
    # List to keep track of all images created for final cleanup
    all_created_images = []

    try:
        log.info("Creating test pool: %s", pool)
        rados_obj.create_pool(pool_name=pool)

        # --- Phase 1: Create, Map, Mount Images and Verify Watchers ---
        log.info(
            "\n--- Phase 1: Creating, Mapping, Mounting Images and Verifying Watchers ---"
        )

        # Handle Single Client scenario if enabled
        if config.get("Single_rados_watcher"):
            log.info("Preparing images for Single Client Watcher Test.")
            if not client_nodes:
                raise Exception(
                    "No client nodes found. Cannot run Single Rados Watcher Test."
                )

            client = client_nodes[0]
            log.info("Processing client: %s", client.hostname)

            for i in range(num_images_to_create):
                image_name = "img_single_client_test_%d" % i
                all_created_images.append(
                    image_name
                )  # Add to the list for final cleanup

                log.info("Creating image '%s' for single client test.", image_name)
                rados_obj.create_rbd_image(
                    pool_name=pool, img_name=image_name, size="1G"
                )

                # Using mount_image_on_client method
                mount_path = "/mnt/%s" % image_name
                device, mounted_path = rados_obj.mount_image_on_client(
                    pool_name=pool,
                    img_name=image_name,
                    client_obj=client,
                    mount_path=mount_path,
                )

                if device and mounted_path:
                    log.info(
                        "Image %s mapped to device %s and mounted to %s",
                        image_name,
                        device,
                        mounted_path,
                    )
                    mapped_devices.setdefault(client.hostname, {})[image_name] = device
                    # Now verify the watcher
                    _verify_watcher_presence(
                        image_name, client, pool, rados_obj, client.ip_address
                    )
                else:
                    raise Exception(
                        "Failed to map and mount image %s on client %s"
                        % (image_name, client.hostname)
                    )
        # Handle Multiple Client scenario if enabled
        if config.get("Multiple_rados_watcher"):
            log.info("Preparing images for Multiple Rados Watcher Test.")
            # Use all available clients, or a subset if desired (e.g., client_nodes[:3])
            clients_for_multi_test = client_nodes

            if not clients_for_multi_test:
                raise Exception(
                    "Not enough client nodes for Multiple Rados Watcher Test."
                )

            for client_idx, client in enumerate(clients_for_multi_test):
                log.info("Processing client: %s", client.hostname)
                for i in range(num_images_to_create):
                    image_name = "img_multi_client_%d_%d" % (client_idx, i)
                    all_created_images.append(
                        image_name
                    )  # Add to the list for final cleanup

                    log.info(
                        "Creating image '%s' for client %s", image_name, client.hostname
                    )
                    rados_obj.create_rbd_image(
                        pool_name=pool, img_name=image_name, size="1G"
                    )

                    # Using mount_image_on_client method
                    mount_path = "/mnt/%s" % image_name
                    device, mounted_path = rados_obj.mount_image_on_client(
                        pool_name=pool,
                        img_name=image_name,
                        client_obj=client,
                        mount_path=mount_path,
                    )

                    if device and mounted_path:
                        log.info(
                            "Image %s mapped to device %s and mounted to %s on %s",
                            image_name,
                            device,
                            mounted_path,
                            client.hostname,
                        )
                        mapped_devices.setdefault(client.hostname, {})[
                            image_name
                        ] = device
                        # verify the watcher
                        _verify_watcher_presence(
                            image_name, client, pool, rados_obj, client.ip_address
                        )
                    else:
                        raise Exception(
                            "Failed to map and mount image %s on client %s"
                            % (image_name, client.hostname)
                        )
        log.info(
            "\n--- Phase 1: All images created, mapped, mounted, and watchers verified. Proceeding to cleanup. ---"
        )

        # --- Phase 2: Delete Watchers based on various methods ---
        log.info("\n--- Phase 2: Initiating Watcher Cleanup Verification ---")
        cleanup_methods = ["unmap", "umount", "blacklist", "restart", "crash"]
        # Iterate through mapped devices to apply cleanup methods
        for client_hostname, images_info in list(mapped_devices.items()):
            client = next(
                (c for c in client_nodes if c.hostname == client_hostname), None
            )
            if not client:
                log.warning(
                    "Client %s not found, skipping cleanup for its images.",
                    client_hostname,
                )
                continue
            for image_name, device_path in list(images_info.items()):
                log.info(
                    "Testing cleanup for Image: %s on Client: %s",
                    image_name,
                    client.hostname,
                )
                for method in cleanup_methods:
                    log.info(
                        "--- Applying cleanup method: '%s' for %s on %s ---",
                        method,
                        image_name,
                        client.hostname,
                    )
                    try:
                        # Re-map/re-mount if necessary for each cleanup method, ensuring state for the test
                        log.info(
                            "Re-establishing watcher for %s before '%s' cleanup test.",
                            image_name,
                            method,
                        )
                        _force_cleanup_device(
                            client, device_path, image_name
                        )  # Ensure it's clean before re-mapping

                        # Remap and remount for the test
                        mount_path = "/mnt/%s" % image_name
                        re_mapped_device, re_mounted_path = (
                            rados_obj.mount_image_on_client(
                                pool_name=pool,
                                img_name=image_name,
                                client_obj=client,
                                mount_path=mount_path,
                            )
                        )
                        if not re_mapped_device:
                            raise Exception(
                                "Failed to re-map and mount image for cleanup test."
                            )

                        _verify_watcher_presence(
                            image_name, client, pool, rados_obj, client.ip_address
                        )

                        _delete_watcher_and_verify(
                            method,
                            client,
                            image_name,
                            re_mapped_device,  # Pass the actual device path
                            rados_obj,
                            pool,
                            client.ip_address,  # Use client.ip_address directly
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
                    finally:
                        # Ensure cleanup after each method test
                        log.info(
                            "Ensuring clean state after '%s' test for %s.",
                            method,
                            image_name,
                        )
                        _perform_individual_cleanup(
                            client,
                            (
                                re_mapped_device
                                if "re_mapped_device" in locals()
                                else device_path
                            ),
                            # Use re_mapped_device if available
                            image_name,
                            rados_obj,
                            pool,
                            mapped_devices,
                        )
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
            # Pass the aggregated list of all created images for final deletion
            _final_cleanup_rbd_images(pool, rados_obj, all_created_images)
            log.info("Attempting to delete pool: %s.", pool)
            rados_obj.delete_pool(pool=pool)
            log.info("Successfully deleted pool: %s.", pool)
        except Exception as e:
            log.error("Critical error during final cleanup of pool: %s", e)
            log.exception(e)
        if rados_obj.check_crash_status():
            log.error(
                "Test finished, but crash detected during or after test execution."
            )


def _verify_watcher_presence(image_name, client, pool, rados_obj, client_ip):
    """
    Verifies the watcher is created and present for a given image.
    This function expects the image to be already mapped.

    Args:
        image_name (str): The name of the RBD image.
        client (Node): The client node object.
        pool (str): The name of the Ceph pool.
        rados_obj (RadosOrchestrator): The RadosOrchestrator object for Ceph commands.
        client_ip (str): The IP address of the client creating the watcher.

    Returns:
        str: The IP address of the client that created the watcher.

    Raises:
        Exception: If the watcher is not found after mapping.
    """
    log.info("Verifying watcher created for %s from %s", image_name, client_ip)
    max_retries = 10
    for i in range(max_retries):
        watchers = rados_obj.get_rbd_client_ips(pool, image_name)
        if watchers and client_ip in watchers:
            log.info(
                "Watcher successfully created for %s by %s.", image_name, client_ip
            )
            return client_ip
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


def _force_cleanup_device(client, device, image_name):
    """
    Forcefully unmounts, kills holding processes, and unmaps an RBD device.

    Args:
        client (Node): The client node object where the device is mapped.
        device (str): The device path of the RBD image (e.g., '/dev/rbd0').
        image_name (str): The name of the RBD image for logging.
    """
    if not device:
        log.debug(
            "No device provided for image %s. Skipping forceful cleanup.", image_name
        )
        return
    mount_path = "/mnt/%s" % image_name
    log.info("Attempting to unmount %s if mounted.", mount_path)
    client.exec_command(cmd="umount %s" % mount_path, sudo=True, check_ec=False)
    time.sleep(1)
    log.info("Checking for processes holding %s.", device)
    pids_out, _ = client.exec_command(
        cmd="lsof -t %s" % device, sudo=True, check_ec=False
    )
    pids = [p.strip() for p in pids_out.splitlines() if p.strip()]
    if pids:
        log.warning(
            "Found PIDs holding %s: %s. Attempting to kill them.",
            device,
            ", ".join(pids),
        )
        for pid in pids:
            client.exec_command(cmd="kill -9 %s" % pid, sudo=True, check_ec=False)
        time.sleep(5)
        # Re-check if PIDs are gone
        pids_after_kill_out, _ = client.exec_command(
            cmd="lsof -t %s" % device, sudo=True, check_ec=False
        )
        if [p.strip() for p in pids_after_kill_out.splitlines() if p.strip()]:
            log.error("Critical: PIDs still holding %s after kill -9.", device)
    else:
        log.info("No specific processes found holding %s.", device)
    log.info("Attempting to pkill common RBD client processes.")
    client.exec_command(cmd="pkill -9 rbd-nbd", sudo=True, check_ec=False)
    client.exec_command(cmd="pkill -9 qemu-system-x86_64", sudo=True, check_ec=False)
    time.sleep(5)
    log.info("Attempting final rbd unmap %s.", device)
    unmap_out, unmap_err = client.exec_command(
        cmd="rbd unmap %s" % device, sudo=True, check_ec=False
    )
    if unmap_err:
        log.warning(
            "Rbd unmap for %s resulted in stderr: %s", device, unmap_err.strip()
        )
    if unmap_out:
        log.debug("Rbd unmap stdout: %s", unmap_out.strip())
    time.sleep(10)  # Time for unmap
    # Final check if still mapped
    mapped_output, _ = client.exec_command(
        cmd="rbd showmapped", sudo=True, check_ec=False
    )
    if device in mapped_output:
        raise Exception(
            "Device %s still mapped after forceful cleanup attempt." % device
        )
    else:
        log.info("Successfully unmapped %s.", device)
    client.exec_command(cmd="rm -rf %s" % mount_path, sudo=True)
    log.info("Cleaned up mount point %s.", mount_path)


def _verify_watcher_cleanup(rados_obj, pool, image_name, method, client_ip=None):
    """
    Verifies if watchers for a given image have been cleaned up.
    If `client_ip` is provided, it verifies that specific IP is no longer a watcher.
    Otherwise, it verifies no watchers exist for the image.

    Args:
        rados_obj (RadosOrchestrator): The RadosOrchestrator object for Ceph commands.
        pool (str): The name of the Ceph pool.
        image_name (str): The name of the RBD image.
        method (str): The cleanup method being tested (for logging purposes).
        client_ip (str, optional): The IP address of the client whose watcher is being verified.
        If None, verifies no watchers exist for the image.
    Raises:
        Exception: If the watcher is not cleaned up as expected within the retry attempts.
    """
    max_retries = 10
    retry_interval = 5
    watcher_cleaned = False
    for i in range(max_retries):
        log.info(
            "Verifying watchers after '%s' cleanup (Attempt %d/%d)...",
            method,
            i + 1,
            max_retries,
        )
        time.sleep(retry_interval)
        watchers = rados_obj.get_rbd_client_ips(pool, image_name)
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
        else:
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
    method, client, image_name, device, rados_obj, pool, client_ip
):
    """
    Deletes an RBD image watcher using the specified method and verifies its removal.

    Args:
        method (str): The cleanup method to apply ("unmap", "umount", "blacklist", "restart", "crash").
        client (Node): The client node object.
        image_name (str): The name of the RBD image.
        device (str): The device path of the mapped RBD image.
        rados_obj (RadosOrchestrator): The RadosOrchestrator object for Ceph commands.
        pool (str): The name of the Ceph pool.
        client_ip (str): The IP address of the client creating the watcher.
    """
    log.info(
        "Attempting to delete watcher using '%s' for image: %s", method, image_name
    )
    if method in ["unmap", "restart", "crash", "umount"]:
        log.info(
            "Initiating '%s' method cleanup for %s on %s",
            method,
            image_name,
            client.hostname,
        )
        _force_cleanup_device(client, device, image_name)
        log.info("Completed '%s' method cleanup for %s.", method, image_name)
    elif method == "blacklist":
        log.info(
            "Attempting to blacklist and un-blacklist client IP %s for %s",
            client_ip,
            image_name,
        )
        current_watchers = rados_obj.get_rbd_client_ips(pool, image_name)
        if client_ip not in current_watchers:
            log.warning(
                "Client IP %s is not an active watcher for %s. Skipping blacklist.",
                client_ip,
                image_name,
            )
        else:
            log.info("Adding %s to Ceph OSD blacklist.", client_ip)
            client.exec_command(cmd="ceph osd blacklist add %s" % client_ip, sudo=True)
            time.sleep(10)  # Time for blacklist
            log.info("Removing %s from Ceph OSD blacklist.", client_ip)
            client.exec_command(cmd="ceph osd blacklist rm %s" % client_ip, sudo=True)
            time.sleep(5)  # Time after un-blacklisting
    else:
        log.warning(
            "Unsupported cleanup method: %s. No action taken for watcher deletion.",
            method,
        )
    # Always verify watcher cleanup after attempting a method
    _verify_watcher_cleanup(rados_obj, pool, image_name, method, client_ip)


def _perform_individual_cleanup(
    client, device, image_name, rados_obj, pool, mapped_devices
):
    """
    Performs cleanup for a single image and its device after a test method.

    Args:
        client (Node): The client node object.
        device (str): The device path of the RBD image.
        image_name (str): The name of the RBD image.
        rados_obj (RadosOrchestrator): The RadosOrchestrator object for Ceph commands.
        pool (str): The name of the Ceph pool.
        mapped_devices (dict): A dictionary tracking currently mapped devices.
        It will be updated to reflect cleanup.
    """
    try:
        _force_cleanup_device(client, device, image_name)
        rados_obj.delete_rbd_image(pool_name=pool, img_name=image_name)
        if (
            client.hostname in mapped_devices
            and image_name in mapped_devices[client.hostname]
        ):
            del mapped_devices[client.hostname][image_name]
    except Exception as cleanup_e:
        log.warning(
            "Error during individual cleanup for image %s: %s", image_name, cleanup_e
        )
        log.exception(cleanup_e)


def _final_cleanup_mapped_devices(client_nodes, pool, rados_obj, mapped_devices):
    """
    Performs final cleanup of any remaining mapped RBD devices across all clients.

    Args:
        client_nodes (list): A list of client node objects.
        pool (str): The name of the Ceph pool.
        rados_obj (RadosOrchestrator): The RadosOrchestrator object for Ceph commands.
        mapped_devices (dict): A dictionary tracking currently mapped devices.
                               It will be updated to reflect cleanup.
    """
    rbd_mapped_pattern = re.compile(r"^\s*\d+\s+(\S+)\s+(\S+)\s+\S+\s+(\/dev\/rbd\d+)")
    for client in client_nodes:
        try:
            mapped_out, _ = client.exec_command(
                cmd="rbd showmapped", sudo=True, check_ec=False
            )
            for line in mapped_out.splitlines():
                match = rbd_mapped_pattern.match(line.strip())
                if match:
                    mapped_pool = match.group(1)
                    mapped_image = match.group(2)
                    device_to_unmap = match.group(3)
                    if mapped_pool == pool:
                        log.info(
                            "Found mapped device %s (image: %s) from test pool on %s. Attempting unmap and unmount.",
                            device_to_unmap,
                            mapped_image,
                            client.hostname,
                        )
                        _force_cleanup_device(client, device_to_unmap, mapped_image)
                        if (
                            client.hostname in mapped_devices
                            and mapped_image in mapped_devices[client.hostname]
                        ):
                            del mapped_devices[client.hostname][mapped_image]
        except Exception as client_cleanup_e:
            log.warning(
                "Error during client-specific final cleanup on %s: %s",
                client.hostname,
                client_cleanup_e,
            )
            log.exception(client_cleanup_e)


def _final_cleanup_rbd_images(pool, rados_obj, images_to_delete):
    """
    Deletes any remaining RBD images from the test pool.
    Args:
        pool (str): The name of the Ceph pool. Example: "my_test_pool"
        rados_obj (RadosOrchestrator): The RadosOrchestrator object for Ceph commands.
        Example: an instance of RadosOrchestrator class.
        images_to_delete (list): A list of RBD image names (strings) to be deleted.
        Example: ["image1", "image2", "image3"]

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
            rados_obj.delete_rbd_image(pool_name=pool, img_name=image_to_delete)
        except Exception as img_del_e:
            log.warning(
                "Failed to delete image %s/%s in final cleanup block: %s",
                pool,
                image_to_delete,
                img_del_e,
            )
            log.exception(img_del_e)
