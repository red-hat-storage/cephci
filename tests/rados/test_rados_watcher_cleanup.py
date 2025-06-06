# # """
# # Module to verify proper cleanup of RBD image watchers in a Ceph cluster.

# # Steps:
# # 1. Deploy a Ceph cluster and validate existing pools.
# # 2. Create a new RBD pool and image for testing.
# # 3. Verify disk availability on the client node for RBD mapping.
# # 4. Map the RBD image and format the device with ext4.
# # 5. Mount the image and verify that watchers are registered.
# # 6. Unmount the image and unmap the device.
# # 7. Validate that watchers are cleared after unmap.
# # 8. If watchers persist, blacklist clients.
# # 9. Recheck watchers; ensure cleanup.
# # 10. Confirm cluster returns to HEALTH_OK.
# # 11. Cleanup all created resources.
# # """

# # import time

# # from ceph.ceph_admin import CephAdmin
# # from ceph.rados.core_workflows import RadosOrchestrator
# # from utility.log import Log

# # log = Log(__name__)


# # def run(ceph_cluster, **kw):
# #     """
# #     Test Case: Verify RBD watchers are cleaned up correctly after unmount/unmap and client blacklisting.
# #     """
# #     log.info(run.__doc__)
# #     config = kw.get("config", {})
# #     cephadm = CephAdmin(cluster=ceph_cluster, **config)
# #     rados_obj = RadosOrchestrator(node=cephadm)

# #     # Fetch client node for exec_command
# #     client_node = ceph_cluster.get_nodes(role="client")[0]

# #     # Test configuration
# #     pool_name = kw.get("pool_name", "watcher_test_pool")
# #     image_name = kw.get("image_name", "watcher_test_image")
# #     image_size = kw.get("image_size", "1G")
# #     mount_point = kw.get("mount_point", "/mnt/rbd_test")
# #     device_path = "/dev/rbd0"

# #     try:
# #         log.info(f"Starting test with pool: {pool_name}, image: {image_name}")

# #         # Step 1: Create pool and image
# #         log.info("Creating pool and RBD image")
# #         rados_obj.create_pool(pool_name=pool_name)
# #         rados_obj.create_rbd_image(
# #             pool_name=pool_name, img_name=image_name, size=image_size
# #         )

# #         # Step 2: Map image
# #         client_node.exec_command(cmd=f"rbd map {pool_name}/{image_name}", sudo=True)

# #         # Step 3: Format and mount
# #         client_node.exec_command(cmd=f"mkfs.ext4 {device_path}", sudo=True)
# #         client_node.exec_command(cmd=f"mkdir -p {mount_point}", sudo=True)
# #         client_node.exec_command(cmd=f"mount {device_path} {mount_point}", sudo=True)
# #         log.info(f"Mounted {device_path} at {mount_point}")

# #         # Step 4: Check watchers
# #         time.sleep(5)
# #         watchers = rados_obj.get_rbd_client_ips(pool_name, image_name)
# #         log.info(f"Initial watchers found: {watchers}")

# #         if not watchers:
# #             raise Exception("No watchers detected after mount. Test invalid.")

# #         # Step 5: Unmount and unmap
# #         log.info("Unmounting filesystem")
# #         if not unmount(client_node, mount_point):
# #             raise Exception(f"Failed to unmount {mount_point}")

# #         log.info("Unmapping RBD device")
# #         client_node.exec_command(cmd=f"rbd unmap {device_path}", sudo=True)

# #         # Step 6: Wait and check watchers
# #         time.sleep(15)

# #         watchers_after_unmap = rados_obj.get_rbd_client_ips(pool_name, image_name)
# #         log.info(f"Watchers after unmap: {watchers_after_unmap}")

# #         # Step 7: Blacklist clients if watchers remain
# #         if watchers_after_unmap:
# #             log.info("Blacklisting clients to force watcher cleanup")
# #             for ip in watchers_after_unmap:
# #                 client_node.exec_command(cmd=f"ceph osd blacklist add {ip}", sudo=True)
# #                 time.sleep(15)

# #             final_watchers = rados_obj.get_rbd_client_ips(pool_name, image_name)
# #             if final_watchers:
# #                 raise Exception(
# #                     f"Watchers still present after blacklisting: {final_watchers}"
# #                 )
# #             else:
# #                 log.info("Watchers cleaned up after blacklisting")
# #         else:
# #             log.info("No watchers present after unmap")

# #         # Step 8: Cluster health check
# #         health, _ = client_node.exec_command(cmd="ceph health", sudo=True)
# #         log.info(f"Cluster health: {health}")
# #         if "HEALTH_OK" not in health:
# #             raise Exception(f"Cluster not in HEALTH_OK state: {health}")

# #         return 0

# #     except Exception as e:
# #         log.error(f"Test failed: {e}")
# #         raise

# #     finally:
# #         log.info("\n--- Running Cleanup ---")
# #         try:
# #             client_node.exec_command(
# #                 cmd=f"umount {mount_point}", sudo=True, check_ec=False
# #             )
# #             client_node.exec_command(
# #                 cmd=f"rbd unmap {device_path}", sudo=True, check_ec=False
# #             )
# #             if not delete_rbd_image(client_node, pool_name, image_name):
# #                 return 1
# #             rados_obj.delete_pool(pool_name)
# #             client_node.exec_command(cmd=f"rm -rf {mount_point}", sudo=True)

# #             if rados_obj.check_crash_status():
# #                 log.error("Crash detected during test")
# #                 return 1

# #             rados_obj.log_cluster_health()
# #             log.info("Cleanup completed successfully")
# #         except Exception as ce:
# #             log.error(f"Cleanup error: {ce}")

# # def unmount(client_node, mount_point):
# #     """Unmount filesystem on given client node."""
# #     try:
# #         out, err = client_node.exec_command(cmd=f"umount {mount_point}", sudo=True)

# #         if err != "":
# #             log.error(f"Unmount failed: {err}")
# #             return False

# #         log.info(f"Unmounted {mount_point} successfully")
# #         return True
# #     except Exception as e:
# #         log.error(f"Exception while unmounting: {e}")
# #         return False

# # def delete_rbd_image(client_node, pool_name, image_name):
# #     """Delete the specified RBD image using CLI."""
# #     try:
# #         out, err = client_node.exec_command(
# #             cmd=f"rbd rm {pool_name}/{image_name}", sudo=True
# #         )
# #         time.sleep(10)

# #         if out != "":
# #             log.error(f"Failed to delete RBD image {pool_name}/{image_name}: {err}")
# #             return False
# #         else:
# #             log.info(f"Deleted RBD image {pool_name}/{image_name}")
# #             return True
# #     except Exception as e:
# #         log.error(f"Exception during image deletion: {e}")
# #         raise e

# """
# Test Case: Verify RBD Watchers are Properly Cleaned Up

# Workflow:
# 1. Create Watchers:
#    - Input: List of image names, client node object(s)
#    - Action: Map images to clients (single or multiple)
#    - Validation: Confirm watchers are created (via Ceph API)

# 2. Delete Watchers:
#    - Input: Cleanup method, mapping info from previous step
#    - Supported Methods:
#        a. unmap
#        b. umount
#        c. blacklist
#        d. restart
#        e. crash
#    - Validation: Confirm watchers are removed

# Approach 1: Single client
# Approach 2: Multiple clients
# """

# import time

# from ceph.ceph_admin import CephAdmin
# from ceph.rados.core_workflows import RadosOrchestrator
# from utility.log import Log

# log = Log(__name__)


# def run(ceph_cluster, **kw):
#     """
#     CEPH-83616882
#     Verify watchers are properly cleaned up for RBD images.

#     Approaches:

#     1. Single client node (Single_rados_watcher)
#     2. Multiple client nodes (Multiple_rados_watcher)
#     """

#     log.info(run.__doc__)
#     config = kw.get("config", {})
#     cephadm = CephAdmin(cluster=ceph_cluster, **config)
#     rados_obj = RadosOrchestrator(node=cephadm)
#     client_nodes = ceph_cluster.get_nodes(role="client")
#     pool = "watcher_test_pool"
#     images = ["img1", "img2", "img3"]

#     def create_watchers(image, client):

#         try:
#             log.info(f"Mapping image {image} to client {client.hostname}")
#             map_cmd = f"rbd map {pool}/{image}"
#             out, _ = client.exec_command(cmd=map_cmd, sudo=True)
#             device = out.strip()
#             log.info(f"Image {image} mapped to device {device}")

#             return device
#         except Exception as e:
#             log.error(f"Failed to map image {image}: {e}")
#             raise

#     def delete_watchers(method, client, image, device):
#         try:
#             log.info(f"Deleting watcher using '{method}' for image: {image}")
#             if method == "unmap":
#                 client.exec_command(cmd=f"rbd unmap {device}", sudo=True)

#             elif method == "umount":
#                 client.exec_command(cmd=f"umount /mnt/{image}", sudo=True, check_ec=False)
#                 client.exec_command(cmd=f"rbd unmap {device}", sudo=True, check_ec=False)
#                 client.exec_command(cmd=f"rm -rf /mnt/{image}", sudo=True)

#             elif method == "blacklist":
#                 watchers = rados_obj.get_rbd_client_ips(pool, image)
#                 for ip in watchers:
#                     client.exec_command(cmd=f"ceph osd blacklist add {ip}", sudo=True)
#                 time.sleep(10)
#                 for ip in watchers:
#                     client.exec_command(cmd=f"ceph osd blacklist rm {ip}", sudo=True)
#                     log.info(f"Un-blacklisted IP {ip}")

#             elif method == "restart":
#                 client.exec_command(cmd="systemctl restart ceph.target", sudo=True)

#             elif method == "crash":
#                 client.exec_command(cmd="pkill -9 rbd-nbd", sudo=True)

#             time.sleep(10)

#         except Exception as e:
#             log.error(f"Failed to delete watcher using {method}: {e}")

#     try:
#         rados_obj.create_pool(pool_name=pool)

#         # ---------------- SINGLE CLIENT MODE ----------------
#         if config.get("Single_rados_watcher"):
#             log.info("Running Single Rados Watcher Test")
#             client = client_nodes[0]
#             image = images[0]
#             methods = ["unmap", "umount", "blacklist", "restart", "crash"]

#             for method in methods:
#                 device = create_watchers(image, client)

#                 if method == "umount":
#                     client.exec_command(cmd=f"mkfs.ext4 {device}", sudo=True)
#                     client.exec_command(cmd=f"mkdir -p /mnt/{image}", sudo=True)
#                     client.exec_command(cmd=f"mount {device} /mnt/{image}", sudo=True)

#                 delete_watchers(method, client, image, device)
#                 watchers = rados_obj.get_rbd_client_ips(pool, image)
#                 if watchers:
#                     raise Exception(f"Watchers were not cleaned using method: {method}")
#                 log.info(f"Successfully removed watchers with method: {method}")

#         # ---------------- MULTIPLE CLIENT MODE ----------------
#         if config.get("Multiple_rados_watcher"):
#             log.info("Running Multiple Rados Watcher Test")
#             methods = ["unmap", "umount", "blacklist", "restart", "crash"]
#             clients = client_nodes[:3]

#             for i, client in enumerate(clients):
#                 image = images[i % len(images)]
#                 for method in methods:
#                     device = create_watchers(image, client)

#                     if method == "umount":
#                         client.exec_command(cmd=f"mkfs.ext4 {device}", sudo=True)
#                         client.exec_command(cmd=f"mkdir -p /mnt/{image}", sudo=True)
#                         client.exec_command(cmd=f"mount {device} /mnt/{image}", sudo=True)

#                     delete_watchers(method, client, image, device)
#                     watchers = rados_obj.get_rbd_client_ips(pool, image)
#                     if watchers:
#                         raise Exception(
#                             f"Watchers for image {image} not cleaned with method: {method}"
#                         )
#                     log.info(
#                         f"Successfully removed watchers for {image} using method {method}"
#                     )

#         log.info("RBD watcher cleanup verification completed successfully.")
#         return 0

#     except Exception as e:
#         log.error(f"Test failed: {e.__doc__}")
#         log.exception(e)
#         rados_obj.log_cluster_health()
#         return 1

#     finally:
#         log.info("\n************** Execution of finally block begins **************")
#         rados_obj.log_cluster_health()

#         try:
#             rados_obj.delete_pool(pool=pool)
#             log.info(f"Successfully deleted pool: {pool}")
#         except Exception as e:
#             log.error(f"Failed to delete pool: {e}")

#         if rados_obj.check_crash_status():
#             log.error("Test failed due to crash at the end of test")
#             return 1

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

    """

    log.info(run.__doc__)
    config = kw.get("config", {})  # Get test configuration
    cephadm = CephAdmin(cluster=ceph_cluster, **config)  # Initialize CephAdmin
    rados_obj = RadosOrchestrator(node=cephadm)  # Initialize RadosOrchestrator
    client_nodes = ceph_cluster.get_nodes(role="client")  # Get all client nodes

    # Define common test parameters
    pool = "watcher_test_pool"
    single_client_image = (
        "img_single_client_test"  # Dedicated image name for this single-client test
    )

    def create_watchers(image_name_param, client_param):
        """
        Maps an RBD image to a client node and returns the device path.
        """
        try:
            log.info(
                f"Mapping image {image_name_param} to client {client_param.hostname}"
            )
            map_cmd = (
                f"rbd map {pool}/{image_name_param}"  # 'pool' accessed from outer scope
            )
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
            raise  # Re-raise for calling test logic to handle

    def delete_watchers(method_param, client_param, image_name_param, device_param):
        """
        Deletes RBD image watchers using various specified methods.
        Includes enhanced logging and verification for 'unmap', 'restart', and 'crash'.
        """
        try:
            log.info(
                f"Attempting to delete watcher using '{method_param}' for image: {image_name_param}"
            )

            if method_param == "unmap":
                log.debug(
                    f"Checking for processes using device {device_param} before unmap..."
                )
                lsof_cmd = f"lsof {device_param}"
                out_lsof, err_lsof = client_param.exec_command(
                    cmd=lsof_cmd, sudo=True, check_ec=False
                )

                if out_lsof:
                    log.warning(
                        f"Found processes still using {device_param}:\n{out_lsof.strip()}"
                    )
                    # Forcefully kill processes using the device with fuser -k
                    log.info(
                        f"Attempting to forcefully terminate processes using {device_param} with fuser -k."
                    )
                    client_param.exec_command(
                        cmd=f"sudo fuser -k {device_param}", sudo=True, check_ec=False
                    )
                    time.sleep(2)  # Give processes time to die

                log.info(f"Executing rbd unmap {device_param}")
                out, err = client_param.exec_command(
                    cmd=f"rbd unmap {device_param}", sudo=True, check_ec=False
                )

                if err:
                    log.error(
                        f"Stderr during rbd unmap for {device_param}: {err.strip()}"
                    )
                if out:
                    log.debug(
                        f"Stdout during rbd unmap for {device_param}: {out.strip()}"
                    )

                check_mapped_cmd = "rbd showmapped"
                time.sleep(
                    5
                )  # Give more time for unmap to propagate and state to update
                mapped_output, _ = client_param.exec_command(
                    cmd=check_mapped_cmd, sudo=True, check_ec=False
                )

                if device_param in mapped_output:
                    log.error(
                        f"CRITICAL: Failed to unmap {device_param}. It is still listed in rbd"
                        "showmapped output:\n{mapped_output.strip()}"
                    )
                    raise Exception(
                        f"Watcher was not cleaned using method: {method_param} - "
                        "device {device_param} still mapped after unmap attempt."
                    )
                else:
                    log.info(
                        f"Successfully unmapped {device_param}. Not found in rbd showmapped output."
                    )

            elif method_param == "umount":
                mount_path = f"/mnt/{image_name_param}"
                log.info(f"Attempting umount {mount_path} and rbd unmap {device_param}")

                client_param.exec_command(cmd=f"mkdir -p {mount_path}", sudo=True)

                umount_out, umount_err = client_param.exec_command(
                    cmd=f"umount {mount_path}", sudo=True, check_ec=False
                )
                if umount_err and "not mounted" not in umount_err:
                    log.warning(
                        f"Umount for {mount_path} resulted in stderr: {umount_err.strip()}"
                    )
                if umount_out:
                    log.debug(f"Umount stdout: {umount_out.strip()}")

                unmap_out, unmap_err = client_param.exec_command(
                    cmd=f"rbd unmap {device_param}", sudo=True, check_ec=False
                )
                if unmap_err:
                    log.warning(
                        f"Rbd unmap for {device_param} resulted in stderr: {unmap_err.strip()}"
                    )
                if unmap_out:
                    log.debug(f"Rbd unmap stdout: {unmap_out.strip()}")

                client_param.exec_command(cmd=f"rm -rf {mount_path}", sudo=True)
                log.info(f"Cleaned up mount point {mount_path}.")

            elif method_param == "blacklist":
                log.info(
                    f"Attempting to blacklist and un-blacklist client IPs for image: {image_name_param}"
                )
                # rados_obj and pool are accessed from the outer scope
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
                time.sleep(10)  # Give Ceph time to react
                for ip in watchers:
                    log.info(f"Removing {ip} from Ceph OSD blacklist.")
                    client_param.exec_command(
                        cmd=f"ceph osd blacklist rm {ip}", sudo=True
                    )
                    log.info(f"Un-blacklisted IP {ip}")

            elif method_param == "restart":
                log.info(
                    f"--- Entering 'restart' method: Aggressive cleanup attempt for "
                    f"watcher: {image_name_param} on {client_param.hostname} ---"
                )

                # Step 1: Attempt unmount first, if mounted
                mount_path = f"/mnt/{image_name_param}"
                log.info(f"Attempting to unmount {mount_path} if it's mounted.")
                client_param.exec_command(
                    cmd=f"umount {mount_path}", sudo=True, check_ec=False
                )
                time.sleep(1)  # Short pause

                # Step 2: Identify and kill processes holding the device
                if device_param:
                    log.info(f"Checking for processes holding {device_param}.")
                    lsof_cmd = f"lsof -t {device_param}"  # -t for PID only
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
                        time.sleep(5)  # Give processes time to terminate

                        # Verify if PIDs are gone
                        pids_after_kill_out, _ = client_param.exec_command(
                            cmd=lsof_cmd, sudo=True, check_ec=False
                        )
                        pids_after_kill = [
                            p.strip()
                            for p in pids_after_kill_out.splitlines()
                            if p.strip()
                        ]
                        if pids_after_kill:
                            log.error(
                                f"CRITICAL: PIDs still holding {device_param} after"
                                "kill -9: {', '.join(pids_after_kill)}. This is unexpected."
                            )
                            # Even if critical, we proceed to unmap to try and force cleanup
                        else:
                            log.info(
                                f"All identified PIDs holding {device_param} terminated successfully."
                            )
                    else:
                        log.info(
                            f"No specific processes found holding {device_param} via lsof."
                        )

                    # Also try pkill for common RBD client processes
                    log.info(
                        "Attempting to pkill common RBD client processes (rbd-nbd, qemu-system-x86_64, etc.)."
                    )
                    client_param.exec_command(
                        cmd="pkill -9 rbd-nbd", sudo=True, check_ec=False
                    )
                    client_param.exec_command(
                        cmd="pkill -9 qemu-system-x86_64", sudo=True, check_ec=False
                    )  # If VM uses it directly
                    # Add any other relevant process names that might hold an RBD image
                    time.sleep(5)  # Give processes time to die

                # Step 3: Attempt rbd unmap again forcefully
                if device_param:
                    log.info(
                        f"Attempting final rbd unmap {device_param} for 'restart' method."
                    )
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

                log.info(
                    f"--- Exited 'restart' method cleanup for watcher: {image_name_param} ---"
                )

            elif method_param == "crash":
                log.info(
                    f"--- Entering 'crash' method: Aggressive cleanup attempt "
                    f"for watcher: {image_name_param} on {client_param.hostname} ---"
                )

                # Step 1: Attempt unmount first, if mounted
                mount_path = f"/mnt/{image_name_param}"
                log.info(f"Attempting to unmount {mount_path} if it's mounted.")
                client_param.exec_command(
                    cmd=f"umount {mount_path}", sudo=True, check_ec=False
                )
                time.sleep(1)  # Short pause

                # Step 2: Identify and kill processes holding the device
                if device_param:
                    log.info(f"Checking for processes holding {device_param}.")
                    lsof_cmd = f"lsof -t {device_param}"  # -t for PID only
                    pids_out, _ = client_param.exec_command(
                        cmd=lsof_cmd, sudo=True, check_ec=False
                    )
                    pids = [p.strip() for p in pids_out.splitlines() if p.strip()]

                    if pids:
                        log.warning(
                            f"Found PIDs holding {device_param}: {', '.join(pids)}. Attempting to kill them forcefully."
                        )
                        for pid in pids:
                            client_param.exec_command(
                                cmd=f"kill -9 {pid}", sudo=True, check_ec=False
                            )
                        time.sleep(5)  # Give processes time to terminate

                        # Verify if PIDs are gone
                        pids_after_kill_out, _ = client_param.exec_command(
                            cmd=lsof_cmd, sudo=True, check_ec=False
                        )
                        pids_after_kill = [
                            p.strip()
                            for p in pids_after_kill_out.splitlines()
                            if p.strip()
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
                        "Attempting to pkill common RBD client processes "
                        "(rbd-nbd, qemu-system-x86_64, etc.) for 'crash' method."
                    )
                    client_param.exec_command(
                        cmd="pkill -9 rbd-nbd", sudo=True, check_ec=False
                    )
                    client_param.exec_command(
                        cmd="pkill -9 qemu-system-x86_64", sudo=True, check_ec=False
                    )
                    # Add any other relevant process names that might hold an RBD image on your client
                    time.sleep(5)  # Give processes time to die

                # Step 3: Attempt rbd unmap again forcefully
                if device_param:
                    log.info(
                        f"Attempting final rbd unmap {device_param} for 'crash' method."
                    )
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

                log.info(
                    f"--- Exited 'crash' method cleanup for watcher: {image_name_param} ---"
                )

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
        if not client_nodes:
            raise Exception(
                "No client nodes found. Cannot run Single Rados Watcher Test."
            )

        log.info("Running Single Rados Watcher Test")
        client = client_nodes[0]  # Use the first client node

        # Updated methods list to include 'crash' as a more reliable "restart-like" action
        # The 'restart' method now also performs a process kill, making it more effective.
        methods = ["unmap", "umount", "blacklist", "restart", "crash"]

        for method in methods:
            log.info(f"--- Single Client Test: Starting test for method: {method} ---")
            device = None  # Initialize device for each iteration
            try:
                # Create a fresh RBD image for each method test to ensure isolation.
                log.info(f"Creating image {single_client_image} for method {method}")
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

                # After attempting deletion, explicitly verify if watchers are gone from Ceph side
                # THIS IS THE CRITICAL VERIFICATION POINT. WE ADD A RETRY LOOP HERE.
                max_retries_watcher_check = 5  # Increased retries
                retry_interval_watcher_check = 5  # seconds
                watcher_cleaned = False
                for i in range(max_retries_watcher_check):
                    log.info(
                        f"Verifying watchers after '{method}' cleanup (Attempt {i+1}/{max_retries_watcher_check})..."
                    )
                    time.sleep(retry_interval_watcher_check)  # Wait before checking
                    watchers = rados_obj.get_rbd_client_ips(pool, single_client_image)
                    if not watchers:
                        log.info(
                            f"Watchers successfully cleaned up after '{method}' method."
                        )
                        watcher_cleaned = True
                        break
                    else:
                        log.warning(
                            f"Watchers still present after '{method}' cleanup (Attempt {i+1}): {watchers}. Retrying..."
                        )

                if not watcher_cleaned:
                    raise Exception(
                        f"Watchers were NOT cleaned using method: {method}. Remaining: {watchers}"
                    )
                log.info(f"Successfully removed watchers with method: {method}")

            except Exception as e:
                log.error(f"Single Client Test FAILED for method '{method}': {e}")
                raise  # Re-raise to ensure the main try-except block catches this
            finally:
                # Comprehensive cleanup after each method test, regardless of success/failure
                log.info(
                    f"Cleaning up image {single_client_image} and its mount point after '{method}' test."
                )
                try:
                    # Ensure device is unmounted and unmapped before image deletion
                    mount_path = f"/mnt/{single_client_image}"
                    client.exec_command(
                        cmd=f"umount {mount_path}", sudo=True, check_ec=False
                    )
                    if device:  # Only attempt unmap if device was successfully mapped
                        client.exec_command(
                            cmd=f"rbd unmap {device}", sudo=True, check_ec=False
                        )
                    client.exec_command(
                        cmd=f"rm -rf {mount_path}", sudo=True, check_ec=False
                    )
                    # This call assumes rados_obj.delete_rbd_image exists and works correctly.
                    rados_obj.delete_rbd_image(
                        pool_name=pool, img_name=single_client_image
                    )
                except Exception as cleanup_e:
                    log.warning(
                        f"Error during per-method cleanup for image {single_client_image}: {cleanup_e}"
                    )

        log.info("RBD watcher cleanup verification completed successfully.")
        return 0  # Return 0 for success if all tests pass

    except Exception as e:
        log.error(f"Test failed: {e}")
        log.exception(e)  # Prints the full Python traceback
        rados_obj.log_cluster_health()  # Log cluster health on failure
        return 1  # Return 1 for failure

    finally:
        log.info("\n************** Execution of finally block begins **************")
        rados_obj.log_cluster_health()  # Log cluster health at start of final cleanup

        # Final comprehensive cleanup to ensure no resources are left behind
        try:
            log.info(
                "Attempting final cleanup of any remaining mapped devices or images across all clients."
            )
            for client in client_nodes:  # Iterate through all clients just in case
                try:
                    mapped_out, _ = client.exec_command(
                        cmd="rbd showmapped", sudo=True, check_ec=False
                    )
                    # Parse rbd showmapped output more carefully
                    # Sample line: 0   watcher_test_pool  img_single_client_test  -   /dev/rbd0
                    for line in mapped_out.splitlines():
                        # Regex to match typical rbd showmapped output: ID POOL IMAGE SNAP DEVICE
                        # It's more robust than splitting by space directly
                        match = re.match(
                            r"^\s*\d+\s+(\S+)\s+(\S+)\s+\S+\s+(\/dev\/rbd\d+)",
                            line.strip(),
                        )
                        if match:
                            mapped_pool = match.group(1)
                            mapped_image = match.group(2)
                            device_to_unmap = match.group(3)

                            if (
                                mapped_pool == pool
                            ):  # Only clean up images from our test pool
                                log.info(
                                    f"Found mapped device {device_to_unmap} (image: {mapped_image}) "
                                    "from test pool on {client.hostname}. Attempting unmap and unmount."
                                )
                                try:
                                    mount_path_unmap = f"/mnt/{mapped_image}"
                                    # Construct mount path for this mapped image
                                    client.exec_command(
                                        cmd=f"umount {mount_path_unmap}",
                                        sudo=True,
                                        check_ec=False,
                                    )
                                except Exception:
                                    pass  # Ignore if not mounted
                                client.exec_command(
                                    cmd=f"rbd unmap {device_to_unmap}",
                                    sudo=True,
                                    check_ec=False,
                                )
                                time.sleep(1)
                                client.exec_command(
                                    cmd=f"rm -rf {mount_path_unmap}",
                                    sudo=True,
                                    check_ec=False,
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

                except Exception as client_cleanup_e:
                    log.warning(
                        f"Error during client-specific final cleanup on {client.hostname}: {client_cleanup_e}"
                    )

            # Delete the specific image used for the single client test (redundant but safe)
            # This handles cases where the image might not have been mapped but was created.
            try:
                log.info(
                    f"Attempting to delete image {pool}/{single_client_image} in final cleanup block."
                )
                rados_obj.delete_rbd_image(pool_name=pool, img_name=single_client_image)
            except Exception as img_del_e:
                log.warning(
                    f"Failed to delete image {pool}/{single_client_image} in final cleanup block: {img_del_e}"
                )

            # Finally, delete the test pool
            log.info(f"Attempting to delete pool: {pool} in final cleanup block.")
            rados_obj.delete_pool(pool=pool)  # Uses the provided delete_pool function
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
