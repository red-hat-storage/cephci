# """
# Module to verify proper cleanup of RBD image watchers in a Ceph cluster.

# Steps:
# 1. Deploy a Ceph cluster and validate existing pools.
# 2. Create a new RBD pool and image for testing.
# 3. Verify disk availability on the client node for RBD mapping.
# 4. Map the RBD image and format the device with ext4.
# 5. Mount the image and verify that watchers are registered.
# 6. Unmount the image and unmap the device.
# 7. Validate that watchers are cleared after unmap.
# 8. If watchers persist, blacklist clients.
# 9. Recheck watchers; ensure cleanup.
# 10. Confirm cluster returns to HEALTH_OK.
# 11. Cleanup all created resources.
# """

# import time

# from ceph.ceph_admin import CephAdmin
# from ceph.rados.core_workflows import RadosOrchestrator
# from utility.log import Log

# log = Log(__name__)


# def run(ceph_cluster, **kw):
#     """
#     Test Case: Verify RBD watchers are cleaned up correctly after unmount/unmap and client blacklisting.
#     """
#     log.info(run.__doc__)
#     config = kw.get("config", {})
#     cephadm = CephAdmin(cluster=ceph_cluster, **config)
#     rados_obj = RadosOrchestrator(node=cephadm)

#     # Fetch client node for exec_command
#     client_node = ceph_cluster.get_nodes(role="client")[0]

#     # Test configuration
#     pool_name = kw.get("pool_name", "watcher_test_pool")
#     image_name = kw.get("image_name", "watcher_test_image")
#     image_size = kw.get("image_size", "1G")
#     mount_point = kw.get("mount_point", "/mnt/rbd_test")
#     device_path = "/dev/rbd0"

#     try:
#         log.info(f"Starting test with pool: {pool_name}, image: {image_name}")

#         # Step 1: Create pool and image
#         log.info("Creating pool and RBD image")
#         rados_obj.create_pool(pool_name=pool_name)
#         rados_obj.create_rbd_image(
#             pool_name=pool_name, img_name=image_name, size=image_size
#         )

#         # Step 2: Map image
#         client_node.exec_command(cmd=f"rbd map {pool_name}/{image_name}", sudo=True)

#         # Step 3: Format and mount
#         client_node.exec_command(cmd=f"mkfs.ext4 {device_path}", sudo=True)
#         client_node.exec_command(cmd=f"mkdir -p {mount_point}", sudo=True)
#         client_node.exec_command(cmd=f"mount {device_path} {mount_point}", sudo=True)
#         log.info(f"Mounted {device_path} at {mount_point}")

#         # Step 4: Check watchers
#         time.sleep(5)
#         watchers = rados_obj.get_rbd_client_ips(pool_name, image_name)
#         log.info(f"Initial watchers found: {watchers}")

#         if not watchers:
#             raise Exception("No watchers detected after mount. Test invalid.")

#         # Step 5: Unmount and unmap
#         log.info("Unmounting filesystem")
#         if not unmount(client_node, mount_point):
#             raise Exception(f"Failed to unmount {mount_point}")

#         log.info("Unmapping RBD device")
#         client_node.exec_command(cmd=f"rbd unmap {device_path}", sudo=True)

#         # Step 6: Wait and check watchers
#         time.sleep(15)

#         watchers_after_unmap = rados_obj.get_rbd_client_ips(pool_name, image_name)
#         log.info(f"Watchers after unmap: {watchers_after_unmap}")

#         # Step 7: Blacklist clients if watchers remain
#         if watchers_after_unmap:
#             log.info("Blacklisting clients to force watcher cleanup")
#             for ip in watchers_after_unmap:
#                 client_node.exec_command(cmd=f"ceph osd blacklist add {ip}", sudo=True)
#                 time.sleep(15)

#             final_watchers = rados_obj.get_rbd_client_ips(pool_name, image_name)
#             if final_watchers:
#                 raise Exception(
#                     f"Watchers still present after blacklisting: {final_watchers}"
#                 )
#             else:
#                 log.info("Watchers cleaned up after blacklisting")
#         else:
#             log.info("No watchers present after unmap")

#         # Step 8: Cluster health check
#         health, _ = client_node.exec_command(cmd="ceph health", sudo=True)
#         log.info(f"Cluster health: {health}")
#         if "HEALTH_OK" not in health:
#             raise Exception(f"Cluster not in HEALTH_OK state: {health}")

#         return 0

#     except Exception as e:
#         log.error(f"Test failed: {e}")
#         raise

#     finally:
#         log.info("\n--- Running Cleanup ---")
#         try:
#             client_node.exec_command(
#                 cmd=f"umount {mount_point}", sudo=True, check_ec=False
#             )
#             client_node.exec_command(
#                 cmd=f"rbd unmap {device_path}", sudo=True, check_ec=False
#             )
#             if not delete_rbd_image(client_node, pool_name, image_name):
#                 return 1
#             rados_obj.delete_pool(pool_name)
#             client_node.exec_command(cmd=f"rm -rf {mount_point}", sudo=True)

#             if rados_obj.check_crash_status():
#                 log.error("Crash detected during test")
#                 return 1

#             rados_obj.log_cluster_health()
#             log.info("Cleanup completed successfully")
#         except Exception as ce:
#             log.error(f"Cleanup error: {ce}")

# def unmount(client_node, mount_point):
#     """Unmount filesystem on given client node."""
#     try:
#         out, err = client_node.exec_command(cmd=f"umount {mount_point}", sudo=True)

#         if err != "":
#             log.error(f"Unmount failed: {err}")
#             return False

#         log.info(f"Unmounted {mount_point} successfully")
#         return True
#     except Exception as e:
#         log.error(f"Exception while unmounting: {e}")
#         return False

# def delete_rbd_image(client_node, pool_name, image_name):
#     """Delete the specified RBD image using CLI."""
#     try:
#         out, err = client_node.exec_command(
#             cmd=f"rbd rm {pool_name}/{image_name}", sudo=True
#         )
#         time.sleep(10)

#         if out != "":
#             log.error(f"Failed to delete RBD image {pool_name}/{image_name}: {err}")
#             return False
#         else:
#             log.info(f"Deleted RBD image {pool_name}/{image_name}")
#             return True
#     except Exception as e:
#         log.error(f"Exception during image deletion: {e}")
#         raise e

"""
Test Case: Verify RBD Watchers are Properly Cleaned Up

Workflow:
1. Create Watchers:
   - Input: List of image names, client node object(s)
   - Action: Map images to clients (single or multiple)
   - Validation: Confirm watchers are created (via Ceph API)

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

import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83616882
    Verify watchers are properly cleaned up for RBD images.

    Approaches:

    1. Single client node (Single_rados_watcher)

    2. Multiple client nodes (Muliple_rados_watcher)

    """

    log.info(run.__doc__)
    config = kw.get("config", {})
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    client_nodes = ceph_cluster.get_nodes(role="client")
    pool = "watcher_test_pool"
    images = ["img1", "img2", "img3"]

    def create_watchers(image, client):
        log.info(f"Creating watcher for image: {image} on client: {client.hostname}")
        try:
            rados_obj.create_rbd_image(pool_name=pool, img_name=image, size="1G")
        except Exception as e:
            log.warning(f"Image {image} may already exist: {e}")
        client.exec_command(cmd=f"rbd map {pool}/{image}", sudo=True)
        time.sleep(10)

    def delete_watchers(method, client, image):
        device = "/dev/rbd0"
        try:
            log.info(f"Deleting watcher using '{method}' for image: {image}")
            if method == "unmap":
                client.exec_command(cmd=f"rbd unmap {device}", sudo=True)
            elif method == "umount":
                client.exec_command(
                    cmd=f"umount /mnt/{image}", sudo=True, check_ec=False
                )
                client.exec_command(
                    cmd=f"rbd unmap {device}", sudo=True, check_ec=False
                )
            elif method == "blacklist":
                watchers = rados_obj.get_rbd_client_ips(pool, image)
                for ip in watchers:
                    client.exec_command(cmd=f"ceph osd blacklist add {ip}", sudo=True)
                time.sleep(10)
                for ip in watchers:
                    client.exec_command(cmd=f"ceph osd blacklist rm {ip}", sudo=True)
                    log.info(f"Un-blacklisted IP {ip}")
            elif method == "restart":
                client.exec_command(cmd="systemctl restart ceph.target", sudo=True)
            elif method == "crash":
                client.exec_command(cmd="pkill -9 rbd-nbd", sudo=True)
            time.sleep(10)
        except Exception as e:
            log.error(f"Failed to delete watcher using {method}: {e}")

    try:
        rados_obj.create_pool(pool_name=pool)

        # ---------------- SINGLE CLIENT MODE ----------------
        if config.get("Single_rados_watcher"):
            log.info("Running Single Rados Watcher Test")
            client = client_nodes[0]
            image = images[0]
            methods = ["unmap", "umount", "blacklist", "restart", "crash"]

            for method in methods:
                create_watchers(image, client)
                delete_watchers(method, client, image)
                watchers = rados_obj.get_rbd_client_ips(pool, image)
                if watchers:
                    raise Exception(f"Watchers were not cleaned using method: {method}")
                log.info(f"Successfully removed watchers with method: {method}")

        # ---------------- MULTIPLE CLIENT MODE ----------------
        if config.get("Muliple_rados_watcher"):
            log.info("Running Multiple Rados Watcher Test")
            methods = ["unmap", "umount", "blacklist", "restart", "crash"]
            clients = client_nodes[:3]

            for i, client in enumerate(clients):
                image = images[i % len(images)]
                for method in methods:
                    create_watchers(image, client)
                    delete_watchers(method, client, image)
                    watchers = rados_obj.get_rbd_client_ips(pool, image)
                    if watchers:
                        raise Exception(
                            f"Watchers for image {image} not cleaned with method: {method}"
                        )
                    log.info(
                        f"Successfully removed watchers for {image} using method {method}"
                    )

        log.info("RBD watcher cleanup verification completed successfully.")
        return 0

    except Exception as e:
        log.error(f"Test failed: {e.__doc__}")
        log.exception(e)
        rados_obj.log_cluster_health()
        return 1

    finally:
        log.info("\n************** Execution of finally block begins **************")
        rados_obj.log_cluster_health()

        try:
            rados_obj.delete_pool(pool=pool)
            log.info(f"Successfully deleted pool: {pool}")
        except Exception as e:
            log.error(f"Failed to delete pool: {e}")

        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
