"""Test case covered -
CEPH-83574134 - Configure immutable object cache daemon
and validate client RBD objects

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Create RBD based pool and an Image
2. Enable the immutable cache client settings
3. Install the ceph-immutable-object-cache package
4. Create a unique Ceph user ID, the keyring
5. Enable the ceph-immutable-object-cache daemon with created client
6. Write some data to the image using FIO
7. Perform snapshot,protect and clone of rbd images
8. Read the cloned images from the cache path
"""

import time

from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)

immutable_cache_path = "/mnt/immutable-cache/"


def configure_immutable_cache(rbd_obj, client_node, user_name):
    """Method to configure immutable object cache.

    Args:
        rbd_obj: RBD object
        client_node: cache client node
        user_name: name of the client user

    Returns:
        int: The return value. 0 for success, 1 for failure.

    """

    # Update the Ceph client configuration
    client_config = {
        "immutable_object_cache_max_size": "10G",
        "immutable_object_cache_path": immutable_cache_path,
        "rbd_parent_cache_enabled": "true",
        "rbd_plugins": "parent_cache",
        "immutable_object_cache_watermark": "0.9",
        "immutable_object_cache_sock": "/var/run/ceph/immutable_object_cache_sock",
        "immutable_object_cache_qos_schedule_tick_min": "50",
        "immutable_object_cache_qos_iops_limit": "0",
        "immutable_object_cache_qos_iops_burst": "0",
        "immutable_object_cache_qos_iops_burst_seconds": "1",
        "immutable_object_cache_qos_bps_limit": "0",
        "immutable_object_cache_qos_bps_burst": "0",
        "immutable_object_cache_qos_bps_burst_seconds": "1",
    }

    try:
        # Create the immutable cache directory and set permissions
        client_node.exec_command(cmd=f"mkdir -p {immutable_cache_path}", sudo=True)
        client_node.exec_command(cmd=f"chmod 777 {immutable_cache_path}", sudo=True)

        for key, value in client_config.items():
            command = f"ceph config set client {key} {value}"
            client_node.exec_command(cmd=command, sudo=True)

        log.info("Updated the client configuration for immutable object cache")

        # Install ceph-immutable-object-cache package
        client_node.exec_command(
            cmd="dnf install ceph-immutable-object-cache -y", sudo=True
        )

        # Generate keyring and create keyring file
        keyring_file = (
            f"/etc/ceph/ceph.client.ceph-immutable-object-cache.{user_name}.keyring"
        )
        client_node.exec_command(cmd=f"touch {keyring_file}", sudo=True)
        client_node.exec_command(cmd=f"chmod 777 {keyring_file}", sudo=True)

        keyring_command = (
            f"ceph auth get-or-create client.ceph-immutable-object-cache.{user_name}"
            " mon 'profile rbd'"
            " osd 'profile rbd-read-only'"
        )
        command = f"{keyring_command} >> {keyring_file}"
        client_node.exec_command(cmd=command, sudo=True)

        log.info(f"Keyring file {keyring_file} created and populated.")

        # Enable and start the ceph-immutable-object-cache service
        service_name = (
            f"ceph-immutable-object-cache@ceph-immutable-object-cache.{user_name}"
        )
        client_node.exec_command(cmd=f"systemctl enable {service_name}", sudo=True)

        # we need to wait for some time before starting daemon
        time.sleep(60)
        client_node.exec_command(cmd=f"systemctl start {service_name}", sudo=True)

        # Check the status of the service
        status_command = f"systemctl status {service_name}"
        status_output = client_node.exec_command(cmd=status_command, sudo=True)

        # Verify object cache daemon got activated successfully
        if "Active" not in status_output[0]:
            log.debug(status_output)
            log.error(f"{service_name} not activated")
            return 1

        log.info("ceph-immutable-object-cache daemon got activated successfully")

    except Exception as err:
        log.error(err)
        return 1

    return 0


def verify_cache_objects(rbd, node, **kw):
    """
    Method to validate cache objects after configuring immutable cache daemon.

    Args:
        rbd: RBD object
        node: cache client node
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 for failure.
    """
    config = kw.get("config")
    pool = config["rep_pool_config"]["pool"]
    image = config["rep_pool_config"]["image1"]
    image_size = config["rep_pool_config"]["size"]
    run_time = config.get("runtime", 60)

    snap_name = f"{pool}/{image}@snap1"
    clone = "cloned_image1"
    try:
        rbd.create_image(
            pool,
            image,
            size=image_size,
        )

        run_fio(
            image_name=image,
            pool_name=pool,
            client_node=node,
            io_type="write",
            runtime=run_time,
        )

        # create snapshot, protect and clone it for the image
        rbd.snap_create(pool, image, "snap1")

        rbd.protect_snapshot(snap_name)

        rbd.create_clone(
            snap_name,
            pool,
            clone,
        )

        # Read IO from cloned images to get cache files
        run_fio(
            image_name=clone,
            pool_name=pool,
            client_node=node,
            io_type="read",
            runtime=run_time,
        )

        # waiting for cache files getting generated
        time.sleep(180)
        out = node.exec_command(cmd=f"ls {immutable_cache_path}0", sudo=True)

        if "rbd_data" not in out[0]:
            log.debug(out)
            log.error("No cache files found")
            return 1

        log.info(f"Cache files found: {out}")
        return 0

    except Exception as err:
        log.error(err)
        return 1

    finally:
        rbd.clean_up(pools=[pool])
        node.exec_command(cmd=f"rm -rf {immutable_cache_path}", sudo=True)


def run(ceph_cluster, **kw):
    """
    Test to configure and validate ceph immutable object cache files.

    Args:
        ceph_cluster: ceph cluster object
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 for failure.
    """
    log.info(
        "Starting CEPH-83574134 - Test to configure and validate"
        "ceph immutable object cache files."
    )

    rbd_obj = initial_rbd_config(**kw)["rbd_reppool"]
    client_node = rbd_obj.ceph_client

    # To configure immutable object cache
    if configure_immutable_cache(rbd_obj, client_node, "user1"):
        log.error("Immutable object cache configuration failed")
        return 1

    log.info("Immutable object cache configuration completed successfully.")

    # To verify and validate immutable object cache files
    if verify_cache_objects(rbd_obj, client_node, **kw):
        log.error("Immutable object cache file creation failed")
        return 1

    log.info("Immutable object cache files created successfully.")
    return 0
