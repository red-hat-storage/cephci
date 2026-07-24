"""Test case covered -
CEPH-83581376 - verify the performance with immutable cache
and without immutable cache for IO operations

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
8. Read the data from cloned images first time with map, mount, unmount
9. Read the data from cloned images second time with map, mount, unmount
10. note down the time differnce of first read and second read make sure
second read should be less time compare to first read in cache
11. Repeat the above operations without immutable cache
12.check the performance make sure cache gives good performance
"""

from test_rbd_immutable_cache import configure_immutable_cache

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.rbd import create_snap_and_clone
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)

immutable_cache_path = "/mnt/immutable-cache/"


def compare_performance(time_difference_without_cache, time_difference_with_cache):
    """
    Compare the performance of with and without cache.

    Args:
        time_difference_without_cache: Time difference for IO operations without cache.
        time_difference_with_cache: Time difference for IO operations with cache.

    Raises:
        Exception: If the performance with cache is not better than without cache.
    """
    log.info(
        f"Time difference for IO operation with cache: {time_difference_with_cache}"
    )
    log.info(
        f"Time difference for IO operation without cache: {time_difference_without_cache}"
    )

    # Compare performance
    if time_difference_with_cache > time_difference_without_cache:
        log.info(
            "Time difference between consecutive reads is greater with cache compared to without cache"
        )
        log.info("Performance is better with cache enabled")
    else:
        log.error(
            "Time difference between consecutive reads is greater without cache compared to with cache"
        )
        raise Exception("Performance validation with/without cache failed")


def convert_time_to_seconds(real_time_out):
    """
    Parse the 'real' time from the output and convert it to seconds.

    Args:
        real_time_out (str): The output string containing timing information.

    Returns:
        float: The time in seconds.
    """
    real_time_str = real_time_out.split("\n")[1].split("\t")[1]
    minutes, seconds = map(float, real_time_str[:-1].split("m"))
    return minutes * 60 + seconds


def create_and_test_io(client, rbd, config, mount_path, io_type):
    """
    Perform I/O operations on an RBD image and return the time taken.

    Args:
        client (object): The client node object.
        rbd (object): The RBD object.
        config (dict): Configuration dictionary.
        mount_path (str): The path where the image is mounted.
        io_type (str): The type of I/O operation, e.g., "write" or "read".

    Returns:
        str: The time taken for the I/O operation.
    """
    io_config = {
        "rbd_obj": rbd,
        "client": client,
        "size": config["fio"]["size"],
        "do_not_create_image": True,
        "config": {
            "file_size": config["fio"]["size"],
            "file_path": [f"{mount_path}/file"],
            "get_time_taken": True,
            "image_spec": [config["image_spec"]],
            "operations": {
                "fs": "ext4",
                "io": True,
                "mount": True,
                "map": True,
            },
            "skip_mkfs": False,
            "cmd_timeout": 2400,
            "io_type": io_type,
        },
    }
    krbd_io_handler(**io_config)
    return io_config["config"].get("time_taken", "")


def perform_clone_io(client, rbd_obj, pool, image, immutable_cache, mount_path, **kw):
    """
    Perform I/O operations on cloned images and return the time taken.

    Args:
        client (object): Client node object.
        rbd (object): RBD object.
        pool (str): Pool name.
        image (str): Image name.
        immutable_cache(Boolean): True if cache configured.
        mount_path (str): image to mount.
        kw: test parameters.

    Returns:
        str: Time taken for the I/O operation.
    """
    config = kw.get("config")
    rbd = rbd_obj.get("rbd")

    # Write Data to the Parent Image
    fio_args = {
        "client_node": client,
        "pool_name": pool,
        "image_name": image,
        "io_type": "write",
        "size": config["fio"]["size"],
    }

    run_fio(**fio_args)

    # Create snapshot, protect, and clone for the image
    snap = "image_snap" + random_string(len=5)
    clone = "image_clone" + random_string(len=5)
    snap_spec = f"{pool}/{image}@{snap}"
    clone_spec = f"{pool}/{clone}"

    create_snap_and_clone(rbd, snap_spec, clone_spec)

    config["image_spec"] = clone_spec

    #  First read of cloned image
    first_read = create_and_test_io(client, rbd, config, mount_path, "read")

    #  Second read of cloned image
    second_read = create_and_test_io(client, rbd, config, mount_path, "read")

    # Extract the 'real' time from the output to in seconds
    first_read_sec = convert_time_to_seconds(first_read[1])
    second_read_sec = convert_time_to_seconds(second_read[1])

    if immutable_cache and second_read_sec >= first_read_sec:
        raise Exception(
            "second read takes more time than first read with cache hence test failed"
        )

    # Compare the times
    time_difference = abs(first_read_sec - second_read_sec)

    log.info(f"First read of cloned image: {first_read_sec}")
    log.info(f"Second read of cloned image: {second_read_sec}")

    return time_difference


def test_immutable_cache_performance(rbd_obj, **kw):
    """
    Test the performance of Immutable Object Cache with and without cache.

    Args:
        rbd_obj (object): RBD object.
        kw (dict): Test data.

    Raises:
        Exception: If any test step fails.
    """
    ceph_cluster = kw.get("ceph_cluster")
    client = ceph_cluster.get_nodes(role="client")[0]

    try:
        mount_path_with_cache = f"/tmp/mnt_{random_string(len=5)}"
        mount_path_without_cache = f"/tmp/mnt_{random_string(len=5)}"

        for pool_type in rbd_obj.get("pool_types"):
            rbd_config = kw.get("config", {}).get(pool_type, {})
            multi_pool_config = getdict(rbd_config)

            for pool, pool_config in multi_pool_config.items():
                multi_image_config = getdict(pool_config)

                for image in multi_image_config.keys():
                    client.exec_command(
                        cmd="ceph config set client rbd_parent_cache_enabled false",
                        sudo=True,
                    )
                    # Perform I/O operations without cache
                    time_difference_without_cache = perform_clone_io(
                        client=client,
                        rbd_obj=rbd_obj,
                        pool=pool,
                        image=image,
                        immutable_cache=False,
                        mount_path=mount_path_with_cache,
                        **kw,
                    )

                    # Configure Immutable Object Cache
                    if configure_immutable_cache(rbd_obj, client, "user1"):
                        log.error("Immutable object cache configuration failed")
                        return 1
                    log.info(
                        "Immutable object cache configuration completed successfully."
                    )

                    # Perform I/O operations with cache
                    time_difference_with_cache = perform_clone_io(
                        client=client,
                        rbd_obj=rbd_obj,
                        pool=pool,
                        image=image,
                        immutable_cache=True,
                        mount_path=mount_path_without_cache,
                        **kw,
                    )

                    # Check performance comparison
                    compare_performance(
                        time_difference_without_cache, time_difference_with_cache
                    )

                    # Removing immutable cache
                    client.exec_command(cmd=f"rm -rf {immutable_cache_path}", sudo=True)
        return 0

    except Exception as e:
        log.error(f"Performance validation with/without cache failed with error: {e}")
        raise Exception(
            f"Performance validation with/without cache failed with error: {e}"
        )


def run(**kw):
    """
    Test to verify performance of with immutable cache and without.

    Args:
        **kw (dict): Keyword arguments containing test configuration.

    Returns:
        int: Return value. 0 for success, 1 for failure.
    """
    log.info(
        "Running test CEPH-83581376 - verify the performance with immutable cache"
        "and without immutable cache for IO operations"
    )
    try:
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_immutable_cache_performance(rbd_obj=rbd_obj, **kw)
    except Exception as e:
        log.error(
            f"Immutable object cache Performance test failed for the error as {str(e)}"
        )
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
