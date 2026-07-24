"""Test case covered -
CEPH-83575581 - Verify the usage of ceph RBD images as
dm-cache and dm-write cache from LVM side to enhance cache mechanism.

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Create RBD based pool and an Image
2. get rbd based image disk using rbd map
3. Create physical volume for RBD based disk
4. Create volume group for RBD based disk
5. Create cache disk, meta disk and data disk for the volume group
6. make disk as dm-cache and dm-write cache based type of cache specified
7. Create Xfs file system on metadata parted disk for file system purpose
8. mount some files on cache disk and write some I/O on it.
9. create snapshot of cache disk image
10. check ceph health status
"""

from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.lvm_utils import lvconvert, lvm_create, lvm_uncache, pvcreate, vgcreate
from utility.utils import run_fio, run_mkfs

log = Log(__name__)


def test_configure_cache(rbd_obj, client_node, pool_name, image_name, cache_type, **kw):
    """Method to configure cache mechansim as dm-cache
    and dm-write cache for RBD image based devices.

    Args:
        rbd_obj: RBD object
        client_node: cache client node
        pool_name: name of the pool
        image_name: name of the image
        cache_type: type of cache i.e dm-cache and dm-write cache
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 for failure.

    """
    config = kw.get("config")
    device_names = []
    lv_sizes = {"data_disk": "4G", "cache_disk": "2G", "meta_disk": "2G"}

    if cache_type == "cache_pool":
        vg_name = "test_cache_vg_pool"
        test_folder_path = "/tmp/test_cache_pool"
        VG_disk = "/dev/test_cache_vg_pool/data_disk"
    elif cache_type == "cache_vol":
        vg_name = "test_cache_vg_vol"
        test_folder_path = "/tmp/test_cache_vol"
        VG_disk = "/dev/test_cache_vg_vol/data_disk"
    else:
        vg_name = "test_write_cache_vg"
        test_folder_path = "/tmp/test_write_cache"
        VG_disk = "/dev/test_write_cache_vg/data_disk"
    try:
        device_names.append(rbd_obj.image_map(pool_name, image_name)[:-1])
        pvcreate(client_node, device_names[0])
        vgcreate(client_node, vg_name, device_names[0])

        for name, size in lv_sizes.items():
            lvm_create(client_node, name, vg_name, size)

        if cache_type == "cache_pool":
            log.info("Configuring dm-cache with cachepool + rbd device for main LV")

            # to configure dm-cache pool in LVM by associating a metadata disk with a cache disk
            command = f"echo 'y' | lvconvert --type cache-pool --poolmetadata {vg_name}/meta_disk {vg_name}/cache_disk"

        elif cache_type == "cache_vol":
            log.info("Configuring dm-cache with cachevol + rbd device for main LV")

            # to configure dm-cache volume in LVM by associating a metadata disk with a cache disk
            command = f"echo 'y' | lvconvert --type cache --cachevol {vg_name}/meta_disk {vg_name}/cache_disk"

        else:
            log.info(
                "Configuring dm-write cache with cachevol + rbd device for main LV"
            )

            # Configure dm-write cache
            command = f"lvchange --activate n {vg_name}/cache_disk"

        client_node.exec_command(cmd=command, sudo=True)

        lvconvert(client_node, cache_type, vg_name, "cache_disk", "data_disk")

        # creating XFS filesystem on VG_disk
        log.info("Creating file system and mounting the cache disk with IO")

        run_mkfs(client_node=client_node, device_name=VG_disk, type="xfs")

        command = f"mkdir -p {test_folder_path}"
        client_node.exec_command(cmd=command, sudo=True)

        # mount the cached disk and run fio
        command = f"mount {VG_disk} {test_folder_path}"
        client_node.exec_command(cmd=command, sudo=True)

        run_fio(
            client_node=client_node,
            device_name=VG_disk,
            runtime=config.get("runtime", 30),
        )

        # check ceph health
        out, error = client_node.exec_command(cmd="ceph -s", sudo=True)

        if "HEALTH_ERR" in out:
            log.debug(out)
            log.error("Cluster went to error health state")
            return 1

        # Flush the cache back to main LV
        log.info("Flushing the cache back to main LV")
        lvm_uncache(client_node, VG_disk)

    except Exception as err:
        log.error(err)
        return 1

    finally:
        log.info("Starting to cleanup cache drive")
        client_node.exec_command(cmd=f"wipefs -af {device_names[0]}", sudo=True)

    return 0


def run(ceph_cluster, **kw):
    """
    Test to configure dm-cache and dm-writecache for RBD images
    from LVM side to enhance cache mechanism.

    Args:
        ceph_cluster: ceph cluster object
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 for failure.
    """
    log.info(
        "Starting CEPH-83575581 - Verify the usage of ceph RBD images as "
        "dm-cache and dm-write cache from LVM side to enhance cache mechanism."
    )

    config = kw.get("config")
    rbd = initial_rbd_config(**kw)["rbd_reppool"]
    cache_client = rbd.ceph_client

    pool_name = config["rep_pool_config"]["pool"]

    # read/write operations + dm-cache as Cache pool + RBD image as Main LV
    image_name_cache_pool = config["rep_pool_config"]["image1"]

    # read/write operations + dm-cache as Cache vol + RBD image as Main LV
    image_name_cache_vol = config["rep_pool_config"]["image2"]

    # write operations + dm-cache write as Cachevol + RBD image as Main LV
    image_name_writecache = config["rep_pool_config"]["image3"]

    image_size = config["rep_pool_config"]["size"]

    try:
        for cache_type, image_name in [
            ("cache_pool", image_name_cache_pool),
            ("cache_vol", image_name_cache_vol),
            ("writecache", image_name_writecache),
        ]:
            rbd.create_image(
                pool_name,
                image_name,
                size=image_size,
            )

            # To configure dm-cache or dm-writecache for rbd image based disk
            if test_configure_cache(
                rbd,
                cache_client,
                pool_name,
                image_name,
                cache_type=cache_type,
                **kw,
            ):
                log.error(f"DM-{cache_type} creation failed")
                return 1
            else:
                log.info(f"Able to make RBD image as DM-cache with {cache_type}")

                # creating snapshot of cache disked image
                if rbd.snap_create(pool_name, image_name, "cache_snap"):
                    log.error("Creation of snapshot failed")
                    return 1
                else:
                    log.info("Able to create snapshot of cache based image")

    except Exception as err:
        log.error(err)
        return 1

    finally:
        rbd.clean_up(pools=[pool_name])

    return 0
