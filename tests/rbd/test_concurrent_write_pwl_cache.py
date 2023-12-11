"""RBD Persistent write back cache, Test concurrent writes to same image

Test Case Covered:
CEPH-83574720 -
Concurrent writes to check exclusive lock feature with persistent cache enables
via same or different IO tools in SSD mode

Steps :
1) Setup persistent write cache for an image
2) Write data to the same image concurrently using two or more long running fio jobs
on different mount points.
3) Verify that data written is present in both mount points and persistent cache file
is generated while data is being written

Environment and limitations:
 - The cluster should have 5 nodes + 1 SSD cache node
 - cluster/global-config-file: config/quincy/upi/octo-5-node-env.yaml
 - Should be Bare-metal.

Support
- Configure cluster with PWL Cache.
- Only replicated pool supported, No EC pools.
"""
from time import sleep

from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.rbd.krbd_io_handler import krbd_io_handler
from tests.rbd.rbd_peristent_writeback_cache import (
    PersistentWriteAheadLog,
    PWLException,
    device_cleanup,
    get_entity_level,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def validate_concurrent_writes(cache, cfg, client):
    """
    Perform concurrent writes to an image with exclusive lock
    and persistent write back cache enabled, and verify

    Args:
        cache: PersistentWriteAheadLog object
        cfg: test config
        client: cache client node
    """
    try:
        config_level, entity = get_entity_level(cfg)
        pool, image = cfg["image_spec"].split("/")

        cache.rbd.toggle_image_feature(
            pool, image, feature_name="object-map,exclusive-lock", action="enable"
        )
        cache.configure_pwl_cache(
            cfg["rbd_persistent_cache_mode"],
            config_level,
            entity,
            cfg["cache_file_size"],
        )

        mount_path_1 = f"/tmp/mnt_{cache.rbd.random_string(len=5)}/file_1"
        io_config = {
            "rbd_obj": cache.rbd,
            "client": client,
            "size": "10G",
            "do_not_create_image": True,
            "config": {
                "file_size": "1G",
                "file_path": [mount_path_1],
                "image_spec": [cfg["image_spec"]],
                "operations": {
                    "fs": "ext4",
                    "io": False,
                    "mount": True,
                    "nounmap": True,
                    "device_map": True,
                },
                "skip_mkfs": False,
            },
        }
        krbd_io_handler(**io_config)

        mount_path_2 = f"/tmp/mnt_{cache.rbd.random_string(len=5)}/file_2"
        io_config["config"]["operations"]["device_map"] = False
        io_config["config"]["file_path"] = [mount_path_2]
        io_config["config"]["operations"]["nounmap"] = True
        io_config["config"]["skip_mkfs"] = True

        krbd_io_handler(**io_config)

        with parallel() as p:
            p.spawn(
                run_fio,
                client_node=cache.rbd.ceph_client,
                filename=mount_path_1,
                size="1G",
            )
            p.spawn(
                run_fio,
                client_node=cache.rbd.ceph_client,
                filename=mount_path_2,
                size="1G",
            )
            try:
                cache.check_cache_file_exists(
                    cfg["image_spec"],
                    cfg["fio"].get("runtime", 120),
                    **cfg,
                )
                log.info(
                    "PWL Cache file created with exclusive lock for concurrent writes..."
                )
            except PWLException as error:
                log.error(f"{error} in entire FIO execution...")
                raise Exception(error)

        # Verification step, check if both file_1 and file_2 are present in both the
        # mount points

        out, err = cache.rbd.exec_cmd(
            cmd=f"ls -l {mount_path_1.rsplit('/', 1)[0]}", all=True
        )
        out_2, err_2 = cache.rbd.exec_cmd(
            cmd=f"ls -l {mount_path_2.rsplit('/', 1)[0]}", all=True
        )
        if not (err or err_2):
            log.info(f"Files in first mount point: {out}")
            log.info(f"Files in second mount point: {out_2}")
            if out == out_2 and "file_1" in out and "file_2" in out:
                log.info(
                    "All files written by both mount points are present in both the mount points"
                )
            else:
                log.error(
                    "Files in mount point 1 do not match with files in mount point 2"
                )
                return 1
        else:
            log.error("Error while fetching files in the mount points")
            return 1
    except Exception as e:
        log.error(f"Validation of concurrent writes failed with error: {e}")
        raise Exception(f"Validation of concurrent writes failed with error: {e}")
    finally:
        device_cleanup(cache.rbd)
        image_status = cache.rbd.image_status(image_spec=cfg["image_spec"], output=True)

        while "Watchers: none" not in image_status:
            sleep(10)
            image_status = cache.rbd.image_status(
                image_spec=cfg["image_spec"], output=True
            )


def run(ceph_cluster, **kw):
    """Concurrent writes with exclusive lock. persistent write cache

    Args:
        ceph_cluster: ceph cluster object
        **kw: test parameters

    Pre-requisites :
        - need client node with ceph-common package, conf and keyring files
        - FIO should be installed on the client.

    """
    log.info("Running PWL....")
    log.info(
        "Running test - Concurrent writes with exclusive lock. persistent write cache"
    )
    config = kw.get("config")
    for level in config.get("levels"):
        cache_client = get_node_by_id(ceph_cluster, config["client"])
        kw["ceph_client"] = cache_client
        rbd_obj = initial_rbd_config(**kw)["rbd_reppool"]
        pool = config["rep_pool_config"]["pool"]
        image = (
            f"{config['rep_pool_config']['pool']}/{config['rep_pool_config']['image']}"
        )
        config["image_spec"] = image

        pwl = PersistentWriteAheadLog(rbd_obj, cache_client, config.get("drive"))

        config["level"] = level
        config_level, entity = get_entity_level(config)

        try:
            # Configure PWL
            pwl.configure_cache_client()

            validate_concurrent_writes(pwl, config, cache_client)
        except Exception as err:
            log.error(err)
            return 1
        finally:
            if config.get("cleanup"):
                pwl.remove_pwl_configuration(config_level, entity)
                rbd_obj.clean_up(pools=[pool])
                pwl.cleanup()
    return 0
