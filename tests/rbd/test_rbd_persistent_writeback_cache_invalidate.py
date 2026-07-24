"""RBD Persistent write back cache, Test cache invalidate at
client, pool and image level

Test Case Covered:
CEPH-83574709 - Verify persistent write back cache invalidate at
image/pool/client(node) level in SSD mode

Steps :
1) Setup persistent write cache for an image at client, pool and image level
2) Write data to the image using fio jobs
3) While IOs are running, issue invalidate cache and check cache status when
Ios started and after issuing command check PWL cache file is deleted in the
mounted path , cache feature is disabled in cache status

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
from tests.rbd.rbd_peristent_writeback_cache import (
    PersistentWriteAheadLog,
    fio_ready,
    get_entity_level,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def validate_cache_invalidate(cache, cfg, client):
    """
    Perform cache invalidate and verify the results

    Args:
        cache: PersistentWriteAheadLog object
        cfg: test config
        client: cache client node
    """
    cache.check_cache_file_exists(
        f"{cfg['rep_pool_config']['pool']}/{cfg['rep_pool_config']['image']}",
        cfg["fio"].get("runtime", 120),
        **cfg,
    )
    out = cache.get_image_cache_status(cfg["image_spec"])
    log.debug(f"RBD status of image: {out}")

    cache_file = out.get("persistent_cache", {}).get("path")
    cache_file = cache_file.replace(f"{cache.pwl_path}/", "")
    sleep(60)
    out, err = cache.invalidate(cfg["image_spec"])
    if not out:
        log.info(f"Cache invalidate for image {cfg['image_spec']} is success")
    else:
        log.error(
            f"Cache invalidate failed for image {cfg['image_spec']} with error {err}"
        )
        raise Exception(
            f"Cache flush invalidate for image {cfg['image_spec']} with error {err}"
        )

    # Verify that the cache file is removed, cache feature is disabled in cache status
    cache_present = False
    try:
        cache.check_cache_file_exists(
            cfg["image_spec"],
            20,
            **cfg,
        )
        log.error("Cache file is present even after cache invalidate")
        cache_present = True
    except Exception:
        log.info("Cache file is removed after cache invalidate")

    if cache_present:
        raise Exception(
            f"Cache file is present in cache status even after \
                        cache invalidate for {cfg['image_spec']}"
        )

    out, err = client.exec_command(cmd=f"ls -l {cache.pwl_path}")
    if cache_file in out + err:
        log.error(
            f"Cache file exists in specified location {cache.pwl_path} even after cache invalidate"
        )
        raise Exception(
            "Cache file exists in specified location even after cache invalidate"
        )
    else:
        log.info(f"Cache file deleted from the specified location {cache.pwl_path}")


def run(ceph_cluster, **kw):
    """Verify persistent write back cache invalidate

    Args:
        ceph_cluster: ceph cluster object
        **kw: test parameters

    Pre-requisites :
        - need client node with ceph-common package, conf and keyring files
        - FIO should be installed on the client.

    """
    log.info(
        "Running test - CEPH-83574709 - Verify persistent write back cache \
            invalidate at image/pool/client(node) level in SSD mode	"
    )
    config = kw.get("config")
    cache_client = get_node_by_id(ceph_cluster, config["client"])
    kw["ceph_client"] = cache_client
    rbd_obj = initial_rbd_config(**kw)["rbd_reppool"]
    pool = config["rep_pool_config"]["pool"]
    image = f"{config['rep_pool_config']['pool']}/{config['rep_pool_config']['image']}"
    config["image_spec"] = image

    pwl = PersistentWriteAheadLog(rbd_obj, cache_client, config.get("drive"))

    for level in config.get("levels"):
        config["level"] = level
        config_level, entity = get_entity_level(config)

        try:
            # Configure PWL
            pwl.configure_cache_client()

            config_level, entity = get_entity_level(config)

            pwl.configure_pwl_cache(
                config["rbd_persistent_cache_mode"],
                config_level,
                entity,
                config["cache_file_size"],
            )

            # Run FIO, validate cache file existence in self.pwl_path under cache node
            with parallel() as p:
                p.spawn(run_fio, **fio_ready(config, cache_client))
                pwl.check_cache_file_exists(
                    image,
                    config["fio"].get("runtime", 120),
                    **config,
                )

            # Run FIO, perform cache invalidate and verify the same
            with parallel() as p:
                p.spawn(run_fio, **fio_ready(config, cache_client))
                p.spawn(validate_cache_invalidate, pwl, config, cache_client)

            return 0
        except Exception as err:
            log.error(err)
        finally:
            if config.get("cleanup"):
                pwl.remove_pwl_configuration(config_level, entity)
                rbd_obj.clean_up(pools=[pool])
                pwl.cleanup()

        return 1
