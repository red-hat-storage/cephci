"""RBD Persistent write back cache.

Environment and limitations:
 - The cluster should have 5 nodes + 1 SSD cache node
 - cluster/global-config-file: config/quincy/upi/octo-5-node-env.yaml
 - Should be Bare-metal.

Support
- Configure cluster with PWL Cache.
- Only replicated pool supported, No EC pools.
- Cannot be executed in parallel, if it is same pool or image.
"""

from time import sleep

from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.rbd.rbd_peristent_writeback_cache import (
    PersistentWriteAheadLog,
    PWLException,
    fio_ready,
    get_entity_level,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def validate_pwl_without_exclusive_lock(cache, cfg, client):
    """Validate PWL cache without exclusive lock feature.

    Method would validate that cache file will not be created
     when exclusive lock feature of image is disabled.

    If Image exclusive-lock = No, then  PWL Cache = No

    Args:
        cache: PersistentWriteAheadLog object
        cfg: test config
        client: cache client node
    """
    config_level, entity = get_entity_level(cfg)
    pool, image = cfg["image_spec"].split("/")
    cache.rbd.toggle_image_feature(
        pool, image, feature_name="object-map,exclusive-lock", action="disable"
    )
    cache.configure_pwl_cache(
        cfg["rbd_persistent_cache_mode"],
        config_level,
        entity,
        cfg["cache_file_size"],
    )

    # Run FIO, validate cache file non-existence during entire FIO.
    with parallel() as p:
        p.spawn(run_fio, **fio_ready(cfg, client))
        try:
            cache.check_cache_file_exists(
                cfg["image_spec"],
                cfg["fio"].get("runtime", 120),
                **cfg,
            )
            raise Exception("PWL Cache file created with exclusive lock disabled..")
        except PWLException as error:
            log.info(f"{error} as expected in entire FIO execution...")


def run(ceph_cluster, **kw):
    """RBD persistent write back cache.

    Args:
        ceph_cluster: ceph cluster object
        **kw: test parameters

    Pre-requisites :
        - need client node with ceph-common package, conf and keyring files
        - FIO should be installed on the client.

    """
    log.info("Running PWL....")
    config = kw.get("config")

    for level in config.get("levels"):
        cache_client = get_node_by_id(ceph_cluster, config["client"])
        kw["ceph_client"] = cache_client
        rbd_obj = initial_rbd_config(**kw)["rbd_reppool"]
        pool = config["rep_pool_config"]["pool"]
        image_spec = (
            f"{config['rep_pool_config']['pool']}/{config['rep_pool_config']['image']}"
        )
        image = config["rep_pool_config"]["image"]
        resize = config["rep_pool_config"].get("resize_to", "")
        size = config["rep_pool_config"]["size"]

        pwl = PersistentWriteAheadLog(rbd_obj, cache_client, config.get("drive"))

        config["level"] = level
        config_level, entity = get_entity_level(config)

        try:
            # Configure PWL
            pwl.configure_cache_client()

            if config.get("validate_exclusive_lock"):
                config["image_spec"] = image_spec
                validate_pwl_without_exclusive_lock(pwl, config, cache_client)
                return 0

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
                    image_spec,
                    config["fio"].get("runtime", 120),
                    **config,
                )

            # Increasing the image size
            if resize and rbd_obj.image_resize(pool, image, resize):
                log.error(f"Image resize failed for {image}")
                return 1

            # shrinking the image to original size
            if resize and rbd_obj.image_resize(pool, image, size):
                log.error(f"Image resize failed for {image}")
                return 1

            image_status = rbd_obj.image_status(image_spec=image_spec, output=True)

            while "Watchers: none" not in image_status:
                log.info(f"Image status: {image_status}")
                sleep(10)
                image_status = rbd_obj.image_status(image_spec=image_spec, output=True)

            # Removing the image
            rbd_obj.remove_image(pool_name=pool, image_name=image)
            if rbd_obj.image_exists(pool_name=pool, image_name=image):
                log.error(f"Image {image} not deleted successfully")
                return 1
        except Exception as err:
            log.error(err)
            return 1
        finally:
            if config.get("cleanup"):
                if level != "image":
                    pwl.remove_pwl_configuration(config_level, entity)
                rbd_obj.clean_up(pools=[pool])
                pwl.cleanup()

    return 0
