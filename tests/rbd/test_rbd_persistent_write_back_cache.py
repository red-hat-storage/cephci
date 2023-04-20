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
    rbd_obj = initial_rbd_config(**kw)["rbd_reppool"]
    cache_client = get_node_by_id(ceph_cluster, config["client"])
    pool = config["rep_pool_config"]["pool"]
    image = f"{config['rep_pool_config']['pool']}/{config['rep_pool_config']['image']}"

    pwl = PersistentWriteAheadLog(rbd_obj, cache_client, config.get("drive"))
    config_level, entity = get_entity_level(config)

    try:
        # Configure PWL
        pwl.configure_cache_client()

        if config.get("validate_exclusive_lock"):
            config["image_spec"] = image
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
                image,
                config["fio"].get("runtime", 120),
                **config,
            )

        return 0
    except Exception as err:
        log.error(err)
    finally:
        if config.get("cleanup"):
            pwl.remove_pwl_configuration(config_level, entity)
            rbd_obj.clean_up(pools=[pool])
            pwl.cleanup()

    return 1
