"""RBD Persistent write back cache with encrypted images.

Test Case Covered:
CEPH-83575409 - Pmem/SSD mode write back cache on encrypted images.

Steps :
1) create image with encryption(luks1,luks2) feature enabled
2) Setup persistent write cache for that encrypted image
3) Write data to the image using fio jobs check cache status
4) Repeat the test on client, pool and image level

Pre-requisites :
- need client node with ceph-common package, conf and keyring files
- FIO should be installed on the client.

Environment and limitations:
- The cluster should have 5 nodes + 1 SSD cache node
- cluster/global-config-file: config/quincy/upi/octo-5-node-env.yaml
- Should be Bare-metal.

Support
- Configure cluster with PWL Cache.
- Only replicated pool supported, No EC pools."""

from ceph.parallel import parallel
from ceph.rbd.utils import random_string
from tests.rbd.rbd_peristent_writeback_cache import (
    PersistentWriteAheadLog,
    get_entity_level,
)
from tests.rbd.rbd_utils import create_passphrase_file, initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def run(ceph_cluster, **kw):
    """RBD Persistent write back cache with encrypted images.

    Args:
        ceph_cluster: ceph cluster object
        **kw: test parameters

    Returns:
        int: 0 on success, 1 on failure

    """
    log.info(
        "Running test - CEPH-83575409 - Pmem/SSD mode write back cache on encrypted images"
    )
    config = kw.get("config")

    try:
        for level in config.get("levels"):
            config["level"] = level
            rbd = initial_rbd_config(**kw)["rbd_reppool"]

            pool = config["rep_pool_config"]["pool"]
            image = config["rep_pool_config"]["image"]
            image_spec = f"{pool}/{image}"
            size = config["rep_pool_config"]["size"]

            cache_client = ceph_cluster.get_nodes(role="client")[0]
            pwl = PersistentWriteAheadLog(rbd, cache_client, config.get("drive"))
            config_level, entity = get_entity_level(config)

            rbd.create_image(pool, image, size)

            # Configute cache client
            pwl.configure_cache_client()

            # Configure PWL
            pwl.configure_pwl_cache(
                config["rbd_persistent_cache_mode"],
                config_level,
                entity,
                config["cache_file_size"],
            )

            for luks in ["luks1", "luks2"]:
                # Adding this check as removed image for luks1
                if luks == "luks2":
                    rbd.create_image(pool, image, size)

                # For image level config reconfigure cache client and PWL for luks2
                if luks == "luks2" and level == "image":
                    pwl.configure_cache_client()
                    pwl.configure_pwl_cache(
                        config["rbd_persistent_cache_mode"],
                        config_level,
                        entity,
                        config["cache_file_size"],
                    )

                # Create passaphrase file and encrypt the image
                passphrase = random_string(len=4) + "_passphrase.bin"
                create_passphrase_file(rbd, passphrase)

                if rbd.encrypt(image_spec, luks, passphrase):
                    log.error(f"Apply RBD {luks} encryption failed on {image_spec}")
                    return 1
                log.info(f"Applied RBD {luks} encryption success on {image_spec}")

                fio_args = {
                    "client_node": cache_client,
                    "pool_name": pool,
                    "image_name": image,
                    "io_type": "write",
                    "size": config["fio"]["size"],
                }

                # Run FIO, validate cache file existence in self.pwl_path under cache node
                with parallel() as p:
                    p.spawn(run_fio, **fio_args)
                    pwl.check_cache_file_exists(
                        image_spec,
                        config["fio"].get("runtime", 120),
                        **config,
                    )
                log.info(
                    f"Persistent write back cache with {level} level on encryption {luks} passed"
                )

                # Removing image to apply encryption of each type
                if rbd.remove_image(pool, image):
                    log.error(f"Removal of image {image} failed")
                    return 1
        return 0

    except Exception as err:
        log.error(err)
        return 1

    finally:
        if config.get("cleanup"):
            rbd.clean_up(pools=[pool])
            pwl.cleanup()
