"""Test case covered -
CEPH-83574644 - Validate "rbd_compression_hint" config
settings on globally, Pool level, and image level.

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Create a pool and an Image, write some data on it
2. set bluestore_compression_mode to passive to enable rbd_compression_hint feature
3. Set compression_algorithm, compression_mode and compression_ratio for the pool
4. verify "rbd_compression_hint" to "compressible" on global, pool and image level
5. verify "rbd_compression_hint" to "incompressible" on global, pool and image level
6. Repeat the above steps for ecpool
"""

import json

from tests.rbd.rbd_utils import get_ceph_config, initial_rbd_config, set_ceph_config
from utility.log import Log

log = Log(__name__)


def validate_compression(self, mode, image_spec, **kw):
    """
    Method to validate after setting rbd_compression_hint
    data is getting compressed or not.

    Args:
        mode: type of compression hints (compressible or incompressible)
        image_spec: poolname + imagename
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 for failure.
    """
    config = kw.get("config")
    io = config.get("io_total", "1G")
    kw["io"] = io
    kw["imagespec"] = image_spec
    pool_name = image_spec.split("/")[0]

    # running io to check compression of data
    self.rbd_bench(**kw)

    out = self.exec_cmd(cmd="ceph df detail --format json-pretty", output=True)
    output = json.loads(out.rstrip())
    pool_info = output["pools"]

    # Iterate over the pools to get compressed data information
    for pool in pool_info:
        if pool["name"] == pool_name:
            compressed_data = pool["stats"]["compress_bytes_used"]
            break
    log.debug(
        f"Pool statistics refer compress_bytes_used for bytes compressed: {pool['stats']}"
    )

    if mode == "compressible":
        if compressed_data == 0:
            log.error("Test Failed: Data did not get compressed in compressible mode")
            return 1
        else:
            log.info(
                f"Test Passed: Data got compressed, bytes compressed: {compressed_data}"
            )
            return 0

    elif mode == "incompressible":
        if compressed_data != 0:
            log.error("Test Failed: Data gets compressed in incompressible mode")
            return 1
        else:
            log.info(
                "Test Passed: Data did not get compressed due to incompressible mode set"
            )
            return 0
    else:
        log.error(f"Invalid mode: {mode}")
        return 1


def test_compression(rbd, pool_type, **kw):
    """
    Run rbd_compression_hint test on supported options,
    i.e compressible and incompressible.

    Args:
        rbd: RBD object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 for failure.
    """
    config = kw.get("config")

    pool_name = config[pool_type]["pool"]

    compression_configs = {
        "compression_algorithm": config.get("compression_algorithm"),
        "compression_mode": config.get("compression_mode"),
        "compression_required_ratio": config.get("compression_ratio"),
    }

    try:
        # set bluestore_compression_mode to passive
        set_ceph_config(rbd, "osd", "bluestore_compression_mode", "passive")

        if (
            get_ceph_config(
                rbd, "osd", "bluestore_compression_mode", output=True
            ).strip()
            != "passive"
        ):
            log.error("Not able to set bluestore_compression_mode to passive")
            return 1

        log.info("bluestore_compression_mode set to passive successfully")

        # commands to set compression related configuration
        rbd.exec_cmd(
            cmd=" && ".join(
                [
                    f"ceph osd pool set {pool_name} {k} {v}"
                    for k, v in compression_configs.items()
                ]
            )
        )

        for option in ["compressible", "incompressible"]:
            # set rbd_compression_hint on global, pool and image level
            for level in ["global", "pool", "image"]:
                image_name = rbd.random_string() + "_test_image"
                # creating separate image for each level to validate compression
                rbd.create_image(
                    pool_name,
                    image_name,
                    size="10G",
                )
                image_spec = pool_name + "/" + image_name

                entity = {"global": "global", "pool": pool_name, "image": image_spec}
                rbd.set_config(level, entity[level], "rbd_compression_hint", option)

                if (
                    rbd.get_config(
                        level, entity[level], "rbd_compression_hint", output=True
                    ).strip()
                    != option
                ):
                    log.error(
                        f"Not able to set rbd_compression_hint on {option} mode in {level} level"
                    )
                    return 1
                log.info(
                    f"Able to set rbd_compression_hint on {option} mode in {level} level"
                )

                validate_compression(rbd, option, image_spec, **kw)

                # remove rbd_compression_hint on each level
                rbd.remove_config(level, entity[level], "rbd_compression_hint")

                out, err = rbd.get_config(
                    level, entity[level], "rbd_compression_hint", all=True
                )
                log.debug(err)

                if "rbd: rbd_compression_hint is not set" not in str(err):
                    log.error(
                        f"Not able to remove rbd_compression_hint on {option} mode in {level} level"
                    )
                    return 1
                log.info(
                    f"Able to remove rbd_compression_hint on {option} mode in {level} level"
                )
                # deleting the image to test other levels with new image
                # otherwise older compressed data got retained the same info.
                rbd.remove_image(pool_name, image_name)

        return 0

    except Exception as error:
        log.error(str(error))
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """
    Test for rbd_compression_hint settings.
    Args:
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 for failure.
    """
    log.info(
        "Starting CEPH-83574644 - Validate rbd_compression_hint config "
        "settings on global level, Pool level, and image level"
    )

    rbd_obj = initial_rbd_config(**kw)

    if rbd_obj:
        if "rbd_reppool" in rbd_obj:
            log.info("Executing test on Replicated pool")
            if test_compression(rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw):
                return 1

        if "rbd_ecpool" in rbd_obj:
            log.info("Executing test on EC pool")
            if test_compression(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw):
                return 1

    return 0
