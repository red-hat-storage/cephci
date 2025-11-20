"""
Test Module to test functionalities of bluestore data compression.
scenario-1: Validate default compression values
scenario-2: Enable bluestore_write_v2 and validate
scenario-3: Disable bluestore_write_v2 and validate
"""

import random
import string

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.test_bluestore_comp_enhancements_class import (
    BLUESTORE_ALLOC_HINTS,
    COMPRESSION_MODES,
    BluestoreDataCompression,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Module to test functionalities of bluestore data compression.
        scenario-1: Validate default compression values
        scenario-2: Enable bluestore_write_v2 and validate
        scenario-3: Disable bluestore_write_v2 and validate
    """
    log.info(run.__doc__)
    config = kw["config"]

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    scenarios_to_run = config.get(
        "scenarios_to_run",
        [
            "scenario-1",
            "scenario-2",
            "scenario-3",
            "scenario-4",
            "scenario-5",
            "scenario-6",
            "scenario-7",
        ],
    )
    recompression_min_gain_to_test = config.get("recompression_min_gain_to_test", [1.2])
    object_sizes_to_test = config.get("object_sizes_to_test", [92000])
    min_alloc_size_to_test = config.get("min_alloc_size_to_test", [4096])
    min_alloc_size_variations = config.get("min_alloc_size_variations", [10, -10])
    pool_level_compression = config.get("pool_level_compression", True)
    compression_config = {
        "rados_obj": rados_obj,
        "mon_obj": mon_obj,
        "cephadm": cephadm,
        "client_node": client_node,
        "ceph_cluster": ceph_cluster,
    }
    bluestore_compression = BluestoreDataCompression(**compression_config)
    try:

        log.info(
            "\n\n ************ Execution begins for bluestore data compression scenarios ************ \n\n"
        )

        if "scenario-1" in scenarios_to_run:
            log.info("STARTED: Scenario 1: Validate default compression values")
            bluestore_compression.validate_default_compression_values()
            log.info("COMPLETED: Scenario 1: Validate default compression values")

        if "scenario-2" in scenarios_to_run:
            log.info("STARTED: Scenario 2: Enable bluestore_write_v2 and validate")
            bluestore_compression.toggle_bluestore_write_v2(toggle_value="true")
            log.info("COMPELTED: Scenario 2: Enable bluestore_write_v2 and validate")

        if "scenario-3" in scenarios_to_run:
            log.info(
                "STARTED: Scenario 3: Compression mode tests ( passive, aggressive, force, none )"
            )
            for compression_mode in [
                COMPRESSION_MODES.PASSIVE,
                COMPRESSION_MODES.AGGRESSIVE,
                COMPRESSION_MODES.FORCE,
                COMPRESSION_MODES.NONE,
            ]:
                for obj_alloc_hint in [
                    BLUESTORE_ALLOC_HINTS.NOHINT,
                    BLUESTORE_ALLOC_HINTS.COMPRESSIBLE,
                    BLUESTORE_ALLOC_HINTS.INCOMPRESSIBLE,
                ]:
                    pool_name = f"test-{compression_mode}-{obj_alloc_hint}"
                    kwargs = {
                        "pool_name": pool_name,
                        "compression_mode": compression_mode,
                        "alloc_hint": obj_alloc_hint,
                        "pool_level_compression": pool_level_compression,
                    }
                    bluestore_compression.validate_compression_modes(**kwargs)

                    if rados_obj.delete_pool(pool=pool_name) is False:
                        raise Exception(f"Deleting pool {pool_name} failed")
            log.info(
                "STARTED: Scenario 3: Compression mode tests ( passive, aggressive, force, none )"
            )

        if "scenario-4" in scenarios_to_run:
            log.info(
                "STARTING :: Scenario 4 -> Tests for partial overwrite with varying write_size"
            )
            compression_mode = COMPRESSION_MODES.FORCE
            obj_alloc_hint = BLUESTORE_ALLOC_HINTS.COMPRESSIBLE

            for recompression_min_gain in recompression_min_gain_to_test:
                for object_size in object_sizes_to_test:
                    pool_id = "".join(
                        random.choices(string.ascii_letters + string.digits, k=6)
                    )
                    pool_name = f"test-recompress-{pool_id}"

                    kwargs = {
                        "pool_name": pool_name,
                        "compression_mode": compression_mode,
                        "alloc_hint": obj_alloc_hint,
                        "recompression_min_gain": recompression_min_gain,
                        "write_size": object_size,
                        "pool_level_compression": pool_level_compression,
                    }
                    bluestore_compression.partial_overwrite(**kwargs)
            log.info(
                "COMPLETED :: Scenario 4 -> Tests for partial overwrite with varying write_size"
            )

        if "scenario-5" in scenarios_to_run:
            for min_alloc_size in min_alloc_size_to_test:
                pool_id = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )
                pool_name = f"test-recompress-{pool_id}-{min_alloc_size}"
                compression_mode = COMPRESSION_MODES.FORCE

                kwargs = {
                    "pool_name": pool_name,
                    "compression_mode": compression_mode,
                    "min_alloc_size": min_alloc_size,
                    "min_alloc_size_variations": min_alloc_size_variations,
                    "pool_level_compression": pool_level_compression,
                }

                bluestore_compression.min_alloc_size_test(**kwargs)

        if "scenario-6" in scenarios_to_run:
            log.info("STARTED: Scenario 6: Disable bluestore_write_v2 and validate")
            bluestore_compression.toggle_bluestore_write_v2(toggle_value="false")
            log.info("COMPELTED: Scenario 6: Disable bluestore_write_v2 and validate")

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        # log cluster health
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # delete all rados pools
        rados_obj.rados_pool_cleanup()
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test executio
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed validation of bluestore v2 data compression.")
    return 0
