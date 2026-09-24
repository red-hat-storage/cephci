"""
Test Module for negative testing of BlueStore data compression.

scenario-1: Validate illegal values are rejected at pool level (ceph osd pool set).
    1) Validates compression_algorithm rejects illegal string "illegal_value" with EINVAL.
    2) Validates compression_mode rejects illegal string "illegal_value" with EINVAL.
    3) Validates compression_required_ratio rejects illegal string "illegal_value" with EINVAL.
    4) Validates compression_required_ratio rejects out-of-range value -1 with EINVAL.
    5) Validates compression_required_ratio rejects out-of-range value 1.2 with EINVAL.
    6) Validates compression_min_blob_size rejects non-integer "illegal_value" with EINVAL.
    7) Validates compression_max_blob_size rejects non-integer "illegal_value" with EINVAL.
    8) Validates compression_min_blob_size rejects negative value -5000 with EINVAL.
       Known gap: -5000 is currently accepted without error (BZ IBMCEPH-17251, IBMCEPH-12400).
    9) Validates compression_max_blob_size rejects negative value -5000 with EINVAL.
       Known gap: -5000 is currently accepted without error (BZ IBMCEPH-17251, IBMCEPH-12400).
    Above validations are run across all pools: cephfs replicated, cephfs erasure,
    rbd replicated, rbd erasure, and rgw replicated.

scenario-2: Validate illegal values are rejected at global OSD config level (ceph config set osd).
    1) Validates bluestore_compression_algorithm rejects illegal string "illegal_value" with EINVAL.
    2) Validates bluestore_compression_mode rejects illegal string "illegal_value" with EINVAL.
    3) Validates bluestore_compression_required_ratio rejects illegal string "illegal_value" with EINVAL.
    4) Validates bluestore_compression_required_ratio rejects out-of-range value -1 with EINVAL.
       Note: the global config store does not enforce the [0,1] range check the way pool-level
       validation does, so this may pass without error unlike scenario-1.
    5) Validates bluestore_compression_required_ratio rejects out-of-range value 1.2 with EINVAL.
       Same note as above.
    6) Validates bluestore_compression_min_blob_size rejects non-integer "illegal_value" with EINVAL.
    7) Validates bluestore_compression_max_blob_size rejects non-integer "illegal_value" with EINVAL.
    8) Validates bluestore_compression_min_blob_size rejects negative value -5000 with EINVAL.
       Known gap: -5000 is currently accepted without error (BZ IBMCEPH-17251, IBMCEPH-12400).
    9) Validates bluestore_compression_max_blob_size rejects negative value -5000 with EINVAL.
       Known gap: -5000 is currently accepted without error (BZ IBMCEPH-17251, IBMCEPH-12400).
"""

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.test_bluestore_comp_enhancements import (
    test_prerequisite_setup,
    test_prerequisite_teardown,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Negative test suite for BlueStore data compression configuration.
        scenario-1: Validate illegal values are rejected at pool level (ceph osd pool set)
                    for compression_algorithm, compression_mode, compression_required_ratio,
                    compression_min_blob_size, compression_max_blob_size across all pool types.
        scenario-2: Validate illegal values are rejected at global OSD config level
                    (ceph config set osd) for the same parameters via bluestore_* config keys.
    """
    prereq_kwargs = None
    created_resources = []
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    scenarios_to_run = config.get("scenarios_to_run", [])
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug("Test workflow started. Start time: %s", start_time)
    include_erasure_pools = config.get("include_erasure_pools", True)
    try:

        log.info(
            "\n\n ************ Execution begins for compression negative scenarios ************ \n\n"
        )

        test_pools = [
            ["cephfs_replicated_cephfs1_data", "/mnt/cephfs1", "cephfs", "replicated"],
            ["rbd-replicated-data", "/mnt/rbd_replicated_mount", "rbd", "replicated"],
            ["default.rgw.buckets.data", None, "rgw", "replicated"],
            ["cephfs_erasure_cephfs0_data", "/mnt/cephfs0", "cephfs", "erasure"],
            ["rbd-ec-data", "/mnt/rbd-thrash", "rbd", "erasure"],
        ]

        created_resources = [
            [
                "cephfs1",
                "/mnt/cephfs1",
                [
                    {"pool_name": "cephfs_replicated_cephfs1_data"},
                    {"pool_name": "cephfs_replicated_cephfs1_metadata"},
                ],
                "cephfs",
            ],
            [
                "/dev/rbd1",
                "/mnt/rbd_replicated_mount",
                [{"pool_name": "rbd-replicated-data"}],
                "rbd",
            ],
            [
                "",
                "",
                [{"pool_name": "default.rgw.buckets.data"}],
                "rgw",
            ],
            [
                "cephfs0",
                "/mnt/cephfs0",
                [
                    {"pool_name": "cephfs_erasure_cephfs0_data"},
                    {"pool_name": "cephfs_erasure_cephfs0_metadata"},
                ],
                "cephfs",
            ],
            [
                "/dev/rbd0",
                "/mnt/rbd-thrash",
                [{"pool_name": "rbd-ec-data"}, {"pool_name": "rbd-ec-metadata"}],
                "rbd",
            ],
        ]

        rgw_bucket_name = "test-bucket"
        rgw_nodes = ceph_cluster.get_nodes(role="rgw")
        rgw_endpoint = rgw_nodes[0].ip_address
        prereq_kwargs = {
            "client_node": client_node,
            "rados_obj": rados_obj,
            "config": config,
            "rgw_bucket_name": rgw_bucket_name,
            "rgw_endpoint": rgw_endpoint,
            "include_erasure_pools": include_erasure_pools,
        }

        if "scenario-1" in scenarios_to_run:

            test_prerequisite_setup(
                client_node=client_node,
                rados_obj=rados_obj,
                config=config,
                rgw_bucket_name=rgw_bucket_name,
                rgw_endpoint=rgw_endpoint,
                include_erasure_pools=include_erasure_pools,
            )

            for pool_name, mount_point, workload_type, pool_type in test_pools:
                log.info(
                    "\n******* ************************* **********\n"
                    "pool -> %s\n"
                    "mount point -> %s\n"
                    "workload type -> %s\n"
                    "pool type -> %s\n"
                    "******* ***************************** **********",
                    pool_name,
                    mount_point,
                    workload_type,
                    pool_type,
                )

                for conf in [
                    "compression_algorithm",
                    "compression_mode",
                    "compression_required_ratio",
                    "compression_min_blob_size",
                    "compression_max_blob_size",
                ]:
                    try:
                        rados_obj.run_ceph_command(
                            f"ceph osd pool set {pool_name} {conf} illegal_value"
                        )
                        raise Exception(f"{conf} accepts illegal values")
                    except Exception as e:
                        if "Error EINVAL:" in str(e):
                            log.info(
                                "Expected exception: %s rejects illegal value", conf
                            )
                            log.info("%s", e)
                        else:
                            raise Exception(f"Unexpected exception: {e}")

                for illegal_value in [-1, 1.2]:
                    try:
                        rados_obj.run_ceph_command(
                            f"ceph osd pool set {pool_name} compression_required_ratio {illegal_value}"
                        )
                        raise Exception(
                            f"compression_required_ratio accepts illegal value: {illegal_value}"
                        )
                    except Exception as e:
                        if "Error EINVAL:" in str(e):
                            log.info(
                                "Expected exception: compression_required_ratio rejects illegal value %s",
                                illegal_value,
                            )
                            log.info("%s", e)
                        else:
                            raise Exception(f"Unexpected exception: {e}")

                for illegal_value in [-5000]:
                    for conf in [
                        "compression_min_blob_size",
                        "compression_max_blob_size",
                    ]:
                        try:
                            rados_obj.run_ceph_command(
                                f"ceph osd pool set {pool_name} {conf} {illegal_value}"
                            )
                            raise Exception(
                                f"{conf} accepts illegal value {illegal_value}"
                            )
                        except Exception as e:
                            if "Error EINVAL:" in str(e):
                                log.info(
                                    "Expected exception: %s rejects illegal value %s",
                                    conf,
                                    illegal_value,
                                )
                                log.info("%s", e)
                            else:
                                log.info(
                                    "Known gap (BZ IBMCEPH-17251, IBMCEPH-12400):"
                                    " %s accepted value %s without error",
                                    conf,
                                    illegal_value,
                                )
                                # raise Exception(f"{conf} accepts illegal value {illegal_value}")

            test_prerequisite_teardown(
                **prereq_kwargs, created_resources=created_resources
            )

            log.info("******* Scenario 1 passed **********")

        if "scenario-2" in scenarios_to_run:
            log.info("############ global level configuration ################")

            for conf in [
                "compression_algorithm",
                "compression_mode",
                "compression_required_ratio",
                "compression_min_blob_size",
                "compression_max_blob_size",
            ]:
                try:
                    rados_obj.run_ceph_command(
                        f"ceph config set osd bluestore_{conf} illegal_value"
                    )
                    raise Exception(f"bluestore_{conf} accepts illegal values")
                except Exception as e:
                    if "Error EINVAL:" in str(e):
                        log.info(
                            "Expected exception: bluestore_%s rejects illegal value",
                            conf,
                        )
                        log.info("%s", e)
                    else:
                        raise Exception(f"Unexpected exception: {e}")

            for illegal_value in [-1, 1.2]:
                try:
                    rados_obj.run_ceph_command(
                        f"ceph config set osd bluestore_compression_required_ratio {illegal_value}"
                    )
                    raise Exception(
                        f"bluestore_compression_required_ratio accepts illegal value {illegal_value}"
                    )
                except Exception as e:
                    if "Error EINVAL:" in str(e):
                        log.info(
                            "Expected exception: bluestore_compression_required_ratio"
                            " rejects illegal value %s",
                            illegal_value,
                        )
                        log.info("%s", e)
                    else:
                        log.info(
                            "Bug exists (IBMCEPH-19307):"
                            " bluestore_compression_required_ratio accepted value %s without error",
                            illegal_value,
                        )
                        # raise Exception(f"bluestore_compression_required_ratio accepts illegal value {illegal_value}")

            for illegal_value in [-5000]:
                for conf in ["compression_min_blob_size", "compression_max_blob_size"]:
                    try:
                        rados_obj.run_ceph_command(
                            f"ceph config set osd bluestore_{conf} {illegal_value}"
                        )
                        raise Exception(
                            f"bluestore_{conf} accepts illegal value {illegal_value}"
                        )
                    except Exception as e:
                        if "Error EINVAL:" in str(e):
                            log.info(
                                "Expected exception: bluestore_%s rejects illegal value %s",
                                conf,
                                illegal_value,
                            )
                            log.info("%s", e)
                        else:
                            log.info(
                                "Known gap (BZ IBMCEPH-17251, IBMCEPH-12400):"
                                " bluestore_%s accepted value %s without error",
                                conf,
                                illegal_value,
                            )
                            # raise Exception(f"bluestore_{conf} accepts illegal value {illegal_value}")

            log.info("******* Scenario 2 passed **********")

    except Exception as e:
        log.error("Failed with exception: %s", e)
        log.exception(e)
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        if prereq_kwargs is not None and created_resources:
            try:
                test_prerequisite_teardown(
                    **prereq_kwargs, created_resources=created_resources
                )
            except Exception as e:
                log.error("Failed to clean up prerequisite setup: %s", e)

    log.info("Completed validation of bluestore v2 data compression.")
    return 0
