"""
Test Module to test functionalities of bluestore data compression.
scenario-1: Validate default compression values
scenario-2: Enable bluestore_write_v2 and validate
scenario-3: Compression mode tests (passive, aggressive, force, none)
scenario-4: Tests for partial overwrite with varying write_size
scenario-5: Test minimum allocation size variations
scenario-6: Test blob sizes by writing IO to a compressed pool and validating blob
 sizes match configured minimum blob size
scenario-7: Test target_blob_size validation based on object allocation hints
scenario-8: Disable bluestore_write_v2 and validate
scenario-9: Test compression validation when primary OSD is down
scenario-10: Test compression validation when primary OSD host is down
scenario-11: Test compression validation when primary OSD host is restarted after IO
scenario-12: Test data integrity by writing data, retrieving data, and verifying checksum across cephfs and rbd
scenario-13: Testing compression algorithms across different pools and workload types across cephfs, rgw and rbd
scenario-14: Testing compression modes (none, force, passive, aggressive) across cephfs, rgw and rbd
scenario-15: Testing compression_required_ratio validation with different blob sizes across cephfs, rgw and rbd
scenario-16: Test compression validation post recovery across cephfs, rgw and rbd
"""

import concurrent.futures as cf
import json
import math
import random
import string
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.utils import get_cluster_timestamp, set_osd_out
from ceph.utils import find_vm_node_by_hostname
from tests.misc_env.cosbench import get_or_create_user
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_bluestore_comp_enhancements_class import (
    BLUESTORE_ALLOC_HINTS,
    COMPRESSION_ALGORITHMS,
    COMPRESSION_MODES,
    BluestoreDataCompression,
    CompressionIO,
    IOTools,
)
from tests.rados.test_osd_thrashing import _cleanup_cephfs, _cleanup_rbd
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Module to test functionalities of bluestore data compression.
        scenario-1: Validate default compression values
        scenario-2: Enable bluestore_write_v2 and validate
        scenario-3: Compression mode tests (passive, aggressive, force, none)
        scenario-4: Tests for partial overwrite with varying write_size
        scenario-5: Test minimum allocation size variations
        scenario-6: Test blob sizes by writing IO to a compressed pool and validating blob
         sizes match configured minimum blob size
        scenario-7: Test target_blob_size validation based on object allocation hints
        scenario-8: Disable bluestore_write_v2 and validate
        scenario-9: Test compression validation when primary OSD is down
        scenario-10: Test compression validation when primary OSD host is down
        scenario-11: Test compression validation when primary OSD host is restarted after IO
        scenario-12: Test data integrity by writing data, retrieving data, and verifying checksum across cephfs and rbd
        scenario-13: Testing compression algorithms across different pools and workload types across cephfs, rgw and rbd
        scenario-14: Testing compression modes (none, force, passive, aggressive) across cephfs, rgw and rbd
        scenario-15: Testing compression_required_ratio validation with different blob sizes across cephfs, rgw and rbd
        scenario-16: Test compression validation post recovery across cephfs, rgw and rbd
    """
    log.info(run.__doc__)
    config = kw["config"]

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    scenarios_to_run = config.get("scenarios_to_run", [])
    recompression_min_gain_to_test = config.get("recompression_min_gain_to_test", [1.2])
    object_sizes_to_test = config.get("object_sizes_to_test", [92000])
    min_alloc_size_to_test = config.get("min_alloc_size_to_test", [4096])
    min_alloc_size_variations = config.get("min_alloc_size_variations", [10, -10])
    pool_level_compression = config.get("pool_level_compression", True)
    pool_type = config.get("pool_type", "replicated")
    bluestore_compression = BluestoreDataCompression(
        rados_obj=rados_obj,
        cephadm=cephadm,
        mon_obj=mon_obj,
        client_node=client_node,
        ceph_cluster=ceph_cluster,
        pool_type=pool_type,
    )
    compression_algorithm = config.get("compression_algorithm", "snappy")
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    objectstore_obj = objectstoreToolWorkflows(node=cephadm)
    try:

        log.info(
            "\n\n ************ Execution begins for bluestore data compression scenarios ************ \n\n"
        )

        log.info("Install FIO on client nodes")
        client_nodes = ceph_cluster.get_nodes(role="client")
        cmd = "yum install fio -y"
        for node in client_nodes:
            node.exec_command(cmd=cmd, sudo=True)

        test_pools = [
            ["cephfs_replicated_cephfs1_data", "/mnt/cephfs1", "cephfs", "replicated"],
            ["rbd-replicated-data", "/mnt/rbd_replicated_mount", "rbd", "replicated"],
            ["default.rgw.buckets.data", None, "rgw", "replicated"],
        ]

        if config.get("include_erasure_pools", False):
            test_pools.append(
                ["cephfs_erasure_cephfs0_data", "/mnt/cephfs0", "cephfs", "erasure"]
            )
            test_pools.append(["rbd-ec-data", "/mnt/rbd-thrash", "rbd", "erasure"])

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
        ]

        if config.get("include_erasure_pools", False):
            created_resources.append(
                [
                    "cephfs0",
                    "/mnt/cephfs0",
                    [
                        {"pool_name": "cephfs_erasure_cephfs0_data"},
                        {"pool_name": "cephfs_erasure_cephfs0_metadata"},
                    ],
                    "cephfs",
                ]
            )

            created_resources.append(
                [
                    "/dev/rbd0",
                    "/mnt/rbd-thrash",
                    [{"pool_name": "rbd-ec-data"}, {"pool_name": "rbd-ec-metadata"}],
                    "rbd",
                ]
            )

        secret_key = None
        access_key = None
        rgw_bucket_name = "test-bucket"
        rgw_nodes = ceph_cluster.get_nodes(role="rgw")
        rgw_endpoint = rgw_nodes[0].ip_address

        if "setup" in scenarios_to_run:
            # Created Erasure and Replicated Ceph file system
            if config.get("include_erasure_pools", False):
                fs_name, mount_path, created_pools = (
                    rados_obj.create_cephfs_filesystem_mount(
                        client_node=client_node,
                        fs_name="cephfs0",
                        pool_type="erasure",
                    )
                )

            fs_name, mount_path, created_pools = (
                rados_obj.create_cephfs_filesystem_mount(
                    client_node=client_node,
                    fs_name="cephfs1",
                    pool_type="replicated",
                )
            )

            # Create EC RBD
            if config.get("include_erasure_pools", False):
                rbd_pool_name = "rbd-ec-data"
                rbd_ec_metadata = "rbd-ec-metadata"
                rbd_image = "rbd_image"
                mount_path, device_path, created_pools = rados_obj.create_ec_rbd_pools(
                    rbd_ec_data_pool=rbd_pool_name,
                    rbd_metadata_pool=rbd_ec_metadata,
                    image_name=rbd_image,
                    crush_failure_domain="osd",
                )

            # Create replicated RBD
            rbd_pool_name = "rbd-replicated-data"
            rbd_image = "rbd_image"
            mount_path, device_path, created_pools = (
                rados_obj.create_replicated_rbd_pools(
                    rbd_pool=rbd_pool_name,
                    image_name=rbd_image,
                    mount_path="/mnt/rbd_replicated_mount",
                )
            )

            # Replicated RGW setup
            rgw_data_pool = "default.rgw.buckets.data"
            rados_obj.create_pool(pool_name=rgw_data_pool, app_name="rgw")

            # Install aws cli
            cmd = "pip3 install awscli"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            # fetch rgw key and secret
            keys = get_or_create_user(client_node)
            access_key = keys["access_key"]
            secret_key = keys["secret_key"]

            cmd = "mkdir .aws"
            try:
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
            except Exception:
                log.error(".aws directory already exists.")

            cmd = """cat <<EOF > ~/.aws/config
[default]
region = us-east-1
EOF"""
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            cmd = f"""cat <<EOF > ~/.aws/credentials
[default]
aws_access_key_id = {access_key}
aws_secret_access_key = {secret_key}
EOF"""
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            cmd = (
                f"aws s3 mb s3://{rgw_bucket_name} --endpoint-url http://{rgw_endpoint}"
            )
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

        log.info(f"Performing validations on {test_pools}")
        comp_io_obj = CompressionIO(
            rados_obj=rados_obj,
            cephadm=cephadm,
            mon_obj=mon_obj,
            client_node=client_node,
            workload_type=None,
            rgw_endpoint=rgw_endpoint,
            rgw_key=secret_key,
            rgw_secret=access_key,
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
                        "compression_algorithm": compression_algorithm,
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
                        "compression_algorithm": compression_algorithm,
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
                    "compression_algorithm": compression_algorithm,
                }

                bluestore_compression.min_alloc_size_test(**kwargs)

        if "scenario-6" in scenarios_to_run:
            log.info(
                """
Test blob sizes by writing IO to a compressed pool and validating
that the blob sizes match the configured minimum blob size.
Steps:
1) Create pool
2) Set OSD config for bluestore_compression_min_blob_size
3) Enable compression on the pool
4) Create RBD image
5) Write IO to the pool
6) Validate blob sizes match the configured blob size
7) Cleanup OSD config and delete pool
            """
            )

            pool_suffix = "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )
            pool_name = f"rados-{pool_suffix}"
            compression_mode = COMPRESSION_MODES.FORCE
            blob_sizes = [8192, 65536, 131072]
            compression_obj = BluestoreDataCompression(
                rados_obj=rados_obj,
                cephadm=cephadm,
                mon_obj=mon_obj,
                client_node=client_node,
                ceph_cluster=ceph_cluster,
            )
            rbd_img: str = "rbd_img" + "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )
            object_size = 100000

            for blob_size in blob_sizes:
                log.info(
                    "Starting blob size test: %s, mode: %s, algo: %s"
                    % (blob_size, compression_mode, compression_algorithm)
                )

                # Step 1: Create pool
                compression_obj.create_pool_wrapper(pool_name=pool_name)
                acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
                primary_osd_id = acting_pg_set[0]

                compression_obj.set_unset_osd_config_and_redeploy(
                    osd_id=primary_osd_id,
                    param="bluestore_compression_min_blob_size",
                    value=blob_size,
                    set=True,
                    without_redeploy=True,
                )

                # Step 2: Enable compression
                compression_obj.enable_compression(
                    compression_mode=compression_mode,
                    compression_algorithm=compression_algorithm,
                    pool_level_compression=pool_level_compression,
                    pool_name=pool_name,
                )

                # Create RBD image
                cmd = "rbd create %s --size 10G --pool %s" % (rbd_img, pool_name)
                try:
                    rados_obj.client.exec_command(cmd=cmd, sudo=True)
                    log.info("Created RBD image: %s" % rbd_img)
                except Exception as e:
                    raise Exception("Failed to create RBD image: %s" % e)

                # Step 3: Write IO
                chk_log_msg = compression_obj.write_io_and_fetch_log_lines(
                    write_utility_type=IOTools.FIO,
                    primary_osd_id=primary_osd_id,
                    pool_name=pool_name,
                    object_size=int(object_size),
                    blob_size=int(object_size),
                    rbd_img=rbd_img,
                    patterns=["target_blob_size"],
                )

                # Step 4: Validate blob sizes
                log.info("Validating blob size: expected %s" % blob_size)
                max_compressed_blob_size = mon_obj.get_config(
                    section=f"osd.{primary_osd_id}", param="bluestore_max_blob_size_hdd"
                )
                for log_line in chk_log_msg.split("\n"):
                    if log_line == "":
                        continue
                    log.debug("Processing: %s" % log_line)
                    try:
                        # Extract the number after "target_blob_size " (e.g., "8192" from log line)
                        size_in_dec = log_line.split("target_blob_size ")[1].split()[0]
                    except (IndexError, ValueError) as e:
                        log.warning(
                            "Could not parse blob size from line: %s, error: %s"
                            % (log_line, e)
                        )
                        continue

                    if int(size_in_dec) != min(
                        int(blob_size), int(max_compressed_blob_size)
                    ):
                        err_msg = "Blob size mismatch: actual=%s, expected=%s" % (
                            size_in_dec,
                            blob_size,
                        )
                        log.error(err_msg)
                        raise AssertionError(err_msg)
                    log.info("Blob size validated: %s" % size_in_dec)
                log.info("Blob size validation passed for size %s" % blob_size)

                # Step 5: Cleanup
                compression_obj.set_unset_osd_config_and_redeploy(
                    osd_id=primary_osd_id,
                    param="bluestore_compression_min_blob_size",
                    value=blob_size,
                    set=False,
                    without_redeploy=True,
                )

                if not rados_obj.delete_pool(pool=pool_name):
                    raise Exception("Failed to delete pool %s" % pool_name)

        if "scenario-7" in scenarios_to_run:
            log.info(
                """
Test target_blob_size validation based on object allocation hints.
If allocation hint is sequential read + append only OR sequential read + immutable,
target_blob_size should be 65536, else 8192.
Steps:
1) Create pool
2) Enable compression on the pool
3) Create RBD image
4) Write IO with allocation hint
5) Validate target_blob_size matches expected blob size based on allocation hint
6) Cleanup pool
            """
            )
            compression_mode = COMPRESSION_MODES.FORCE
            compression_obj = BluestoreDataCompression(
                rados_obj=rados_obj,
                cephadm=cephadm,
                mon_obj=mon_obj,
                client_node=client_node,
                ceph_cluster=ceph_cluster,
            )
            object_size = 100000

            alloc_hints_to_test = [
                {
                    "hint": BLUESTORE_ALLOC_HINTS.SEQUENTIAL_READ
                    | BLUESTORE_ALLOC_HINTS.APPEND_ONLY,
                    "expected_blob_size": 65536,
                    "description": "sequential read and append only",
                },
                {
                    "hint": BLUESTORE_ALLOC_HINTS.SEQUENTIAL_READ
                    | BLUESTORE_ALLOC_HINTS.IMMUTABLE,
                    "expected_blob_size": 65536,
                    "description": "sequential read and immutable",
                },
                {
                    "hint": BLUESTORE_ALLOC_HINTS.NOHINT,
                    "expected_blob_size": 8192,
                    "description": "no hint",
                },
                {
                    "hint": BLUESTORE_ALLOC_HINTS.COMPRESSIBLE,
                    "expected_blob_size": 8192,
                    "description": "compressible",
                },
            ]

            for hint_config in alloc_hints_to_test:
                alloc_hint = hint_config["hint"]
                expected_blob_size = hint_config["expected_blob_size"]
                description = hint_config["description"]

                # Create unique pool name for each hint test
                hint_pool_suffix = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )
                hint_pool_name = f"rados-{hint_pool_suffix}"
                hint_rbd_img = "rbd_img" + "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )

                log.info(
                    "Starting allocation hint test: %s (hint=%s), expected blob size: %s, mode: %s, algo: %s"
                    % (
                        description,
                        alloc_hint,
                        expected_blob_size,
                        compression_mode,
                        compression_algorithm,
                    )
                )

                # Step 1: Create pool
                compression_obj.create_pool_wrapper(pool_name=hint_pool_name)
                acting_pg_set = rados_obj.get_pg_acting_set(pool_name=hint_pool_name)
                primary_osd_id = acting_pg_set[0]

                # Step 2: Enable compression
                compression_obj.enable_compression(
                    compression_mode=compression_mode,
                    compression_algorithm=compression_algorithm,
                    pool_level_compression=pool_level_compression,
                    pool_name=hint_pool_name,
                )

                # Create RBD image
                cmd = "rbd create %s --size 10G --pool %s" % (
                    hint_rbd_img,
                    hint_pool_name,
                )
                try:
                    rados_obj.client.exec_command(cmd=cmd, sudo=True)
                    log.info("Created RBD image: %s" % hint_rbd_img)
                except Exception as e:
                    raise Exception("Failed to create RBD image: %s" % e)

                # Step 3: Write IO with allocation hint
                chk_log_msg = compression_obj.write_io_and_fetch_log_lines(
                    write_utility_type=IOTools.LIBRADOS,
                    primary_osd_id=primary_osd_id,
                    pool_name=hint_pool_name,
                    object_size=int(object_size),
                    blob_size=int(object_size),
                    rbd_img=hint_rbd_img,
                    patterns=["target_blob_size"],
                    alloc_hint=alloc_hint,
                )

                # Step 4: Validate target_blob_size
                blob_sizes_found = []
                for log_line in chk_log_msg.split("\n"):
                    if log_line == "":
                        continue
                    log.debug("Processing: %s" % log_line)
                    try:
                        # Extract the number after "target_blob_size " (e.g., "8192" from log line)
                        size_in_dec = log_line.split("target_blob_size ")[1].split()[0]
                        blob_sizes_found.append(size_in_dec)
                        log.info("Found blob size: %s" % size_in_dec)
                    except (IndexError, ValueError) as e:
                        log.warning(
                            "Could not parse blob size from line: %s, error: %s"
                            % (log_line, e)
                        )
                        continue

                if not blob_sizes_found:
                    err_msg = "No target_blob_size found in logs for hint %s (%s)" % (
                        alloc_hint,
                        description,
                    )
                    log.error(err_msg)
                    raise AssertionError(err_msg)

                # Validate that all found blob sizes match expected
                for size_in_dec in blob_sizes_found:
                    if int(size_in_dec) != int(expected_blob_size):
                        err_msg = (
                            "Blob size mismatch for int %s (%s): actual=%s, expected=%s"
                            % (alloc_hint, description, size_in_dec, expected_blob_size)
                        )
                        log.error(err_msg)
                        raise AssertionError(err_msg)

                log.info(
                    "Target blob size validation passed for hint %s (%s): %s"
                    % (alloc_hint, description, expected_blob_size)
                )

                # Step 5: Cleanup pool
                if not rados_obj.delete_pool(pool=hint_pool_name):
                    raise Exception("Failed to delete pool %s" % hint_pool_name)

        if "scenario-8" in scenarios_to_run:
            log.info("STARTED: Scenario 8: Disable bluestore_write_v2 and validate")
            bluestore_compression.toggle_bluestore_write_v2(toggle_value="false")
            log.info("COMPELTED: Scenario 8: Disable bluestore_write_v2 and validate")

        if "scenario-9" in scenarios_to_run:
            log.info(
                """
Test compression validation when primary OSD is down.
Steps:
1) Create Pool
2) Enable compression
3) Shutdown primary osd
4) Write IO to the pools
5) Read the collections from the secondary and tertiary OSD and check if they are compressed
6) Restart the primary OSD
7) Check all the objects in primary OSD
8) They should be compressed
9) Clean up the pool
            """
            )
            # initializations
            compression_mode = COMPRESSION_MODES.FORCE
            compression_obj = BluestoreDataCompression(
                rados_obj=rados_obj,
                cephadm=cephadm,
                mon_obj=mon_obj,
                client_node=client_node,
                ceph_cluster=ceph_cluster,
            )
            object_size = 100000
            objectstore_obj = objectstoreToolWorkflows(node=cephadm)

            # Create unique pool name and rbd image name
            pool_suffix = "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )
            pool_name = f"rados-{pool_suffix}"
            rbd_img = "rbd_img" + "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )

            log.info(
                "Starting scenario-9: Compression validation with primary OSD down, mode: %s, algo: %s"
                % (compression_mode, compression_algorithm)
            )

            # Step 1: Create pool
            log.info("Step 1: Creating pool %s" % pool_name)
            compression_obj.create_pool_wrapper(pool_name=pool_name)
            acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
            primary_osd_id = acting_pg_set[0]
            secondary_osd_id = acting_pg_set[1] if len(acting_pg_set) > 1 else None
            tertiary_osd_id = acting_pg_set[2] if len(acting_pg_set) > 2 else None

            log.info(
                "PG acting set: primary=%s, secondary=%s, tertiary=%s"
                % (primary_osd_id, secondary_osd_id, tertiary_osd_id)
            )
            if secondary_osd_id is None or tertiary_osd_id is None:
                raise Exception(
                    "No secondary or tertiary OSD found in acting set. "
                    "Test requires at least 2 OSDs in acting set."
                )

            # Step 2: Enable compression
            log.info("Step 2: Enabling compression on pool %s" % pool_name)
            compression_obj.enable_compression(
                compression_mode=compression_mode,
                compression_algorithm=compression_algorithm,
                pool_level_compression=pool_level_compression,
                pool_name=pool_name,
            )

            # Create RBD image
            cmd = "rbd create %s --size 10G --pool %s" % (rbd_img, pool_name)
            try:
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
                log.info("Created RBD image: %s" % rbd_img)
            except Exception as e:
                raise Exception("Failed to create RBD image: %s" % e)

            # Step 3: Shutdown primary OSD
            log.info("Step 3: Shutting down primary OSD %s" % primary_osd_id)
            if not rados_obj.change_osd_state(action="stop", target=primary_osd_id):
                raise Exception("Failed to stop primary OSD %s" % primary_osd_id)

            # Wait for PGs to reach active + clean
            log.info("Waiting for cluster to stabilize after OSD shutdown")
            wait_for_clean_pg_sets(rados_obj=rados_obj, timeout=12000)

            # Step 4: Write IO to the pool
            log.info("Step 4: Writing IO to pool %s" % pool_name)
            compression_obj.write_io_and_fetch_log_lines(
                write_utility_type=IOTools.FIO,
                primary_osd_id=str(secondary_osd_id),
                pool_name=pool_name,
                object_size=int(object_size),
                blob_size=int(object_size),
                rbd_img=rbd_img,
                patterns=[],
            )
            time.sleep(10)

            # Step 5: Read blobs from secondary OSD and check if they are compressed.
            log.info("Step 5: Checking compression on secondary and tertiary OSDs")
            blobs_secondary = compression_obj.validate_blobs_are_compressed(
                osd_id=secondary_osd_id,
                objectstore_obj=objectstore_obj,
                pool_name=pool_name,
            )
            if blobs_secondary is False:
                raise Exception("Blobs on secondary OSD are not compressed")

            # Read blobs from tertiary OSD and check if they are compressed.
            blobs_tertiary = compression_obj.validate_blobs_are_compressed(
                osd_id=tertiary_osd_id,
                objectstore_obj=objectstore_obj,
                pool_name=pool_name,
            )
            if blobs_tertiary is False:
                raise Exception("Blobs on tertiary OSD are not compressed")

            # Step 6: Restart the primary OSD
            log.info("Step 6: Restarting primary OSD %s" % primary_osd_id)
            if not rados_obj.change_osd_state(action="start", target=primary_osd_id):
                raise Exception("Failed to start primary OSD %s" % primary_osd_id)

            # Wait for cluster to stabilize and recovery
            log.info("Waiting for cluster to stabilize and recovery after OSD restart")
            wait_for_clean_pg_sets(rados_obj=rados_obj, timeout=12000)

            acting_pg_set_post_osd_restart = rados_obj.get_pg_acting_set(
                pool_name=pool_name
            )
            acting_pg_set_pre_osd_restart = acting_pg_set

            if (
                acting_pg_set_post_osd_restart.sort()
                != acting_pg_set_pre_osd_restart.sort()
            ):
                raise Exception("PG acting set changed after OSD restart")

            # Step 7: Check the blobs of the object in primary osd are compressed
            log.info("Step 7: Checking objects in primary OSD %s" % primary_osd_id)
            is_primary_compressed = compression_obj.validate_blobs_are_compressed(
                osd_id=primary_osd_id,
                objectstore_obj=objectstore_obj,
                pool_name=pool_name,
            )
            if not is_primary_compressed:
                raise Exception(
                    "Blobs on primary OSD which was down during IO are not compressed"
                )

            # Step 8: Clean up the pool
            log.info("Step 8: Cleaning up pool %s" % pool_name)
            if not rados_obj.delete_pool(pool=pool_name):
                raise Exception("Failed to delete pool %s" % pool_name)

            log.info("Scenario-9 completed successfully")

        if "scenario-10" in scenarios_to_run:
            log.info(
                """
Test compression validation when primary OSD host is down.
Steps:
1) Create pool
2) Fetch primary OSD, secondary, tertiary OSD
3) Shutdown OSD host of primary OSD
4) Enable compression
5) Write IO to the pool
6) Check the secondary_osd and tertiary_osd for compression
7) Restart primary OSD host
8) Validate that the object written to pool is stored compressed in primary OSD
9) Cleanup
            """
            )
            # Initializations
            compression_mode = COMPRESSION_MODES.FORCE
            compression_obj = BluestoreDataCompression(
                rados_obj=rados_obj,
                cephadm=cephadm,
                mon_obj=mon_obj,
                client_node=client_node,
                ceph_cluster=ceph_cluster,
            )
            object_size = 100000
            objectstore_obj = objectstoreToolWorkflows(node=cephadm)

            # Create unique pool name and rbd image name
            pool_suffix = "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )
            pool_name = f"rados-{pool_suffix}"
            rbd_img = "rbd_img" + "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )

            log.info(
                "Starting scenario-10: Compression validation with primary OSD host down, mode: %s, algo: %s"
                % (compression_mode, compression_algorithm)
            )

            # Step 1: Create pool
            compression_obj.create_pool_wrapper(pool_name=pool_name)
            acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
            primary_osd_id = acting_pg_set[0]
            secondary_osd_id = acting_pg_set[1] if len(acting_pg_set) > 1 else None
            tertiary_osd_id = acting_pg_set[2] if len(acting_pg_set) > 2 else None

            if secondary_osd_id is None or tertiary_osd_id is None:
                raise Exception(
                    "No secondary or tertiary OSD found in acting set. "
                    "Test requires at least 3 OSDs in acting set."
                )

            # Step 2: Get primary OSD host
            primary_osd_host = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=str(primary_osd_id)
            )
            if primary_osd_host is None:
                raise Exception(
                    "Failed to fetch host node for primary OSD %s" % primary_osd_id
                )
            primary_osd_hostname = getattr(primary_osd_host, "hostname", None)
            if primary_osd_hostname is None:
                raise Exception(
                    "Host node for primary OSD %s does not have hostname attribute"
                    % primary_osd_id
                )
            log.info(
                "Primary OSD %s is on host %s" % (primary_osd_id, primary_osd_hostname)
            )

            # Step 3: Shutdown OSD host of primary OSD
            log.info(
                "Step 3: Shutting down OSD host %s (primary OSD %s)"
                % (primary_osd_hostname, primary_osd_id)
            )
            target_node = find_vm_node_by_hostname(ceph_cluster, primary_osd_hostname)
            if target_node is None:
                raise Exception(
                    "Failed to find VM node for hostname %s" % primary_osd_hostname
                )
            log.info("Shutting down host %s" % primary_osd_hostname)
            target_node.shutdown(wait=True)
            log.info("Host %s shutdown successfully" % primary_osd_hostname)

            # Step 4: Enable compression
            log.info("Step 4: Enabling compression on pool %s" % pool_name)
            compression_obj.enable_compression(
                compression_mode=compression_mode,
                compression_algorithm=compression_algorithm,
                pool_level_compression=pool_level_compression,
                pool_name=pool_name,
            )

            # Create RBD image
            cmd = "rbd create %s --size 10G --pool %s" % (rbd_img, pool_name)
            try:
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
                log.info("Created RBD image: %s" % rbd_img)
            except Exception as e:
                raise Exception("Failed to create RBD image: %s" % e)

            # Step 5: Write IO to the pool
            log.info("Step 5: Writing IO to pool %s" % pool_name)
            compression_obj.write_io_and_fetch_log_lines(
                write_utility_type=IOTools.FIO,
                primary_osd_id=str(secondary_osd_id),
                pool_name=pool_name,
                object_size=int(object_size),
                blob_size=int(object_size),
                rbd_img=rbd_img,
                patterns=[],
            )
            time.sleep(10)

            # Step 6: Check the secondary_osd and tertiary_osd for compression
            log.info("Checking compression on secondary and tertiary OSDs...")
            blobs_secondary = compression_obj.validate_blobs_are_compressed(
                osd_id=secondary_osd_id,
                objectstore_obj=objectstore_obj,
                pool_name=pool_name,
            )
            if blobs_secondary is False:
                raise Exception("Blobs on secondary OSD are not compressed")

            blobs_tertiary = compression_obj.validate_blobs_are_compressed(
                osd_id=tertiary_osd_id,
                objectstore_obj=objectstore_obj,
                pool_name=pool_name,
            )
            if blobs_tertiary is False:
                raise Exception("Blobs on tertiary OSD are not compressed")

            # Step 7: Restart primary OSD host
            log.info("Step 7: Restarting primary OSD host %s" % primary_osd_hostname)
            target_node.power_on()
            log.info("Host %s powered on successfully" % primary_osd_hostname)

            # Wait for PGs to reach active + clean
            log.info("Waiting for cluster to stabilize and recovery after host restart")
            wait_for_clean_pg_sets(rados_obj=rados_obj)

            acting_pg_set_post_osd_restart = rados_obj.get_pg_acting_set(
                pool_name=pool_name
            )
            acting_pg_set_pre_osd_restart = acting_pg_set

            if (
                acting_pg_set_post_osd_restart.sort()
                != acting_pg_set_pre_osd_restart.sort()
            ):
                raise Exception("PG acting set changed after OSD restart")

            # Step 8: Validate that the object written to pool is stored compressed in primary OSD
            log.info(
                "Step 8: Checking objects in primary OSD %s for compression"
                % primary_osd_id
            )
            is_primary_compressed = compression_obj.validate_blobs_are_compressed(
                osd_id=primary_osd_id,
                objectstore_obj=objectstore_obj,
                pool_name=pool_name,
            )
            if not is_primary_compressed:
                raise Exception(
                    "Blobs on primary OSD which was down during IO are not compressed"
                )

            # Step 9: Cleanup
            log.info("Step 9: Cleaning up pool %s" % pool_name)
            if not rados_obj.delete_pool(pool=pool_name):
                raise Exception("Failed to delete pool %s" % pool_name)

            log.info("Scenario-10 completed successfully")

        if "scenario-11" in scenarios_to_run:
            log.info(
                """
Test compression validation when primary OSD host is restarted after IO.
Steps:
1) Create pool, store primary, secondary and tertiary OSD IDs
2) Enable compression
3) Write IO to the pool
4) Validate primary, secondary, tertiary OSDs are compressed
5) Restart Primary OSD host
6) Post restart validate primary OSD is still compressed
7) Delete pool
            """
            )
            compression_mode = COMPRESSION_MODES.FORCE
            compression_obj = BluestoreDataCompression(
                rados_obj=rados_obj,
                cephadm=cephadm,
                mon_obj=mon_obj,
                client_node=client_node,
                ceph_cluster=ceph_cluster,
            )
            object_size = 100000
            objectstore_obj = objectstoreToolWorkflows(node=cephadm)

            # Create unique pool name
            pool_suffix = "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )
            pool_name = f"rados-{pool_suffix}"
            rbd_img = "rbd_img" + "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )

            log.info(
                "Starting scenario-11: Compression validation with primary OSD host "
                "restart after IO, mode: %s, algo: %s"
                % (compression_mode, compression_algorithm)
            )

            # Step 1: Create pool
            compression_obj.create_pool_wrapper(pool_name=pool_name)
            acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
            primary_osd_id = acting_pg_set[0]
            secondary_osd_id = acting_pg_set[1] if len(acting_pg_set) > 1 else None
            tertiary_osd_id = acting_pg_set[2] if len(acting_pg_set) > 2 else None

            log.info(
                "PG acting set: primary=%s, secondary=%s, tertiary=%s"
                % (primary_osd_id, secondary_osd_id, tertiary_osd_id)
            )

            if secondary_osd_id is None or tertiary_osd_id is None:
                raise Exception(
                    "No secondary or tertiary OSD found in acting set. "
                    "Test requires at least 3 OSDs in acting set."
                )

            # Get primary OSD host
            primary_osd_host = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=str(primary_osd_id)
            )
            if primary_osd_host is None:
                raise Exception(
                    "Failed to fetch host node for primary OSD %s" % primary_osd_id
                )
            primary_osd_hostname = getattr(primary_osd_host, "hostname", None)
            if primary_osd_hostname is None:
                raise Exception(
                    "Host node for primary OSD %s does not have hostname attribute"
                    % primary_osd_id
                )
            log.info(
                "Primary OSD %s is on host %s" % (primary_osd_id, primary_osd_hostname)
            )

            # Step 2: Enable compression
            log.info("Step 2: Enabling compression on pool %s" % pool_name)
            compression_obj.enable_compression(
                compression_mode=compression_mode,
                compression_algorithm=compression_algorithm,
                pool_level_compression=pool_level_compression,
                pool_name=pool_name,
            )

            # Create RBD image
            cmd = "rbd create %s --size 10G --pool %s" % (rbd_img, pool_name)
            try:
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
                log.info("Created RBD image: %s" % rbd_img)
            except Exception as e:
                raise Exception("Failed to create RBD image: %s" % e)

            # Step 3: Write IO to the pool
            log.info("Step 3: Writing IO to pool %s" % pool_name)
            compression_obj.write_io_and_fetch_log_lines(
                write_utility_type=IOTools.FIO,
                primary_osd_id=str(primary_osd_id),
                pool_name=pool_name,
                object_size=int(object_size),
                blob_size=int(object_size),
                rbd_img=rbd_img,
                patterns=[],
            )
            time.sleep(10)

            # Step 4: Validate primary, secondary, tertiary is compressed
            log.info(
                "Step 4: Validating compression on primary, secondary, and tertiary OSDs..."
            )

            blobs_primary = compression_obj.validate_blobs_are_compressed(
                osd_id=primary_osd_id,
                objectstore_obj=objectstore_obj,
                pool_name=pool_name,
            )
            if blobs_primary is False:
                raise Exception("Blobs on primary OSD are not compressed")

            blobs_secondary = compression_obj.validate_blobs_are_compressed(
                osd_id=secondary_osd_id,
                objectstore_obj=objectstore_obj,
                pool_name=pool_name,
            )
            if blobs_secondary is False:
                raise Exception("Blobs on secondary OSD are not compressed")

            blobs_tertiary = compression_obj.validate_blobs_are_compressed(
                osd_id=tertiary_osd_id,
                objectstore_obj=objectstore_obj,
                pool_name=pool_name,
            )
            if blobs_tertiary is False:
                raise Exception("Blobs on tertiary OSD are not compressed")

            log.info(
                "All OSDs (primary, secondary, tertiary) are compressed successfully"
            )

            # Step 5: Restart Primary OSD host
            log.info("Step 5: Restarting primary OSD host %s" % primary_osd_hostname)
            target_node = find_vm_node_by_hostname(ceph_cluster, primary_osd_hostname)
            if target_node is None:
                raise Exception(
                    "Failed to find VM node for hostname %s" % primary_osd_hostname
                )

            # stop the host and wait for PGs to reach active + clean
            target_node.shutdown(wait=True)
            log.info("Host %s shutdown successfully" % primary_osd_hostname)

            # start the host and wait for PGs to reach active + clean
            target_node.power_on()
            log.info("Host %s powered on successfully" % primary_osd_hostname)

            log.info("Waiting for cluster to stabilize and recovery after host restart")
            wait_for_clean_pg_sets(rados_obj=rados_obj)

            # Ensure the PG acting set is the same as before the restart
            acting_pg_set_post_osd_restart = rados_obj.get_pg_acting_set(
                pool_name=pool_name
            )
            acting_pg_set_pre_osd_restart = acting_pg_set

            if (
                acting_pg_set_post_osd_restart.sort()
                != acting_pg_set_pre_osd_restart.sort()
            ):
                raise Exception("PG acting set changed after OSD restart")

            # Step 6: Post restart validate primary OSD is still compressed
            log.info(
                "Step 6: Validating primary OSD %s is still compressed after host restart"
                % primary_osd_id
            )
            is_primary_compressed = compression_obj.validate_blobs_are_compressed(
                osd_id=primary_osd_id,
                objectstore_obj=objectstore_obj,
                pool_name=pool_name,
            )
            if not is_primary_compressed:
                raise Exception(
                    "Blobs on primary OSD are not compressed after host restart"
                )

            log.info("Primary OSD is still compressed after host restart")

            # Step 7: Delete pool
            log.info("Step 7: Deleting pool %s" % pool_name)
            if not rados_obj.delete_pool(pool=pool_name):
                raise Exception("Failed to delete pool %s" % pool_name)

            log.info("Scenario-11 completed successfully")

        if "scenario-12" in scenarios_to_run:
            """
            Test data integrity by writing data, retrieving data, and verifying checksum.
            Steps:
            1) Create pool
            2) Enable compression
            3) Write data to the pool and store md5 checksum using fio
            4) Read data from the pool and verify md5 checksum using fio
                If checksum does not match, exit code of FIO command will be 1
            5) Cleanup pool
            """
            compression_mode = COMPRESSION_MODES.FORCE
            compression_algorithm = COMPRESSION_ALGORITHMS.snappy
            compression_obj = BluestoreDataCompression(
                rados_obj=rados_obj,
                cephadm=cephadm,
                mon_obj=mon_obj,
                client_node=client_node,
                ceph_cluster=ceph_cluster,
            )
            rbd_img = "rbd_img" + "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )
            object_size = "16MB"

            # Create unique pool name
            pool_suffix = "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )
            pool_name = f"rados-{pool_suffix}"

            log.info(
                "Starting scenario-12: Write data, retrieve data, and verify checksum, "
                "mode: %s, algo: %s" % (compression_mode, compression_algorithm)
            )

            # Step 1: Create pool
            log.info("Step 1: Creating pool %s" % pool_name)
            compression_obj.create_pool_wrapper(pool_name=pool_name)

            # Step 2: Enable compression
            log.info("Step 2: Enabling compression on pool %s" % pool_name)
            compression_obj.enable_compression(
                compression_mode=compression_mode,
                compression_algorithm=compression_algorithm,
                pool_level_compression=pool_level_compression,
                pool_name=pool_name,
            )

            # Create RBD image
            cmd = "rbd create %s --size 10G --pool %s" % (rbd_img, pool_name)
            try:
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
                log.info("Created RBD image: %s" % rbd_img)
            except Exception as e:
                raise Exception("Failed to create RBD image: %s" % e)

            log.info("Step 3:  Write data to the pool and store md5 checksum using fio")
            cmd_write: str = f"""fio --ioengine=rbd --direct=1 --name=test-verify --iodepth=1 \
    --bs=4M --rw=write --numjobs=1 --size={object_size} --pool={pool_name} --rbdname={rbd_img} \
    --zero_buffers=0 --refill_buffers=1 --buffer_compress_percentage=70 --verify=md5 --do_verify=0 \
    --verify_dump=1
        """
            _, _, exit_code, _ = compression_obj.client_node.exec_command(
                cmd=cmd_write, sudo=True, pretty_print=True, verbose=True
            )
            if exit_code != 0:
                raise Exception("Failed to write data to the pool")

            log.info(
                "Step 4:  read data from the pool and verify md5 checksum are identical using fio"
            )
            cmd_read: str = f"""fio --ioengine=rbd --direct=1 --name=test-verify --iodepth=1 \
    --bs=4M --rw=read --numjobs=1 --size={object_size} --pool={pool_name} --rbdname={rbd_img} \
    --zero_buffers=0 --refill_buffers=1 --buffer_compress_percentage=70 --verify=md5 --do_verify=1 \
    --verify_fatal=1 --verify_state_load=1
        """
            _, _, exit_code, _ = compression_obj.client_node.exec_command(
                cmd=cmd_read, sudo=True, pretty_print=True, verbose=True
            )
            if exit_code != 0:
                raise Exception(
                    "Checksum of data written to compressed pool and read from compressed pool does not match"
                )

            # Cleanup pool
            log.info("Step 5: Cleaning up pool %s" % pool_name)
            if not rados_obj.delete_pool(pool=pool_name):
                raise Exception("Failed to delete pool %s" % pool_name)

            log.info("Scenario-12 completed successfully")

        if "scenario-13" in scenarios_to_run:
            log.info(
                """
Test compression algorithms across different pools and workload types (cephfs, rbd, rgw).
Steps:
1) Iterate through test pools (cephfs, rbd, rgw)
2) For each pool, test different compression algorithms (snappy, zlib, zstd, lz4)
3) Enable compression on the pool with specific algorithm
4) Write IO to the pool with compressible data
5) Map objects to their primary OSDs
6) Stop OSDs and fetch blob information from objectstore
7) Validate compressed_length < compression_percentage * logical_length
8) Start OSDs and cleanup
            """
            )
            compression_mode = COMPRESSION_MODES.FORCE
            objectstore_obj.nostop = True
            objectstore_obj.nostart = True
            log.info(
                "Set objectstore_obj.nostop=True and nostart=True to prevent OSD restarts during blob inspection"
            )

            log.info(f"Checking pools: {test_pools}")
            log.info(f"Total pools to test: {len(test_pools)}")
            if len(test_pools) == 0:
                log.error("CephFS, RBD and RGW setup has failed")
                return 1

            for pool_name, mount_point, workload_type, pool_type in test_pools:
                log.info(
                    f"\n{'='*80}\n"
                    f"Starting Scenario-13 tests for pool: {pool_name}\n"
                    f"Mount point: {mount_point}\n"
                    f"Workload type: {workload_type}\n"
                    f"Pool type: {pool_type}\n"
                    f"{'='*80}"
                )
                for compression_percentage in ["70"]:
                    log.info(
                        f"Testing with compression percentage: {compression_percentage}%"
                    )
                    for algorithm in [
                        "snappy"
                    ]:  # removed zlib, zstd, lz4 for time being,
                        # Since suite execution time for all algorithms is greater than 8 hours.
                        log.info(
                            f"\n******* testing compression algorithm **********\n"
                            f"algorithm -> {algorithm}\n"
                            f"pool -> {pool_name}\n"
                            f"mount point -> {mount_point}\n"
                            f"workload type -> {workload_type}\n"
                            f"compress percentage -> {compression_percentage}\n"
                            f"pool type -> {pool_type}\n"
                            f"******* ***************************** **********"
                        )

                        log.info(
                            f"Enabling compression on pool {pool_name} with algorithm {algorithm}"
                        )
                        bluestore_compression.enable_compression(
                            compression_mode=compression_mode,
                            compression_algorithm=algorithm,
                            pool_level_compression=pool_level_compression,
                            pool_name=pool_name,
                        )
                        log.info(
                            f"Successfully enabled compression on pool {pool_name}"
                        )
                        time.sleep(30)

                        comp_io_obj.workload_type = workload_type
                        rbd_cephfs_folder_full_path = mount_point
                        file_path = "/tmp/testfile_%s_%s_%s" % (
                            pool_name,
                            algorithm,
                            "".join(
                                random.choices(
                                    string.ascii_letters + string.digits, k=6
                                )
                            ),
                        )
                        log.info(f"Writing IO to file: {file_path}")
                        log.info(
                            f"File size: 10m, Compression percentage: {compression_percentage}%"
                        )
                        comp_io_obj.write(
                            file_size="10m",
                            compression_percentage=compression_percentage,
                            file_full_path=file_path,
                            offset="0",
                            rbd_cephfs_folder_full_path=rbd_cephfs_folder_full_path,
                            rgw_bucket_name=rgw_bucket_name,
                        )
                        log.info(f"Completed writing IO to pool {pool_name}")

                        log.info(f"Listing objects in pool {pool_name}")
                        cmd = f"rados ls -p {pool_name}"
                        out = rados_obj.client.exec_command(cmd=cmd, sudo=True)
                        log.info(
                            f"Found objects in pool {pool_name}: {out[0] if out[0] else 'None'}"
                        )

                        osd_to_object_map = {}
                        # {23: [obj1, obj2] }
                        log.info("Mapping objects to their primary OSDs")
                        for obj_name in out[0].split():
                            log.debug(f"Processing object: {obj_name}")
                            osd_map = rados_obj.get_osd_map(
                                pool=pool_name, obj=obj_name
                            )
                            acting_pg_set = osd_map["acting"]
                            primary_osd_id = acting_pg_set[0]
                            log.debug(
                                f"Object {obj_name} is on primary OSD {primary_osd_id}"
                            )
                            osd_to_object_map[primary_osd_id] = osd_to_object_map.get(
                                primary_osd_id, []
                            )
                            osd_to_object_map[primary_osd_id].append(obj_name)
                        log.info(
                            f"OSD to object mapping completed. Total OSDs with objects: {len(osd_to_object_map)}"
                        )

                        obj_name_to_blobs_map = {}
                        log.info("Fetching blob information from OSDs")
                        for primary_osd_id, obj_names_list in osd_to_object_map.items():
                            log.info(
                                f"Processing OSD ID {primary_osd_id} with {len(obj_names_list)}"
                                f" objects: {obj_names_list}"
                            )
                            log.info(
                                f"Stopping OSD {primary_osd_id} to fetch blob information"
                            )
                            rados_obj.change_osd_state(
                                action="stop", target=primary_osd_id
                            )
                            log.info(f"OSD {primary_osd_id} stopped successfully")

                            for obj_name in obj_names_list:
                                log.info(
                                    f"Processing object {obj_name} in OSD {primary_osd_id}"
                                )
                                log.debug(
                                    f"Listing object {obj_name} in OSD {primary_osd_id}"
                                )
                                obj_json = objectstore_obj.list_objects(
                                    osd_id=int(primary_osd_id), obj_name=obj_name
                                ).strip()
                                if not obj_json:
                                    log.error(
                                        f"Object {obj_name} could not be listed in OSD {primary_osd_id}"
                                    )
                                    raise

                                log.debug(
                                    f"Fetching object dump for {obj_name} from OSD {primary_osd_id}"
                                )
                                blobs_json = objectstore_obj.fetch_object_dump(
                                    osd_id=int(primary_osd_id), obj=obj_json
                                )
                                blobs_json = json.loads(blobs_json)
                                obj_name_to_blobs_map[obj_name] = blobs_json
                                log.info(
                                    f"Successfully fetched blob information for object {obj_name}"
                                )

                            log.info(
                                f"Starting OSD {primary_osd_id} after blob inspection"
                            )
                            rados_obj.change_osd_state(
                                action="start", target=primary_osd_id
                            )
                            log.info(f"OSD {primary_osd_id} started successfully")

                        log.info(
                            "Proceeding to validate compressed length < compress percent * blob size/logical length"
                        )
                        log.info(
                            f"Total objects to validate: {len(obj_name_to_blobs_map)}"
                        )
                        for obj_name, blobs_json in obj_name_to_blobs_map.items():
                            log.info(f"Validating blobs for object: {obj_name}")
                            extents = blobs_json["onode"]["extents"][:-1]
                            log.info(f"Number of extents to validate: {len(extents)}")
                            for idx, blob_detail in enumerate(extents):
                                blob = blob_detail["blob"]
                                logical_length = blob["logical_length"]
                                compressed_length = blob["compressed_length"]
                                log.debug(
                                    f"Blob {idx}: logical_length={logical_length},"
                                    f" compressed_length={compressed_length}"
                                )

                                # Adding 5% buffer. It's observed that when fio writes data and we
                                # pass 70% compression , the resulting compression would be 65%
                                # There would be around ~ 70% compressible data.
                                # 100 KB -> 35KB
                                int_compression_percentage = (
                                    (100 - int(compression_percentage)) + 5
                                ) / 100  # 0.35
                                expected_max_compressed = (
                                    logical_length * int_compression_percentage
                                )
                                msg = (
                                    f"\nObject: {obj_name}, Blob index: {idx}\n"
                                    f"compressed_length -> {compressed_length}\n"
                                    f"logical_length -> {logical_length}\n"
                                    f"Compression percentage: {compression_percentage}%\n"
                                    f"Expected max compressed (with 5% buffer): {expected_max_compressed}\n"
                                    f"Pass condition: compressed_length"
                                    f" < {int_compression_percentage} * {logical_length}\n"
                                    f"Actual check: {compressed_length} < {expected_max_compressed}"
                                )

                                # Bug https://bugzilla.redhat.com/show_bug.cgi?id=2427146
                                # Blobs with more than one extents are not compressed despite compression being enabled
                                if (
                                    len(blob["extents"]) > 1
                                ) and compressed_length == 0:
                                    msg += (
                                        "When extents > 1, compression does not occur"
                                    )
                                    msg += "\n***PASS***"
                                elif (
                                    not compressed_length
                                    < logical_length * int_compression_percentage
                                ):
                                    msg += "\n***FAIL****"
                                    log.error(msg)
                                    raise
                                else:
                                    msg += "\n***PASS****"
                                    log.info(msg)
                            log.info(f"Completed validation for object {obj_name}")
                        log.info(
                            f"Validation summary for algorithm {algorithm}, pool {pool_name}: "
                        )

                        log.info(
                            f"Starting cleanup for pool {pool_name}, algorithm {algorithm}"
                        )
                        comp_io_obj.clean(
                            rbd_cephfs_folder_full_path=rbd_cephfs_folder_full_path,
                            rgw_bucket_name=rgw_bucket_name,
                            pool_name=pool_name,
                            compression_obj=bluestore_compression,
                        )

                        log.info(
                            f"Completed clean up for pool {pool_name}, algorithm {algorithm}"
                        )

            objectstore_obj.nostop = None
            objectstore_obj.nostart = None
            log.info("Reset objectstore_obj.nostop and nostart flags")

            log.info("******* Scenario 13 passed **********")

        if "scenario-14" in scenarios_to_run:
            log.info(
                """
Test compression modes (none, force, passive, aggressive) across different pools (cephfs, rbd, rgw).
Steps:
1) Iterate through test pools (cephfs, rbd, rgw)
2) For each pool, test different compression modes (none, force, passive, aggressive)
3) Enable compression on the pool with specific mode
4) Write IO to the pool with compressible data
5) Fetch pool statistics
6) Validate compression stats based on mode:
   - force/aggressive: compression should occur
   - passive/none: compression should not occur (unless hints are passed)
7) Cleanup
            """
            )
            compression_percentage = "70"
            algorithm = "snappy"
            log.info(
                f"Test configuration: compression_percentage={compression_percentage}%, algorithm={algorithm}"
            )
            if len(test_pools) == 0:
                log.error("CephFS, RBD and RGW setup has failed")
                return 1

            for mode in ["none", "passive", "aggressive", "force"]:
                for pool_name, mount_point, workload_type, pool_type in test_pools:
                    log.info(
                        f"\n{'='*80}\n"
                        f"Starting Scenario-14 tests for pool: {pool_name}\n"
                        f"Mount point: {mount_point}\n"
                        f"Workload type: {workload_type}\n"
                        f"Pool type: {pool_type}\n"
                        f"{'='*80}"
                    )
                    log.info(
                        f"\n******* testing compression mode **********\n"
                        f"pool -> {pool_name}\n"
                        f"mount point -> {mount_point}\n"
                        f"workload type -> {workload_type}\n"
                        f"pool type -> {pool_type}\n"
                        f"mode -> {mode}\n"
                        f"******* ***************************** **********"
                    )
                    rgw_bucket_name = "test-bucket"

                    log.info(f"Enabling compression mode '{mode}' on pool {pool_name}")
                    bluestore_compression.enable_compression(
                        compression_mode=mode,
                        compression_algorithm=algorithm,
                        pool_level_compression=pool_level_compression,
                        pool_name=pool_name,
                    )
                    log.info(
                        f"Successfully enabled compression mode '{mode}' on pool {pool_name}"
                    )

                    log.info(f"Fetching pool statistics before IO for {pool_name}")
                    pool_stats = bluestore_compression.get_pool_stats(
                        pool_name=pool_name,
                    )
                    log.info(f"\n === POOL {pool_name} stats === ")
                    log.info(f"Compression stats for pool : {pool_name}")
                    log.info(json.dumps(pool_stats, indent=4))

                    comp_io_obj.workload_type = workload_type
                    rbd_cephfs_folder_full_path = mount_point
                    file_path = "/tmp/testfile_%s_%s_%s" % (
                        pool_name,
                        mode,
                        "".join(
                            random.choices(string.ascii_letters + string.digits, k=6)
                        ),
                    )
                    log.info(f"Writing IO to file: {file_path}")
                    log.info(
                        f"File size: 10m, Compression percentage: {compression_percentage}%"
                    )
                    comp_io_obj.write(
                        file_size="10m",
                        compression_percentage=compression_percentage,
                        file_full_path=file_path,
                        offset="0",
                        rbd_cephfs_folder_full_path=rbd_cephfs_folder_full_path,
                        rgw_bucket_name=rgw_bucket_name,
                    )
                    log.info(
                        f"Completed writing IO to pool {pool_name} with mode {mode}"
                    )

                    time.sleep(20)

                    log.info(f"Fetching pool statistics for {pool_name}")
                    pool_stats = bluestore_compression.get_pool_stats(
                        pool_name=pool_name,
                    )
                    log.info(f"\n === POOL {pool_name} stats === ")
                    log.info(f"Compression stats for pool : {pool_name}")
                    log.info(json.dumps(pool_stats, indent=4))
                    err_msg = ""
                    test_pass = False
                    if mode == "force":
                        log.info("Validating FORCE mode: compression should occur")
                        if (
                            pool_stats["compress_under_bytes"] == 0
                            or pool_stats["compress_bytes_used"] == 0
                        ):
                            err_msg = "When compression mode is force, compression should occur"
                            test_pass = False
                            log.error(f"Validation FAILED: {err_msg}")
                        else:
                            test_pass = True
                            log.info(
                                "Validation PASSED: Compression occurred as expected in FORCE mode"
                            )
                    elif mode == "aggressive":
                        log.info(
                            "Validating AGGRESSIVE mode: compression should occur (unless incompressible hint)"
                        )
                        if (
                            pool_stats["compress_under_bytes"] == 0
                            or pool_stats["compress_bytes_used"] == 0
                        ):
                            err_msg = (
                                "When compression mode is aggressive,"
                                " compression should occur(unless incompressible hint is passed, "
                                "hints are not passed in this test)"
                            )
                            test_pass = False
                            log.error(f"Validation FAILED: {err_msg}")
                        else:
                            test_pass = True
                            log.info(
                                "Validation PASSED: Compression occurred as expected in AGGRESSIVE mode"
                            )
                    elif mode == "passive":
                        log.info(
                            "Validating PASSIVE mode: compression should not occur (unless compressible hint)"
                        )
                        if (
                            pool_stats["compress_under_bytes"] != 0
                            or pool_stats["compress_bytes_used"] != 0
                        ):
                            err_msg = (
                                "When compression mode is passive, compression should not occur(unless "
                                "compressible hint is passed, hints are not passed in this test)"
                            )
                            test_pass = False
                            log.error(f"Validation FAILED: {err_msg}")
                        else:
                            test_pass = True
                            log.info(
                                "Validation PASSED: Compression did not occur as expected in PASSIVE mode"
                            )
                    elif mode == "none":
                        log.info("Validating NONE mode: compression should not occur")
                        if (
                            pool_stats["compress_under_bytes"] != 0
                            or pool_stats["compress_bytes_used"] != 0
                        ):
                            err_msg = "When compression mode is None, compression should not occur"
                            test_pass = False
                            log.error(f"Validation FAILED: {err_msg}")
                        else:
                            test_pass = True
                            log.info(
                                "Validation PASSED: Compression did not occur as expected in NONE mode"
                            )

                    if test_pass is False:
                        log.error(
                            f"\n******* VALIDATION FAILED **********\n"
                            f"pool -> {pool_name}\n"
                            f"mount point -> {mount_point}\n"
                            f"workload type -> {workload_type}\n"
                            f"pool type -> {pool_type}\n"
                            f"mode -> {mode}\n"
                            f"err msg -> {err_msg}\n"
                            f"test pass -> {test_pass}\n"
                            f"******* ***************************** **********\n"
                        )
                        raise

                    log.info(f"Validation PASSED for pool {pool_name} with mode {mode}")
                    log.info(f"Starting cleanup for pool {pool_name}, mode {mode}")
                    comp_io_obj.clean(
                        rbd_cephfs_folder_full_path=rbd_cephfs_folder_full_path,
                        rgw_bucket_name=rgw_bucket_name,
                        pool_name=pool_name,
                        compression_obj=bluestore_compression,
                    )

                    log.info(f"Completed clean up for {pool_name} with mode {mode}")

            log.info("******* Test 14 passed **********")

        if "scenario-15" in scenarios_to_run:
            log.info(
                """
Test compression_required_ratio validation with different blob sizes across different pools (cephfs, rbd, rgw).
Steps:
1) Iterate through test pools (cephfs, rbd, rgw)
2) For each pool, test different blob sizes (32768, 65536) and compression_required_ratio (0.5, 0.8)
3) Enable compression with compression_required_ratio and compression_min_blob_size
4) Write IO to the pool with compressible data
5) Map objects to their primary OSDs
6) Stop OSDs and fetch blob information from objectstore
7) Validate compression based on ratio:
   - If compressed_AU/original_AU < ratio, compression should occur
   - If compressed_AU/original_AU >= ratio, compression should not occur
8) Start OSDs and cleanup
            """
            )
            algorithm = "snappy"
            compression_percentage = "70"
            log.info(
                f"Test configuration: algorithm={algorithm}, compression_percentage={compression_percentage}%"
            )
            objectstore_obj.nostop = True
            objectstore_obj.nostart = True
            log.info(
                "Set objectstore_obj.nostop=True and nostart=True to prevent OSD restarts during blob inspection"
            )
            if len(test_pools) == 0:
                log.error("CephFS, RBD and RGW setup has failed")
                return 1

            for pool_name, mount_point, workload_type, pool_type in test_pools:
                log.info(
                    f"\n{'='*80}\n"
                    f"Starting Scenario-15 tests for pool: {pool_name}\n"
                    f"Mount point: {mount_point}\n"
                    f"Workload type: {workload_type}\n"
                    f"Pool type: {pool_type}\n"
                    f"{'='*80}"
                )
                for blob_size in [32768, 65536]:
                    for ratio in [0.5]:
                        log.info(f"Testing with compression_required_ratio: {ratio}")
                        log.info(
                            f"\n******* testing compression required ratio **********\n"
                            f"pool -> {pool_name}\n"
                            f"mount point -> {mount_point}\n"
                            f"workload type -> {workload_type}\n"
                            f"pool type -> {pool_type}\n"
                            f"blob size -> {blob_size}\n"
                            f"ratio -> {ratio}\n"
                            f"******* ***************************** **********"
                        )
                        rgw_bucket_name = "test-bucket"

                        log.info(
                            f"Enabling compression on pool {pool_name} with:\n"
                            f"  compression_mode: FORCE\n"
                            f"  compression_algorithm: {algorithm}\n"
                            f"  compression_required_ratio: {ratio}\n"
                            f"  compression_min_blob_size: {blob_size}"
                        )
                        bluestore_compression.enable_compression(
                            compression_mode=COMPRESSION_MODES.FORCE,
                            compression_algorithm=algorithm,
                            pool_level_compression=pool_level_compression,
                            compression_required_ratio=ratio,
                            pool_name=pool_name,
                            compression_min_blob_size=blob_size,
                        )
                        log.info(
                            f"Successfully enabled compression on pool {pool_name}"
                        )
                        time.sleep(30)

                        comp_io_obj.workload_type = workload_type
                        rbd_cephfs_folder_full_path = mount_point
                        file_path = "/tmp/testfile_%s_%s_%s" % (
                            pool_name,
                            str(ratio),
                            "".join(
                                random.choices(
                                    string.ascii_letters + string.digits, k=6
                                )
                            ),
                        )
                        log.info(f"Writing IO to file: {file_path}")
                        log.info(
                            f"File size: 10m, Compression percentage: {compression_percentage}%"
                        )
                        comp_io_obj.write(
                            file_size="10m",
                            compression_percentage=compression_percentage,
                            file_full_path=file_path,
                            offset="0",
                            rbd_cephfs_folder_full_path=rbd_cephfs_folder_full_path,
                            rgw_bucket_name=rgw_bucket_name,
                        )
                        log.info(f"Completed writing IO to pool {pool_name}")

                        int_compression_percentage = (
                            (100 - int(compression_percentage)) + 5
                        ) / 100  # 0.35
                        log.info(
                            f"Calculated compression percentage (with 5% buffer): {int_compression_percentage}"
                        )
                        log.info(f"Listing objects in pool {pool_name}")
                        cmd = f"rados ls -p {pool_name}"
                        out = rados_obj.client.exec_command(cmd=cmd, sudo=True)
                        log.info(
                            f"Found objects in pool {pool_name}: {out[0] if out[0] else 'None'}"
                        )

                        osd_to_object_map = {}
                        log.info("Mapping objects to their primary OSDs")
                        # {23: [obj1, obj2] }
                        for obj_name in out[0].split():
                            log.debug(f"Processing object: {obj_name}")
                            osd_map = rados_obj.get_osd_map(
                                pool=pool_name, obj=obj_name
                            )
                            acting_pg_set = osd_map["acting"]
                            primary_osd_id = acting_pg_set[0]
                            log.debug(
                                f"Object {obj_name} is on primary OSD {primary_osd_id}"
                            )
                            osd_to_object_map[primary_osd_id] = osd_to_object_map.get(
                                primary_osd_id, []
                            )
                            osd_to_object_map[primary_osd_id].append(obj_name)
                        log.info(
                            f"OSD to object mapping completed. Total OSDs with objects: {len(osd_to_object_map)}"
                        )

                        obj_name_to_blobs_map = {}
                        log.info("Fetching blob information from OSDs")
                        for primary_osd_id, obj_names_list in osd_to_object_map.items():
                            log.info(
                                f"Processing OSD ID {primary_osd_id} with"
                                f" {len(obj_names_list)} objects: {obj_names_list}"
                            )
                            log.info(
                                f"Stopping OSD {primary_osd_id} to fetch blob information"
                            )
                            rados_obj.change_osd_state(
                                action="stop", target=primary_osd_id
                            )
                            log.info(f"OSD {primary_osd_id} stopped successfully")

                            for obj_name in obj_names_list:
                                log.info(
                                    f"Processing object {obj_name} in OSD {primary_osd_id}"
                                )
                                log.debug(
                                    f"Listing object {obj_name} in OSD {primary_osd_id}"
                                )
                                obj_json = objectstore_obj.list_objects(
                                    osd_id=int(primary_osd_id), obj_name=obj_name
                                ).strip()
                                if not obj_json:
                                    log.error(
                                        f"Object {obj_name} could not be listed in OSD {primary_osd_id}"
                                    )
                                    raise

                                log.debug(
                                    f"Fetching object dump for {obj_name} from OSD {primary_osd_id}"
                                )
                                blobs_json = objectstore_obj.fetch_object_dump(
                                    osd_id=int(primary_osd_id), obj=obj_json
                                )
                                blobs_json = json.loads(blobs_json)
                                obj_name_to_blobs_map[obj_name] = blobs_json
                                log.info(
                                    f"Successfully fetched blob information for object {obj_name}"
                                )

                            log.info(
                                f"Starting OSD {primary_osd_id} after blob inspection"
                            )
                            rados_obj.change_osd_state(
                                action="start", target=primary_osd_id
                            )
                            log.info(f"OSD {primary_osd_id} started successfully")

                        log.info(
                            f"Object name to blobs map: {json.dumps(obj_name_to_blobs_map, indent=4)}"
                        )
                        log.info(
                            f"OSD ID to object map : {json.dumps(osd_to_object_map, indent=4)}"
                        )

                        log.info(
                            f"Starting validation of compression_required_ratio={ratio} with blob_size={blob_size}"
                        )
                        log.info(
                            f"Validation rule: If compressed_AU/original_AU < {ratio}, compression should occur"
                        )
                        for obj_name, blobs_json in obj_name_to_blobs_map.items():
                            log.info(f"Validating blobs for object: {obj_name}")
                            extents = blobs_json["onode"]["extents"][:-1]
                            log.info(f"Number of extents to validate: {len(extents)}")
                            for idx, blob_detail in enumerate(extents):
                                log.debug(f"Processing blob {idx} of object {obj_name}")
                                log.debug(blob_detail)
                                blob = blob_detail["blob"]
                                logical_length = blob["logical_length"]
                                compressed_length = blob["compressed_length"]
                                actual_AU = math.ceil(logical_length / 4096)
                                log.debug(
                                    f"Blob {idx}: logical_length={logical_length}, "
                                    f"compressed_length={compressed_length}, actual_AU={actual_AU}"
                                )

                                msg = (
                                    f"\nPool -> {pool_name}\n"
                                    f"Object -> {obj_name}\n"
                                    f"Blob index -> {idx}\n"
                                    f"compressed_length -> {compressed_length}\n"
                                    f"logical_length -> {logical_length}\n"
                                    f"Actual AU (logical_length/4096) -> {actual_AU}\n"
                                    f"Calculated compressed length -> {blob_size * int_compression_percentage}\n"
                                    f"int com per -> {int_compression_percentage}\n"
                                    f"blob size -> {blob_size}\n"
                                    f"compression_required_ratio -> {ratio}"
                                )
                                # no compression
                                if compressed_length == 0:
                                    log.info(
                                        f"Blob {idx} is NOT compressed (compressed_length=0)"
                                    )
                                    # 8192 * 0.35 is used to obtain 35% of 8192
                                    # which is post compression size.
                                    compressed_AU = math.ceil(
                                        (logical_length * int_compression_percentage)
                                        / 4096
                                    )
                                    calculated_ratio = compressed_AU / actual_AU
                                    log.info(
                                        f"Calculated compressed_AU: {compressed_AU}, "
                                        f"calculated_ratio: {calculated_ratio}"
                                    )
                                    # blobs with length <= minimum allocation size are not compressed
                                    # Bug https://bugzilla.redhat.com/show_bug.cgi?id=2427146
                                    # Blobs with more than one extents are not compressed despite compression
                                    # being enabled
                                    if (
                                        len(blob["extents"]) > 1
                                    ) or logical_length == 4096:
                                        msg += (
                                            f"\ncompressed AU -> {compressed_AU}\n"
                                            f"calculated_ratio -> {calculated_ratio}\n"
                                            f"When extents are > 1\n"
                                            f"Expected: No compression (ratio > required ratio) ✓"
                                        )
                                        log.info(msg)
                                        log.info("***PASS***")
                                    elif float(calculated_ratio) > float(ratio):
                                        msg += (
                                            f"\ncompressed AU -> {compressed_AU}\n"
                                            f"calculated_ratio -> {calculated_ratio}\n"
                                            f"compressed length == 0 and {calculated_ratio} > {ratio}\n"
                                            f"Expected: No compression (ratio > required ratio) ✓"
                                        )
                                        log.info(msg)
                                        log.info("***PASS***")
                                    else:
                                        msg += (
                                            f"\ncompressed AU -> {compressed_AU}\n"
                                            f"calculated_ratio -> {calculated_ratio}\n"
                                            f"err -> When calculated ratio < required ratio, compression should occur"
                                        )
                                        log.error(msg)
                                        log.error("***FAIL***")
                                        raise
                                else:
                                    log.info(
                                        f"Blob {idx} is compressed (compressed_length={compressed_length})"
                                    )
                                    compressed_AU = math.ceil(compressed_length / 4096)
                                    calculated_ratio = compressed_AU / actual_AU
                                    log.info(
                                        f"Calculated compressed_AU: {compressed_AU}, "
                                        f"calculated_ratio: {calculated_ratio}"
                                    )
                                    # For 8192 the compression required ratio is not honoured in bluestore v1
                                    # In bluestore v2 for all blob sizes , compression required ratio is not honoured
                                    if logical_length == 8192 or float(
                                        calculated_ratio
                                    ) <= float(ratio):
                                        msg += (
                                            f"\ncompressed AU -> {compressed_AU}\n"
                                            f"calculated_ratio -> {calculated_ratio}\n"
                                            f"compressed length > 0 and {calculated_ratio} < {ratio}\n"
                                            f"Expected: Compression occurred (ratio < required ratio) ✓"
                                        )
                                        log.info(msg)
                                        log.info("***PASS***")
                                    else:
                                        msg += (
                                            f"\ncompressed AU -> {compressed_AU}\n"
                                            f"calculated_ratio -> {calculated_ratio}\n"
                                            f"When calculated ratio > required ratio, compression should not occur"
                                        )
                                        log.error(msg)
                                        log.error("***FAIL***")
                                        raise
                            log.info(f"Completed validation for object {obj_name}")
                        log.info(
                            f"Validation summary for ratio={ratio}, blob_size={blob_size}, pool={pool_name}: "
                        )

                        log.info(
                            f"Starting cleanup for pool {pool_name}, ratio={ratio}, blob_size={blob_size}"
                        )
                        comp_io_obj.clean(
                            rbd_cephfs_folder_full_path=rbd_cephfs_folder_full_path,
                            rgw_bucket_name=rgw_bucket_name,
                            pool_name=pool_name,
                            compression_obj=bluestore_compression,
                        )

                        log.info(
                            f"Completed clean up for {pool_name} with ratio={ratio}, blob_size={blob_size}"
                        )

            objectstore_obj.nostop = None
            objectstore_obj.nostart = None
            log.info("Reset objectstore_obj.nostop and nostart flags")

            log.info("******* Test 15 passed **********")

        if "scenario-16" in scenarios_to_run:
            log.info(
                """
Test compression validation post recovery across cephfs, rgw and rbd.
Steps:
1) Iterate through test pools (cephfs, rbd, rgw)
2) Enable compression on the pool
3) Write IO to the pool with compressible data
4) Get object and primary OSD from acting set
5) Stop primary OSD and validate compression on initial write
6) Start primary OSD
7) Mark primary OSD out
8) Wait for active + clean state
9) Get newly added OSD from new acting set
10) Validate that objects are compressed on newly added OSD
11) Cleanup pool
            """
            )
            # initializations
            compression_percentage = 70
            log.info("Starting scenario-16")

            if len(test_pools) == 0:
                log.error("CephFS, RBD and RGW setup has failed")
                return 1
            # Step 1: Create pool
            for pool_name, mount_point, workload_type, pool_type in test_pools:
                log.info(
                    f"\n******* Starting scenario 16 **********\n"
                    f"pool -> {pool_name}\n"
                    f"mount point -> {mount_point}\n"
                    f"workload type -> {workload_type}\n"
                    f"pool type -> {pool_type}\n"
                    f"******* ***************************** **********"
                )

                bluestore_compression.enable_compression(
                    compression_mode=COMPRESSION_MODES.FORCE,
                    compression_algorithm=COMPRESSION_ALGORITHMS.snappy,
                    pool_level_compression=pool_level_compression,
                    pool_name=pool_name,
                )
                log.info(
                    f"Enabled comrpession on the pool {pool_name} pool type : {pool_type}"
                )

                time.sleep(20)

                comp_io_obj.workload_type = workload_type
                rbd_cephfs_folder_full_path = mount_point

                comp_io_obj.write(
                    file_size="10m",
                    compression_percentage=str(compression_percentage),
                    file_full_path="/tmp/testfile_%s_%s"
                    % (
                        pool_name,
                        "".join(
                            random.choices(string.ascii_letters + string.digits, k=6)
                        ),
                    ),
                    offset="0",
                    rbd_cephfs_folder_full_path=rbd_cephfs_folder_full_path,
                    rgw_bucket_name=rgw_bucket_name,
                )
                log.info(
                    f"Completed IO on the pool {pool_name} pool type : {pool_type}"
                )

                cmd = f"rados ls -p {pool_name}"
                out = rados_obj.client.exec_command(cmd=cmd, sudo=True)

                # {23: [obj1, obj2] }
                obj_name = out[0].split()[0]
                osd_map = rados_obj.get_osd_map(pool=pool_name, obj=obj_name)
                acting_pg_set = osd_map["acting"]
                primary_osd_id = acting_pg_set[0]

                log.info(f"PG acting set:{acting_pg_set}")
                obj_name_to_blobs_map = {}
                rados_obj.change_osd_state(action="stop", target=primary_osd_id)
                log.debug(f"Processing object {obj_name} in OSD {primary_osd_id}")
                obj_json = objectstore_obj.list_objects(
                    osd_id=int(primary_osd_id), obj_name=obj_name
                ).strip()
                if not obj_json:
                    log.error(
                        f"Object {obj_name} could not be listed in OSD {primary_osd_id}"
                    )
                    raise
                blobs_json = objectstore_obj.fetch_object_dump(
                    osd_id=int(primary_osd_id), obj=obj_json
                )
                blobs_json = json.loads(blobs_json)
                for blob_detail in blobs_json["onode"]["extents"][:-1]:
                    blob = blob_detail["blob"]
                    compressed_length = blob["compressed_length"]
                    if compressed_length == 0:
                        log.info(
                            f"Compression did not occur on initial write operation: "
                            f"compressed lenght : {compressed_length}"
                        )
                        raise
                    else:
                        log.info("Compression occured on the blob. continue")
                log.info(
                    f"Object {obj_name} is compressed. pool {pool_name} pool type : {pool_type}\n"
                    f"OSD ID: {primary_osd_id}"
                )

                rados_obj.change_osd_state(action="start", target=primary_osd_id)
                log.info(
                    f"Setting OSD {primary_osd_id} out. pool {pool_name} pool type : {pool_type}\n"
                )
                set_osd_out(ceph_cluster, primary_osd_id)

                log.info(
                    f"Waiting for clean pg set. After marking OSD out {primary_osd_id}.\n"
                    f" pool {pool_name} pool type : {pool_type}\n"
                )
                method_should_succeed(wait_for_clean_pg_sets, rados_obj)

                log.info(
                    f"Fetching new acting set. pool {pool_name} pool type : {pool_type}\n"
                )
                osd_map = rados_obj.get_osd_map(pool=pool_name, obj=obj_name)
                new_acting_pg_set = osd_map["acting"]
                log.info(
                    f"new acting set ->  {new_acting_pg_set}.pool {pool_name}, pool type : {pool_type}\n"
                )
                newly_added_osd = list(set(new_acting_pg_set) - set(acting_pg_set))[0]

                log.info(
                    f"newly added OSD ->  {newly_added_osd}.pool {pool_name},  pool type : {pool_type}\n"
                )
                obj_json = objectstore_obj.list_objects(
                    osd_id=newly_added_osd, obj_name=obj_name
                ).strip()
                if not obj_json:
                    log.error(
                        f"Object {obj_name} could not be listed in OSD {newly_added_osd}"
                    )
                    raise
                blobs_json = objectstore_obj.fetch_object_dump(
                    osd_id=int(newly_added_osd), obj=obj_json
                )
                blobs_json = json.loads(blobs_json)
                for blob_detail in blobs_json["onode"]["extents"][:-1]:
                    blob = blob_detail["blob"]
                    compressed_length = blob["compressed_length"]
                    # Bug https://bugzilla.redhat.com/show_bug.cgi?id=2427146
                    # Blobs with more than one extents are not compressed despite compression being enabled
                    if len(blob["extents"]) > 1 and compressed_length == 0:
                        log.info(
                            "Data will not be stored in the extent. Hence compression would not occur."
                        )
                    elif compressed_length == 0:
                        log.info(
                            f"Compression did not occur on initial write operation:"
                            f" compressed lenght : {compressed_length}"
                        )
                        raise
                    else:
                        log.info("Compression occured on the blob. continue")

                log.info(
                    f"\nObject {obj_name} is compressed on newly added OSD to acting set"
                    f". pool {pool_name} pool type : {pool_type}\n"
                    f"OSD ID: {newly_added_osd}\n"
                    f"***PASS***"
                )

                comp_io_obj.clean(
                    rbd_cephfs_folder_full_path=rbd_cephfs_folder_full_path,
                    rgw_bucket_name=rgw_bucket_name,
                    pool_name=pool_name,
                    compression_obj=bluestore_compression,
                )

                log.info(f"Completed clean up for {pool_name}")

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

        # Clean up objects in the pools
        if config.get("cleanup_objects", False):
            for pool_name, mount_point, workload_type, pool_type in test_pools:
                log.info(
                    f"Deleting objects of pool={pool_name} mount_point={mount_point} pool_type={pool_type}"
                )
                comp_io_obj.workload_type = workload_type
                rbd_cephfs_folder_full_path = mount_point
                comp_io_obj.clean(
                    rbd_cephfs_folder_full_path=rbd_cephfs_folder_full_path,
                    rgw_bucket_name=rgw_bucket_name,
                    pool_name=pool_name,
                    compression_obj=bluestore_compression,
                )

        # Clean up pools (if configured)
        if config.get("cleanup_pools", False) and created_resources:
            log.debug("Cleaning up test resources...")

            # Parallel cleanup of CephFS and RBD
            cleanup_tasks = []
            with cf.ThreadPoolExecutor(max_workers=2) as cleanup_executor:
                for name, mount_path, _, workload_type in created_resources:
                    # Submit cleanup tasks
                    if workload_type == "cephfs":
                        task = (
                            cleanup_executor.submit(
                                _cleanup_cephfs, client_node, mount_path, name
                            )
                            if (mount_path or name)
                            else None
                        )
                    elif workload_type == "rbd":
                        task = (
                            cleanup_executor.submit(
                                _cleanup_rbd, client_node, mount_path, name
                            )
                            if mount_path
                            else None
                        )
                cleanup_tasks.append(task)

            for task in cleanup_tasks:
                # Wait for cleanup tasks to complete
                try:
                    task.result()
                except Exception as e:
                    log.warning(f"cleanup failed: {e}")

            # Clean up rados pools sequentially (depends on CephFS/RBD cleanup)
            log.info(f"Deleting {len(created_resources)} pool(s)...")
            failed_pools = []

            for name, mount_path, created_pools, workload_type in created_resources:
                for pool in created_pools:
                    pool_name = pool["pool_name"]
                    try:
                        rados_obj.delete_pool(pool=pool_name)
                    except Exception as e:
                        log.error(f"Failed to delete pool {pool_name}: {e}")
                        failed_pools.append(pool_name)
            if failed_pools:
                log.warning(
                    f"Failed to delete {len(failed_pools)} pool(s): {failed_pools}"
                )
            else:
                log.info("All pools deleted successfully")
        else:
            log.info("Skipping cleanup (cleanup_pools=False or no pools created)")

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed validation of bluestore v2 data compression.")
    return 0
