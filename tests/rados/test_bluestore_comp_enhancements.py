"""
Test Module to test functionalities of bluestore data compression.
scenario-1: Validate default compression values
scenario-2: Enable bluestore_write_v2 and validate
scenario-3: Disable bluestore_write_v2 and validate
"""

import random
import string
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.utils import get_cluster_timestamp
from ceph.utils import find_vm_node_by_hostname
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_bluestore_comp_enhancements_class import (
    BLUESTORE_ALLOC_HINTS,
    COMPRESSION_ALGORITHMS,
    COMPRESSION_MODES,
    BluestoreDataCompression,
    IOTools,
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
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:

        log.info(
            "\n\n ************ Execution begins for bluestore data compression scenarios ************ \n\n"
        )

        log.info("Install FIO on client nodes")
        client_nodes = ceph_cluster.get_nodes(role="client")
        cmd = "yum install fio -y"
        for node in client_nodes:
            node.exec_command(cmd=cmd, sudo=True)

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

            pool_suffix = "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )
            pool_name = f"rados-{pool_suffix}"
            compression_mode = COMPRESSION_MODES.FORCE
            compression_algorithm = COMPRESSION_ALGORITHMS.snappy
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
            compression_mode = COMPRESSION_MODES.FORCE
            compression_algorithm = COMPRESSION_ALGORITHMS.snappy
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
            # initializations
            compression_mode = COMPRESSION_MODES.FORCE
            compression_algorithm = COMPRESSION_ALGORITHMS.snappy
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
            # Initializations
            compression_mode = COMPRESSION_MODES.FORCE
            compression_algorithm = COMPRESSION_ALGORITHMS.snappy
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
            compression_mode = COMPRESSION_MODES.FORCE
            compression_algorithm = COMPRESSION_ALGORITHMS.snappy
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
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed validation of bluestore v2 data compression.")
    return 0
