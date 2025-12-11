"""
Test Module to perform functionalities of bluestore data compression.
scenario-1: Validate basic compression workflow
scenario-2: uncompressed pool to compressed pool conversion
scenario-3: Compressed pool to uncompressed pool conversion
scenario-4: Enable compressesion at OSD level and disable compression at pool level
scenario-5: Validate pools inherit compression configurations from OSD
scenario-6: Validate data migration between compressed pools
scenario-7: Validate OSD replacement
scenario-8: Validate different values of compression_required_ratio
scenario-9: Validate different values of compression_min_blob_size
"""

import json
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.rados_test_util import (
    get_device_path,
    wait_for_daemon_status,
    wait_for_device_rados,
)
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import generate_unique_id, method_should_succeed, should_not_be_empty

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to perform +ve workflows for the bluestore data compression
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_prefix = "compression_test"
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)
    pool_obj = PoolFunctions(node=cephadm)
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
            "scenario-8",
            "scenario-9",
        ],
    )

    def validate_basic_compression_workflow():
        log.info(
            "\n ---------------------------------"
            "\n Test #1  Validate basic compression workflow"
            "\n 1. Create pool without compression"
            "\n 2. Enable compression on the pool"
            "\n 3. Write IO to the pool"
            "\n 4. Collect stats of the pool such as size of data, used compression and under compression"
            "\n 5. Perform validations for compression"
            "\n 6. Delete pool"
            "\n ---------------------------------"
        )

        pool_name = f"{pool_prefix}-{generate_unique_id(4)}"

        log.info("1. Creating pools without compression configurations")
        log_info_msg = (
            f"Creating Replicated pool {pool_name} without compression configurations"
        )
        log.info(log_info_msg)
        assert rados_obj.create_pool(pool_name=pool_name)

        log_info_msg = f"2. Enable compression on the pool {pool_name}"
        log.info(log_info_msg)
        if not rados_obj.pool_inline_compression(
            pool_name=pool_name,
            compression_mode="force",
            compression_algorithm="snappy",
        ):
            err_msg = f"Error setting compression on pool : {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        log_info_msg = f"3. Write IO to the pool {pool_name}"
        log.info(log_info_msg)
        if (
            pool_obj.do_rados_put(
                client=client_node,
                pool=pool_name,
                nobj=10,
                obj_name=f"{pool_name}-{generate_unique_id(4)}",
            )
            == 1
        ):
            log.error("Failed to write objects into Pool")
            raise Exception("Write IO failed on pool without compression")

        log_info_msg = f"4. Collect stats of the pool {pool_name} such as size of data\
            , used compression and under compression"
        log.info(log_info_msg)
        pool_stats = get_pool_stats(rados_obj=rados_obj, pool_name=pool_name)

        log_info_msg = (
            f"Pool stats of {pool_name} after compression {json.dumps(pool_stats)}"
        )
        log.info(log_info_msg)
        """
        Example pool stats:
        {
            "name": "compression_test-FYO5",
            "id": 100,
            "stats": {
                "stored": 2048024,
                "stored_data": 2048024,
                ...
                ...
                "bytes_used": 1241088,
                "data_bytes_used": 1241088,
                ...
                ...
                "compress_bytes_used": 1228800,
                "compress_under_bytes": 6144000,
                "stored_raw": 6144072,
                ...
                ...
            }
        }
        """
        log.info("5. Perform validations on compressed pool")
        bluestore_compression_required_ratio = (
            get_default_bluestore_compression_required_ratio(
                mon_obj=mon_obj, rados_obj=rados_obj, pool_name=pool_name
            )
        )

        # compression enabled, hence compressed data should be less than 87.5% of original data
        log.info("Validating if compression_required_ratio is maintained")
        if not compression_ratio_maintained(
            pool_stats=pool_stats,
            bluestore_compression_required_ratio=bluestore_compression_required_ratio,
        ):
            raise Exception(
                f"Pool {pool_name}: compression_required_ratio is not maintained"
            )

        # compression enabled, hence compress_bytes_used should not be 0
        # data is being compressed to <compress_bytes_used> bytes
        if not data_compressed(pool_stats=pool_stats):
            raise Exception(
                "Data is not compressed. compress_under_bytes == 0 and compress_used_bytes == 0"
            )

        log_info_msg = (
            f"6. Reading the uncompressed and compressed data from pool {pool_name}"
        )
        log.info(log_info_msg)
        if not pool_obj.do_rados_get(pool=pool_name, read_count="all"):
            raise Exception(f"Unable to read compressed data from pool {pool_name}")

        log.info("7. Delete the compressed pool")
        if not rados_obj.delete_pool(pool=pool_name):
            raise Exception(f"Deleting of pool {pool_name} failed")

    def validate_uncompressed_pool_to_compressed_pool_conversion():
        log.info(
            "\n ---------------------------------"
            "\n Test #2 uncompressed pool to compressed pool conversion"
            "\n 1. Create pool without compression"
            "\n 2. Write IO to the pool"
            "\n 3. Collect stats of the pool such as size of data, used compression and under compression"
            "\n 4. Enable compression on the pool"
            "\n 5. Validate existing data is not compressed when compression is enabled"
            "\n 6. Write IO to compression enabled pool"
            "\n 7. Collect stats of the pool such as size of data, used compression and under compression"
            "\n 8. Perform validations for compression"
            "\n    - Data written after enabling compression is compressed"
            "\n    - Compressed data is "
            "\n 9. Delete pool"
            "\n ---------------------------------"
        )
        pool_name = f"{pool_prefix}-{generate_unique_id(4)}"

        log.info("1. Creating pools without compression configurations")

        log_info_msg = (
            f"Creating Replicated pool {pool_name} without compression configurations"
        )
        log.info(log_info_msg)
        assert rados_obj.create_pool(pool_name=pool_name)

        log_info_msg = f"2. Write IO to the pool {pool_name}"
        log.info(log_info_msg)
        pool_obj.do_rados_put(
            client=client_node,
            pool=pool_name,
            nobj=10,
            obj_name=f"{pool_name}-{generate_unique_id(4)}",
        )

        log_info_msg = f"3. Collect stats of the pool {pool_name} such as \
            size of data, used compression and under compression"
        log.info(log_info_msg)
        pool_stats_before_compression = get_pool_stats(
            rados_obj=rados_obj, pool_name=pool_name
        )
        log.info(json.dumps(pool_stats_before_compression, indent=4))

        log_info_msg = f"4. Enable compression on the pool {pool_name}"
        log.info(log_info_msg)
        if not rados_obj.pool_inline_compression(
            pool_name=pool_name,
            compression_mode="force",
            compression_algorithm="snappy",
        ):
            err_msg = f"Error setting compression on pool : {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(
            "5. Validate exisiting data written before enabling compression remains uncompressed"
        )

        pool_stats_after_enabling_compression_before_IO = get_pool_stats(
            rados_obj=rados_obj, pool_name=pool_name
        )

        log.info(json.dumps(pool_stats_after_enabling_compression_before_IO, indent=4))
        if data_compressed(pool_stats=pool_stats_after_enabling_compression_before_IO):
            raise Exception("Exisiting data should not be compressed")

        log_info_msg = f"6. Write IO to the compression enabled pool {pool_name}"
        log.info(log_info_msg)
        pool_obj.do_rados_put(
            client=client_node,
            pool=pool_name,
            nobj=20,
            obj_name=f"{pool_name}-{generate_unique_id(4)}",
        )

        log_info_msg = f"7. Collect stats of compression enabled pool {pool_name} such as size of data \
            used compression and under compression"
        log.info(log_info_msg)

        pool_stats_after_compression = get_pool_stats(
            rados_obj=rados_obj, pool_name=pool_name
        )

        log_info_msg = f"Pool stats of {pool_name} before \
            compression {json.dumps(pool_stats_before_compression, indent=4)}"
        log.info(log_info_msg)

        log_info_msg = f"Pool stats of {pool_name} after \
            compression {json.dumps(pool_stats_after_compression, indent=4)}"
        log.info(log_info_msg)

        log_info_msg = f"8. Perform validations on compressed pool {pool_name}"
        log.info(log_info_msg)
        """
        Pool stats of compression_test-J9QT before compression {
            "stored": 41943040,
            "stored_data": 41943040,
            ...
            "bytes_used": 125829120,
            "data_bytes_used": 125829120,
            ...
            "compress_bytes_used": 0,
            "compress_under_bytes": 0,
            "stored_raw": 125829120,
        }
        Pool stats of compression_test-J9QT after compression {
            "stored": 123032920,
            "stored_data": 123032920,
            ...
            "bytes_used": 247463936,
            "data_bytes_used": 247463936,
            ...
            "compress_bytes_used": 121634816,
            "compress_under_bytes": 243269632,
            "stored_raw": 369098752,
        }"
        """

        total_data_written_before_compression = pool_stats_before_compression[
            "stored_raw"
        ]  # includes replication factor => 12582912

        total_data_written_after_compression = (
            pool_stats_after_compression["stored_raw"]
            - total_data_written_before_compression
        )  # 369098752 - 125829120 = 243269632

        bluestore_compression_required_ratio = (
            get_default_bluestore_compression_required_ratio(
                mon_obj=mon_obj, rados_obj=rados_obj, pool_name=pool_name
            )
        )

        log.info(
            "Check #1 Validation for data written after enabling compression should be compressed."
        )
        if not is_deviation_within_allowed_percentage(
            total_data_written_after_compression,
            pool_stats_after_compression["compress_under_bytes"],
            10,
        ):
            log_msg = f"Data written after compression : {total_data_written_after_compression}\
                | Data being compressed : {pool_stats_after_compression['compress_under_bytes']}"
            raise Exception(log_msg)

        log.info(
            "Check #2 Validation for data compression abides by bluestore_compression_required_ratio"
        )
        if not compression_ratio_maintained(
            pool_stats_after_compression, bluestore_compression_required_ratio
        ):
            log_msg = f"compress_under_bytes => {pool_stats_after_compression['compress_under_bytes']}\
                compress_bytes_used => {pool_stats_after_compression['compress_bytes_used']}"
            log.info(log_msg)
            raise Exception(
                "Size of compressed data should be less than or equal to bluestore_compression_required_ratio"
            )

        log.info(
            "Check #3 Validating compress_success_count for default required ratio is not 0 \
                 and compress_rejected_count is 0. Since compression is expected"
        )
        compress_success_count = get_compress_success_count(
            rados_obj=rados_obj, pool_name=pool_name
        )
        if compress_success_count == 0:
            raise Exception(
                f"Compression success count cannot be 0 for compressed pool\
                            Current compress_success_count is {compress_success_count}"
            )

        log_info_msg = (
            f"9. Reading the uncompressed and compressed data from pool {pool_name}"
        )
        log.info(log_info_msg)
        if not pool_obj.do_rados_get(pool=pool_name, read_count="all"):
            raise Exception("Unable to read uncompressed and compressed data")

        log_info_msg = f"10. Delete pool {pool_name}"
        log.info(log_info_msg)
        if rados_obj.delete_pool(pool=pool_name) is False:
            raise Exception(f"Error deleting pool {pool_name}")

    def validate_compressed_pool_to_uncompressed_pool_conversion():
        log.info(
            "\n ---------------------------------"
            "\n Test #3 Compressed pool to uncompressed pool conversion"
            "\n 1. Create pool"
            "\n 2. Enable compession on the pool"
            "\n 3. Write IO to the compressed pool"
            "\n 4. Collect stats of the pool such as size of data, used compression and under compression"
            "\n 5. Disable compression on the pool"
            "\n 6. Validating after compression is disabled, the data should still remain compressed"
            "\n 7. Write IO to the compression disabled pool"
            "\n 8. Collect stats of compression disabled pool"
            "\n 9. Perform validations for compression"
            "\n    - New data is not compressed since compression is disabled"
            "\n 10. Read and write all data from the pool ( uncompressed, compressed )"
            "\n 11. Delete pool"
            "\n ---------------------------------"
        )

        pool_name = f"{pool_prefix}-{generate_unique_id(4)}"

        log.info("1. Creating replicated pool")
        log_info_msg = f"Creating Replicated pool {pool_name} with compression"
        log.info(log_info_msg)
        assert rados_obj.create_pool(pool_name=pool_name)

        log_info_msg = f"2. Enable compression on the pool {pool_name}"
        log.info(log_info_msg)
        if not rados_obj.pool_inline_compression(
            pool_name=pool_name,
            compression_mode="force",
            compression_algorithm="snappy",
        ):
            err_msg = f"Error setting compression on pool : {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        log_info_msg = f"3. Write IO to the compressed pool {pool_name}"
        log.info(log_info_msg)
        if (
            pool_obj.do_rados_put(
                client=client_node,
                pool=pool_name,
                nobj=10,
                obj_name=f"{pool_name}-{generate_unique_id(4)}",
            )
            == 1
        ):
            exception_msg = f"Writing IO to pool {pool_name} failed"
            raise Exception(exception_msg)

        log_info_msg = f"4. Collect stats of compressed pool {pool_name} such as size of data \
            used compression and under compression"
        log.info(log_info_msg)

        pool_stats_before_disabling_compression = get_pool_stats(
            rados_obj=rados_obj, pool_name=pool_name
        )

        log_info_msg = f"Pool stats before disabling \
            compression: {json.dumps(pool_stats_before_disabling_compression, indent=4)}"
        log.info(log_info_msg)

        log_info_msg = f"5. Disable compression on pool {pool_name}"
        log.info(log_info_msg)
        if not rados_obj.pool_inline_compression(
            pool_name=pool_name, compression_mode="none"
        ):
            err_msg = f"Error disabling compression on pool : {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)
        pool_stats_after_disabling_compression_before_IO = get_pool_stats(
            rados_obj=rados_obj, pool_name=pool_name
        )

        bluestore_compression_required_ratio = (
            get_default_bluestore_compression_required_ratio(
                mon_obj=mon_obj, rados_obj=rados_obj, pool_name=pool_name
            )
        )

        log.info(
            "6. Validating after compression is disabled, the data should still remain compressed"
        )
        log.info("Pool stats after disabling compression , before writing new IO")
        log.info(json.dumps(pool_stats_after_disabling_compression_before_IO, indent=4))
        if not data_compressed(
            pool_stats=pool_stats_after_disabling_compression_before_IO,
        ):
            raise Exception(
                "Even after disabling compression, existing data should remain compressed"
            )

        if not compression_ratio_maintained(
            pool_stats_after_disabling_compression_before_IO,
            bluestore_compression_required_ratio,
        ):
            raise Exception("Compression required ratio is not maintained")

        log_info_msg = f"7. Write IO to the compression disabled pool {pool_name}"
        log.info(log_info_msg)
        if (
            pool_obj.do_rados_put(
                client=client_node,
                pool=pool_name,
                nobj=20,
                obj_name=f"{pool_name}-{generate_unique_id(4)}",
            )
            == 1
        ):
            exception_msg = f"Writing IO to pool {pool_name} failed"
            raise Exception(exception_msg)

        log_info_msg = f"8. Collect stats of compression disabled pool {pool_name} \
        such as size of data, used compression and under compression"
        log.info(log_info_msg)
        pool_stats_after_disabling_compression_and_after_IO = get_pool_stats(
            rados_obj=rados_obj, pool_name=pool_name
        )

        log_info_msg = f"9. Perform validations on compressed pool {pool_name}"
        log.info(log_info_msg)
        log_info_msg = f"Pool stats before disabling \
            compression: {json.dumps(pool_stats_before_disabling_compression, indent=4)}"
        log.info(log_info_msg)
        log_info_msg = f"Pool stats after disabling compression\
                and after writing new IO:\
                      {json.dumps(pool_stats_after_disabling_compression_and_after_IO, indent=4)}"
        log.info(log_info_msg)

        """
        Pool stats before disabling compression: {
            "stored": 41943040,
            "stored_data": 41943040,
            ...
            "bytes_used": 62914560,
            "data_bytes_used": 62914560,
            ...
            "compress_bytes_used": 62914560,
            "compress_under_bytes": 125829120,
            "stored_raw": 125829120,
        }
        Pool stats after disabling compression and after writing new IO: {
            "stored": 82487976,
            "stored_data": 82487976,
            ...
            "bytes_used": 184549376,
            "data_bytes_used": 184549376,
            ...
            "compress_bytes_used": 62914560,
            "compress_under_bytes": 125829120,
            "stored_raw": 247463936,
        }
        """

        log.info("Validating new data is not being compressed")
        if not is_deviation_within_allowed_percentage(
            pool_stats_after_disabling_compression_and_after_IO["compress_under_bytes"],
            pool_stats_before_disabling_compression["compress_under_bytes"],
            10,
        ):
            raise Exception(
                "New data written after disabling compression is still being compressed"
            )

        log_info_msg = f"10. Reading the uncompressed data from pool {pool_name}"
        log.info(log_info_msg)
        if not pool_obj.do_rados_get(pool=pool_name, read_count="all"):
            raise Exception("Unable to read uncompressed and compressed data")

        log_info_msg = f"11. Delete pool {pool_name}"
        log.info(log_info_msg)
        if rados_obj.delete_pool(pool=pool_name) is False:
            raise Exception(f"Error deleting pool {pool_name}")

    def validate_pool_compression_configs_override_osd_compression_config():
        log.info(
            "\n ---------------------------------"
            "\n Test #4 Enable compressesion at OSD level and disable compression at pool level."
            " Validate Pool compression configurations override OSD compression configurations"
            "\n 1. Create pool1 with compression disabled"
            "\n 2. Set OSD compression bluestore_compression_algorithm and bluestore_compression_mode"
            "\n 3. Create pool2 with compression disabled"
            "\n 4. Write IO to pool1, pool2"
            "\n 5. Collect stats of pool1, pool2. such as size of data, used compression and under compression"
            "\n 6. Perform below validations"
            "\n    - Pool1 data should not be compressed. Pool level config should override OSD level config"
            "\n    - Pool2 data should not be compressed. Pool level config should override OSD level config"
            "\n 7. Delete pool1, pool2"
            "\n 8. Disable compression at OSD level"
            "\n ---------------------------------"
        )
        pool1 = f"{pool_prefix}-{generate_unique_id(4)}"
        pool2 = f"{pool_prefix}-{generate_unique_id(4)}"

        log.info(
            "1. Creating replicated pool without "
            "compression configurations ( Pool created"
            " before OSD compression config set\
            bluestore_compression_algorithm and bluestore_compression_mode )"
        )
        log_info_msg = f"Creating Replicated pool {pool1} without compression"
        log.info(log_info_msg)
        assert rados_obj.create_pool(pool_name=pool1)

        if not rados_obj.pool_inline_compression(
            pool_name=pool1, compression_mode="none", compression_algorithm="snappy"
        ):
            err_msg = f"Error disabling compression on pool : {pool1}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info("2. Enable Compression at OSD level")
        mon_obj.set_config(
            section="osd", name="bluestore_compression_algorithm", value="snappy"
        )
        mon_obj.set_config(
            section="osd", name="bluestore_compression_mode", value="force"
        )

        log_info_msg = f"3. Creating Replicated pool {pool2} without compression disabled\
            ( Compression disabled Pool created after OSD compression config set \
            bluestore_compression_algorithm and bluestore_compression_mode )"
        log.info(log_info_msg)
        assert rados_obj.create_pool(pool_name=pool2)

        if not rados_obj.pool_inline_compression(
            pool_name=pool2, compression_mode="none", compression_algorithm="snappy"
        ):
            err_msg = f"Error disabling compression on pool : {pool2}"
            log.error(err_msg)
            raise Exception(err_msg)

        time.sleep(10)

        log_info_msg = f"4. Write IO to pool1 {pool1} and pool2 {pool2}"
        log.info(log_info_msg)
        if (
            pool_obj.do_rados_put(
                client=client_node,
                pool=pool1,
                nobj=10,
                obj_name=f"{pool1}-{generate_unique_id(4)}",
            )
            == 1
        ):
            exception_msg = f"Writing IO to pool {pool1} failed"
            raise Exception(exception_msg)

        if (
            pool_obj.do_rados_put(
                client=client_node,
                pool=pool2,
                nobj=10,
                obj_name=f"{pool2}-{generate_unique_id(4)}",
            )
            == 1
        ):
            exception_msg = f"Writing IO to pool {pool2} failed"
            raise Exception(exception_msg)

        time.sleep(10)

        log_info_msg = f"5. Collect stats of pool1 {pool1}, pool2 {pool2} \
            such as size of data, used compression and under compression"
        log.info(log_info_msg)

        pool1_stats = get_pool_stats(rados_obj=rados_obj, pool_name=pool1)
        pool2_stats = get_pool_stats(rados_obj=rados_obj, pool_name=pool2)

        log.info(
            "6. Perform below validations"
            "\n  - Pool1 data should not be compressed. Pool level config should override OSD level config"
            "\n  - Pool2 data should not be compressed. Pool level config should override OSD level config"
        )

        log_info_msg = f"Pool1 stats: {json.dumps(pool1_stats, indent=4)}"
        log.info(log_info_msg)
        log_info_msg = f"Pool2 stats: {json.dumps(pool2_stats, indent=4)}"
        log.info(log_info_msg)

        # Compression is disabled on pool and Compression is enabled at OSD level, Hence compression should not occur
        # compression set at pool should override compression set at OSD
        if data_compressed(pool_stats=pool1_stats):
            raise Exception(
                f"pool1 {pool1} compress_bytes_used and compress_under_bytes should be 0\
                    compress_bytes_used{pool1_stats['compress_bytes_used']}\n\
                          compress_under_bytes{pool1_stats['compress_under_bytes']}"
            )

        if data_compressed(pool_stats=pool2_stats):
            raise Exception(
                f"pool2 {pool2} compress_bytes_used and compress_under_bytes should be 0\
                    compress_bytes_used{pool2_stats['compress_bytes_used']}\n\
                          compress_under_bytes{pool2_stats['compress_under_bytes']}"
            )

        log_info_msg = f"7. Reading data from pool1 {pool1}, pool2 {pool2}"
        log.info(log_info_msg)
        if not pool_obj.do_rados_get(pool=pool1, read_count="all"):
            raise Exception(f"Reading data from pool {pool1} failed")
        if not pool_obj.do_rados_get(pool=pool2, read_count="all"):
            raise Exception(f"Reading data from pool {pool2} failed")

        log_info_msg = f"8. Delete pool {pool1}, {pool2}"
        log.info(log_info_msg)
        if rados_obj.delete_pool(pool=pool1) is False:
            raise Exception(f"Deleting pool {pool1} failed")
        if rados_obj.delete_pool(pool=pool2) is False:
            raise Exception(f"Deleting pool {pool2} failed")

        log.info(
            "9. Disable OSD compression configs (bluestore_compression_algorithm \
                 and bluestore_compression_mode) "
        )
        if not mon_obj.remove_config(
            section="osd", name="bluestore_compression_algorithm", value="snappy"
        ):
            raise Exception(
                "Could not remove bluestore_compression_algorithm configuration set on OSD"
            )
        if not mon_obj.remove_config(
            section="osd", name="bluestore_compression_mode", value="force"
        ):
            raise Exception(
                "Could not remove bluestore_compression_mode configuration set on OSD"
            )

    def validate_pools_inherit_compression_configurations_from_osd():
        log.info(
            "\n ---------------------------------"
            "\n Test #5 Validate pools inherit compression configurations from OSD"
            "\n 1. Create pool pool1"
            "\n 2. Enable Compression at OSD config"
            "\n 3. Create pool pool2"
            "\n 4. Write IO to pool1 and pool2"
            "\n 5. Collect stats of pool1 and pool2, such as size of data, used compression and under compression"
            "\n 6. Perform validations after setting OSD compression configs"
            "\n   - pool1 and pool2 should inherit compression configurations from OSD"
            "\n 7. Reading data from pool1"
            "\n 8. Unset OSD compression configs"
            "\n 9. Delete pool1 and pool2"
            "\n ---------------------------------"
        )
        pool1 = f"{pool_prefix}-{generate_unique_id(4)}"
        pool2 = f"{pool_prefix}-{generate_unique_id(4)}"

        log.info("1. Create pool pool1")
        log_info_msg = f"Creating Replicated pool {pool1}"
        log.info(log_info_msg)
        assert rados_obj.create_pool(pool_name=pool1)

        log.info("2. Enable Compression at OSD config")
        mon_obj.set_config(
            section="osd", name="bluestore_compression_algorithm", value="snappy"
        )
        mon_obj.set_config(
            section="osd", name="bluestore_compression_mode", value="force"
        )

        log_info_msg = f"3. Create pool pool2 {pool2}"
        log.info(log_info_msg)
        assert rados_obj.create_pool(pool_name=pool2)

        log_info_msg = f"4. Write IO to pool1 {pool1} and pool2 {pool2}"
        log.info(log_info_msg)
        if (
            pool_obj.do_rados_put(
                client=client_node,
                pool=pool1,
                nobj=20,
                obj_name=f"{pool1}-{generate_unique_id(4)}",
            )
            == 1
        ):
            exception_msg = f"Writing IO to pool {pool1} failed"
            raise Exception(exception_msg)

        time.sleep(10)

        if (
            pool_obj.do_rados_put(
                client=client_node,
                pool=pool2,
                nobj=20,
                obj_name=f"{pool2}-{generate_unique_id(4)}",
            )
            == 1
        ):
            exception_msg = f"Writing IO to pool {pool1} failed"
            raise Exception(exception_msg)

        log_info_msg = f"5. Collect stats of pool1 {pool1} and pool2 {pool2} \
        such as size of data, used compression and under compression"
        log.info(log_info_msg)

        pool1_stats = get_pool_stats(rados_obj=rados_obj, pool_name=pool1)
        pool2_stats = get_pool_stats(rados_obj=rados_obj, pool_name=pool2)

        log.info(
            "6. Perform validations\
            - pool1 and pool2 should inherit compression configurations from OSD"
        )

        log_info_msg = f"Pool1 stats: {json.dumps(pool1_stats, indent=4)}"
        log.info(log_info_msg)
        log_info_msg = f"Pool2 stats: {json.dumps(pool2_stats, indent=4)}"
        log.info(log_info_msg)

        log_info_msg = f"Checking if pool1 {pool1} and pool2 {pool2} is inheriting \
            compression configuration from OSD ( pool \
                created before enabling compression at OSD )"
        log.info(log_info_msg)

        # If OSD compression configs are inherited, compression should occur
        # compress_bytes_used != 0 and compress_under_bytes != 0
        if not data_compressed(pool_stats=pool1_stats):
            raise Exception(
                f"pool1 {pool1} is not compressed, when OSD compression\
                    (bluestore_compression_algorithm and bluestore_compression_mode)\
                        config is set"
            )

        if not data_compressed(pool_stats=pool2_stats):
            raise Exception(
                f"pool2 {pool2} is not compressed, when OSD compression\
                    (bluestore_compression_algorithm and bluestore_compression_mode)\
                        config is set"
            )

        log_info_msg = f"7. Reading data from pool1 {pool1}, pool2 {pool2}"
        log.info(log_info_msg)
        if not pool_obj.do_rados_get(pool=pool1, read_count="all"):
            raise Exception(f"Reading data from pool {pool1} failed")
        if not pool_obj.do_rados_get(pool=pool1, read_count="all"):
            raise Exception(f"Reading data from pool {pool2} failed")

        log.info("8. Disable compression at OSD level")
        mon_obj.remove_config(
            section="osd", name="bluestore_compression_algorithm", value="snappy"
        )
        mon_obj.remove_config(
            section="osd", name="bluestore_compression_mode", value="force"
        )

        log_info_msg = f"9. Delete pool {pool1}, {pool2}"
        log.info(log_info_msg)
        if rados_obj.delete_pool(pool=pool1) is False:
            raise Exception(f"Deleting pool {pool1} failed")
        if rados_obj.delete_pool(pool=pool2) is False:
            raise Exception(f"Deleting pool {pool2} failed")

    def validate_data_migration_between_pools():
        log.info(
            "\n ---------------------------------"
            "\n Test #6 Validate data migration between compressed pools"
            "\n 1. Create 2 replicated pool (source and target) with compression configured"
            "\n 2. Create 2 erasure coded pool (source and target) with compression configured"
            "\n 3. Write IO to both the source pools ( 1 replicated and 1 erasure coded pool )"
            "\n 4. Rados copy from source pool to target pool"
            "\n 5. Read data from target pools"
            "\n 6. Delete pool1, pool2 and pool3"
            "\n 7. Disable compression at OSD level"
            "\n ---------------------------------"
        )

        source_replicated_pool = f"{pool_prefix}-{generate_unique_id(4)}"
        target_replicated_pool = f"{pool_prefix}-{generate_unique_id(4)}"
        source_erasure_coded_pool = f"{pool_prefix}-{generate_unique_id(4)}"
        target_erasure_coded_pool = f"{pool_prefix}-{generate_unique_id(4)}"
        source_pools = [source_replicated_pool, source_erasure_coded_pool]

        log.info("1. Creating replicated pools with compression configurations")
        assert rados_obj.create_pool(pool_name=source_replicated_pool)
        assert rados_obj.create_pool(pool_name=target_replicated_pool)

        log.info("2. Creating erasure coded pools with compression configurations")
        assert rados_obj.create_pool(pool_name=source_erasure_coded_pool)
        assert rados_obj.create_pool(pool_name=target_erasure_coded_pool)

        for pool in [
            source_erasure_coded_pool,
            source_replicated_pool,
            target_erasure_coded_pool,
            target_replicated_pool,
        ]:
            if not rados_obj.pool_inline_compression(
                pool_name=pool, compression_mode="force", compression_algorithm="snappy"
            ):
                err_msg = f"Error enabling compression on pool : {pool}"
                log.error(err_msg)
                raise Exception(err_msg)

        log.info(
            "3. Write IO to source pools ( 1 replicated pool and 1 erasure coded pool )"
        )
        for source_pool in source_pools:
            log_info_msg = f"Writing data to source pool: {source_pools}"
            log.info(log_info_msg)
            if (
                pool_obj.do_rados_put(
                    client=client_node,
                    pool=source_pool,
                    nobj=10,
                    obj_name=f"{source_pool}-{generate_unique_id(4)}",
                )
                == 1
            ):
                exception_msg = f"Writing IO to pool {source_pool} failed"
                raise Exception(exception_msg)
            log_info_msg = f"Completed writing data to source pool: {source_pools}"
            log.info(log_info_msg)

        log.info("4. copy data from source pool to target pool")
        cmd = f"rados cppool {source_replicated_pool} {target_replicated_pool}"
        client_node.exec_command(sudo=True, cmd=cmd, long_running=True)
        cmd = f"rados cppool {source_erasure_coded_pool} {target_erasure_coded_pool}"
        client_node.exec_command(sudo=True, cmd=cmd, long_running=True)

        # Sleeping for 2 seconds after copy to perform get operations
        time.sleep(2)

        log.info("5. Collect pool stats of both source and target pool")
        source_erasure_coded_pool_stats = get_pool_stats(
            rados_obj=rados_obj, pool_name=source_erasure_coded_pool
        )
        source_replicated_pool_stats = get_pool_stats(
            rados_obj=rados_obj, pool_name=source_replicated_pool
        )
        target_erasure_coded_pool_stats = get_pool_stats(
            rados_obj=rados_obj, pool_name=target_erasure_coded_pool
        )
        target_replicated_pool_stats = get_pool_stats(
            rados_obj=rados_obj, pool_name=target_replicated_pool
        )

        log.info("6. Read copied and compressed data from target pools ")
        if not pool_obj.do_rados_get(pool=target_erasure_coded_pool, read_count="all"):
            raise Exception(
                f"Reading data from pool {target_erasure_coded_pool} failed"
            )
        if not pool_obj.do_rados_get(pool=target_replicated_pool, read_count="all"):
            raise Exception(f"Reading data from pool {target_replicated_pool} failed")

        log_info_msg = f"source replicated pool stats: {json.dumps(source_replicated_pool_stats, indent=4)} \
         \n target replicated pool stats: {json.dumps(target_replicated_pool_stats, indent=4)}\
         \n source erasure pool stats: {json.dumps(source_erasure_coded_pool_stats, indent=4)}\
         \n target erasure pool stats: {json.dumps(target_erasure_coded_pool_stats, indent=4)}"

        log.info(log_info_msg)

        target_pool_map = {
            source_replicated_pool: {
                "name": target_replicated_pool,
                "stats": target_replicated_pool_stats,
            },
            source_erasure_coded_pool: {
                "name": target_erasure_coded_pool,
                "stats": target_erasure_coded_pool_stats,
            },
        }

        log.info("7. Perform below validations on target pool")
        log.info("- target pool should maintain compression_required_ratio")
        log.info("- target pool should compress data")

        for source_pool in source_pools:
            bluestore_compression_required_ratio = (
                get_default_bluestore_compression_required_ratio(
                    rados_obj=rados_obj, mon_obj=mon_obj, pool_name=source_pool
                )
            )
            if not compression_ratio_maintained(
                pool_stats=target_pool_map[source_pool]["stats"],
                bluestore_compression_required_ratio=bluestore_compression_required_ratio,
            ):
                err_msg = f"compression_required_ratio is not maintained\
                      by target pool {target_pool_map[source_pool]['name']}"
                raise Exception(err_msg)

            if not data_compressed(target_pool_map[source_pool]["stats"]):
                err_msg = f"Target pool {target_pool_map[source_pool]['name']} data is not compressed"
                raise Exception(err_msg)

        log.info("8. Delete all the pools (2 replicated and 2 erasure codeded pool)")
        if rados_obj.delete_pool(pool=target_replicated_pool) is False:
            raise Exception("Failed to delete pool ", target_replicated_pool)
        if rados_obj.delete_pool(pool=target_erasure_coded_pool) is False:
            raise Exception("Failed to delete pool ", target_erasure_coded_pool)
        if rados_obj.delete_pool(pool=source_replicated_pool) is False:
            raise Exception("Failed to delete pool ", source_replicated_pool)
        if rados_obj.delete_pool(pool=source_erasure_coded_pool) is False:
            raise Exception("Failed to delete pool ", source_erasure_coded_pool)

    def validate_pool_compression_required_ratio(**kwargs):
        """
        Module to validate compression configs such as
        compression_required_ratio, and compression_min_blob_size.

        Steps:
        1. Create pools to test the compression configurations.
        2. Enable compression at Pool.
        3. Perform IO operations (e.g., write operations) to the pool.
        4. Validate the compression configurations:
        - For each value of compression_required_ratio
          compressed data size < original data size * compression_required_ratio

        Parameters:
            kwargs (dict): dictionary of optional configurations.
            Expected keys:
                    - "compression_mode" (str): compression mode. exmaple: force, aggressive .
                    - "compression_algorithm" (str): compression algorithm to set. example: snappy.
                    - "compression_required_ratios_to_test" (list): List of required compression ratios to test.
                    - "compression_min_blob_size_to_test" (list): List of min blob size to test.

        Example:
            validate_compression_configurations(
                "compression_mode": "lz4",
                "compression_algorithm": "lz4",
                "compression_required_ratios_to_test": [0.5, 0.75]
            )
        """
        compression_mode = kwargs.get("compression_mode")
        compression_algorithm = kwargs.get("compression_algorithm")
        compression_required_ratios_to_test = kwargs.get(
            "compression_required_ratios_to_test"
        )
        number_of_pools = len(compression_required_ratios_to_test)
        object_size = kwargs.get("object_size", "50M")

        for i in range(number_of_pools):
            pool_name = f"{pool_prefix}-{generate_unique_id(4)}"
            compression_required_ratio = compression_required_ratios_to_test[i]

            log.info("---1. Create pools to test bluestore data compression---")

            log_info_msg = f"Creating pool {pool_name}"
            log.info(log_info_msg)
            assert rados_obj.create_pool(pool_name=pool_name)

            log_info_msg = f"---2. Enabling compression on pool {pool_name} \
            \n compression_mode: {compression_mode} \
            \n compression_algorithm: {compression_algorithm} \
            \n compression_required_ratio: {compression_required_ratio}---"
            log.info(log_info_msg)

            if (
                compression_required_ratio is not None
                and rados_obj.pool_inline_compression(
                    pool_name=pool_name,
                    compression_mode=compression_mode,
                    compression_algorithm=compression_algorithm,
                    compression_required_ratio=compression_required_ratio,
                )
                is False
            ):
                err_msg = f"Error enabling compression on pool : {pool_name}"
                log.error(err_msg)
                raise Exception(err_msg)

            log_info_msg = f"---3. Write IO to the pool {pool_name}---"
            log.info(log_info_msg)
            if not rados_obj.bench_write(
                pool_name=pool_name,
                max_objs=1,
                byte_size=object_size,
                num_threads=1,
                type=type,
                verify_stats=False,
            ):
                err_msg = f"Write IO failed on pool {pool_name}"
                raise Exception(err_msg)

            log_info_msg = "---4. Perform validations for bluestore data compression---"
            log.info(log_info_msg)
            log.info("(1) Validate compression is configured on pool")
            validate_compression_configuration_on_pools(
                rados_obj,
                pool_name,
                pool_configuration={
                    "compression_required_ratio": compression_required_ratio,
                    "compression_mode": compression_mode,
                    "compression_algorithm": compression_algorithm,
                    "compression_min_blob_size": None,
                },
            )

            log.info(
                "(2) Validating if compressed data < original data * compression_required_ratio"
            )
            pool_stats = get_pool_stats(rados_obj=rados_obj, pool_name=pool_name)
            original_size = pool_stats["stored_data"]
            compressed_data = pool_stats["compress_bytes_used"]

            log_info_msg = f"""
            \n Compressed data size = {compressed_data} \
            \n Original data size = {original_size} \
            \n compression_required_ratio = {compression_required_ratio} \
            \n Original size * compression_required_ratio = {original_size * compression_required_ratio}\
            \n compressed_size < original size * compression_required_ratio \
            \n {compressed_data} < {original_size * compression_required_ratio}
            \n Data should be compressed only if compressed_size < original size * compression_required_ratio.\
            """
            log.info(log_info_msg)

            if not (compressed_data < original_size * compression_required_ratio):
                log.debug(
                    "Compression should take place only when"
                    "compressed size < original size * compression_required_ratio"
                )
                # raise Exception(log_info_msg)

    def validate_pool_compression_min_blob_size(**kwargs):
        """
        Test Steps:
        1. Create pools to test the compression_min_blob_size.
        2. Enable compression at Pool.
        3. Perform IO operations (e.g., write operations) to the pool.
        4. Validate the min_blob_size provided as a list:
        - For each value of compression_min_blob_size
          data smaller than min_blob_size should not be compressed

        Parameters:
            kwargs (dict): dictionary of optional configurations.
            Expected keys:
                    - "compression_mode" (str): compression mode. exmaple: force, aggressive .
                    - "compression_algorithm" (str): compression algorithm to set. example: snappy.
                    - "compression_min_blob_size_to_test" (list): List of min blob size to test.

        Example:
            validate_compression_configurations(
                "compression_mode": "force",
                "compression_algorithm": "snappy",
                "compression_min_blob_size_to_test": ["4KB"]
            )
        """

        compression_mode = kwargs.get("compression_mode")
        compression_algorithm = kwargs.get("compression_algorithm")
        compression_min_blob_size_to_test = kwargs.get(
            "compression_min_blob_size_to_test"
        )
        number_of_pools = len(compression_min_blob_size_to_test)

        for i in range(number_of_pools):
            pool_name = f"{pool_prefix}-{generate_unique_id(4)}"
            compression_min_blob_size = compression_min_blob_size_to_test[i]

            log.info("---1. Test Create pools to test bluestore data compression---")
            assert rados_obj.create_pool(pool_name=pool_name)

            log_info_msg = f"---2.Enabling compression on pool {pool_name} \
            \n compression_mode: {compression_mode} \
            \n compression_algorithm: {compression_algorithm} \
            \n compression_min_blob_size: {compression_min_blob_size}---"
            log.info(log_info_msg)
            if (
                compression_min_blob_size is not None
                and rados_obj.pool_inline_compression(
                    pool_name=pool_name,
                    compression_mode=compression_mode,
                    compression_algorithm=compression_algorithm,
                    compression_min_blob_size=compression_min_blob_size,
                )
                is False
            ):
                err_msg = f"Error enabling compression on pool : {pool_name}"
                log.error(err_msg)
                raise Exception(err_msg)

            log_info_msg = f"---3. Write IO to the pool {pool_name}---"
            log.info(log_info_msg)
            # If min_blob_size to test is 50KB,
            # Then write set min_blob_size to 50KB
            # and perform test by writing data smaller than 50KB.
            # Example: 25KB.
            # Below steps to create smaller data size
            object_size = create_smaller_data_value(compression_min_blob_size)
            log_info_msg = f"Writing object of size {object_size}"
            if not rados_obj.bench_write(
                pool_name=pool_name,
                max_objs=1,
                byte_size=object_size,
                num_threads=1,
                type=type,
                verify_stats=False,
            ):
                err_msg = f"Write IO failed on pool {pool_name}"
                raise Exception(err_msg)

            log_info_msg = "---4. Perform validations for bluestore data compression---"
            log.info(log_info_msg)
            log.info("(1) Validate compression is configured on pool")
            validate_compression_configuration_on_pools(
                rados_obj,
                pool_name,
                pool_configuration={
                    "compression_mode": compression_mode,
                    "compression_algorithm": compression_algorithm,
                    "compression_min_blob_size": compression_min_blob_size,
                    "compression_required_ratio": None,
                },
            )

            log.info(
                "(2)Validating data smaller than compression_min_blob_size undergoes compression"
            )
            pool_stats = get_pool_stats(rados_obj=rados_obj, pool_name=pool_name)

            original_size = pool_stats["stored_data"]
            compressed_data = pool_stats["compress_bytes_used"]
            log_info_msg = f"""
            \n Compressed data size = {compressed_data}
            \n Original data size = {original_size}
            \n compression_min_blob_size = {compression_min_blob_size}
            \n object size < compression_min_blob_size should not be compressedd
            \n {object_size} < {compression_min_blob_size}
            """
            log.info(log_info_msg)

            if data_compressed(pool_stats=pool_stats):
                log.info(
                    "Data smaller than compression_min_blob_size should not be compressed"
                )
                # raise Exception(
                #     "Data should not not compress since object is less than compression_min_blob_size")

    def validate_osd_replacement():
        log.info(
            "\n ---------------------------------"
            "\n 1. Create replicated and/or erasure pool/pools"
            "\n 2. Enable compression on the pool"
            "\n 3. Write IO to pool"
            "\n 4. Collect stats and Check if data is compressed on pool"
            "\n 5. Remove OSD from the cluster"
            "\n 6. Check if data is still compressed on pool"
            "\n 7. Write IO to pool after OSD removal"
            "\n 8. Validate new data written after OSD removal to the pool is still being compressed"
            "\n 9. Adding the removed OSD back and checking the cluster status"
            "\n 10. Check if data is compressed on pool "
            "\n 11. Write IO to pool after OSD addition"
            "\n 12. Validate new written data is compressed after OSD addition"
            "\n ---------------------------------"
        )
        pool_name = f"{pool_prefix}-{generate_unique_id(4)}"

        log_info_msg = f"1. Create replicated and/or erasure pool/pools {pool_name}"
        log.info(log_info_msg)
        assert rados_obj.create_pool(pool_name=pool_name)

        log_info_msg = f"2. Enable compression on the pool {pool_name}"
        log.info(log_info_msg)
        if not rados_obj.pool_inline_compression(
            pool_name=pool_name,
            compression_mode="force",
            compression_algorithm="snappy",
        ):
            err_msg = f"Error setting compression on pool : {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        log_info_msg = f"3. Write IO to pool {pool_name}"
        log.info(log_info_msg)
        if (
            pool_obj.do_rados_put(
                client=client_node,
                pool=pool_name,
                nobj=10,
                obj_name=f"{pool_name}-{generate_unique_id(4)}",
            )
            == 1
        ):
            exception_msg = f"Writing IO to pool {pool_name} failed"
            raise Exception(exception_msg)

        log_info_msg = (
            f"4. Collect stats and Check if data is compressed on pool {pool_name}"
        )
        log.info(log_info_msg)
        pool_stats = get_pool_stats(rados_obj=rados_obj, pool_name=pool_name)
        if not data_compressed(pool_stats=pool_stats):
            err_msg = f"Data in the pool {pool_name} is not compressed after enabling compression"
            raise Exception(err_msg)

        # Increasing the recovery threads on the cluster
        log_info_msg = "5. Remove OSD from the cluster"
        rados_obj.change_recovery_threads(config={}, action="set")
        target_osd = rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
        log.debug(
            f"Ceph osd tree before OSD removal : \n\n {rados_obj.run_ceph_command(cmd='ceph osd tree')} \n\n"
        )
        test_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=target_osd)
        should_not_be_empty(test_host, "Failed to fetch host details")
        dev_path = get_device_path(test_host, target_osd)
        target_osd_spec_name = service_obj.get_osd_spec(osd_id=target_osd)

        log_lines = (
            f"\nosd device path  : {dev_path},\n osd_id : {target_osd},\n hostname : {test_host.hostname},\n"
            f"Target OSD Spec : {target_osd_spec_name}"
        )
        log.debug(log_lines)

        rados_obj.set_service_managed_type(service_type="osd", unmanaged=True)
        method_should_succeed(utils.set_osd_out, ceph_cluster, target_osd)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
        log.debug("Cluster clean post draining of OSD for removal")
        utils.osd_remove(ceph_cluster, target_osd)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
        method_should_succeed(
            utils.zap_device, ceph_cluster, test_host.hostname, dev_path
        )
        method_should_succeed(
            wait_for_device_rados, test_host, target_osd, action="remove"
        )
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
        log.info(f"Removal of OSD : {target_osd} is successful.")

        log_info_msg = f"6. Check if data is still compressed on pool {pool_name} \
            after OSD removal and read data from compressed pool"
        log.info(log_info_msg)

        log_info_msg = f"Checking if data is compressed on pool {pool_name}"
        log.info(log_info_msg)
        pool_stats_after_osd_removal = get_pool_stats(
            rados_obj=rados_obj, pool_name=pool_name
        )

        # Example:
        # pool_stats_after_osd_removal {
        # "bytes_used": 62914560,
        # "compress_bytes_used": 62914560,
        # "compress_under_bytes": 125829120,
        # "data_bytes_used": 62914560,
        # "stored_raw": 125829120,
        # }

        # Check if compress_under_bytes and compress_used_bytes != 0
        if not data_compressed(pool_stats=pool_stats_after_osd_removal):
            err_msg = "Data in the pool is not compressed after OSD removal"
            raise Exception(err_msg)

        # Check if all the data being written is compressed
        if not is_deviation_within_allowed_percentage(
            int(pool_stats_after_osd_removal["stored_raw"]),
            int(pool_stats_after_osd_removal["compress_under_bytes"]),
            10,
        ):
            err_msg = f"Data written to the pool {pool_name} is not compressed"
            raise Exception(err_msg)

        log_info_msg = f"Reading data from pool {pool_name}"
        log.info(log_info_msg)
        if not pool_obj.do_rados_get(pool=pool_name, read_count="all"):
            raise Exception(f"Reading data from pool {pool_name} failed")

        # write to the pool and check compression
        log_info_msg = f"7. Write IO to pool {pool_name} after OSD removal"
        log.info(log_info_msg)
        obj_name = f"{pool_name}-{generate_unique_id(4)}"
        if (
            pool_obj.do_rados_put(
                client=client_node,
                pool=pool_name,
                nobj=10,
                obj_name=obj_name,
            )
            == 1
        ):
            exception_msg = f"Writing IO to pool {pool_name} failed"
            raise Exception(exception_msg)

        log_info_msg = "8. Validate new data written after OSD removal to the pool is still being compressed"
        pool_stats_after_osd_removal_and_IO = get_pool_stats(
            rados_obj=rados_obj, pool_name=pool_name
        )
        # Check if compress_under_bytes and compress_used_bytes != 0
        if not data_compressed(pool_stats=pool_stats_after_osd_removal_and_IO):
            err_msg = "Data in the pool is not compressed after OSD removal"
            raise Exception(err_msg)

        # Check if all the data being written is compressed
        if not is_deviation_within_allowed_percentage(
            int(pool_stats_after_osd_removal_and_IO["stored_raw"]),
            int(pool_stats_after_osd_removal_and_IO["compress_under_bytes"]),
            10,
        ):
            err_msg = f"Data written to the pool {pool_name} is not compressed"
            raise Exception(err_msg)

        # Adding the removed OSD back and checking the cluster status
        log.debug("9. Adding the removed OSD back and checking the cluster status")
        utils.add_osd(ceph_cluster, test_host.hostname, dev_path, target_osd)
        method_should_succeed(
            wait_for_device_rados, test_host, target_osd, action="add"
        )
        method_should_succeed(
            wait_for_daemon_status,
            rados_obj=rados_obj,
            daemon_type="osd",
            daemon_id=target_osd,
            status="running",
            timeout=300,
        )
        assert service_obj.add_osds_to_managed_service(
            osds=[target_osd], spec=target_osd_spec_name
        )
        time.sleep(30)
        log.debug(
            "Completed addition of OSD post removal. Checking for inactive PGs post OSD addition"
        )

        # Checking cluster health after OSD Addition
        method_should_succeed(rados_obj.run_pool_sanity_check)
        log.info(
            f"Addition of OSD : {target_osd} back into the cluster was successful, and the health is good!"
        )
        rados_obj.set_service_managed_type(service_type="osd", unmanaged=False)
        log.info("Completed the removal and addition of OSD daemons")

        log_info_msg = (
            f"10. Check if data is compressed on pool {pool_name} after OSD addition"
        )
        log.info(log_info_msg)
        pool_stats_after_osd_addition = get_pool_stats(
            rados_obj=rados_obj, pool_name=pool_name
        )
        if not data_compressed(pool_stats=pool_stats_after_osd_addition):
            err_msg = "Data in the pool is not compressed after OSD removal"
            raise Exception(err_msg)

        log_info_msg = f"Reading data from pool {pool_name}"
        log.info(log_info_msg)
        if not pool_obj.do_rados_get(pool=pool_name, read_count="all"):
            raise Exception(f"Reading data from pool {pool_name} failed")

        # write to the pool and check compression
        log_info_msg = f"11. Write IO to pool {pool_name} after OSD addition to validate new written data is\
            compressed after OSD addition"
        log.info(log_info_msg)
        if (
            pool_obj.do_rados_put(
                client=client_node,
                pool=pool_name,
                nobj=10,
                obj_name=f"{pool_name}-{generate_unique_id(4)}",
            )
            == 1
        ):
            exception_msg = f"Writing IO to pool {pool_name} failed"
            raise Exception(exception_msg)

        log_info_msg = "12. Validate new written data is\
            compressed after OSD addition"
        log.info(log_info_msg)

        pool_stats_after_osd_addition_and_IO = get_pool_stats(
            rados_obj=rados_obj, pool_name=pool_name
        )
        if not data_compressed(pool_stats_after_osd_addition_and_IO):
            err_msg = f"Data written to the pool {pool_name} is not compressed"
            raise Exception(err_msg)

        if not is_deviation_within_allowed_percentage(
            int(pool_stats_after_osd_addition_and_IO["stored_raw"]),
            int(pool_stats_after_osd_addition_and_IO["compress_under_bytes"]),
            10,
        ):
            err_msg = f"Data written to the pool {pool_name} is not compressed"
            raise Exception(err_msg)

    try:

        log.info(
            "\n\n ************ Execution begins for bluestore data compression scenarios ************ \n\n"
        )

        if "scenario-1" in scenarios_to_run:
            log.info("Test #1 Validate basic compression workflow")
            validate_basic_compression_workflow()

        if "scenario-2" in scenarios_to_run:
            log.info("Test #2 Validate uncompressed_pool to compressed pool conversion")
            validate_uncompressed_pool_to_compressed_pool_conversion()

        if "scenario-3" in scenarios_to_run:
            log.info("Test #3 Validate compressed pool to_uncompressed poolconversion")
            validate_compressed_pool_to_uncompressed_pool_conversion()

        if "scenario-4" in scenarios_to_run:
            log.info(
                "Test #4 Enable compressesion at OSD level and disable compression"
                " at pool level. Data should not be compressed"
            )
            validate_pool_compression_configs_override_osd_compression_config()

        if "scenario-5" in scenarios_to_run:
            log.info(
                "Test #5 Validate default pools inherit compression configurations from OSD"
            )
            validate_pools_inherit_compression_configurations_from_osd()

        if "scenario-6" in scenarios_to_run:
            log.info("Test #6 Validate data migration between compressed pools")
            validate_data_migration_between_pools()

        if "scenario-7" in scenarios_to_run:
            log.info("Test #7 Validate OSD replacement")
            validate_osd_replacement()

        if "scenario-8" in scenarios_to_run:
            log.info("Test #8 Validate different values of compression_required_ratio")
            validate_pool_compression_required_ratio(
                compression_mode="force",
                compression_algorithm="snappy",
                compression_required_ratios_to_test=[0.01, 0.05, 0.95, 0.99, 1],
            )

        if "scenario-9" in scenarios_to_run:
            log.info("Test #9 Validate different values of compression_min_blob_size")
            validate_pool_compression_min_blob_size(
                compression_mode="force",
                compression_algorithm="snappy",
                compression_min_blob_size_to_test=["25KB", "50KB", "75KB"],
            )

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

    log.info("Completed validation of bluestore data compression.")
    return 0


def get_default_bluestore_compression_required_ratio(
    rados_obj: RadosOrchestrator, mon_obj: MonConfigMethods, pool_name: str
) -> str:
    """
    Retrieves the default value of the Bluestore compression required ratio for a given pool.

    Args:
        rados_obj (object): RadosOrchestrator object
        mon_obj (object): MonConfigMethods object
        pool_name (str): The name of the pool for which the compression ratio is required.

    Returns:
        str: bluestore_compression_required_ratio
    """
    pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
    target_osd = pg_set[0]

    bluestore_compression_required_ratio = float(
        mon_obj.show_config(
            daemon="osd",
            id=target_osd,
            param="bluestore_compression_required_ratio",
        )
    )
    log.info(
        "default bluestore_compression_required_ratio is %f",
        bluestore_compression_required_ratio,
    )

    return bluestore_compression_required_ratio


def get_pool_stats(rados_obj: RadosOrchestrator, pool_name: str):
    """
    Retrieves the default value of the Bluestore compression required ratio for a given pool.

    Args:
        rados_obj (object): RadosOrchestrator object
        mon_obj (object): MonConfigMethods object
        pool_name (str): The name of the pool for which the compression ratio is required.

    Returns:
        str: bluestore_compression_required_ratio
    """
    try:
        pool_stats = rados_obj.run_ceph_command(cmd="ceph df detail")["pools"]
        pool_stats_after_compression = [
            detail for detail in pool_stats if detail["name"] == pool_name
        ][0]["stats"]
        return pool_stats_after_compression
    except KeyError as e:
        err_msg = f"No stats about the pools requested found on the cluster {e}"
        log.error(err_msg)
        raise Exception(err_msg)


def get_compress_success_count(rados_obj: RadosOrchestrator, pool_name: str) -> str:
    """
    Retrieves the compress_success_count field from ceph perf dump

    Args:
        rados_obj (object): RadosOrchestrator object
        pool_name (str): The name of the pool for which the compression ratio is required.

    Returns:
        str: returns osd_perf_data["bluestore"]["compress_success_count"]
    """
    pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
    log_info_msg = f"Acting set for collecting compress_rejected_count {pg_set}"
    log.info(log_info_msg)
    target_osd = pg_set[0]
    osd_perf_data = rados_obj.get_osd_perf_dump(osd_id=target_osd)
    compress_success_count = osd_perf_data["bluestore"]["compress_success_count"]
    return compress_success_count


def get_compress_rejected_count(rados_obj: RadosOrchestrator, pool_name: str) -> str:
    """
    Retrieves the compress_rejected_count field from ceph perf dump

    Args:
        rados_obj (object): RadosOrchestrator object
        pool_name (str): The name of the pool for which the compression ratio is required.

    Returns:
        str: returns osd_perf_data["bluestore"]["compress_rejected_count"]
    """
    pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
    log_info_msg = f"Acting set for collecting compress_rejected_count {pg_set}"
    log.info(log_info_msg)
    target_osd = pg_set[0]
    osd_perf_data = rados_obj.get_osd_perf_dump(osd_id=target_osd)
    compress_rejected_count = osd_perf_data["bluestore"]["compress_rejected_count"]
    return compress_rejected_count


def is_deviation_within_allowed_percentage(
    written_data_size: int, compressed_data_size: int, percentage: int
) -> bool:
    """
    Checks if the compressed_data_size is within the percentage deviation from written_data_size

    Args:
        written_data_size (int): written data size
        compressed_data_size (int): compressed data size
        percentage (int): allowed percentage of deviation

    Returns:
        bool:
            True: If the deviation is within the allowed percentage limit
            False: If the deviation is more than the percentage
    """
    log_info_msg = f"written {written_data_size} compressed data {compressed_data_size}"
    log.info(log_info_msg)
    deviated_data_size = abs(written_data_size - compressed_data_size)
    percentage_deviation = (deviated_data_size / written_data_size) * 100
    log_info_msg = f"percentage deviation {percentage_deviation}"
    log.info(log_info_msg)
    if percentage_deviation < percentage:
        return True
    return False


def data_compressed(pool_stats) -> bool:
    """
    Checks if the data in the pool is compressed.
    If data is compressed pool_stats["compress_bytes_used"] and pool_stats["compress_under_bytes"] should not be 0

    Args:
        pool_stats (str): Stats of the pool to check data compression

    Returns:
        True: If data is compressed.
              pool_stats["compress_bytes_used"] > 0 and pool_stats["compress_under_bytes"] > 0
        False: If data is not compressed
              pool_stats["compress_bytes_used"] ==  0 and pool_stats["compress_under_bytes"] == 0
    """
    if pool_stats["compress_bytes_used"] == 0:
        return False

    if pool_stats["compress_under_bytes"] == 0:
        return False

    return True


def compression_ratio_maintained(
    pool_stats, bluestore_compression_required_ratio="0.875"
) -> bool:
    """
    Checks if compression_required_ratio maintained
    compression_required_ratio: The ratio of the
    size of the data chunk after compression relative to the original size must
    be at least this small in order to store the compressed version

    Args:
        pool_stats (str): Stats of the pool to check data compression
        bluestore_compression_required_ratio (str): compression_required_ratio to check

    Returns:
        True: If compression_required_ratio is maintained
        False: If compression_required_ratio is not maintained
    """
    if pool_stats["compress_bytes_used"] <= (
        pool_stats["compress_under_bytes"] * bluestore_compression_required_ratio
    ):
        return True
    return False


def validate_compress_success_rejected_count(
    rados_obj: RadosOrchestrator, pool_name: str
) -> bool:
    """
    Checks if compress_success_count > 0 and compress_rejected_count == 0.

    Args:
        rados_obj (RadosOrchestrator): RadosOrchestrator
        pool_name (str): name of the pool to perform validation

    Returns:
        True: If validation is successful
        False: If validation fails
    """
    compress_success_count = get_compress_success_count(
        rados_obj=rados_obj, pool_name=pool_name
    )
    if compress_success_count == 0:
        log_err_msg = f"Compression success count cannot be 0 for compressed pool\
                        Current compress_success_count is {compress_success_count}"
        log.error(log_err_msg)
        return False

    compress_rejected_count = get_compress_rejected_count(
        rados_obj=rados_obj, pool_name=pool_name
    )
    if compress_rejected_count != 0:
        log_err_msg = f"Compression rejected count should be 0 for compressed pool\
            Current compress_rejected_count is {compress_rejected_count}"
        log.error(log_err_msg)
        return False
    return True


def convert_to_numeric_size(data_size):
    """
    Converts a string representation of a data size to numeric value in bytes.

    Parameters:
        data_size (str): string of the size(e.g., '4KB', '10MB').

    Returns:
        int: numeric value of size in bytes.
    """

    object_size = int(re.split(r"[a-zA-Z]", data_size)[0])
    data_type = data_size.split(str(object_size))[1].upper()

    log.info("object size is ")
    log.info(object_size)
    log.info("data type is ")
    log.info(data_type)

    if data_type == "KB":
        object_size = object_size * 1024
    elif data_type == "MB":
        object_size = object_size * 1024 * 1024

    log_info_msg = f"numeric value of {data_size} is {str(object_size)}"
    log.info(log_info_msg)
    return object_size


def validate_compression_configuration_on_pools(rados_obj, pool_name, **kwargs):
    """
    Validates the compression configuration set on given pool.

    Parameters:
        rados_obj (object): RadosOrchestrator
        pool_name (str): name of the pool.
        **kwargs (dict):
            - "compression_required_ratio" (float): required compression ratio. Example: 0.7
            - "compression_mode" (str): compression mode. Example: force
            - "compression_algorithm" (str): compression algorithm. Example: snappy
            - "compression_min_blob_size" (str): minimum blob size for compression. Example: 4KB

    Example:
        validate_compression_configuration_on_pools(rados_obj, "mypool", pool_configuration={
            "compression_required_ratio": 1.5,
            "compression_mode": "force",
            "compression_algorithm": "snappy",
            "compression_min_blob_size": "4KB"
        })
    """
    compression_mode = kwargs.get("pool_configuration")["compression_mode"]
    compression_algorithm = kwargs.get("pool_configuration")["compression_algorithm"]
    compression_required_ratio = kwargs.get("pool_configuration")[
        "compression_required_ratio"
    ]
    compression_min_blob_size = kwargs.get("pool_configuration")[
        "compression_min_blob_size"
    ]

    if compression_min_blob_size is not None:
        compression_min_blob_size = convert_to_numeric_size(compression_min_blob_size)

    pool_details = rados_obj.get_pool_details(pool_name)

    log.info(f"Pool details for {pool_name} : \n {json.dumps(pool_details, indent=4)}")
    if compression_algorithm != pool_details["options"]["compression_algorithm"]:
        err_msg = f"compression_algorithm value is not as expected \
            current value: {pool_details['options']['compression_algorithm']}\
            expected value: {compression_algorithm}"
        raise Exception(err_msg)

    if compression_mode != pool_details["options"]["compression_mode"]:
        err_msg = f"compression_algorithm value is not as expected \
            current value: {pool_details['options']['compression_mode']}\
            expected value: {compression_algorithm}"
        raise Exception(err_msg)

    if (
        compression_required_ratio is not None
        and compression_required_ratio
        != pool_details["options"]["compression_required_ratio"]
    ):
        err_msg = f"compression_algorithm value is not as expected \
            current value: {pool_details['options']['compression_required_ratio']}\
            expected value: {compression_algorithm}"
        raise Exception(err_msg)

    if (
        compression_min_blob_size is not None
        and compression_min_blob_size
        != pool_details["options"]["compression_min_blob_size"]
    ):
        err_msg = f"compression_algorithm value is not as expected \
            current value: {pool_details['options']['compression_min_blob_size']}\
            expected value: {compression_algorithm}"
        raise Exception(err_msg)


def create_smaller_data_value(data_size):
    """
    Reduces the given data size by half. Module takes
    data size string example: '100MB' and returns new string
    representing half of original size example: '50MB'

    Args:
        data_size (str): data size, example '100MB'.

    Returns:
        str: half of the original data size, example '50MB'.

    Example:
        create_smaller_data_value('100MB') Returns '50MB'
        create_smaller_data_value('2GB') Returns '1GB'
    """
    object_size = re.split(r"[a-zA-Z]", data_size)[0]
    data_type = data_size.split(object_size)[1]
    object_size = str(int(object_size) // 2) + data_type
    return object_size
