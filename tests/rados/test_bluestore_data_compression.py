"""
Test Module to perform functionalities of bluestore data compression.
Test #1  Validate basic compression workflow
Test #2 uncompressed pool to compressed pool conversion
Test #3 Compressed pool to uncompressed pool conversion
Test #4 Enable compressesion at OSD level and disable compression at pool level
Test #5 Validate pools inherit compression configurations from OSD
Test #6 Validate data migration between compressed pools
"""

import json
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import generate_unique_id

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
        log.info(
            f"Creating Replicated pool {pool_name} without compression configurations"
        )
        assert rados_obj.create_pool(pool_name=pool_name)

        log.info(f"2. Enable compression on the pool {pool_name}")
        if not rados_obj.pool_inline_compression(
            pool_name=pool_name,
            compression_mode="force",
            compression_algorithm="snappy",
        ):
            err_msg = f"Error setting compression on pool : {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(f"3. Write IO to the pool {pool_name}")
        if not rados_obj.bench_write(
            pool_name=pool_name, byte_size="20KB", verify_stats=False, max_objs=100
        ):
            log.error("Failed to write objects into Pool")
            raise Exception("Write IO failed on pool without compression")

        log.info(
            f"4. Collect stats of the pool {pool_name} such as size of data, used compression and under compression"
        )
        try:
            pool_stats = rados_obj.run_ceph_command(cmd="ceph df detail")["pools"]
            pool_stats_after_compression = [
                detail for detail in pool_stats if detail["name"] == pool_name
            ][0]["stats"]
        except KeyError as e:
            err_msg = f"No stats about the pools requested found on the cluster {e}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(
            f"Pool stats of {pool_name} after compression {json.dumps(pool_stats_after_compression)}"
        )
        log.info(f"5. Perform validations on compressed pool {pool_name}")

        # compression enabled, hence compressed data should be less than 87.5% of original data
        if not (
            pool_stats_after_compression["data_bytes_used"]
            < pool_stats_after_compression["stored_raw"] * 0.87500
        ):
            raise Exception(
                f"Pool {pool_name}: compressed_data is not less than 87.5% of original data"
            )

        # compression enabled, hence compress_bytes_used should not be 0
        if pool_stats_after_compression["compress_bytes_used"] == 0:
            raise Exception(f"Pool {pool_name}: compress_bytes_used = 0")

        # compression enabled, hence compress_bytes_used should not be 0
        if pool_stats_after_compression["compress_under_bytes"] == 0:
            raise Exception(f"Pool {pool_name}: compress_under_bytes = 0")

        pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
        log.info(f"Acting set for removal and addition of OSDs {pg_set}")
        target_osd = pg_set[0]

        osd_perf_data = rados_obj.get_osd_perf_dump(osd_id=target_osd)
        compress_success_count = osd_perf_data["bluestore"]["compress_success_count"]
        if compress_success_count == 0:
            raise Exception(
                f"Compression success count cannot be 0 for compressed pool\
                            Current compress_success_count is {compress_success_count} for osd {target_osd}"
            )

        compress_rejected_count = osd_perf_data["bluestore"]["compress_rejected_count"]
        if compress_rejected_count != 0:
            raise Exception(
                f"Compression rejected count should be 0 for compressed pool\
                Current compress_rejected_count is {compress_rejected_count} for osd {target_osd}"
            )

        log.info(
            f"6. Reading the uncompressed and compressed data from pool {pool_name}"
        )
        if rados_obj.bench_read(pool_name=pool_name) is False:
            raise Exception(
                f"Error reading compressed data from compressed pool {pool_name}"
            )

        log.info(f"7. Delete pool {pool_name}")
        if rados_obj.delete_pool(pool=pool_name) is False:
            raise Exception(f"Error deleting pool {pool_name}")

    def validate_uncompressed_pool_to_compressed_pool_conversion():
        log.info(
            "\n ---------------------------------"
            "\n Test #2 uncompressed pool to compressed pool conversion"
            "\n 1. Create pool without compression"
            "\n 2. Write IO to the pool"
            "\n 3. Collect stats of the pool such as size of data, used compression and under compression"
            "\n 4. Enable compression on the pool"
            "\n 5. Write IO to compression enabled pool"
            "\n 6. Collect stats of the pool such as size of data, used compression and under compression"
            "\n 7. Perform validations for compression"
            "\n 8. Delete pool"
            "\n ---------------------------------"
        )
        pool_name = f"{pool_prefix}-{generate_unique_id(4)}"

        log.info("1. Creating pools without compression configurations")
        log.info(
            f"Creating Replicated pool {pool_name} without compression configurations"
        )
        assert rados_obj.create_pool(pool_name=pool_name)

        log.info(f"2. Write IO to the pool {pool_name}")
        if not rados_obj.bench_write(
            pool_name=pool_name, byte_size="20KB", verify_stats=False, max_objs=100
        ):
            log.error("Failed to write objects into Pool")
            raise Exception("Write IO failed on pool without compression")

        log.info(
            f"3. Collect stats of the pool {pool_name} such as size of data, used compression and under compression"
        )
        try:
            pool_stats = rados_obj.run_ceph_command(cmd="ceph df detail")["pools"]
            pool_stats_before_compression = [
                detail for detail in pool_stats if detail["name"] == pool_name
            ][0]["stats"]
        except KeyError as e:
            err_msg = f"No stats about the pools requested found on the cluster {e}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(f"4. Enable compression on the pool {pool_name}")
        if not rados_obj.pool_inline_compression(
            pool_name=pool_name,
            compression_mode="force",
            compression_algorithm="snappy",
        ):
            err_msg = f"Error setting compression on pool : {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(f"5. Write IO to the compression enabled pool {pool_name}")
        if not rados_obj.bench_write(
            pool_name=pool_name, byte_size="20KB", verify_stats=False, max_objs=100
        ):
            log.error("Failed to write objects into Pool")
            raise Exception(f"Write IO failed on compression enabled pool {pool_name}")

        log.info(
            f"6. Collect stats of compression enabled pool {pool_name} such as size of data,"
            " used compression and under compression"
        )
        try:
            pool_stats = rados_obj.run_ceph_command(cmd="ceph df detail")["pools"]
            pool_stats_after_compression = [
                detail for detail in pool_stats if detail["name"] == pool_name
            ][0]["stats"]
        except KeyError as e:
            err_msg = f"No stats about the pools requested found on the cluster {e}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(
            f"Pool stats of {pool_name} before compression {json.dumps(pool_stats_before_compression)}"
        )
        log.info(
            f"Pool stats of {pool_name} after compression {json.dumps(pool_stats_after_compression)}"
        )
        log.info(f"7. Perform validations on compressed pool {pool_name}")

        # compression enabled, hence compressed data should be less than 87.5% of original data
        if not (
            pool_stats_after_compression["data_bytes_used"]
            < pool_stats_before_compression["data_bytes_used"] * 0.87500
        ):
            raise Exception(
                f"Pool {pool_name}: compressed_data is not less than 87.5% of original data"
            )

        # compression enabled, hence compress_bytes_used should not be 0
        if pool_stats_after_compression["compress_bytes_used"] == 0:
            raise Exception(f"Pool {pool_name}: compress_bytes_used = 0")

        # compression enabled, hence compress_bytes_used should not be 0
        if pool_stats_after_compression["compress_under_bytes"] == 0:
            raise Exception(f"Pool {pool_name}: compress_under_bytes = 0")

        # validating compress_success_count > 0 and compress_rejectd_count == 0
        pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
        log.info(f"Acting set for removal and addition of OSDs {pg_set}")
        target_osd = pg_set[0]

        osd_perf_data = rados_obj.get_osd_perf_dump(osd_id=target_osd)
        compress_success_count = osd_perf_data["bluestore"]["compress_success_count"]
        if compress_success_count == 0:
            raise Exception(
                f"Compression success count cannot be 0 for compressed pool\
                            Current compress_success_count is {compress_success_count} for osd {target_osd}"
            )

        compress_rejected_count = osd_perf_data["bluestore"]["compress_rejected_count"]
        if compress_rejected_count != 0:
            raise Exception(
                f"Compression rejected count should be 0 for compressed pool\
                Current compress_rejected_count is {compress_rejected_count} for osd {target_osd}"
            )

        log.info(
            f"8. Reading the uncompressed and compressed data from pool {pool_name}"
        )
        if rados_obj.bench_read(pool_name=pool_name) is False:
            raise Exception(
                f"Error reading compressed data from compressed pool {pool_name}"
            )

        log.info(f"9. Delete pool {pool_name}")
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
            "\n 6. Write IO to uncompressed pool"
            "\n 7. Collect stats of the pool such as size of data, used compression and under compression"
            "\n 8. Perform validations for compression"
            "\n 9. Read and write all data from the pool ( uncompressed, compressed )"
            "\n 10. Delete pool"
            "\n ---------------------------------"
        )

        pool_name = f"{pool_prefix}-{generate_unique_id(4)}"

        log.info("1. Creating replicated pool")
        log.info(f"Creating Replicated pool {pool_name} with compression")
        assert rados_obj.create_pool(pool_name=pool_name)

        log.info(f"2. Enable compression on the pool {pool_name}")
        if not rados_obj.pool_inline_compression(
            pool_name=pool_name,
            compression_mode="force",
            compression_algorithm="snappy",
        ):
            err_msg = f"Error setting compression on pool : {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(f"3. Write IO to the compressed pool {pool_name}")
        if not rados_obj.bench_write(
            pool_name=pool_name, byte_size="20KB", verify_stats=False, max_objs=100
        ):
            log.error("Failed to write objects into compressed Pool")
            raise Exception("Write IO failed on pool with compression")

        log.info(
            f"4. Collect stats of compressed pool {pool_name} such as size of data,"
            " used compression and under compression"
        )
        try:
            pool_stats = rados_obj.run_ceph_command(cmd="ceph df detail")["pools"]
            pool_stats_before_disabling_compression = [
                detail for detail in pool_stats if detail["name"] == pool_name
            ][0]["stats"]
        except KeyError as e:
            err_msg = f"No stats about the pools requested found on the cluster {e}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(f"5. Disable compression on pool {pool_name}")
        if not rados_obj.pool_inline_compression(
            pool_name=pool_name, compression_mode="none"
        ):
            err_msg = f"Error disabling compression on pool : {pool_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(f"6. Write IO to the compression disabled pool {pool_name}")
        if not rados_obj.bench_write(
            pool_name=pool_name, byte_size="20KB", verify_stats=False, max_objs=100
        ):
            log.error("Failed to write objects into compression disabled Pool")
            raise Exception("Write IO failed on compression disabled pool")

        log.info(
            f"7. Collect stats of compression disabled pool {pool_name} "
            "such as size of data, used compression and under compression"
        )
        try:
            pool_stats = rados_obj.run_ceph_command(cmd="ceph df detail")["pools"]
            pool_stats_after_disabling_compression = [
                detail for detail in pool_stats if detail["name"] == pool_name
            ][0]["stats"]
        except KeyError as e:
            err_msg = f"No stats about the pools requested found on the cluster {e}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(f"8. Perform validations on compressed pool {pool_name}")

        log.info(
            f"Pool stats before disabling compression: {pool_stats_before_disabling_compression}"
        )
        log.info(
            f"Pool stats after disabling compression: {pool_stats_after_disabling_compression}"
        )

        if (
            pool_stats_after_disabling_compression["data_bytes_used"]
            == pool_stats_before_disabling_compression["data_bytes_used"]
        ):
            raise Exception(
                f"Data in the pool {pool_name} still being compressed after disabling compression"
            )

        # After disabling compression compress_bytes_used should be 0
        if pool_stats_after_disabling_compression["compress_bytes_used"] != 0:
            raise Exception(
                f"compress_bytes_used should be 0 when compression \
                            is disabled {pool_name}. \
                            Current compress_bytes_used is \
                                {pool_stats_after_disabling_compression['compress_bytes_used']}"
            )

        # After disabling compression compress_under_bytes should be 0
        if pool_stats_after_disabling_compression["compress_under_bytes"] != 0:
            raise Exception(
                f"compress_under_bytes should be 0 when compression\
                is disabled on pool {pool_name}. \
                    Current compress_under_bytes is {pool_stats_after_disabling_compression['compress_under_bytes']}"
            )

        log.info(f"9. Reading the uncompressed data from pool {pool_name}")
        if rados_obj.bench_read(pool_name=pool_name) is False:
            raise Exception(f"Error reading uncompressed data from pool {pool_name}")

        log.info(f"9. Delete pool {pool_name}")
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
        log.info(f"Creating Replicated pool {pool1} without compression")
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

        log.info(
            f"3. Creating Replicated pool {pool2} without compression configurations"
            " ( Pool created after OSD compression config set \
            bluestore_compression_algorithm and bluestore_compression_mode ) "
        )
        assert rados_obj.create_pool(pool_name=pool2)

        if not rados_obj.pool_inline_compression(
            pool_name=pool2, compression_mode="none", compression_algorithm="snappy"
        ):
            err_msg = f"Error disabling compression on pool : {pool2}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(f"4. Write IO to pool1 {pool1} and pool2 {pool2}")
        if not rados_obj.bench_write(
            pool_name=pool1, byte_size="20KB", verify_stats=False, max_objs=100
        ):
            log.error("Failed to write objects into Pool")
            raise Exception("Write IO failed on pool without compression")

        if not rados_obj.bench_write(
            pool_name=pool2, byte_size="20KB", verify_stats=False, max_objs=100
        ):
            log.error("Failed to write objects into Pool")
            raise Exception("Write IO failed on pool without compression")

        log.info(
            f"5. Collect stats of pool1 {pool1}, pool2 {pool2} "
            "such as size of data, used compression and under compression"
        )
        try:
            pool_stats = rados_obj.run_ceph_command(cmd="ceph df detail")["pools"]
            pool1_stats = [detail for detail in pool_stats if detail["name"] == pool1][
                0
            ]["stats"]
            pool2_stats = [detail for detail in pool_stats if detail["name"] == pool2][
                0
            ]["stats"]

        except KeyError as e:
            err_msg = f"No stats about the pools requested found on the cluster {e}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(
            "6. Perform below validations"
            "\n  - Pool1 data should not be compressed. Pool level config should override OSD level config"
            "\n  - Pool2 data should not be compressed. Pool level config should override OSD level config"
        )

        log.info(f"Pool1 stats: {pool1_stats}")
        log.info(f"Pool2 stats: {pool2_stats}")

        # When compression is disabled, compress_bytes_used and compress_under_bytes should be 0
        if (
            pool1_stats["compress_bytes_used"] != 0
            and pool1_stats["compress_under_bytes"] != 0
        ):
            raise Exception(
                f"pool1 {pool1} compress_bytes_used and compress_under_bytes should be 0\
                    compress_bytes_used{pool1_stats['compress_bytes_used']}\n\
                          compress_under_bytes{pool1_stats['compress_under_bytes']}"
            )

        if (
            pool2_stats["compress_bytes_used"] != 0
            and pool2_stats["compress_under_bytes"] != 0
        ):
            raise Exception(
                f"pool2 {pool2} compress_bytes_used and compress_under_bytes should be 0\
                    compress_bytes_used{pool2_stats['compress_bytes_used']}\n\
                          compress_under_bytes{pool2_stats['compress_under_bytes']}"
            )

        log.info(f"8. Reading data from pool1 {pool1}, pool2 {pool2}")
        if rados_obj.bench_read(pool_name=pool1) is False:
            raise Exception(f"Reading data from pool {pool1} failed")
        if rados_obj.bench_read(pool_name=pool2) is False:
            raise Exception(f"Reading data from pool {pool2} failed")

        log.info(f"9. Delete pool {pool1}, {pool2}")
        if rados_obj.delete_pool(pool=pool1) is False:
            raise Exception(f"Deleting pool {pool1} failed")
        if rados_obj.delete_pool(pool=pool2) is False:
            raise Exception(f"Deleting pool {pool2} failed")

        log.info(
            "10. Disable OSD compression configs (bluestore_compression_algorithm \
                 and bluestore_compression_mode) "
        )
        mon_obj.remove_config(
            section="osd", name="bluestore_compression_algorithm", value="snappy"
        )
        mon_obj.remove_config(
            section="osd", name="bluestore_compression_mode", value="force"
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
        log.info(f"Creating Replicated pool {pool1}")
        assert rados_obj.create_pool(pool_name=pool1)

        log.info("2. Enable Compression at OSD config")
        mon_obj.set_config(
            section="osd", name="bluestore_compression_algorithm", value="snappy"
        )
        mon_obj.set_config(
            section="osd", name="bluestore_compression_mode", value="force"
        )

        log.info(f"3. Create pool pool2 {pool2}")
        assert rados_obj.create_pool(pool_name=pool2)

        log.info(f"4. Write IO to pool1 {pool1} and {pool2}")
        if not rados_obj.bench_write(
            pool_name=pool1, byte_size="20KB", verify_stats=False, max_objs=100
        ):
            log.error("Failed to write objects into Pool")
            raise Exception("Write IO failed on pool without compression")

        if not rados_obj.bench_write(
            pool_name=pool2, byte_size="20KB", verify_stats=False, max_objs=100
        ):
            log.error("Failed to write objects into Pool")
            raise Exception("Write IO failed on pool without compression")

        log.info(
            f"5. ColCollect stats of pool1 {pool1} and pool2 {pool2}, "
            "such as size of data, used compression and under compression"
        )
        try:
            pool_stats = rados_obj.run_ceph_command(cmd="ceph df detail")["pools"]
            pool1_stats = [detail for detail in pool_stats if detail["name"] == pool1][
                0
            ]["stats"]
            pool2_stats = [detail for detail in pool_stats if detail["name"] == pool2][
                0
            ]["stats"]

        except KeyError as e:
            err_msg = f"No stats about the pools requested found on the cluster {e}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(
            "6. Perform validations\
            - pool1 and pool2 should inherit compression configurations from OSD"
        )

        log.info(f"Pool1 stats: {pool1_stats}")
        log.info(f"Pool2 stats: {pool2_stats}")

        log.info(
            f"Checking if pool1 {pool1} and pool2 {pool2} is inheriting "
            "compression configuration from OSD ( pool "
            "created before enabling compression at OSD )"
        )

        # If OSD compression configs are inherited, compression should occur
        # compress_bytes_used != 0 and compress_under_bytes != 0
        if (
            pool1_stats["compress_bytes_used"] == 0
            and pool1_stats["compress_under_bytes"] == 0
        ):
            raise Exception(
                f"pool1 {pool1} is not compressed, when OSD compression\
                    bluestore_compression_algorithm and bluestore_compression_mode\
                        config is set"
            )

        if (
            pool2_stats["compress_bytes_used"] == 0
            and pool2_stats["compress_under_bytes"] == 0
        ):
            raise Exception(
                f"pool2 {pool2} is not compressed, when OSD compression\
                    bluestore_compression_algorithm and bluestore_compression_mode\
                        config is set"
            )

        log.info(f"7. Reading data from pool1 {pool1}, pool2 {pool2}")
        if rados_obj.bench_read(pool_name=pool1) is False:
            raise Exception(f"Reading data from pool {pool1} failed")
        if rados_obj.bench_read(pool_name=pool2) is False:
            raise Exception(f"Reading data from pool {pool2} failed")

        log.info("8. Disable compression at OSD level")
        mon_obj.remove_config(
            section="osd", name="bluestore_compression_algorithm", value="snappy"
        )
        mon_obj.remove_config(
            section="osd", name="bluestore_compression_mode", value="force"
        )

        log.info(f"9. Delete pool {pool1}, {pool2}")
        if rados_obj.delete_pool(pool=pool1) is False:
            raise Exception(f"Deleting pool {pool1} failed")
        if rados_obj.delete_pool(pool=pool2) is False:
            raise Exception(f"Deleting pool {pool2} failed")

    def validate_data_migration_between_pools():
        log.info(
            "\n ---------------------------------"
            "\n Test #6 Validate data migration between compressed pools"
            "\n 1. Create 2 replicated pool with compression configured"
            "\n 2. Create 2 erasure coded pool with compression configured"
            "\n 3. Write IO to both the pools"
            "\n 4. Rados copy from one pool to another"
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

        log.info("Creating pools without compression configurations")
        assert rados_obj.create_pool(pool_name=source_replicated_pool)
        assert rados_obj.create_pool(pool_name=target_replicated_pool)
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

        log.info("Write IO to source pools")
        if not rados_obj.bench_write(
            pool_name=source_replicated_pool,
            byte_size="20KB",
            verify_stats=False,
            max_objs=100,
        ):
            log.error("Failed to write objects into Pool")
            raise Exception("Write IO failed on pool without compression")

        if not rados_obj.bench_write(
            pool_name=source_erasure_coded_pool,
            byte_size="20KB",
            verify_stats=False,
            max_objs=100,
        ):
            log.error("Failed to write objects into Pool")
            raise Exception("Write IO failed on pool without compression")

        log.info("copy data from source pool to target pool")
        cmd = f"rados cppool {source_replicated_pool} {target_replicated_pool}"
        client_node.exec_command(sudo=True, cmd=cmd, long_running=True)
        cmd = f"rados cppool {source_erasure_coded_pool} {target_erasure_coded_pool}"
        client_node.exec_command(sudo=True, cmd=cmd, long_running=True)

        # Sleeping for 2 seconds after copy to perform get operations
        time.sleep(2)

        log.debug("Finished writing data into the pool. Checking pool stats")
        try:
            pool_stats = rados_obj.run_ceph_command(cmd="ceph df detail")["pools"]
            source_erasure_coded_pool_stats = [
                detail
                for detail in pool_stats
                if detail["name"] == source_erasure_coded_pool
            ][0]["stats"]
            source_replicated_pool_stats = [
                detail
                for detail in pool_stats
                if detail["name"] == source_replicated_pool
            ][0]["stats"]
            target_erasure_coded_pool_stats = [
                detail
                for detail in pool_stats
                if detail["name"] == target_erasure_coded_pool
            ][0]["stats"]
            target_replicated_pool_stats = [
                detail
                for detail in pool_stats
                if detail["name"] == target_replicated_pool
            ][0]["stats"]
        except KeyError as e:
            err_msg = f"No stats about the pools requested found on the cluster {e}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info("Read data from target pools")
        rados_obj.bench_read(pool_name=target_replicated_pool)
        rados_obj.bench_read(pool_name=target_erasure_coded_pool)

        log.info(f"source replicated pool stats: {source_replicated_pool_stats}")
        log.info(f"target replicated pool stats: {target_replicated_pool_stats}")
        log.info(f"source erasure pool stats: {source_erasure_coded_pool_stats}")
        log.info(f"target erasure pool stats: {target_erasure_coded_pool_stats}")

        target_stats = {
            source_replicated_pool: target_replicated_pool_stats,
            source_erasure_coded_pool: target_erasure_coded_pool_stats,
        }

        source_stats = {
            source_replicated_pool: source_replicated_pool_stats,
            source_erasure_coded_pool: source_erasure_coded_pool_stats,
        }

        log.info("Perform compression related validations")
        for source_pool in source_pools:
            if (
                source_stats[source_pool]["data_bytes_used"]
                != target_stats[source_pool]["data_bytes_used"]
            ):
                raise Exception("source and target data changed after cppool")

            if (
                source_stats[source_pool]["compress_bytes_used"]
                != target_stats[source_pool]["compress_bytes_used"]
            ):
                raise Exception("source and target data changed after cppool")

            if (
                source_stats[source_pool]["compress_under_bytes"]
                != target_stats[source_pool]["compress_under_bytes"]
            ):
                raise Exception("source and target data changed after cppool")

        log.info("Delete pools")
        rados_obj.delete_pool(pool=target_replicated_pool)
        rados_obj.delete_pool(pool=target_erasure_coded_pool)
        rados_obj.delete_pool(pool=target_replicated_pool)
        rados_obj.delete_pool(pool=target_erasure_coded_pool)

    try:

        log.info(
            "\n\n ************ Execution begins for bluestore data compression scenarios ************ \n\n"
        )

        log.info("Test #1 Validate basic compression workflow")
        validate_basic_compression_workflow()

        log.info("Test #2 Validate uncompressed_pool to compressed pool conversion")
        validate_uncompressed_pool_to_compressed_pool_conversion()

        log.info("Test #3 Validate compressed pool to_uncompressed poolconversion")
        validate_compressed_pool_to_uncompressed_pool_conversion()

        log.info(
            "Test #4 Enable compressesion at OSD level and disable compression"
            " at pool level. Data should not be compressed"
        )
        validate_pool_compression_configs_override_osd_compression_config()

        log.info(
            "Test #5 Validate default pools inherit compression configurations from OSD"
        )
        validate_pools_inherit_compression_configurations_from_osd()

        log.info("Test #6 Validate data migration between compressed pools")
        validate_data_migration_between_pools()

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
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
