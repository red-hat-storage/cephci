"""Module to verify scenarios related to BlueStore config changes"""

import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Common test module to verify BlueStore config changes
    and functionalities.
    Currently, covers the following tests:
     - CEPH-83571646: BlueStore Checksum Algorithms
     - CEPH-83571675: BlueStore cache size tuning
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    checksum = "crc32c"

    def restart_osd_service():
        osd_services = rados_obj.list_orch_services(service_type="osd")
        for osd_service in osd_services:
            cephadm.shell(args=[f"ceph orch restart {osd_service}"])
        time.sleep(30)

    def create_pool_write_iops(param, pool_type):
        try:
            pool_name = f"{pool_type}_pool_{param}"
            assert (
                rados_obj.create_pool(pool_name=pool_name)
                if "repli" in pool_type
                else rados_obj.create_erasure_pool(
                    name=pool_name, **{"pool_name": pool_name}
                )
            )
            if param == checksum:
                # set checksum value for the pool
                rados_obj.set_pool_property(
                    pool=pool_name, props="csum_type", value=param
                )
                # verify checksum value for the pool
                assert (
                    param
                    == rados_obj.get_pool_property(pool=pool_name, props="csum_type")[
                        "csum_type"
                    ]
                )
            # rados bench will perform IOPs and also verify the num of objs written
            assert rados_obj.bench_write(
                pool_name=pool_name, **{"max_objs": 500, "verify_stats": False}
            )
        except Exception:
            raise
        finally:
            assert rados_obj.delete_pool(pool=pool_name)

    def modify_cache_size(factor):
        cache_value = int(1073741824 * factor)
        cache_cfg = {
            "section": "osd",
            "name": "bluestore_cache_size_hdd",
            "value": cache_value,
        }
        assert mon_obj.set_config(**cache_cfg)
        out = mon_obj.get_config(section="osd", param="bluestore_cache_size_hdd")
        log.info(
            f"bluestore_cache_size_hdd modified value - {out} | Expected {cache_value}"
        )
        assert int(out.strip("\n")) == cache_value

        cache_value = int(3221225472 * factor)
        cache_cfg = {
            "section": "osd",
            "name": "bluestore_cache_size_ssd",
            "value": cache_value,
        }
        assert mon_obj.set_config(**cache_cfg)
        out = mon_obj.get_config(section="osd", param="bluestore_cache_size_ssd")
        log.info(
            f"bluestore_cache_size_ssd modified value - {out} | Expected {cache_value}"
        )
        assert int(out.strip("\n")) == cache_value

    if config.get("checksums"):
        doc = (
            "\n #CEPH-83571646"
            "\n\t Apply all the applicable different checksum algorithms on pools backed by bluestore"
            "\n\t\t Valid algos: none, crc32c, crc32c_16, crc32c_8, xxhash32, xxhash64"
            "\n\t 1. Create individual replicated pools for each checksum"
            "\n\t 2. Verify the default checksum algorithm is crc32c"
            "\n\t 3. Set different checksum algorithm as global and for each pool"
            "\n\t 4. Verify the checksum algo being set correctly"
            "\n\t 5. Write data to each pool using rados bench"
            "\n\t 6. cleanup - Remove all the pools created"
        )
        log.info(doc)
        log.info("Running test case to verify BlueStore checksum algorithms")
        checksum_list = config.get("checksums")
        pool_type = config.get("pool_type", "erasure")

        try:
            # verify default checksum value
            out, _ = cephadm.shell(["ceph config get osd bluestore_csum_type"])
            log.info(f"BlueStore OSD default checksum: {out} | Expected: crc32c")
            assert "crc32c" in out

            for checksum in checksum_list:
                # create pools with given config when OSD csum_type is default crc32c
                (
                    create_pool_write_iops(param=checksum, pool_type="replicated")
                    if pool_type == "replicated"
                    else create_pool_write_iops(param=checksum, pool_type="ec")
                )

            for checksum in checksum_list:
                # set the global checksum value
                cfg = {
                    "section": "osd",
                    "name": "bluestore_csum_type",
                    "value": checksum,
                }
                assert mon_obj.set_config(**cfg)

                # verify the newly set global checksum value
                out = mon_obj.get_config(section="osd", param="bluestore_csum_type")
                assert checksum in out
                log.info(f"global checksum set verified - {out}")

                # create pools with given config when OSD csum_type is varied
                (
                    create_pool_write_iops(param=checksum, pool_type="replicated")
                    if pool_type == "replicated"
                    else create_pool_write_iops(param=checksum, pool_type="ec")
                )
        except Exception as E:
            log.error(f"Verification failed with exception: {E.__doc__}")
            log.error(E)
            log.exception(E)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # reset global checksum config
            assert mon_obj.remove_config(
                **{"section": "osd", "name": "bluestore_csum_type"}
            )
            # Commenting the osd service restarts until bug fix : https://bugzilla.redhat.com/show_bug.cgi?id=2279839
            # restart osd services
            # restart_osd_service()
            wait_for_clean_pg_sets(rados_obj, timeout=300, sleep_interval=10)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("BlueStore Checksum algorithm verification completed.")
        return 0

    if config.get("bluestore_cache"):
        doc = (
            "\n #CEPH-83571675"
            "\n\t Verify BlueStore cache default values."
            "\n\t Tune cache parameters and perform IOPS"
            "\n\t 1. Verify the default value for - bluestore_cache_size(0)"
            " | bluestore_cache_size_hdd (1GB) | bluestore_cache_size_ssd (3GB)"
            "\n\t 2. Modify the value of bluestore_cache_size_ssd and bluestore_cache_size_hdd"
            "\n\t 3. Verify the values being reflected in ceph config"
            "\n\t 4. Create replicated and ec pool and perform IOPS"
            "\n\t 5. cleanup - Remove all the pools created and reset configs modified"
        )
        log.info(doc)
        log.info("Running test case to verify BlueStore Cache size tuning")
        pool_type = config.get("pool_type", None)

        try:
            # verify default value for bluestore cache
            out = mon_obj.get_config(section="osd", param="bluestore_cache_size")
            log.info(f"bluestore_cache_size default value - {out} | Expected 0")
            assert int(out.strip("\n")) == 0

            out = mon_obj.get_config(section="osd", param="bluestore_cache_size_hdd")
            log.info(
                f"bluestore_cache_size_hdd default value - {out} | Expected 1073741824"
            )
            assert int(out.strip("\n")) == 1073741824

            out = mon_obj.get_config(section="osd", param="bluestore_cache_size_ssd")
            log.info(
                f"bluestore_cache_size_ssd default value - {out} | Expected 3221225472"
            )
            assert int(out.strip("\n")) == 3221225472

            # modify ssd and hdd cache (increase)
            modify_cache_size(factor=1.5)

            # restart osd services
            restart_osd_service()

            # perform iops
            create_pool_write_iops(param="cache_inc", pool_type="replicated")
            if pool_type != "replicated":
                create_pool_write_iops(param="cache_inc", pool_type="ec")

            # modify ssd and hdd cache (decrease)
            modify_cache_size(factor=0.7)

            # restart osd services
            restart_osd_service()

            # perform iops
            create_pool_write_iops(param="cache_dec", pool_type="replicated")
            if pool_type != "replicated":
                create_pool_write_iops(param="cache_dec", pool_type="ec")

        except Exception as E:
            log.error(f"Verification failed with exception: {E.__doc__}")
            log.error(E)
            log.exception(E)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # reset modified cache configs
            mon_obj.remove_config(
                **{"section": "osd", "name": "bluestore_cache_size_hdd"}
            )
            mon_obj.remove_config(
                **{"section": "osd", "name": "bluestore_cache_size_ssd"}
            )
            # restart osd services
            restart_osd_service()
            wait_for_clean_pg_sets(rados_obj, timeout=300, sleep_interval=10)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("BlueStore cache size tuning verification completed.")
        return 0
