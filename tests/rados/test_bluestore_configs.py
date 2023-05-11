""" Module to verify scenarios related to BlueStore config changes"""
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
     - CEPH-83571646: Checksum Algorithms
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)

    def restart_osd_service():
        osd_services = rados_obj.list_orch_services(service_type="osd")
        for osd_service in osd_services:
            cephadm.shell(args=[f"ceph orch restart {osd_service}"])
        time.sleep(30)

    def create_pool_write_iops(param, pool_type):
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
            cephadm.shell([f"ceph osd pool set {pool_name} csum_type {param}"])
        # rados bench will perform IOPs and also verify the num of objs written
        assert rados_obj.bench_write(pool_name=pool_name, **{"max_objs": 500})
        assert rados_obj.detete_pool(pool=pool_name)

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

        try:
            # verify default checksum value
            out, _ = cephadm.shell(["ceph config get osd bluestore_csum_type"])
            log.info(f"BlueStore OSD default checksum: {out} | Expected: crc32c")
            assert "crc32c" in out

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

                # restart osd services
                restart_osd_service()

                # create pools with given config
                create_pool_write_iops(
                    param=checksum, pool_type="replicated"
                ) if "crc" in checksum else create_pool_write_iops(
                    param=checksum, pool_type="ec"
                )

        except Exception as E:
            log.error(f"Verification failed with exception: {E.__doc__}")
            log.error(E)
            log.exception(E)
            return 1
        finally:
            # reset global checksum config
            assert mon_obj.remove_config(
                **{"section": "osd", "name": "bluestore_csum_type"}
            )

            # restart osd services
            restart_osd_service()
            wait_for_clean_pg_sets(rados_obj, timeout=300, _sleep=10)

        log.info("BlueStore Checksum algorithm verification completed.")
        return 0
