"""Test module to verify replica-1 non-resilient pool functionalities"""

import time
from copy import deepcopy

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.crushtool_workflows import CrushToolWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rbd.rbd_utils import Rbd
from utility.log import Log
from utility.utils import generate_unique_id

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to verify scenarios related to replica-1
        # CEPH-83575297: replica-1 pool creation and regression
        # CEPH-83582009: EIO flag positive checks
        # CEPH-83582010: EIO flag negative checks
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    crush_obj = CrushToolWorkflows(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    rbd_obj = Rbd(**kw)
    repli_pools = []
    zone_unique = generate_unique_id(length=4)

    def set_eio_flag(_pool: str, val: str):
        log.info(f"Setting eio flag for pool {_pool} to {val}")
        assert rados_obj.set_pool_property(pool=_pool, props="eio", value=val)
        time.sleep(5)
        _pool_prop = rados_obj.get_pool_property(pool=_pool, props="eio")
        _eio_flag = _pool_prop["eio"]

        log.info(f"EIO flag for pool {_pool} is {_eio_flag} | Expected - {val}")
        return _eio_flag

    try:
        if config.get("replica-1"):
            doc_text = """
            # CEPH-83575297
            1. Deploy ceph cluster with one OSD per host
            2. Remove the crush device for all OSDs
            3. Set unique device class for few OSDs and replicated device
            class for all others.
            4. Divide the hosts into different zones
            5. Create unique crush rule for each unique device class
            6. Set MON Config mon_allow_pool_size_one to true
            7. Create replia-1 pool for each zone device class OSD
            8. Ensure proper health warning is generated.
            9. Perform scrubbing, deep-scrubbing and repair on replica-1 pools
            10. Create RBD images on replica-1 pool
            11. Enable compression of replica-1 pool
            """
            log.info(doc_text)
            log.info("Running test case to verify replica-1 non-resilient pool")

            # Fetch all the OSDs on the cluster
            osd_list = rados_obj.get_osd_list(status="up")
            log.debug(f"List of OSDs: {osd_list}")

            # remove the device class for all the OSDs
            assert crush_obj.remove_device_class(osd_list=osd_list)

            # divide OSDs into different device classes(zones) and their respective
            # hosts into unique zones
            for osd_id in osd_list:
                if int(osd_id) % 2 == 0:
                    zone_name = f"zone{osd_id}-{zone_unique}"
                    cephadm.shell(
                        [f"ceph osd crush set-device-class zone{osd_id} {osd_id}"]
                    )
                    cephadm.shell([f"ceph osd crush add-bucket {zone_name} zone"])
                    cephadm.shell([f"ceph osd crush move {zone_name} root=default"])
                    osd_node = rados_obj.fetch_host_node(
                        daemon_type="osd", daemon_id=osd_id
                    )
                    cephadm.shell(
                        [f"ceph osd crush move {osd_node.hostname} zone={zone_name}"]
                    )

            # divide OSDs into different device classes(replicated)
            replicated_osd = " ".join(
                [str(osd_id) for osd_id in osd_list if int(osd_id) % 2 != 0]
            )
            cephadm.shell(
                [f"ceph osd crush set-device-class replicated {replicated_osd}"]
            )

            # generate replicated crush rule to append cluster crush rule
            rule_name = "general-replicated"
            zone_rules = """        id 111
            type replicated
            step take default class replicated
            step chooseleaf firstn 0 type host
            step emit"""
            assert crush_obj.add_crush_rule(rule_name=rule_name, rules=zone_rules)

            # generate zone level crush rules to append cluster crush rule
            for osd_id in osd_list:
                if int(osd_id) % 2 == 0:
                    rule_name = f"zone{osd_id}"
                    zone_rules = f"""        id 5{osd_id}
                    type replicated
                    step take default class zone{osd_id}
                    step chooseleaf firstn 0 type host
                    step emit"""
                    assert crush_obj.add_crush_rule(
                        rule_name=rule_name, rules=zone_rules
                    )

            # set general replicated crush rule to all existing pools
            pool_list = rados_obj.list_pools()
            log.debug(f"List of pools in the cluster: {pool_list}")
            for pool in pool_list:
                rados_obj.set_pool_property(
                    pool=pool, props="crush_rule", value="general-replicated"
                )

            # set mon mon_allow_pool_size_one true
            assert mon_obj.set_config(
                section="mon", name="mon_allow_pool_size_one", value=True
            )

            # create replica-1 pools for each zone device class osd
            for osd_id in osd_list:
                if int(osd_id) % 2 == 0:
                    pool_cfg = {
                        "pool_name": f"replica1_zone{osd_id}",
                        "pg_num": 1,
                        "pgp_num": 1,
                        "crush_rule": f"zone{osd_id}",
                        "disable_pg_autoscale": True,
                    }
                    assert rados_obj.create_pool(**pool_cfg)

                    # set pool size to 1
                    out, _ = cephadm.shell(
                        [
                            f"ceph osd pool set {pool_cfg['pool_name']} size 1 --yes-i-really-mean-it"
                        ]
                    )
                    log.info(out)
                    repli_pools.append(pool_cfg["pool_name"])

            # check ceph status to confirm replica pools have been created.
            health_detail, _ = cephadm.shell(["ceph health detail"])
            assert "pool(s) have no replicas configured" in health_detail

            # print the pool detail for each replica-1 pool
            for _pool in repli_pools:
                pool_detail = rados_obj.get_pool_details(pool=_pool)
                log.info(f"{_pool} Pool details: ")
                log.info(pool_detail)

            # perform scrub, deep-scrub and repairs on the replica1 pool pg
            for _pool in repli_pools:
                pg_id = rados_obj.get_pgid(pool_name=_pool)[0]
                # trigger repair
                cephadm.shell([f"ceph pg repair {pg_id}"])
                rados_obj.run_scrub(pool=_pool)
                rados_obj.run_deep_scrub(pool=_pool)

            # ensure acting set of each replica1 pool has only one OSD
            for _pool in repli_pools:
                pg_acting_set = rados_obj.get_pg_acting_set(pool_name=_pool)
                assert len(pg_acting_set) == 1
                log.info(f"Acting set of {_pool}: {pg_acting_set}")

            # create and map rbd images
            for _pool in repli_pools:
                if rbd_obj.create_image(
                    pool_name=_pool, image_name=f"img-{_pool}", size="4M"
                ):
                    raise Exception(f"RBD image creation failed on pool: {_pool}")
                log.info(f"RBD image creation success on pool: {_pool}")

            # enable compression and compression ratios for each replica 1 pool
            compress_cfg = {
                "compression_algorithm": "snappy",
                "compression_mode": "force",
                "compression_required_ratio": 0.5,
            }
            for _pool in repli_pools:
                assert rados_obj.pool_inline_compression(
                    pool_name=_pool, **compress_cfg
                )

            log.info("replica-1 pool tests completed successfully")

        if config.get("eio"):
            doc_text = """
                # CEPH-83582009 | CEPH-83582010
                1. On a cluster with replica-1 pools configured
                2. Perform sanity check for EIO flag
                3. Trigger IOs using rados bench and set EIO flag during active IOPS
                4. With EIO flag already set on a pool, try writing data to it
            """
            log.info(doc_text)

            log.info(
                "Running test case to verify EIO flag on replica-1 non-resilient pool"
            )

            pool_list = rados_obj.list_pools()
            repli_pools = [pool for pool in pool_list if "replica1" in pool]
            repli_pools_org = deepcopy(repli_pools)
            log.info(f"List of replica-1 pools on the cluster: {repli_pools}")

            # choose a replica-1 pool to do sanity checks
            pool_name = repli_pools.pop()
            log.info(
                f"Pool on which EIO flag sanity test will be performed: {pool_name}"
            )

            # fetch default value of eio flag for the chosen pool
            pool_prop = rados_obj.get_pool_property(pool=pool_name, props="eio")
            eio_flag = pool_prop["eio"]

            # default value of eio flag should be false
            # {"pool":"replica1_zone4","pool_id":10,"eio":false}
            if eio_flag:
                log.error(
                    f"Default value of EIO flag for replica-1 pool {pool_name} expected to be false"
                )
                log.error(f"Actual value: {eio_flag}")
                raise AssertionError(
                    f"Expected eio flag to be false, actual value: {eio_flag}"
                )

            log.info(f"Default value of EIO flag has been verified: {eio_flag}")

            # positive check | True
            eio_flag = set_eio_flag(_pool=pool_name, val="true")
            assert eio_flag is True

            # positive check | False
            eio_flag = set_eio_flag(_pool=pool_name, val="false")
            assert eio_flag is False

            # positive check | 1
            eio_flag = set_eio_flag(_pool=pool_name, val="1")
            assert eio_flag is True

            # positive check | 0
            eio_flag = set_eio_flag(_pool=pool_name, val="0")
            assert eio_flag is False

            # invalid input check
            for value in ["111", "text", "-1"]:
                try:
                    cmd = f"ceph osd pool set {pool_name} eio {value}"
                    out, _ = cephadm.shell(args=[cmd])
                except Exception as err:
                    log.info(f"Expected to fail: {err}")
                    assert "expecting value" in str(err)

            log.info(
                f"Sanity checks for EIO flag on pool {pool_name} have been completed"
            )

            # choose a replica-1 pool to do in flight IOPs check
            pool_name = repli_pools.pop()
            log.info(f"Pool on which in-flight IOs test will be performed: {pool_name}")

            # start ios using rados bench
            rados_obj.bench_write(pool_name=pool_name, background=True)
            time.sleep(5)

            # ensure ios have started on the pool
            pool_stat = pool_obj.fetch_pool_stats(pool=pool_name)
            log.info(f"Pool stats after IOs have started: {pool_stat}")
            if not pool_stat["client_io_rate"]:
                log.error(f"Expected to have client IOs on pool {pool_name}")
                raise Exception(f"Expected to have client IOs on pool {pool_name}")

            log.info(
                f"Client IOs started successfully on {pool_name}, proceeding to set EIO flag"
            )

            eio_flag = set_eio_flag(_pool=pool_name, val="true")
            assert eio_flag is True
            time.sleep(10)

            # ensure in-flight ios have halted on the pool
            pool_stat = pool_obj.fetch_pool_stats(pool=pool_name)
            log.info(
                f"Pool stats after EIO flag during acting background IOs: {pool_stat}"
            )
            if pool_stat["client_io_rate"]:
                log.error(f"Expected client IOs to halt on pool {pool_name}")
                raise Exception(f"Expected client IOs to halt on pool {pool_name}")

            log.info(
                f"In-flight IOs stopped once EIO flag was set on the pool {pool_name}"
            )

            # the recovery of IOs cannot be verified with rados bench, as the process
            # stops as soon as it receives the EIO signal
            # Future scope: Figure out an IO tool with which IO recovery can be verified
            if False:
                log.info("proceeding to unset EIO flag to resume IOs after 15 secs")
                time.sleep(15)

                eio_flag = set_eio_flag(_pool=pool_name, val="false")
                assert eio_flag is False
                time.sleep(5)

                # ensure ios have resumed on the pool
                pool_stat = pool_obj.fetch_pool_stats(pool=pool_name)
                log.info(f"Pool stats after EIO flag is removed: {pool_stat}")
                if not pool_stat["client_io_rate"]:
                    log.error(f"Expected to have client IOs resume on pool {pool_name}")
                    raise Exception(
                        f"Expected to have client IOs resume on pool {pool_name}"
                    )

                log.info(
                    f"Client IOs resumed successfully on {pool_name}, proceeding to set EIO flag"
                )

            # Perform IOs on a pool on which EIO flag is already set
            log.info("Running client IOs on a pool with active EIO flag")
            log.info(f"Pool on which EIO flag is already set: {pool_name}")

            if rados_obj.bench_write(pool_name=pool_name):
                log.error(f"Expected rados bench to fail on pool {pool_name}")
                raise Exception(f"Expected rados bench to fail on pool {pool_name}")

            log.info(f"Client IOs could not run on pool {pool_name} as expected")
            log.info("All relevant tests around EIO have been verified")
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info("************* Execution of finally block starts ***********")
        if config.get("eio"):
            # unset eio flag on all replica-1 pools
            for pool in repli_pools_org:
                set_eio_flag(_pool=pool, val="false")
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    return 0
