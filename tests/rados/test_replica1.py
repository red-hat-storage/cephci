""" Test module to verify replica-1 non-resilient pool functionalities"""

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.crushtool_workflows import CrushToolWorkflows
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rbd.rbd_utils import Rbd
from utility.log import Log
from utility.utils import generate_unique_id

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
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
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    crush_obj = CrushToolWorkflows(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    rbd_obj = Rbd(**kw)
    repli_pools = []
    zone_unique = generate_unique_id(length=4)

    log.info("Running test case to verify replica-1 non-resilient pool")

    try:
        # Fetch all the OSDs on the cluster
        out, _ = cephadm.shell(args=["ceph osd ls"])
        osd_list = out.strip().split("\n")
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
            [osd_id for osd_id in osd_list if int(osd_id) % 2 != 0]
        )
        cephadm.shell([f"ceph osd crush set-device-class replicated {replicated_osd}"])

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
                assert crush_obj.add_crush_rule(rule_name=rule_name, rules=zone_rules)

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
            assert rados_obj.pool_inline_compression(pool_name=_pool, **compress_cfg)
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    return 0
