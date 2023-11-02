from ceph.rbd.utils import getdict
from ceph.rbd.workflows.pool import create_ecpool, create_pool
from utility.log import Log

log = Log(__name__)


def create_snap_and_clone(rbd, snap_spec, clone_spec):
    """_summary_

    Args:
        snap_spec (_type_): _description_
        clone_spec (_type_): _description_
    """
    snap_config = {"snap-spec": snap_spec}

    out, err = rbd.snap.create(**snap_config)
    if out or err and "100% complete" not in err:
        log.error(f"Snapshot creation failed for {snap_spec}")
        return 1

    out, err = rbd.snap.protect(**snap_config)
    if out or err:
        log.error(f"Snapshot protect failed for {snap_spec}")
        return 1

    clone_config = {"source-snap-spec": snap_spec, "dest-image-spec": clone_spec}

    out, err = rbd.clone(**clone_config)
    if out or err:
        log.error(f"Clone creation failed for {clone_spec}")
        return 1

    return 0


def config_rbd_multi_pool(
    rbd,
    multi_pool_config,
    is_ec_pool,
    ceph_version,
    config,
    client,
    is_secondary=False,
    cluster="ceph",
):
    """ """
    # If any pool level test config is present, pop it out
    # so that it does not get mistaken as another image configuration
    pool_test_config = multi_pool_config.pop("test_config", None)

    for pool, pool_config in multi_pool_config.items():
        if create_pool(
            pool=pool,
            pg_num=pool_config.get("pg_num", 64),
            pgp_num=pool_config.get("pgp_num", 64),
            client=client,
            cluster=cluster,
        ):
            log.error(f"Pool creation failed for pool {pool}")
            return 1

        if ceph_version >= 3:
            pool_init_conf = {"pool-name": pool, "cluster": cluster}
            rbd.pool.init(**pool_init_conf)

        if is_ec_pool and create_ecpool(
            pool=pool_config.get("data_pool"),
            k_m=pool_config.get("ec-pool-k-m"),
            profile=pool_config.get("ec_profile", "use_default"),
            pg_num=pool_config.get("ec_pg_num", 16),
            pgp_num=pool_config.get("ec_pgp_num", 16),
            failure_domain=pool_config.get("failure_domain", ""),
            client=client,
            cluster=cluster,
        ):
            log.error(f"EC Pool creation failed for {pool_config.get('data_pool')}")
            return 1

        if is_ec_pool and ceph_version >= 3:
            pool_init_conf = {
                "pool-name": pool_config.get("data_pool"),
                "cluster": cluster,
            }
            rbd.pool.init(**pool_init_conf)

        multi_image_config = getdict(pool_config)
        image_config = {
            k: v
            for k, v in multi_image_config.items()
            if v.get("is_secondary", False) == is_secondary
        }

        for image, image_config_val in image_config.items():
            if not config.get("do_not_create_image"):
                create_config = {"pool": pool, "image": image}
                if is_ec_pool:
                    create_config.update({"data-pool": pool_config.get("data_pool")})
                # Update any other specific arguments that rbd create command takes if passed in config
                create_config.update(
                    {
                        k: v
                        for k, v in image_config_val.items()
                        if k not in ["io_total", "test_config", "is_secondary"]
                    }
                )
                create_config.update({"cluster": cluster})
                out, err = rbd.create(**create_config)
                if out or err:
                    log.error(f"Image {image} creation failed with err {out} {err}")
                    return 1

    # Add back the popped pool test config once configuration is complete
    if pool_test_config:
        multi_pool_config["test_config"] = pool_test_config
    return 0
