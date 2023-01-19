from ceph.parallel import parallel
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

log = Log(__name__)


def faster_exports(rbd, pool_type, **kw):
    """
    Run rbd export tests
    Args:
        rbd: RBD object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: test data
    """
    pool = kw["config"][pool_type]["pool"]
    image = kw["config"][pool_type]["image"]
    snap = kw["config"][pool_type].get("snap", f"{image}_snap")
    clone = kw["config"][pool_type].get("clone", f"{image}_clone")
    dir_name = rbd.random_string()
    config = kw.get("config")

    rbd.exec_cmd(cmd="mkdir {}".format(dir_name))

    rbd.exec_cmd(
        cmd="rbd bench-write --io-total {} {}/{}".format(
            config.get("io-total"), pool, image
        )
    )
    rbd.exec_cmd(cmd="rbd snap create {}/{}@{}".format(pool, image, snap))
    rbd.exec_cmd(cmd="rbd snap protect {}/{}@{}".format(pool, image, snap))
    rbd.exec_cmd(
        cmd="rbd clone {pool}/{}@{} {pool}/{}".format(image, snap, clone, pool=pool)
    )

    with parallel() as p:
        p.spawn(
            rbd.exec_cmd,
            cmd="rbd bench-write --io-total {} {}/{}".format(
                config.get("io-total"), pool, clone
            ),
        )
        p.spawn(
            rbd.exec_cmd,
            cmd="rbd export {}/{} {}".format(pool, clone, dir_name + "/export9876"),
        )

    with parallel() as p:
        p.spawn(rbd.exec_cmd, cmd="rbd resize -s {} {}/{}".format("20G", pool, image))
        p.spawn(
            rbd.exec_cmd,
            cmd="rbd export {}/{} {}".format(pool, image, dir_name + "/export9877_1"),
        )

    with parallel() as p:
        p.spawn(
            rbd.exec_cmd,
            cmd="rbd resize -s {} --allow-shrink {}/{}".format("8G", pool, image),
        )
        p.spawn(
            rbd.exec_cmd,
            cmd="rbd export {}/{} {}".format(pool, image, dir_name + "/export9877_2"),
        )

    with parallel() as p:
        p.spawn(rbd.exec_cmd, cmd="rbd flatten {}/{}".format(pool, clone))
        p.spawn(
            rbd.exec_cmd,
            cmd="rbd export {}/{} {}".format(pool, image, dir_name + "/export9878"),
        )

    with parallel() as p:
        p.spawn(
            rbd.exec_cmd,
            cmd="rbd export {}/{} {}".format(pool, clone, dir_name + "/export9879"),
        )
        p.spawn(rbd.exec_cmd, cmd="rbd lock add {}/{} lok".format(pool, clone))

    with parallel() as p:
        p.spawn(
            rbd.exec_cmd,
            cmd="rbd export {}/{} {}".format(pool, clone, dir_name + "/export9880_1"),
        )
        p.spawn(rbd.exec_cmd, cmd="rbd resize -s {} {}/{}".format("20G", pool, image))

    with parallel() as p:
        p.spawn(
            rbd.exec_cmd,
            cmd="rbd export {}/{} {}".format(pool, clone, dir_name + "/export9880_2"),
        )
        p.spawn(
            rbd.exec_cmd,
            cmd="rbd resize -s {} --allow-shrink {}/{}".format("8G", pool, image),
        )
    if config.get("cleanup", True):
        rbd.clean_up(dir_name=dir_name, pools=[pool])

    return rbd.flag


def run(**kw):
    """
    Run clone exports on rbd
    Args:
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Test case covered -
    CEPH-9876 - Export clones while read/write operations are happening on the clones
    Pre-requisites :
    1. Cluster must be up and running with capacity to create pool
       (At least with 64 pgs)
    2. We need atleast one client node with ceph-common package,
       conf and keyring files

    Test Case Flow:
    1. Create a pool and an Image, write some data on it
    2. Create snapshot for Image, protect it and create a clone
    3. Export the clone when read/write operations are running on clone
    4. Repeat the above steps for ecpool
    """
    log.info("Running rbd export tests")
    rbd_obj = initial_rbd_config(**kw)
    rc = 1
    if rbd_obj:
        log.info("Executing test on replicated pool")
        rc = faster_exports(rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw)

        if rc:
            return rc

        log.info("Executing test on ec pool")
        rc = faster_exports(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw)

    return rc
