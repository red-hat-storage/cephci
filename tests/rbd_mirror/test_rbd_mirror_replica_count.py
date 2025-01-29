"""Test case covered -
CEPH-9518 - Increase/Reduce replica count of the Primary Pool
while the mirror job is in progress

Pre-requisites :
1. Two Clusters must be up and running to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files on each node.

Test Case Flow:
    1. Create a pool on both clusters.
    2. Create an Image on primary mirror cluster in same pool.
    3. Configure mirroring (peer bootstrap) between two clusters.
    4. Enable pool mode journal based mirroring on the pool respectively.
    5. Start running IOs on the primary image.
    6. While IOs is running decrease the replica count of mirror pool,
        (if rep count is 3 decrease to 2).
    7. IO's should not stop and cluster health status should be healthy
    8. While IOs is running increase the replica count of mirror pool,
        (if rep count is 2 increase to 3).
    9. IO's should not stop and cluster health status should be healthy
    10. check data consistency among mirror clusters.
"""

from ceph.parallel import parallel
from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from tests.rbd_mirror.rbd_mirror_utils import execute_dynamic
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Verification of pool replica size changes by decreasing
    and increasing of mirror pool replica size while IO's are progress.

    Args:
        **kw: test data

    Returns:
        0 - if test case pass
        1 - it test case fails
    """

    try:
        log.info(
            "Starting CEPH-9518 - Increase/Reduce replica count of the Primary Pool "
            "while the mirror job is in progress"
        )
        config = kw.get("config")
        rbd = Rbd(**kw)

        poolname = rbd.random_string() + "_test_pool"
        imagename = rbd.random_string() + "_test_image"
        image_feature = "exclusive-lock,layering,journaling"

        # create pool and image on primary site
        size = config.get("imagesize", "2G")
        if not rbd.create_pool(poolname=poolname):
            log.error(f"Pool creation failed for pool {poolname}")
            return 1
        rbd.create_image(
            pool_name=poolname,
            image_name=imagename,
            size=size,
            image_feature=image_feature,
        )

        # enable journal based mirroring for pool mode
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        imagespec = poolname + "/" + imagename
        mirror2.create_pool(poolname=poolname)
        kw["peer_mode"] = kw.get("peer_mode", "bootstrap")
        kw["rbd_client"] = kw.get("rbd_client", "client.admin")
        mirror1.config_mirror(mirror2, poolname=poolname, mode="pool", **kw)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")

        out = mirror1.exec_cmd(cmd=f"ceph osd pool get {poolname} size", output=True)

        results = {}
        replica_size = int(out.split(":")[1].strip())
        # testing both replica size decrement and increment
        for new_size in [replica_size - 1, replica_size + 1]:
            # executing IO and replica count change in parallel
            with parallel() as p:
                p.spawn(
                    execute_dynamic,
                    mirror1,
                    "benchwrite",
                    results,
                    imagespec=imagespec,
                    io=config.get("io_total", "1G"),
                )
                p.spawn(
                    execute_dynamic,
                    mirror1,
                    "exec_cmd",
                    results,
                    cmd=f"ceph osd pool set {poolname} size {new_size}",
                )

            # Validation for new pool size config
            out = mirror1.exec_cmd(
                cmd=f"ceph osd pool get {poolname} size", output=True
            )
            pool_replica_size = int(out.split(":")[1].strip())
            if pool_replica_size != new_size:
                log.error("Pool replica size not matching to user set size")
                return 1
            log.info(
                f"Pool replica size:{pool_replica_size}, matching to user set size:{new_size}"
            )

            # Validation for IO completion
            if results["benchwrite"] == 0:
                log.info("IO operation is successfull")
            else:
                log.error("IO's got stopped due to change of replica size")
                return 1

            # Validation for ceph health status
            out = mirror1.exec_cmd(cmd="ceph -s", output=True)
            if "HEALTH_ERR" in out:
                log.error("Ceph cluster went error state")
                return 1

        # Validation for mirror site data consistency
        mirror1.check_data(peercluster=mirror2, imagespec=imagespec)
        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
