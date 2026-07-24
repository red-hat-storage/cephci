"""Test case covered -
CEPH-9507 - Create a mirror image, when secondary cluster
doesn't have enough space left to mirror the image copy

Pre-requisites :
1. Two Clusters must be up and running to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files on each node.

Test Case Flow:
    1. Create a pool on both clusters.
    2. Create an Image on primary mirror cluster in same pool.
    3. Configure mirroring (peer bootstrap) between two clusters.
    4. Enable image mode snapshot based mirroring on the pool respectively.
    5. Start running IOs on the primary image.
    6. keep on running IOs till secondary cluster becomes full
    7. verify after secondary becomes full, mirroring should fail
"""

from ceph.parallel import parallel
from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def run(**kw):
    """Verification of mirroring failure,
    when secondary cluster storage is full.

    Args:
        **kw: test data

    Returns:
        0 - if test case pass
        1 - if test case fails
    """

    try:
        log.info(
            "Starting CEPH-9507 - Verification of mirroring failure "
            "when secondary cluster storage is full"
        )
        rbd = Rbd(**kw)
        config = kw.get("config")
        mode = config.get("mode")
        mirrormode = config.get("mirrormode")
        poolname = rbd.random_string() + "_test_pool"
        imagename = rbd.random_string() + "_test_image"

        rbd1, rbd2 = [
            Rbd(**kw, req_cname=cluster_name)
            for cluster_name in kw.get("ceph_cluster_dict").keys()
        ]

        mirror1 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd1"), config
        )
        mirror2 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd2"), config
        )

        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "10G"),
            io_total=config.get("io-total", "5G"),
            mode=mode,
            mirrormode=mirrormode,
            peer_mode=config.get("peer_mode", "bootstrap"),
            rbd_client=config.get("rbd_client", "client.admin"),
            build=config.get("build"),
            **kw,
        )

        mirror1.mirror_snapshot_schedule_add(poolname=poolname, interval="1m")

        # Filling up secondary cluster
        for i in range(0, 10):
            imagename = "test_mirror_image" + mirror2.random_string()
            imagespec = poolname + "/" + imagename
            mirror2.create_image(imagespec=imagespec, size="10G")

        mirror2_image_list = rbd2.list_images(poolname)
        while True:
            try:
                with parallel() as p:
                    for imagename in mirror2_image_list:
                        if imagename.startswith("test_mirror_image"):
                            p.spawn(
                                run_fio,
                                image_name=imagename,
                                pool_name=poolname,
                                size="4GB",
                                client_node=rbd2.ceph_client,
                            )
            except Exception as e:
                log.info(f"An IO error occurred: {e}")

            # Validation for secondary cluster full
            out = mirror2.exec_cmd(cmd="ceph -s", output=True)

            strings_to_check = ["HEALTH_ERR", "full osd", "nearfull osd"]

            # Validation for secondary cluster full
            if any(string in out for string in strings_to_check):
                log.info(f"secondary cluster full, ceph status of secondary {out}")
                break
            else:
                log.debug(out)
                log.info("Ceph status indicates no error. Continuing FIO.")

        # checking status of primary before trying with new mirrored images
        out = mirror1.exec_cmd(cmd="ceph -s", output=True)
        log.info("ceph status of primary {out}")

        # Try enabling mirrored image once secondary is full
        imagename = "test_image" + mirror1.random_string()
        imagespec = poolname + "/" + imagename

        try:
            mirror1.create_image(imagespec=imagespec, size="10G")
            mirror1.enable_mirror_image(poolname, imagename, mirrormode)
            mirror1.mirror_snapshot_schedule_add(
                poolname=poolname, imagename=imagename, interval="1m"
            )
            mirror2.verify_snapshot_schedule(imagespec, interval=1)
        except Exception as e:
            log.info(
                f"An error occurred: {e}. As Expected mirroring gets failed due to secondary cluster full"
            )

        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
