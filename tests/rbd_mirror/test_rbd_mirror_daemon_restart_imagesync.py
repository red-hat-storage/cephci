import time

from ceph.parallel import parallel
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """
    Verification of image sync continuation after multiple rbd mirror daemon restarts.
    Args:
        **kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise
    Test case covered -
    CEPH-10468 - Bug 1348940 - Restart of RBD daemon is again initiating full Sync/Copy of an Image
    Pre-requisites -
    Two ceph clusters with rbd mirror configured along with at least 1 client.

    Test Case Flow:
        1. Configure mirorring between two clusters.
        2. induce decoy by adding images and io in background
        3. Reduce concurrent image syncs to delay sync process to be able to verify with focus in
           images under observation.
        4. Initiate resync of images and restart rbd mirorring daemon in between.
        5. Make sure that image sync doesn't start from the begining.
    """
    log.info(
        "Starting RBD mirroring test case - CEPH-10468"
        " - Restart of RBD daemon is again initiating full Sync/Copy of an Image"
    )

    config = kw.get("config")

    if not (config.get("rep_pool_config") or config.get("ec_pool_config")):
        pool_image_config = {
            "rep_pool_config": {
                "mode": "pool",
                "mirrormode": "journal",
                "image": "rbd_image_rep",
                "pool": "rbd_pool_10468",
                "size": "25G",
                "io_total": "10M",
            },
            "ec_pool_config": {
                "pool": "rbd_image_pool_10468",
                "image": "rbd_image_ec",
                "mode": "image",
                "data_pool": "data_pool",
                "mirrormode": "snapshot",
                "size": "25G",
                "io_total": "10M",
            },
        }
    kw["config"].update(pool_image_config)
    mirror_obj = rbd_mirror_config(**kw)

    mirror1 = mirror_obj["rep_rbdmirror"]["mirror1"]
    mirror2 = mirror_obj["rep_rbdmirror"]["mirror2"]

    rep_image_spec = (
        f'{config["rep_pool_config"]["pool"]}/{config["rep_pool_config"]["image"]}'
    )
    ec_image_spec = (
        f'{config["ec_pool_config"]["pool"]}/{config["ec_pool_config"]["image"]}'
    )

    def initiate_resync_and_wait_sync_to_start(img_spec):
        mirror2.resync(img_spec)
        time.sleep(5)
        mirror2.wait_for_status(
            imagespec=img_spec, state_pattern="down+unknown", retry_interval=5
        )

    def verify_resync(img_spec):
        """Observe sync percentage till image goes to up+replying"""
        sync_percent = 0
        while mirror2.mirror_status("image", img_spec, "state") == "up+syncing":
            curr_sync_percent = int(
                mirror2.mirror_status("image", img_spec, "description")[-3:-1]
            )
            if curr_sync_percent >= sync_percent:
                sync_percent = curr_sync_percent
                continue
            log.info(
                f"img_spec current sync percent:{curr_sync_percent} is greater than or eaqual to previous sync_percent"
            )
            if curr_sync_percent < sync_percent:
                log.err("Syncing seemed to be restarted, test case failed")
                return 1
            time.sleep(2)
        return 0

    def restart_daemon_till_sync_completes(img_spec):
        """Until sync of images in img_spec completes and restart count is less than 11,
        restart rbd mirror daemon"""
        restart_count = 0
        while (
            (mirror2.mirror_status("image", img_spec[0], "state") != "up+replaying")
            and (mirror2.mirror_status("image", img_spec[1], "state") != "up+replaying")
            and restart_count < 10
        ):
            log.info(f"Restarting rbd-mirror daemon {restart_count}st/nd/rd/th time")
            mirror2.change_service_state(None, "restart")
            time.sleep(60)
            restart_count += 1

        log.info(
            "Restarted mirror daemon till image sync completed or restarted 10 times during execution, method returns 0"
        )
        return 0

    decoy_imagespec = f'{config["rep_pool_config"]["pool"]}/decoy_'

    def decoy_setup():
        # Set decoy - increase number of images under mirroring and decrease concurrent imagesyncs
        # to get comfortable window for cephci to monitor images under focus for verification
        mirror2.exec_cmd(
            cmd="ceph config set global rbd_mirror_concurrent_image_syncs 1"
        )
        for i in range(1, 10):
            mirror2.create_image(imagespec=decoy_imagespec + f"{i}", size="1G")
            mirror2.image_feature_enable(
                imagespec=decoy_imagespec + f"{i}", image_feature="journaling"
            )

    def generate_background_ios():
        for i in range(1, 3):
            for i in range(1, 10):
                # Since images are being created at mirror2 in decoy_setup()
                # IOs can run only from mirror2
                mirror2.benchwrite(imagespec=decoy_imagespec + f"{i}", io="10M")
            time.sleep(5)
        return 0

    with parallel() as p:
        p.spawn(mirror1.benchwrite, imagespec=rep_image_spec, io="1G")
        p.spawn(mirror1.benchwrite, imagespec=ec_image_spec, io="1G")
        p.spawn(decoy_setup)

    with parallel() as p:
        for img_spec in [rep_image_spec, ec_image_spec]:
            p.spawn(mirror1.benchwrite, imagespec=img_spec, io="100M")
            p.spawn(initiate_resync_and_wait_sync_to_start, img_spec)

        for each_rc in p:
            if each_rc == 1:
                log.error("Initiating resync and waiting for syncing status failed")
                return 1

    with parallel() as p:
        for img_spec in [rep_image_spec, ec_image_spec]:
            p.spawn(verify_resync, img_spec)
        p.spawn(generate_background_ios)
        p.spawn(restart_daemon_till_sync_completes, [rep_image_spec, ec_image_spec])

        for each_rc in p:
            if each_rc == 1:
                log.error("Image resync has restarted in between, Testcase failed")
                return 1

    return 0
