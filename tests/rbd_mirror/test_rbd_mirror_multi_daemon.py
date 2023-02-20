import time

from ceph.parallel import parallel
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """
    Multiple rbd-mirroring daemons per cluster - configuration and verification of daemon failure
    Args:
        **kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise
    Test case covered -
    CEPH-83574962 - Multiple rbd-mirroring daemons per cluster - configuration and verification of daemon failure
    Pre-requisites -
    Two ceph clusters with rbd mirror configured along with:
        1. At least 2 rbd mirror daemons per cluster
        2. 1 client

    Test Case Flow:
    1. Configure rbd mirroring between two clusters with both ec and replicated images.
    2. Bring down one of the rbd mirroring daemon on cluster 2
    3. Do relocation operation from cluster 1 to 2
    4. Bring up the brought down daemon
    5. Bring down leader daemon on both clusters.
    6. Perform failover from cluster 2 to cluster 1
    7. Check for data integrity.
    """
    log.info("Starting CEPH-83574962")

    config = {
        "rep_pool_config": {
            "mode": "image",
            "mirrormode": "snapshot",
            "image": "rbd_image_rep",
            "io_total": "1G",
            "pool": "rbd_pool",
            "size": "10G",
        },
        "ec_pool_config": {
            "pool": "rbd_pool",
            "image": "rbd_image_ec",
            "mode": "image",
            "data_pool": "data_pool",
            "io_total": "1G",
            "mirrormode": "snapshot",
            "size": "10G",
        },
    }
    kw["config"].update(config)
    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        mirror1 = mirror_obj["rep_rbdmirror"]["mirror1"]
        mirror2 = mirror_obj["rep_rbdmirror"]["mirror2"]

        rep_image_spec = (
            f'{config["rep_pool_config"]["pool"]}/{config["rep_pool_config"]["image"]}'
        )
        ec_image_spec = (
            f'{config["ec_pool_config"]["pool"]}/{config["ec_pool_config"]["image"]}'
        )
        image_spec_list = [rep_image_spec, ec_image_spec]

        log.info(
            "Stopping an rbd daemon in cluster 2 and performing relocate operation"
        )

        # Gather service name in cluster2 and stop non-leader daemon
        mirror2.update_ceph_rbdmirror(config["rep_pool_config"]["pool"], leader=False)
        service_name_mirror2 = mirror2.get_rbd_service_name("rbd-mirror")
        mirror2.change_service_state(service_name_mirror2, "stop")

        # Relocate: Demote image at cluster1
        for imagespec in image_spec_list:
            mirror1.demote(imagespec=imagespec)
        with parallel() as p:
            p.spawn(
                mirror2.wait_for_status,
                imagespec=rep_image_spec,
                state_pattern="up+unknown",
            )
            p.spawn(
                mirror2.wait_for_status,
                imagespec=ec_image_spec,
                state_pattern="up+unknown",
            )
        # Relocate: promote image at cluster2
        for imagespec in image_spec_list:
            mirror2.promote(imagespec=imagespec)

        with parallel() as p:
            p.spawn(mirror2.benchwrite, imagespec=rep_image_spec, io="1G")
            p.spawn(mirror2.benchwrite, imagespec=ec_image_spec, io="1G")
        time.sleep(30)
        mirror2.change_service_state(service_name_mirror2, "start")

        log.info(
            "Marking leader rbd daemon in booth clusters and performing failover operation"
        )
        # Stop leader daemon on both the clusters
        service_names = []

        for cluster in [mirror1, mirror2]:
            cluster.update_ceph_rbdmirror(config["rep_pool_config"]["pool"], True)
            service_names.append(cluster.get_rbd_service_name("rbd-mirror"))
            cluster.change_service_state(service_names[-1], "stop")

        # Failover: force promote both images on cluster1
        for imagespec in image_spec_list:
            mirror1.promote(imagespec=imagespec, force=True)

        with parallel() as p:
            p.spawn(
                mirror1.wait_for_status,
                imagespec=rep_image_spec,
                state_pattern="up+stopped",
            )
            p.spawn(
                mirror1.wait_for_status,
                imagespec=ec_image_spec,
                state_pattern="up+stopped",
            )
        with parallel() as p:
            p.spawn(mirror1.benchwrite, imagespec=rep_image_spec, io="1G")
            p.spawn(mirror1.benchwrite, imagespec=ec_image_spec, io="1G")

        # Failover: demote images at cluster2 and initiate resync
        for imagespec in image_spec_list:
            mirror2.demote(imagespec=imagespec)
            time.sleep(10)
            mirror2.resync(imagespec)

        with parallel() as p:
            p.spawn(mirror1.change_service_state, service_names[0], "start")
            p.spawn(mirror2.change_service_state, service_names[1], "start")
            time.sleep(30)
            for imagespec in image_spec_list:
                p.spawn(mirror1.check_data, peercluster=mirror2, imagespec=imagespec)

    return 0
