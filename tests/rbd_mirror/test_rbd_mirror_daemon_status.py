from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Verification rbd mirror and daemon status on cluster

    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    Returns:
        0 - if test case pass
        1 - it test case fails
    Test case covered : CEPH-83573760
    Test Case Flow:
    1. Deploy two clusters using Cephadm
    2. Configures RBD Mirroring
    3. Check the mirror status on primary and secoundary clusters
    4. Check the rbd-daemons on cluster
    """
    try:
        log.info("Starting RBD mirroring test case")
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        # Check the mirror status
        poolname = mirror1.random_string() + "_tier_1_rbd_mirror_pool"
        imagename = mirror1.random_string() + "_tier_1_rbd_mirror_image"

        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "1G"),
            mode="pool",
        )

        # Check rbd-daemon runing status
        out1 = mirror1.mirror_daemon_status("rbd-mirror")
        out2 = mirror1.mirror_daemon_status("rbd-mirror")
        log.info(f"RBD Daemon Status of cluster rbd1 : {out1}")
        log.info(f"RBD Daemon Status of cluster rbd2 : {out2}")
        if out1 == 1 or out2 == 1:
            return 1
        return 0

    except ValueError as ve:
        log.error(
            f"{kw.get('ceph_cluster_dict').values} has less or more clusters Than Expected(2 clusters expected)"
        )
        log.error(ve)
    except Exception as e:
        log.error(e)
        return 1
