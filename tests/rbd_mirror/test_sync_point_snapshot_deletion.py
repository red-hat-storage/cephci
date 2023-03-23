"""Automating syncpoint snapshot deletion verification.

Testcase information:
CEPH-10466 - sync-point snapshot should not be listed after the re-sync completes
Steps:
1. configure mirroring between two clusters.
2. Induce failure - Failover primary image.
3. While failing back,check snapshot list and make sure that sync point snapshot is deleted.
"""

from ceph.parallel import parallel
from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror.rbd_mirror_utils import parallel_bench_on_n_images as write_data
from tests.rbd_mirror.rbd_mirror_utils import prepare_for_failback, rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def run(**kw):
    log.info("Starting execution for sync-point snapshot deletion verification")

    mirror_obj = rbd_mirror_config(**kw)

    mirror1 = mirror_obj["rep_rbdmirror"]["mirror1"]
    mirror2 = mirror_obj["rep_rbdmirror"]["mirror2"]

    imagespecs = [
        f'{kw["config"]["rep_pool_config"]["pool"]}/{kw["config"]["rep_pool_config"]["image"]}',
        f'{kw["config"]["ec_pool_config"]["pool"]}/{kw["config"]["ec_pool_config"]["image"]}',
    ]
    rbd1, rbd2 = [
        Rbd(**kw, req_cname=cluster_name)
        for cluster_name in kw["ceph_cluster_dict"].keys()
    ]

    def count_number_of_mirror_snaps():
        """Returns count of snaps with 'mirror' keyword in name"""
        snapcounts = 0
        for imagespec in imagespecs:
            snapcounts += str(
                rbd1.snap_ls(
                    imagespec.split("/")[0], imagespec.split("/")[1], None, True
                )
            ).count("mirror")
        return snapcounts

    initial_snapcount = count_number_of_mirror_snaps()

    with parallel() as p:
        for imagespec in imagespecs:
            p.spawn(mirror2.promote, force=True, imagespec=imagespec)
            write_data(mirror2, imagespecs, "100M")

        for imagespec in imagespecs:
            p.spawn(prepare_for_failback, mirror1, imagespec)

    post_failback_snapcounts = count_number_of_mirror_snaps()

    if initial_snapcount < post_failback_snapcounts:
        log.error(
            "Sync point snapshot has been found remaining after the resync operation."
        )
        return 1

    log.info("Sync point snaps are now found stale after resync, test case passed.")
    return 0
