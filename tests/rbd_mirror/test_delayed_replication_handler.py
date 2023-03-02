"""Module to execute delayed replication feature test cases.

Test case covered -
CEPH-11492 : Delayed Replication-Delay on secondary.
Pre-requisites -
Two ceph clusters with rbd mirror configured along with at least 1 client.
Test Case Flow:
Note : rbd_mirror util module rbd_mirror_config handles initial mirror config between
clusters and it can be skipped by using skip_initial_config in each iteration for
different test case.
Based on the user config, particular test case will be executed.

1. polarion-id: CEPH-11492 - Delayed Replication-Delay on secondary.
   config: delay_at_secondary

   a. Record initial md5sum value of images,
   b. Set delay at secondary site using config rbd_mirroring_replay_delay.
   c. Observe secondary image till mentioned delay that there are no mirroring
      activity between clusters,if nochanged observed,testcase passed.

2. polarion-id: CEPH-11490- Delayed Replication-Failover with delay set.
   config: direct_failover_with_delay

   a. Record initial md5sum value of images.
   b. perform force failover operation after some io operation.
   c. Check md5sum of images and comapre with initial value.
   d. If md5sum remains same, testcase is passed.
"""

import datetime
import time

from ceph.parallel import parallel
from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from tests.rbd_mirror.rbd_mirror_utils import parallel_bench_on_n_images as write_data
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Handler for delayed replication scenarios.
    Args:
        kw:
          skip_initial_config: Skip initial config of mirroring.
          direct_failover_with_delay: Execute failover scenario directly.
          pool_image_config: details of rep_pool_config and ec_pool_config.
          delay: delay duration in seconds (default: 600)
    Returns:
        int: The return value. 0 for success, 1 otherwise

    IMPORTANT: This feature is not supported for snapshot based mirroring.
    mirrormode - of each pool config must be journal.
    """
    log.info("Starting execution for Delayed replication scenario")

    config = kw.get("config")

    if not config.get("pool_image_config"):
        pool_image_config = {
            "rep_pool_config": {
                "mode": "pool",
                "mirrormode": "journal",
                "image": "rbd_image_rep",
                "pool": "rbd_pool_delayed_replication_handler_200",
                "io_total": "100M",
            },
            "ec_pool_config": {
                "pool": "rbd_pool_delayed_replication_handler_200",
                "image": "rbd_image_ec",
                "mode": "image",
                "data_pool": "data_pool",
                "mirrormode": "journal",
                "io_total": "100M",
            },
        }
        kw["config"].update(pool_image_config)
    imagespecs = [
        f'{kw["config"]["rep_pool_config"]["pool"]}/{kw["config"]["rep_pool_config"]["image"]}',
        f'{kw["config"]["ec_pool_config"]["pool"]}/{kw["config"]["ec_pool_config"]["image"]}',
    ]

    delay = config.get("delay", 600)

    mirror1, mirror2 = [
        rbdmirror.RbdMirror(cluster, config)
        for cluster in kw.get("ceph_cluster_dict").values()
    ]

    rbd1, rbd2 = [
        Rbd(**kw, req_cname=cluster_name)
        for cluster_name in kw.get("ceph_cluster_dict").keys()
    ]

    def export_and_get_md5sum():
        md5s = {}
        with parallel() as p:
            for imagespec in imagespecs:
                p.spawn(
                    mirror2.export_image,
                    imagespec=imagespec,
                    path=f"export_{imagespec.split('/')[1]}",
                )

        try:
            with parallel() as p:
                for imagespec in imagespecs:
                    p.spawn(
                        mirror2.exec_cmd,
                        cmd=f"md5sum export_{imagespec.split('/')[1]}",
                        output=True,
                        ceph_args=False,
                    )

                for result in p:
                    # Reading md5sum out put '<md5sum> <filename>' and reformatting it to
                    # {"<file-name>":"<<md5sum>"} and update the result dictionary.
                    md5s.update(
                        {f"{result.split('  ')[1][:-1]}": f"{result.split('  ')[0]}"}
                    )
            return md5s
        finally:
            with parallel() as p:
                for imagespec in imagespecs:
                    p.spawn(
                        mirror2.exec_cmd,
                        cmd=f"rm export_{imagespec.split('/')[1]}",
                        output=True,
                        ceph_args=False,
                    )

    def get_initial_md5sums():
        log.debug(
            "Waiting for reply to complete and fetching initial md5sum of image exports"
        )
        with parallel() as p:
            for imagespec in imagespecs:
                p.spawn(mirror2.wait_for_replay_complete, imagespec)

        return export_and_get_md5sum()

    def write_data_and_verify_no_mirror_till_delay():
        starttime = datetime.datetime.now()
        wait_till = datetime.timedelta(seconds=(delay - 5))
        log.info(f"{starttime}")
        write_data(mirror1, imagespecs, "100M")
        while datetime.datetime.now() - starttime <= wait_till:
            if initial_md5sums != export_and_get_md5sum():
                log.error("Mirorring activity seen on secondary without desired delay")
                return 1
            log.debug(
                "Verified that no data has bee added to secondary image in last 50 seconds"
            )
            time.sleep(50)

        log.info(
            "No mirorring ios seen in secondary untill the delay, Test case passed"
        )
        return 0

    def config_delay_at_secondary():
        mirror2.exec_cmd(
            cmd=f"ceph config set client rbd_mirroring_replay_delay {delay}"
        )

    try:
        if not config.get("skip_initial_config"):
            rbd_mirror_config(**kw)

        initial_md5sums = get_initial_md5sums()

        if config.get("delay_at_secondary"):
            log.debug(
                "Executing delayed replication scenario when delay configured at secondary"
            )
            config_delay_at_secondary()
            return write_data_and_verify_no_mirror_till_delay()

        if config.get("direct_failover_with_delay"):
            config_delay_at_secondary()
            time.sleep(20)

            write_data(mirror1, imagespecs, "100M")
            time.sleep(20)
            with parallel() as p:
                for imagespec in imagespecs:
                    p.spawn(mirror2.promote, force=True, imagespec=imagespec)
            time.sleep(20)
            if initial_md5sums != export_and_get_md5sum():
                log.error("Mirroring has happened during failover unexpectedly")
                return 1
            else:
                log.debug(
                    "Delay has been respected during Image force promote, TC passed"
                )
                return 0

    finally:
        if config.get("delay_at_primary"):
            for imagespec in imagespecs:
                rbd1.image_meta(
                    action="set",
                    image_spec=imagespec,
                    key="conf_rbd_mirroring_replay_delay",
                    value=0,
                )
        else:
            mirror2.exec_cmd(cmd="ceph config set client rbd_mirroring_replay_delay 0")
        time.sleep(10)
