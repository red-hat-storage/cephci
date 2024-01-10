"""
The program verifies the Bluestore rescue procedure
"""

import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_scrub import RadosScrubber
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    scrub_object = RadosScrubber(node=cephadm)
    rados_object = RadosOrchestrator(node=cephadm)
    ceph_nodes = kw.get("ceph_nodes")
    osd_list = []
    try:
        # setting the noout flag
        scrub_object.set_osd_flags("set", "noout")

        for node in ceph_nodes:
            if node.role == "osd":
                node_osds = rados_object.collect_osd_daemon_ids(node)
                # Pick a single OSD from the host OSDs list
                node_osds = random.sample(node_osds, 1)
                osd_list = osd_list + node_osds
        log.info(f"The number of OSDs in the cluster are-{len(osd_list)}")
        osd_id = random.choice(osd_list)
        log.info(f"The tests are performing on the {osd_id} OSD ")
        osd_node = rados_object.fetch_host_node(
            daemon_type="osd", daemon_id=str(osd_id)
        )
        if not rados_object.change_osd_state(action="stop", target=osd_id):
            log.error(f" Failed to stop the OSD : {osd_id}")
            return 1

        cmd_base = f"cephadm shell --name osd.{osd_id} --"
        cmd_bluestore_export = (
            f"{cmd_base}  ceph-bluestore-tool bluefs-export --path "
            f"/var/lib/ceph/osd/ceph-{osd_id} --out-dir  /tmp/bluefs_export_osd.{osd_id}"
        )
        output = osd_node.exec_command(sudo=True, cmd=cmd_bluestore_export)
        log.info(f"The blue store export output is -{output}")
        if "Segmentation fault" in output:
            log.error("The output contain the Segmentation fault")
            return 1

        cmd_bluestore_recover = (
            f"{cmd_base}  ceph-bluestore-tool -l /proc/self/fd/1 --log-level 5 --path "
            f"/var/lib/ceph/osd/ceph-{osd_id} fsck --debug_bluefs=5/5 --bluefs_replay_recovery=true"
        )
        osd_node.exec_command(sudo=True, cmd=cmd_bluestore_recover)
        osd_status_falg = False
        time_end = time.time() + 60 * 4
        while time.time() < time_end:
            osd_status, status_desc = rados_object.get_daemon_status(
                daemon_type="osd", daemon_id=osd_id
            )
            if osd_status == 1 or status_desc == "running":
                log.info(f"OSD-{osd_id} is up and running")
                osd_status_falg = True
                break
            log.info(f"osd-{osd_id} is not up and running")
            time.sleep(10)
        if not osd_status_falg:
            log.error(f"The OSD-{osd_id} is not up after the bluestore recovery")
            return 1
        log.info(f"The OSD-{osd_id} is up and running after the bluestore recovery")
    except Exception as er:
        log.error(f"Exception hit while command execution. {er}")
        return 1
    finally:
        if not rados_object.change_osd_state(action="start", target=osd_id):
            log.error(f" Failed to stop the OSD : {osd_id}")
            return 1
    return 0
