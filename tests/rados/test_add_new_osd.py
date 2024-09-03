import random

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83574780
    Verify addition of a new OSD on existing OSD device, expected to fail
    1. Create a cluster with few OSDs
    2. Choose on OSD from the existing OSD list
    3. Fetch OSD metadata to get the disk and hostname
    4. Use ceph orch command to add new OSD on same disk
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    client = ceph_cluster.get_nodes(role="client")[0]

    log.info("Running test case to verify addition of new OSD on existing OSD disk")

    try:
        out, _ = cephadm.shell(args=["ceph osd ls"])
        osd_list = out.strip().split("\n")
        osd_id = random.choice(osd_list)
        osd_metadata = ceph_cluster.get_osd_metadata(osd_id=int(osd_id), client=client)
        log.debug(osd_metadata)
        osd_device = f"/dev/{osd_metadata['devices']}"
        osd_hostname = osd_metadata["hostname"]
        # try to add OSD on the same device
        out, err = cephadm.shell(
            [f"ceph orch daemon add osd {osd_hostname}:{osd_device}"]
        )
        log.info(out)
        assert "Created no osd(s) on host" in out and "already created" in out
        log.info(
            "Verification complete, addition of new OSD on existing OSD disk, failed as expected"
        )
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
