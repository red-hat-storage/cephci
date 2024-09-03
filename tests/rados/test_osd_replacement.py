import datetime
import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83572702
    Verify replacement of an OSD along with retaining its OSD ID
    1. Create a cluster with few OSDs
    2. Choose on OSD from the existing OSD list
    3. Fetch OSD metadata to get the disk and hostname
    4. Set OSD service unmanaged to True
    5. Use ceph osd rm with --replace and --zap flags to replace the OSD
    6. Add the OSD back to the cluster using ceph orch daemon add method
    7. Set OSD service unmanaged to False
    8. Repeat steps 5
    9. Cluster should add the OSD back with the same ID automatically
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
        log.debug(f"List of OSDs: {osd_list}")
        osd_id = int(random.choice(osd_list))
        osd_list.pop(osd_id)
        osd_metadata = ceph_cluster.get_osd_metadata(osd_id=int(osd_id), client=client)
        log.debug(osd_metadata)
        osd_device = f"/dev/{osd_metadata['devices']}"
        osd_hostname = osd_metadata["hostname"]

        # set unmanaged to True for OSD
        utils.set_osd_devices_unmanaged(ceph_cluster, osd_id, unmanaged=True)
        # Replace the OSD
        utils.osd_replace(ceph_cluster, osd_id)

        time.sleep(15)
        # check status of OSD from ceph osd tree output
        assert rados_obj.fetch_osd_status(_osd_id=osd_id) == "destroyed"

        # Attempt to add the OSD back to the cluster
        out, err = cephadm.shell(
            [f"ceph orch daemon add osd {osd_hostname}:{osd_device}"]
        )
        log.info(out)
        assert f"Created osd(s) {osd_id} on host '{osd_hostname}'" in out
        destroyed_osd = rados_obj.run_ceph_command(cmd="ceph osd tree destroyed")[
            "nodes"
        ]
        log.debug(destroyed_osd)
        if len(destroyed_osd) != 0:
            raise AssertionError("Cluster still has OSD(s) with destroyed status")
        time.sleep(5)
        assert rados_obj.fetch_osd_status(_osd_id=osd_id) == "up"

        # set unmanaged to False for OSD
        log.info("Set OSD service unmanaged to false")
        utils.set_osd_devices_unmanaged(ceph_cluster, osd_list[0], unmanaged=False)

        # Second workflow where unmanaged is not set to True
        # Cluster will recover and add the new OSD on the same device itself
        log.info(
            "***** Initiating second workflow - unmanaged is not set to False \n"
            "Cluster will recover and add the new OSD on the same device itself *********"
        )
        osd_id = random.choice(osd_list)
        # Replace the OSD
        utils.osd_replace(ceph_cluster, osd_id)
        # wait for 15 mins for ceph cluster to add back the OSD
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=900)
        while datetime.datetime.now() < endtime:
            destroyed_osd = rados_obj.run_ceph_command(cmd="ceph osd tree destroyed")[
                "nodes"
            ]
            if (
                len(destroyed_osd) == 0
                and rados_obj.fetch_osd_status(_osd_id=osd_id) == "up"
            ):
                log.info("No more OSDs have status 'destroyed'")
                log.info(f"OSD {osd_id} status is UP")
                break
            log.info(
                f"Cluster still has OSDs with status 'destroyed' or OSD {osd_id} is yet to come up, "
                f"sleeping for 15 secs"
            )
            time.sleep(15)
        else:
            log.error(
                f"Cluster failed to add back OSD {osd_id} or it couldn't come up within timeout"
            )
            log.info(f"List of OSD(s) with destroyed status: {destroyed_osd}")
            raise Exception(f"Cluster failed to add back OSD {osd_id}")
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        if rados_obj.fetch_osd_status(_osd_id=osd_id) == "destroyed":
            out, err = cephadm.shell(
                [f"ceph orch daemon add osd {osd_hostname}:{osd_device}"]
            )
        utils.set_osd_devices_unmanaged(ceph_cluster, osd_id, unmanaged=False)

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Verification complete for replacement of OSD by retaining its ID")
    return 0
