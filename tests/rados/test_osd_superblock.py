import random

from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from utility.log import Log

log = Log(__name__)


# Exception for command success when
# crash is expected
class NoCrash(Exception):
    pass


def run(ceph_cluster, **kw):
    """
    # CEPH-83593841
    # BZ-2079897
    OSD superblock corruption leads to OSD failure mostly during reboots
     when a Customer is upgrading, in order to verify the recovery,
     OSD superblock present in PG meta will be corrupted with COT tool
    Purpose of this test is to verify recovery and repair of OSD superblock
     when OSD is restarted
    1. Choose an OSD at random
    2. Check for existence of osd_superblock object in PG 'meta' using ceph-objectstore-tool
    3. Check if an OMAP entry exists for the osd_superblock object
    4. Create a file with garbage data use it to manipulate content of osd_superblock object
    5. Corrupt osd_superblock file by injecting garbage data using COT tool
    6. Run any command using COT tool and observe the crash
    6. Restart OSD service
    7. Run any command using COT tool, should not crash
    """
    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config.get("rhbuild")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    objectstore_obj = objectstoreToolWorkflows(node=cephadm)

    log.info(
        "Running test case to verify OSD Superblock "
        "redundancy and consequently OSDs' resiliency during corruption"
    )

    if float(rhbuild.split("-")[0]) < 7.1:
        log.info(
            "Passing test without execution, feature/fix has been merged in RHCS 7.1"
        )
        return 0

    try:
        # choosing an OSD at random
        osd_list = rados_obj.get_active_osd_list()
        log.info(f"List of active OSDs on the cluster: {osd_list}")
        osd_id = random.choice(osd_list)
        log.info(f"Random OSD chosen for the test: {osd_id}")

        # Check for osd_superblock object in PG meta
        out = objectstore_obj.list_objects(
            osd_id=osd_id, pgid="meta", obj_name="osd_superblock"
        )
        log.info(f"Output of object list from PG 'meta': \n {out}")
        assert "osd_superblock" in out

        # list omap entries for the osd_superblock object
        omap_entries = objectstore_obj.list_omap(
            osd_id=osd_id, pgid="meta", obj="osd_superblock"
        )
        log.info(f"OMAP entries in object osd_superblock from PG 'meta': \n {out}")
        assert "osd_superblock" in omap_entries

        # print the data present in osd_superblock omap
        try:
            out = objectstore_obj.get_omap(
                osd_id=osd_id, pgid="meta", obj="osd_superblock", key="osd_superblock"
            )
            log.info(f"osd_superblock OMAP data: \n {out}")
        except UnicodeDecodeError:
            log.info("OMAP content not in utf-8 encoding, skipping logging")

        # create a garbage file to corrupt osd_superblock object
        log.info("Creating garbage data to push into osd_superblock object")
        _data = "This is a random string to corrupt OSD superblock"
        edit_cmd = f"echo '{_data}' > /tmp/garbage_data"
        osd_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
        out, _ = osd_host.exec_command(sudo=True, cmd=edit_cmd)

        # preserve the original content of osd_superblock object
        log.info("Preserving the original content of osd_superblock object")
        objectstore_obj.get_bytes(
            osd_id=osd_id,
            pgid="meta",
            obj="osd_superblock",
            out_file="/tmp/org_osd_superblock",
        )

        # overwrite osd_superblock data
        log.info("Corrupting osd_superblock data with garbage value")
        objectstore_obj.set_bytes(
            osd_id=osd_id,
            pgid="meta",
            obj="osd_superblock",
            in_file="/tmp/garbage_data",
            start=False,
        )

        # running any COT command beyond this point should result in a crash
        try:
            objectstore_obj.list_objects(osd_id=osd_id, pgid="meta")
            log.error(
                "An Exception should have been hit during the execution of list object"
                "COT command as OSD superblock has been corrupted"
            )
            raise NoCrash(
                "Did not encounter excepted crash in COT tool after OSD superblock corruption"
            )
        except CommandFailed as er:
            log.error(
                f"Failure is expected because OSD superblock has been corrupted,"
                f"Error: \n {er}"
            )

        # restart the OSD service
        if not rados_obj.change_osd_state(action="restart", target=osd_id):
            log.error(f"Could not restart osd - {osd_id} successfully")
            raise Exception(f"osd.{osd_id} could not restart")

        # now that OSD has been restarted, run the osd superblock object list command again
        out = objectstore_obj.list_objects(
            osd_id=osd_id, pgid="meta", obj_name="osd_superblock"
        )
        log.info(f"Output of object list from PG 'meta': \n {out}")
        assert "osd_superblock" in out

        log.info(
            "OSD superblock corruption and recovery has been verified"
            f"successfully for OSD osd.{osd_id} on {osd_host.hostname}"
        )
    except Exception as e:
        log.error(f"Execution failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )

        # restart the OSD service
        if "osd_id" in locals() or "osd_id" in globals():
            if not rados_obj.change_osd_state(action="start", target=osd_id):
                log.error(f"Could not restart osd - {osd_id} successfully")
                return 1

        # Delete the created osd pool
        rados_obj.rados_pool_cleanup()

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
