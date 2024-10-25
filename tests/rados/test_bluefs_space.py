# Test cannot run on Pacific builds due to CBT bluefs-stats not being verbose"
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83600867
    Bugs -
        Reef: BZ-2317044
        Squid: BZ-2317027
    Test to verify bluefs file are not inflated and do no occupy more space than
    their real size
    1. Create a data pool with single PG
    2. Use rados bench to fill the data pool
    3. Write small number of large OMAPs to the pool
    4. Run deep-scrub on the concerned pool to verify OMAP entries
    5. Write huge number of large OMAP data to the pool
    6. Fetch the primary OSD for the pool
    7. Perform OSD compaction for the primary OSD
    8. Capture and log perf dump bluefs for the primary OSD
    9. Fetch ceph-bluestore-tool bluefs stats and check DB utilization
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    bluestore_obj = BluestoreToolWorkflows(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    omap_config = config["omap_config"]

    log.info("Running test case to verify bluefs space utilization is under control")

    try:
        log.debug(
            f"Creating replicated pool on the cluster with name {omap_config['pool_name']}"
        )

        # disable autoscaler at global level
        assert mon_obj.set_config(
            section="global", name="osd_pool_default_pg_autoscale_mode", value="off"
        ), "Could not set pg_autoscale mode to OFF"

        method_should_succeed(rados_obj.create_pool, **omap_config)
        pool_name = omap_config.pop("pool_name")
        normal_objs = omap_config["normal_objs"]
        if normal_objs > 0:
            # create n number of objects without any omap entry
            rados_obj.bench_write(
                pool_name=pool_name,
                **{
                    "rados_write_duration": 600,
                    "byte_size": "4096KB",
                    "max_objs": normal_objs,
                    "verify_stats": False,
                },
            )

        # calculate objects to be written with omaps and begin omap creation process
        omap_obj_num = omap_config["obj_end"] - omap_config["obj_start"]
        log.debug(
            f"Beginning to create omap entries on the pool. Count : {omap_obj_num}"
        )
        omap_config["retain_script"] = True
        if not pool_obj.fill_omap_entries(pool_name=pool_name, **omap_config):
            log.error(f"Omap entries not generated on pool {pool_name}")
            raise Exception(f"Omap entries not generated on pool {pool_name}")

        log.info(
            f"Log ceph df detail: \n {rados_obj.get_cephdf_stats(pool_name=pool_name, detail=True)}"
        )

        # Write 1500 large OMAP objects on the pool, 150 at a time to
        # not overwhelm VM cluster
        for _ in range(10):
            cmd_options = f"--pool {pool_name} --start 0 --end 150 --key-count 200001"
            cmd = f"python3 generate_omap_entries.py {cmd_options} &> /dev/null &"
            rados_obj.client.exec_command(
                sudo=True, cmd=cmd, timeout=600, check_ec=False
            )
            time.sleep(10)

        cmd_options = f"--pool {pool_name} --start 0 --end 150 --key-count 200001"
        cmd = f"python3 generate_omap_entries.py {cmd_options}"
        rados_obj.client.exec_command(sudo=True, cmd=cmd, timeout=900)
        time.sleep(10)

        log.info(
            f"Log ceph df detail: \n {rados_obj.get_cephdf_stats(pool_name=pool_name, detail=True)}"
        )

        # fetch acting set and primary osd for the created pool
        acting_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
        primary_osd = acting_set[0]
        log.info(f"Acting set for {pool_name}: {acting_set}")
        log.info(f"Primary OSD for the pool {pool_name}: {primary_osd}")

        # perform OSD compaction for the primary OSD
        log.info(f"Starting OSD compaction for OSD {primary_osd}")
        out, _ = cephadm.shell(
            args=[f"ceph tell osd.{primary_osd} compact"], timeout=1200
        )
        log.info(out)
        assert "elapsed_time" in out, "OSD Compaction output not as expected"
        time.sleep(5)

        # log ceph tell osd.# perf dump bluefs
        bluefs_perf_dump = rados_obj.get_osd_perf_dump(
            osd_id=primary_osd, filter="bluefs"
        )
        log.debug(f"OSD perf dump bluefs for OSD.{primary_osd}: \n {bluefs_perf_dump}")

        # capture bluefs stats for the primary OSD
        out = bluestore_obj.show_bluefs_stats(osd_id=primary_osd)
        log.info(f"bluefs stats for OSD.{primary_osd}: \n {out}")

        # check if necessary info is present in bluefs-stats output
        pattern_list = [
            "DEV/LEV",
            "DB",
            "MAXIMUMS",
        ]
        for pattern in pattern_list:
            assert pattern in out, f"{pattern} not found in bluefs stats output"

        # get the actual and real DB value from bluefs_stats output
        real_db_cmd = f"echo '{out}'" + " | awk '/DB.*[0-9]/ {print $12}' | head -1"
        actual_db_cmd = f"echo '{out}'" + " | awk '/DB.*[0-9]/ {print $4}' | head -1"
        real_db_max_cmd = f"echo '{out}'" + " | awk '/DB.*[0-9]/ {print $12}' | tail -1"
        actual_db_max_cmd = (
            f"echo '{out}'" + " | awk '/DB.*[0-9]/ {print $4}' | tail -1"
        )

        real_db = float(rados_obj.client.exec_command(cmd=real_db_cmd)[0].strip())
        actual_db = float(rados_obj.client.exec_command(cmd=actual_db_cmd)[0].strip())
        real_db_max = float(
            rados_obj.client.exec_command(cmd=real_db_max_cmd)[0].strip()
        )
        actual_db_max = float(
            rados_obj.client.exec_command(cmd=actual_db_max_cmd)[0].strip()
        )

        if (actual_db - real_db) / real_db * 100 > 5.0:
            log.error("variance between DB real and DB actual is greater than 5%")
        if (actual_db_max - real_db_max) / real_db_max * 100 > 10.0:
            log.error(
                "variance between DB real maximum and DB actual maximum is greater than 5%"
            )

        log.info("DB utilization is under check and not inflated")

    except Exception as e:
        log.error(f"Execution failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # Delete the created osd pool
        rados_obj.rados_pool_cleanup()

        # enable PG autoscaling
        mon_obj.remove_config(
            section="global", name="osd_pool_default_pg_autoscale_mode"
        )

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
