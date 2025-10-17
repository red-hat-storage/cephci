"""
Module to Verify the hotfix bugs: 2392395, 2392394

Verification that all OSD beacons generated adhere to osd_beacon_report_interval even where there are scrub_purged_snaps
generated on the cluster.

Created multiple rbd pools and Images & Ceph file system with subvolumes and snapshots.
With multiple snapshot Creation + deletion + snap purge with scrubs and deep-scrubs happening on the cluster.
"""

import concurrent.futures
import datetime
import random
import re
import statistics
import threading
import time
from collections import defaultdict
from typing import Any, Dict

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods, MonElectionStrategies
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Module to Verify the hotfix bugs: 2392395, 2392394

    Verification that all OSD beacons generated adhere to osd_beacon_report_interval even where there
    are scrub_purged_snaps generated on the cluster.
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_deamon_object = MonElectionStrategies(rados_obj=rados_obj)
    mon_config_obj = MonConfigMethods(rados_obj=rados_obj)
    test_duration = config.get("test_duration", 10)
    heartbeat_interval = config.get("heartbeat_interval", 60)
    pool_name_rbd = config.get("pool_name_rbd", "test_rbd_new")
    num_rbd_pools = config.get("num_rbd_pools", 5)
    num_threads = config.get("num_threads", 5)
    # ceph_fs_vol = "stress_cephfs"
    rbd_pools = [f"{pool_name_rbd}-{i}" for i in range(num_rbd_pools)]
    log.info(
        "Running test to see if OSDs send more beacons than what is specified via osd_beacon_timeout set"
    )
    try:
        # enable the file logging
        if not rados_obj.enable_file_logging():
            log.error("Error while setting config to enable logging into file")
            return 1

        leader_mon = mon_deamon_object.get_mon_quorum_leader()
        mon_host = rados_obj.get_host_object(hostname=leader_mon)

        # Updating OSD heartbeat interval
        mon_config_obj.set_config(
            section="osd", name="osd_beacon_report_interval", value=heartbeat_interval
        )
        # updating debug level on leader mon
        mon_config_obj.set_config(
            section=f"mon.{leader_mon}", name="debug_mon", value="10/10"
        )

        init_time, _ = mon_host.exec_command(
            cmd="sudo date -u '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        init_time = init_time.strip()

        log.debug(f"stress test start time : {init_time}")
        fs_name = rados_obj.run_ceph_command(cmd="ceph fs ls")[0]["name"]

        trbd = threading.Thread(
            target=run_rbd_snapshot_stress,
            args=(rados_obj,),
            kwargs={
                "pool_name": pool_name_rbd,
                "duration": test_duration,
                "parallel_jobs": num_threads,
            },
        )
        tcfs = threading.Thread(
            target=run_cephfs_snapshot_stress,
            args=(rados_obj,),
            kwargs={
                "fs_name": fs_name,
                "duration": test_duration,
                "parallel_jobs": num_threads,
            },
        )
        trbd.start()
        tcfs.start()
        trbd.join()
        tcfs.join()
        log.debug("Completed the stress workflows, proceeding to check logs")
        time.sleep(10)

        end_time, _ = mon_host.exec_command(
            cmd="sudo date -u '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        end_time = end_time.strip()
        log.debug(f"stress test end time : {end_time}")
        mon_config_obj.remove_config(section="osd", name="osd_beacon_report_interval")
        mon_config_obj.remove_config(section=f"mon.{leader_mon}", name="debug_mon")

        # log analysis
        results = parse_beacons_remote(
            rados_obj, leader_mon=leader_mon, start_time=init_time, end_time=end_time
        )
        if not results:
            log.error("results not obtained from logs")
            return 1

        fail_flag = False
        for osd_id, stats in results.items():
            log.debug(f"Parsing results for OSD : {osd_id}")
            interval_min = stats.get("interval_min")
            interval_max = stats.get("interval_max")
            interval_avg = stats.get("interval_avg")

            # Providing leeway of 5 seconds for heartbeats to vary
            if interval_min is not None and abs(interval_min - heartbeat_interval) > 5:
                error_msg = (
                    f"The minimum interval of {heartbeat_interval} was not honoured."
                    f"min_interval for heartbeats on OSD : {interval_min}"
                )
                log.error(error_msg)
                fail_flag = True
            if interval_max is not None and abs(interval_max - heartbeat_interval) > 5:
                error_msg = (
                    f"The Maximum interval of {heartbeat_interval} was not honoured."
                    f"min_interval for heartbeats on OSD : {interval_max}"
                )
                log.error(error_msg)
                fail_flag = True
            if interval_avg is not None and abs(interval_avg - heartbeat_interval) > 5:
                error_msg = (
                    f"The Average interval of {heartbeat_interval} was not honoured."
                    f"min_interval for heartbeats on OSD : {interval_avg}"
                )
                log.error(error_msg)
                fail_flag = True

        if fail_flag:
            log.error("Test failed post analysis of results")
            return 1

        log.info("Completed tests and collecting data. Analysing results")
    except Exception as err:
        log.error(f"Exception hit during the test execution : {err}")
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info("\n\nExecution of finally block\n\n")
        mon_config_obj.remove_config(section="osd", name="osd_beacon_report_interval")
        mon_config_obj.remove_config(section="mon", name="debug_mon")
        for pool in rbd_pools:
            rados_obj.delete_pool(pool=pool)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed Verification of Hotfix provided")
    return 0


def run_rbd_snapshot_stress(
    rados_obj,
    pool_name,
    num_pools=5,
    images_per_pool=5,
    snaps_per_image=10,
    parallel_jobs=5,
    duration=30,
):
    """
    Stress script for Ceph RBD snapshots:
    - Creates pools + images
    - Writes random data
    - Creates and deletes/purges snapshots continuously
    - Forces purged-snaps scrub

    Args:
        rados_obj: rados Object
        pool_name: pool name prefix
        num_pools: Number of pools to create
        images_per_pool: Images per pool
        snaps_per_image: Snapshots per image
        parallel_jobs: Max parallel operations
        duration: how long the job will run in minutes
    """
    pools = [f"{pool_name}-{i}" for i in range(num_pools)]

    def write_data(pool: str, img: str):
        size_mb = random.randint(16, 128)
        log.debug(f"Writing ~{size_mb}MB data to {pool}/{img}")
        cmd = f"rbd bench-write {pool}/{img} --io-size 4K --io-threads 4 --io-total {size_mb}M"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

    def setup_pools():
        log.info("Creating pools and images in parallel...")

        def setup_pool(pool: str):
            # Replicated pools as of now
            rados_obj.create_pool(pool_name=pool, app_name="rbd")
            for img in range(images_per_pool):
                rados_obj.create_rbd_image(pool_name=pool, img_name=f"img{img}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_jobs) as ex:
            list(ex.map(setup_pool, pools))
        log.info("All pools and images created.")

    def process_image(pool: str, img: int):
        local_image = f"{pool}/img{img}"
        snaps = []

        def snapper():
            # While writes are ongoing, keep taking snapshots
            for s in range(snaps_per_image):
                snapname = f"snap{s}-{int(time.time())}"
                snaps.append(snapname)
                cmd = f"rbd snap create {local_image}@{snapname}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)

        # Run writer + snapper in parallel
        t1 = threading.Thread(target=write_data(pool, f"img{img}"))
        t2 = threading.Thread(target=snapper)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Proceeding to image purges / removal
        if img % 3 == 0:
            for snapname in snaps:
                cmd = f"rbd snap rm {local_image}@{snapname}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
        else:
            cmd = f"rbd snap purge {local_image}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

    def stress_cycle():
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=duration * 60)
        while end_time > datetime.datetime.now():
            with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_jobs) as ex:
                futures = []
                for pool in pools:
                    for img in range(images_per_pool):
                        futures.append(ex.submit(process_image, pool, img))
                for _ in concurrent.futures.as_completed(futures):
                    pass

            log.debug(">>> Completed one RBD stress cycle, sleeping...")
            rados_obj.run_scrub()
            rados_obj.run_deep_scrub()
            time.sleep(10)

    try:
        setup_pools()
        stress_cycle()
    finally:
        for pool in pools:
            rados_obj.delete_pool(pool=pool)


def run_cephfs_snapshot_stress(
    rados_obj,
    fs_name="cephfs",
    num_subvols=5,
    files_per_subvol=10,
    snaps_per_subvol=10,
    parallel_jobs=5,
    volume_group="cfsstress",
    duration=30,
):
    """Stress test for CephFS snapshots:
    - Creates a subvolume group
    - Creates multiple subvolumes
    - Writes random files into them
    - Creates/deletes snapshots continuously

    Args:
        rados_obj: rados object
        fs_name: CephFS filesystem name
        num_subvols: number of subvolumes to create
        files_per_subvol: files to create per subvolume
        snaps_per_subvol: snapshots per subvolume per cycle
        parallel_jobs: parallelism
        volume_group: CephFS subvolume group name
        duration: how long the job will run in minutes
    """

    subvols = [f"subvol-{i}" for i in range(num_subvols)]

    def setup_subvols():
        log.info("Creating CephFS subvolumes...")
        cmd = f"ceph fs subvolumegroup create {fs_name} {volume_group}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

        def create_sv(sv):
            cmd_subvol = f"ceph fs subvolume create {fs_name} {sv} 256M --group_name {volume_group}"
            rados_obj.client.exec_command(cmd=cmd_subvol, sudo=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_jobs) as ex:
            list(ex.map(create_sv, subvols))
        log.debug("All subvolumes created.")

    def write_data(mount_path, filename):
        size_mb = random.randint(4, 32)
        filepath = f"{mount_path}/{filename}"
        log.debug(f"Writing ~{size_mb}MB file {filepath}")
        cmd = f"dd if=/dev/urandom of={filepath} bs=1M count={size_mb} conv=fsync"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

    def process_subvol(sv):
        # Mount path
        mount_path = f"/mnt/{volume_group}/{sv}"

        # Get subvolume path and mount via ceph-fuse (lazy mount per thread)
        cmd = f"ceph fs subvolume getpath {fs_name} {sv} --group_name {volume_group}"
        subvol_path, _ = rados_obj.client.exec_command(cmd=cmd, sudo=True)
        if not subvol_path:
            return

        cmd = f"mkdir {mount_path}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        # make sure ceph-fuse package is installed on client node
        cmd = f"ceph-fuse --client_fs {fs_name} -r {subvol_path.strip()} {mount_path}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

        # Write files
        for f in range(files_per_subvol):
            write_data(mount_path, f"file{f}.dat")

        # Create snapshots
        snaps = []
        for s in range(1, snaps_per_subvol + 1):
            snapname = f"snap{s}-{int(time.time())}"
            snaps.append(snapname)
            cmd = f"ceph fs subvolume snapshot create {fs_name} {sv} {snapname} --group_name {volume_group}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

        for snapname in snaps:
            cmd = f"ceph fs subvolume snapshot rm {fs_name} {sv} {snapname} --group_name {volume_group}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

    def stress_cycle():
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=duration * 60)
        while end_time > datetime.datetime.now():
            with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_jobs) as ex:
                futures = [ex.submit(process_subvol, sv) for sv in subvols]
                for _ in concurrent.futures.as_completed(futures):
                    pass

            log.debug(">>> Completed one CephFS stress cycle, sleeping...")
            rados_obj.run_scrub()
            rados_obj.run_deep_scrub()
            time.sleep(10)

    def cleanup_subvols():
        log.info("Cleaning up CephFS subvolumes and group...")

        def delete_sv(sv):
            cmd = f"ceph fs subvolume rm {fs_name} {sv} --group_name {volume_group} --force"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_jobs) as ex:
            list(ex.map(delete_sv, subvols))

        cmd = f"ceph fs subvolumegroup rm {fs_name} {volume_group} --force"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.debug("Cleanup complete.")

    # === Run ===
    try:
        setup_subvols()
        stress_cycle()
    finally:
        cleanup_subvols()


def parse_beacons_remote(
    rados_obj,
    leader_mon: str,
    start_time: str,
    end_time: str,
) -> Dict[int, Dict[str, Any]]:
    """
    Parse Ceph monitor log on a remote host via SSH efficiently:
    1. Grep osd_beacon lines into a temp file on remote host
    2. Use awk to pre-filter by timestamp
    3. Stream filtered lines into Python for processing
    """
    LINE_RE = re.compile(
        r"^(\S+).*?osd_beacon\((.*?)\).*?from osd\.(\d+)\b", re.IGNORECASE
    )

    def extract_scrub(inner: str) -> str:
        m = re.search(r"last_purged_snaps_scrub\s+([0-9T:\.\+\-]+)", inner)
        return m.group(1) if m else None

    def extract_beacon_version(inner: str) -> str:
        m = re.search(r"\bv(\d+)\b", inner)
        return "v" + m.group(1) if m else None

    beacons_by_osd = defaultdict(list)
    seen_keys = defaultdict(set)

    mon_host = rados_obj.get_host_object(hostname=leader_mon)
    remote_tmp = "/tmp/osd_beacon_temp.log"
    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    mon_log_path = f"/var/log/ceph/{fsid}/ceph-mon.{leader_mon}.log"

    cmd = (
        f"grep osd_beacon {mon_log_path} | "
        f"awk -v start='{start_time}' -v end='{end_time}' "
        f"'$1 >= start && $1 <= end' "
        f"> {remote_tmp}"
    )
    mon_host.exec_command(cmd=cmd, sudo=True)

    try:
        log_lines, err = mon_host.exec_command(cmd=f"sudo cat {remote_tmp}", sudo=True)
        for line in log_lines.splitlines():
            m = LINE_RE.search(line)
            if not m:
                continue
            ts_str, inner, osd_id_str = m.groups()
            ts_dt = None
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
                try:
                    ts_dt = datetime.datetime.strptime(ts_str, fmt)
                    break
                except ValueError:
                    continue
            if ts_dt is None:
                log.debug(f"[ERROR] Could not parse timestamp: {ts_str}")
                continue

            osd_id = int(osd_id_str)
            scrub = extract_scrub(inner)
            version = extract_beacon_version(inner)

            key = version if version else f"{ts_str}|{scrub or ''}"
            if key in seen_keys[osd_id]:
                continue
            seen_keys[osd_id].add(key)
            beacons_by_osd[osd_id].append((ts_dt, scrub, version))
    except Exception as e:
        log.error(f"Error streaming filtered remote log: {e}")
        return {}

    if not beacons_by_osd:
        log.error("No osd_beacon lines parsed in the given time range.")
        return {}

    summary: Dict[int, Dict[str, Any]] = {}

    log.debug("\n\n Printing OSD beacon stat collection \n\n ")
    for osd in sorted(beacons_by_osd.keys()):
        rows = sorted(beacons_by_osd[osd], key=lambda x: x[0])
        count = len(rows)

        intervals = (
            [(rows[i][0] - rows[i - 1][0]).total_seconds() for i in range(1, count)]
            if count >= 2
            else []
        )
        interval_min = min(intervals) if intervals else None
        interval_max = max(intervals) if intervals else None
        interval_avg = statistics.mean(intervals) if intervals else None

        scrub_values = [scrub for _, scrub, _ in rows if scrub is not None]
        scrub_updated = len(set(scrub_values)) > 1 if scrub_values else False
        scrub_previous = scrub_values[0] if scrub_values else None
        scrub_latest = scrub_values[-1] if scrub_values else None

        summary[osd] = {
            "count": count,
            "intervals": intervals,
            "interval_min": interval_min,
            "interval_max": interval_max,
            "interval_avg": interval_avg,
            "timeline": rows,
            "scrub_updated": scrub_updated,
            "scrub_previous": scrub_previous,
            "scrub_latest": scrub_latest,
        }

        log.debug(f"OSD.{osd}:")
        log.debug(f"  Total unique beacons: {count}")
        if intervals:
            log.debug(
                f"  Beacon intervals (sec): min={interval_min:.2f}, max={interval_max:.2f},"
                f" avg={interval_avg:.2f} over {len(intervals)} intervals"
            )
        else:
            log.debug("  Beacon intervals: insufficient data (need >=2 beacons)")
        log.debug(f"  last_purged_snaps_scrub updated: {scrub_updated}")
        log.debug(f"  scrub_previous={scrub_previous}, scrub_latest={scrub_latest}")
        log.debug("  last_purged_snaps_scrub timeline:")
        for ts, scrub, ver in rows:
            if scrub:
                log.debug(
                    f"    Beacon @ {ts}  -> last_purged_snaps_scrub={scrub}  [{ver or 'no-ver'}]"
                )
            else:
                log.debug(
                    f"    Beacon @ {ts}  -> last_purged_snaps_scrub= <none>  [{ver or 'no-ver'}]"
                )
        log.debug("")
    mon_host.exec_command(cmd=f"rm -rf {remote_tmp}", sudo=True)
    return summary
