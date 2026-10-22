import json
import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83633040 - Empty _omap_commit_ops under multi-rank pinned metadata soak.

    Steps:
      1. Ensure max_mds=2 with two active ranks
      2. Fuse-mount FS; create rank_0 / rank_1 trees and pin with ceph.dir.pin
      3. Initial metadata burst under both pinned trees
      4. Parallel multi-rank create/rename/unlink soak
      5. Assert no new MDS crashes; both ranks remain active; FS writable

    Config (optional):
      soak_seconds, sleep_between, creates_per_rank, renames_per_rank,
      workers_per_rank
    """
    client1 = None
    fs_util = None
    fuse_mounting_dir = None
    base = None
    try:
        tc = "CEPH-83633040"
        log.info("Running cephfs %s empty _omap_commit_ops multi-rank soak test", tc)
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config") or {}
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        if len(clients) < 1:
            log.error("This test requires at least 1 client")
            return 1

        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        default_fs = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, default_fs)
        if not fs_details:
            fs_util.create_fs(client1, default_fs)

        soak_seconds = int(config.get("soak_seconds", 300))
        sleep_between = int(config.get("sleep_between", 10))
        creates_per_rank = int(config.get("creates_per_rank", 100))
        renames_per_rank = int(config.get("renames_per_rank", 30))
        workers_per_rank = int(config.get("workers_per_rank", 2))

        log.info("Archiving pre-existing crash records")
        client1.exec_command(sudo=True, cmd="ceph crash archive-all", check_ec=False)

        log.info("Setting max_mds=2 for %s", default_fs)
        client1.exec_command(sudo=True, cmd=f"ceph fs set {default_fs} max_mds 2")
        retry_mds_status = retry(CommandFailed, tries=10, delay=30)(
            fs_util.get_mds_status
        )
        retry_mds_status(client1, 2, vol_name=default_fs)

        client1.exec_command(
            sudo=True, cmd="yum install -y --nogpgcheck attr", check_ec=False
        )

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(8)
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse_{mounting_dir}_omap/"
        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir,
            extra_params=f" --client_fs {default_fs}",
        )

        base = f"{fuse_mounting_dir.rstrip('/')}/test_empty_omap"
        client1.exec_command(
            sudo=True,
            cmd=(
                f"mkdir -p {base}/rank_0 {base}/rank_1 && "
                f"setfattr -n ceph.dir.pin -v 0 {base}/rank_0 && "
                f"setfattr -n ceph.dir.pin -v 1 {base}/rank_1"
            ),
        )
        for rank in (0, 1):
            pin, _ = client1.exec_command(
                sudo=True,
                cmd=f"getfattr -n ceph.dir.pin --only-values {base}/rank_{rank}",
            )
            if pin.strip() != str(rank):
                raise CommandFailed(
                    f"Expected ceph.dir.pin={rank} on {base}/rank_{rank}, got {pin!r}"
                )
            log.info("Verified pin on %s/rank_%s = %s", base, rank, pin.strip())

        log.info("Initial metadata burst under both pinned ranks")
        client1.exec_command(
            sudo=True,
            cmd=(
                f"for r in 0 1; do "
                f"  d={base}/rank_$r/burst; mkdir -p $d; "
                f"  for i in $(seq 1 200); do echo y > $d/f_$i; done; "
                f"  for i in $(seq 1 50); do mv $d/f_$i $d/n_$i; done; "
                f"done"
            ),
            timeout=600,
        )

        client1.upload_file(
            sudo=True,
            src="tests/cephfs/lib/mds_empty_omap_commit_soak.sh",
            dst="/root/mds_empty_omap_commit_soak.sh",
        )
        client1.exec_command(
            sudo=True, cmd="chmod +x /root/mds_empty_omap_commit_soak.sh"
        )

        soak_cmd = (
            f"BASE={base} SOAK_SECONDS={soak_seconds} "
            f"SLEEP_BETWEEN={sleep_between} "
            f"CREATES_PER_RANK={creates_per_rank} "
            f"RENAMES_PER_RANK={renames_per_rank} "
            f"WORKERS_PER_RANK={workers_per_rank} "
            f"APPLY_PINS=1 REQUIRE_PINS=1 "
            f"SOAK_LOG=/root/mds_empty_omap_commit_soak.log "
            f"/root/mds_empty_omap_commit_soak.sh"
        )
        log.info("Starting parallel multi-rank soak: %s", soak_cmd)
        out, _ = client1.exec_command(
            sudo=True, cmd=soak_cmd, timeout=soak_seconds + 1800
        )
        log.info("Soak output (tail):\n%s", "\n".join(out.splitlines()[-40:]))

        log.info("Validating MDS ranks still active")
        retry_mds_status(client1, 2, vol_name=default_fs)
        fs_status, _ = client1.exec_command(
            sudo=True, cmd=f"ceph fs status {default_fs} -f json"
        )
        log.info("fs status: %s", fs_status)

        crashes = fs_util.get_crash_ls_new(client1)
        if crashes:
            log.error("New crashes after empty omap soak: %s", json.dumps(crashes))
            raise CommandFailed(
                f"Unexpected ceph crash(es) after empty omap soak: {crashes}"
            )
        log.info("No new ceph crashes after soak")

        probe = f"{base}/rank_0/postsoak_probe"
        client1.exec_command(
            sudo=True,
            cmd=f"echo ok > {probe} && cat {probe} && rm -f {probe}",
        )

        log.info(
            "TEST PASSED | %s | empty omap soak | soak_seconds=%s no MDS crash",
            tc,
            soak_seconds,
        )
        return 0
    except CommandFailed as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        if client1:
            try:
                client1.exec_command(
                    sudo=True,
                    cmd=(
                        "pkill -f '/root/mds_empty_omap_commit_soak.sh' || true; "
                        "sleep 2"
                    ),
                    check_ec=False,
                )
            except Exception:
                pass
        if client1 and base:
            try:
                client1.exec_command(
                    sudo=True,
                    cmd=f"rm -rf -- '{base}'",
                    check_ec=False,
                    timeout=600,
                )
            except Exception:
                pass
        if fs_util and client1 and fuse_mounting_dir:
            try:
                fs_util.client_clean_up(
                    "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir
                )
            except Exception as e:
                log.warning("Cleanup umount warning: %s", e)
