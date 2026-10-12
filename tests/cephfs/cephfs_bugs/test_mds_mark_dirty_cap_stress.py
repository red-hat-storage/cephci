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
    CDir::mark_dirty get_version() < pv under concurrent cap/metadata stress.

    Steps:
      1. Fuse-mount FS
      2. Run concurrent Python cap-stress on one hot tree
         (create/write/append/truncate/unlink/rename/chmod)
      3. Assert no new MDS crashes; MDS ranks remain active; FS writable

    Config (optional):
      soak_seconds, workers, dirs
    """
    client1 = None
    fs_util = None
    fuse_mounting_dir = None
    try:
        log.info("Running mark_dirty / get_version cap-stress test")
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
        workers = int(config.get("workers", 12))
        dirs = int(config.get("dirs", 24))

        log.info("Archiving pre-existing crash records")
        client1.exec_command(sudo=True, cmd="ceph crash archive-all", check_ec=False)

        # Keep multi-MDS topology consistent with suite deploy (max_mds=2).
        client1.exec_command(sudo=True, cmd=f"ceph fs set {default_fs} max_mds 2")
        retry_mds_status = retry(CommandFailed, tries=10, delay=30)(
            fs_util.get_mds_status
        )
        retry_mds_status(client1, 2, vol_name=default_fs)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(8)
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse_{mounting_dir}_cap/"
        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir,
            extra_params=f" --client_fs {default_fs}",
        )

        hot = f"{fuse_mounting_dir.rstrip('/')}/test_mark_dirty/hot"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {hot}")

        client1.upload_file(
            sudo=True,
            src="tests/cephfs/lib/mds_mark_dirty_cap_stress.py",
            dst="/root/mds_mark_dirty_cap_stress.py",
        )
        stress_cmd = (
            f"python3 /root/mds_mark_dirty_cap_stress.py "
            f"--hot {hot} --workers {workers} --dirs {dirs} "
            f"--seconds {soak_seconds}"
        )
        log.info("Starting cap-stress: %s", stress_cmd)
        out, _ = client1.exec_command(
            sudo=True,
            cmd=stress_cmd,
            timeout=soak_seconds + 1800,
        )
        log.info("Cap-stress output (tail):\n%s", "\n".join(out.splitlines()[-40:]))
        if "DONE" not in out:
            raise CommandFailed("Cap-stress did not report DONE")
        if "hard_err=" in out:
            for line in reversed(out.splitlines()):
                if line.startswith("DONE"):
                    if "hard_err=0" not in line:
                        raise CommandFailed(f"Cap-stress reported hard errors: {line}")
                    break

        log.info("Validating MDS ranks still active")
        retry_mds_status(client1, 2, vol_name=default_fs)

        crashes = fs_util.get_crash_ls_new(client1)
        if crashes:
            log.error("New crashes after mark_dirty soak: %s", json.dumps(crashes))
            raise CommandFailed(
                f"Unexpected ceph crash(es) after mark_dirty soak: {crashes}"
            )
        log.info("No new ceph crashes after soak")

        probe = f"{hot}/postsoak_probe"
        client1.exec_command(
            sudo=True,
            cmd=f"echo ok > {probe} && cat {probe} && rm -f {probe}",
        )

        log.info(
            "TEST PASSED | mark_dirty cap-stress | soak_seconds=%s workers=%s "
            "dirs=%s no MDS crash",
            soak_seconds,
            workers,
            dirs,
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
        if client1 and fuse_mounting_dir:
            try:
                client1.exec_command(
                    sudo=True,
                    cmd=f"rm -rf {fuse_mounting_dir.rstrip('/')}/test_mark_dirty",
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
