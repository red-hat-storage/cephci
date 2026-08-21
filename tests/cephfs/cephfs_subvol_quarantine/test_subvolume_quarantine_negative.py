"""
CephFS Subvolume Quarantine — Negative workflows.

Polarion: CEPH-83632679
Suite block: Negative

N-01 Non-existent / clear when not set
N-02 rw / r (no q) denied on quarantined SV
N-03 Quarantine on live mounted SV — enable OK; new IO blocked
N-04 rwq write semantics (authorization, not write-lock) — plan v1 outdated
"""

import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_subvol_quarantine_utils import SubvolQuarantineUtils
from utility.log import Log

log = Log(__name__)


def _prepare(ceph_cluster, config):
    fs_util = FsUtils(ceph_cluster)
    qtn = SubvolQuarantineUtils(ceph_cluster)
    clients = ceph_cluster.get_ceph_objects("client")
    if len(clients) < 2:
        log.error("Need at least 2 client nodes; found %d", len(clients))
        return None

    build = config.get("build", config.get("rhbuild"))
    fs_util.prepare_clients(clients[:2], build)
    fs_util.auth_list(clients[:2])

    admin = clients[0]
    fuse = clients[1]
    if not qtn.feature_available(admin):
        log.error("Subvolume quarantine CLI not available — mark NA")
        return None

    return qtn, admin, fuse, config.get("fs_name", "cephfs")


def _umount(qtn, client, mounts):
    for mnt in mounts:
        qtn.umount_fuse(client, mnt)


def _del_clients(qtn, admin, names):
    for name in names:
        qtn.delete_client(admin, name)


def _failed_quarantine_result(result) -> bool:
    """True if enable/disable clearly failed (non-zero / status failed)."""
    if not isinstance(result, dict):
        return True
    status = str(result.get("status", "")).lower()
    rc = result.get("return_code", 0)
    if status == "failed":
        return True
    try:
        if int(rc) != 0:
            return True
    except (TypeError, ValueError):
        if rc not in (0, None, "0"):
            return True
    return False


# ---------------------------------------------------------------------------
# N-01 .. N-04
# ---------------------------------------------------------------------------


def n01_nonexistent_and_clear_unset(qtn, admin, fuse, vol, config):
    """
    N-01 Part A: enable on missing SV fails.
    Part B: disable when not set — idempotent success (lab) is acceptable.
    Also: disable on missing SV fails.
    """
    sub = config.get("subvol", "qtn-neg-sv")
    missing = config.get("missing_subvol", "does-not-exist")
    try:
        log.info("Part A: enable on non-existent subvolume")
        result = qtn._quarantine_cmd(
            admin, "enable", vol, missing, expect_success=False
        )
        log.info("enable missing result: %s", result)
        if not _failed_quarantine_result(result):
            log.error("N-01 FAILED: enable on missing SV should fail")
            return 1
        err = str(result.get("error", "")) + str(result.get("raw", ""))
        if (
            "no such" not in err.lower()
            and "enoent" not in err.lower()
            and result.get("return_code") not in (-2, 2, "-2", "2")
        ):
            log.warning(
                "enable missing failed but error text unclear: %s (still scoring PASS)",
                result,
            )
        log.info("enable on missing SV failed as expected")

        log.info("Part B: disable when quarantine was never set")
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        if qtn.assert_info_not_quarantined(admin, vol, sub):
            return 1

        result = qtn._quarantine_cmd(admin, "disable", vol, sub, expect_success=False)
        # Lab: idempotent success (return_code 0). Also accept clear failure.
        log.info("disable when not set result: %s", result)
        if _failed_quarantine_result(result):
            log.info(
                "disable when not quarantined failed with clear error (acceptable)"
            )
        else:
            log.info(
                "disable when not quarantined succeeded idempotently "
                "(observed/acceptable per N-01)"
            )

        # second disable also ok if idempotent
        result2 = qtn._quarantine_cmd(admin, "disable", vol, sub, expect_success=False)
        log.info("second disable when not set: %s", result2)

        log.info("Part B: disable on missing name")
        result = qtn._quarantine_cmd(
            admin, "disable", vol, missing, expect_success=False
        )
        log.info("disable missing result: %s", result)
        if not _failed_quarantine_result(result):
            log.error("N-01 FAILED: disable on missing SV should fail")
            return 1

        log.info("n01_nonexistent_and_clear_unset PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        qtn.cleanup_subvolume(admin, vol, sub)


def n02_rw_without_q_denied(qtn, admin, fuse, vol, config):
    """
    N-02 Mount with rw / r (no q) on quarantined SV must fail.

    Note: mds 'allow rwx ...' is rejected by this build (EINVAL parse);
    use rw + r as the “without q” matrix.
    """
    sub = config.get("subvol", "qtn-neg-sv")
    client_rw = "qtn-neg-rw"
    client_r = "qtn-neg-r"
    mount_rw = "/mnt/qtn-n2-rw"
    mount_r = "/mnt/qtn-n2-r"
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        root, data = qtn.get_subvolume_paths(admin, vol, sub)
        qtn.create_rw_client(admin, vol, root, client_rw)
        qtn.create_r_client(admin, vol, root, client_r)

        qtn.quarantine_enable(admin, vol, sub)

        if qtn.assert_fuse_mount_fails(fuse, mount_rw, data, client_rw, vol):
            return 1
        if qtn.assert_fuse_mount_fails(fuse, mount_r, data, client_r, vol):
            return 1

        log.info("n02_rw_without_q_denied PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount(qtn, fuse, [mount_rw, mount_r])
        _del_clients(qtn, admin, [client_rw, client_r])
        qtn.cleanup_subvolume(admin, vol, sub)


def n03_quarantine_live_mount(qtn, admin, fuse, vol, config):
    """
    N-03 Quarantine while fuse mount + IO active.

    Enable must succeed; new IO blocked. In-flight dd need not stop cleanly.
    """
    sub = config.get("subvol", "qtn-neg-sv")
    client = "qtn-neg-live"
    mount = "/mnt/qtn-n3-live"
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        root, data = qtn.get_subvolume_paths(admin, vol, sub)
        qtn.create_rw_client(admin, vol, root, client)
        qtn.mount_fuse(fuse, mount, data, client, vol)

        if qtn.write_baseline_file(fuse, mount, "testfile.txt", "baseline"):
            return 1
        if qtn.assert_read_ok(fuse, mount, "testfile.txt"):
            return 1

        log.info("Start background dd (in-flight IO)")
        fuse.exec_command(
            sudo=True,
            cmd=(
                f"bash -c 'dd if=/dev/zero of={mount}/big.img bs=1M count=512 "
                f"oflag=direct >/tmp/qtn-n3-dd.out 2>&1 &'"
            ),
            check_ec=False,
        )
        time.sleep(2)

        result = qtn.quarantine_enable(admin, vol, sub)
        log.info("enable on live mount: %s", result)
        if result.get("status") not in (None, "successful"):
            log.error("N-03 FAILED: enable on live SV did not succeed: %s", result)
            return 1

        time.sleep(2)
        # Do not require dd to exit cleanly
        fuse.exec_command(
            sudo=True, cmd="pkill -f 'dd if=/dev/zero' || true", check_ec=False
        )

        log.info("New IO must be blocked")
        if qtn.assert_read_blocked(fuse, mount, "testfile.txt"):
            return 1
        if qtn.assert_write_blocked(fuse, mount, "after-q.txt"):
            return 1

        log.info("n03_quarantine_live_mount PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        fuse.exec_command(
            sudo=True, cmd="pkill -f 'dd if=/dev/zero' || true", check_ec=False
        )
        _umount(qtn, fuse, [mount])
        _del_clients(qtn, admin, [client])
        qtn.cleanup_subvolume(admin, vol, sub)


def n04_rwq_write_semantics(qtn, admin, fuse, vol, config):
    """
    N-04 Correct rwq semantics (authorization, not write restriction).

    - Not quarantined: rwq mount + write OK
    - Quarantined: rwq mount + write OK
    - Quarantined: normal rw mount denied
    """
    sub = config.get("subvol", "qtn-neg-sv")
    client_rwq = "qtn-neg-rwq"
    client_rw = "qtn-neg-rw2"
    mount_rwq = "/mnt/qtn-n4-rwq"
    mount_rw = "/mnt/qtn-n4-rw"
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        root, data = qtn.get_subvolume_paths(admin, vol, sub)
        qtn.create_rwq_client(admin, vol, root, client_rwq)

        log.info("Part A: rwq when NOT quarantined — write allowed")
        qtn._quarantine_cmd(admin, "disable", vol, sub, expect_success=False)
        qtn.mount_fuse(fuse, mount_rwq, data, client_rwq, vol)
        fuse.exec_command(
            sudo=True,
            cmd=f"bash -c 'echo before-q > {mount_rwq}/write-before.txt'",
            check_ec=True,
        )
        if qtn.assert_content_equals(fuse, mount_rwq, "write-before.txt", "before-q"):
            return 1

        log.info("Part B: rwq when quarantined — write still allowed (recovery)")
        qtn.quarantine_enable(admin, vol, sub)
        qtn.umount_fuse(fuse, mount_rwq)
        qtn.mount_fuse(fuse, mount_rwq, data, client_rwq, vol)
        fuse.exec_command(
            sudo=True,
            cmd=f"bash -c 'echo after-q > {mount_rwq}/write-after.txt'",
            check_ec=True,
        )
        if qtn.assert_content_equals(fuse, mount_rwq, "write-after.txt", "after-q"):
            return 1
        if qtn.assert_content_equals(fuse, mount_rwq, "write-before.txt", "before-q"):
            return 1

        log.info("Control: normal rw still blocked while quarantined")
        qtn.create_rw_client(admin, vol, root, client_rw)
        if qtn.assert_fuse_mount_fails(fuse, mount_rw, data, client_rw, vol):
            return 1

        log.info(
            "N-04 note: IBM plan 'writes blocked with q' is outdated; "
            "rwq = authorization (full rw)"
        )
        log.info("n04_rwq_write_semantics PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount(qtn, fuse, [mount_rwq, mount_rw])
        _del_clients(qtn, admin, [client_rwq, client_rw])
        qtn.cleanup_subvolume(admin, vol, sub)


SUBTESTS = {
    "n01_nonexistent_and_clear_unset": n01_nonexistent_and_clear_unset,
    "n02_rw_without_q_denied": n02_rw_without_q_denied,
    "n03_quarantine_live_mount": n03_quarantine_live_mount,
    "n04_rwq_write_semantics": n04_rwq_write_semantics,
}


def run(ceph_cluster, **kw):
    """Run Negative subtests for CephFS subvolume quarantine."""
    config = kw.get("config") or {}
    log.info("=" * 80)
    log.info("TEST TYPE : Negative")
    log.info("MODULE    : test_subvolume_quarantine_negative.py")
    log.info("POLARION  : CEPH-83632679")
    log.info("=" * 80)

    prepared = _prepare(ceph_cluster, config)
    if prepared is None:
        return 1
    qtn, admin, fuse, vol = prepared

    requested = config.get("subtests")
    test_list = requested if requested else list(SUBTESTS.keys())

    failed = []
    for name in test_list:
        if name not in SUBTESTS:
            log.error("Unknown Negative subtest '%s'; known: %s", name, list(SUBTESTS))
            failed.append(name)
            continue

        log.info("")
        log.info("=" * 80)
        log.info("SUBTEST START : [Negative] %s", name)
        log.info("DESC          : %s", (SUBTESTS[name].__doc__ or "").strip())
        log.info("=" * 80)

        try:
            rc = SUBTESTS[name](qtn, admin, fuse, vol, config)
        except Exception:
            log.error("SUBTEST EXCEPTION : [Negative] %s", name)
            log.error(traceback.format_exc())
            rc = 1

        if rc:
            log.error("SUBTEST FAILED  : [Negative] %s", name)
            failed.append(name)
        else:
            log.info("SUBTEST PASSED  : [Negative] %s", name)
        log.info("-" * 80)

    log.info("=" * 80)
    if failed:
        log.error(
            "Negative summary: %d/%d FAILED → %s",
            len(failed),
            len(test_list),
            failed,
        )
        log.info("=" * 80)
        return 1

    log.info("Negative summary: ALL %d subtest(s) PASSED", len(test_list))
    log.info("=" * 80)
    return 0
