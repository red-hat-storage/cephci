"""
CephFS Subvolume Quarantine — Disruptive workflows.

Polarion: CEPH-83632682 (Resilience)
Suite block: Disruptive

D-01 MDS restart / fail — quarantine retained; rw still blocked; rwq OK
D-02 FS fail + joinable recover — quarantine retained
D-03 Active MDS host reboot — quarantine retained
D-04 MGR then MON orch restart — still blocked; restore after disable
"""

from __future__ import annotations

import json
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_subvol_quarantine_utils import (
    SubvolQuarantineUtils,
    skip_quarantine_tests_unless_supported,
)
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

    return qtn, fs_util, admin, fuse, config.get("fs_name", "cephfs"), ceph_cluster


def _umount(qtn, client, mounts):
    for mnt in mounts:
        qtn.umount_fuse(client, mnt)


def _del_clients(qtn, admin, names):
    for name in names:
        qtn.delete_client(admin, name)


def _wait_for_active_mds(admin, vol, timeout=180):
    """Block until at least one MDS is active for *vol*."""
    end = time.time() + timeout
    while time.time() < end:
        try:
            active = FsUtils.get_active_mdss(admin, vol)
        except Exception as exc:
            log.warning("get_active_mdss failed: %s", exc)
            active = []
        if active:
            log.info("Active MDS: %s", active)
            return active
        time.sleep(5)
    log.error("Timed out waiting for active MDS on %s", vol)
    return []


def _resolve_mds_daemon(admin, mds_name):
    """
    Return (daemon_name, hostname) for an MDS name from fs status.

    Matches orch ps entries by daemon_id / daemon_name containing *mds_name*.
    """
    out, _ = admin.exec_command(
        sudo=True, cmd="ceph orch ps --daemon_type mds -f json", check_ec=False
    )
    try:
        daemons = json.loads(out) if out else []
    except json.JSONDecodeError:
        daemons = []

    for d in daemons:
        did = str(d.get("daemon_id", "") or "")
        dname = str(d.get("daemon_name", "") or "")
        if not dname and did:
            dname = f"mds.{did}"
        if mds_name in did or mds_name in dname or did.endswith(mds_name):
            if not dname.startswith("mds."):
                dname = f"mds.{dname}" if not dname.startswith("mds") else dname
            return dname, d.get("hostname")
    return f"mds.{mds_name}", None


def _orch_daemon_restart(admin, daemon_name):
    admin.exec_command(
        sudo=True,
        cmd=f"ceph orch daemon restart {daemon_name}",
        check_ec=False,
    )


def _first_orch_daemon(admin, daemon_type):
    out, _ = admin.exec_command(
        sudo=True,
        cmd=f"ceph orch ps --daemon_type {daemon_type} -f json",
        check_ec=False,
    )
    try:
        daemons = json.loads(out) if out else []
    except json.JSONDecodeError:
        daemons = []
    for d in daemons:
        status = str(d.get("status_desc", "") or "").lower()
        if "running" not in status and daemons:
            continue
        dname = str(d.get("daemon_name", "") or "")
        did = str(d.get("daemon_id", "") or "")
        if not dname:
            dname = f"{daemon_type}.{did}" if did else ""
        if dname:
            return dname
    if daemons:
        d = daemons[0]
        dname = str(d.get("daemon_name", "") or "")
        did = str(d.get("daemon_id", "") or "")
        return dname or (f"{daemon_type}.{did}" if did else None)
    return None


def _setup_quarantined_sv(qtn, admin, fuse, vol, sub, client_rw, mount_rw, content):
    """Create SV, baseline file, enable quarantine. Returns (root, data) or None."""
    if qtn.setup_subvolume(admin, vol, sub):
        return None
    root, data = qtn.get_subvolume_paths(admin, vol, sub)
    qtn.create_rw_client(admin, vol, root, client_rw)
    qtn.mount_fuse(fuse, mount_rw, data, client_rw, vol_name=vol)
    if qtn.write_baseline_file(fuse, mount_rw, "t.txt", content):
        _umount(qtn, fuse, [mount_rw])
        return None
    qtn.umount_fuse(fuse, mount_rw)
    qtn.quarantine_enable(admin, vol, sub)
    return root, data


# ---------------------------------------------------------------------------
# D-01 .. D-04
# ---------------------------------------------------------------------------


def d01_mds_restart_retains(qtn, fs_util, admin, fuse, vol, config, ceph_cluster):
    """
    D-01: After active MDS fail/restart, quarantine remains.
    Normal fuse mount denied; rwq recovery still works.
    """
    sub = config.get("sub_d01", "qtn-d01")
    client_rw = config.get("client_d01", "qtn-d01-rw")
    client_rwq = config.get("client_d01q", "qtn-d01-rwq")
    mount_rw = config.get("mount_d01", "/mnt/qtn-d01")
    mount_rwq = config.get("mount_d01q", "/mnt/qtn-d01-q")
    content = "before-mds-restart"
    mounts = [mount_rw, mount_rwq]
    clients = [client_rw, client_rwq]

    try:
        paths = _setup_quarantined_sv(
            qtn, admin, fuse, vol, sub, client_rw, mount_rw, content
        )
        if paths is None:
            return 1
        root, data = paths
        qtn.create_rwq_client(admin, vol, root, client_rwq)

        if qtn.assert_fuse_mount_fails(fuse, mount_rw, data, client_rw, vol_name=vol):
            return 1

        active = _wait_for_active_mds(admin, vol)
        if not active:
            return 1
        mds_name = active[0]
        daemon_name, hostname = _resolve_mds_daemon(admin, mds_name)
        log.info(
            "Failing/restarting MDS name=%s daemon=%s host=%s",
            mds_name,
            daemon_name,
            hostname,
        )

        admin.exec_command(sudo=True, cmd=f"ceph mds fail {mds_name}", check_ec=False)
        time.sleep(5)
        _orch_daemon_restart(admin, daemon_name)
        time.sleep(10)

        if not _wait_for_active_mds(admin, vol, timeout=240):
            return 1
        time.sleep(5)

        if qtn.assert_info_quarantined(admin, vol, sub):
            log.error("D-01: info no longer shows quarantined after MDS restart")
            return 1

        if qtn.assert_fuse_mount_fails(fuse, mount_rw, data, client_rw, vol_name=vol):
            log.error("D-01: normal mount succeeded after MDS restart")
            return 1

        qtn.mount_fuse(fuse, mount_rwq, data, client_rwq, vol_name=vol)
        if qtn.assert_content_equals(fuse, mount_rwq, "t.txt", content):
            return 1
        if qtn.assert_write_ok(fuse, mount_rwq, "recovery.txt"):
            return 1

        log.info("d01_mds_restart_retains PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount(qtn, fuse, mounts)
        try:
            qtn.quarantine_disable(admin, vol, sub)
        except Exception:
            pass
        qtn.cleanup_subvolume(admin, vol, sub)
        _del_clients(qtn, admin, clients)


def d02_fs_fail_recover_retains(qtn, fs_util, admin, fuse, vol, config, ceph_cluster):
    """
    D-02: ceph fs fail + joinable true — quarantine retained; mount still denied.
    """
    sub = config.get("sub_d02", "qtn-d02")
    client_rw = config.get("client_d02", "qtn-d02-rw")
    mount_rw = config.get("mount_d02", "/mnt/qtn-d02")
    content = "before-fs-fail"
    mounts = [mount_rw]
    clients = [client_rw]

    try:
        paths = _setup_quarantined_sv(
            qtn, admin, fuse, vol, sub, client_rw, mount_rw, content
        )
        if paths is None:
            return 1
        _root, data = paths

        admin.exec_command(sudo=True, cmd=f"ceph fs fail {vol}", check_ec=False)
        time.sleep(5)
        admin.exec_command(
            sudo=True, cmd=f"ceph fs set {vol} joinable true", check_ec=False
        )

        if not _wait_for_active_mds(admin, vol, timeout=300):
            return 1
        # Allow MDS to re-load quarantine state from journal/optmetadata
        time.sleep(15)

        if qtn.assert_info_quarantined(admin, vol, sub):
            log.error("D-02: quarantine not retained after fs fail/recover")
            return 1

        if qtn.assert_fuse_mount_fails(fuse, mount_rw, data, client_rw, vol_name=vol):
            log.error("D-02: normal mount succeeded after fs fail/recover")
            return 1

        log.info("d02_fs_fail_recover_retains PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount(qtn, fuse, mounts)
        try:
            admin.exec_command(
                sudo=True, cmd=f"ceph fs set {vol} joinable true", check_ec=False
            )
            _wait_for_active_mds(admin, vol, timeout=120)
        except Exception:
            pass
        try:
            qtn.quarantine_disable(admin, vol, sub)
        except Exception:
            pass
        qtn.cleanup_subvolume(admin, vol, sub)
        _del_clients(qtn, admin, clients)


def d03_mds_host_reboot_retains(qtn, fs_util, admin, fuse, vol, config, ceph_cluster):
    """
    D-03: Reboot host running active MDS — quarantine retained.

    Skips (PASS/NA) when config skip_mds_reboot=true or MDS host cannot be
    resolved — reboot is destructive and may be unsuitable for shared labs.
    """
    if config.get("skip_mds_reboot", False):
        log.info("D-03 N/A: skip_mds_reboot=true in config")
        return 0

    sub = config.get("sub_d03", "qtn-d03")
    client_rw = config.get("client_d03", "qtn-d03-rw")
    mount_rw = config.get("mount_d03", "/mnt/qtn-d03")
    content = "before-mds-reboot"
    mounts = [mount_rw]
    clients = [client_rw]

    try:
        paths = _setup_quarantined_sv(
            qtn, admin, fuse, vol, sub, client_rw, mount_rw, content
        )
        if paths is None:
            return 1
        _root, data = paths

        active = _wait_for_active_mds(admin, vol)
        if not active:
            return 1
        mds_name = active[0]
        _daemon_name, hostname = _resolve_mds_daemon(admin, mds_name)
        if not hostname:
            log.warning("D-03 N/A: could not resolve hostname for MDS %s", mds_name)
            return 0

        mds_node = ceph_cluster.get_node_by_hostname(hostname)
        if mds_node is None:
            log.warning(
                "D-03 N/A: no cluster node for hostname %s (MDS %s)",
                hostname,
                mds_name,
            )
            return 0

        log.info("Rebooting MDS host %s (MDS %s)", hostname, mds_name)
        fs_util.reboot_node_v1(mds_node)
        if not _wait_for_active_mds(admin, vol, timeout=600):
            return 1
        time.sleep(15)

        if qtn.assert_info_quarantined(admin, vol, sub):
            log.error("D-03: quarantine not retained after MDS host reboot")
            return 1

        if qtn.assert_fuse_mount_fails(fuse, mount_rw, data, client_rw, vol_name=vol):
            log.error("D-03: normal mount succeeded after MDS host reboot")
            return 1

        log.info("d03_mds_host_reboot_retains PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount(qtn, fuse, mounts)
        try:
            qtn.quarantine_disable(admin, vol, sub)
        except Exception:
            pass
        qtn.cleanup_subvolume(admin, vol, sub)
        _del_clients(qtn, admin, clients)


def d04_mgr_mon_restart_still_blocked(
    qtn, fs_util, admin, fuse, vol, config, ceph_cluster
):
    """
    D-04: After MGR and MON orch restart, quarantined SV still blocks normal mount;
    disable restores access.
    """
    sub = config.get("sub_d04", "qtn-d04")
    client_rw = config.get("client_d04", "qtn-d04-rw")
    mount_rw = config.get("mount_d04", "/mnt/qtn-d04")
    content = "before-mgr-mon-restart"
    mounts = [mount_rw]
    clients = [client_rw]

    try:
        paths = _setup_quarantined_sv(
            qtn, admin, fuse, vol, sub, client_rw, mount_rw, content
        )
        if paths is None:
            return 1
        _root, data = paths

        if qtn.assert_fuse_mount_fails(fuse, mount_rw, data, client_rw, vol_name=vol):
            return 1

        mgr_daemon = _first_orch_daemon(admin, "mgr")
        if not mgr_daemon:
            log.error("D-04: no MGR daemon found via orch ps")
            return 1
        log.info("Restarting MGR daemon %s", mgr_daemon)
        _orch_daemon_restart(admin, mgr_daemon)
        time.sleep(15)

        mon_daemon = _first_orch_daemon(admin, "mon")
        if not mon_daemon:
            log.error("D-04: no MON daemon found via orch ps")
            return 1
        log.info("Restarting MON daemon %s", mon_daemon)
        _orch_daemon_restart(admin, mon_daemon)
        time.sleep(20)

        admin.exec_command(sudo=True, cmd="ceph -s", check_ec=False)

        if qtn.assert_info_quarantined(admin, vol, sub):
            log.error("D-04: quarantine lost after MGR/MON restart")
            return 1

        if qtn.assert_fuse_mount_fails(fuse, mount_rw, data, client_rw, vol_name=vol):
            log.error("D-04: normal mount succeeded after MGR/MON restart")
            return 1

        qtn.quarantine_disable(admin, vol, sub)
        qtn.mount_fuse(fuse, mount_rw, data, client_rw, vol_name=vol)
        if qtn.assert_content_equals(fuse, mount_rw, "t.txt", content):
            return 1
        if qtn.assert_write_ok(fuse, mount_rw, "after-disable.txt"):
            return 1

        log.info("d04_mgr_mon_restart_still_blocked PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount(qtn, fuse, mounts)
        try:
            qtn.quarantine_disable(admin, vol, sub)
        except Exception:
            pass
        qtn.cleanup_subvolume(admin, vol, sub)
        _del_clients(qtn, admin, clients)


SUBTESTS = {
    "d01_mds_restart_retains": d01_mds_restart_retains,
    "d02_fs_fail_recover_retains": d02_fs_fail_recover_retains,
    "d03_mds_host_reboot_retains": d03_mds_host_reboot_retains,
    "d04_mgr_mon_restart_still_blocked": d04_mgr_mon_restart_still_blocked,
}


def run(ceph_cluster, **kw):
    """Run Disruptive subtests for CephFS subvolume quarantine."""
    config = kw.get("config") or {}
    if skip_quarantine_tests_unless_supported(config, ceph_cluster):
        return 0

    log.info("=" * 80)
    log.info("TEST TYPE : Disruptive")
    log.info("MODULE    : test_subvolume_quarantine_disruptive.py")
    log.info("POLARION  : CEPH-83632682")
    log.info("=" * 80)

    prepared = _prepare(ceph_cluster, config)
    if prepared is None:
        return 1
    qtn, fs_util, admin, fuse, vol, cluster = prepared

    requested = config.get("subtests")
    test_list = requested if requested else list(SUBTESTS.keys())

    # Default: skip host reboot in CI unless explicitly enabled
    if "skip_mds_reboot" not in config:
        config = dict(config)
        config["skip_mds_reboot"] = True
        log.info(
            "D-03 default skip_mds_reboot=true (set skip_mds_reboot=false to enable)"
        )

    failed = []
    for name in test_list:
        if name not in SUBTESTS:
            log.error(
                "Unknown Disruptive subtest '%s'; known: %s", name, list(SUBTESTS)
            )
            failed.append(name)
            continue

        log.info("")
        log.info("=" * 80)
        log.info("SUBTEST START : [Disruptive] %s", name)
        log.info("DESC          : %s", (SUBTESTS[name].__doc__ or "").strip())
        log.info("=" * 80)

        try:
            rc = SUBTESTS[name](qtn, fs_util, admin, fuse, vol, config, cluster)
        except Exception:
            log.error("SUBTEST EXCEPTION : [Disruptive] %s", name)
            log.error(traceback.format_exc())
            rc = 1

        if rc:
            log.error("SUBTEST FAILED  : [Disruptive] %s", name)
            failed.append(name)
        else:
            log.info("SUBTEST PASSED  : [Disruptive] %s", name)
        log.info("-" * 80)

    log.info("=" * 80)
    if failed:
        log.error(
            "Disruptive summary: %d/%d FAILED → %s",
            len(failed),
            len(test_list),
            failed,
        )
        log.info("=" * 80)
        return 1

    log.info("Disruptive summary: ALL %d subtest(s) PASSED", len(test_list))
    log.info("=" * 80)
    return 0
