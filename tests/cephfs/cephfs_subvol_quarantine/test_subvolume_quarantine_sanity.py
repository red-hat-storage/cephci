"""
CephFS Subvolume Quarantine — Sanity (Acceptance) workflows.

Polarion: CEPH-83632677
Suite block: Sanity

Acceptance TCs (primary):
  acceptance1_quarantine_flag   — enable succeeds; full info (quarantine=disabled) then minimal (enabled)
  acceptance2_blocks_normal     — normal (no q) mount/I/O blocked
  acceptance3_rwq_recovery      — rwq recovery client retains access; normal stays blocked

Additional Sanity coverage (prior lab phases, no workarounds):
  prereq_feature_health
  lifecycle_disable_restore     — disable restores full access; baseline intact
  isolation_sibling             — sibling subvolume unaffected
  mds_asok                      — ceph tell mds.<id> quarantine enable|disable
  e2e_incident_workflow         — full incident checklist
"""

import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_subvol_quarantine_utils import SubvolQuarantineUtils
from utility.log import Log

log = Log(__name__)

BASELINE_A = "baseline-tenant-a"
BASELINE_B = "baseline-tenant-b"
BASELINE_FILE = "testfile.txt"


class LabContext:
    """Shared tenant lab layout for Sanity subtests."""

    def __init__(self, qtn, admin, fuse_client, vol_name, config):
        self.qtn = qtn
        self.admin = admin
        self.fuse = fuse_client
        self.vol = vol_name
        self.sub_a = config.get("subvol_a", "qtn-tenant-a")
        self.sub_b = config.get("subvol_b", "qtn-tenant-b")
        self.mount_a = config.get("mount_a", "/mnt/qtn-normal")
        self.mount_b = config.get("mount_b", "/mnt/qtn-tenant-b")
        self.mount_recovery = config.get("mount_recovery", "/mnt/qtn-recovery")
        self.client_a = config.get("client_a", "qtn-normal")
        self.client_b = config.get("client_b", "qtn-tenant-b")
        self.client_recovery = config.get("client_recovery", "qtn-recovery")
        self.root_a = self.data_a = None
        self.root_b = self.data_b = None
        self._with_b = False
        self._with_recovery = False
        self._mounted_a = False

    def create_subvolume_a(self):
        log.info("Create subvolume %s", self.sub_a)
        if self.qtn.setup_subvolume(self.admin, self.vol, self.sub_a):
            raise CommandFailed(f"failed to create {self.sub_a}")
        self.root_a, self.data_a = self.qtn.get_subvolume_paths(
            self.admin, self.vol, self.sub_a
        )
        log.info("DATA=%s ROOT=%s", self.data_a, self.root_a)

    def setup(
        self,
        with_b=False,
        with_recovery=False,
        mount_a=True,
        write_baseline=True,
    ):
        self._with_b = with_b
        self._with_recovery = with_recovery
        self.create_subvolume_a()

        self.qtn.create_rw_client(self.admin, self.vol, self.root_a, self.client_a)
        if mount_a:
            self.qtn.mount_fuse(
                self.fuse, self.mount_a, self.data_a, self.client_a, self.vol
            )
            self._mounted_a = True
            if write_baseline and self.qtn.write_baseline_file(
                self.fuse, self.mount_a, BASELINE_FILE, BASELINE_A
            ):
                raise CommandFailed("failed to write baseline on tenant-a")

        if with_b:
            log.info("Create sibling subvolume %s", self.sub_b)
            if self.qtn.setup_subvolume(self.admin, self.vol, self.sub_b):
                raise CommandFailed(f"failed to create {self.sub_b}")
            self.root_b, self.data_b = self.qtn.get_subvolume_paths(
                self.admin, self.vol, self.sub_b
            )
            self.qtn.create_rw_client(self.admin, self.vol, self.root_b, self.client_b)
            self.qtn.mount_fuse(
                self.fuse, self.mount_b, self.data_b, self.client_b, self.vol
            )
            if self.qtn.write_baseline_file(
                self.fuse, self.mount_b, BASELINE_FILE, BASELINE_B
            ):
                raise CommandFailed("failed to write baseline on tenant-b")

        if with_recovery:
            self.qtn.create_rwq_client(
                self.admin, self.vol, self.root_a, self.client_recovery
            )

        log.info("Lab setup complete")

    def cleanup(self):
        log.info("Lab cleanup starting")
        for mnt in (self.mount_recovery, self.mount_a, self.mount_b):
            self.qtn.umount_fuse(self.fuse, mnt)
        for cname in (self.client_a, self.client_b, self.client_recovery):
            self.qtn.delete_client(self.admin, cname)
        self.qtn.cleanup_subvolume(self.admin, self.vol, self.sub_a)
        if self._with_b:
            self.qtn.cleanup_subvolume(self.admin, self.vol, self.sub_b)
        log.info("Lab cleanup done")


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
    fuse_client = clients[1]
    vol_name = config.get("fs_name", "cephfs")
    return qtn, admin, fuse_client, vol_name


# ---------------------------------------------------------------------------
# Subtests
# ---------------------------------------------------------------------------


def subtest_prereq_feature_health(qtn, admin, fuse_client, vol_name, config):
    """Confirm feature CLI, HEALTH_OK, volume present, volumes module on."""
    out, _ = admin.exec_command(sudo=True, cmd="ceph versions", check_ec=False)
    log.info("ceph versions:\n%s", out)

    if not qtn.feature_available(admin):
        log.error("quarantine enable/disable CLI not present")
        return 1
    log.info("quarantine CLI present")

    health, _ = admin.exec_command(sudo=True, cmd="ceph health")
    health = health.strip()
    log.info("ceph health: %s", health)
    if "HEALTH_OK" not in health:
        log.error("cluster not HEALTH_OK: %s", health)
        return 1

    vols, _ = admin.exec_command(sudo=True, cmd="ceph fs volume ls")
    if vol_name not in vols:
        log.error("volume %s not in fs volume ls: %s", vol_name, vols)
        return 1

    mods, _ = admin.exec_command(sudo=True, cmd="ceph mgr module ls")
    if "volumes" not in mods:
        log.error("volumes module not listed")
        return 1

    log.info("prereq_feature_health PASSED")
    return 0


def subtest_acceptance1_quarantine_flag(qtn, admin, fuse_client, vol_name, config):
    """
    Acceptance 1 — Quarantine flag can be set on a subvolume.

    Expected before enable:
      - full subvolume info (path, size, pools, etc.)
      - quarantine field is "disabled" (string), not enabled
    After enable:
      - subvolume ls still lists the subvolume (names only, no quarantine field)
      - subvolume getpath is blocked while quarantined
      - subvolume info is minimal with quarantine "enabled"
    After disable:
      - full info restored with quarantine "disabled"
    """
    lab = LabContext(qtn, admin, fuse_client, vol_name, config)
    try:
        lab.create_subvolume_a()

        log.info("Confirm subvolume exists and is not quarantined")
        if qtn.assert_subvolume_ls_complete(admin, vol_name, [lab.sub_a]):
            return 1
        if qtn.assert_info_not_quarantined(admin, vol_name, lab.sub_a):
            return 1

        log.info("Set quarantine: enable %s", lab.sub_a)
        result = qtn.quarantine_enable(admin, vol_name, lab.sub_a)
        log.info("enable result: %s", result)
        if result.get("status") != "successful" and result.get("return_code", 0) != 0:
            log.error("enable did not return success: %s", result)
            return 1

        log.info("Re-check: subvolume ls must still list %s (names only)", lab.sub_a)
        if qtn.assert_subvolume_ls_complete(admin, vol_name, [lab.sub_a]):
            return 1

        log.info("Re-check: getpath must be blocked while quarantined")
        if qtn.assert_mgr_op_fails(
            admin,
            f"ceph fs subvolume getpath {vol_name} {lab.sub_a}",
            "getpath while quarantined",
        ):
            return 1

        log.info("Re-check: subvolume info minimal quarantine status")
        if qtn.assert_info_quarantined(admin, vol_name, lab.sub_a):
            return 1

        log.info("Cleanup: quarantine disable")
        qtn.quarantine_disable(admin, vol_name, lab.sub_a)
        if qtn.assert_info_not_quarantined(admin, vol_name, lab.sub_a):
            log.error("info still looks quarantined after disable")
            return 1

        log.info("acceptance1_quarantine_flag PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        lab.cleanup()


def subtest_acceptance2_blocks_normal(qtn, admin, fuse_client, vol_name, config):
    """
    Acceptance 2 — Quarantined subvolume blocks standard mount (no q).

    Covers:
      A) Live mount then enable → read/write EACCES
      B) Fresh mount after enable → Permission denied (mount fails)
    """
    lab = LabContext(qtn, admin, fuse_client, vol_name, config)
    try:
        # --- Sequence A: mount first, then quarantine ---
        lab.setup(mount_a=True, write_baseline=True)

        log.info("A: baseline I/O before quarantine")
        if qtn.assert_read_ok(fuse_client, lab.mount_a, BASELINE_FILE):
            return 1

        log.info("A: enable quarantine")
        qtn.quarantine_enable(admin, vol_name, lab.sub_a)

        log.info("A: retry I/O on live normal mount — expect EACCES")
        if qtn.assert_read_blocked(fuse_client, lab.mount_a, BASELINE_FILE):
            return 1
        if qtn.assert_write_blocked(fuse_client, lab.mount_a, "newfile.txt"):
            return 1

        # --- Sequence B: remount after quarantine must fail ---
        log.info("B: umount and retry normal fuse mount — expect Permission denied")
        qtn.umount_fuse(fuse_client, lab.mount_a)
        lab._mounted_a = False
        if qtn.assert_fuse_mount_fails(
            fuse_client, lab.mount_a, lab.data_a, lab.client_a, vol_name
        ):
            return 1

        qtn.quarantine_disable(admin, vol_name, lab.sub_a)

        log.info("acceptance2_blocks_normal PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        lab.cleanup()


def subtest_acceptance3_rwq_recovery(qtn, admin, fuse_client, vol_name, config):
    """
    Acceptance 3 — Quarantined subvolume accessible with rwq.

    Recovery mount succeeds with R/W; normal client remains blocked.
    """
    lab = LabContext(qtn, admin, fuse_client, vol_name, config)
    try:
        lab.setup(with_recovery=True, mount_a=True, write_baseline=True)

        log.info("Enable quarantine")
        qtn.quarantine_enable(admin, vol_name, lab.sub_a)

        log.info("Normal client still blocked")
        if qtn.assert_read_blocked(fuse_client, lab.mount_a, BASELINE_FILE):
            return 1
        if qtn.assert_write_blocked(fuse_client, lab.mount_a, "blocked.txt"):
            return 1

        log.info("Mount recovery client with rwq")
        qtn.mount_fuse(
            fuse_client,
            lab.mount_recovery,
            lab.data_a,
            lab.client_recovery,
            vol_name,
        )

        log.info("Recovery client can read baseline and write")
        if qtn.assert_content_equals(
            fuse_client, lab.mount_recovery, BASELINE_FILE, BASELINE_A
        ):
            return 1
        if qtn.assert_write_ok(fuse_client, lab.mount_recovery, "recovery.txt"):
            return 1

        log.info("Confirm normal client remains blocked after recovery I/O")
        if qtn.assert_read_blocked(fuse_client, lab.mount_a, BASELINE_FILE):
            return 1

        qtn.umount_fuse(fuse_client, lab.mount_recovery)
        qtn.quarantine_disable(admin, vol_name, lab.sub_a)

        log.info("After disable: normal client sees recovery write")
        if qtn.assert_read_ok(fuse_client, lab.mount_a, "recovery.txt"):
            return 1

        log.info("acceptance3_rwq_recovery PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        lab.cleanup()


def subtest_lifecycle_disable_restore(qtn, admin, fuse_client, vol_name, config):
    """Contain and release: blocked while enabled; full rw after disable; data intact."""
    lab = LabContext(qtn, admin, fuse_client, vol_name, config)
    try:
        lab.setup()

        if qtn.assert_read_ok(fuse_client, lab.mount_a, BASELINE_FILE):
            return 1

        qtn.quarantine_enable(admin, vol_name, lab.sub_a)
        if qtn.assert_read_blocked(fuse_client, lab.mount_a, BASELINE_FILE):
            return 1
        if qtn.assert_write_blocked(fuse_client, lab.mount_a, "newfile.txt"):
            return 1

        qtn.quarantine_disable(admin, vol_name, lab.sub_a)
        if qtn.assert_content_equals(
            fuse_client, lab.mount_a, BASELINE_FILE, BASELINE_A
        ):
            return 1
        if qtn.assert_write_ok(fuse_client, lab.mount_a, "after-disable.txt"):
            return 1

        log.info("lifecycle_disable_restore PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        lab.cleanup()


def subtest_isolation_sibling(qtn, admin, fuse_client, vol_name, config):
    """Quarantine tenant-a; sibling tenant-b stays fully accessible."""
    lab = LabContext(qtn, admin, fuse_client, vol_name, config)
    try:
        lab.setup(with_b=True)

        qtn.quarantine_enable(admin, vol_name, lab.sub_a)

        if qtn.assert_read_blocked(fuse_client, lab.mount_a, BASELINE_FILE):
            return 1
        if qtn.assert_content_equals(
            fuse_client, lab.mount_b, BASELINE_FILE, BASELINE_B
        ):
            return 1
        if qtn.assert_write_ok(fuse_client, lab.mount_b, "still-ok.txt"):
            return 1

        # ls must still list both (names only; no quarantine indication)
        if qtn.assert_subvolume_ls_complete(admin, vol_name, [lab.sub_a, lab.sub_b]):
            return 1

        qtn.quarantine_disable(admin, vol_name, lab.sub_a)
        log.info("isolation_sibling PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        lab.cleanup()


def subtest_mds_asok(qtn, admin, fuse_client, vol_name, config):
    """MDS admin socket quarantine enable/disable (A-01)."""
    lab = LabContext(qtn, admin, fuse_client, vol_name, config)
    try:
        lab.setup()

        mds = qtn.get_active_mds_name(admin, vol_name)
        log.info("Active MDS: %s", mds)

        result = qtn.mds_quarantine(admin, mds, "enable", lab.root_a)
        log.info("mds enable result: %s", result)
        if result.get("status") not in (None, "successful"):
            log.error("unexpected mds enable status: %s", result)
            return 1
        if qtn.assert_read_blocked(fuse_client, lab.mount_a, BASELINE_FILE):
            return 1

        result = qtn.mds_quarantine(admin, mds, "disable", lab.root_a)
        log.info("mds disable result: %s", result)
        if qtn.assert_content_equals(
            fuse_client, lab.mount_a, BASELINE_FILE, BASELINE_A
        ):
            return 1

        log.info("mds_asok PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        lab.cleanup()


def subtest_e2e_incident_workflow(qtn, admin, fuse_client, vol_name, config):
    """
    End-to-end incident workflow:
      baseline → enable → EACCES → sibling OK → info quarantined →
      rwq recovery OK → disable → restored
    """
    lab = LabContext(qtn, admin, fuse_client, vol_name, config)
    try:
        lab.setup(with_b=True, with_recovery=True)

        log.info("E2E-1: baseline I/O OK")
        if qtn.assert_read_ok(fuse_client, lab.mount_a, BASELINE_FILE):
            return 1
        if qtn.assert_write_ok(fuse_client, lab.mount_a, "e2e-baseline.txt"):
            return 1

        log.info("E2E-2: quarantine enable")
        qtn.quarantine_enable(admin, vol_name, lab.sub_a)

        log.info("E2E-3: normal client EACCES")
        if qtn.assert_read_blocked(fuse_client, lab.mount_a, BASELINE_FILE):
            return 1

        log.info("E2E-4: sibling still OK")
        if qtn.assert_read_ok(fuse_client, lab.mount_b, BASELINE_FILE):
            return 1

        log.info("E2E-5: subvolume info shows minimal quarantined status")
        if qtn.assert_info_quarantined(admin, vol_name, lab.sub_a):
            return 1

        log.info("E2E-5b: subvolume ls still lists names (no quarantine fields)")
        if qtn.assert_subvolume_ls_complete(admin, vol_name, [lab.sub_a, lab.sub_b]):
            return 1

        log.info("E2E-5c: getpath blocked while quarantined")
        if qtn.assert_mgr_op_fails(
            admin,
            f"ceph fs subvolume getpath {vol_name} {lab.sub_a}",
            "e2e getpath",
        ):
            return 1

        log.info("E2E-6: rwq recovery R/W")
        qtn.mount_fuse(
            fuse_client,
            lab.mount_recovery,
            lab.data_a,
            lab.client_recovery,
            vol_name,
        )
        if qtn.assert_read_ok(fuse_client, lab.mount_recovery, BASELINE_FILE):
            return 1
        if qtn.assert_write_ok(fuse_client, lab.mount_recovery, "e2e-recovered.txt"):
            return 1
        qtn.umount_fuse(fuse_client, lab.mount_recovery)

        log.info("E2E-7: quarantine disable")
        qtn.quarantine_disable(admin, vol_name, lab.sub_a)

        log.info("E2E-8: normal client restored")
        if qtn.assert_content_equals(
            fuse_client, lab.mount_a, BASELINE_FILE, BASELINE_A
        ):
            return 1
        if qtn.assert_read_ok(fuse_client, lab.mount_a, "e2e-recovered.txt"):
            return 1

        log.info("e2e_incident_workflow PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        lab.cleanup()


SUBTESTS = {
    "prereq_feature_health": subtest_prereq_feature_health,
    "acceptance1_quarantine_flag": subtest_acceptance1_quarantine_flag,
    "acceptance2_blocks_normal": subtest_acceptance2_blocks_normal,
    "acceptance3_rwq_recovery": subtest_acceptance3_rwq_recovery,
    "lifecycle_disable_restore": subtest_lifecycle_disable_restore,
    "isolation_sibling": subtest_isolation_sibling,
    "mds_asok": subtest_mds_asok,
    "e2e_incident_workflow": subtest_e2e_incident_workflow,
}


def run(ceph_cluster, **kw):
    """
    Run Sanity / Acceptance subtests for CephFS subvolume quarantine.

    Optional config:
      subtests: list of names (default: all)
      fs_name, subvol_a/b, mount_*, client_*
    """
    config = kw.get("config") or {}
    log.info("=" * 80)
    log.info("TEST TYPE : Sanity (Acceptance)")
    log.info("MODULE    : test_subvolume_quarantine_sanity.py")
    log.info("POLARION  : CEPH-83632677")
    log.info("=" * 80)

    prepared = _prepare(ceph_cluster, config)
    if prepared is None:
        return 1
    qtn, admin, fuse_client, vol_name = prepared

    requested = config.get("subtests")
    test_list = requested if requested else list(SUBTESTS.keys())

    failed = []
    for name in test_list:
        if name not in SUBTESTS:
            log.error("Unknown Sanity subtest '%s'; known: %s", name, list(SUBTESTS))
            failed.append(name)
            continue

        log.info("")
        log.info("=" * 80)
        log.info("SUBTEST START : [Sanity] %s", name)
        log.info("DESC          : %s", (SUBTESTS[name].__doc__ or "").strip())
        log.info("=" * 80)

        try:
            rc = SUBTESTS[name](qtn, admin, fuse_client, vol_name, config)
        except Exception:
            log.error("SUBTEST EXCEPTION : [Sanity] %s", name)
            log.error(traceback.format_exc())
            rc = 1

        if rc:
            log.error("SUBTEST FAILED  : [Sanity] %s", name)
            failed.append(name)
        else:
            log.info("SUBTEST PASSED  : [Sanity] %s", name)
        log.info("-" * 80)

    log.info("=" * 80)
    if failed:
        log.error(
            "Sanity summary: %d/%d FAILED → %s",
            len(failed),
            len(test_list),
            failed,
        )
        log.info("=" * 80)
        return 1

    log.info("Sanity summary: ALL %d subtest(s) PASSED", len(test_list))
    log.info("=" * 80)
    return 0
