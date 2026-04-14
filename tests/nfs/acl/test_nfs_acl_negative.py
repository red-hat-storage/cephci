"""
NFS v4 ACL Negative Tests.

Runs all denial / failure / boundary tests in a single pass:
  - ACL Ops (Atomic Failure)
  - ACE Eval (Deny then Allow -> access denied)
  - Permission Bits (Append only, Delete denied, Delete via parent)
  - Identity (UID mismatch, Non-member denied, UID vs GID precedence)
  - Symlink (Skip symlink -P, Traversal difference -P vs -L)
"""

from cli.exceptions import ConfigError
from tests.nfs.lib.nfs_acl import NfsAcl
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

TEST_UID_1 = 1001
TEST_USER_1 = "user1"
TEST_UID_2 = 2000
TEST_USER_2 = "user2"
TEST_UID_3 = 2002
TEST_USER_3 = "user3"
TEST_GID_1 = 3001
TEST_GROUP_1 = "group1"

# NFSv4 expanded permission / flag strings as returned by nfs4_getfacl.
# nfs4_setfacl accepts short forms (r, rw, rwx) but nfs4_getfacl returns
# the full permission bits.  Update these constants if the server behaviour
# changes.
PERM_R = "rtcy"  # r   -> rtcy
PERM_RX = "rxtcy"  # rx  -> rxtcy
PERM_W = "watcy"  # w   -> watcy
PERM_WX = "waxtcy"  # wx  -> waxtcy
PERM_A = "atcy"  # a   -> atcy
PERM_RW = "rwatcy"  # rw  -> rwatcy
PERM_RWX = "rwaxtcy"  # rwx -> rwaxtcy  (files)
PERM_RWX_DIR = "rwaDxtcy"  # rwx -> rwaDxtcy (directories)

# Known issues: map test name (as it appears in the results table) to a
# tracker reference.  Tests listed here are still executed and reported,
# but a failure is marked as "KNOWN" instead of a hard failure.
# Add / remove entries as bugs are filed or fixed.
KNOWN_ISSUES = {
    "Append Only": "IBMCEPH-13884",
    "Delete via Parent": "IBMCEPH-13884",
}
FLAGS_INHERIT = "fdi"  # f or d -> fdi


def run(ceph_cluster, **kw):
    """Entry point called by the test framework."""
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.1")
    no_clients = int(config.get("clients", "1"))
    mount_type = config.get("mount_type", "nfs")

    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname

    log.info(
        "\n"
        + "=" * 70
        + "\n"
        + "  NFS ACL NEGATIVE TESTS\n"
        + "  mount_type=%s  nfs_version=%s  clients=%s\n"
        + "=" * 70,
        mount_type,
        version,
        no_clients,
    )

    try:
        setup_nfs_cluster(
            clients,
            nfs_server_name,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs,
            ceph_cluster=ceph_cluster,
        )

        client = clients[0]
        acl = NfsAcl(client, nfs_mount)
        acl.install_acl_tools()

        NfsAcl.create_user(client, TEST_USER_1, TEST_UID_1)
        NfsAcl.create_user(client, TEST_USER_2, TEST_UID_2)
        NfsAcl.create_user(client, TEST_USER_3, TEST_UID_3)
        NfsAcl.create_group(client, TEST_GROUP_1, TEST_GID_1)
        NfsAcl.add_user_to_group(client, TEST_USER_1, TEST_GROUP_1)

        results = []

        # --- ACL Ops ---
        results.append(("Atomic Failure", _run_test(_test_atomic_failure, acl)))

        # --- ACE Eval ---
        results.append(("Deny then Allow", _run_test(_test_deny_then_allow, acl)))

        # --- Permission Bits ---
        results.append(("Append Only", _run_test(_test_append_only, acl)))
        results.append(("Delete Denied", _run_test(_test_delete_denied, acl)))
        results.append(("Delete via Parent", _run_test(_test_delete_via_parent, acl)))

        # --- Identity ---
        results.append(("UID Mismatch", _run_test(_test_uid_mismatch, acl)))
        results.append(("Non-member Denied", _run_test(_test_non_member_denied, acl)))
        results.append(
            ("UID vs GID Precedence", _run_test(_test_uid_vs_gid_precedence, acl))
        )

        # --- Symlink ---
        results.append(("Skip Symlink (-P)", _run_test(_test_skip_symlink, acl)))
        results.append(
            ("Traversal Difference", _run_test(_test_traversal_difference, acl))
        )

        return _report_results(results)

    except Exception as e:
        log.error("Fatal error in ACL negative tests: %s", e)
        return 1
    finally:
        for c in clients:
            NfsAcl.delete_user(c, TEST_USER_1)
            NfsAcl.delete_user(c, TEST_USER_2)
            NfsAcl.delete_user(c, TEST_USER_3)
            NfsAcl.delete_group(c, TEST_GROUP_1)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
        log.info("Cleanup completed")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_test(fn, *args, **kwargs):
    """Run a sub-test with prominent start/end banners."""
    name = fn.__name__.replace("_test_", "").replace("_", " ").title()
    NfsAcl.log_test_start(name)
    try:
        rc = fn(*args, **kwargs)
        NfsAcl.log_test_end(name, rc == 0)
        return rc
    except Exception as e:
        log.error("Sub-test %s raised an exception: %s", fn.__name__, e)
        NfsAcl.log_test_end(name, False)
        return 1


def _report_results(results):
    """Log a summary table and return 0 if all passed, 1 otherwise."""
    hard_failures = []
    known_failures = []
    log.info("=" * 60)
    log.info("NEGATIVE TEST RESULTS")
    log.info("=" * 60)
    for name, rc in results:
        if rc == 0:
            log.info("  %-35s PASS", name)
        elif name in KNOWN_ISSUES:
            log.info("  %-35s FAIL (KNOWN: %s)", name, KNOWN_ISSUES[name])
            known_failures.append(name)
        else:
            log.info("  %-35s FAIL", name)
            hard_failures.append(name)
    log.info("=" * 60)
    if known_failures:
        log.warning("Known failures: %s", known_failures)
    if hard_failures:
        log.error("Unexpected failures: %s", hard_failures)
    if hard_failures or known_failures:
        return 1
    log.info("All negative tests passed")
    return 0


# ---------------------------------------------------------------------------
# ACL Ops
# ---------------------------------------------------------------------------


def _test_atomic_failure(acl):
    log.info("=== Test: Atomic Failure ===")
    acl.create_file("f1")
    acl.write_file("f1", "atomic_failure_data")
    acl.set_acl("f1", f"A::{TEST_UID_1}:r")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_R}"):
        log.error("ACE not found after set (before atomic failure)")
        return 1
    original_acl = acl.get_acl("f1")

    log.info("Verify user1 can read before the failed operation")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("User1 cannot read before atomic failure test")
        return 1

    bad_file = "/tmp/bad_acl.txt"
    acl.client.exec_command(sudo=True, cmd=f"echo 'INVALID' > {bad_file}")
    out, err = acl.set_acl_from_file_expect_fail("f1", bad_file)
    log.info("Invalid spec file output: %s, stderr: %s", out, err)

    current_acl = acl.get_acl("f1")
    if original_acl != current_acl:
        log.error(
            "ACL changed after failed operation! Original: %s, Current: %s",
            original_acl,
            current_acl,
        )
        return 1
    log.info("ACL unchanged after failed operation. Original: %s", original_acl)

    log.info("Access verification: user1 should still read after failed setfacl")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("User1 lost read access after failed atomic operation")
        return 1

    acl.client.exec_command(sudo=True, cmd=f"rm -f {bad_file}")
    log.info("Atomic Failure: PASSED")
    return 0


# ---------------------------------------------------------------------------
# ACE Eval
# ---------------------------------------------------------------------------


def _test_deny_then_allow(acl):
    log.info("=== Test: Deny then Allow ===")
    acl.create_file("f1")
    acl.write_file("f1", "deny_allow_test")

    log.info("Step 1: Set Deny ACE and verify via getfacl")
    acl.set_acl("f1", f"D::{TEST_UID_1}:rx")
    acl_after_deny = acl.get_acl("f1")
    log.info("ACL after Deny: %s", acl_after_deny)

    log.info("Step 2: Access should fail — Deny")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=False):
        log.error(
            "Access was allowed but should have been denied (Deny precedes Allow)"
        )
        return 1

    log.info("Step 3: Add Allow ACE and verify via getfacl")
    acl.add_acl("f1", f"A::{TEST_UID_1}:rx")
    acl_after_allow = acl.get_acl("f1")
    log.info("ACL after Allow added: %s", acl_after_allow)

    log.info("Step 4: Access should pass — Deny precedes Allow")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error(
            "Access was denied but should have been allowed (Deny precedes Allow)"
        )
        return 1
    log.info("Deny then Allow: PASSED")
    return 0


# ---------------------------------------------------------------------------
# Permission Bits
# ---------------------------------------------------------------------------


def _test_append_only(acl):
    log.info("=== Test: Append Only ===")
    acl.create_file("f1")
    acl.set_acl("f1", f"A::{TEST_UID_1}:a")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_A}"):
        log.error("Append ACE not found after set")
        return 1

    log.info("Verify append succeeds")
    if not acl.verify_access(
        TEST_USER_1, "f1", operation="append", expect_success=True
    ):
        log.error("Append access denied when it should be allowed")
        return 1

    log.info("Verify overwrite (truncate write) fails")
    if not acl.verify_access(
        TEST_USER_1, "f1", operation="write", expect_success=False
    ):
        log.error("Overwrite succeeded when it should have been denied (append-only)")
        return 1

    log.info("Append Only: PASSED")
    return 0


def _test_delete_denied(acl):
    log.info("=== Test: Delete Denied ===")
    acl.create_file("f1")
    acl.set_acl("f1", f"A::{TEST_UID_1}:wx")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_WX}"):
        log.error("Write ACE not found after set")
        return 1

    if not acl.verify_access(
        TEST_USER_1, "f1", operation="delete", expect_success=False
    ):
        log.error("Delete succeeded when it should have been denied")
        return 1
    log.info("Delete Denied: PASSED")
    return 0


def _test_delete_via_parent(acl):
    log.info("=== Test: Delete via Parent ===")
    acl.create_dir("d1")
    acl.create_file("d1/f1")
    acl.set_acl("d1", f"A::{TEST_UID_1}:Dx")
    acl_entries = acl.get_acl("d1")
    log.info("ACL on d1 after set D: %s", acl_entries)

    if not acl.verify_access(
        TEST_USER_1, "d1/f1", operation="delete", expect_success=True
    ):
        log.error("Delete via parent permission failed")
        return 1
    acl.cleanup_test_files("d1")
    log.info("Delete via Parent: PASSED")
    return 0


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


def _test_uid_mismatch(acl):
    log.info("=== Test: UID Mismatch ===")
    acl.write_file("f1", "uid_mismatch_test")
    acl.set_acl("f1", f"A::{TEST_UID_1}:rw")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_RW}"):
        log.error("RW ACE for UID %s not found after set", TEST_UID_1)
        return 1

    if not acl.verify_access(TEST_USER_2, "f1", operation="read", expect_success=False):
        log.error("UID mismatch: access allowed unexpectedly")
        return 1
    log.info("UID Mismatch: PASSED")
    return 0


def _test_non_member_denied(acl):
    log.info("=== Test: Non-member Denied ===")
    acl.write_file("f1", "non_member_test")
    acl.set_acl("f1", f"A:g:{TEST_GID_1}:r")
    if not acl.verify_acl_contains("f1", f"A:g:{TEST_GID_1}:{PERM_R}"):
        log.error("Group read ACE for GID %s not found after set", TEST_GID_1)
        return 1

    if not acl.verify_access(TEST_USER_3, "f1", operation="read", expect_success=False):
        log.error("Non-member was granted access")
        return 1
    log.info("Non-member Denied: PASSED")
    return 0


def _test_uid_vs_gid_precedence(acl):
    log.info("=== Test: UID vs GID Precedence ===")
    acl.write_file("f1", "precedence_test")

    log.info("Step 1: Set Deny ACE for UID %s", TEST_UID_1)
    acl.set_acl("f1", f"D::{TEST_UID_1}:r")
    acl_after_deny = acl.get_acl("f1")
    log.info("ACL after UID Deny: %s", acl_after_deny)

    log.info("Step 2: Add Allow ACE for GID %s", TEST_GID_1)
    acl.add_acl("f1", f"A:g:{TEST_GID_1}:r")
    acl_after_allow = acl.get_acl("f1")
    log.info("ACL after GID Allow added: %s", acl_after_allow)

    log.info("Step 3: Access should fail — UID Deny takes precedence over GID Allow")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=False):
        log.error("UID Deny did not take precedence over GID Allow")
        return 1
    log.info("UID vs GID Precedence: PASSED")
    return 0


# ---------------------------------------------------------------------------
# Symlink
# ---------------------------------------------------------------------------


def _setup_symlink_env(acl):
    acl.cleanup_test_files("d1", "real")
    acl.create_dir("d1")
    acl.create_dir("real")
    acl.create_file("real/f_real")
    acl.create_symlink("real", "d1/link_real")


def _test_skip_symlink(acl):
    log.info("=== Test: Skip Symlink (-P) ===")
    _setup_symlink_env(acl)
    acl.write_file("real/f_real", "skip_symlink_data")

    original_acl = acl.get_acl("real/f_real")
    log.info("Original ACL on real/f_real: %s", original_acl)

    NfsAcl.create_user(acl.client, "user_1002", 1002)
    acl.set_acl_recursive("d1", "A::1002:wx", follow_symlinks=False)

    current_acl = acl.get_acl("real/f_real")
    log.info("ACL on real/f_real after -P: %s", current_acl)
    if any("1002" in entry for entry in current_acl):
        log.error("Skip symlink (-P) failed: ACL was applied to symlink target")
        NfsAcl.delete_user(acl.client, "user_1002")
        return 1
    if not acl.verify_acl_not_contains("real/f_real", f"A::1002:{PERM_WX}"):
        log.error("ACE for UID 1002 unexpectedly present on target after -P")
        NfsAcl.delete_user(acl.client, "user_1002")
        return 1

    log.info(
        "Access verification: user with UID 1002 should NOT write to symlink target"
    )
    if not acl.verify_access(
        "user_1002", "real/f_real", operation="write", expect_success=False
    ):
        log.error("UID 1002 user can write to symlink target despite -P skip")
        NfsAcl.delete_user(acl.client, "user_1002")
        return 1

    NfsAcl.delete_user(acl.client, "user_1002")
    acl.cleanup_test_files("d1", "real")
    log.info("Skip Symlink (-P): PASSED")
    return 0


def _test_traversal_difference(acl):
    log.info("=== Test: Traversal Difference (-P vs -L) ===")
    _setup_symlink_env(acl)
    acl.write_file("real/f_real", "traversal_diff_data")

    NfsAcl.create_user(acl.client, "user_1002", 1002)

    log.info("Run -P (skip symlinks)")
    acl.set_acl_recursive("d1", "A::1002:wx", follow_symlinks=False)
    acl_after_p = acl.get_acl("real/f_real")
    log.info("ACL on real/f_real after -P: %s", acl_after_p)
    if any("1002" in entry for entry in acl_after_p):
        log.error("Traversal -P: ACL was unexpectedly applied to target")
        NfsAcl.delete_user(acl.client, "user_1002")
        return 1

    log.info("Access verification: UID 1002 should NOT write to target after -P")
    if not acl.verify_access(
        "user_1002", "real/f_real", operation="write", expect_success=False
    ):
        log.error("UID 1002 can write to target after -P (should be denied)")
        NfsAcl.delete_user(acl.client, "user_1002")
        return 1

    log.info("Run -L (follow symlinks)")
    acl.set_acl_recursive("d1", "A::1002:wx", follow_symlinks=True)
    acl_after_l = acl.get_acl("real/f_real")
    log.info("ACL on real/f_real after -L: %s", acl_after_l)
    if not any("1002" in entry for entry in acl_after_l):
        log.error("Traversal -L: ACL was NOT applied to target")
        NfsAcl.delete_user(acl.client, "user_1002")
        return 1

    log.info("Verify ACE present for UID 1002 after -L")
    if not acl.verify_acl_contains("real/f_real", f"A::1002:{PERM_WX}"):
        log.error("ACE for UID 1002 not found after -L recursive set")
        NfsAcl.delete_user(acl.client, "user_1002")
        return 1

    log.info("Access verification: UID 1002 should now write to target after -L")
    if not acl.verify_access(
        "user_1002", "real/f_real", operation="write", expect_success=True
    ):
        log.error("UID 1002 cannot write to target after -L (should be allowed)")
        NfsAcl.delete_user(acl.client, "user_1002")
        return 1

    log.info("Clear difference between -P (skip) and -L (follow) confirmed")
    NfsAcl.delete_user(acl.client, "user_1002")
    acl.cleanup_test_files("d1", "real")
    log.info("Traversal Difference: PASSED")
    return 0
