"""
NFS v4 ACL Functional Tests.

Runs all positive / feature-verification tests in a single pass:
  - ACL Ops (GETATTR, SETATTR, Full Replace, Incremental, Spec File)
  - ACE Eval (Allow then Deny -> access allowed)
  - Permission Bits (Read, Write)
  - Identity (UID match, Non-existent UID, GID-based, Multiple groups)
  - Flags / Inheritance (FILE_INHERIT, DIR_INHERIT, NO_PROPAGATE, INHERIT_ONLY)
  - POSIX Interaction (ACL->chmod, chmod->ACL)
  - Symlink (Follow -L, Inode proof)
  - Recursive (Apply, Override, Mixed)
  - Persistence (Rename, Hard link, Restart, Reboot)
"""

from time import sleep

from ceph.waiter import WaitUntil
from cli.exceptions import ConfigError
from cli.utilities.utils import get_ip_from_node, reboot_node
from tests.nfs.lib.nfs_acl import NfsAcl
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

TEST_UID_1 = 1001
TEST_USER_1 = "user1"
TEST_UID_2 = 1002
TEST_USER_2 = "user2"
TEST_UID_3 = 2000
TEST_USER_3 = "user3"
TEST_GID_1 = 3001
TEST_GROUP_1 = "group1"
TEST_GID_2 = 3002
TEST_GROUP_2 = "group2"

# NFSv4 expanded permission / flag strings as returned by nfs4_getfacl.
# nfs4_setfacl accepts short forms (r, rw, rwx) but nfs4_getfacl returns
# the full permission bits.  Update these constants if the server behaviour
# changes.
PERM_R = "rtcy"  # r   -> rtcy
PERM_W = "watcy"  # w   -> watcy
PERM_RW = "rwatcy"  # rw  -> rwatcy
PERM_RWX = "rwaxtcy"  # rwx -> rwaxtcy  (files)
PERM_RX = "rxtcy"  # rx  -> rxtcy
PERM_RWX_DIR = "rwaDxtcy"  # rwx -> rwaDxtcy (directories, includes D=delete-child)
FLAGS_INHERIT = "fdi"  # f or d -> fdi   (file/dir inherit + inherit-only)

# Known issues: map test name (as it appears in the results table) to a
# tracker reference.  Tests listed here are still executed and reported,
# but a failure is marked as "KNOWN" instead of a hard failure.
# Add / remove entries as bugs are filed or fixed.
KNOWN_ISSUES = {
    "DIR_INHERIT": "IBMCEPH-13880, IBMCEPH-13881",
    "NO_PROPAGATE": "IBMCEPH-13881",
    "INHERIT_ONLY": "IBMCEPH-13881",
}


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
    client = clients[0]
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
        + "  NFS ACL FUNCTIONAL TESTS\n"
        + "  mount_type=%s  nfs_version=%s  clients=%s\n"
        + "=" * 70,
        mount_type,
        version,
        no_clients,
    )

    try:
        if mount_type == "nfs":
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
        else:
            log.error(
                "Invalid mount type: %s. Supported types are: %s", mount_type, ["nfs"]
            )
            return 1

        acl = NfsAcl(client, nfs_mount)
        acl.install_acl_tools()

        NfsAcl.create_user(client, TEST_USER_1, TEST_UID_1)
        NfsAcl.create_user(client, TEST_USER_2, TEST_UID_2)
        NfsAcl.create_user(client, TEST_USER_3, TEST_UID_3)
        NfsAcl.create_group(client, TEST_GROUP_1, TEST_GID_1)
        NfsAcl.create_group(client, TEST_GROUP_2, TEST_GID_2)
        NfsAcl.add_user_to_group(client, TEST_USER_1, TEST_GROUP_1)
        NfsAcl.add_user_to_group(client, TEST_USER_1, TEST_GROUP_2)

        results = []

        # --- ACL Ops ---
        results.append(("GETATTR Before Set", _run_test(_test_getattr_before_set, acl)))
        results.append(("SETATTR/GETATTR", _run_test(_test_setattr_getattr, acl)))
        results.append(("Full Replace (-s)", _run_test(_test_full_replace, acl)))
        results.append(("Incremental Add (-a)", _run_test(_test_incremental_add, acl)))
        results.append(("Spec File Apply", _run_test(_test_spec_file_apply, acl)))

        # --- ACE Eval ---
        results.append(("Allow then Deny", _run_test(_test_allow_then_deny, acl)))

        # --- Permission Bits ---
        results.append(("Read Permission", _run_test(_test_read_permission, acl)))
        results.append(("Write Permission", _run_test(_test_write_permission, acl)))

        # --- Identity ---
        results.append(("UID Match", _run_test(_test_uid_match, acl)))
        results.append(("Non-existent UID", _run_test(_test_non_existent_uid, acl)))
        results.append(("GID-based Access", _run_test(_test_gid_based_access, acl)))
        results.append(("Multiple Groups", _run_test(_test_multiple_groups, acl)))

        # --- Flags / Inheritance ---
        results.append(("FILE_INHERIT", _run_test(_test_file_inherit, acl)))
        results.append(("DIR_INHERIT", _run_test(_test_dir_inherit, acl)))
        results.append(("NO_PROPAGATE", _run_test(_test_no_propagate, acl)))
        results.append(("INHERIT_ONLY", _run_test(_test_inherit_only, acl)))

        # --- POSIX Interaction ---
        results.append(("ACL to chmod", _run_test(_test_acl_to_chmod, acl)))
        results.append(("chmod to ACL", _run_test(_test_chmod_to_acl, acl)))

        # --- Symlink ---
        results.append(("Follow Symlink (-L)", _run_test(_test_follow_symlink, acl)))
        results.append(("Inode Proof", _run_test(_test_inode_proof, acl)))

        # --- Recursive ---
        results.append(("Apply Recursively", _run_test(_test_apply_recursive, acl)))
        results.append(("Override Existing", _run_test(_test_override_existing, acl)))
        results.append(("Mixed Existing", _run_test(_test_mixed_existing, acl)))

        # --- Persistence (destructive tests last) ---
        results.append(("Rename Persistence", _run_test(_test_rename, acl)))
        results.append(("Hard Link Persistence", _run_test(_test_hard_link, acl)))
        results.append(
            (
                "Restart Persistence",
                _run_test(_test_restart, acl, client, nfs_name),
            )
        )
        results.append(
            (
                "Reboot Persistence",
                _run_test(_test_reboot, acl, nfs_node),
            )
        )

        return _report_results(results)

    except Exception as e:
        log.error("Fatal error in ACL functional tests: %s", e)
        return 1
    finally:
        for c in clients:
            NfsAcl.delete_user(c, TEST_USER_1)
            NfsAcl.delete_user(c, TEST_USER_2)
            NfsAcl.delete_user(c, TEST_USER_3)
            NfsAcl.delete_group(c, TEST_GROUP_1)
            NfsAcl.delete_group(c, TEST_GROUP_2)
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
    log.info("FUNCTIONAL TEST RESULTS")
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
    log.info("All functional tests passed")
    return 0


def _acl_entries(acl_list):
    """Return ACL entries with the ``# file:`` header stripped for comparison."""
    return [e for e in acl_list if not e.startswith("# file:")]


# ---------------------------------------------------------------------------
# ACL Ops
# ---------------------------------------------------------------------------


def _test_getattr_before_set(acl):
    log.info("=== Test: GETATTR Before Set ===")
    acl.create_file("f1")
    acl.write_file("f1", "getattr_test_data")

    default_acl = acl.get_acl("f1")
    log.info("Default ACL entries: %s", default_acl)
    if not default_acl:
        log.error("Expected default ACL entries but got none")
        return 1

    acl.set_acl("f1", f"A::{TEST_UID_1}:rw")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_RW}"):
        log.error("ACL was not updated after nfs4_setfacl")
        return 1

    log.info("Access verification: user1 should be able to read and write")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("User1 cannot read despite A::1001:rw ACL")
        return 1
    if not acl.verify_access(TEST_USER_1, "f1", operation="write", expect_success=True):
        log.error("User1 cannot write despite A::1001:rw ACL")
        return 1

    log.info("GETATTR Before Set: PASSED")
    return 0


def _test_setattr_getattr(acl):
    log.info("=== Test: SETATTR / GETATTR ===")
    acl.create_file("f1")
    acl.write_file("f1", "setattr_test_data")
    acl.set_acl("f1", f"A::{TEST_UID_1}:rw")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_RW}"):
        log.error("ACL not stored/retrieved correctly")
        return 1

    log.info("Access verification: user1 should read and write")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("User1 read failed despite rw ACL")
        return 1
    if not acl.verify_access(TEST_USER_1, "f1", operation="write", expect_success=True):
        log.error("User1 write failed despite rw ACL")
        return 1

    log.info("Access verification: user3 (different UID) should be denied")
    if not acl.verify_access(TEST_USER_3, "f1", operation="read", expect_success=False):
        log.error("User3 could read despite no matching ACL")
        return 1

    log.info("SETATTR/GETATTR: PASSED")
    return 0


def _test_full_replace(acl):
    log.info("=== Test: Full Replace (-s) ===")
    acl.create_file("f1")
    acl.write_file("f1", "full_replace_data")
    acl.set_acl("f1", f"A::{TEST_UID_1}:r")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_R}"):
        log.error("Initial ACE A::%s:r not set before replace", TEST_UID_1)
        return 1

    acl.set_acl("f1", f"A::{TEST_UID_2}:rw")

    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_2}:{PERM_RW}"):
        log.error("UID 1002 ACE not found after full replace")
        return 1
    if not acl.verify_acl_not_contains("f1", f"A::{TEST_UID_1}:{PERM_R}"):
        log.error("UID 1001 ACE still present after full replace")
        return 1

    log.info("Access verification: user2 (UID 1002) should have rw access")
    if not acl.verify_access(TEST_USER_2, "f1", operation="read", expect_success=True):
        log.error("User2 cannot read after full replace")
        return 1
    if not acl.verify_access(TEST_USER_2, "f1", operation="write", expect_success=True):
        log.error("User2 cannot write after full replace")
        return 1

    log.info("Access verification: user1 (UID 1001) should be denied after replace")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=False):
        log.error("User1 can still read after ACL was replaced away")
        return 1

    log.info("Full Replace: PASSED")
    return 0


def _test_incremental_add(acl):
    log.info("=== Test: Incremental (-a) ===")
    acl.create_file("f1")
    acl.write_file("f1", "incremental_data")
    acl.set_acl("f1", f"A::{TEST_UID_1}:r")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_R}"):
        log.error("Initial ACE not set before incremental add")
        return 1

    acl.add_acl("f1", f"A::{TEST_UID_2}:w")

    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_R}"):
        log.error("Original ACE A::1001:r missing")
        return 1
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_2}:{PERM_W}"):
        log.error("Added ACE A::1002:w not found")
        return 1

    log.info("Access verification: user1 can read, user2 can write")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("User1 cannot read despite A::1001:r ACE")
        return 1
    if not acl.verify_access(TEST_USER_2, "f1", operation="write", expect_success=True):
        log.error("User2 cannot write despite A::1002:w ACE")
        return 1

    log.info("Incremental Add: PASSED")
    return 0


def _test_spec_file_apply(acl):
    log.info("=== Test: Spec File Apply ===")
    acl.create_file("f1")
    acl.write_file("f1", "spec_file_data")
    acl.set_acl("f1", f"A::{TEST_UID_1}:r")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_R}"):
        log.error("Initial ACE not set before spec file apply")
        return 1

    spec_file = "/tmp/acl_spec_test.txt"
    acl.save_acl_to_file("f1", spec_file)
    acl.client.exec_command(sudo=True, cmd=f"echo 'A::{TEST_UID_2}:rw' >> {spec_file}")
    acl.set_acl_from_file("f1", spec_file)

    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_R}"):
        log.error("Original ACE missing after spec file apply")
        return 1
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_2}:{PERM_RW}"):
        log.error("Appended ACE not found after spec file apply")
        return 1

    log.info("Access verification: user1 reads, user2 reads and writes")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("User1 cannot read after spec file apply")
        return 1
    if not acl.verify_access(TEST_USER_2, "f1", operation="read", expect_success=True):
        log.error("User2 cannot read after spec file apply")
        return 1
    if not acl.verify_access(TEST_USER_2, "f1", operation="write", expect_success=True):
        log.error("User2 cannot write after spec file apply")
        return 1

    acl.client.exec_command(sudo=True, cmd=f"rm -f {spec_file}")
    log.info("Spec File Apply: PASSED")
    return 0


# ---------------------------------------------------------------------------
# ACE Eval
# ---------------------------------------------------------------------------


def _test_allow_then_deny(acl):
    log.info("=== Test: Allow then Deny ===")
    acl.create_file("f1")
    acl.write_file("f1", "allow_deny_test")

    log.info("Step 1: Set Allow ACE and verify via getfacl")
    acl.set_acl("f1", f"A::{TEST_UID_1}:rw")
    acl_after_allow = acl.get_acl("f1")
    log.info("ACL after Allow: %s", acl_after_allow)
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_RW}"):
        log.error("Allow ACE not found after set")
        return 1

    log.info("Step 1a: Access should succeed with only Allow ACE")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("Read denied with Allow ACE only — expected success")
        return 1
    if not acl.verify_access(TEST_USER_1, "f1", operation="write", expect_success=True):
        log.error("Write denied with Allow ACE only — expected success")
        return 1

    log.info("Step 2: Add Deny ACE and verify via getfacl")
    acl.add_acl("f1", f"D::{TEST_UID_1}:w")
    acl_after_deny = acl.get_acl("f1")
    log.info("ACL after Deny added: %s", acl_after_deny)

    log.info("Step 2a: Access should fail now that Deny ACE follows Allow")
    if not acl.verify_access(
        TEST_USER_1, "f1", operation="write", expect_success=False
    ):
        log.error("Write succeeded after Deny ACE was added — expected denial")
        return 1

    log.info("Step 2b: Access should succeed now that Deny ACE follows Allow")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("Read failed after Deny ACE was added for write — expected success")
        return 1

    log.info("Allow then Deny: PASSED")
    return 0


# ---------------------------------------------------------------------------
# Permission Bits
# ---------------------------------------------------------------------------


def _test_read_permission(acl):
    log.info("=== Test: Read Permission ===")
    acl.write_file("f1", "hello")
    acl.set_acl("f1", f"A::{TEST_UID_1}:r")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_R}"):
        log.error("Read ACE not found after set")
        return 1

    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("Read access denied when it should be allowed")
        return 1
    log.info("Read Permission: PASSED")
    return 0


def _test_write_permission(acl):
    log.info("=== Test: Write Permission ===")
    acl.create_file("f1")
    acl.set_acl("f1", f"A::{TEST_UID_1}:w")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_W}"):
        log.error("Write ACE not found after set")
        return 1

    if not acl.verify_access(TEST_USER_1, "f1", operation="write", expect_success=True):
        log.error("Write access denied when it should be allowed")
        return 1
    log.info("Write Permission: PASSED")
    return 0


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


def _test_uid_match(acl):
    log.info("=== Test: UID Match ===")
    acl.write_file("f1", "uid_match_test")
    acl.set_acl("f1", f"A::{TEST_UID_1}:rw")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_RW}"):
        log.error("UID match ACE not found after set")
        return 1

    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("UID match: access denied unexpectedly")
        return 1
    log.info("UID Match: PASSED")
    return 0


def _test_non_existent_uid(acl):
    log.info("=== Test: Non-existent UID ===")
    acl.create_file("f1")
    acl.write_file("f1", "non_existent_uid_data")
    acl.set_acl("f1", "A::9999:rw")

    if not acl.verify_acl_contains("f1", f"A::9999:{PERM_RW}"):
        log.error("ACL for non-existent UID not stored")
        return 1

    log.info("Access verification: existing users without UID 9999 should be denied")
    if not acl.verify_access(TEST_USER_3, "f1", operation="read", expect_success=False):
        log.error("User3 (UID %d) could read despite only UID 9999 in ACL", TEST_UID_3)
        return 1

    log.info("Non-existent UID: PASSED")
    return 0


def _test_gid_based_access(acl):
    log.info("=== Test: GID-based Access ===")
    acl.write_file("f1", "gid_access_test")
    acl.set_acl("f1", f"A:g:{TEST_GID_1}:r")

    if not acl.verify_acl_contains("f1", f"A:g:{TEST_GID_1}:{PERM_R}"):
        log.error("GID ACE not found after set")
        return 1

    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("Group member denied access")
        return 1
    log.info("GID-based Access: PASSED")
    return 0


def _test_multiple_groups(acl):
    log.info("=== Test: Multiple Groups ===")
    acl.write_file("f1", "multi_group_test")
    acl.set_acl("f1", f"A:g:{TEST_GID_1}:r")
    acl.add_acl("f1", f"A:g:{TEST_GID_2}:w")

    if not acl.verify_acl_contains("f1", f"A:g:{TEST_GID_1}:{PERM_R}"):
        log.error("GID1 ACE not found after set")
        return 1
    if not acl.verify_acl_contains("f1", f"A:g:{TEST_GID_2}:{PERM_W}"):
        log.error("GID2 ACE not found after add")
        return 1

    if not acl.verify_access(
        TEST_USER_1, "f1", operation="append", expect_success=True
    ):
        log.error("Multiple group access failed: write denied")
        return 1
    log.info("Multiple Groups: PASSED")
    return 0


# ---------------------------------------------------------------------------
# Flags / Inheritance
# ---------------------------------------------------------------------------


def _test_file_inherit(acl):
    log.info("=== Test: FILE_INHERIT ===")
    acl.cleanup_test_files("d1")
    acl.create_dir("d1")
    acl.set_acl("d1", f"A:f:{TEST_UID_1}:rwx")

    if not acl.verify_acl_contains(
        "d1", f"A:{FLAGS_INHERIT}:{TEST_UID_1}:{PERM_RWX_DIR}"
    ):
        log.error("FILE_INHERIT ACE not found on parent dir after set")
        return 1

    acl.create_file("d1/f2")
    acl.write_file("d1/f2", "inherited_file_data")

    acl_entries = acl.get_acl("d1/f2")
    log.info("d1/f2 ACL: %s", acl_entries)
    if not any(str(TEST_UID_1) in entry for entry in acl_entries):
        log.error("FILE_INHERIT: new file did not inherit ACL from parent")
        return 1

    log.info("Access verification: user1 should be able to read inherited file")
    if not acl.verify_access(
        TEST_USER_1, "d1/f2", operation="read", expect_success=True
    ):
        log.error("FILE_INHERIT: user1 cannot read inherited file")
        return 1

    acl.cleanup_test_files("d1")
    log.info("FILE_INHERIT: PASSED")
    return 0


def _test_dir_inherit(acl):
    log.info("=== Test: DIR_INHERIT ===")
    acl.cleanup_test_files("d1")
    acl.create_dir("d1")
    acl.set_acl("d1", f"A:d:{TEST_UID_1}:rwx")

    if not acl.verify_acl_contains(
        "d1", f"A:{FLAGS_INHERIT}:{TEST_UID_1}:{PERM_RWX_DIR}"
    ):
        log.error("DIR_INHERIT ACE not found on parent dir after set")
        return 1

    acl.create_dir("d1/sub")

    acl_entries = acl.get_acl("d1/sub")
    log.info("d1/sub ACL: %s", acl_entries)
    if not any(str(TEST_UID_1) in entry for entry in acl_entries):
        log.error("DIR_INHERIT: subdirectory did not inherit ACL from parent")
        return 1

    log.info(
        "Access verification: user1 should be able to create a file in inherited subdir"
    )
    out, err = acl.run_as_user(
        TEST_USER_1, f"touch {acl._full_path('d1/sub/test_file')}", check_ec=False
    )
    if err and ("denied" in err.lower() or "permission" in err.lower()):
        log.error("DIR_INHERIT: user1 cannot write in inherited subdir")
        return 1
    log.info("User1 successfully created file in inherited subdir")

    acl.cleanup_test_files("d1")
    log.info("DIR_INHERIT: PASSED")
    return 0


def _test_no_propagate(acl):
    log.info("=== Test: NO_PROPAGATE ===")
    acl.cleanup_test_files("d1")
    acl.create_dir("d1")
    acl.set_acl("d1", f"A:nfd:{TEST_UID_1}:rwx")
    if not acl.verify_acl_contains(
        "d1", f"A:{FLAGS_INHERIT}:{TEST_UID_1}:{PERM_RWX_DIR}"
    ):
        log.error("NO_PROPAGATE ACE not found on parent dir after set")
        return 1

    acl.create_dir("d1/sub")
    acl.create_file("d1/sub/test_file")
    acl.write_file("d1/sub/test_file", "propagation_test")
    acl.create_dir("d1/sub/sub2")

    sub_acl = acl.get_acl("d1/sub")
    if not any(str(TEST_UID_1) in entry for entry in sub_acl):
        log.error("NO_PROPAGATE: d1/sub should have inherited ACL")
        return 1

    log.info("Access verification: user1 should access d1/sub (inherited)")
    if not acl.verify_access(
        TEST_USER_1, "d1/sub/test_file", operation="read", expect_success=True
    ):
        log.error("NO_PROPAGATE: user1 cannot read in d1/sub despite inherited ACL")
        return 1

    sub2_acl = acl.get_acl("d1/sub/sub2")
    if any(str(TEST_UID_1) in entry for entry in sub2_acl):
        log.error("NO_PROPAGATE: d1/sub/sub2 should NOT have inherited ACL")
        return 1

    acl.cleanup_test_files("d1")
    log.info("NO_PROPAGATE: PASSED")
    return 0


def _test_inherit_only(acl):
    log.info("=== Test: INHERIT_ONLY ===")
    acl.cleanup_test_files("d1")
    acl.create_dir("d1")
    acl.set_acl("d1", f"A:i:{TEST_UID_1}:rwx")
    if not acl.verify_acl_contains(
        "d1", f"A:{FLAGS_INHERIT}:{TEST_UID_1}:{PERM_RWX_DIR}"
    ):
        log.error("INHERIT_ONLY ACE not found on dir after set")
        return 1

    acl_entries = acl.get_acl("d1")
    log.info("d1 ACL: %s", acl_entries)
    has_effective = any(
        str(TEST_UID_1) in entry and ":i" not in entry.lower() for entry in acl_entries
    )
    if has_effective:
        log.error("INHERIT_ONLY: ACE should not be effective on parent d1")
        return 1
    acl.cleanup_test_files("d1")
    log.info("INHERIT_ONLY: PASSED")
    return 0


# ---------------------------------------------------------------------------
# POSIX Interaction
# ---------------------------------------------------------------------------


def _test_acl_to_chmod(acl):
    log.info("=== Test: ACL to chmod ===")
    acl.create_file("f1")
    acl.write_file("f1", "acl_to_chmod_data")
    acl.set_acl("f1", "A::OWNER@:rwx")

    acl.add_acl("f1", f"A::{TEST_UID_1}:r")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_R}"):
        log.error("User1 read ACE not found after add")
        return 1

    mode = acl.get_mode("f1")
    log.info("Octal mode of f1: %s", mode)
    if not mode.startswith("7"):
        log.error("Expected owner mode to reflect rwx (7xx), got %s", mode)
        return 1

    acl_entries = acl.get_acl("f1")
    if not acl_entries:
        log.error("ACL entries lost after mode check")
        return 1

    log.info("Access verification: user1 should be able to read (has A::1001:r)")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("User1 cannot read despite having read ACL")
        return 1

    log.info("ACL to chmod: PASSED")
    return 0


def _test_chmod_to_acl(acl):
    log.info("=== Test: chmod to ACL ===")
    acl.create_file("f1")
    acl.write_file("f1", "chmod_to_acl_data")
    acl.set_acl("f1", f"A::{TEST_UID_1}:rwx")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_RWX}"):
        log.error("rwx ACE not found after set (before chmod)")
        return 1

    log.info("Access verification: user1 should have rwx before chmod")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("User1 cannot read before chmod")
        return 1

    initial_acl = acl.get_acl("f1")
    acl.chmod("f1", "644")
    updated_acl = acl.get_acl("f1")

    if initial_acl == updated_acl:
        log.error(
            "ACL did not change after chmod 644. Before: %s, After: %s",
            initial_acl,
            updated_acl,
        )
        return 1

    mode = acl.get_mode("f1")
    if mode != "644":
        log.error("Mode mismatch: expected 644, got %s", mode)
        return 1

    log.info("Access verification: verify file is still readable after chmod 644")
    if not acl.verify_access(TEST_USER_1, "f1", operation="read", expect_success=True):
        log.error("User1 cannot read after chmod 644 (should still be world-readable)")
        return 1

    log.info("chmod to ACL: PASSED")
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


def _test_follow_symlink(acl):
    log.info("=== Test: Follow Symlink (-L) ===")
    _setup_symlink_env(acl)
    acl.write_file("real/f_real", "symlink_follow_data")

    # override with read and execute permission because without x, user will not be able to traverse the directory
    # test may fail if we provide only read permission
    acl.set_acl_recursive("d1", f"A::{TEST_UID_2}:rx", follow_symlinks=True)

    if not acl.verify_acl_contains("real/f_real", f"A::{TEST_UID_2}:{PERM_RX}"):
        log.error("Follow symlink (-L) failed: ACL not applied to symlink target")
        return 1

    log.info("Access verification: user2 should read the symlink target file")
    if not acl.verify_access(
        TEST_USER_2, "real/f_real", operation="read", expect_success=True
    ):
        log.error("User2 cannot read symlink target despite ACL applied via -L")
        return 1

    acl.cleanup_test_files("d1", "real")
    log.info("Follow Symlink (-L): PASSED")
    return 0


def _test_inode_proof(acl):
    log.info("=== Test: Inode Proof ===")
    _setup_symlink_env(acl)

    inode_real = acl.inode("real/f_real")
    inode_via_link = acl.inode("d1/link_real/f_real")
    log.info("Inode real=%s, via symlink=%s", inode_real, inode_via_link)

    if inode_real != inode_via_link:
        log.error("Inode mismatch")
        return 1
    acl.cleanup_test_files("d1", "real")
    log.info("Inode Proof: PASSED")
    return 0


# ---------------------------------------------------------------------------
# Recursive
# ---------------------------------------------------------------------------


def _test_apply_recursive(acl):
    log.info("=== Test: Apply ACL Recursively ===")
    acl.cleanup_test_files("d1")
    acl.create_dir("d1/d2/d3")
    acl.create_file("d1/f1")
    acl.write_file("d1/f1", "recursive_data_f1")
    acl.create_file("d1/d2/f2")
    acl.write_file("d1/d2/f2", "recursive_data_f2")
    acl.set_acl_recursive("d1", f"A::{TEST_UID_1}:rwx")

    if not acl.verify_acl_contains("d1/f1", f"A::{TEST_UID_1}:{PERM_RWX}"):
        log.error("Recursive ACL not applied to d1/f1")
        return 1
    if not acl.verify_acl_contains("d1/d2/f2", f"A::{TEST_UID_1}:{PERM_RWX}"):
        log.error("Recursive ACL not applied to d1/d2/f2")
        return 1

    log.info("Access verification: user1 should read both nested files")
    if not acl.verify_access(
        TEST_USER_1, "d1/f1", operation="read", expect_success=True
    ):
        log.error("User1 cannot read d1/f1 despite recursive rwx ACL")
        return 1
    if not acl.verify_access(
        TEST_USER_1, "d1/d2/f2", operation="read", expect_success=True
    ):
        log.error("User1 cannot read d1/d2/f2 despite recursive rwx ACL")
        return 1

    acl.cleanup_test_files("d1")
    log.info("Apply Recursively: PASSED")
    return 0


def _test_override_existing(acl):
    log.info("=== Test: Override Existing ACLs ===")
    acl.cleanup_test_files("d1")
    acl.create_dir("d1/d2")
    acl.create_file("d1/d2/f2")
    acl.write_file("d1/d2/f2", "override_data")
    acl.set_acl_recursive("d1", f"A::{TEST_UID_1}:rwx")
    if not acl.verify_acl_contains("d1/d2/f2", f"A::{TEST_UID_1}:{PERM_RWX}"):
        log.error("Initial recursive ACL not applied before override")
        return 1

    # override with read and execute permission because without x, user will not be able to traverse the directory
    # test may fail if we provide only read permission
    acl.set_acl_recursive("d1", f"A::{TEST_UID_2}:rx")

    if not acl.verify_acl_contains("d1/d2/f2", f"A::{TEST_UID_2}:{PERM_RX}"):
        log.error("New ACL not found after override")
        return 1
    if not acl.verify_acl_not_contains("d1/d2/f2", f"A::{TEST_UID_1}:{PERM_RWX}"):
        log.error("Old ACL still present after override")
        return 1

    log.info("Access verification: user2 reads, user1 denied after override")
    if not acl.verify_access(
        TEST_USER_2, "d1/d2/f2", operation="read", expect_success=True
    ):
        log.error("User2 cannot read after override")
        return 1
    if not acl.verify_access(
        TEST_USER_1, "d1/d2/f2", operation="read", expect_success=False
    ):
        log.error("User1 can still read after ACL was overridden away")
        return 1

    acl.cleanup_test_files("d1")
    log.info("Override Existing: PASSED")
    return 0


def _test_mixed_existing(acl):
    log.info("=== Test: Mixed Existing ACLs ===")
    acl.cleanup_test_files("d1")
    acl.create_dir("d1/d2")
    acl.create_file("d1/f1")
    acl.write_file("d1/f1", "mixed_data_f1")
    acl.create_file("d1/d2/f2")
    acl.write_file("d1/d2/f2", "mixed_data_f2")
    acl.set_acl("d1/f1", f"A::{TEST_UID_1}:rwx")
    if not acl.verify_acl_contains("d1/f1", f"A::{TEST_UID_1}:{PERM_RWX}"):
        log.error("Initial ACE on d1/f1 not set")
        return 1
    acl.set_acl("d1/d2/f2", f"A::{TEST_UID_2}:w")
    if not acl.verify_acl_contains("d1/d2/f2", f"A::{TEST_UID_2}:{PERM_W}"):
        log.error("Initial ACE on d1/d2/f2 not set")
        return 1

    # override with read and execute permission because without x, user will not be able to traverse the directory
    # test may fail if we provide only read permission
    acl.set_acl_recursive("d1", f"A::{TEST_UID_1}:rx")

    if not acl.verify_acl_contains("d1/f1", f"A::{TEST_UID_1}:{PERM_RX}"):
        log.error("d1/f1 not normalized")
        return 1
    if not acl.verify_acl_contains("d1/d2/f2", f"A::{TEST_UID_1}:{PERM_RX}"):
        log.error("d1/d2/f2 not normalized")
        return 1
    if not acl.verify_acl_not_contains("d1/d2/f2", f"A::{TEST_UID_2}:{PERM_W}"):
        log.error("Old ACL still present after normalization")
        return 1

    log.info("Access verification: user1 can read both files after normalization")
    if not acl.verify_access(
        TEST_USER_1, "d1/f1", operation="read", expect_success=True
    ):
        log.error("User1 cannot read d1/f1 after normalization")
        return 1
    if not acl.verify_access(
        TEST_USER_1, "d1/d2/f2", operation="read", expect_success=True
    ):
        log.error("User1 cannot read d1/d2/f2 after normalization")
        return 1

    log.info("Access verification: user1 should NOT be able to write (only r)")
    if not acl.verify_access(
        TEST_USER_1, "d1/f1", operation="write", expect_success=False
    ):
        log.error("User1 can write d1/f1 despite read-only normalization")
        return 1

    acl.cleanup_test_files("d1")
    log.info("Mixed Existing: PASSED")
    return 0


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def _test_rename(acl):
    log.info("=== Test: Rename Persistence ===")
    acl.create_file("f1")
    acl.write_file("f1", "rename_test_data")
    acl.set_acl("f1", f"A::{TEST_UID_1}:rw")
    if not acl.verify_acl_contains("f1", f"A::{TEST_UID_1}:{PERM_RW}"):
        log.error("ACE not found after set (before rename)")
        return 1
    original_acl = acl.get_acl("f1")
    acl.rename("f1", "f2")
    renamed_acl = acl.get_acl("f2")

    if _acl_entries(original_acl) != _acl_entries(renamed_acl):
        log.error(
            "ACL changed after rename. Original: %s, Renamed: %s",
            original_acl,
            renamed_acl,
        )
        return 1
    log.info("ACL preserved after rename (ignoring file header)")

    log.info("Access verification: user1 should still read/write the renamed file")
    if not acl.verify_access(TEST_USER_1, "f2", operation="read", expect_success=True):
        log.error("User1 cannot read renamed file despite preserved ACL")
        return 1
    if not acl.verify_access(TEST_USER_1, "f2", operation="write", expect_success=True):
        log.error("User1 cannot write renamed file despite preserved ACL")
        return 1

    acl.cleanup_test_files("f2")
    log.info("Rename Persistence: PASSED")
    return 0


def _test_hard_link(acl):
    log.info("=== Test: Hard Link Persistence ===")
    acl.create_file("f2")
    acl.write_file("f2", "hardlink_test_data")
    acl.set_acl("f2", f"A::{TEST_UID_1}:rw")
    if not acl.verify_acl_contains("f2", f"A::{TEST_UID_1}:{PERM_RW}"):
        log.error("ACE not found after set (before hard link)")
        return 1
    original_acl = acl.get_acl("f2")
    acl.create_hardlink("f2", "f3")
    link_acl = acl.get_acl("f3")

    if _acl_entries(original_acl) != _acl_entries(link_acl):
        log.error(
            "ACL differs on hard link. Original: %s, Link: %s",
            original_acl,
            link_acl,
        )
        return 1
    log.info("ACL matches on hard link (ignoring file header)")

    log.info("Access verification: user1 should read/write via the hard link")
    if not acl.verify_access(TEST_USER_1, "f3", operation="read", expect_success=True):
        log.error("User1 cannot read via hard link")
        return 1
    if not acl.verify_access(TEST_USER_1, "f3", operation="write", expect_success=True):
        log.error("User1 cannot write via hard link")
        return 1

    acl.cleanup_test_files("f2", "f3")
    log.info("Hard Link Persistence: PASSED")
    return 0


def _test_restart(acl, client, nfs_name):
    log.info("=== Test: Restart Persistence ===")
    acl.create_file("f2")
    acl.write_file("f2", "restart_test_data")
    acl.set_acl("f2", f"A::{TEST_UID_1}:rw")
    if not acl.verify_acl_contains("f2", f"A::{TEST_UID_1}:{PERM_RW}"):
        log.error("ACE not found after set (before restart)")
        return 1
    original_acl = acl.get_acl("f2")

    NfsAcl.restart_nfs_service(client, nfs_name)

    restarted_acl = acl.get_acl("f2")
    if _acl_entries(original_acl) != _acl_entries(restarted_acl):
        log.error(
            "ACL changed after restart. Original: %s, Restarted: %s",
            original_acl,
            restarted_acl,
        )
        return 1
    log.info("ACL preserved after restart")

    log.info("Access verification: user1 should still read/write after service restart")
    if not acl.verify_access(TEST_USER_1, "f2", operation="read", expect_success=True):
        log.error("User1 cannot read after NFS restart")
        return 1
    if not acl.verify_access(TEST_USER_1, "f2", operation="write", expect_success=True):
        log.error("User1 cannot write after NFS restart")
        return 1

    acl.cleanup_test_files("f2")
    log.info("Restart Persistence: PASSED")
    return 0


def _test_reboot(acl, nfs_node):
    log.info("=== Test: Reboot Persistence ===")
    acl.create_file("f2")
    acl.write_file("f2", "reboot_test_data")
    acl.set_acl("f2", f"A::{TEST_UID_1}:rw")
    if not acl.verify_acl_contains("f2", f"A::{TEST_UID_1}:{PERM_RW}"):
        log.error("ACE not found after set (before reboot)")
        return 1
    original_acl = acl.get_acl("f2")

    log.info("Rebooting NFS server node %s", nfs_node.hostname)
    reboot_node(nfs_node)
    log.info("Waiting 60s for NFS server to stabilise after reboot")
    sleep(60)

    for w in WaitUntil(timeout=120, interval=5):
        assigned_ips = get_ip_from_node(nfs_node)
        if assigned_ips:
            log.info("NFS server IP after reboot: %s", assigned_ips[0])
            break
        if w.expired:
            log.error("NFS server IP not assigned after reboot")
            return 1

    rebooted_acl = acl.get_acl("f2")
    if _acl_entries(original_acl) != _acl_entries(rebooted_acl):
        log.error(
            "ACL changed after reboot. Original: %s, Rebooted: %s",
            original_acl,
            rebooted_acl,
        )
        return 1
    log.info("ACL preserved after reboot")

    log.info(
        "Access verification: user1 should still read/write after reboot and remount"
    )
    if not acl.verify_access(TEST_USER_1, "f2", operation="read", expect_success=True):
        log.error("User1 cannot read after reboot")
        return 1
    if not acl.verify_access(TEST_USER_1, "f2", operation="write", expect_success=True):
        log.error("User1 cannot write after reboot")
        return 1

    acl.cleanup_test_files("f2")
    log.info("Reboot Persistence: PASSED")
    return 0
