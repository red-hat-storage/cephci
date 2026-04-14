"""
NFS v4 ACL Scale / Large ACE Count Tests.

Runs all large-ACL and performance tests in a single pass:
  - Apply 2000+ ACEs, Verify ACE count
  - Random access validation, Non-listed user
  - Performance check
  - Overwrite behaviour, Append behaviour, Duplicate ACE handling
  - Scale limit test
  - Persistence after remount
"""

from time import sleep

from cli.exceptions import ConfigError
from tests.nfs.lib.nfs_acl import NfsAcl
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

# NFSv4 expanded permission / flag strings as returned by nfs4_getfacl.
# nfs4_setfacl accepts short forms (r, rw, rwx) but nfs4_getfacl returns
# the full permission bits.  Update these constants if the server behaviour
# changes.
PERM_R = "rtcy"  # r   -> rtcy
PERM_RX = "rxtcy"  # rx  -> rxtcy
PERM_W = "watcy"  # w   -> watcy
PERM_WX = "waxtcy"  # wx  -> waxtcy
PERM_RW = "rwatcy"  # rw  -> rwatcy
PERM_RWX = "rwaxtcy"  # rwx -> rwaxtcy  (files)
PERM_RWX_DIR = "rwaDxtcy"  # rwx -> rwaDxtcy (directories)
FLAGS_INHERIT = "fdi"  # f or d -> fdi

# Known issues: map test name (as it appears in the results table) to a
# tracker reference.  Tests listed here are still executed and reported,
# but a failure is marked as "KNOWN" instead of a hard failure.
# Add / remove entries as bugs are filed or fixed.
KNOWN_ISSUES = {
    # "Apply 2000+ ACEs": "BZ#1234567",
    # "Scale Limit": "TRACKER-9876 - description",
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
        + "  NFS ACL SCALE TESTS\n"
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

        NfsAcl.create_user(client, "u1001", 1001)
        NfsAcl.create_user(client, "u1500", 1500)
        NfsAcl.create_user(client, "u4000", 4000)
        NfsAcl.create_user(client, "u5000", 5000)

        results = []

        results.append(("Apply 2000+ ACEs", _run_test(_test_apply_large_acl, acl)))
        results.append(("Verify ACE Count", _run_test(_test_verify_ace_count, acl)))
        results.append(
            ("Random Access Validation", _run_test(_test_random_access, acl, client))
        )
        results.append(
            ("Non-listed User", _run_test(_test_non_listed_user, acl, client))
        )
        results.append(("Performance Check", _run_test(_test_performance_check, acl)))
        results.append(
            ("Overwrite Behaviour", _run_test(_test_overwrite_behaviour, acl))
        )
        results.append(("Append Behaviour", _run_test(_test_append_behaviour, acl)))
        results.append(("Duplicate ACE Handling", _run_test(_test_duplicate_ace, acl)))
        results.append(("Scale Limit", _run_test(_test_scale_limit, acl)))
        results.append(
            (
                "Persistence After Remount",
                _run_test(
                    _test_persistence_remount,
                    acl,
                    client,
                    nfs_server_name,
                    nfs_mount,
                    nfs_export,
                    version,
                    port,
                    mount_type,
                ),
            )
        )

        return _report_results(results)

    except Exception as e:
        log.error("Fatal error in ACL scale tests: %s", e)
        return 1
    finally:
        for c in clients:
            NfsAcl.delete_user(c, "u1001")
            NfsAcl.delete_user(c, "u1500")
            NfsAcl.delete_user(c, "u4000")
            NfsAcl.delete_user(c, "u5000")
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
    log.info("SCALE TEST RESULTS")
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
    log.info("All scale tests passed")
    return 0


def _apply_2001_aces(acl):
    """Generate and apply a large ACL file (UIDs 1000-3000 = 2001 entries)."""
    acl.create_file("f1")
    acl_file = "/tmp/acl2.txt"
    acl.generate_acl_file(acl_file, uid_start=1000, uid_end=3000, permission="rx")
    acl.set_acl_from_file("f1", acl_file)
    return acl_file


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _test_apply_large_acl(acl):
    log.info("=== Test: Apply 2000+ ACEs ===")
    acl_file = _apply_2001_aces(acl)

    if not acl.verify_acl_contains("f1", f"A::1500:{PERM_RX}"):
        log.error("Spot check failed: A::1500:%s not found in large ACL", PERM_RX)
        return 1
    if not acl.verify_acl_contains("f1", f"A::2500:{PERM_RX}"):
        log.error("Spot check failed: A::2500:%s not found in large ACL", PERM_RX)
        return 1

    log.info(
        "Access verification: u1500 (in ACL) should read, u4000 (not in ACL) should not"
    )
    if not acl.verify_access("u1500", "f1", operation="read", expect_success=True):
        log.error("u1500 denied read despite ACL granting rx")
        return 1
    if not acl.verify_access("u4000", "f1", operation="read", expect_success=False):
        log.error("u4000 granted read despite not being in ACL")
        return 1

    acl.client.exec_command(sudo=True, cmd=f"rm -f {acl_file}")
    log.info("Apply 2000+ ACEs: PASSED")
    return 0


def _test_verify_ace_count(acl):
    log.info("=== Test: Verify ACE Count ===")
    acl_file = _apply_2001_aces(acl)

    entries = acl.get_acl("f1")
    count = len(entries)
    log.info("Total ACE count: %d", count)

    if count < 2001:
        log.error("Expected at least 2001 ACEs, got %d", count)
        return 1

    log.info("Access verification: u1500 should read with rx permission")
    if not acl.verify_access("u1500", "f1", operation="read", expect_success=True):
        log.error("u1500 denied read despite being in large ACL")
        return 1

    acl.client.exec_command(sudo=True, cmd=f"rm -f {acl_file}")
    log.info("Verify ACE Count (%d): PASSED", count)
    return 0


def _test_random_access(acl, client):
    log.info("=== Test: Random Access Validation ===")
    acl_file = _apply_2001_aces(acl)

    if not acl.verify_acl_contains("f1", f"A::1500:{PERM_RX}"):
        log.error("ACE for UID 1500 not found before access check")
        return 1

    if not acl.verify_access("u1500", "f1", operation="read", expect_success=True):
        log.error("User u1500 denied access despite ACL")
        return 1

    if not acl.verify_access("u1500", "f1", operation="write", expect_success=False):
        log.error("User u1500 granted access to write despite ACL")
        return 1

    acl.client.exec_command(sudo=True, cmd=f"rm -f {acl_file}")
    log.info("Random Access Validation: PASSED")
    return 0


def _test_non_listed_user(acl, client):
    log.info("=== Test: Non-listed User ===")
    acl_file = _apply_2001_aces(acl)

    if not acl.verify_acl_not_contains("f1", f"A::4000:{PERM_RX}"):
        log.error("ACE for UID 4000 unexpectedly found in ACL")
        return 1

    if not acl.verify_access("u4000", "f1", operation="read", expect_success=False):
        log.error("User u4000 granted access despite not being in ACL")
        return 1

    acl.client.exec_command(sudo=True, cmd=f"rm -f {acl_file}")
    log.info("Non-listed User: PASSED")
    return 0


def _test_performance_check(acl):
    log.info("=== Test: Performance Check ===")
    acl_file = _apply_2001_aces(acl)

    out, elapsed = acl.timed_getfacl("f1")
    log.info("nfs4_getfacl completed in %s seconds", elapsed)

    if elapsed is not None and elapsed > 10:
        log.error("Performance issue: nfs4_getfacl took %.2f seconds (>10s)", elapsed)
        return 1

    if elapsed is not None:
        log.info("Performance acceptable: %.2f seconds", elapsed)
    else:
        log.warning("Could not parse timing information")

    acl.client.exec_command(sudo=True, cmd=f"rm -f {acl_file}")
    log.info("Performance Check: PASSED")
    return 0


def _test_overwrite_behaviour(acl):
    log.info("=== Test: Overwrite Behaviour ===")
    acl_file = _apply_2001_aces(acl)

    count_before = len(acl.get_acl("f1"))
    log.info("ACE count before overwrite: %d", count_before)

    acl.set_acl("f1", "A::1001:wx")
    if not acl.verify_acl_contains("f1", f"A::1001:{PERM_WX}"):
        log.error("New ACE not found after overwrite")
        return 1

    entries = acl.get_acl("f1")
    count_after = len(entries)
    log.info("ACE count after overwrite: %d (was %d)", count_after, count_before)

    if count_after >= count_before:
        log.error(
            "Overwrite did not reduce ACE count: before=%d, after=%d",
            count_before,
            count_after,
        )
        return 1

    log.info(
        "Access verification: u1001 (wx) should write, u1500 (no longer in ACL) should not read"
    )
    if not acl.verify_access("u1001", "f1", operation="write", expect_success=True):
        log.error("u1001 denied write after overwrite to wx")
        return 1
    if not acl.verify_access("u1001", "f1", operation="read", expect_success=False):
        log.error("u1001 granted read despite only having wx")
        return 1
    if not acl.verify_access("u1500", "f1", operation="read", expect_success=False):
        log.error("u1500 granted read after ACL was overwritten (old ACL removed)")
        return 1

    acl.client.exec_command(sudo=True, cmd=f"rm -f {acl_file}")
    log.info("Overwrite Behaviour: PASSED")
    return 0


def _test_append_behaviour(acl):
    log.info("=== Test: Append Behaviour ===")
    acl_file = _apply_2001_aces(acl)

    count_before = len(acl.get_acl("f1"))
    acl.add_acl("f1", "A::5000:wx")
    if not acl.verify_acl_contains("f1", f"A::5000:{PERM_WX}"):
        log.error("Appended ACE A::5000:%s not found", PERM_WX)
        return 1
    if not acl.verify_acl_contains("f1", f"A::1500:{PERM_RX}"):
        log.error("Original ACE A::1500:%s lost after append", PERM_RX)
        return 1

    count_after = len(acl.get_acl("f1"))
    log.info("ACE count before append=%d, after=%d", count_before, count_after)
    if count_after != count_before + 1:
        log.error(
            "Expected ACE count to increase by 1 after append: before=%d, after=%d",
            count_before,
            count_after,
        )
        return 1

    log.info(
        "Access verification: u5000 (wx) should write, u1500 (rx) should still read"
    )
    if not acl.verify_access("u5000", "f1", operation="write", expect_success=True):
        log.error("u5000 denied write after append with wx")
        return 1
    if not acl.verify_access("u1500", "f1", operation="read", expect_success=True):
        log.error("u1500 denied read after append (original rx ACE should persist)")
        return 1

    acl.client.exec_command(sudo=True, cmd=f"rm -f {acl_file}")
    log.info("Append Behaviour: PASSED")
    return 0


def _test_duplicate_ace(acl):
    log.info("=== Test: Duplicate ACE Handling ===")
    acl_file = _apply_2001_aces(acl)

    if not acl.verify_acl_contains("f1", f"A::1500:{PERM_RX}"):
        log.error("ACE A::1500:%s not found before duplicate add", PERM_RX)
        return 1

    count_before = len(acl.get_acl("f1"))
    acl.add_acl("f1", "A::1500:rx")
    count_after = len(acl.get_acl("f1"))

    if not acl.verify_acl_contains("f1", f"A::1500:{PERM_RX}"):
        log.error("ACE A::1500:%s not found after duplicate add", PERM_RX)
        return 1

    log.info("Duplicate ACE: count before=%d, after=%d", count_before, count_after)
    if count_after != count_before:
        log.error(
            "Duplicate ACE changed the count: before=%d, after=%d (expected no change)",
            count_before,
            count_after,
        )
        return 1

    log.info("Access verification: u1500 should still read after duplicate add")
    if not acl.verify_access("u1500", "f1", operation="read", expect_success=True):
        log.error("u1500 denied read after duplicate ACE add")
        return 1
    if not acl.verify_access("u1500", "f1", operation="write", expect_success=False):
        log.error("u1500 granted write despite only having rx")
        return 1

    acl.client.exec_command(sudo=True, cmd=f"rm -f {acl_file}")
    log.info("Duplicate ACE Handling: PASSED")
    return 0


def _test_scale_limit(acl):
    log.info("=== Test: Scale Limit ===")
    acl.create_file("f1")
    big_acl_file = "/tmp/acl_big.txt"

    log.info("Generating large ACL file (UIDs 1000-4270 = 3271 entries)")
    acl.generate_acl_file(big_acl_file, uid_start=1000, uid_end=4270, permission="rx")

    out, err = acl.set_acl_from_file_expect_fail("f1", big_acl_file)
    log.info("Scale limit apply output: %s, stderr: %s", out, err)

    entries = acl.get_acl("f1")
    count = len(entries)
    log.info("ACE count after scale limit test: %d (attempted 3271)", count)

    if count <= 3271:
        log.error(
            "Scale limit not enforced: expected 3271 ACEs, got %d",
            count,
        )
        acl.client.exec_command(sudo=True, cmd=f"rm -f {big_acl_file}")
        return 1

    acl.client.exec_command(sudo=True, cmd=f"rm -f {big_acl_file}")
    log.info("Scale Limit: PASSED (server capped ACEs at 3271)")
    return 0


def _test_persistence_remount(
    acl, client, nfs_server, nfs_mount, nfs_export, version, port, mount_type
):
    log.info("=== Test: Persistence After Remount ===")
    acl_file = _apply_2001_aces(acl)

    if not acl.verify_acl_contains("f1", f"A::1500:{PERM_RX}"):
        log.error("Pre-remount spot check failed for UID 1500")
        return 1
    if not acl.verify_acl_contains("f1", f"A::2500:{PERM_RX}"):
        log.error("Pre-remount spot check failed for UID 2500")
        return 1

    log.info("Pre-remount access verification: u1500 should read")
    if not acl.verify_access("u1500", "f1", operation="read", expect_success=True):
        log.error("u1500 denied read before remount")
        return 1

    pre_remount_count = len(acl.get_acl("f1"))
    log.info("ACE count before remount: %d", pre_remount_count)

    NfsAcl.remount_export(
        client,
        nfs_mount,
        nfs_server,
        f"{nfs_export}_0",
        version=str(version),
        port=str(port),
        mount_type=mount_type,
    )
    sleep(5)

    if not acl.verify_acl_contains("f1", f"A::1500:{PERM_RX}"):
        log.error("ACL lost after remount for UID 1500")
        return 1
    if not acl.verify_acl_contains("f1", f"A::2500:{PERM_RX}"):
        log.error("ACL entry for UID 2500 lost after remount")
        return 1

    entries = acl.get_acl("f1")
    post_remount_count = len(entries)
    log.info(
        "ACE count before remount=%d, after remount=%d",
        pre_remount_count,
        post_remount_count,
    )
    if pre_remount_count != post_remount_count:
        log.error(
            "ACE count changed after remount! before=%d, after=%d",
            pre_remount_count,
            post_remount_count,
        )
        return 1

    log.info("Post-remount access verification: u1500 should still read")
    if not acl.verify_access("u1500", "f1", operation="read", expect_success=True):
        log.error("u1500 denied read after remount (ACL not persisted)")
        return 1
    if not acl.verify_access("u4000", "f1", operation="read", expect_success=False):
        log.error("u4000 granted read after remount (should still be denied)")
        return 1

    acl.client.exec_command(sudo=True, cmd=f"rm -f {acl_file}")
    log.info("Persistence After Remount: PASSED")
    return 0
