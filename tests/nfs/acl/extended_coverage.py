"""
NFS v4 ACL Extended Coverage Tests.

Covers additional customer-found scenarios beyond functional/negative/scale/
hotfix coverage:

  1. A directory carrying an inherited GROUP@ access rule (``A:fdi:GROUP@:...``
     with write/append/delete-child permissions) must pass that write access
     on to newly created children -- regardless of the creating client's
     umask. A default/inherited ACL rule is supposed to override the umask,
     not the other way around.
     (IBMCEPH-21565 -- inherited GROUP@ write bits are lost on both new
     subdirectories and new files; the file case additionally gets an
     explicit Deny entry for write instead of the inherited Allow.)

  2. A partial-permission inheritable ALLOW ACE (``A:fdig:<gid>:w``) must be
     removable by reapplying the exact same ACE string that was used to add
     it, via ``nfs4_setfacl -x``.
     (IBMCEPH-19377 -- the server silently rewrites the single requested ACE
     into a DENY+ALLOW pair at add time, e.g. ``D:fdig:<gid>:rx`` +
     ``A:fdig:<gid>:waDtcy``; because ``-x`` matches by exact ACE string, the
     original string never matches what is actually stored, so ``-x`` exits
     0 ("success") while the entry for ``<gid>`` remains untouched.)

  3. An explicit DENY-all ACE for OWNER@ (``nfs4_setfacl -s
     'D::OWNER@:rwaDxtTcCy'``) must actually deny the owner all access.
     (IBMCEPH-21577 -- a compatibility patch that backfills empty base ACL
     entries from the file's mode bits cannot tell "empty because OWNER@ was
     never mentioned" apart from "empty because OWNER@ was explicitly
     denied everything", so it silently reconstructs an ALLOW for the owner
     from the pre-existing mode bits, discarding the explicit DENY.)

  4. A directory carrying an inherited EVERYONE@ access rule
     (``A:fdi:EVERYONE@:...`` with write/append/delete-child permissions)
     must pass that write access on to newly created children -- same as
     item 1, but for the EVERYONE@ special identity.
     (IBMCEPH-21579 -- same root cause as IBMCEPH-21565: the umask-override
     check only fires when the parent's default ACL has a POSIX ACL_MASK
     entry, which is only created for named user/group ACEs, never for
     OWNER@/GROUP@/EVERYONE@. Inherited EVERYONE@ write bits are lost on
     both new subdirectories and new files; the file case additionally
     gets an explicit Deny entry for write instead of the inherited
     Allow.)

  5. Concurrent ``nfs4_setfacl``/``nfs4_getfacl`` from different clients on
     the same file must not cause ACL reads to fail.
     (IBMCEPH-21590 -- NFS-Ganesha exposes NFSv4 ACLs via the kernel's
     "system.nfs4_acl" pseudo-xattr, queried with the standard two-step
     ``getxattr(NULL, 0)`` (size) then ``getxattr(buf, size)`` (value).
     Each call independently re-synthesizes the ACL from current server
     state with nothing tying the two together, so while one client is
     actively resizing the file's ACL, another client's read can be handed
     a stale size and the kernel returns ``ERANGE`` ("Numerical result out
     of range") -- reproducible at a double-digit-percent hit rate under
     completely ordinary concurrent multi-client ACL usage, with no crash
     on the server side.)
"""

import re

from cli.exceptions import ConfigError
from tests.nfs.lib.nfs_acl import NfsAcl
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

# Group used to exercise GROUP@ special-identity inheritance
GID_W = 6000
GROUP_W = "nfsacl_wgrp"
UID_W = 6001
USER_W = "nfsacl_wuser"

PERM_DIR = "rwaDxtcy"

BUG_ID = "IBMCEPH-21565"

# Group id used to exercise the partial-permission wildcard ALLOW ACE
# removal scenario. No actual group needs to exist on the client -- the
# ACE identity is just a numeric gid on the wire.
WILDCARD_GID = 4020
WILDCARD_ACE = f"A:fdig:{WILDCARD_GID}:w"
BUG_ID_WILDCARD_ACE_REMOVAL = "IBMCEPH-19377"

# User used to exercise the OWNER@ explicit-deny bypass scenario.
OWNER_DENY_UID = 7001
OWNER_DENY_USER = "nfsacl_denyowner"
BUG_ID_OWNER_DENY_BYPASS = "IBMCEPH-21577"

BUG_ID_EVERYONE_WRITE_UMASK = "IBMCEPH-21579"

# Parameters for the concurrent ACL set/get race scenario. No real users
# need to exist on the client -- nfs4_setfacl accepts raw numeric ids, and
# this test only checks read consistency, not access enforcement.
RACE_LARGE_ACE_COUNT = 60
RACE_LARGE_BASE_UID = 50000
RACE_SMALL_UID = 60000
RACE_DURATION_SECS = 15
BUG_ID_CONCURRENT_ACL_RACE = "IBMCEPH-21590"

# Known issues: tests listed here are still executed and reported, but a
# failure is marked as "KNOWN" instead of a hard failure.
KNOWN_ISSUES = {
    "Inherited GROUP@ Dir Write vs Umask": BUG_ID,
    "Inherited GROUP@ File Write vs Umask": BUG_ID,
    "Wildcard ALLOW ACE Removal (fdig)": BUG_ID_WILDCARD_ACE_REMOVAL,
    "OWNER@ Explicit Deny Bypass": BUG_ID_OWNER_DENY_BYPASS,
    "Inherited EVERYONE@ Dir Write vs Umask": BUG_ID_EVERYONE_WRITE_UMASK,
    "Inherited EVERYONE@ File Write vs Umask": BUG_ID_EVERYONE_WRITE_UMASK,
    "Concurrent ACL Read Race": BUG_ID_CONCURRENT_ACL_RACE,
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
        + "  NFS ACL EXTENDED COVERAGE\n"
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
            enable_rdma=config.get("enable_rdma", False),
            rdma_port=config.get("rdma_port"),
        )

        acl = NfsAcl(client, nfs_mount)
        acl.install_acl_tools()
        # Restrictive umask on purpose: an inherited ACL rule is supposed to
        # take precedence over the creating client's umask, not the other
        # way around (IBMCEPH-21565).
        acl.set_umask("0027")

        NfsAcl.create_group(client, GROUP_W, GID_W)
        NfsAcl.create_user(client, USER_W, UID_W, gid=GID_W)

        results = []
        results.append(
            (
                "Inherited GROUP@ Dir Write vs Umask",
                _run_test(_test_inherited_group_write_dir_umask, acl),
            )
        )
        results.append(
            (
                "Inherited GROUP@ File Write vs Umask",
                _run_test(_test_inherited_group_write_file_umask, acl),
            )
        )
        results.append(
            (
                "Wildcard ALLOW ACE Removal (fdig)",
                _run_test(_test_wildcard_allow_ace_removal, acl),
            )
        )
        results.append(
            (
                "OWNER@ Explicit Deny Bypass",
                _run_test(_test_owner_deny_bypass, acl),
            )
        )
        results.append(
            (
                "Inherited EVERYONE@ Dir Write vs Umask",
                _run_test(_test_inherited_everyone_write_dir_umask, acl),
            )
        )
        results.append(
            (
                "Inherited EVERYONE@ File Write vs Umask",
                _run_test(_test_inherited_everyone_write_file_umask, acl),
            )
        )
        results.append(
            (
                "Concurrent ACL Read Race",
                _run_test(_test_concurrent_acl_read_race, acl),
            )
        )

        return _report_results(results)

    except Exception as e:
        log.error("Extended coverage test hit an exception: %s", e)
        return 1
    finally:
        log.info("Cleanup: removing test user/group and NFS cluster")
        for c in clients:
            NfsAcl.delete_user(c, USER_W)
            NfsAcl.delete_group(c, GROUP_W)
            NfsAcl.delete_user(c, OWNER_DENY_USER)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
        log.info("Cleanup completed")


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
    log.info("EXTENDED COVERAGE RESULTS")
    log.info("=" * 60)
    for name, rc in results:
        if rc == 0:
            log.info("  %-40s PASS", name)
        elif name in KNOWN_ISSUES:
            log.info("  %-40s FAIL (KNOWN: %s)", name, KNOWN_ISSUES[name])
            known_failures.append(name)
        else:
            log.info("  %-40s FAIL", name)
            hard_failures.append(name)
    log.info("=" * 60)
    if known_failures:
        log.warning("Known failures: %s", known_failures)
    if hard_failures:
        log.error("Unexpected failures: %s", hard_failures)
    if hard_failures or known_failures:
        return 1
    log.info("All extended coverage tests passed")
    return 0


def _group_write_allowed(entries):
    """
    Return True only if GROUP@ is genuinely allowed to write.

    A naive "does any GROUP@ entry contain 'w'" check is not sufficient here:
    IBMCEPH-21565's file-creation symptom is an explicit Deny entry
    (``D::GROUP@:wax``) which itself contains the letter 'w'. So a Deny
    entry for GROUP@ carrying 'w' must count as "write NOT allowed", and an
    Allow entry for GROUP@ must actually be present and carry 'w' for this
    to return True.
    """
    denied = any(
        e.startswith("D:") and "GROUP@" in e and "w" in e.split(":")[-1]
        for e in entries
    )
    if denied:
        return False
    return any(
        e.startswith("A:") and "GROUP@" in e and "w" in e.split(":")[-1]
        for e in entries
    )


def _setup_write_inherit_parent(acl):
    """
    Create a parent directory owned by GROUP_W with setgid, carrying an
    inherit-only GROUP@ rule that grants write (IBMCEPH-21565 setup).
    """
    parent = "write-inherit-parent"
    acl.cleanup_test_files(parent)
    acl.create_dir(parent)

    parent_path = acl._full_path(parent)
    # chown to the test group so GROUP@ in the ACE below resolves to GROUP_W;
    # setgid (2770) so new children inherit GROUP_W as their owning group.
    acl.client.exec_command(sudo=True, cmd=f"chown root:{GROUP_W} {parent_path}")
    acl.chmod(parent, "2770")

    acl.add_acl(parent, f"A:fdi:GROUP@:{PERM_DIR}")
    if not acl.verify_acl_contains(parent, f"A:fdi:GROUP@:{PERM_DIR}"):
        log.error("Inherit ACE A:fdi:GROUP@:%s missing on parent", PERM_DIR)
        return None
    return parent


def _test_inherited_group_write_dir_umask(acl):
    """
    A new subdirectory created under a parent's inherited GROUP@ write rule
    must retain write access, regardless of the creating client's umask.

    Bug (IBMCEPH-21565): the subdirectory's own GROUP@ access ACE comes back
    as e.g. ``A::GROUP@:rxtcy`` -- write/append/delete-child silently
    dropped -- and the resulting mode bits (e.g. 2750 under umask 0027)
    match plain umask-only behaviour, as if the inherited rule were never
    applied.
    """
    log.info("=== Test: Inherited GROUP@ Dir Write vs Umask (%s) ===", BUG_ID)
    parent = _setup_write_inherit_parent(acl)
    if parent is None:
        return 1

    acl.create_dir(f"{parent}/child-dir")

    entries = acl.get_acl(f"{parent}/child-dir")
    if not _group_write_allowed(entries):
        log.error(
            "[%s] child-dir's inherited GROUP@ ACE lost write bits. ACL: %s",
            BUG_ID,
            entries,
        )
        return 1

    # Functional proof: a non-owner member of GROUP_W should actually be
    # able to create something inside child-dir, not just show the right
    # letters in nfs4_getfacl.
    out, err = acl.run_as_user(
        USER_W,
        f"touch {acl._full_path(parent + '/child-dir/x')}",
        check_ec=False,
    )
    if acl._is_permission_failure(err):
        log.error(
            "[%s] %s denied create under child-dir despite inherited "
            "GROUP@ write ACE",
            BUG_ID,
            USER_W,
        )
        return 1

    log.info("Inherited GROUP@ Dir Write vs Umask: PASSED")
    return 0


def _test_inherited_group_write_file_umask(acl):
    """
    A new file created under a parent's inherited GROUP@ write rule must
    retain write access, regardless of the creating client's umask.

    Bug (IBMCEPH-21565): the file comes back with an explicit Deny entry
    (``D::GROUP@:wax``) instead of the inherited Allow, and the resulting
    mode bits (e.g. 640 under umask 0027) match plain umask-only behaviour,
    as if the inherited rule were never applied.
    """
    log.info("=== Test: Inherited GROUP@ File Write vs Umask (%s) ===", BUG_ID)
    parent = "write-inherit-parent"
    if not acl.verify_acl_contains(parent, f"A:fdi:GROUP@:{PERM_DIR}"):
        parent = _setup_write_inherit_parent(acl)
        if parent is None:
            return 1

    acl.create_file(f"{parent}/child-file")

    entries = acl.get_acl(f"{parent}/child-file")
    if not _group_write_allowed(entries):
        log.error(
            "[%s] child-file's inherited GROUP@ ACE lost write bits (or was "
            "replaced by a Deny). ACL: %s",
            BUG_ID,
            entries,
        )
        return 1

    if not acl.verify_access(
        USER_W, f"{parent}/child-file", operation="write", expect_success=True
    ):
        log.error(
            "[%s] %s denied write to child-file despite inherited GROUP@ " "write ACE",
            BUG_ID,
            USER_W,
        )
        return 1

    log.info("Inherited GROUP@ File Write vs Umask: PASSED")
    return 0


def _acl_has_identity(entries, identity):
    """Return True if any ACL entry references the given numeric uid/gid."""
    needle = f":{identity}:"
    return any(needle in e for e in entries)


def _test_wildcard_allow_ace_removal(acl):
    """
    A partial-permission inheritable ALLOW ACE (``A:fdig:<gid>:w``) must be
    removable by reapplying the exact same ACE string that was used to add
    it.

    Bug (IBMCEPH-19377): Ganesha's NFSv4<->POSIX ACL translation cannot
    represent a partial permission set ("w" only) as a single POSIX ACL
    entry, so it silently rewrites the one ACE the client sent into a
    DENY+ALLOW pair (e.g. ``D:fdig:<gid>:rx`` + ``A:fdig:<gid>:waDtcy``).
    Because ``nfs4_setfacl -x`` matches by exact ACE string, the original
    string used on add never matches what is actually stored, so ``-x``
    exits 0 ("success") while the entry for ``<gid>`` remains untouched.
    """
    log.info(
        "=== Test: Wildcard ALLOW ACE Removal via Original String (%s) ===",
        BUG_ID_WILDCARD_ACE_REMOVAL,
    )
    name = "wildcard-allow-dir"
    acl.cleanup_test_files(name)
    acl.create_dir(name)

    acl.add_acl(name, WILDCARD_ACE)
    entries_after_add = acl.get_acl(name)
    if not _acl_has_identity(entries_after_add, WILDCARD_GID):
        log.error(
            "[%s] ACE for gid %s missing immediately after add. ACL: %s",
            BUG_ID_WILDCARD_ACE_REMOVAL,
            WILDCARD_GID,
            entries_after_add,
        )
        return 1

    acl.remove_acl(name, WILDCARD_ACE)
    entries_after_remove = acl.get_acl(name)
    if _acl_has_identity(entries_after_remove, WILDCARD_GID):
        log.error(
            "[%s] nfs4_setfacl -x '%s' reported success but left the gid %s "
            "ACE in place (server had silently rewritten it on add). "
            "ACL after -x: %s",
            BUG_ID_WILDCARD_ACE_REMOVAL,
            WILDCARD_ACE,
            WILDCARD_GID,
            entries_after_remove,
        )
        return 1

    log.info("Wildcard ALLOW ACE Removal via Original String: PASSED")
    return 0


def _owner_allow_granted(entries):
    """Return True if OWNER@ has any ALLOW entry carrying real permission bits."""
    return any(
        e.startswith("A::OWNER@:") and len(e.split(":")[-1]) > 0 for e in entries
    )


def _test_owner_deny_bypass(acl):
    """
    An explicit DENY-all ACE for OWNER@ must actually deny the owner all
    access to the file.

    Bug (IBMCEPH-21577): ceph_set_acl()'s compatibility patch backfills
    empty base POSIX ACL entries (ACL_USER_OBJ/ACL_GROUP_OBJ/ACL_OTHER) from
    the file's current mode bits whenever the converted ACL_USER_OBJ
    permset is empty. It cannot distinguish "empty because the client never
    mentioned OWNER@" from "empty because the client explicitly denied
    OWNER@ everything" (``D::OWNER@:rwaDxtTcCy`` with no ALLOW), so it
    silently reconstructs an ALLOW for the owner from the pre-existing mode
    bits, discarding the explicit DENY -- a real access-control bypass.
    """
    log.info("=== Test: OWNER@ Explicit Deny Bypass (%s) ===", BUG_ID_OWNER_DENY_BYPASS)
    name = "owner-deny-test"
    acl.cleanup_test_files(name)
    acl.create_file(name)
    path = acl._full_path(name)

    NfsAcl.create_user(acl.client, OWNER_DENY_USER, OWNER_DENY_UID)
    acl.client.exec_command(sudo=True, cmd=f"chown {OWNER_DENY_USER} {path}")
    acl.chmod(name, "644")

    # Full-replace with a single explicit DENY-all ACE for OWNER@ -- no
    # ALLOW ACE for OWNER@ anywhere in the spec.
    acl.set_acl(name, "D::OWNER@:rwaDxtTcCy")

    entries = acl.get_acl(name)
    if _owner_allow_granted(entries):
        log.error(
            "[%s] Explicit 'D::OWNER@:rwaDxtTcCy' was silently replaced by "
            "an ALLOW entry reconstructed from mode bits. ACL: %s",
            BUG_ID_OWNER_DENY_BYPASS,
            entries,
        )
        return 1

    if not acl.verify_access(
        OWNER_DENY_USER, name, operation="write", expect_success=False
    ):
        log.error(
            "[%s] %s was able to write to the file despite the explicit "
            "OWNER@ deny-all ACE",
            BUG_ID_OWNER_DENY_BYPASS,
            OWNER_DENY_USER,
        )
        return 1

    if not acl.verify_access(
        OWNER_DENY_USER, name, operation="read", expect_success=False
    ):
        log.error(
            "[%s] %s was able to read the file despite the explicit OWNER@ "
            "deny-all ACE",
            BUG_ID_OWNER_DENY_BYPASS,
            OWNER_DENY_USER,
        )
        return 1

    log.info("OWNER@ Explicit Deny Bypass: PASSED")
    return 0


def _everyone_write_allowed(entries):
    """
    Return True only if EVERYONE@ is genuinely allowed to write.

    Mirrors _group_write_allowed: IBMCEPH-21579's file-creation symptom is
    an explicit Deny entry (``D::EVERYONE@:rwax``) which itself contains
    the letter 'w', so a Deny entry for EVERYONE@ carrying 'w' must count
    as "write NOT allowed", and an Allow entry for EVERYONE@ must actually
    be present and carry 'w' for this to return True.
    """
    denied = any(
        e.startswith("D:") and "EVERYONE@" in e and "w" in e.split(":")[-1]
        for e in entries
    )
    if denied:
        return False
    return any(
        e.startswith("A:") and "EVERYONE@" in e and "w" in e.split(":")[-1]
        for e in entries
    )


def _setup_everyone_write_inherit_parent(acl):
    """
    Create a world-writable parent directory carrying an inherit-only
    EVERYONE@ rule that grants write (IBMCEPH-21579 setup).
    """
    parent = "everyone-write-parent"
    acl.cleanup_test_files(parent)
    acl.create_dir(parent)
    acl.chmod(parent, "777")

    acl.add_acl(parent, f"A:fdi:EVERYONE@:{PERM_DIR}")
    if not acl.verify_acl_contains(parent, f"A:fdi:EVERYONE@:{PERM_DIR}"):
        log.error("Inherit ACE A:fdi:EVERYONE@:%s missing on parent", PERM_DIR)
        return None
    return parent


def _test_inherited_everyone_write_dir_umask(acl):
    """
    A new subdirectory created under a parent's inherited EVERYONE@ write
    rule must retain write access, regardless of the creating client's
    umask.

    Bug (IBMCEPH-21579): same root cause as IBMCEPH-21565 but for the
    EVERYONE@ special identity -- ceph_fsal_mkdir()'s umask-override check
    only widens the create mode when the parent's default ACL has a POSIX
    ACL_MASK entry, which is only created for named user/group ACEs, never
    for OWNER@/GROUP@/EVERYONE@. The subdirectory's own EVERYONE@ access
    ACE comes back with write silently dropped, and the resulting mode
    bits match plain umask-only behaviour as if the inherited rule were
    never applied.
    """
    log.info(
        "=== Test: Inherited EVERYONE@ Dir Write vs Umask (%s) ===",
        BUG_ID_EVERYONE_WRITE_UMASK,
    )
    parent = _setup_everyone_write_inherit_parent(acl)
    if parent is None:
        return 1

    acl.create_dir(f"{parent}/child-dir")

    entries = acl.get_acl(f"{parent}/child-dir")
    if not _everyone_write_allowed(entries):
        log.error(
            "[%s] child-dir's inherited EVERYONE@ ACE lost write bits. ACL: %s",
            BUG_ID_EVERYONE_WRITE_UMASK,
            entries,
        )
        return 1

    # Functional proof: an arbitrary non-owner user (covered by EVERYONE@)
    # should actually be able to create something inside child-dir, not
    # just show the right letters in nfs4_getfacl.
    out, err = acl.run_as_user(
        USER_W,
        f"touch {acl._full_path(parent + '/child-dir/x')}",
        check_ec=False,
    )
    if acl._is_permission_failure(err):
        log.error(
            "[%s] %s denied create under child-dir despite inherited "
            "EVERYONE@ write ACE",
            BUG_ID_EVERYONE_WRITE_UMASK,
            USER_W,
        )
        return 1

    log.info("Inherited EVERYONE@ Dir Write vs Umask: PASSED")
    return 0


def _test_inherited_everyone_write_file_umask(acl):
    """
    A new file created under a parent's inherited EVERYONE@ write rule
    must retain write access, regardless of the creating client's umask.

    Bug (IBMCEPH-21579): the file comes back with an explicit Deny entry
    (``D::EVERYONE@:rwax``) instead of the inherited Allow, and the
    resulting mode bits match plain umask-only behaviour, as if the
    inherited rule were never applied.
    """
    log.info(
        "=== Test: Inherited EVERYONE@ File Write vs Umask (%s) ===",
        BUG_ID_EVERYONE_WRITE_UMASK,
    )
    parent = "everyone-write-parent"
    if not acl.verify_acl_contains(parent, f"A:fdi:EVERYONE@:{PERM_DIR}"):
        parent = _setup_everyone_write_inherit_parent(acl)
        if parent is None:
            return 1

    acl.create_file(f"{parent}/child-file")

    entries = acl.get_acl(f"{parent}/child-file")
    if not _everyone_write_allowed(entries):
        log.error(
            "[%s] child-file's inherited EVERYONE@ ACE lost write bits (or "
            "was replaced by a Deny). ACL: %s",
            BUG_ID_EVERYONE_WRITE_UMASK,
            entries,
        )
        return 1

    if not acl.verify_access(
        USER_W, f"{parent}/child-file", operation="write", expect_success=True
    ):
        log.error(
            "[%s] %s denied write to child-file despite inherited "
            "EVERYONE@ write ACE",
            BUG_ID_EVERYONE_WRITE_UMASK,
            USER_W,
        )
        return 1

    log.info("Inherited EVERYONE@ File Write vs Umask: PASSED")
    return 0


def _test_concurrent_acl_read_race(acl):
    """
    nfs4_getfacl reads of a file must not fail while another client is
    concurrently resizing that file's ACL via nfs4_setfacl.

    Bug (IBMCEPH-21590): nfs4_getfacl queries the kernel's "system.nfs4_acl"
    pseudo-xattr with the standard two-step getxattr(NULL, 0) (size) then
    getxattr(buf, size) (value). Each call independently triggers a fresh
    GETATTR(FATTR4_ACL) round trip to Ganesha, which re-synthesizes the ACL
    from current server state with nothing tying the two calls together.
    If another client resizes the ACL between them, the kernel detects the
    size mismatch and returns ERANGE ("Numerical result out of range") to
    the reading application. Reproduced at a double-digit-percent hit rate
    with a single concurrent writer and reader; Ganesha itself never logs
    an error for this and the service is not disrupted -- it is purely a
    client-visible read-consistency failure under ordinary concurrent
    multi-client ACL usage.

    Runs one remote shell script that backgrounds a writer (alternating a
    60-entry and a 1-entry ACL) and loops nfs4_getfacl in the foreground
    for RACE_DURATION_SECS, then reports how many reads failed.
    """
    log.info("=== Test: Concurrent ACL Read Race (%s) ===", BUG_ID_CONCURRENT_ACL_RACE)
    name = "acl-race-test"
    acl.cleanup_test_files(name)
    acl.create_file(name)
    path = acl._full_path(name)

    large_spec = ",".join(
        f"A::{RACE_LARGE_BASE_UID + i}:r" for i in range(RACE_LARGE_ACE_COUNT)
    )
    small_spec = f"A::{RACE_SMALL_UID}:r"

    script = (
        f"F={path}\n"
        f"end=$((SECONDS+{RACE_DURATION_SECS}))\n"
        "( while [ $SECONDS -lt $end ]; do\n"
        f"    nfs4_setfacl -s '{large_spec}' \"$F\" 2>/dev/null\n"
        f"    nfs4_setfacl -s '{small_spec}' \"$F\" 2>/dev/null\n"
        "  done ) &\n"
        "WPID=$!\n"
        "n=0; fail=0\n"
        "while [ $SECONDS -lt $end ]; do\n"
        "  n=$((n+1))\n"
        '  out=$(nfs4_getfacl "$F" 2>&1)\n'
        '  if echo "$out" | grep -q "Failed operation"; then\n'
        "    fail=$((fail+1))\n"
        "    if [ $fail -le 5 ]; then\n"
        '      echo "SAMPLE_FAILURE #$fail (read $n): '
        '$(echo "$out" | tr "\\n" " ")"\n'
        "    fi\n"
        "  fi\n"
        "done\n"
        "wait $WPID\n"
        'echo "RACE_RESULT: $fail failures out of $n reads"\n'
    )

    out, _ = acl.client.exec_command(
        sudo=True, cmd=script, timeout=RACE_DURATION_SECS + 30
    )
    log.info("Concurrent ACL read race output:\n%s", out)

    match = re.search(r"RACE_RESULT: (\d+) failures out of (\d+) reads", out)
    if not match:
        log.error(
            "[%s] Could not parse race test result from output: %s",
            BUG_ID_CONCURRENT_ACL_RACE,
            out,
        )
        return 1

    failures, total = int(match.group(1)), int(match.group(2))
    hit_rate = (failures / total * 100) if total else 0
    log.info(
        "Concurrent ACL read race: %d/%d reads failed (%.1f%% hit rate)",
        failures,
        total,
        hit_rate,
    )

    if failures > 0:
        log.error(
            "[%s] %d/%d (%.1f%%) concurrent nfs4_getfacl reads failed with "
            "ERANGE while another client concurrently resized the same "
            "file's ACL",
            BUG_ID_CONCURRENT_ACL_RACE,
            failures,
            total,
            hit_rate,
        )
        return 1

    log.info("Concurrent ACL Read Race: PASSED")
    return 0
