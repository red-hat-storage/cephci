"""
NFS v4 ACL Hotfix Coverage Tests.

Covers customer/hotfix scenarios:
  1. Named-group ACE on parent applies immediately; inheritance only affects
     newly created children (not existing ones). Functional checks for a
     GID member vs a non-member.  (IBMCEPH-19501 — inherited file loses w/a)
  2. Multi-group inherit: several GIDs retain access on child-dir but lose
     w/a together on child-file.  (IBMCEPH-19502)
  3. Special-identity (OWNER@/GROUP@/EVERYONE@) access ACEs must remain after
     adding inherit-only (fdi) ACEs via nfs4_setfacl -a.
"""

from cli.exceptions import ConfigError
from tests.nfs.lib.nfs_acl import NfsAcl
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

# Group inherit workflow
GID_1 = 3100
GROUP_1 = "nfsacl_g3100"
USER_GID1 = "acluser1000"
UID_GID1 = 3200
GID_OTHER = 3101
GROUP_OTHER = "nfsacl_g3101"
USER_OTHER = "acluserOTHER"
UID_OTHER = 3201

# Multi-group inherit
MULTI_GIDS = (4020, 4046, 4050)
MULTI_GROUPS = ("g4020", "g4046", "g4050")
MULTI_USERS = ("u4020", "u4046", "u4050")
MULTI_UIDS = (40200, 40460, 40500)

PERM_DIR = "rwaDxtcy"

# Known issues: failures listed here are still executed and reported as KNOWN.
KNOWN_ISSUES = {
    "Inherited File Write Bits": "IBMCEPH-19501",
    "Multi-Group File Write Bits": "IBMCEPH-19501",
    "Special Identity Access ACE": "IBMCEPH-19502",
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
        + "  NFS ACL HOTFIX COVERAGE\n"
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
        # Avoid world-readable defaults (EVERYONE@:r) that mask ACL denial checks
        acl.set_umask("0027")

        _setup_identities(client)

        results = []
        results.append(
            ("Group Inheritance Workflow", _run_test(_test_group_inheritance, acl))
        )
        results.append(
            (
                "Inherited File Write Bits",
                _run_test(_test_inherited_file_write_bits, acl),
            )
        )
        results.append(
            ("Multi-Group Inheritance", _run_test(_test_multi_group_inheritance, acl))
        )
        results.append(
            (
                "Multi-Group File Write Bits",
                _run_test(_test_multi_group_file_write_bits, acl),
            )
        )
        results.append(
            ("Special Identity Access ACE", _run_test(_test_special_identity_aces, acl))
        )

        return _report_results(results)

    except Exception as e:
        log.error("Hotfix coverage test hit an exception: %s", e)
        return 1
    finally:
        log.info("Cleanup: removing test users/groups and NFS cluster")
        for c in clients:
            NfsAcl.delete_user(c, USER_GID1)
            NfsAcl.delete_user(c, USER_OTHER)
            for u in MULTI_USERS:
                NfsAcl.delete_user(c, u)
            NfsAcl.delete_group(c, GROUP_1)
            NfsAcl.delete_group(c, GROUP_OTHER)
            for g in MULTI_GROUPS:
                NfsAcl.delete_group(c, g)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
        log.info("Cleanup completed")


def _setup_identities(client):
    NfsAcl.create_group(client, GROUP_1, GID_1)
    NfsAcl.create_group(client, GROUP_OTHER, GID_OTHER)
    NfsAcl.create_user(client, USER_GID1, UID_GID1, gid=GID_1)
    NfsAcl.create_user(client, USER_OTHER, UID_OTHER, gid=GID_OTHER)

    for gname, gid, uname, uid in zip(
        MULTI_GROUPS, MULTI_GIDS, MULTI_USERS, MULTI_UIDS
    ):
        NfsAcl.create_group(client, gname, gid)
        NfsAcl.create_user(client, uname, uid, gid=gid)


def _run_test(fn, *args, **kwargs):
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
    hard_failures = []
    known_failures = []
    log.info("=" * 60)
    log.info("HOTFIX COVERAGE RESULTS")
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
    log.info("All hotfix coverage tests passed")
    return 0


def _ace_has_write(entries, who_fragment):
    """Return True if any ACE matching *who_fragment* includes write (w/a)."""
    for entry in entries:
        if who_fragment in entry and ("w" in entry.split(":")[-1]):
            return True
    return False


# ---------------------------------------------------------------------------
# 1) Named-group inheritance workflow
# ---------------------------------------------------------------------------


def _test_group_inheritance(acl):
    """Parent group ACE applies now; only new children inherit."""
    log.info("=== Test: Group Inheritance Workflow ===")
    parent = "parent-dir-1"
    acl.cleanup_test_files(parent)
    acl.create_dir(f"{parent}/child-dir-1")
    acl.create_file(f"{parent}/child-file-1")

    # Access ACE only (no inherit flags) — parent only
    acl.add_acl(parent, f"A:g:{GID_1}:{PERM_DIR}")
    if not acl.verify_acl_contains(parent, f"A:g:{GID_1}:{PERM_DIR}"):
        log.error("Access ACE A:g:%s:%s missing on parent", GID_1, PERM_DIR)
        return 1
    if not acl.verify_acl_not_contains(f"{parent}/child-dir-1", f"A:g:{GID_1}:"):
        log.error("Existing child-dir unexpectedly got GID ACE after non-inherit add")
        return 1

    # Inherit-only ACE (fdi + group)
    acl.add_acl(parent, f"A:fdig:{GID_1}:{PERM_DIR}")
    if not acl.verify_acl_contains(parent, f"A:fdig:{GID_1}:{PERM_DIR}"):
        log.error("Inherit ACE A:fdig:%s:%s missing on parent", GID_1, PERM_DIR)
        return 1
    if not acl.verify_acl_not_contains(f"{parent}/child-dir-1", f"A:g:{GID_1}:"):
        log.error("Existing child-dir changed after inherit ACE was added")
        return 1
    if not acl.verify_acl_not_contains(f"{parent}/child-file-1", f"A:g:{GID_1}:"):
        log.error("Existing child-file changed after inherit ACE was added")
        return 1

    # New children should inherit
    acl.create_dir(f"{parent}/new-child-dir")
    acl.create_file(f"{parent}/new-child-file")

    if not acl.verify_acl_contains(
        f"{parent}/new-child-dir", f"A:g:{GID_1}:{PERM_DIR}"
    ):
        log.error("new-child-dir missing inherited access ACE")
        return 1
    if not acl.verify_acl_contains(
        f"{parent}/new-child-dir", f"A:fdig:{GID_1}:{PERM_DIR}"
    ):
        log.error("new-child-dir missing inherited inherit-only ACE")
        return 1
    if not acl.verify_acl_contains(f"{parent}/new-child-file", f"A:g:{GID_1}:"):
        log.error("new-child-file missing inherited GID ACE")
        return 1

    # Member of GID: parent + new-child-dir OK; old children denied
    parent_path = acl._full_path(parent)
    out, err = acl.run_as_user(
        USER_GID1, f"touch {parent_path}/from-member.txt", check_ec=False
    )
    if err and ("denied" in err.lower() or "permission" in err.lower()):
        log.error("GID member cannot create under parent")
        return 1
    acl.client.exec_command(sudo=True, cmd=f"rm -f {parent_path}/from-member.txt")

    out, err = acl.run_as_user(
        USER_GID1, f"ls {acl._full_path(parent + '/new-child-dir')}", check_ec=False
    )
    if err and ("denied" in err.lower() or "permission" in err.lower()):
        log.error("GID member cannot list new-child-dir")
        return 1

    out, err = acl.run_as_user(
        USER_GID1,
        f"touch {acl._full_path(parent + '/new-child-dir/file-by-member')}",
        check_ec=False,
    )
    if err and ("denied" in err.lower() or "permission" in err.lower()):
        log.error("GID member cannot create inside new-child-dir")
        return 1

    if not acl.verify_access(
        USER_GID1, f"{parent}/new-child-file", operation="read", expect_success=True
    ):
        log.error("GID member cannot read new-child-file")
        return 1

    out, err = acl.run_as_user(
        USER_GID1, f"ls {acl._full_path(parent + '/child-dir-1')}", check_ec=False
    )
    if not (err and ("denied" in err.lower() or "permission" in err.lower())):
        log.error("GID member unexpectedly listed pre-existing child-dir-1")
        return 1
    if not acl.verify_access(
        USER_GID1, f"{parent}/child-file-1", operation="read", expect_success=False
    ):
        log.error("GID member unexpectedly read pre-existing child-file-1")
        return 1

    # Non-member denied on parent create / new children
    out, err = acl.run_as_user(
        USER_OTHER, f"touch {parent_path}/should-fail-other", check_ec=False
    )
    if not (err and ("denied" in err.lower() or "permission" in err.lower())):
        log.error("Non-member unexpectedly created under parent")
        return 1
    out, err = acl.run_as_user(
        USER_OTHER, f"ls {acl._full_path(parent + '/new-child-dir')}", check_ec=False
    )
    if not (err and ("denied" in err.lower() or "permission" in err.lower())):
        log.error("Non-member unexpectedly listed new-child-dir")
        return 1
    if not acl.verify_access(
        USER_OTHER, f"{parent}/new-child-file", operation="read", expect_success=False
    ):
        log.error("Non-member unexpectedly read new-child-file")
        return 1

    log.info("Group Inheritance Workflow: PASSED")
    return 0


def _test_inherited_file_write_bits(acl):
    """
    Inherited named-group ACE on a new file should retain write (w/a).

    Current bug (IBMCEPH-19501): child-file gets A:g:<gid>:rtcy and write is denied.
    """
    log.info("=== Test: Inherited File Write Bits ===")
    parent = "parent-dir-1"
    # Reuse tree from previous test when present; otherwise rebuild
    if not acl.verify_acl_contains(parent, f"A:fdig:{GID_1}:{PERM_DIR}"):
        log.info("Parent inherit setup missing; rebuilding")
        if _test_group_inheritance(acl) != 0:
            log.error("Failed to rebuild group inheritance setup")
            return 1

    entries = acl.get_acl(f"{parent}/new-child-file")
    if not _ace_has_write(entries, f"g:{GID_1}"):
        log.error(
            "Inherited file ACE for GID %s lost write bits. ACL: %s", GID_1, entries
        )
        return 1
    if not acl.verify_access(
        USER_GID1, f"{parent}/new-child-file", operation="write", expect_success=True
    ):
        log.error("GID member cannot write new-child-file despite expected w/a")
        return 1

    log.info("Inherited File Write Bits: PASSED")
    return 0


# ---------------------------------------------------------------------------
# 2) Multi-group inheritance
# ---------------------------------------------------------------------------


def _setup_multi_group_parent(acl):
    parent = "multi-group-parent"
    acl.cleanup_test_files(parent)
    acl.create_dir(parent)
    for gid in MULTI_GIDS:
        acl.add_acl(parent, f"A:g:{gid}:{PERM_DIR}")
        acl.add_acl(parent, f"A:fdig:{gid}:{PERM_DIR}")
    for gid in MULTI_GIDS:
        if not acl.verify_acl_contains(parent, f"A:g:{gid}:{PERM_DIR}"):
            log.error("Missing access ACE for GID %s on parent", gid)
            return None
        if not acl.verify_acl_contains(parent, f"A:fdig:{gid}:{PERM_DIR}"):
            log.error("Missing inherit ACE for GID %s on parent", gid)
            return None
    acl.create_dir(f"{parent}/child-dir")
    acl.create_file(f"{parent}/child-file")
    return parent


def _test_multi_group_inheritance(acl):
    """Multiple named-group ACEs inherit onto new children."""
    log.info("=== Test: Multi-Group Inheritance ===")
    parent = _setup_multi_group_parent(acl)
    if parent is None:
        return 1

    for gid in MULTI_GIDS:
        if not acl.verify_acl_contains(f"{parent}/child-dir", f"A:g:{gid}:{PERM_DIR}"):
            log.error("child-dir missing access ACE for GID %s", gid)
            return 1
        if not acl.verify_acl_contains(
            f"{parent}/child-dir", f"A:fdig:{gid}:{PERM_DIR}"
        ):
            log.error("child-dir missing inherit ACE for GID %s", gid)
            return 1
        if not acl.verify_acl_contains(f"{parent}/child-file", f"A:g:{gid}:"):
            log.error("child-file missing ACE for GID %s", gid)
            return 1

    # Functional: first group user can create under child-dir and read child-file
    out, err = acl.run_as_user(
        MULTI_USERS[0],
        f"touch {acl._full_path(parent + '/child-dir/f1')}",
        check_ec=False,
    )
    if err and ("denied" in err.lower() or "permission" in err.lower()):
        log.error("%s cannot create under child-dir", MULTI_USERS[0])
        return 1
    if not acl.verify_access(
        MULTI_USERS[0], f"{parent}/child-file", operation="read", expect_success=True
    ):
        log.error("%s cannot read child-file", MULTI_USERS[0])
        return 1

    log.info("Multi-Group Inheritance: PASSED")
    return 0


def _test_multi_group_file_write_bits(acl):
    """
    All named-group ACEs on inherited child-file should retain write (w/a).

    Current bug (IBMCEPH-19502): all GIDs become rtcy together; write denied.
    """
    log.info("=== Test: Multi-Group File Write Bits ===")
    parent = "multi-group-parent"
    if not acl.verify_acl_contains(f"{parent}/child-file", f"A:g:{MULTI_GIDS[0]}:"):
        parent = _setup_multi_group_parent(acl)
        if parent is None:
            return 1

    entries = acl.get_acl(f"{parent}/child-file")
    for gid in MULTI_GIDS:
        if not _ace_has_write(entries, f"g:{gid}"):
            log.error(
                "child-file ACE for GID %s lost write bits. ACL: %s", gid, entries
            )
            return 1

    if not acl.verify_access(
        MULTI_USERS[0], f"{parent}/child-file", operation="write", expect_success=True
    ):
        log.error("%s cannot write child-file despite expected w/a", MULTI_USERS[0])
        return 1

    log.info("Multi-Group File Write Bits: PASSED")
    return 0


# ---------------------------------------------------------------------------
# 3) Special-identity access ACE retention
# ---------------------------------------------------------------------------


def _test_special_identity_aces(acl):
    """
    ACCESS Allow ACEs for OWNER@/GROUP@/EVERYONE@ must remain after adding
    inherit-only (fdi) ACEs for the same specials.
    """
    log.info("=== Test: Special Identity Access ACE ===")
    parent = "special-id-parent"
    acl.cleanup_test_files(parent)
    acl.create_dir(parent)

    acl.add_acl(parent, "A::OWNER@:rwaDxtTcCy")
    acl.add_acl(parent, "A::GROUP@:rwaDxtcy")
    acl.add_acl(parent, "A::EVERYONE@:rxtcy")
    acl.add_acl(parent, "A:fdi:OWNER@:rwaDxtTcCy")
    acl.add_acl(parent, "A:fdi:GROUP@:rwaDxtcy")
    acl.add_acl(parent, "A:fdi:EVERYONE@:rxtcy")

    entries = acl.get_acl(parent)
    log.info("Parent ACL after special-identity set: %s", entries)

    required = [
        "A::OWNER@:rwaDxtTcCy",
        "A::GROUP@:rwaDxtcy",
        "A::EVERYONE@:rxtcy",
        "A:fdi:OWNER@:rwaDxtTcCy",
        "A:fdi:GROUP@:rwaDxtcy",
        "A:fdi:EVERYONE@:rxtcy",
    ]
    missing = [ace for ace in required if not any(ace in e for e in entries)]
    if missing:
        log.error(
            "Special-identity ACL conversion dropped expected ACEs: %s. Full ACL: %s",
            missing,
            entries,
        )
        return 1

    log.info("Special Identity Access ACE: PASSED")
    return 0
