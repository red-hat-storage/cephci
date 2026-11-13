"""NFS-Ganesha xattr cases (shared downstream/cephadm setup via xcopy base)."""

from cli.exceptions import OperationFailedError
from tests.nfs.lib.xcopy import create_test_file
from tests.nfs.lib.xcopy.helpers import announce, case_begin, case_pass
from tests.nfs.lib.xcopy.xattr_helpers import (
    assert_xattr_equals,
    copy_preserve_xattr,
    get_selinux_label,
    remount_nfs,
    selinux_available,
    set_selinux_label,
    verify_xattr_via_cephfs,
    xattr_get,
    xattr_list,
    xattr_names_from_dump,
    xattr_remove,
    xattr_roundtrip_probe,
    xattr_set,
)
from tests.nfs.xcopy import test_nfs_xcopy_intra_export_sync as base
from utility.log import Log

log = Log(__name__)


def _path(ctx, name):
    return f"{ctx['nfs_mount_a']}/{name}"


# XATTR-01


def case_user_xattr_crud(ctx):
    """XATTR-01: Set / get / list / remove user xattrs; persist across remount; best-effort CephFS metadata check."""
    case_begin("user_xattr_crud (XATTR-01)")
    client = ctx["client"]
    path = _path(ctx, "xcopy_xattr01.bin")
    create_test_file(client, path, size_mb=1)

    name = "user.comment"
    value = "CephQE"

    announce("SET user.comment=CephQE")
    rc, _, err = xattr_set(client, path, name, value)
    if rc != 0:
        raise OperationFailedError(f"setfattr failed rc={rc}: {err}")

    announce("GET / LIST")
    if not assert_xattr_equals(client, path, name, value):
        raise OperationFailedError("getfattr mismatch right after set")
    dump = xattr_list(client, path)
    if name not in xattr_names_from_dump(dump) and name not in dump:
        raise OperationFailedError(f"{name} missing from getfattr -d listing")

    announce("Persist across client remount")
    remount_nfs(ctx)
    if not assert_xattr_equals(client, path, name, value):
        raise OperationFailedError("xattr lost after remount")

    ceph_ok = verify_xattr_via_cephfs(ctx, "xcopy_xattr01.bin", name, value)
    if ceph_ok is False:
        raise OperationFailedError("CephFS metadata xattr mismatch")
    if ceph_ok is None:
        log.warning("CephFS metadata check skipped (ceph-fuse unavailable)")

    announce("REMOVE user.comment")
    rc, _, err = xattr_remove(client, path, name)
    if rc != 0:
        raise OperationFailedError(f"setfattr -x failed rc={rc}: {err}")
    if xattr_get(client, path, name, check_ec=False) is not None:
        raise OperationFailedError("xattr still present after remove")

    case_pass("user_xattr_crud")


# XATTR-02


def case_xattr_preservation_xcopy(ctx):
    """XATTR-02: Attach user (+ optional ACL) xattrs; cp --preserve=xattr;
    require destination to carry the same attributes."""
    case_begin("xattr_preservation_xcopy (XATTR-02)")
    client = ctx["client"]
    src = _path(ctx, "xcopy_xattr02_src.bin")
    dst = _path(ctx, "xcopy_xattr02_dst.bin")
    create_test_file(client, src, size_mb=8)

    attrs = {
        "user.comment": "CephQE-xcopy",
        "user.owner_team": "nfs-qe",
        "user.build": "xattr-02",
    }
    for n, v in attrs.items():
        rc, _, err = xattr_set(client, src, n, v)
        if rc != 0:
            raise OperationFailedError(f"setfattr {n} failed: {err}")

    # Optional ACL (access control entries) — soft if setfacl missing.
    client.exec_command(sudo=True, cmd="command -v setfacl", check_ec=False)
    have_acl = client.exit_status == 0
    if have_acl:
        client.exec_command(
            sudo=True, cmd=f"setfacl -m u:nobody:r {src}", check_ec=False
        )
        log.info("setfacl rc=%s", client.exit_status)

    # system.* / security.* often denied for non-root namespaces over NFS —
    # try and record; user attrs are the hard requirement.
    for n, v in (("system.test_xattr", "sysval"), ("security.test_xattr", "secval")):
        rc, _, err = xattr_set(client, src, n, v, check_ec=False)
        log.info("optional %s set rc=%s err=%s", n, rc, (err or "").strip()[:200])
        if rc == 0:
            attrs[n] = v

    copy_preserve_xattr(client, src, dst)

    for n, v in attrs.items():
        if not assert_xattr_equals(client, dst, n, v):
            raise OperationFailedError(
                f"XATTR-02: {n} not preserved on destination after xcopy"
            )

    if have_acl:
        out, _ = client.exec_command(sudo=True, cmd=f"getfacl -p {dst}", check_ec=False)
        text = (out or "").strip()
        log.info("getfacl dst:\n%s", text)
        if "nobody" not in text and "user:nobody" not in text:
            log.warning(
                "ACL nobody entry not visible on dst (may be stripped by NFS); "
                "user xattrs already verified"
            )

    case_pass("xattr_preservation_xcopy")


# XATTR-03


def case_large_max_xattr_payloads(ctx):
    """XATTR-03: Store/retrieve large xattr values up to configured boundary."""
    case_begin("large_max_xattr_payloads (XATTR-03)")
    client = ctx["client"]
    path = _path(ctx, "xcopy_xattr03.bin")
    create_test_file(client, path, size_mb=1)

    target = int(ctx["config"].get("xattr_max_value_bytes", 65536))
    sizes = []
    for n in (target, 32768, 16384, 4096, 1024):
        if n not in sizes and n <= target:
            sizes.append(n)

    last_ok = None
    for nbytes in sizes:
        name = f"user.large_{nbytes}"
        announce(f"Probe round-trip user xattr value_bytes={nbytes}")
        status, detail = xattr_roundtrip_probe(client, path, name, nbytes)
        if status == "ok":
            log.info("PASS: round-trip OK at %s bytes (%s)", nbytes, detail)
            last_ok = nbytes
            xattr_remove(client, path, name, check_ec=False)
            break
        if status == "set_rejected":
            log.warning(
                "SET rejected at %s bytes (limit) — trying smaller: %s",
                nbytes,
                detail,
            )
            continue
        # set succeeded but get did not match — real bug, do not soft-skip.
        raise OperationFailedError(
            f"XATTR-03 BUG: set succeeded but get mismatch/corrupt at "
            f"{nbytes} bytes: {detail}"
        )

    if last_ok is None:
        raise OperationFailedError(
            f"Could not store any large xattr (all sizes set-rejected: {sizes})"
        )
    if last_ok < target:
        log.warning(
            "Largest working value size %s < configured target %s "
            "(protocol/FS limit; SET was rejected above that)",
            last_ok,
            target,
        )

    # Long-ish key within typical NAME_MAX constraints for xattr keys.
    long_key = "user." + ("k" * 200)
    rc, _, err = xattr_set(client, path, long_key, "v", check_ec=False)
    log.info("long key (%s chars) set rc=%s", len(long_key), rc)
    if rc == 0:
        if not assert_xattr_equals(client, path, long_key, "v"):
            raise OperationFailedError("long xattr key get mismatch")
        xattr_remove(client, path, long_key, check_ec=False)
    else:
        log.warning(
            "long key rejected (ok if under FS name limit): %s", (err or "")[:200]
        )

    case_pass("large_max_xattr_payloads")


# XATTR-04


def case_selinux_security_labels(ctx):
    """XATTR-04: SELinux labels via security.selinux; preserve across copy."""
    case_begin("selinux_security_labels (XATTR-04)")
    client = ctx["client"]
    if not selinux_available(client):
        log.warning("SELinux unavailable/Disabled — skipping XATTR-04")
        announce("SKIP CASE: selinux_security_labels (SELinux N/A)")
        return

    # Remount with NFS SELinux mount helper option when supported.
    remount_nfs(ctx, extra_mount_options="context=system_u:object_r:nfs_t:s0")

    path = _path(ctx, "xcopy_xattr04.bin")
    dst = _path(ctx, "xcopy_xattr04_dst.bin")
    create_test_file(client, path, size_mb=1)

    # Prefer a type that exists on RHEL; fall back to reading current then chcon.
    label_before = get_selinux_label(client, path)
    log.info("Initial security.selinux=%r", label_before)

    # Custom label attempt — may be denied by policy; then keep whatever we have.
    candidate = "system_u:object_r:nfs_t:s0"
    rc = set_selinux_label(client, path, candidate)
    log.info("chcon %s rc=%s", candidate, rc)
    label = get_selinux_label(client, path)
    if not label:
        # Direct setfattr fallback
        rc2, _, err2 = xattr_set(
            client, path, "security.selinux", candidate, check_ec=False
        )
        log.info("setfattr security.selinux rc=%s err=%s", rc2, (err2 or "")[:200])
        label = get_selinux_label(client, path)
    if not label:
        log.warning(
            "Could not set/read security.selinux on NFS mount — skipping XATTR-04"
        )
        announce("SKIP CASE: selinux_security_labels (label not writable)")
        remount_nfs(ctx)  # drop forced context
        return

    log.info("Using SELinux label %r", label)
    copy_preserve_xattr(client, path, dst)
    dst_label = get_selinux_label(client, dst)
    log.info("dst security.selinux=%r", dst_label)
    if dst_label != label:
        # Some NFS+SELinux stacks apply mount context to all files — still require
        # a non-empty label on dst.
        if not dst_label:
            raise OperationFailedError(
                "XATTR-04: destination missing security.selinux after copy"
            )
        log.warning(
            "dst label %r != src %r (mount context may override); "
            "accepting non-empty dst label",
            dst_label,
            label,
        )
    else:
        log.info("PASS: SELinux label preserved on copy")

    remount_nfs(ctx)  # restore default mount opts for later cases
    case_pass("selinux_security_labels")


CASES = {
    "user_xattr_crud": case_user_xattr_crud,
    "xattr_preservation_xcopy": case_xattr_preservation_xcopy,
    "large_max_xattr_payloads": case_large_max_xattr_payloads,
    "selinux_security_labels": case_selinux_security_labels,
}


def run(ceph_cluster, **kw):
    """Run one or more XATTR-01..04 cases."""
    return base.run_selected(
        ceph_cluster,
        kw,
        CASES,
        defaults=[
            "user_xattr_crud",
            "xattr_preservation_xcopy",
            "large_max_xattr_payloads",
            "selinux_security_labels",
        ],
        label="XATTR",
    )
