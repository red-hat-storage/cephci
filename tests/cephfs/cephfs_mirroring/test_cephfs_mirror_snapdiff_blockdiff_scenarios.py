"""
test_cephfs_mirror_snapdiff_blockdiff_scenarios.py

CEPH-83633198 — validate_snapdiff_blockdiff_scenarios

Snapdiff / blockdiff incremental-sync regressions (RHCS >= 10.0).

Ported from upstream qa/tasks/cephfs/test_mirroring.py:

1. test_cephfs_mirror_blockdiff_pure_truncate (PR #70923 / #79156)
   Size-only truncate / sparse grow must update remote size when blockdiff
   returns zero changed extents.

2. test_cephfs_mirror_blockdiff_empty_clones (PR #73531 / #82726)
   Blockdiff must handle objects that are empty on both sides of a snapshot
   (adjacent size-0 LIST_SNAPS entries) without crashing the MDS.

3. test_cephfs_mirror_sparse_file_sync (PR #70780 / #78964)
   Incremental blockdiff over a sparse file with absent hole objects must
   treat holes as -ENOENT (not -EIO) and complete sync.

4. test_cephfs_mirror_incremental_sync_with_directory_rename (PR #70757)
   Unchanged descendants survive same-parent rename, cross-parent rename,
   and same-name directory replacement across snapdiff sync.

Uses ceph-fuse mounts. No asok — waits via
``ceph fs snapshot mirror status`` and verifies with md5sum_script helper.
"""

import json
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

# Defaults match cephfs-mirror option cephfs_mirror_blockdiff_min_file_size (16 MiB).
_BLOCKDIFF_MIN_SIZE = 16 * 1024 * 1024
_OBJECT_SIZE = 4 * 1024 * 1024  # default CephFS object size
_NUM_OBJECTS = 8  # 32 MiB with default 4 MiB layout
_CLONE_WAIT_TIMEOUT = 120
_CLONE_WAIT_INTERVAL = 2
# TODO(BZ-tracker#74984): pin mtimes so snapdiff reports truncated/regrown files.
_MTIME_SNAP_A = "2020-01-01 00:00:00 UTC"
_MTIME_SNAP_B = "2020-01-01 00:00:02 UTC"
_CASES = [
    # (file_name, initial_size_mb, final_size_mb)
    ("object_boundary_shrink", 64, 32),
    ("partial_object_shrink", 64, 33),
    ("same_object_shrink", 19, 18),
    ("sparse_grow", 20, 40),
]
_MD5SUM_SCRIPT = "md5sum_script.sh"
_MD5SUM_DST = f"/root/{_MD5SUM_SCRIPT}"
_MD5SUM_SRC = "tests/cephfs/cephfs_mirror_upgrade/md5sum_script.sh"


def _scenario_dirs(suffix):
    """Return unique top-level dir names for this run (avoid remount collisions)."""
    return {
        "pure": f"d0_{suffix}",
        "empty": f"empty_clones_{suffix}",
        "sparse": f"sparse_sync_{suffix}",
        "rename": f"directory_rename_{suffix}",
        "sparse_file": "sparse",
    }


def _write_file(client, path, content):
    """Write exact content (no added newline)."""
    parent = path.rsplit("/", 1)[0]
    client.exec_command(sudo=True, cmd=f"mkdir -p {parent}")
    escaped = content.replace("'", "'\"'\"'")
    client.exec_command(sudo=True, cmd=f"printf '%s' '{escaped}' > {path}")


def _read_file(client, path):
    out, _ = client.exec_command(sudo=True, cmd=f"cat {path}")
    return out


def _path_exists(client, path):
    try:
        client.exec_command(sudo=True, cmd=f"test -e {path}", check_ec=True)
        return True
    except CommandFailed:
        return False


def _list_dir(client, path):
    out, _ = client.exec_command(sudo=True, cmd=f"ls {path}")
    return out.strip().split() if out.strip() else []


def _snapshot_file_state(client, path):
    """Return (file_type, size_bytes, sha256) for path — matches upstream."""
    stat_out, _ = client.exec_command(sudo=True, cmd=f"stat -c '%F:%s' {path}")
    file_type, size = stat_out.strip().rsplit(":", 1)
    sha_out, _ = client.exec_command(sudo=True, cmd=f"sha256sum {path}")
    digest = sha_out.strip().split()[0]
    return file_type, int(size), digest


def _list_snaps(client, dir_path):
    """Return snapshot names under ``dir_path/.snap`` (empty if none)."""
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"ls -1 {dir_path}/.snap 2>/dev/null || true",
        check_ec=False,
    )
    return [name for name in (out or "").splitlines() if name.strip()]


def _remove_all_snaps(client, dir_path):
    """``rmdir`` every CephFS snapshot under ``dir_path/.snap``."""
    for snap in _list_snaps(client, dir_path):
        client.exec_command(
            sudo=True,
            cmd=f"rmdir {dir_path}/.snap/{snap}",
            check_ec=False,
        )


def _cleanup_scenario_dirs(
    client,
    mount_base,
    dir_names,
    fs_mirroring_utils=None,
    source_fs=None,
    remove_from_mirror=False,
):
    """Clear leftover scenario dirs so re-runs do not hit 'snap File exists'.

    Order: demirror path (optional) → remove all snaps → ``rm -rf`` the dir.
    """
    if not client or not mount_base:
        return
    base = mount_base.rstrip("/")
    for dir_name in dir_names:
        dir_path = f"{base}/{dir_name}"
        mirror_path = f"/{dir_name}"
        if (
            remove_from_mirror
            and fs_mirroring_utils is not None
            and source_fs is not None
        ):
            try:
                fs_mirroring_utils.remove_path_from_mirroring(
                    client, source_fs, mirror_path
                )
                log.info("Removed leftover mirror path %s", mirror_path)
            except Exception as exc:
                log.debug("remove_path_from_mirroring %s skipped: %s", mirror_path, exc)
        try:
            _remove_all_snaps(client, dir_path)
            # Second pass in case a snap was created while demirroring
            _remove_all_snaps(client, dir_path)
            client.exec_command(sudo=True, cmd=f"rm -rf {dir_path}", check_ec=False)
            log.info("Cleaned scenario dir %s", dir_path)
        except Exception as exc:
            log.warning("Cleanup of %s failed: %s", dir_path, exc)


def _safe_umount_and_rm(client, mnt):
    """Lazy-umount then remove the mountpoint only if it is no longer mounted."""
    if not client or not mnt:
        return
    client.exec_command(sudo=True, cmd=f"umount -l {mnt}", check_ec=False)
    # Avoid ``rm -rf`` on a still-mounted path (would delete CephFS contents).
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"mountpoint -q {mnt}; echo $?",
        check_ec=False,
    )
    still_mounted = (out or "").strip().splitlines()
    rc = still_mounted[-1] if still_mounted else "1"
    if rc == "0":
        log.warning("Skipping rm -rf %s — still a mountpoint after umount -l", mnt)
        return
    client.exec_command(sudo=True, cmd=f"rm -rf {mnt}", check_ec=False)


def _assert_no_new_crashes(fs_util, client, context):
    """Fail if ``ceph crash ls-new`` is non-empty (call after ``crash archive-all``)."""
    crashes = fs_util.get_crash_ls_new(client)
    if crashes:
        log.error("New crashes after %s: %s", context, json.dumps(crashes, default=str))
        raise CommandFailed(f"Unexpected ceph crash(es) during {context}: {crashes}")
    log.info("No new ceph crashes (%s)", context)


def _wait_for_object_clone_sizes(
    client,
    data_pool,
    oid,
    expected_sizes,
    deadline=None,
    interval=_CLONE_WAIT_INTERVAL,
):
    """Wait until rados listsnaps clone sizes match expected_sizes.

    ``deadline`` is a ``time.monotonic()`` absolute; when omitted a fresh
    ``_CLONE_WAIT_TIMEOUT`` window is used.
    """
    if deadline is None:
        deadline = time.monotonic() + _CLONE_WAIT_TIMEOUT
    last_sizes = None
    while time.monotonic() < deadline:
        out, err = client.exec_command(
            sudo=True,
            cmd=f"rados -p {data_pool} listsnaps {oid} --format=json",
            check_ec=False,
        )
        try:
            clones = json.loads(out).get("clones", [])
            last_sizes = [clone.get("size") for clone in clones]
            log.info(
                "listsnaps %s: sizes=%s (want %s)", oid, last_sizes, expected_sizes
            )
            if last_sizes == expected_sizes:
                return
        except (json.JSONDecodeError, TypeError, AttributeError) as exc:
            log.warning(
                "listsnaps parse for %s failed: %s (err=%s out=%r)",
                oid,
                exc,
                err,
                out,
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(interval, remaining))
    raise CommandFailed(
        f"listsnaps {oid}: expected sizes {expected_sizes}, last seen {last_sizes}"
    )


def _run_pure_truncate(
    fs_mirroring_utils, src, tgt, src_base, tgt_base, source_fs, target_fs, dirs
):
    """Scenario 1 — upstream test_cephfs_mirror_blockdiff_pure_truncate."""
    pure_dir = dirs["pure"]
    mirror_path = f"/{pure_dir}"
    log.info("=" * 60)
    log.info("SCENARIO 1: blockdiff pure truncate")
    log.info("=" * 60)

    src.exec_command(sudo=True, cmd=f"mkdir -p {src_base}/{pure_dir}")
    fs_mirroring_utils.add_path_for_mirroring(src, source_fs, mirror_path)

    for file_name, initial_mb, _ in _CASES:
        src.exec_command(
            sudo=True,
            cmd=(
                f"dd if=/dev/zero of={src_base}/{pure_dir}/{file_name} "
                f"bs=1M count={initial_mb} conv=fsync"
            ),
        )

    log.info("snap_a baseline full sync")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=pure_dir,
        snap_name="snap_a",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    log.info("Size-only truncate / sparse grow")
    for file_name, initial_mb, final_mb in _CASES:
        src.exec_command(
            sudo=True,
            cmd=f"truncate -s {final_mb}M {src_base}/{pure_dir}/{file_name}",
        )
        if final_mb > initial_mb:
            src.exec_command(
                sudo=True,
                cmd=f"touch -m {src_base}/{pure_dir}/{file_name}",
            )

    log.info("snap_b size-only incremental sync")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=pure_dir,
        snap_name="snap_b",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    if not _path_exists(tgt, f"{tgt_base}/{pure_dir}/.snap/snap_b"):
        raise CommandFailed("snap_b missing on secondary (pure truncate)")

    mismatches = []
    for file_name, _, final_mb in _CASES:
        src_path = f"{src_base}/{pure_dir}/.snap/snap_b/{file_name}"
        tgt_path = f"{tgt_base}/{pure_dir}/.snap/snap_b/{file_name}"
        source_state = _snapshot_file_state(src, src_path)
        dest_state = _snapshot_file_state(tgt, tgt_path)
        expected_size = final_mb * 1024 * 1024
        log.info(
            "pure truncate case %s: source=%s destination=%s expected_size=%s",
            file_name,
            source_state,
            dest_state,
            expected_size,
        )
        if source_state != dest_state:
            mismatches.append((file_name, source_state, dest_state))
        if dest_state[1] != expected_size:
            mismatches.append(
                (
                    file_name,
                    f"expected_size={expected_size}",
                    f"dest_size={dest_state[1]}",
                )
            )
    if mismatches:
        raise CommandFailed(f"pure truncate snap_b mismatches: {mismatches}")

    log.info("SCENARIO 1 PASSED: blockdiff pure truncate")
    return mirror_path


def _run_empty_clones(
    fs_mirroring_utils, fs_util_ceph1, src, tgt, src_base, source_fs, target_fs, dirs
):
    """Scenario 2 — upstream test_cephfs_mirror_blockdiff_empty_clones."""
    empty_dir = dirs["empty"]
    mirror_path = f"/{empty_dir}"
    file_path = f"{src_base}/{empty_dir}/f"

    log.info("=" * 60)
    log.info("SCENARIO 2: blockdiff empty clones")
    log.info("=" * 60)

    fs_info = fs_util_ceph1.get_fs_info(src, source_fs)
    data_pool = fs_info.get("data_pool_name")
    if not data_pool:
        raise CommandFailed(f"Could not resolve data pool for {source_fs}")
    log.info("Data pool for listsnaps: %s", data_pool)

    src.exec_command(sudo=True, cmd=f"mkdir -p {src_base}/{empty_dir}")
    fs_mirroring_utils.add_path_for_mirroring(src, source_fs, mirror_path)

    src.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/urandom of={file_path} bs=1M count=32 conv=fsync",
    )
    ino_out, _ = src.exec_command(sudo=True, cmd=f"stat -c %i {file_path}")
    ino = int(ino_out.strip())
    oids = [f"{ino:x}.{objno:08x}" for objno in range(_NUM_OBJECTS)]
    log.info("File inode=%s oids=%s", ino, oids)

    def regrow_file(mtime):
        # Write last object only; leave others empty. Pin mtime for snapdiff.
        src.exec_command(
            sudo=True,
            cmd=(
                f"dd if=/dev/urandom of={file_path} bs=1M seek=31 count=1 "
                f"conv=notrunc,fsync"
            ),
        )
        src.exec_command(sudo=True, cmd=f"touch -m -d '{mtime}' {file_path}")

    # No snapshots yet: truncate leaves empty objects without clones
    src.exec_command(sudo=True, cmd=f"truncate -s 0 {file_path}")
    empty_deadline = time.monotonic() + _CLONE_WAIT_TIMEOUT
    for oid in oids:
        _wait_for_object_clone_sizes(src, data_pool, oid, [0], deadline=empty_deadline)

    regrow_file(_MTIME_SNAP_A)
    log.info("empty_clones snap_a")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=empty_dir,
        snap_name="snap_a",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    # Truncating under snap_a clones the empty objects: [clone 0, head 0]
    src.exec_command(sudo=True, cmd=f"truncate -s 0 {file_path}")
    clone_deadline = time.monotonic() + _CLONE_WAIT_TIMEOUT
    for oid in oids[:-1]:
        _wait_for_object_clone_sizes(
            src, data_pool, oid, [0, 0], deadline=clone_deadline
        )
    _wait_for_object_clone_sizes(
        src, data_pool, oids[-1], [_OBJECT_SIZE, 0], deadline=clone_deadline
    )

    # Direct crash check (not MDS gid proxy): archive then assert no new crashes.
    log.info("Archiving pre-existing crash records before empty-clones snap_b")
    src.exec_command(sudo=True, cmd="ceph crash archive-all", check_ec=False)

    regrow_file(_MTIME_SNAP_B)
    log.info("empty_clones snap_b (incremental — must not crash MDS)")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=empty_dir,
        snap_name="snap_b",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    _assert_no_new_crashes(fs_util_ceph1, src, "empty-clones snap_b incremental sync")
    log.info("SCENARIO 2 PASSED: blockdiff empty clones")
    return mirror_path


def _run_sparse_file_sync(
    fs_mirroring_utils, src, tgt, src_base, source_fs, target_fs, dirs
):
    """Scenario 3 — upstream test_cephfs_mirror_sparse_file_sync."""
    sparse_dir = dirs["sparse"]
    sparse_file = dirs["sparse_file"]
    mirror_path = f"/{sparse_dir}"
    file_path = f"{src_base}/{sparse_dir}/{sparse_file}"

    log.info("=" * 60)
    log.info("SCENARIO 3: sparse file sync (hole objects)")
    log.info("=" * 60)

    src.exec_command(sudo=True, cmd=f"mkdir -p {src_base}/{sparse_dir}")
    # 64MB sparse: 1MB data at 0, 32MB, 60MB; other 4MB objects are holes
    src.exec_command(sudo=True, cmd=f"truncate -s 64M {file_path}")
    src.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/zero of={file_path} bs=1M count=1 conv=notrunc",
    )
    src.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/zero of={file_path} bs=1M seek=32 count=1 conv=notrunc",
    )
    src.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/zero of={file_path} bs=1M seek=60 count=1 conv=notrunc",
    )
    # TODO(BZ-tracker#74984): pin mtime so snapdiff reports the file
    src.exec_command(
        sudo=True,
        cmd=f"touch -m -d '{_MTIME_SNAP_A}' {file_path}",
    )

    fs_mirroring_utils.add_path_for_mirroring(src, source_fs, mirror_path)

    log.info("sparse_sync snap_a full sync")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=sparse_dir,
        snap_name="snap_a",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    log.info("Rewrite 1 MiB at offset 32 MiB (incremental blockdiff path)")
    src.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/urandom of={file_path} bs=1M seek=32 count=1 conv=notrunc",
    )
    src.exec_command(
        sudo=True,
        cmd=f"touch -m -d '{_MTIME_SNAP_B}' {file_path}",
    )

    log.info("sparse_sync snap_b incremental sync over sparse holes")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=sparse_dir,
        snap_name="snap_b",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    log.info("SCENARIO 3 PASSED: sparse file sync")
    return mirror_path


def _run_directory_rename(
    fs_mirroring_utils, src, tgt, src_base, tgt_base, source_fs, target_fs, dirs
):
    """Scenario 4 — upstream incremental sync with directory rename."""
    rename_dir = dirs["rename"]
    mirror_path = f"/{rename_dir}"
    old_dir = f"{rename_dir}/dirB"
    new_dir = f"{rename_dir}/dirB-renamed"
    renamed_file = "sub/deeper/unchanged"
    renamed_contents = "unchanged across directory rename"
    cross_old = f"{rename_dir}/d1/X"
    cross_new = f"{rename_dir}/d2/Y"
    cross_file = "sub/deeper/unchanged"
    cross_contents = "unchanged across parent directory rename"
    replacement_src = f"{rename_dir}/replace/A"
    replacement_dst = f"{rename_dir}/replace/B"
    replacement_file = "sub/deeper/unchanged"
    replacement_contents = "unchanged across same-name directory replacement"

    log.info("=" * 60)
    log.info("SCENARIO 4: incremental sync with directory rename")
    log.info("=" * 60)

    src.exec_command(
        sudo=True,
        cmd=(
            f"mkdir -p {src_base}/{old_dir}/sub/deeper "
            f"{src_base}/{cross_old}/sub/deeper "
            f"{src_base}/{rename_dir}/d2 "
            f"{src_base}/{replacement_src}/sub/deeper "
            f"{src_base}/{replacement_dst}"
        ),
    )
    _write_file(src, f"{src_base}/{old_dir}/{renamed_file}", renamed_contents)
    _write_file(src, f"{src_base}/{cross_old}/{cross_file}", cross_contents)
    _write_file(
        src,
        f"{src_base}/{replacement_src}/{replacement_file}",
        replacement_contents,
    )
    _write_file(src, f"{src_base}/{replacement_dst}/old-only", "stale data")

    fs_mirroring_utils.add_path_for_mirroring(src, source_fs, mirror_path)

    log.info("directory_rename snap0 baseline")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=rename_dir,
        snap_name="snap0",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    log.info("directory_rename snap1 same-parent rename dirB → dirB-renamed")
    src.exec_command(
        sudo=True,
        cmd=f"mv {src_base}/{old_dir} {src_base}/{new_dir}",
    )
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=rename_dir,
        snap_name="snap1",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    remote_snap1 = f"{tgt_base}/{rename_dir}/.snap/snap1"
    entries = _list_dir(tgt, remote_snap1)
    if "dirB" in entries:
        raise CommandFailed(
            f"Old name dirB still present on secondary snap1: {entries}"
        )
    if _path_exists(tgt, f"{remote_snap1}/dirB"):
        raise CommandFailed("Old name dirB still exists on secondary snap1")
    content = _read_file(tgt, f"{remote_snap1}/dirB-renamed/{renamed_file}").rstrip(
        "\n"
    )
    if content != renamed_contents:
        raise CommandFailed(
            f"Renamed nested content mismatch: expected={renamed_contents!r} "
            f"got={content!r}"
        )
    log.info("PASS: same-parent rename nested content preserved")

    log.info("directory_rename snap2 cross-parent rename d1/X → d2/Y")
    src.exec_command(
        sudo=True,
        cmd=f"mv {src_base}/{cross_old} {src_base}/{cross_new}",
    )
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=rename_dir,
        snap_name="snap2",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    content = _read_file(
        tgt, f"{tgt_base}/{rename_dir}/.snap/snap2/d2/Y/{cross_file}"
    ).rstrip("\n")
    if content != cross_contents:
        raise CommandFailed(
            f"Cross-rename nested content mismatch: expected={cross_contents!r} "
            f"got={content!r}"
        )
    log.info("PASS: cross-parent rename nested content preserved")

    log.info("directory_rename snap3 same-name replace rm B; mv A B")
    src.exec_command(sudo=True, cmd=f"rm -rf {src_base}/{replacement_dst}")
    src.exec_command(
        sudo=True,
        cmd=f"mv {src_base}/{replacement_src} {src_base}/{replacement_dst}",
    )
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=rename_dir,
        snap_name="snap3",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    remote_repl = f"{tgt_base}/{rename_dir}/.snap/snap3/replace/B"
    repl_entries = _list_dir(tgt, remote_repl)
    if "old-only" in repl_entries:
        raise CommandFailed(
            f"Stale old-only still present after replace: {repl_entries}"
        )
    content = _read_file(tgt, f"{remote_repl}/{replacement_file}").rstrip("\n")
    if content != replacement_contents:
        raise CommandFailed(
            f"Replace nested content mismatch: expected={replacement_contents!r} "
            f"got={content!r}"
        )
    log.info("PASS: same-name replace — new content present, old-only gone")
    log.info("SCENARIO 4 PASSED: directory rename incremental sync")
    return mirror_path


def run(ceph_cluster, **kw):
    """
    validate_snapdiff_blockdiff_scenarios (CEPH-83633198)

    Validate pure-truncate, empty-clones, sparse-file, and directory-rename sync.

    Returns:
        0 on success, 1 on failure, -1 if skipped (RHCS < 10.0).
    """
    log.info("POLARION  : CEPH-83633198")
    log.info("TESTCASE : validate_snapdiff_blockdiff_scenarios")
    config = kw.get("config") or {}
    if CephfsMirroringUtils.skip_if_rhcs_below(config, "10.0"):
        log.info(
            "Skipping test: requires Ceph version >= 10.0 (rhbuild=%s)",
            config.get("rhbuild"),
        )
        return -1

    source_clients = None
    target_clients = None
    source_fs = None
    target_fs = None
    fuse_src = None
    fuse_tgt = None
    fs_mirroring_utils = None
    peer_uuid = None
    target_user = "mirror_remote"
    target_site_name = "remote_site"
    mirroring_paths = []
    dirs = None
    md5_clients = []

    try:
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        test_data = kw.get("test_data")
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )

        fs_util_ceph1 = FsUtils(ceph_cluster_dict.get("ceph1"), test_data=test_data)
        fs_util_ceph2 = FsUtils(ceph_cluster_dict.get("ceph2"), test_data=test_data)
        fs_mirroring_utils = CephfsMirroringUtils(
            ceph_cluster_dict.get("ceph1"), ceph_cluster_dict.get("ceph2")
        )

        build = config.get("build", config.get("rhbuild"))
        source_clients = ceph_cluster_dict.get("ceph1").get_ceph_objects("client")
        target_clients = ceph_cluster_dict.get("ceph2").get_ceph_objects("client")

        if not source_clients or not target_clients:
            log.error(
                "This test requires at least 1 client node on both ceph1 and ceph2."
            )
            return 1

        fs_util_ceph1.prepare_clients(source_clients, build)
        fs_util_ceph2.prepare_clients(target_clients, build)
        fs_util_ceph1.auth_list(source_clients)
        fs_util_ceph2.auth_list(target_clients)

        source_fs = "cephfs" if not erasure else "cephfs-ec"
        target_fs = "cephfs" if not erasure else "cephfs-ec"
        for fs_util, client, fs_name in [
            (fs_util_ceph1, source_clients[0], source_fs),
            (fs_util_ceph2, target_clients[0], target_fs),
        ]:
            if not fs_util.get_fs_info(client, fs_name):
                fs_util.create_fs(client, fs_name)
            fs_util.wait_for_mds_process(client, fs_name)

        log.info("Deploy CephFS mirroring")
        token = fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site_name,
        )
        if not token:
            raise CommandFailed(
                "CephFS mirroring deploy failed (no bootstrap token returned)"
            )
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )

        # Soft check: default is 16 MiB; cluster may override — log, don't hard-fail.
        out, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=(
                "ceph config get client.cephfs-mirror "
                "cephfs_mirror_blockdiff_min_file_size"
            ),
        )
        actual_min = int(out.strip())
        if actual_min != _BLOCKDIFF_MIN_SIZE:
            log.warning(
                "cephfs_mirror_blockdiff_min_file_size is %s (expected default %s); "
                "continuing with cluster value",
                actual_min,
                _BLOCKDIFF_MIN_SIZE,
            )
        else:
            log.info(
                "Verified cephfs_mirror_blockdiff_min_file_size=%s (16 MiB)",
                actual_min,
            )

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )
        dir_suffix = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(6)
        )
        dirs = _scenario_dirs(dir_suffix)
        scenario_dir_names = [
            dirs["pure"],
            dirs["empty"],
            dirs["sparse"],
            dirs["rename"],
        ]
        fuse_src = f"/mnt/cephfs_fuse{mounting_dir}_primary/"
        fuse_tgt = f"/mnt/cephfs_fuse{mounting_dir}_secondary/"

        log.info("FUSE-mount primary and secondary")
        source_clients[0].exec_command(sudo=True, cmd=f"mkdir -p {fuse_src}")
        target_clients[0].exec_command(sudo=True, cmd=f"mkdir -p {fuse_tgt}")
        fs_util_ceph1.fuse_mount(
            [source_clients[0]],
            fuse_src,
            extra_params=f" --client_fs {source_fs}",
        )
        fs_util_ceph2.fuse_mount(
            [target_clients[0]],
            fuse_tgt,
            extra_params=f" --client_fs {target_fs}",
        )

        src = source_clients[0]
        tgt = target_clients[0]
        src_base = fuse_src.rstrip("/")
        tgt_base = fuse_tgt.rstrip("/")

        for client in (src, tgt):
            client.upload_file(
                sudo=True,
                src=_MD5SUM_SRC,
                dst=_MD5SUM_DST,
            )
            md5_clients.append(client)

        # Run all scenarios; collect per-scenario failures instead of aborting early.
        scenario_failures = []
        scenarios = [
            (
                "pure_truncate",
                lambda: _run_pure_truncate(
                    fs_mirroring_utils,
                    src,
                    tgt,
                    src_base,
                    tgt_base,
                    source_fs,
                    target_fs,
                    dirs,
                ),
            ),
            (
                "empty_clones",
                lambda: _run_empty_clones(
                    fs_mirroring_utils,
                    fs_util_ceph1,
                    src,
                    tgt,
                    src_base,
                    source_fs,
                    target_fs,
                    dirs,
                ),
            ),
            (
                "sparse_file_sync",
                lambda: _run_sparse_file_sync(
                    fs_mirroring_utils,
                    src,
                    tgt,
                    src_base,
                    source_fs,
                    target_fs,
                    dirs,
                ),
            ),
            (
                "directory_rename",
                lambda: _run_directory_rename(
                    fs_mirroring_utils,
                    src,
                    tgt,
                    src_base,
                    tgt_base,
                    source_fs,
                    target_fs,
                    dirs,
                ),
            ),
        ]
        for name, runner in scenarios:
            try:
                mirroring_paths.append(runner())
            except Exception as exc:
                log.error("Scenario %s FAILED: %s", name, exc)
                log.error(traceback.format_exc())
                scenario_failures.append((name, str(exc)))

        if scenario_failures:
            summary = "; ".join(f"{n}: {e}" for n, e in scenario_failures)
            raise CommandFailed(f"Scenario failures: {summary}")

        log.info("=" * 60)
        log.info(
            "ALL SNAPDIFF/BLOCKDIFF SCENARIOS PASSED "
            "(truncate + empty-clones + sparse + directory-rename)"
        )
        log.info("=" * 60)
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        for client in md5_clients:
            try:
                client.exec_command(
                    sudo=True, cmd=f"rm -f {_MD5SUM_DST}", check_ec=False
                )
            except Exception as exc:
                log.debug("md5sum_script cleanup skipped: %s", exc)

        if config.get("cleanup", True):
            log.info("Cleanup")
            scenario_dir_names = (
                [dirs["pure"], dirs["empty"], dirs["sparse"], dirs["rename"]]
                if dirs
                else []
            )
            try:
                if source_clients and fuse_src and scenario_dir_names:
                    _cleanup_scenario_dirs(
                        source_clients[0],
                        fuse_src,
                        scenario_dir_names,
                        fs_mirroring_utils=fs_mirroring_utils,
                        source_fs=source_fs,
                        remove_from_mirror=True,
                    )
            except Exception as exc:
                log.warning("Primary scenario-dir cleanup failed: %s", exc)

            try:
                if target_clients and fuse_tgt and scenario_dir_names:
                    _cleanup_scenario_dirs(
                        target_clients[0],
                        fuse_tgt,
                        scenario_dir_names,
                        remove_from_mirror=False,
                    )
            except Exception as exc:
                log.warning("Secondary scenario-dir cleanup failed: %s", exc)

            try:
                if fs_mirroring_utils and source_clients and source_fs:
                    paths = list(mirroring_paths) or [
                        f"/{name}" for name in scenario_dir_names
                    ]
                    for mpath in paths:
                        try:
                            fs_mirroring_utils.remove_path_from_mirroring(
                                source_clients[0], source_fs, mpath
                            )
                        except Exception as exc:
                            log.debug(
                                "Mirror path removal for %s skipped: %s", mpath, exc
                            )
            except Exception as exc:
                log.warning("Mirror path cleanup failed: %s", exc)

            # Use cached peer_uuid only — get_peer_uuid_by_name retries ~4 minutes.
            try:
                if (
                    fs_mirroring_utils
                    and source_clients
                    and target_clients
                    and source_fs
                    and target_fs
                    and peer_uuid
                ):
                    fs_mirroring_utils.destroy_cephfs_mirroring(
                        source_fs,
                        source_clients[0],
                        target_fs,
                        target_clients[0],
                        target_user,
                        peer_uuid,
                    )
            except Exception as exc:
                log.warning("Destroy mirroring failed: %s", exc)

            for client, mnt in [
                (source_clients[0] if source_clients else None, fuse_src),
                (target_clients[0] if target_clients else None, fuse_tgt),
            ]:
                try:
                    _safe_umount_and_rm(client, mnt)
                except Exception as exc:
                    log.warning("Unmount/cleanup %s failed: %s", mnt, exc)
