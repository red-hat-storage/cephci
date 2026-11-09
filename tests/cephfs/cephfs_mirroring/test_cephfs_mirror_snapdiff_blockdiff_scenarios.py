"""
test_cephfs_mirror_snapdiff_blockdiff_scenarios.py

CEPH-83633198 — validate_snapdiff_blockdiff_scenarios

Blockdiff incremental-sync regressions (RHCS >= 10.0).

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

Uses ceph-fuse mounts. No asok — waits via
``ceph fs snapshot mirror status`` and verifies with md5sum_script helper.
"""

import json
import random
import string
import time
import traceback

from looseversion import LooseVersion

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

_BLOCKDIFF_MIN_SIZE = 16777216  # 16 MiB default
_PURE_DIR = "d0"
_EMPTY_CLONES_DIR = "empty_clones"
_SPARSE_DIR = "sparse_sync"
_SPARSE_FILE = "sparse"
_CASES = [
    # (file_name, initial_size_mb, final_size_mb)
    ("object_boundary_shrink", 64, 32),
    ("partial_object_shrink", 64, 33),
    ("same_object_shrink", 19, 18),
    ("sparse_grow", 20, 40),
]
_OBJECT_SIZE = 4194304
_NUM_OBJECTS = 8  # 32 MiB with default 4 MiB layout
_SCENARIO_DIRS = (_PURE_DIR, _EMPTY_CLONES_DIR, _SPARSE_DIR)


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
    fs_mirroring_utils=None,
    source_fs=None,
    remove_from_mirror=False,
):
    """Clear leftover scenario dirs so re-runs do not hit 'snap File exists'.

    Order: demirror path (optional) → remove all snaps → ``rm -rf`` the dir.
    Leftover ``/d0`` (and siblings) persist across remounts of the same FS.
    """
    if not client or not mount_base:
        return
    base = mount_base.rstrip("/")
    for dir_name in _SCENARIO_DIRS:
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


def _get_mds_rank0_gid(client, fs_name):
    """Return active rank-0 MDS gid from ``ceph fs status``."""
    out, _ = client.exec_command(sudo=True, cmd=f"ceph fs status {fs_name} -f json")
    data = json.loads(out)
    for entry in data.get("mdsmap", []):
        if entry.get("rank") == 0 and entry.get("state") == "active":
            return entry.get("gid")
    # Fallback: first active entry
    for entry in data.get("mdsmap", []):
        if entry.get("state") == "active":
            return entry.get("gid")
    raise CommandFailed(f"No active MDS found for {fs_name}")


def _wait_for_object_clone_sizes(
    client, data_pool, oid, expected_sizes, timeout=120, interval=2
):
    """Wait until rados listsnaps clone sizes match expected_sizes."""
    elapsed = 0
    last_sizes = None
    while elapsed < timeout:
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
            log.debug("listsnaps parse for %s failed: %s (err=%s)", oid, exc, err)
        time.sleep(interval)
        elapsed += interval
    raise CommandFailed(
        f"listsnaps {oid}: expected sizes {expected_sizes}, last seen {last_sizes}"
    )


def _run_pure_truncate(
    fs_mirroring_utils, src, tgt, src_base, tgt_base, source_fs, target_fs, snap_names
):
    """Scenario 1 — upstream test_cephfs_mirror_blockdiff_pure_truncate."""
    mirror_path = f"/{_PURE_DIR}"
    log.info("=" * 60)
    log.info("SCENARIO 1: blockdiff pure truncate")
    log.info("=" * 60)

    src.exec_command(sudo=True, cmd=f"mkdir -p {src_base}/{_PURE_DIR}")
    fs_mirroring_utils.add_path_for_mirroring(src, source_fs, mirror_path)

    for file_name, initial_mb, _ in _CASES:
        src.exec_command(
            sudo=True,
            cmd=(
                f"dd if=/dev/zero of={src_base}/{_PURE_DIR}/{file_name} "
                f"bs=1M count={initial_mb} conv=fsync"
            ),
        )

    log.info("snap_a baseline full sync")
    snap_names.append("snap_a")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=_PURE_DIR,
        snap_name="snap_a",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    log.info("Size-only truncate / sparse grow")
    for file_name, initial_mb, final_mb in _CASES:
        src.exec_command(
            sudo=True,
            cmd=f"truncate -s {final_mb}M {src_base}/{_PURE_DIR}/{file_name}",
        )
        if final_mb > initial_mb:
            src.exec_command(
                sudo=True,
                cmd=f"touch -m {src_base}/{_PURE_DIR}/{file_name}",
            )

    log.info("snap_b size-only incremental sync")
    snap_names.append("snap_b")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=_PURE_DIR,
        snap_name="snap_b",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    if not fs_mirroring_utils.path_exists(tgt, f"{tgt_base}/{_PURE_DIR}/.snap/snap_b"):
        raise CommandFailed("snap_b missing on secondary (pure truncate)")

    mismatches = []
    for file_name, _, final_mb in _CASES:
        src_path = f"{src_base}/{_PURE_DIR}/.snap/snap_b/{file_name}"
        tgt_path = f"{tgt_base}/{_PURE_DIR}/.snap/snap_b/{file_name}"
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
    fs_mirroring_utils,
    fs_util_ceph1,
    src,
    tgt,
    src_base,
    source_fs,
    target_fs,
    snap_names,
):
    """Scenario 2 — upstream test_cephfs_mirror_blockdiff_empty_clones."""
    mirror_path = f"/{_EMPTY_CLONES_DIR}"
    file_rel = f"{_EMPTY_CLONES_DIR}/f"
    file_path = f"{src_base}/{file_rel}"

    log.info("=" * 60)
    log.info("SCENARIO 2: blockdiff empty clones")
    log.info("=" * 60)

    fs_info = fs_util_ceph1.get_fs_info(src, source_fs)
    data_pool = fs_info.get("data_pool_name")
    if not data_pool:
        raise CommandFailed(f"Could not resolve data pool for {source_fs}")
    log.info("Data pool for listsnaps: %s", data_pool)

    src.exec_command(sudo=True, cmd=f"mkdir -p {src_base}/{_EMPTY_CLONES_DIR}")
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
    for oid in oids:
        _wait_for_object_clone_sizes(src, data_pool, oid, [0])

    regrow_file("2020-01-01 00:00:00 UTC")
    log.info("empty_clones snap_a")
    snap_names.append("snap_a")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=_EMPTY_CLONES_DIR,
        snap_name="snap_a",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    # Truncating under snap_a clones the empty objects: [clone 0, head 0]
    src.exec_command(sudo=True, cmd=f"truncate -s 0 {file_path}")
    for oid in oids[:-1]:
        _wait_for_object_clone_sizes(src, data_pool, oid, [0, 0])
    _wait_for_object_clone_sizes(src, data_pool, oids[-1], [_OBJECT_SIZE, 0])

    active_gid = _get_mds_rank0_gid(src, source_fs)
    log.info("MDS rank-0 gid before snap_b sync: %s", active_gid)

    regrow_file("2020-01-01 00:00:02 UTC")
    log.info("empty_clones snap_b (incremental — must not crash MDS)")
    snap_names.append("snap_b")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=_EMPTY_CLONES_DIR,
        snap_name="snap_b",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    gid_after = _get_mds_rank0_gid(src, source_fs)
    if gid_after != active_gid:
        raise CommandFailed(
            f"MDS rank-0 gid changed during empty-clones sync "
            f"(before={active_gid}, after={gid_after}) — possible MDS crash/restart"
        )
    log.info("MDS rank-0 gid unchanged: %s", gid_after)
    log.info("SCENARIO 2 PASSED: blockdiff empty clones")
    return mirror_path


def _run_sparse_file_sync(
    fs_mirroring_utils, src, tgt, src_base, source_fs, target_fs, snap_names
):
    """Scenario 3 — upstream test_cephfs_mirror_sparse_file_sync."""
    mirror_path = f"/{_SPARSE_DIR}"
    file_path = f"{src_base}/{_SPARSE_DIR}/{_SPARSE_FILE}"

    log.info("=" * 60)
    log.info("SCENARIO 3: sparse file sync (hole objects)")
    log.info("=" * 60)

    src.exec_command(sudo=True, cmd=f"mkdir -p {src_base}/{_SPARSE_DIR}")
    # 64MB sparse: 1MB data at 0, 32MB, 60MB; other 4MB objects are holes
    src.exec_command(sudo=True, cmd=f"truncate -s 64M {file_path}")
    src.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/zero of={file_path} bs=1M count=1 conv=notrunc",
    )
    src.exec_command(
        sudo=True,
        cmd=(f"dd if=/dev/zero of={file_path} bs=1M seek=32 count=1 " f"conv=notrunc"),
    )
    src.exec_command(
        sudo=True,
        cmd=(f"dd if=/dev/zero of={file_path} bs=1M seek=60 count=1 " f"conv=notrunc"),
    )
    # Pin mtime so snapdiff reports the file (tracker #74984)
    src.exec_command(
        sudo=True,
        cmd=f"touch -m -d '2020-01-01 00:00:00 UTC' {file_path}",
    )

    fs_mirroring_utils.add_path_for_mirroring(src, source_fs, mirror_path)

    log.info("sparse_sync snap_a full sync")
    snap_names.append("snap_a")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=_SPARSE_DIR,
        snap_name="snap_a",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    log.info("Rewrite 1 MiB at offset 32 MiB (incremental blockdiff path)")
    src.exec_command(
        sudo=True,
        cmd=(
            f"dd if=/dev/urandom of={file_path} bs=1M seek=32 count=1 " f"conv=notrunc"
        ),
    )
    src.exec_command(
        sudo=True,
        cmd=f"touch -m -d '2020-01-01 00:00:02 UTC' {file_path}",
    )

    log.info("sparse_sync snap_b incremental sync over sparse holes")
    snap_names.append("snap_b")
    fs_mirroring_utils.sync_and_verify_snapshot(
        source_client=src,
        target_client=tgt,
        source_mount=src_base,
        dir_name=_SPARSE_DIR,
        snap_name="snap_b",
        source_fs=source_fs,
        target_fs=target_fs,
        mirror_path=mirror_path,
    )

    log.info("SCENARIO 3 PASSED: sparse file sync")
    return mirror_path


def run(ceph_cluster, **kw):
    """
    validate_snapdiff_blockdiff_scenarios (CEPH-83633198)

    Validate blockdiff pure-truncate, empty-clones, and sparse-file sync.

    Returns:
        0 on success, 1 on failure, -1 if skipped (RHCS < 10.0).
    """
    log.info("POLARION  : CEPH-83633198")
    log.info("TESTCASE : validate_snapdiff_blockdiff_scenarios")
    config = kw.get("config") or {}
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
        rhcs_ver = str(build).split("-")[0] if build else "0"
        if LooseVersion(rhcs_ver) < LooseVersion("10.0"):
            log.info("Skipping test: requires Ceph version >= 10.0 (build=%s)", build)
            return -1

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

        out, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=(
                "ceph config get client.cephfs-mirror "
                "cephfs_mirror_blockdiff_min_file_size"
            ),
        )
        actual_min = int(out.strip())
        if actual_min != _BLOCKDIFF_MIN_SIZE:
            raise CommandFailed(
                f"cephfs_mirror_blockdiff_min_file_size is {actual_min}, "
                f"expected {_BLOCKDIFF_MIN_SIZE} (16 MiB)"
            )
        log.info(
            "Verified cephfs_mirror_blockdiff_min_file_size=%s (16 MiB)",
            actual_min,
        )

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )
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

        md5sum_script = "md5sum_script.sh"
        for client in (src, tgt):
            client.upload_file(
                sudo=True,
                src="tests/cephfs/cephfs_mirror_upgrade/md5sum_script.sh",
                dst=f"/root/{md5sum_script}",
            )

        pure_snaps = []
        empty_snaps = []
        sparse_snaps = []

        path1 = _run_pure_truncate(
            fs_mirroring_utils,
            src,
            tgt,
            src_base,
            tgt_base,
            source_fs,
            target_fs,
            pure_snaps,
        )
        mirroring_paths.append(path1)

        path2 = _run_empty_clones(
            fs_mirroring_utils,
            fs_util_ceph1,
            src,
            tgt,
            src_base,
            source_fs,
            target_fs,
            empty_snaps,
        )
        mirroring_paths.append(path2)

        path3 = _run_sparse_file_sync(
            fs_mirroring_utils,
            src,
            tgt,
            src_base,
            source_fs,
            target_fs,
            sparse_snaps,
        )
        mirroring_paths.append(path3)

        log.info("=" * 60)
        log.info(
            "ALL BLOCKDIFF PURE-TRUNCATE + EMPTY-CLONES + SPARSE-FILE CHECKS PASSED"
        )
        log.info("=" * 60)
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        if config.get("cleanup", True):
            log.info("Cleanup")
            # Always clear known scenario dirs (not only tracked snaps) so a
            # failed mid-run still leaves the FS re-runnable.
            try:
                if source_clients and fuse_src:
                    _cleanup_scenario_dirs(
                        source_clients[0],
                        fuse_src,
                        fs_mirroring_utils=fs_mirroring_utils,
                        source_fs=source_fs,
                        remove_from_mirror=True,
                    )
            except Exception as exc:
                log.warning("Primary scenario-dir cleanup failed: %s", exc)

            try:
                if target_clients and fuse_tgt:
                    _cleanup_scenario_dirs(
                        target_clients[0], fuse_tgt, remove_from_mirror=False
                    )
            except Exception as exc:
                log.warning("Secondary scenario-dir cleanup failed: %s", exc)

            try:
                if fs_mirroring_utils and source_clients and source_fs:
                    paths = list(mirroring_paths) or [
                        f"/{name}" for name in _SCENARIO_DIRS
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

            try:
                if (
                    fs_mirroring_utils
                    and source_clients
                    and target_clients
                    and source_fs
                    and target_fs
                ):
                    live_peer = peer_uuid
                    try:
                        live_peer = fs_mirroring_utils.get_peer_uuid_by_name(
                            source_clients[0], source_fs
                        )
                    except Exception:
                        pass
                    if live_peer:
                        fs_mirroring_utils.destroy_cephfs_mirroring(
                            source_fs,
                            source_clients[0],
                            target_fs,
                            target_clients[0],
                            target_user,
                            live_peer,
                        )
            except Exception as exc:
                log.warning("Destroy mirroring failed: %s", exc)

            for client, mnt in [
                (source_clients[0] if source_clients else None, fuse_src),
                (target_clients[0] if target_clients else None, fuse_tgt),
            ]:
                if not client or not mnt:
                    continue
                try:
                    client.exec_command(
                        sudo=True, cmd=f"umount -l {mnt}", check_ec=False
                    )
                    client.exec_command(sudo=True, cmd=f"rm -rf {mnt}", check_ec=False)
                except Exception as exc:
                    log.warning("Unmount/cleanup %s failed: %s", mnt, exc)
