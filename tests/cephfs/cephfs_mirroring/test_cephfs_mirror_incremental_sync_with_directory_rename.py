"""
test_cephfs_mirror_incremental_sync_with_directory_rename.py

Regression for PR #70757 / cephfs_mirror: fix incremental sync after directory
rename. Unchanged descendants of a renamed (or same-name replaced) directory
must survive snapdiff sync on the secondary.

Ported from upstream:
  qa/tasks/cephfs/test_mirroring.py
  ::TestMirroring.test_cephfs_mirror_incremental_sync_with_directory_rename

Scenarios (one mirrored dir ``directory_rename``, snaps snap0–snap3):
  1. Same-parent rename  dirB → dirB-renamed  (nested unchanged file present)
  2. Cross-parent rename d1/X → d2/Y
  3. Same-name replace   rm replace/B; mv replace/A replace/B  (no old-only)

Requires RHCS >= 10.0 (fix landed for 10.0).
Uses ceph-fuse mounts (same as upstream fs/mirror).
"""

import random
import string
import traceback

from looseversion import LooseVersion

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

_DIR_NAME = "directory_rename"


def run(ceph_cluster, **kw):
    """
    Validate incremental sync keeps unchanged descendants after directory rename.

    Returns:
        0 on success, 1 on failure, -1 if skipped (RHCS < 10.0).
    """
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
    mirror_path = f"/{_DIR_NAME}"
    snap_names = []

    # Paths / content — match upstream test exactly
    old_dir = f"{_DIR_NAME}/dirB"
    new_dir = f"{_DIR_NAME}/dirB-renamed"
    renamed_file = "sub/deeper/unchanged"
    renamed_contents = "unchanged across directory rename"
    cross_old = f"{_DIR_NAME}/d1/X"
    cross_new = f"{_DIR_NAME}/d2/Y"
    cross_file = "sub/deeper/unchanged"
    cross_contents = "unchanged across parent directory rename"
    replacement_src = f"{_DIR_NAME}/replace/A"
    replacement_dst = f"{_DIR_NAME}/replace/B"
    replacement_file = "sub/deeper/unchanged"
    replacement_contents = "unchanged across same-name directory replacement"

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
        # rhbuild is typically ``10.0-rhel-9``; compare numeric prefix only
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

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )
        fuse_src = f"/mnt/cephfs_fuse{mounting_dir}_primary/"
        fuse_tgt = f"/mnt/cephfs_fuse{mounting_dir}_secondary/"

        log.info("FUSE-mount primary and secondary (upstream fs/mirror uses fuse)")
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

        # ── Build tree (exact upstream layout) ────────────────────────────
        log.info("Creating directory_rename tree on primary")
        src.exec_command(
            sudo=True,
            cmd=(
                f"mkdir -p {src_base}/{old_dir}/sub/deeper "
                f"{src_base}/{cross_old}/sub/deeper "
                f"{src_base}/{_DIR_NAME}/d2 "
                f"{src_base}/{replacement_src}/sub/deeper "
                f"{src_base}/{replacement_dst}"
            ),
        )
        fs_mirroring_utils.write_file_content(
            src, f"{src_base}/{old_dir}/{renamed_file}", renamed_contents
        )
        fs_mirroring_utils.write_file_content(
            src, f"{src_base}/{cross_old}/{cross_file}", cross_contents
        )
        fs_mirroring_utils.write_file_content(
            src,
            f"{src_base}/{replacement_src}/{replacement_file}",
            replacement_contents,
        )
        fs_mirroring_utils.write_file_content(
            src, f"{src_base}/{replacement_dst}/old-only", "stale data"
        )

        log.info("Add mirror path %s", mirror_path)
        fs_mirroring_utils.add_path_for_mirroring(src, source_fs, mirror_path)

        # Existing checksum helper used across cephfs-mirror tests
        md5sum_script = "md5sum_script.sh"
        for client in (src, tgt):
            client.upload_file(
                sudo=True,
                src="tests/cephfs/cephfs_mirror_upgrade/md5sum_script.sh",
                dst=f"/root/{md5sum_script}",
            )

        # ── snap0: baseline ───────────────────────────────────────────────
        log.info("=" * 60)
        log.info("STEP: snap0 baseline")
        log.info("=" * 60)
        snap_names.append("snap0")
        fs_mirroring_utils.sync_and_verify_snapshot(
            source_client=src,
            target_client=tgt,
            source_mount=src_base,
            dir_name=_DIR_NAME,
            snap_name="snap0",
            source_fs=source_fs,
            target_fs=target_fs,
            mirror_path=mirror_path,
        )

        # ── snap1: same-parent rename ─────────────────────────────────────
        log.info("=" * 60)
        log.info("STEP: snap1 same-parent rename dirB → dirB-renamed")
        log.info("=" * 60)
        src.exec_command(
            sudo=True,
            cmd=f"mv {src_base}/{old_dir} {src_base}/{new_dir}",
        )
        snap_names.append("snap1")
        fs_mirroring_utils.sync_and_verify_snapshot(
            source_client=src,
            target_client=tgt,
            source_mount=src_base,
            dir_name=_DIR_NAME,
            snap_name="snap1",
            source_fs=source_fs,
            target_fs=target_fs,
            mirror_path=mirror_path,
        )

        remote_snap1 = f"{tgt_base}/{_DIR_NAME}/.snap/snap1"
        entries = fs_mirroring_utils.list_dir_entries(tgt, remote_snap1)
        if "dirB" in entries:
            raise CommandFailed(
                f"Old name dirB still present on secondary snap1: {entries}"
            )
        if fs_mirroring_utils.path_exists(tgt, f"{remote_snap1}/dirB"):
            raise CommandFailed("Old name dirB still exists on secondary snap1")
        content = fs_mirroring_utils.read_file_content(
            tgt, f"{remote_snap1}/dirB-renamed/{renamed_file}"
        ).rstrip("\n")
        if content != renamed_contents:
            raise CommandFailed(
                f"Renamed nested content mismatch: expected={renamed_contents!r} "
                f"got={content!r}"
            )
        log.info("PASS: same-parent rename nested content preserved")

        # ── snap2: cross-parent rename ────────────────────────────────────
        log.info("=" * 60)
        log.info("STEP: snap2 cross-parent rename d1/X → d2/Y")
        log.info("=" * 60)
        src.exec_command(
            sudo=True,
            cmd=f"mv {src_base}/{cross_old} {src_base}/{cross_new}",
        )
        snap_names.append("snap2")
        fs_mirroring_utils.sync_and_verify_snapshot(
            source_client=src,
            target_client=tgt,
            source_mount=src_base,
            dir_name=_DIR_NAME,
            snap_name="snap2",
            source_fs=source_fs,
            target_fs=target_fs,
            mirror_path=mirror_path,
        )

        content = fs_mirroring_utils.read_file_content(
            tgt, f"{tgt_base}/{_DIR_NAME}/.snap/snap2/d2/Y/{cross_file}"
        ).rstrip("\n")
        if content != cross_contents:
            raise CommandFailed(
                f"Cross-rename nested content mismatch: expected={cross_contents!r} "
                f"got={content!r}"
            )
        log.info("PASS: cross-parent rename nested content preserved")

        # ── snap3: same-name replace ──────────────────────────────────────
        log.info("=" * 60)
        log.info("STEP: snap3 same-name replace rm B; mv A B")
        log.info("=" * 60)
        src.exec_command(sudo=True, cmd=f"rm -rf {src_base}/{replacement_dst}")
        src.exec_command(
            sudo=True,
            cmd=f"mv {src_base}/{replacement_src} {src_base}/{replacement_dst}",
        )
        snap_names.append("snap3")
        fs_mirroring_utils.sync_and_verify_snapshot(
            source_client=src,
            target_client=tgt,
            source_mount=src_base,
            dir_name=_DIR_NAME,
            snap_name="snap3",
            source_fs=source_fs,
            target_fs=target_fs,
            mirror_path=mirror_path,
        )

        remote_repl = f"{tgt_base}/{_DIR_NAME}/.snap/snap3/replace/B"
        repl_entries = fs_mirroring_utils.list_dir_entries(tgt, remote_repl)
        if "old-only" in repl_entries:
            raise CommandFailed(
                f"Stale old-only still present after replace: {repl_entries}"
            )
        content = fs_mirroring_utils.read_file_content(
            tgt, f"{remote_repl}/{replacement_file}"
        ).rstrip("\n")
        if content != replacement_contents:
            raise CommandFailed(
                f"Replace nested content mismatch: expected={replacement_contents!r} "
                f"got={content!r}"
            )
        log.info("PASS: same-name replace — new content present, old-only gone")

        log.info("=" * 60)
        log.info("ALL DIRECTORY-RENAME INCREMENTAL SYNC CHECKS PASSED")
        log.info("=" * 60)
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        if config.get("cleanup", True):
            log.info("Cleanup")
            try:
                if source_clients and snap_names and fuse_src:
                    for snap in snap_names:
                        source_clients[0].exec_command(
                            sudo=True,
                            cmd=(
                                f"rmdir {fuse_src.rstrip('/')}/"
                                f"{_DIR_NAME}/.snap/{snap}"
                            ),
                            check_ec=False,
                        )
            except Exception as exc:
                log.warning("Snapshot cleanup failed: %s", exc)

            try:
                if fs_mirroring_utils and source_clients and source_fs:
                    fs_mirroring_utils.remove_path_from_mirroring(
                        source_clients[0], source_fs, mirror_path
                    )
            except Exception as exc:
                log.warning("Mirror path removal failed: %s", exc)

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
