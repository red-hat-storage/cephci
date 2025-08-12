from ceph.ceph import CommandFailed
from tests.cephfs.lib.cephfs_refinode_utils import RefInodeUtils
from utility.log import Log

log = Log(__name__)

MOUNT_POINTS = {
    "Fuse": {
        "dirs": ["f_dir1", "f_dir2"],
        "files": {
            "f_dir1": [
                "f_file1",
                "f_file1_hl",
                "f_file_hl2",
                "f_file1_hl3",
                "f_file1_hl4",
                "f_file1_hl5",
            ],
            "f_dir2": [
                "f_file2",
                "f_file1_hl",
                "f_file_hl2",
                "f_file1_hl3",
                "f_file1_hl4",
                "f_file1_hl5",
            ],
        },
    },
    "Kernel": {
        "dirs": ["k_dir1", "k_dir2"],
        "files": {
            "k_dir1": [
                "k_file1",
                "k_file1_hl",
                "k_file_hl2",
                "k_file1_hl3",
                "k_file1_hl4",
                "k_file1_hl5",
            ],
            "k_dir2": [
                "k_file2",
                "k_file1_hl",
                "k_file_hl2",
                "k_file1_hl3",
                "k_file1_hl4",
                "k_file1_hl5",
            ],
        },
    },
    "NFS": {
        "dirs": ["n_dir1", "n_dir2"],
        "files": {
            "n_dir1": [
                "n_file1",
                "n_file1_hl",
                "n_file_hl2",
                "n_file1_hl3",
                "n_file1_hl4",
                "n_file1_hl5",
            ],
            "n_dir2": [
                "n_file2",
                "n_file1_hl",
                "n_file_hl2",
                "n_file1_hl3",
                "n_file1_hl4",
                "n_file1_hl5",
            ],
        },
    },
}


def get_file_path(mount_type, dir_idx, filename, mount_paths):
    base_path = mount_paths[mount_type][dir_idx]
    directory = MOUNT_POINTS[mount_type]["dirs"][dir_idx]
    return "%s/%s/%s" % (base_path, directory, filename)


def create_dirs_on_mounts(ref_inode_utils, client, base_paths_map, mount_points_map):
    log.info("Creating test directories on mount points...")
    for mount_type, base_paths in base_paths_map.items():
        for idx, dir_to_create in enumerate(
            mount_points_map[mount_type]["dirs"][: len(base_paths)]
        ):
            ref_inode_utils.create_directories(client, base_paths[idx], [dir_to_create])
            log.info(
                "Created directory '%s' under %s (%s)",
                dir_to_create,
                base_paths[idx],
                mount_type,
            )


def run_dd_on_existing_files(client, base_paths_map, mount_points_map, source_label):
    log.info("Running dd IO on existing files for %s", source_label)
    processed_files = []
    for mount_type, base_paths in base_paths_map.items():
        for idx, dirname in enumerate(
            mount_points_map[mount_type]["dirs"][: len(base_paths)]
        ):
            fname = mount_points_map[mount_type]["files"].get(dirname, [])[0]
            if not fname:
                continue
            file_path = "%s/%s/%s" % (base_paths[idx].rstrip("/"), dirname, fname)
            log.info("Running dd IO for existing file: %s", file_path)
            client.exec_command(
                sudo=True,
                cmd="dd if=/dev/zero of=%s bs=100M count=5 conv=notrunc" % file_path,
                timeout=600,
            )
            processed_files.append((mount_type, idx, fname, source_label))
    return processed_files


def create_hardlinks(ref_inode_utils, client, all_mount_paths, suffix):
    log.info("Creating hardlinks with suffix '%s' across all mount paths...", suffix)
    for source_type, paths_map in all_mount_paths.items():
        for mount_type, base_paths in paths_map.items():
            for dir_idx, base_path in enumerate(base_paths):
                orig_fname = "%s_file1" % mount_type[0].lower()
                hardlink_fname = "%s_file1_%s" % (mount_type[0].lower(), suffix)
                orig_path = get_file_path(
                    mount_type, dir_idx, orig_fname, {mount_type: [base_path]}
                )
                hl_path = get_file_path(
                    mount_type, dir_idx, hardlink_fname, {mount_type: [base_path]}
                )
                try:
                    ref_inode_utils.create_hardlink(client, orig_path, hl_path)
                    log.info(
                        "Created hardlink for %s (%s): %s -> %s",
                        mount_type,
                        source_type,
                        orig_path,
                        hl_path,
                    )
                except CommandFailed as e:
                    log.error(
                        "Failed to create hardlink for %s (%s): %s",
                        mount_type,
                        source_type,
                        e,
                    )
                    return False
    return True


def create_snapshots(
    fs_util,
    ref_inode_utils,
    client,
    default_fs,
    subvolume_names,
    subvol_group_name,
    direct_paths,
    snap_name,
):
    log.info("Creating snapshots '%s' for directories and subvolumes...", snap_name)
    for mount_type, base_paths in direct_paths.items():
        if mount_type == "NFS":
            continue
        for idx, dirname in enumerate(
            MOUNT_POINTS[mount_type]["dirs"][: len(base_paths)]
        ):
            full_dir_path = "%s/%s" % (base_paths[idx], dirname)
            ref_inode_utils.create_snapshot_on_dir(client, full_dir_path, snap_name)
    for subvol_name in subvolume_names:
        kwargs = dict(
            client=client,
            vol_name=default_fs,
            subvol_name=subvol_name,
            snap_name=snap_name,
            validate=True,
        )
        if subvol_name.startswith("subvolume_ref_inode_grp_"):
            kwargs["group_name"] = subvol_group_name
        fs_util.create_snapshot(**kwargs)


def run(ceph_cluster, **kwargs):
    try:
        test_data, config = kwargs.get("test_data"), kwargs.get("config")
        ref_inode_utils = RefInodeUtils(ceph_cluster)
        log.info("Preparing CephFS test environment...")
        fs_util, clients, erasure = ref_inode_utils.prepare_environment(
            ceph_cluster, test_data, config
        )
        if not clients:
            return 1
        client = clients[0]
        default_fs = "cephfs_ref-ec" if erasure else "cephfs_ref"

        if fs_util.get_fs_info(client, default_fs):
            log.info("Filesystem %s exists — removing it for clean run" % default_fs)
            fs_util.remove_fs(client, default_fs, validate=True)
        log.info("Creating filesystem %s" % default_fs)
        fs_util.create_fs(client, default_fs)

        nfs_server = ceph_cluster.get_ceph_objects("nfs")[0].node.hostname
        fs_util.create_nfs(
            client, "cephfs-nfs", validate=True, placement="1 %s" % nfs_server
        )

        log.info("Mounting root filesystem paths (Fuse, Kernel, NFS)...")
        fuse_root_dir, kernel_root_dir, nfs_root_dir = [
            m[0]
            for m in ref_inode_utils.mount_rootfs(
                client, fs_util, default_fs, nfs_server, "cephfs-nfs"
            )
        ]

        subvol_group_name = "subvol_group1"
        log.info("Creating subvolumes under group '%s'...", subvol_group_name)
        subvolume_names = ref_inode_utils.create_subvolumes(
            client, fs_util, default_fs, subvol_group_name, 1, 2
        )
        log.info("Mounting subvolumes...")
        fuse_subvol_dirs, kernel_subvol_dirs, nfs_subvol_dirs = (
            ref_inode_utils.mount_subvolumes(
                client,
                fs_util,
                default_fs,
                subvolume_names,
                subvol_group_name,
                nfs_server,
                "cephfs-nfs",
            )
        )

        DIRECT = {
            "Fuse": [fuse_root_dir],
            "Kernel": [kernel_root_dir],
            "NFS": [nfs_root_dir],
        }
        SUBVOL = {
            "Fuse": fuse_subvol_dirs,
            "Kernel": kernel_subvol_dirs,
            "NFS": nfs_subvol_dirs,
        }
        all_mount_paths = {"rootfs": DIRECT, "subvol": SUBVOL}

        log.info("Enabling allow_referent_inodes feature on filesystem %s", default_fs)
        ref_inode_utils.allow_referent_inode_feature_enablement(
            client, default_fs, enable=True
        )

        # Directory creation
        create_dirs_on_mounts(ref_inode_utils, client, DIRECT, MOUNT_POINTS)
        create_dirs_on_mounts(ref_inode_utils, client, SUBVOL, MOUNT_POINTS)
        # Initial IO
        all_created_files = run_dd_on_existing_files(
            client, DIRECT, MOUNT_POINTS, "rootfs"
        ) + run_dd_on_existing_files(client, SUBVOL, MOUNT_POINTS, "subvol")

        log.info("Flushing MDS journal...")
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Gather inode numbers
        inode_numbers = {}
        for mount_type, idx, fname, source in all_created_files:
            base_paths = DIRECT if source == "rootfs" else SUBVOL
            path = get_file_path(mount_type, idx, fname, base_paths)
            try:
                inode = ref_inode_utils.get_inode_number(client, path)
                inode_numbers.setdefault(mount_type, {})[
                    "%s:%s" % (base_paths[mount_type][idx], fname)
                ] = inode
            except Exception as e:
                log.warning(
                    "Failed to get inode number for %s at %s: %s", fname, path, e
                )

        # Hardlink/snapshot cycles
        hardlink_suffixes = ["hl", "hl2", "hl3", "hl4", "hl5"]
        for i, suffix in enumerate(hardlink_suffixes, start=1):
            log.info("Hardlink/Snapshot cycle %s ", i)
            if not create_hardlinks(ref_inode_utils, client, all_mount_paths, suffix):
                return 1
            ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)
            create_snapshots(
                fs_util,
                ref_inode_utils,
                client,
                default_fs,
                subvolume_names,
                subvol_group_name,
                DIRECT,
                "snap%s" % i,
            )
            run_dd_on_existing_files(client, DIRECT, MOUNT_POINTS, "rootfs")
            run_dd_on_existing_files(client, SUBVOL, MOUNT_POINTS, "subvol")

        log.info("All CephFS referent inode scenarios PASSED.")
        return 0

    except Exception as err:
        log.exception("Error during run: %s", err)
        return 1
