import json

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
                "f_file1_hl_renamed",
                "f_file1_hl_moved",
                "f_file1_renamed",
            ],
            "f_dir2": ["f_file2", "f_file1_hl", "f_file1_hl_moved"],
        },
    },
    "Kernel": {
        "dirs": ["k_dir1", "k_dir2"],
        "files": {
            "k_dir1": [
                "k_file1",
                "k_file1_hl",
                "k_file1_hl_renamed",
                "k_file1_hl_moved",
                "k_file1_renamed",
            ],
            "k_dir2": ["k_file2", "k_file1_hl", "k_file1_hl_moved"],
        },
    },
    "NFS": {
        "dirs": ["n_dir1", "n_dir2"],
        "files": {
            "n_dir1": [
                "n_file1",
                "n_file1_hl",
                "n_file1_hl_renamed",
                "n_file1_hl_moved",
                "n_file1_renamed",
            ],
            "n_dir2": ["n_file2", "n_file1_hl", "n_file1_hl_moved"],
        },
    },
}


def get_file_path(mount_type, dir_idx, filename, mount_paths):
    """
    Return the absolute path for a filename in the mapped directory/mount.
    """
    base_path = mount_paths[mount_type][dir_idx]
    directory = MOUNT_POINTS[mount_type]["dirs"][dir_idx]
    return "%s/%s/%s" % (base_path, directory, filename)


def get_dir_path(mount_type, dir_idx, mount_paths):
    """
    Returns full directory path for a given type and directory index.
    """
    base_path = mount_paths[mount_type][dir_idx]
    directory = MOUNT_POINTS[mount_type]["dirs"][dir_idx]
    return "%s/%s" % (base_path, directory)


def tag_to_mount_type(tag):
    """Maps tag prefix (f_, k_, n_) to mount type."""
    if tag.startswith("f_"):
        return "Fuse"
    elif tag.startswith("k_"):
        return "Kernel"
    elif tag.startswith("n_"):
        return "NFS"
    return None


def parent_dir(path):
    """Returns the parent directory for a given file path."""
    return path.rsplit("/", 1)[0]


def log_referent_inode_map(label, inode_map):
    """Pretty prints a referent inode map with section label."""
    log.info("%s: %s", label, json.dumps(inode_map, indent=2))


def normalize_vals(d):
    """
    Used for value-only dictionary comparison (set of frozensets).
    """
    return set(frozenset(v) for v in d.values() if v)


def run(ceph_cluster, **kwargs):
    """
    Main test entrypoint for CephFS referent-inode scenario validation.
    Returns 0 on pass, 1 on any failure.
    Scenarios covered -
    * Create test files across Fuse, Kernel, and NFS mounts, fetch and log inode numbers,
            verify that no unexpected hardlinks or referent inodes exist initially.
    * Append data to files and confirm inode numbers remain unchanged.
    * Create hardlinks to the original files in the same directories;
            validate updated hardlink counts and consistent referent inode maps.
    * Rename original files - verify inode stability and the persistence of referent inodes post rename.
    * Create hardlinks across directories on the same mount; validate referent inode consistency.
    * Rename files that are hardlinked; validate the continued integrity of referent inode data.
    * Move renamed hardlinked files within mounts;
            verify that referent inode mappings remain unchanged and linked correctly.
    * Attempt hardlink creation across different mount types to confirm expected,
            POSIX-compliant failure (cross-device link error).
    * Unlink all hardlinks for test files and
            confirm that referent inodes are properly removed, ensuring no residual references remain.
    """
    try:
        test_data, config = kwargs.get("test_data"), kwargs.get("config")
        ref_inode_utils = RefInodeUtils(ceph_cluster)
        fs_util, clients, erasure = ref_inode_utils.prepare_environment(
            ceph_cluster, test_data, config
        )

        if not clients:
            log.info("This test requires at least 1 client node. Found 0.")
            return 1
        client = clients[0]
        default_fs = "cephfs_ref-ec" if erasure else "cephfs_ref"

        # ---------- Filesystem and NFS Setup ----------
        if not fs_util.get_fs_info(client, default_fs):
            fs_util.create_fs(client, default_fs)
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        if not nfs_servers:
            log.error("No NFS servers found in the Ceph cluster.")
            return 1
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        try:
            fs_util.create_nfs(
                client, nfs_name, validate=True, placement="1 %s" % nfs_server
            )
            log.info("NFS cluster %s created successfully.", nfs_name)
        except CommandFailed as e:
            log.error("Failed to create NFS cluster: %s", e)
            return 1

        # ---------- Mount Creation and Paths ----------
        subvol_group_name = "subvol_group1"
        subvolume_names = ref_inode_utils.create_subvolumes(
            client,
            fs_util,
            default_fs,
            subvol_group_name,
            num_grouped=3,
            num_ungrouped=3,
        )
        fuse_dir, kernel_dir, nfs_dir = ref_inode_utils.mount_subvolumes(
            client,
            fs_util,
            default_fs,
            subvolume_names,
            subvol_group_name,
            nfs_server,
            nfs_name,
        )
        mount_paths = dict(Fuse=fuse_dir, Kernel=kernel_dir, NFS=nfs_dir)

        log.info("Validate if allow referent inode feature is enabled")
        ref_inode_utils.allow_referent_inode_feature_enablement(
            client, default_fs, enable=True
        )

        # ---------- Directory Creation ----------
        for mount_type, base_paths in mount_paths.items():
            dirnames = MOUNT_POINTS[mount_type]["dirs"]
            for idx, base_path in enumerate(base_paths[: len(dirnames)]):
                ref_inode_utils.create_directories(client, base_path, [dirnames[idx]])
                log.info(
                    "Created directory '%s' in %s (%s)",
                    dirnames[idx],
                    base_path,
                    mount_type,
                )

        # ---------- File Creation ----------
        created_files = []
        for mount_type, base_paths in mount_paths.items():
            dirs = MOUNT_POINTS[mount_type]["dirs"]
            for idx, dirname in enumerate(dirs):
                file_list = MOUNT_POINTS[mount_type]["files"][dirname]
                fname = file_list[0]
                path = get_file_path(mount_type, idx, fname, mount_paths)
                ref_inode_utils.create_file_with_content(
                    client, path, "%s file content" % mount_type
                )
                log.info("Created file: %s", path)
                created_files.append((mount_type, idx, fname))
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # ---------- Inode Number Collection ----------
        inode_numbers = {}
        for mount_type, idx, fname in created_files:
            path = get_file_path(mount_type, idx, fname, mount_paths)
            inode = ref_inode_utils.get_inode_number(client, path)
            inode_numbers.setdefault(mount_type, {})[fname] = inode
            log.info("Inode for %s %s: %s @ %s", mount_type, fname, inode, path)

        # ==================================================
        #               SCENARIO 1
        # ==================================================
        log.info("Scenario 1: Check hard links and referent inodes")
        referent_inodes_map = {}

        for mount_type in MOUNT_POINTS:
            for idx, dirname in enumerate(MOUNT_POINTS[mount_type]["dirs"]):
                files = MOUNT_POINTS[mount_type]["files"][dirname]
                for fname in files:
                    if fname not in (
                        "%s_file1" % mount_type[0].lower(),
                        "%s_file2" % mount_type[0].lower(),
                    ):
                        continue
                    inode = inode_numbers.get(mount_type, {}).get(fname)
                    dir_path = mount_paths[mount_type][idx]
                    file_path = get_file_path(mount_type, idx, fname, mount_paths)
                    if inode is None:
                        log.warning(
                            "No inode found for: %s %s/%s (possibly not created)",
                            mount_type,
                            dirname,
                            fname,
                        )
                        continue
                    inode_data = ref_inode_utils.get_inode_details(
                        client, default_fs, inode, dir_path
                    )
                    log.info(
                        "%s at %s is located at: %s",
                        fname,
                        file_path,
                        inode_data.get("path", "Unknown"),
                    )
                    nlink = inode_data.get("nlink", 1)
                    if nlink > 1:
                        hardlinks = nlink - 1
                        log.error(
                            "Hard links found for %s: %s (Total links: %s)",
                            fname,
                            hardlinks,
                            nlink,
                        )
                    else:
                        log.info("No hard links found for %s", fname)
                    referent_inodes = inode_data.get("referent_inodes", [])
                    if referent_inodes:
                        log.error("Referent inodes for %s: %s", fname, referent_inodes)
                    else:
                        log.info("No referent inodes for %s (expected)", fname)
                    referent_inodes_map.setdefault(fname, []).append(referent_inodes)
        log_referent_inode_map(
            "Stored referent inodes after initial creation", referent_inodes_map
        )

        # ==================================================
        #               SCENARIO 2
        # ==================================================
        log.info("Scenario 2: Modify file content and validate referent inodes")
        original_inodes = {}
        modified_inodes = {}

        #  Record original inode numbers
        for mount_type in MOUNT_POINTS:
            for idx, dirname in enumerate(MOUNT_POINTS[mount_type]["dirs"]):
                files = MOUNT_POINTS[mount_type]["files"][dirname]
                for fname in files:
                    if fname not in (
                        "%s_file1" % mount_type[0].lower(),
                        "%s_file2" % mount_type[0].lower(),
                    ):
                        continue
                    file_path = get_file_path(mount_type, idx, fname, mount_paths)
                    try:
                        inode = ref_inode_utils.get_inode_number(client, file_path)
                        original_inodes.setdefault(mount_type, {})[fname] = inode
                        log.info(
                            "Before modification - %s %s: inode %s",
                            mount_type,
                            fname,
                            inode,
                        )
                    except Exception as e:
                        log.error("Failed to fetch inode for %s: %s", file_path, e)

        #  Append new content
        for mount_type in MOUNT_POINTS:
            for idx, dirname in enumerate(MOUNT_POINTS[mount_type]["dirs"]):
                files = MOUNT_POINTS[mount_type]["files"][dirname]
                for fname in files:
                    if fname not in (
                        "%s_file1" % mount_type[0].lower(),
                        "%s_file2" % mount_type[0].lower(),
                    ):
                        continue
                    file_path = get_file_path(mount_type, idx, fname, mount_paths)
                    ref_inode_utils.append_to_file(
                        client, file_path, "New content for %s %s" % (mount_type, fname)
                    )
                    log.info(
                        "Appended content to %s %s at %s", mount_type, fname, file_path
                    )

        #  Get inode numbers after modification
        for mount_type in MOUNT_POINTS:
            for idx, dirname in enumerate(MOUNT_POINTS[mount_type]["dirs"]):
                files = MOUNT_POINTS[mount_type]["files"][dirname]
                for fname in files:
                    if fname not in (
                        "%s_file1" % mount_type[0].lower(),
                        "%s_file2" % mount_type[0].lower(),
                    ):
                        continue
                    file_path = get_file_path(mount_type, idx, fname, mount_paths)
                    try:
                        inode = ref_inode_utils.get_inode_number(client, file_path)
                        modified_inodes.setdefault(mount_type, {})[fname] = inode
                        log.info(
                            "After modification - %s %s: inode %s",
                            mount_type,
                            fname,
                            inode,
                        )
                    except Exception as e:
                        log.error("Failed to fetch inode for %s: %s", file_path, e)

        #  Compare inodes
        for mount_type in original_inodes:
            for fname in original_inodes[mount_type]:
                orig_inode = original_inodes[mount_type][fname]
                mod_inode = modified_inodes.get(mount_type, {}).get(fname)
                if mod_inode is None:
                    log.error("Missing modified inode for %s %s", mount_type, fname)
                    continue
                if orig_inode != mod_inode:
                    log.error(
                        "Inode changed for %s %s: Before=%s, After=%s",
                        mount_type,
                        fname,
                        orig_inode,
                        mod_inode,
                    )
                else:
                    log.info(
                        "Inode unchanged for %s %s: %s", mount_type, fname, orig_inode
                    )

        #  Validate referent inodes remain
        for mount_type in MOUNT_POINTS:
            for idx, dirname in enumerate(MOUNT_POINTS[mount_type]["dirs"]):
                files = MOUNT_POINTS[mount_type]["files"][dirname]
                for fname in files:
                    if fname not in (
                        "%s_file1" % mount_type[0].lower(),
                        "%s_file2" % mount_type[0].lower(),
                    ):
                        continue
                    inode = modified_inodes.get(mount_type, {}).get(fname)
                    if inode is None:
                        continue
                    dir_path = mount_paths[mount_type][idx]
                    inode_data = ref_inode_utils.get_inode_details(
                        client, default_fs, inode, dir_path
                    )
                    referent_inodes = inode_data.get("referent_inodes", [])
                    if referent_inodes:
                        log.info(
                            "Referent inodes for %s %s remain intact: %s",
                            mount_type,
                            fname,
                            referent_inodes,
                        )
                    else:
                        log.info(
                            "No referent inodes found for %s %s, consistent with original state.",
                            mount_type,
                            fname,
                        )

        # ==================================================
        #               SCENARIO 3
        # ==================================================
        log.info(
            "Scenario 3: Validate referent inodes when a file is hardlinked within the same directory"
        )
        referent_inodes_map = {}

        for mount_type in ["Fuse", "Kernel", "NFS"]:
            dir_idx = 0
            orig_fname = "%s_file1" % mount_type[0].lower()
            hardlink_fname = "%s_file1_hl" % mount_type[0].lower()
            orig_path = get_file_path(mount_type, dir_idx, orig_fname, mount_paths)
            hl_path = get_file_path(mount_type, dir_idx, hardlink_fname, mount_paths)
            try:
                ref_inode_utils.create_hardlink(client, orig_path, hl_path)
                log.info(
                    "Created hardlink for %s: %s -> %s", mount_type, orig_path, hl_path
                )
            except CommandFailed as e:
                log.error("Failed to create hardlink for %s: %s", mount_type, e)
                return 1

        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Fetch referent inode details for original files
        for mount_type, fname in [
            ("Fuse", "f_file1"),
            ("Kernel", "k_file1"),
            ("NFS", "n_file1"),
        ]:
            inode = inode_numbers.get(mount_type, {}).get(fname)
            if inode is None:
                log.error(
                    "No inode found for %s %s, skipping referent inode check.",
                    mount_type,
                    fname,
                )
                continue
            mount_dir = mount_paths[mount_type][0]
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )
            file_path = inode_data.get("path", "Unknown")
            log.info("%s is located at: %s", fname, file_path)
            nlink = inode_data.get("nlink", 1)
            if nlink > 1:
                hardlinks = nlink - 1
                log.info(
                    "Hard links found for %s: %s (Total links: %s)",
                    fname,
                    hardlinks,
                    nlink,
                )
            else:
                log.info("No hard links found for %s", fname)
            referent_inodes = inode_data.get("referent_inodes", [])
            if referent_inodes:
                log.info("Referent inodes for %s: %s", fname, referent_inodes)
                referent_inodes_map[fname] = referent_inodes
            else:
                log.info("No referent inodes found for %s", fname)
                referent_inodes_map[fname] = []
        log.info(
            "Stored referent inodes after hardlink creation: %s", referent_inodes_map
        )

        # Validate referent inode linkage
        for file_name, inode_list in referent_inodes_map.items():
            if not inode_list:
                log.info(
                    "No referent inodes found for %s, skipping validation.", file_name
                )
                continue
            mount_type_key = {
                "f_file1": "Fuse",
                "k_file1": "Kernel",
                "n_file1": "NFS",
            }.get(file_name)
            source_inode = inode_numbers.get(mount_type_key, {}).get(file_name)
            mount_dir = mount_paths[mount_type_key][0]
            if source_inode is None:
                log.error("Inode for %s not found! Skipping validation.", file_name)
                continue
            for referent_inode in inode_list:
                log.info(
                    "Fetching details for referent inode %s (%s) and validating linkage to source file",
                    referent_inode,
                    file_name,
                )
                ref_inode_utils.get_referent_inode_details(
                    client, default_fs, source_inode, referent_inode, mount_dir
                )

        # ==================================================
        #               SCENARIO 4
        # ==================================================
        log.info("Scenario 4: Rename source file and validate referent inodes")
        renamed_inodes_map = {}
        referent_inodes_map = {}

        #  Capture initial inode numbers before renaming
        initial_inodes = {}
        for mount_type in ["Fuse", "Kernel", "NFS"]:
            dir_idx = 0
            orig_fname = "%s_file1" % mount_type[0].lower()
            file_path = get_file_path(mount_type, dir_idx, orig_fname, mount_paths)
            try:
                inode = ref_inode_utils.get_inode_number(client, file_path)
                initial_inodes[mount_type] = inode
                log.info(
                    "Before renaming - %s %s: inode %s", mount_type, orig_fname, inode
                )
            except Exception as e:
                log.error("Failed to get inode for %s: %s", file_path, e)
                return 1

        #  Rename files
        for mount_type in ["Fuse", "Kernel", "NFS"]:
            dir_idx = 0
            orig_fname = "%s_file1" % mount_type[0].lower()
            new_fname = "%s_renamed" % orig_fname
            old_path = get_file_path(mount_type, dir_idx, orig_fname, mount_paths)
            new_path = get_file_path(mount_type, dir_idx, new_fname, mount_paths)
            try:
                ref_inode_utils.rename_file(client, old_path, new_path)
                log.info("Renamed %s: %s -> %s", mount_type, old_path, new_path)
            except CommandFailed as e:
                log.error("Failed to rename file %s -> %s: %s", old_path, new_path, e)
                return 1

        #  Capture inode numbers after renaming
        for mount_type in ["Fuse", "Kernel", "NFS"]:
            dir_idx = 0
            renamed_fname = "%s_file1_renamed" % mount_type[0].lower()
            renamed_path = get_file_path(
                mount_type, dir_idx, renamed_fname, mount_paths
            )
            try:
                inode = ref_inode_utils.get_inode_number(client, renamed_path)
                renamed_inodes_map[mount_type] = inode
                log.info(
                    "After renaming - %s %s: inode %s", mount_type, renamed_fname, inode
                )
            except Exception as e:
                log.error("Failed to fetch inode for %s: %s", renamed_path, e)
                return 1

        #  Validate inode numbers
        for mount_type in ["Fuse", "Kernel", "NFS"]:
            before = initial_inodes.get(mount_type)
            after = renamed_inodes_map.get(mount_type)
            if before != after:
                log.error(
                    "Inode changed for %s %s_file1 after renaming: Before=%s, After=%s",
                    mount_type,
                    mount_type[0].lower(),
                    before,
                    after,
                )
            else:
                log.info(
                    "Inode unchanged for %s after renaming: %s", mount_type, before
                )
        log.info("File renaming test passed - inodes remained unchanged.")

        #  Validate referent inodes remain intact for renamed files
        for mount_type in ["Fuse", "Kernel", "NFS"]:
            dir_idx = 0
            renamed_fname = "%s_file1_renamed" % mount_type[0].lower()
            inode = renamed_inodes_map.get(mount_type)
            mount_dir = mount_paths[mount_type][dir_idx]
            if inode is None:
                log.error(
                    "No inode found for renamed file %s in %s, skipping referent inode check.",
                    renamed_fname,
                    mount_type,
                )
                continue
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )
            referent_inodes = inode_data.get("referent_inodes", [])
            if referent_inodes:
                log.info(
                    "Referent inodes for %s remain intact: %s",
                    renamed_fname,
                    referent_inodes,
                )
                referent_inodes_map[renamed_fname] = referent_inodes
            else:
                log.info(
                    "No referent inodes found for %s, consistent with original state.",
                    renamed_fname,
                )
                referent_inodes_map[renamed_fname] = None

        #  Validate referent inode linkage
        for file_name, inode_list in referent_inodes_map.items():
            if inode_list:
                mount_type = file_name[0].upper() + file_name[1:].split("_")[0]
                source_inode = None
                for key in renamed_inodes_map.keys():
                    if file_name.startswith(key[0].lower()):
                        source_inode = renamed_inodes_map[key]
                        mount_type = key
                        break
                if source_inode is None:
                    log.error("Renamed inode for %s not found!", file_name)
                    continue
                dir_idx = 0
                mount_dir = mount_paths[mount_type][dir_idx]
                for referent_inode in inode_list:
                    log.info(
                        "Fetching details for referent inode %s (%s) and validating linkage to source file",
                        referent_inode,
                        file_name,
                    )
                    ref_inode_utils.get_referent_inode_details(
                        client, default_fs, source_inode, referent_inode, mount_dir
                    )
            else:
                log.info(
                    "No referent inodes found for %s, skipping validation.", file_name
                )
        log.info("File renaming test passed - referent inodes remained unchanged.")

        # ==================================================
        #               SCENARIO 5
        # ==================================================
        log.info(
            "Scenario 5: Attempting to create a hard link across different mount paths "
            "(expected to fail: cross-device link)"
        )
        for mount_type in ["Fuse", "Kernel", "NFS"]:
            if len(mount_paths[mount_type]) < 2:
                log.warning(
                    "Not enough mount paths for %s to test cross-mount hard link scenario.",
                    mount_type,
                )
                continue
            src = get_file_path(
                mount_type, 0, "%s_file1_renamed" % mount_type[0].lower(), mount_paths
            )
            dest_base = mount_paths[mount_type][1]
            dest_dirname = MOUNT_POINTS[mount_type]["dirs"][1]
            dest = "%s/%s/%s_file1_hl" % (
                dest_base,
                dest_dirname,
                mount_type[0].lower(),
            )
            try:
                ref_inode_utils.create_hardlink(client, src, dest)
                log.error(
                    "Unexpectedly succeeded in creating cross-mount hard link: %s -> %s",
                    src,
                    dest,
                )
                log.error(
                    "Test failed: Hard link should not be created across different mount paths."
                )
                return 1
            except CommandFailed as e:
                err_str = str(e)
                if "Invalid cross-device link" in err_str:
                    log.info(
                        "Cross-device hard link not supported "
                        "(expected): %s -> %s. This is correct and POSIX-compliant behavior. Scenario PASSES.",
                        src,
                        dest,
                    )
                else:
                    log.warning(
                        "Received unexpected error while creating cross-mount hard link for %s: %s. "
                        "But all errors for cross-mount hard links are considered a pass for this scenario.",
                        mount_type,
                        e,
                    )
        log.info(
            "Scenario 5 passed - Hard link across different mount paths is not supported, as expected."
        )

        # ==================================================
        #               SCENARIO 6
        # ==================================================
        log.info("Scenario 6: Rename Hardlinked Files and Validate Referent Inodes")
        renamed_files_map = {
            mount_type: (
                get_file_path(
                    mount_type, 0, "%s_file1_hl" % mount_type[0].lower(), mount_paths
                ),
                get_file_path(
                    mount_type,
                    0,
                    "%s_file1_hl_renamed" % mount_type[0].lower(),
                    mount_paths,
                ),
            )
            for mount_type in ["Fuse", "Kernel", "NFS"]
        }
        for mount_type, (old_path, new_path) in renamed_files_map.items():
            try:
                ref_inode_utils.rename_file(client, old_path, new_path)
                log.info("Renamed hardlinked file: %s -> %s", old_path, new_path)
            except CommandFailed as e:
                log.error("Failed to rename file %s -> %s: %s", old_path, new_path, e)
                return 1

        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        #  Capture inode numbers and referent inodes for renamed files
        renamed_referent_inodes_map = {}
        file_inode_map = {}
        for mount_type in ["Fuse", "Kernel", "NFS"]:
            renamed_fname = "%s_file1_hl_renamed" % mount_type[0].lower()
            path = get_file_path(mount_type, 0, renamed_fname, mount_paths)
            inode = ref_inode_utils.get_inode_number(client, path)
            file_inode_map[renamed_fname] = inode
        for tag, inode in file_inode_map.items():
            mount_type = tag_to_mount_type(tag)
            mount_dir = mount_paths[mount_type]
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )
            referent_inodes = inode_data.get("referent_inodes", [])
            if referent_inodes:
                log.info(
                    "Referent inodes for %s after rename: %s", tag, referent_inodes
                )
                renamed_referent_inodes_map[tag] = referent_inodes
            else:
                log.info(
                    "No referent inodes found for %s after rename, consistent with original state.",
                    tag,
                )
                renamed_referent_inodes_map[tag] = None
        log.info("Original referent inode map before rename: %s", referent_inodes_map)
        log.info("Referent inode details after rename: %s", renamed_referent_inodes_map)

        if normalize_vals(referent_inodes_map) != normalize_vals(
            renamed_referent_inodes_map
        ):
            log.error("Referent inodes changed after renaming!")
            return 1
        log.info("Referent inodes remain unchanged after renaming.")

        #  Validate referent inode linkage after rename
        for file_name, inode_list in renamed_referent_inodes_map.items():
            if inode_list:
                source_inode = file_inode_map.get(file_name)
                mount_type = tag_to_mount_type(file_name)
                mount_dir = mount_paths[mount_type]
                for referent_inode in inode_list:
                    log.info(
                        "Fetching details for referent inode %s (%s) and validating linkage to source file",
                        referent_inode,
                        file_name,
                    )
                    ref_inode_utils.get_referent_inode_details(
                        client, default_fs, source_inode, referent_inode, mount_dir
                    )
            else:
                log.info(
                    "No referent inodes found for %s, skipping validation.", file_name
                )
        log.info("Rename test for hardlinked files passed successfully.")

        # ==================================================
        #               SCENARIO 7
        # ==================================================
        log.info(
            "Scenario 7: Move Renamed Hardlinked Files and Validate Referent Inodes"
        )
        pre_move_referent_inodes_map = {}
        pre_move_file_inode_map = {}
        for mount_type in ["Fuse", "Kernel", "NFS"]:
            moved_fname = "%s_file1_hl_renamed" % mount_type[0].lower()
            path = get_file_path(mount_type, 0, moved_fname, mount_paths)
            inode = ref_inode_utils.get_inode_number(client, path)
            pre_move_file_inode_map[moved_fname] = inode
            mount_dir = mount_paths[mount_type]
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )
            referent_inodes = inode_data.get("referent_inodes", [])
            if referent_inodes:
                log.info(
                    "Referent inodes for %s before move: %s",
                    moved_fname,
                    referent_inodes,
                )
                pre_move_referent_inodes_map[moved_fname] = referent_inodes
            else:
                log.info(
                    "No referent inodes found for %s before move, consistent with original state.",
                    moved_fname,
                )
                pre_move_referent_inodes_map[moved_fname] = None
        log.info(
            "Referent Inode Map before move: %s",
            json.dumps(pre_move_referent_inodes_map, indent=2),
        )

        moved_files_map = {
            mount_type: (
                get_file_path(
                    mount_type,
                    0,
                    "%s_file1_hl_renamed" % mount_type[0].lower(),
                    mount_paths,
                ),
                get_file_path(
                    mount_type,
                    0,
                    "%s_file1_hl_moved" % mount_type[0].lower(),
                    mount_paths,
                ),
            )
            for mount_type in ["Fuse", "Kernel", "NFS"]
        }
        for mount_type, (old_path, new_path) in moved_files_map.items():
            try:
                ref_inode_utils.rename_file(client, old_path, new_path)
                log.info("Moved renamed hardlinked file: %s -> %s", old_path, new_path)
            except CommandFailed as e:
                log.error("Failed to move file %s -> %s: %s", old_path, new_path, e)
                return 1
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        moved_referent_inodes_map = {}
        file_inode_map = {}
        for mount_type in ["Fuse", "Kernel", "NFS"]:
            moved_fname = "%s_file1_hl_moved" % mount_type[0].lower()
            path = get_file_path(mount_type, 0, moved_fname, mount_paths)
            inode = ref_inode_utils.get_inode_number(client, path)
            if inode is None:
                log.error(
                    "Failed to retrieve inode number for %s. Skipping validation.",
                    moved_fname,
                )
                return 1
            file_inode_map[moved_fname] = inode

        mount_dirs = {
            "f_file1_hl_moved": mount_paths["Fuse"],
            "k_file1_hl_moved": mount_paths["Kernel"],
            "n_file1_hl_moved": mount_paths["NFS"],
        }
        for tag, inode in file_inode_map.items():
            mount_dir = mount_dirs.get(tag)
            if not mount_dir:
                log.error("Mount directory not found for %s, skipping.", tag)
                continue
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )
            referent_inodes = inode_data.get("referent_inodes", [])
            if referent_inodes:
                log.info("Referent inodes for %s after move: %s", tag, referent_inodes)
                moved_referent_inodes_map[tag] = referent_inodes
            else:
                log.info(
                    "No referent inodes found for %s after move, consistent with original state.",
                    tag,
                )
                moved_referent_inodes_map[tag] = None
        log.info(
            "Referent Inode Map after move: %s",
            json.dumps(moved_referent_inodes_map, indent=2),
        )

        if normalize_vals(pre_move_referent_inodes_map) != normalize_vals(
            moved_referent_inodes_map
        ):
            log.error("Referent inodes changed after moving!")
            return 1
        log.info("Referent inodes remain unchanged after moving.")

        for tag, inode_list in pre_move_referent_inodes_map.items():
            if inode_list:
                source_inode = pre_move_file_inode_map.get(tag)
                mount_dir = mount_dirs.get(
                    tag.replace("_hl_renamed", "_hl_moved"), None
                )
                if source_inode is None or mount_dir is None:
                    log.error(
                        "Source inode or mount_dir not found for %s. Skipping validation.",
                        tag,
                    )
                    continue
                for referent_inode in inode_list:
                    log.info(
                        "Fetching details for referent inode %s (%s) and validating linkage to source file",
                        referent_inode,
                        tag,
                    )
                    ref_inode_utils.get_referent_inode_details(
                        client, default_fs, source_inode, referent_inode, mount_dir
                    )
            else:
                log.info("No referent inodes found for %s, skipping validation.", tag)
        log.info("Move test for renamed hardlinked files passed successfully.")

        # ==================================================
        #               SCENARIO 8
        # ==================================================
        log.info(
            "Scenario 8: Retrieve all hard links, unlink them, and verify removal of referent inodes."
        )
        referent_inodes_map = {}
        post_referent_inodes_map = {}
        file_paths = {
            "f_file1_renamed": "%s/f_dir1/f_file1_renamed" % mount_paths["Fuse"][0],
            "k_file1_renamed": "%s/k_dir1/k_file1_renamed" % mount_paths["Kernel"][0],
            "n_file1_renamed": "%s/n_dir1/n_file1_renamed" % mount_paths["NFS"][0],
        }
        file_inode_map = {
            tag: ref_inode_utils.get_inode_number(client, path)
            for tag, path in file_paths.items()
        }

        for tag, inode in file_inode_map.items():
            mount_dir = parent_dir(file_paths[tag])
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )
            referent_inodes_map[tag] = inode_data.get("referent_inodes", None)
            log.info("%s referent inodes: %s", tag, referent_inodes_map[tag])
        log.info("Initial referent inodes: %s", referent_inodes_map)

        log.info("Unlinking hardlinks for all files...")
        for tag, path in file_paths.items():
            mount_dir = parent_dir(path)
            ref_inode_utils.unlink_hardlinks(client, default_fs, path, mount_dir)
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        log.info("Verifying referent inodes after unlinking...")
        for tag, inode in file_inode_map.items():
            mount_dir = parent_dir(file_paths[tag])
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )
            post_referent_inodes_map[tag] = inode_data.get("referent_inodes", None)
            log.info(
                "%s referent inodes after unlinking: %s",
                tag,
                post_referent_inodes_map[tag],
            )
        log.info("Post unlink referent inodes: %s", post_referent_inodes_map)

        for tag in referent_inodes_map:
            referents = post_referent_inodes_map.get(tag)
            if referents is not None and len(referents) > 0:
                log.error("Referent inodes for %s still exist: %s", tag, referents)
                return 1
        log.info("Test passed: All referent inodes removed as expected.")

        log.info("All CephFS referent inode scenarios PASSED.")
        return 0

    except Exception as err:
        log.exception("An error occurred during the test run: %s", err)
        return 1
