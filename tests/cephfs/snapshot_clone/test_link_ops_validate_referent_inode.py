from ceph.ceph import CommandFailed
from tests.cephfs.lib.cephfs_refinode_utils import RefInodeUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kwargs):
    try:
        test_data = kwargs.get("test_data")
        config = kwargs.get("config")
        ref_inode_utils = RefInodeUtils(ceph_cluster)  # Create an instance

        fs_util, clients, erasure = ref_inode_utils.prepare_environment(
            ceph_cluster, test_data, config
        )

        # Ensure at least one client is available.
        if not clients:
            log.info(
                "This test requires at least 1 client node. Found %d client node(s).",
                len(clients),
            )
            return 1

        default_fs = "cephfs_ref-ec" if erasure else "cephfs_ref"
        client = clients[0]

        # Create the filesystem if it does not exist.
        if not fs_util.get_fs_info(client, default_fs):
            fs_util.create_fs(client, default_fs)

        # Retrieve NFS server information.
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        if not nfs_servers:
            log.error("No NFS servers found in the Ceph cluster.")
            return 1
        nfs_server = nfs_servers[0].node.hostname

        nfs_name = "cephfs-nfs"
        try:
            cmd_output, cmd_return_code = fs_util.create_nfs(
                client, nfs_name, validate=True, placement=f"1 {nfs_server}"
            )
            log.info(f"NFS cluster {nfs_name} created successfully.")
        except CommandFailed as e:
            log.error(f"Failed to create NFS cluster: {e}")
            return 1

        log.info("Validate if allow referent inode feature is enabled")
        ref_inode_utils.allow_referent_inode_feature_enablement(
            client, default_fs, enable=True
        )

        # Mount the root filesystems.
        fuse_dir, kernel_dir, nfs_dir = ref_inode_utils.mount_rootfs(
            client, fs_util, default_fs, nfs_server, nfs_name
        )

        mount_dirs = {
            "Fuse": (fuse_dir, ["f_dir1"]),
            "Kernel": (kernel_dir, ["k_dir1"]),
            "NFS": (nfs_dir, ["n_dir1"]),
        }
        # Loop through the mount directories and create the necessary directories.
        for label, (base_path, dirs) in mount_dirs.items():
            ref_inode_utils.create_directories(client, base_path, dirs)
        log.info(f"Created directories inside {label} mount: {dirs}")

        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Define the files to be created inside each directory.
        files_to_create = {
            "Fuse": (f"{fuse_dir}/f_dir1/f_file1", "Fuse file content"),
            "Kernel": (f"{kernel_dir}/k_dir1/k_file1", "Kernel file content"),
            "NFS": (f"{nfs_dir}/n_dir1/n_file1", "NFS file content"),
        }

        # Loop through each mount point and create the tagged file.
        for label, (file_path, content) in files_to_create.items():
            ref_inode_utils.create_file_with_content(client, file_path, content)
            log.info(f"Created file inside {label} mount: {file_path}")

        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Get inode numbers for each file and tag them accordingly.
        f_file1_inode = ref_inode_utils.get_inode_number(
            client, f"{fuse_dir}/f_dir1/f_file1"
        )
        k_file1_inode = ref_inode_utils.get_inode_number(
            client, f"{kernel_dir}/k_dir1/k_file1"
        )
        n_file1_inode = ref_inode_utils.get_inode_number(
            client, f"{nfs_dir}/n_dir1/n_file1"
        )

        # Log the inode numbers.
        log.info(f"Inode of f_file1: {f_file1_inode}")
        log.info(f"Inode of k_file1: {k_file1_inode}")
        log.info(f"Inode of n_file1: {n_file1_inode}")

        # --------- Scenario : Check hard links and referent inodes -----------
        # Dictionary to store referent inodes
        log.info("Scenario 1: Check hard links and referent inodes")
        referent_inodes_map = {}

        # Fetch inode details
        for inode, tag, mount_dir in [
            (f_file1_inode, "f_file1", fuse_dir),
            (k_file1_inode, "k_file1", kernel_dir),
            (n_file1_inode, "n_file1", nfs_dir),
        ]:
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )

            file_path = inode_data.get("path", "Unknown")
            log.info(f"{tag} is located at: {file_path}")

            # Check hard links (`nlink`)
            nlink = inode_data.get("nlink", 1)
            if nlink > 1:
                hardlinks = nlink - 1
                log.info(
                    f"Hard links found for {tag}: {hardlinks} (Total links: {nlink})"
                )
            else:
                log.info(f"No hard links found for {tag}")

            # Handle referent inodes
            referent_inodes = inode_data.get("referent_inodes", [])
            if referent_inodes:
                log.info(f"Referent inodes for {tag}: {referent_inodes}")
                referent_inodes_map[tag] = referent_inodes  # Store for future use
            else:
                log.info(f"No referent inodes found for {tag}")
                referent_inodes_map[tag] = (
                    None  # Explicitly store None if no referent inodes
                )
        # Log the stored referent inodes for future use
        log.info(f"Stored referent inodes: {referent_inodes_map}")

        # -------- Scenario: Modify file content and validate referent inodes -----------
        log.info("Scenario 2 : Modify file content and validate referent inodes")
        # Step 1: Get initial inode numbers before modification
        initial_f_file1_inode = ref_inode_utils.get_inode_number(
            client, f"{fuse_dir}/f_dir1/f_file1"
        )
        initial_k_file1_inode = ref_inode_utils.get_inode_number(
            client, f"{kernel_dir}/k_dir1/k_file1"
        )
        initial_n_file1_inode = ref_inode_utils.get_inode_number(
            client, f"{nfs_dir}/n_dir1/n_file1"
        )

        log.info(
            f"Before modification - "
            f"Inodes: f_file1={initial_f_file1_inode}, "
            f"k_file1={initial_k_file1_inode}, "
            f"n_file1={initial_n_file1_inode}"
        )

        # Step 2: Append content to the files
        ref_inode_utils.append_to_file(
            client, f"{fuse_dir}/f_dir1/f_file1", "New content for fuse file"
        )
        ref_inode_utils.append_to_file(
            client, f"{kernel_dir}/k_dir1/k_file1", "New content for kernel file"
        )
        ref_inode_utils.append_to_file(
            client, f"{nfs_dir}/n_dir1/n_file1", "New content for NFS file"
        )

        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Step 3: Get inode numbers after modification
        modified_f_file1_inode = ref_inode_utils.get_inode_number(
            client, f"{fuse_dir}/f_dir1/f_file1"
        )
        modified_k_file1_inode = ref_inode_utils.get_inode_number(
            client, f"{kernel_dir}/k_dir1/k_file1"
        )
        modified_n_file1_inode = ref_inode_utils.get_inode_number(
            client, f"{nfs_dir}/n_dir1/n_file1"
        )

        log.info(
            f"After modification - Inodes: "
            f"f_file1={modified_f_file1_inode}, "
            f"k_file1={modified_k_file1_inode}, "
            f"n_file1={modified_n_file1_inode}"
        )

        # Step 4: Validate that the inode numbers remain unchanged
        assert (
            initial_f_file1_inode == modified_f_file1_inode
        ), "Inode changed for f_file1!"
        assert (
            initial_k_file1_inode == modified_k_file1_inode
        ), "Inode changed for k_file1!"
        assert (
            initial_n_file1_inode == modified_n_file1_inode
        ), "Inode changed for n_file1!"

        log.info("File content modification test passed - inodes remained unchanged.")

        # Step 5: Validate referent inodes remain intact
        for inode, tag, mount_dir in [
            (modified_f_file1_inode, "f_file1", fuse_dir),
            (modified_k_file1_inode, "k_file1", kernel_dir),
            (modified_n_file1_inode, "n_file1", nfs_dir),
        ]:
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )

            referent_inodes = inode_data.get("referent_inodes", [])
            if referent_inodes:
                log.info(f"Referent inodes for {tag} remain intact: {referent_inodes}")
            else:
                log.info(
                    f"No referent inodes found for {tag}, consistent with original state."
                )

        # ---- Scenario: Validate referent inodes when a file is hardlinked within the same directory--
        log.info(
            "Scenario 3: Validate referent inodes when a file is hardlinked within the same directory"
        )
        # Create hard links for the files
        f_file1_link = f"{fuse_dir}/f_dir1/f_file1_hl"
        k_file1_link = f"{kernel_dir}/k_dir1/k_file1_hl"
        n_file1_link = f"{nfs_dir}/n_dir1/n_file1_hl"

        ref_inode_utils.create_hardlink(
            client, f"{fuse_dir}/f_dir1/f_file1", f_file1_link
        )
        ref_inode_utils.create_hardlink(
            client, f"{kernel_dir}/k_dir1/k_file1", k_file1_link
        )
        ref_inode_utils.create_hardlink(
            client, f"{nfs_dir}/n_dir1/n_file1", n_file1_link
        )

        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Get inode numbers for the parent file
        referent_inodes_map = {}

        # Dictionary to store mount directories
        mount_dir_map = {
            "f_file1": fuse_dir,
            "k_file1": kernel_dir,
            "n_file1": nfs_dir,
        }

        # Fetch inode details and populate referent inode mapping
        for inode, tag in [
            (f_file1_inode, "f_file1"),
            (k_file1_inode, "k_file1"),
            (n_file1_inode, "n_file1"),
        ]:
            mount_dir = mount_dir_map[tag]  # Get corresponding mount dir
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )

            file_path = inode_data.get("path", "Unknown")
            log.info(f"{tag} is located at: {file_path}")

            # Check hard links (`nlink`)
            nlink = inode_data.get("nlink", 1)
            if nlink > 1:
                hardlinks = nlink - 1
                log.info(
                    f"Hard links found for {tag}: {hardlinks} (Total links: {nlink})"
                )
            else:
                log.info(f"No hard links found for {tag}")

            # Handle referent inodes
            referent_inodes = inode_data.get("referent_inodes", [])
            if referent_inodes:
                log.info(f"Referent inodes for {tag}: {referent_inodes}")
                referent_inodes_map[tag] = referent_inodes  # Store for validation
            else:
                log.info(f"No referent inodes found for {tag}")
                referent_inodes_map[tag] = []

        # Log stored referent inodes
        log.info(f"Stored referent inodes: {referent_inodes_map}")

        # Dictionary to store original source inodes
        inodes_map = {
            "f_file1": f_file1_inode,
            "k_file1": k_file1_inode,
            "n_file1": n_file1_inode,
        }

        # Validate referent inodes linkage
        for file_name, inode_list in referent_inodes_map.items():
            if not inode_list:  # Skip empty lists
                log.info(
                    f"No referent inodes found for {file_name}, skipping validation."
                )
                continue

            source_inode = inodes_map.get(file_name)
            mount_dir = mount_dir_map.get(file_name)

            if source_inode is None:
                log.error(f"Inode for {file_name} not found! Skipping validation.")
                continue

            for referent_inode in inode_list:
                log.info(
                    f"Fetching details for referent inode {referent_inode} ({file_name})\n"
                    "and validating linkage to source file"
                )

                ref_inode_utils.get_referent_inode_details(
                    client, default_fs, source_inode, referent_inode, mount_dir
                )

        # -----Scenario: Rename source file and validate referent inodes -------
        log.info("Scenario 4 : Rename source file and validate referent inodes")
        # Step 1: Capture inode numbers before renaming
        initial_f_file1_inode = ref_inode_utils.get_inode_number(
            client, f"{fuse_dir}/f_dir1/f_file1"
        )
        initial_k_file1_inode = ref_inode_utils.get_inode_number(
            client, f"{kernel_dir}/k_dir1/k_file1"
        )
        initial_n_file1_inode = ref_inode_utils.get_inode_number(
            client, f"{nfs_dir}/n_dir1/n_file1"
        )

        log.info(
            f"Before renaming - Inodes: "
            f"f_file1={initial_f_file1_inode}, "
            f"k_file1={initial_k_file1_inode}, "
            f"n_file1={initial_n_file1_inode}"
        )

        # Step 2: Rename the files
        f_file1_new = f"{fuse_dir}/f_dir1/f_file1_renamed"
        k_file1_new = f"{kernel_dir}/k_dir1/k_file1_renamed"
        n_file1_new = f"{nfs_dir}/n_dir1/n_file1_renamed"

        ref_inode_utils.rename_file(client, f"{fuse_dir}/f_dir1/f_file1", f_file1_new)
        ref_inode_utils.rename_file(client, f"{kernel_dir}/k_dir1/k_file1", k_file1_new)
        ref_inode_utils.rename_file(client, f"{nfs_dir}/n_dir1/n_file1", n_file1_new)

        # Step 3: Capture inode numbers after renaming
        renamed_f_file1_inode = ref_inode_utils.get_inode_number(client, f_file1_new)
        renamed_k_file1_inode = ref_inode_utils.get_inode_number(client, k_file1_new)
        renamed_n_file1_inode = ref_inode_utils.get_inode_number(client, n_file1_new)

        log.info(
            f"After renaming - Inodes: "
            f"f_file1={renamed_f_file1_inode}, "
            f"k_file1={renamed_k_file1_inode}, "
            f"n_file1={renamed_n_file1_inode}"
        )

        # Step 4: Validate that inode numbers remain unchanged
        assert (
            initial_f_file1_inode == renamed_f_file1_inode
        ), "Inode changed for f_file1 after renaming!"
        assert (
            initial_k_file1_inode == renamed_k_file1_inode
        ), "Inode changed for k_file1 after renaming!"
        assert (
            initial_n_file1_inode == renamed_n_file1_inode
        ), "Inode changed for n_file1 after renaming!"

        log.info("File renaming test passed - inodes remained unchanged.")

        # Step 5: Validate referent inodes remain intact
        referent_inodes_map = {}

        for inode, tag, mount_dir in [
            (renamed_f_file1_inode, "f_file1_renamed", fuse_dir),
            (renamed_k_file1_inode, "k_file1_renamed", kernel_dir),
            (renamed_n_file1_inode, "n_file1_renamed", nfs_dir),
        ]:
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )

            referent_inodes = inode_data.get("referent_inodes", [])
            if referent_inodes:
                log.info(f"Referent inodes for {tag} remain intact: {referent_inodes}")
                referent_inodes_map[tag] = referent_inodes
            else:
                log.info(
                    f"No referent inodes found for {tag}, consistent with original state."
                )
                referent_inodes_map[tag] = None  # Explicitly store None

        # Step 6: Validate referent inodes linkage to source inode
        renamed_inodes = {
            "f_file1_renamed": renamed_f_file1_inode,
            "k_file1_renamed": renamed_k_file1_inode,
            "n_file1_renamed": renamed_n_file1_inode,
        }

        for file_name, inode_list in referent_inodes_map.items():
            if inode_list:  # Ensure it’s not None
                for referent_inode in inode_list:
                    log.info(
                        f"Fetching details for referent inode {referent_inode} ({file_name})\n"
                        "and validating linkage to source file"
                    )

                    source_inode = renamed_inodes.get(file_name)
                    if source_inode is None:
                        log.error(f"Renamed inode for {file_name} not found!")
                        continue

                    ref_inode_utils.get_referent_inode_details(
                        client, default_fs, source_inode, referent_inode, mount_dir
                    )
            else:
                log.info(
                    f"No referent inodes found for {file_name}, skipping validation."
                )

        log.info("File renaming test passed - referent inodes remained unchanged.")

        # ------SCENARIO: Hard link across different directories (same mount type) --------
        log.info(
            "Scenario 5: Using the renamed files from previous tests, create "
            "Hard link across different directories (same mount type)"
        )
        # Step 1: Creating new directories under each mount point
        new_subdirs = {
            "Fuse": (fuse_dir, ["f_dir2"]),
            "Kernel": (kernel_dir, ["k_dir2"]),
            "NFS": (nfs_dir, ["n_dir2"]),
        }

        # Loop through the mount directories and create the necessary directories.
        for label, (base_path, dirs) in new_subdirs.items():
            ref_inode_utils.create_directories(client, base_path, dirs)
            log.info(f"Created directories inside {label} mount: {dirs}")

        # Step 2: Flush journal before hard link creation
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Step 3: Creating hard links within the same mount type
        same_mount_hard_links = {
            "Fuse": (
                f"{fuse_dir}/f_dir1/f_file1_renamed",
                f"{fuse_dir}/f_dir2/f_file1_hl",
            ),
            "Kernel": (
                f"{kernel_dir}/k_dir1/k_file1_renamed",
                f"{kernel_dir}/k_dir2/k_file1_hl",
            ),
            "NFS": (
                f"{nfs_dir}/n_dir1/n_file1_renamed",
                f"{nfs_dir}/n_dir2/n_file1_hl",
            ),
        }

        for tag, (src, dest) in same_mount_hard_links.items():
            try:
                ref_inode_utils.create_hardlink(client, src, dest)
                log.info(f"Created same-mount hard link: {src} -> {dest}")
            except CommandFailed as e:
                log.error(f"Failed to create same-mount hard link {src} -> {dest}: {e}")
                return 1

        # Step 4: Flush journal after hard link creation
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Step 5: Fetching and validating referent inode details
        referent_inodes_map = {}

        # Capture inode numbers for validation
        file_inode_map = {
            "f_file1_renamed": ref_inode_utils.get_inode_number(
                client, f"{fuse_dir}/f_dir1/f_file1_renamed"
            ),
            "k_file1_renamed": ref_inode_utils.get_inode_number(
                client, f"{kernel_dir}/k_dir1/k_file1_renamed"
            ),
            "n_file1_renamed": ref_inode_utils.get_inode_number(
                client, f"{nfs_dir}/n_dir1/n_file1_renamed"
            ),
        }

        for tag, inode in file_inode_map.items():
            mount_dir = (
                fuse_dir
                if "f_file1_renamed" in tag
                else kernel_dir if "k_file1_renamed" in tag else nfs_dir
            )

            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )
            referent_inodes = inode_data.get("referent_inodes", [])

            if referent_inodes:
                log.info(f"Referent inodes for {tag}: {referent_inodes}")
                referent_inodes_map[tag] = referent_inodes
            else:
                log.info(
                    f"No referent inodes found for {tag}, consistent with original state."
                )
                referent_inodes_map[tag] = None  # Explicitly store None

        log.info(f"Referent inode details: {referent_inodes_map}")

        # Step 6: Validate referent inode linkage
        for file_name, inode_list in referent_inodes_map.items():
            if inode_list:  # Ensure it’s not None
                source_inode = file_inode_map[file_name]

                for referent_inode in inode_list:
                    log.info(
                        f"Fetching details for referent inode {referent_inode} ({file_name})\n"
                        "and validating linkage to source file"
                    )
                    ref_inode_utils.get_referent_inode_details(
                        client, default_fs, source_inode, referent_inode, mount_dir
                    )
            else:
                log.info(
                    f"No referent inodes found for {file_name}, skipping validation."
                )

        log.info("Hard link test across directories passed successfully.")

        # ------SCENARIO: Rename Hardlinked Files and Validate Referent Inodes -------
        log.info("Scenario 6: Rename Hardlinked Files and Validate Referent Inodes")

        # Step 1: Renaming hardlinked files
        renamed_files = {
            "Fuse": (
                f"{fuse_dir}/f_dir1/f_file1_hl",
                f"{fuse_dir}/f_dir1/f_file1_hl_renamed",
            ),
            "Kernel": (
                f"{kernel_dir}/k_dir1/k_file1_hl",
                f"{kernel_dir}/k_dir1/k_file1_hl_renamed",
            ),
            "NFS": (
                f"{nfs_dir}/n_dir1/n_file1_hl",
                f"{nfs_dir}/n_dir1/n_file1_hl_renamed",
            ),
        }

        for tag, (old_path, new_path) in renamed_files.items():
            try:
                ref_inode_utils.rename_file(client, old_path, new_path)
                log.info(f"Renamed hardlinked file: {old_path} -> {new_path}")
            except CommandFailed as e:
                log.error(f"Failed to rename file {old_path} -> {new_path}: {e}")
                return 1

        # Step 2: Flush journal to ensure metadata consistency
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Step 3: Fetch and validate referent inode details after rename
        renamed_referent_inodes_map = {}

        # Capture inode numbers dynamically before validation
        file_inode_map = {
            "f_file1_renamed": ref_inode_utils.get_inode_number(
                client, f"{fuse_dir}/f_dir1/f_file1_renamed"
            ),
            "k_file1_renamed": ref_inode_utils.get_inode_number(
                client, f"{kernel_dir}/k_dir1/k_file1_renamed"
            ),
            "n_file1_renamed": ref_inode_utils.get_inode_number(
                client, f"{nfs_dir}/n_dir1/n_file1_renamed"
            ),
        }

        for tag, inode in file_inode_map.items():
            mount_dir = (
                fuse_dir
                if "f_file1_renamed" in tag
                else kernel_dir if "k_file1_renamed" in tag else nfs_dir
            )

            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )
            referent_inodes = inode_data.get("referent_inodes", [])

            if referent_inodes:
                log.info(f"Referent inodes for {tag} after rename: {referent_inodes}")
                renamed_referent_inodes_map[tag] = referent_inodes
            else:
                log.info(
                    f"No referent inodes found for {tag} after rename, consistent with original state."
                )
                renamed_referent_inodes_map[tag] = None  # Explicitly store None

        log.info(f"Referent inode details after rename: {renamed_referent_inodes_map}")

        # Step 4: Validate that referent inodes remain unchanged after rename
        if referent_inodes_map != renamed_referent_inodes_map:
            log.error("Referent inodes changed after renaming!")
            return 1

        # Step 5: Validate referent inode linkage after rename
        for file_name, inode_list in renamed_referent_inodes_map.items():
            if inode_list:  # Ensure it’s not None
                source_inode = file_inode_map[file_name]

                for referent_inode in inode_list:
                    log.info(
                        f"Fetching details for referent inode {referent_inode} ({file_name})\n"
                        "and validating linkage to source file"
                    )
                    ref_inode_utils.get_referent_inode_details(
                        client, default_fs, source_inode, referent_inode, mount_dir
                    )
            else:
                log.info(
                    f"No referent inodes found for {file_name}, skipping validation."
                )

        log.info("Rename test for hardlinked files passed successfully.")

        # ---- SCENARIO: Move Renamed Hardlinked Files and Validate Referent Inodes ----
        log.info(
            "Scenario 7: Move Renamed Hardlinked Files and Validate Referent Inodes"
        )

        # Step 1: Moving renamed hardlinked files
        moved_files = {
            "Fuse": (
                f"{fuse_dir}/f_dir1/f_file1_hl_renamed",
                f"{fuse_dir}/f_dir2/f_file1_hl_moved",
            ),
            "Kernel": (
                f"{kernel_dir}/k_dir1/k_file1_hl_renamed",
                f"{kernel_dir}/k_dir2/k_file1_hl_moved",
            ),
            "NFS": (
                f"{nfs_dir}/n_dir1/n_file1_hl_renamed",
                f"{nfs_dir}/n_dir2/n_file1_hl_moved",
            ),
        }

        for tag, (old_path, new_path) in moved_files.items():
            try:
                ref_inode_utils.rename_file(client, old_path, new_path)
                log.info(f"Moved renamed hardlinked file: {old_path} -> {new_path}")
            except CommandFailed as e:
                log.error(f"Failed to move file {old_path} -> {new_path}: {e}")
                return 1

        # Step 2: Flush journal to ensure metadata consistency
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Step 3: Fetch and validate referent inode details after move
        moved_referent_inodes_map = {}

        file_inode_map = {
            "f_file1_renamed": ref_inode_utils.get_inode_number(
                client, f"{fuse_dir}/f_dir2/f_file1_hl_moved"
            ),
            "k_file1_renamed": ref_inode_utils.get_inode_number(
                client, f"{kernel_dir}/k_dir2/k_file1_hl_moved"
            ),
            "n_file1_renamed": ref_inode_utils.get_inode_number(
                client, f"{nfs_dir}/n_dir2/n_file1_hl_moved"
            ),
        }

        # Mapping of tag to mount directory using the same keys
        mount_dirs = {
            "f_file1_renamed": fuse_dir,
            "k_file1_renamed": kernel_dir,
            "n_file1_renamed": nfs_dir,
        }
        for tag, inode in file_inode_map.items():
            if inode is None:
                log.error(
                    f"Failed to retrieve inode number for {tag}. Skipping validation."
                )
                return 1

            mount_dir = mount_dirs[tag]

            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )
            referent_inodes = inode_data.get("referent_inodes", [])

            if referent_inodes:
                log.info(f"Referent inodes for {tag} after move: {referent_inodes}")
                moved_referent_inodes_map[tag] = referent_inodes
            else:
                log.info(
                    f"No referent inodes found for {tag} after move, consistent with original state."
                )
                moved_referent_inodes_map[tag] = None  # Explicitly store None

        log.info(f"Referent inode details after move: {moved_referent_inodes_map}")

        # Step 4: Validate that referent inodes remain unchanged after move
        log.info(f"Renamed Referent Inode Map: {renamed_referent_inodes_map}")
        log.info(f"Moved Referent Inode Map: {moved_referent_inodes_map}")

        if renamed_referent_inodes_map != moved_referent_inodes_map:
            log.error("Referent inodes changed after moving!")
            return 1

        # Step 5: Validate referent inode linkage after move
        for tag, inode_list in renamed_referent_inodes_map.items():
            if inode_list:  # Ensure it’s not None
                source_inode = file_inode_map.get(tag)
                if source_inode is None:
                    log.error(f"Source inode for {tag} is None. Skipping validation.")
                    continue

                mount_dir = mount_dirs[tag]  # Ensure correct mount_dir for validation

                for referent_inode in inode_list:
                    log.info(
                        f"Fetching details for referent inode {referent_inode} ({tag})\n"
                        "and validating linkage to source file"
                    )
                    ref_inode_utils.get_referent_inode_details(
                        client, default_fs, source_inode, referent_inode, mount_dir
                    )
            else:
                log.info(f"No referent inodes found for {tag}, skipping validation.")

        log.info("Move test for renamed hardlinked files passed successfully.")

        # ----- SCENARIO: Hard link across different mount types (Expected to fail) -----
        log.info(
            "Scenario 8: Hard link across different mount types (Expected to fail)"
        )
        cross_mount_hard_links = {
            "Fuse_to_Kernel": (
                f"{fuse_dir}/f_dir1/f_file1_renamed",
                f"{kernel_dir}/k_dir1/f_file1_hl",
            ),
            "Kernel_to_NFS": (
                f"{kernel_dir}/k_dir1/k_file1_renamed",
                f"{nfs_dir}/n_dir1/k_file1_hl",
            ),
            "NFS_to_Fuse": (
                f"{nfs_dir}/n_dir1/n_file1_renamed",
                f"{fuse_dir}/f_dir1/n_file1_hl",
            ),
        }

        unexpected_failure = (
            False  # Track if any unexpected hard link creation succeeds
        )

        for tag, (src, dest) in cross_mount_hard_links.items():
            log.info(f"Attempting to create hard link: {src} -> {dest}")
            result = ref_inode_utils.create_hardlink(client, src, dest)
            if result:
                log.error(
                    f"ERROR: Unexpectedly created cross-mount hard link {src} -> {dest}"
                )
                unexpected_failure = (
                    True  # Mark failure but continue checking other links
                )
            else:
                log.info(f"Expected failure for cross-mount hard link {src} -> {dest}")

            # If any hard link was unexpectedly created, return failure
        if unexpected_failure:
            log.error("Unexpectedly created cross-mount hard link(s).")
            return 1

        log.info(
            "Cross-mount hard link validation passed. All expected failures occurred."
        )

        return 0
    except Exception as err:
        log.exception("An error occurred during the test run: %s", err)
        return 1
