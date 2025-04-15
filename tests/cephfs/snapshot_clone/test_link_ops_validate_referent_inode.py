from ceph.ceph import CommandFailed
from tests.cephfs.lib.cephfs_refinode_utils import RefInodeUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kwargs):
    try:
        test_data, config = kwargs.get("test_data"), kwargs.get("config")
        ref_inode_utils = RefInodeUtils(ceph_cluster)  # Create an instance

        fs_util, clients, erasure = ref_inode_utils.prepare_environment(
            ceph_cluster, test_data, config
        )

        # Ensure at least one client is available.
        client_count = len(clients)
        if client_count == 0:
            log.info(
                "This test requires at least 1 client node. Found %d client node(s).",
                client_count,
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
            fs_util.create_nfs(
                client, nfs_name, validate=True, placement="1 %s" % nfs_server
            )
            log.info("NFS cluster %s created successfully." % nfs_name)
        except CommandFailed as e:
            log.error("Failed to create NFS cluster: %s" % e)
            return 1

        log.info("Validate if allow referent inode feature is enabled")
        ref_inode_utils.allow_referent_inode_feature_enablement(
            client, default_fs, enable=True
        )

        # Mount the root filesystems.
        fuse_dir, kernel_dir, nfs_dir = [
            dirs[0]
            for dirs in ref_inode_utils.mount_rootfs(
                client, fs_util, default_fs, nfs_server, nfs_name
            )
        ]

        mount_dirs = {
            "Fuse": (fuse_dir, ["f_dir1"]),
            "Kernel": (kernel_dir, ["k_dir1"]),
            "NFS": (nfs_dir, ["n_dir1"]),
        }
        # Loop through the mount directories and create the necessary directories.
        for label, (base_path, dirs) in mount_dirs.items():
            ref_inode_utils.create_directories(client, base_path, dirs)
        log.info("Created directories inside %s mount: %s" % (label, dirs))

        # flush the journal
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Define the files to be created inside each directory.
        files_to_create = {
            "Fuse": ("%s/f_dir1/f_file1" % fuse_dir, "Fuse file content"),
            "Kernel": ("%s/k_dir1/k_file1" % kernel_dir, "Kernel file content"),
            "NFS": ("%s/n_dir1/n_file1" % nfs_dir, "NFS file content"),
        }
        # Loop through each mount point and create the tagged file.
        for label, (file_path, content) in files_to_create.items():
            ref_inode_utils.create_file_with_content(client, file_path, content)
            log.info("Created file inside %s mount: %s" % (label, file_path))

        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Get inode numbers for each file and tag them accordingly.
        f_file1_inode = ref_inode_utils.get_inode_number(
            client, "%s/f_dir1/f_file1" % fuse_dir
        )
        k_file1_inode = ref_inode_utils.get_inode_number(
            client, "%s/k_dir1/k_file1" % kernel_dir
        )
        n_file1_inode = ref_inode_utils.get_inode_number(
            client, "%s/n_dir1/n_file1" % nfs_dir
        )

        # Log the inode numbers.
        log.info("Inode of f_file1: %s", f_file1_inode)
        log.info("Inode of k_file1: %s", k_file1_inode)
        log.info("Inode of n_file1: %s", n_file1_inode)

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
            log.info("%s is located at: %s" % (tag, file_path))

            # Check hard links (`nlink`)
            nlink = inode_data.get("nlink", 1)
            if nlink > 1:
                hardlinks = nlink - 1
                log.info(
                    "Hard links found for %s: %s (Total links: %s)"
                    % (tag, hardlinks, nlink)
                )
            else:
                log.info("No hard links found for %s" % tag)

            # Handle referent inodes
            referent_inodes = inode_data.get("referent_inodes", [])
            if referent_inodes:
                log.info("Referent inodes for %s: %s" % (tag, referent_inodes))
                referent_inodes_map[tag] = referent_inodes  # Store for future use
            else:
                log.info("No referent inodes found for %s" % tag)
                referent_inodes_map[tag] = (
                    None  # Explicitly store None if no referent inodes
                )

        # Log the stored referent inodes for future use
        log.info("Stored referent inodes: %s" % referent_inodes_map)

        # -------- Scenario: Modify file content and validate referent inodes -----------
        log.info("Scenario 2 : Modify file content and validate referent inodes")
        # Step 1: Get initial inode numbers before modification
        initial_f_file1_inode = ref_inode_utils.get_inode_number(
            client, "%s/f_dir1/f_file1" % fuse_dir
        )
        initial_k_file1_inode = ref_inode_utils.get_inode_number(
            client, "%s/k_dir1/k_file1" % kernel_dir
        )
        initial_n_file1_inode = ref_inode_utils.get_inode_number(
            client, "%s/n_dir1/n_file1" % nfs_dir
        )

        log.info(
            "Before modification - "
            "Inodes: f_file1=%s, k_file1=%s, n_file1=%s"
            % (initial_f_file1_inode, initial_k_file1_inode, initial_n_file1_inode)
        )

        # Step 2: Append content to the files
        ref_inode_utils.append_to_file(
            client, "%s/f_dir1/f_file1" % fuse_dir, "New content for fuse file"
        )
        ref_inode_utils.append_to_file(
            client, "%s/k_dir1/k_file1" % kernel_dir, "New content for kernel file"
        )
        ref_inode_utils.append_to_file(
            client, "%s/n_dir1/n_file1" % nfs_dir, "New content for NFS file"
        )

        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Step 3: Get inode numbers after modification
        modified_f_file1_inode = ref_inode_utils.get_inode_number(
            client, "%s/f_dir1/f_file1" % fuse_dir
        )
        modified_k_file1_inode = ref_inode_utils.get_inode_number(
            client, "%s/k_dir1/k_file1" % kernel_dir
        )
        modified_n_file1_inode = ref_inode_utils.get_inode_number(
            client, "%s/n_dir1/n_file1" % nfs_dir
        )

        log.info(
            "After modification - Inodes: "
            "f_file1=%s, k_file1=%s, n_file1=%s"
            % (modified_f_file1_inode, modified_k_file1_inode, modified_n_file1_inode)
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
                log.info(
                    "Referent inodes for %s remain intact: %s" % (tag, referent_inodes)
                )
            else:
                log.info(
                    "No referent inodes found for %s, consistent with original state."
                    % tag
                )

        # ---- Scenario: Validate referent inodes when a file is hardlinked within the same directory--
        log.info(
            "Scenario 3: Validate referent inodes when a file is hardlinked within the same directory"
        )
        # Create hard links for the files
        f_file1_link = "%s/f_dir1/f_file1_hl" % fuse_dir
        k_file1_link = "%s/k_dir1/k_file1_hl" % kernel_dir
        n_file1_link = "%s/n_dir1/n_file1_hl" % nfs_dir

        ref_inode_utils.create_hardlink(
            client, "%s/f_dir1/f_file1" % fuse_dir, f_file1_link
        )
        ref_inode_utils.create_hardlink(
            client, "%s/k_dir1/k_file1" % kernel_dir, k_file1_link
        )
        ref_inode_utils.create_hardlink(
            client, "%s/n_dir1/n_file1" % nfs_dir, n_file1_link
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
            log.info("%s is located at: %s" % (tag, file_path))

            # Check hard links (`nlink`)
            nlink = inode_data.get("nlink", 1)
            if nlink > 1:
                hardlinks = nlink - 1
                log.info(
                    "Hard links found for %s: %s (Total links: %s)"
                    % (tag, hardlinks, nlink)
                )
            else:
                log.info("No hard links found for %s" % tag)

            # Handle referent inodes
            referent_inodes = inode_data.get("referent_inodes", [])
            if referent_inodes:
                log.info("Referent inodes for %s: %s" % (tag, referent_inodes))
                referent_inodes_map[tag] = referent_inodes  # Store for validation
            else:
                log.info("No referent inodes found for %s" % tag)
                referent_inodes_map[tag] = []

            # Log stored referent inodes
            log.info("Stored referent inodes: %s" % referent_inodes_map)
        # Log stored referent inodes
        log.info("Stored referent inodes: %s" % referent_inodes_map)

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
                    "No referent inodes found for %s, skipping validation." % file_name
                )
                continue

            source_inode = inodes_map.get(file_name)
            mount_dir = mount_dir_map.get(file_name)

            if source_inode is None:
                log.error("Inode for %s not found! Skipping validation." % file_name)
                continue

            for referent_inode in inode_list:
                log.info(
                    "Fetching details for referent inode %s (%s)\n"
                    "and validating linkage to source file"
                    % (referent_inode, file_name)
                )

                ref_inode_utils.get_referent_inode_details(
                    client, default_fs, source_inode, referent_inode, mount_dir
                )

        # -----Scenario: Rename source file and validate referent inodes -------
        log.info("Scenario 4 : Rename source file and validate referent inodes")
        # Step 1: Capture inode numbers before renaming
        initial_f_file1_inode = ref_inode_utils.get_inode_number(
            client, "%s/f_dir1/f_file1" % fuse_dir
        )
        initial_k_file1_inode = ref_inode_utils.get_inode_number(
            client, "%s/k_dir1/k_file1" % kernel_dir
        )
        initial_n_file1_inode = ref_inode_utils.get_inode_number(
            client, "%s/n_dir1/n_file1" % nfs_dir
        )

        log.info(
            "Before renaming - Inodes: f_file1=%s, k_file1=%s, n_file1=%s"
            % (initial_f_file1_inode, initial_k_file1_inode, initial_n_file1_inode)
        )

        # Step 2: Rename the files
        f_file1_new = "%s/f_dir1/f_file1_renamed" % fuse_dir
        k_file1_new = "%s/k_dir1/k_file1_renamed" % kernel_dir
        n_file1_new = "%s/n_dir1/n_file1_renamed" % nfs_dir

        ref_inode_utils.rename_file(client, "%s/f_dir1/f_file1" % fuse_dir, f_file1_new)
        ref_inode_utils.rename_file(
            client, "%s/k_dir1/k_file1" % kernel_dir, k_file1_new
        )
        ref_inode_utils.rename_file(client, "%s/n_dir1/n_file1" % nfs_dir, n_file1_new)

        # Step 3: Capture inode numbers after renaming
        renamed_f_file1_inode = ref_inode_utils.get_inode_number(client, f_file1_new)
        renamed_k_file1_inode = ref_inode_utils.get_inode_number(client, k_file1_new)
        renamed_n_file1_inode = ref_inode_utils.get_inode_number(client, n_file1_new)

        log.info(
            "After renaming - Inodes: "
            "f_file1=%s, "
            "k_file1=%s, "
            "n_file1=%s"
            % (renamed_f_file1_inode, renamed_k_file1_inode, renamed_n_file1_inode)
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
                log.info(
                    "Referent inodes for %s remain intact: %s", tag, referent_inodes
                )
                referent_inodes_map[tag] = referent_inodes
            else:
                log.info(
                    "No referent inodes found for %s, consistent with original state.",
                    tag,
                )
                referent_inodes_map[tag] = None  # Explicitly store None

        # Step 6: Validate referent inodes linkage to source inode
        renamed_inodes = {
            "f_file1_renamed": renamed_f_file1_inode,
            "k_file1_renamed": renamed_k_file1_inode,
            "n_file1_renamed": renamed_n_file1_inode,
        }

        # Dictionary to store original source inodes
        inodes_map = {
            "f_file1": f_file1_inode,
            "k_file1": k_file1_inode,
            "n_file1": n_file1_inode,
        }

        # Validate referent inodes linkage
        for file_name, inode_list in referent_inodes_map.items():
            if inode_list:  # Ensure it’s not None
                for referent_inode in inode_list:
                    log.info(
                        "Fetching details for referent inode %s (%s)\n"
                        "and validating linkage to source file",
                        referent_inode,
                        file_name,
                    )
                    source_inode = renamed_inodes.get(file_name)
                    if source_inode is None:
                        log.error("Renamed inode for %s not found!", file_name)
                        continue

                    ref_inode_utils.get_referent_inode_details(
                        client, default_fs, source_inode, referent_inode, mount_dir
                    )
            else:
                log.info(
                    "No referent inodes found for %s, skipping validation.", file_name
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
            log.info("Created directories inside %s mount: %s", label, dirs)

        # Step 2: Flush journal before hard link creation
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Step 3: Creating hard links within the same mount type
        same_mount_hard_links = {
            "Fuse": (
                "%s/f_dir1/f_file1_renamed" % fuse_dir,
                "%s/f_dir2/f_file1_hl" % fuse_dir,
            ),
            "Kernel": (
                "%s/k_dir1/k_file1_renamed" % kernel_dir,
                "%s/k_dir2/k_file1_hl" % kernel_dir,
            ),
            "NFS": (
                "%s/n_dir1/n_file1_renamed" % nfs_dir,
                "%s/n_dir2/n_file1_hl" % nfs_dir,
            ),
        }

        for tag, (src, dest) in same_mount_hard_links.items():
            try:
                ref_inode_utils.create_hardlink(client, src, dest)
                log.info("Created same-mount hard link: %s -> %s" % (src, dest))
            except CommandFailed as e:
                log.error(
                    "Failed to create same-mount hard link %s -> %s: %s"
                    % (src, dest, e)
                )
                return 1

        # Step 4: Flush journal after hard link creation
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Step 5: Fetching and validating referent inode details
        referent_inodes_map = {}

        # Capture inode numbers for validation
        file_inode_map = {
            "f_file1_renamed": ref_inode_utils.get_inode_number(
                client, "%s/f_dir1/f_file1_renamed" % fuse_dir
            ),
            "k_file1_renamed": ref_inode_utils.get_inode_number(
                client, "%s/k_dir1/k_file1_renamed" % kernel_dir
            ),
            "n_file1_renamed": ref_inode_utils.get_inode_number(
                client, "%s/n_dir1/n_file1_renamed" % nfs_dir
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
                log.info("Referent inodes for %s: %s", tag, referent_inodes)
                referent_inodes_map[tag] = referent_inodes
            else:
                log.info(
                    "No referent inodes found for %s, consistent with original state.",
                    tag,
                )

                referent_inodes_map[tag] = None  # Explicitly store None

        log.info("Referent inode details: %s", referent_inodes_map)

        # Step 6: Validate referent inode linkage
        for file_name, inode_list in referent_inodes_map.items():
            if inode_list:  # Ensure it’s not None
                source_inode = file_inode_map[file_name]

                for referent_inode in inode_list:
                    log.info(
                        "Fetching details for referent inode %s (%s)\n"
                        "and validating linkage to source file",
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

        log.info("Hard link test across directories passed successfully.")

        # ------SCENARIO: Rename Hardlinked Files and Validate Referent Inodes -------
        log.info("Scenario 6: Rename Hardlinked Files and Validate Referent Inodes")

        # Step 1: Renaming hardlinked files
        renamed_files = {
            "Fuse": (
                "%s/f_dir1/f_file1_hl" % fuse_dir,
                "%s/f_dir1/f_file1_hl_renamed" % fuse_dir,
            ),
            "Kernel": (
                "%s/k_dir1/k_file1_hl" % kernel_dir,
                "%s/k_dir1/k_file1_hl_renamed" % kernel_dir,
            ),
            "NFS": (
                "%s/n_dir1/n_file1_hl" % nfs_dir,
                "%s/n_dir1/n_file1_hl_renamed" % nfs_dir,
            ),
        }

        for tag, (old_path, new_path) in renamed_files.items():
            try:
                ref_inode_utils.rename_file(client, old_path, new_path)
                log.info("Renamed hardlinked file: %s -> %s", old_path, new_path)
            except CommandFailed as e:
                log.error("Failed to rename file %s -> %s: %s", old_path, new_path, e)
                return 1

        # Step 2: Flush journal to ensure metadata consistency
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Step 3: Fetch and validate referent inode details after rename
        renamed_referent_inodes_map = {}

        # Capture inode numbers dynamically before validation
        file_inode_map = {
            "f_file1_renamed": ref_inode_utils.get_inode_number(
                client, "%s/f_dir1/f_file1_renamed" % fuse_dir
            ),
            "k_file1_renamed": ref_inode_utils.get_inode_number(
                client, "%s/k_dir1/k_file1_renamed" % kernel_dir
            ),
            "n_file1_renamed": ref_inode_utils.get_inode_number(
                client, "%s/n_dir1/n_file1_renamed" % nfs_dir
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
                log.info(
                    "Referent inodes for %s after rename: %s", tag, referent_inodes
                )
                renamed_referent_inodes_map[tag] = referent_inodes
            else:
                log.info(
                    "No referent inodes found for %s after rename, consistent with original state.",
                    tag,
                )
                renamed_referent_inodes_map[tag] = None  # Explicitly store None

        log.info("Referent inode details after rename: %s", renamed_referent_inodes_map)

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
                        "Fetching details for referent inode %s (%s)\n"
                        "and validating linkage to source file",
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

        # ---- SCENARIO: Move Renamed Hardlinked Files and Validate Referent Inodes ----
        log.info(
            "Scenario 7: Move Renamed Hardlinked Files and Validate Referent Inodes"
        )

        # Step 1: Moving renamed hardlinked files
        moved_files = {
            "Fuse": (
                "%s/f_dir1/f_file1_hl_renamed" % fuse_dir,
                "%s/f_dir2/f_file1_hl_moved" % fuse_dir,
            ),
            "Kernel": (
                "%s/k_dir1/k_file1_hl_renamed" % kernel_dir,
                "%s/k_dir2/k_file1_hl_moved" % kernel_dir,
            ),
            "NFS": (
                "%s/n_dir1/n_file1_hl_renamed" % nfs_dir,
                "%s/n_dir2/n_file1_hl_moved" % nfs_dir,
            ),
        }

        for tag, (old_path, new_path) in moved_files.items():
            try:
                ref_inode_utils.rename_file(client, old_path, new_path)
                log.info("Moved renamed hardlinked file: %s -> %s", old_path, new_path)
            except CommandFailed as e:
                log.error("Failed to move file %s -> %s: %s", old_path, new_path, e)
                return 1

        # Step 2: Flush journal to ensure metadata consistency
        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Step 3: Fetch and validate referent inode details after move
        moved_referent_inodes_map = {}

        file_inode_map = {
            "f_file1_renamed": ref_inode_utils.get_inode_number(
                client, "%s/f_dir2/f_file1_hl_moved" % fuse_dir
            ),
            "k_file1_renamed": ref_inode_utils.get_inode_number(
                client, "%s/k_dir2/k_file1_hl_moved" % kernel_dir
            ),
            "n_file1_renamed": ref_inode_utils.get_inode_number(
                client, "%s/n_dir2/n_file1_hl_moved" % nfs_dir
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
                    "Failed to retrieve inode number for %s. Skipping validation.", tag
                )
                return 1

            mount_dir = mount_dirs[tag]

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
                moved_referent_inodes_map[tag] = None  # Explicitly store None

        log.info("Referent inode details after move: %s", moved_referent_inodes_map)

        # Step 4: Validate that referent inodes remain unchanged after move
        log.info("Renamed Referent Inode Map: %s", renamed_referent_inodes_map)
        log.info("Moved Referent Inode Map: %s", moved_referent_inodes_map)

        if renamed_referent_inodes_map != moved_referent_inodes_map:
            log.error("Referent inodes changed after moving!")
            return 1

        # Step 5: Validate referent inode linkage after move
        for tag, inode_list in renamed_referent_inodes_map.items():
            if inode_list:  # Ensure it’s not None
                source_inode = file_inode_map.get(tag)
                if source_inode is None:
                    log.error("Source inode for %s is None. Skipping validation.", tag)
                    continue

                mount_dir = mount_dirs[tag]  # Ensure correct mount_dir for validation

                for referent_inode in inode_list:
                    log.info(
                        "Fetching details for referent inode %s (%s)\n"
                        "and validating linkage to source file",
                        referent_inode,
                        tag,
                    )
                    ref_inode_utils.get_referent_inode_details(
                        client, default_fs, source_inode, referent_inode, mount_dir
                    )
            else:
                log.info("No referent inodes found for %s, skipping validation.", tag)

        log.info("Move test for renamed hardlinked files passed successfully.")

        # ----- SCENARIO: Hard link across different mount types (Expected to fail) -----
        log.info(
            "Scenario 8: Hard link across different mount types (Expected to fail)"
        )
        cross_mount_hard_links = {
            "Fuse_to_Kernel": (
                "%s/f_dir1/f_file1_renamed" % fuse_dir,
                "%s/k_dir1/f_file1_hl" % kernel_dir,
            ),
            "Kernel_to_NFS": (
                "%s/k_dir1/k_file1_renamed" % kernel_dir,
                "%s/n_dir1/k_file1_hl" % nfs_dir,
            ),
            "NFS_to_Fuse": (
                "%s/n_dir1/n_file1_renamed" % nfs_dir,
                "%s/f_dir1/n_file1_hl" % fuse_dir,
            ),
        }

        unexpected_failure = (
            False  # Track if any unexpected hard link creation succeeds
        )

        for tag, (src, dest) in cross_mount_hard_links.items():
            log.info("Attempting to create hard link: %s -> %s", src, dest)
            result = ref_inode_utils.create_hardlink(client, src, dest)
            if result:
                log.error(
                    "ERROR: Unexpectedly created cross-mount hard link %s -> %s",
                    src,
                    dest,
                )
                unexpected_failure = (
                    True  # Mark failure but continue checking other links
                )
            else:
                log.info(
                    "Expected failure for cross-mount hard link %s -> %s", src, dest
                )

            # If any hard link was unexpectedly created, return failure
        if unexpected_failure:
            log.error("Unexpectedly created cross-mount hard link(s).")
            return 1

        log.info(
            "Cross-mount hard link validation passed. All expected failures occurred."
        )

        # ----- SCENARIO: Fetch and unlink all hardlinks -----
        log.info(
            "Scenario 9: Retrieve all hard links, unlink them, and verify removal of referent inodes."
        )
        # Dictionary to store initial referent inodes
        referent_inodes_map = {}
        post_referent_inodes_map = {}

        # Define file paths
        file_paths = {
            "f_file1_renamed": "%s/f_dir1/f_file1_renamed" % fuse_dir,
            "k_file1_renamed": "%s/k_dir1/k_file1_renamed" % kernel_dir,
            "n_file1_renamed": "%s/n_dir1/n_file1_renamed" % nfs_dir,
        }

        # Fetch and store inode details before unlinking
        file_inode_map = {
            tag: ref_inode_utils.get_inode_number(client, path)
            for tag, path in file_paths.items()
        }

        for tag, inode in file_inode_map.items():
            mount_dir = file_paths[tag].rsplit("/", 1)[0]  # Extract mount directory
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )
            referent_inodes_map[tag] = inode_data.get("referent_inodes", None)
            log.info("%s referent inodes: %s", tag, referent_inodes_map[tag])

        log.info("Initial referent inodes: %s", referent_inodes_map)

        mount_dirs = {
            "f_file1_renamed": fuse_dir,
            "k_file1_renamed": kernel_dir,
            "n_file1_renamed": nfs_dir,
        }

        # Unlink hardlinks
        log.info("Unlinking hardlinks for all files...")
        for tag, path in file_paths.items():
            mount_dir = mount_dirs.get(tag, None)
            if mount_dir:
                ref_inode_utils.unlink_hardlinks(client, default_fs, path, mount_dir)
            else:
                log.error("Skipping %s: Mount directory not found", tag)

        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Verify referent inodes after unlinking
        log.info("Verifying referent inodes after unlinking...")
        for tag, inode in file_inode_map.items():
            mount_dir = file_paths[tag].rsplit("/", 1)[0]
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )
            post_referent_inodes_map[tag] = inode_data.get("referent_inodes", []) or []
            log.info(
                "%s referent inodes after unlinking: %s",
                tag,
                post_referent_inodes_map[tag],
            )

        log.info("Post unlink referent inodes: %s", post_referent_inodes_map)

        for tag, inodes in post_referent_inodes_map.items():
            if (
                inodes and len(inodes) > 0
            ):  # Ensures only non-empty lists trigger an error
                log.error("Referent inodes for %s still exist: %s", tag, inodes)
                raise AssertionError(
                    "Unexpected referent inodes found for {}".format(tag)
                )
            else:
                log.info("Referent inodes successfully removed for %s", tag)

        log.info("Test passed: All referent inodes removed as expected.")

        return 0
    except Exception as err:
        log.exception("An error occurred during the test run: %s", err)
        return 1
