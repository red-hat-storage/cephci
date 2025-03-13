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

        # ---------------- Scenario 1: Check hard links and referent inodes ----------------
        # Dictionary to store referent inodes
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

        # Scenario 2: Validate referent inodes when a file is hardlinked within the same directory --
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

        for file_name, inode_list in referent_inodes_map.items():
            if inode_list:  # Ensure it’s not None
                for referent_inode in inode_list:
                    log.info(
                        f"Fetching details for referent inode {referent_inode} ({file_name})\n"
                        "and validating linkage to source file"
                    )

                    # Fetch the original source inode for validation
                    source_inode = eval(
                        f"{file_name}_inode"
                    )  # Dynamically fetch inode (e.g., f_file1_inode)

                    ref_inode_utils.get_referent_inode_details(
                        client, default_fs, source_inode, referent_inode, mount_dir
                    )
            else:
                log.info(
                    f"No referent inodes found for {file_name}, skipping validation."
                )

        # ---------------- SCENARIO 3: Hard link across different directories (same mount type) ----------------
        # Creating new directories under each mount point
        new_subdirs = {
            "Fuse": (fuse_dir, ["f_dir2"]),
            "Kernel": (kernel_dir, ["k_dir2"]),
            "NFS": (nfs_dir, ["n_dir2"]),
        }
        # Loop through the mount directories and create the necessary directories.
        for label, (base_path, dirs) in new_subdirs.items():
            ref_inode_utils.create_directories(client, base_path, dirs)
            log.info(f"Created directories inside {label} mount: {dirs}")

        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Attempting to create hard links within the same mount type
        same_mount_hard_links = {
            "Fuse": (f"{fuse_dir}/f_dir1/f_file1", f"{fuse_dir}/f_dir2/f_file1_hl"),
            "Kernel": (
                f"{kernel_dir}/k_dir1/k_file1",
                f"{kernel_dir}/k_dir2/k_file1_hl",
            ),
            "NFS": (f"{nfs_dir}/n_dir1/n_file1", f"{nfs_dir}/n_dir2/n_file1_hl"),
        }

        for tag, (src, dest) in same_mount_hard_links.items():
            try:
                ref_inode_utils.create_hardlink(client, src, dest)
                log.info(f"Created same-mount hard link: {src} -> {dest}")
            except CommandFailed as e:
                log.error(f"Failed to create same-mount hard link {src} -> {dest}: {e}")
                return 1

        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)

        # Fetching and validating referent inode details
        referent_inodes_map = {}

        for inode, tag, mount_dir in [
            (f_file1_inode, "f_file1", fuse_dir),
            (k_file1_inode, "k_file1", kernel_dir),
            (n_file1_inode, "n_file1", nfs_dir),
        ]:
            inode_data = ref_inode_utils.get_inode_details(
                client, default_fs, inode, mount_dir
            )

            referent_inodes = inode_data.get("referent_inodes", [])
            if referent_inodes:
                log.info(f"Referent inodes for {tag}: {referent_inodes}")
                referent_inodes_map[tag] = referent_inodes

        log.info(f"Referent inode details: {referent_inodes_map}")

        for file_name, inode_list in referent_inodes_map.items():
            if inode_list:
                for referent_inode in inode_list:
                    log.info(
                        f"Fetching details for referent inode {referent_inode} ({file_name})\n"
                        "and validating linkage to source file"
                    )

                    # Fetch the original source inode for validation
                    source_inode = eval(
                        f"{file_name}_inode"
                    )  # Dynamically fetch inode (e.g., f_file1_inode)

                    ref_inode_utils.get_referent_inode_details(
                        client, default_fs, source_inode, referent_inode, mount_dir
                    )
            else:
                log.info(
                    f"No referent inodes found for {file_name}, skipping validation."
                )

        # ---------------- SCENARIO 4: Hard link across different mount types (Expected to fail) ----------------

        cross_mount_hard_links = {
            "Fuse_to_Kernel": (
                f"{fuse_dir}/f_dir1/f_file1",
                f"{kernel_dir}/k_dir1/f_file1_hl",
            ),
            "Kernel_to_NFS": (
                f"{kernel_dir}/k_dir1/k_file1",
                f"{nfs_dir}/n_dir1/k_file1_hl",
            ),
            "NFS_to_Fuse": (
                f"{nfs_dir}/n_dir1/n_file1",
                f"{fuse_dir}/f_dir1/n_file1_hl",
            ),
        }

        unexpected_failure = False  # Track if any unexpected issue occurs
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

        return 0
    except Exception as err:
        log.exception("An error occurred during the test run: %s", err)
        return 1
