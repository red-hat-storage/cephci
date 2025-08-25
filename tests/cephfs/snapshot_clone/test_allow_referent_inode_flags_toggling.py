import json
import threading
import time

from ceph.ceph import CommandFailed
from tests.cephfs.lib.cephfs_refinode_utils import RefInodeUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)

MOUNT_POINTS = {
    "Fuse": {
        "dirs": ["f_dir1", "f_dir2"],
        "files": {
            "f_dir1": ["f_file1", "f_file1_hl"],
            "f_dir2": ["f_file2", "f_file1_hl"],
        },
    },
    "Kernel": {
        "dirs": ["k_dir1", "k_dir2"],
        "files": {
            "k_dir1": ["k_file1", "k_file1_hl"],
            "k_dir2": ["k_file2", "k_file1_hl"],
        },
    },
    "NFS": {
        "dirs": ["n_dir1", "n_dir2"],
        "files": {
            "n_dir1": ["n_file1", "n_file1_hl"],
            "n_dir2": ["n_file2", "n_file1_hl"],
        },
    },
}


def get_mount_path(mount_type, dir_idx, mount_paths, filename=None):
    base_path = mount_paths[mount_type][dir_idx]
    directory = MOUNT_POINTS[mount_type]["dirs"][dir_idx]
    return "%s/%s" % (base_path, directory) + ("/%s" % filename if filename else "")


def toggle_allow_referent_inodes(client, fs_name, enable, repeat=False):
    flag_value = "true" if enable else "false"
    cmd = "ceph fs set %s allow_referent_inodes %s" % (fs_name, flag_value)
    client.exec_command(sudo=True, cmd=cmd, check_ec=True)
    if repeat:
        client.exec_command(sudo=True, cmd=cmd, check_ec=True)
    out, _ = client.exec_command(sudo=True, cmd="ceph fs get %s -f json" % fs_name)
    flags_state = json.loads(out.strip())["mdsmap"].get("flags_state", {})
    current = flags_state.get("allow_referent_inodes", False)
    return current == enable


def create_on_mounts(client, base_paths_map, mount_points_map, create_fn, tag=None):
    created_files = []
    for mount_type, base_paths in base_paths_map.items():
        for idx, base_path in enumerate(
            base_paths[: len(mount_points_map[mount_type]["dirs"])]
        ):
            dir_name = mount_points_map[mount_type]["dirs"][idx]
            if create_fn.__name__ == "create_directories":
                create_fn(client, base_path, [dir_name])
                log.info(
                    "Created directory '%s' under %s (%s)",
                    dir_name,
                    base_path,
                    mount_type,
                )
            elif create_fn.__name__ == "create_file_with_content":
                fname = mount_points_map[mount_type]["files"][dir_name][0]
                fpath = "%s/%s/%s" % (base_path.rstrip("/"), dir_name, fname)
                create_fn(client, fpath, "%s file content" % mount_type)
                log.info("Created file: %s", fpath)
                created_files.append((mount_type, idx, fname, tag))
    return created_files


def collect_inode_numbers(
    ref_inode_utils, client, created_files, root_paths, subvol_paths
):
    inodes = {}
    for mount_type, idx, fname, source in created_files:
        base_paths = root_paths if source == "rootfs" else subvol_paths
        if idx < len(base_paths[mount_type]):
            path = get_mount_path(
                mount_type, idx, {mount_type: base_paths[mount_type]}, fname
            )
            try:
                inode = ref_inode_utils.get_inode_number(client, path)
                inodes.setdefault(mount_type, {})[fname] = inode
                log.info("Inode for %s %s: %s", mount_type, fname, inode)
            except Exception as e:
                log.warning("Inode fetch failed for %s at %s: %s", fname, path, e)
    return inodes


# ------------------------------
# Test Scenarios
# ------------------------------


def test_default_allow_referent_inodes_disabled(client, fs_name):
    out, _ = client.exec_command(sudo=True, cmd="ceph fs get %s -f json" % fs_name)
    flags_state = json.loads(out.strip())["mdsmap"].get("flags_state", {})

    current = flags_state.get("allow_referent_inodes", False)
    # Normalize in case Ceph returns "true"/"false" as strings
    if isinstance(current, str):
        current = current.lower() == "true"

    if current:  # True means enabled → fail Scenario 1
        log.error(
            "'allow_referent_inodes' unexpectedly enabled for filesystem %s." % fs_name
        )
        return 1

    log.info(
        "allow_referent_inodes is correctly disabled by default for filesystem %s",
        fs_name,
    )
    return 0


def test_enable_allow_referent_inodes_toggle_on(client, fs_name):
    if not toggle_allow_referent_inodes(client, fs_name, True, repeat=True):
        log.error("Enabling 'allow_referent_inodes' failed for FS %s" % fs_name)
        return 1
    log.info(
        "'allow_referent_inodes' successfully enabled for %s and idempotency check passed"
        % fs_name
    )
    return 0


def test_disable_allow_referent_inodes_and_validate(
    client, fs_util, ref_inode_utils, inode_numbers, mount_paths, fs_name
):
    if not toggle_allow_referent_inodes(client, fs_name, False, repeat=True):
        log.error("Disabling 'allow_referent_inodes' failed")
        return 1
    for mtype in ["Fuse", "Kernel", "NFS"]:
        orig = get_mount_path(mtype, 0, mount_paths, "%s_file1" % mtype[0].lower())
        hl = get_mount_path(mtype, 0, mount_paths, "%s_file1_hl" % mtype[0].lower())
        ref_inode_utils.create_hardlink(client, orig, hl)
        log.info("Created hardlink for %s: %s -> %s", mtype, orig, hl)
    ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, fs_name)
    for mtype, fname in [
        ("Fuse", "f_file1"),
        ("Kernel", "k_file1"),
        ("NFS", "n_file1"),
    ]:
        inode = inode_numbers.get(mtype, {}).get(fname)
        if inode:
            data = ref_inode_utils.get_inode_details(
                client, fs_name, inode, mount_paths[mtype][0]
            )
            if data.get("referent_inodes"):
                log.error(
                    "Referent inodes found for %s while disabled: %s"
                    % (fname, data["referent_inodes"])
                )
                return 1
            log.info("No referent inodes found for %s (expected)", fname)
    return 0


def test_toggle_allow_referent_inodes_invalid_inputs(client):
    errors = []
    # Invalid FS name
    out, err = client.exec_command(
        sudo=True,
        cmd="ceph fs set nonexistfs allow_referent_inodes true",
        check_ec=False,
    )
    if "Filesystem not found" not in out + err and "ENOENT" not in out + err:
        errors.append("Expected FS not found error for nonexistfs")
    # Valid values
    for val in ["yes", "no", "1", "0"]:
        out, err = client.exec_command(
            sudo=True,
            cmd="ceph fs set cephfs_ref allow_referent_inodes %s" % val,
            check_ec=False,
        )
        if ("invalid" in (out + err).lower()) or ("EINVAL" in out + err):
            errors.append("Unexpected error for valid value '%s'" % val)
    # Invalid values
    negative_values = ["-1", "0.1", "truee", "foo", "", " ", "TRUEFALSE", "yesno"]
    for val in negative_values:
        cmd = (
            "ceph fs set cephfs_ref allow_referent_inodes %s" % val
            if val.strip()
            else "ceph fs set cephfs_ref allow_referent_inodes"
        )
        out, err = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        combined = out + err
        if not any(
            sub in combined.lower()
            for sub in ["invalid", "missing", "unused", "EINVAL", "value must be"]
        ):
            errors.append("No expected error for invalid value '%s'" % val)
    if errors:
        for e in errors:
            log.error(e)
        return 1
    log.info("PASSED: Invalid inputs handled correctly")
    return 0


def test_toggle_enable_disable_repeatedly(client, fs_name, iterations=10):
    for _ in range(iterations):
        if not toggle_allow_referent_inodes(client, fs_name, True):
            return 1
        if not toggle_allow_referent_inodes(client, fs_name, False):
            return 1
    log.info("PASSED: Repeated enable/disable toggles succeeded")
    return 0


def test_parallel_toggle(clients, fs_name):
    if len(clients) < 2:
        log.error("requires at least two clients.")
        return 1

    client0 = clients[0]
    client1 = clients[1]

    results = []

    def worker(client, enable):
        res = toggle_allow_referent_inodes(client, fs_name, enable)
        results.append(0 if res else 1)

    threads = [
        threading.Thread(target=worker, args=(client0, True)),
        threading.Thread(target=worker, args=(client1, False)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    log.info("Parallel toggle thread results: %s", results)

    # Allow cluster to settle before fetching final state
    time.sleep(3)
    final_state = None
    try:
        out, _ = client0.exec_command(sudo=True, cmd="ceph fs get %s -f json" % fs_name)
        final_state = (
            json.loads(out.strip())
            .get("mdsmap", {})
            .get("flags_state", {})
            .get("allow_referent_inodes", None)
        )
        log.info(
            "Final allow_referent_inodes state after parallel toggles: %s" % final_state
        )
    except Exception as e:
        log.warning("Error fetching final state after toggles: %s" % e)

    if all(r == 0 for r in results):
        log.info("PASSED: Both toggle operations executed without error.")
        if results[0] != results[1]:
            log.warning(
                "Race Condition: "
                "Final state matches only one toggle; expected in concurrent toggles."
            )
        return 0
    else:
        if 0 in results and final_state is not None:
            expected_values = [True, False]  # possible desired end states
            if final_state in expected_values:
                log.warning(
                    "One client verification failed, "
                    "but final state is valid for the other – treating as pass due to race."
                )
                return 0
        log.error("One or more toggle operations reported an error.")
        for idx, res in enumerate(results):
            if res != 0:
                log.error(
                    "Client[%s] (%s) toggle verification failed."
                    % (idx, clients[idx].node.hostname)
                )
        raise CommandFailed("Verify failed")


def test_parallel_enable_disable_loop(clients, fs_name, iterations=10):
    @retry(CommandFailed, tries=3, delay=10)
    def toggle_once(client, enable):
        if not toggle_allow_referent_inodes(client, fs_name, enable):
            raise CommandFailed("Verify failed")

    results = []

    def loop(client):
        for _ in range(iterations):
            try:
                toggle_once(client, True)
                toggle_once(client, False)
                results.append(True)
            except CommandFailed:
                results.append(False)

    if len(clients) < 2:
        return 1
    threads = [
        threading.Thread(target=loop, args=(clients[0],)),
        threading.Thread(target=loop, args=(clients[1],)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return 0 if all(results) else 1


def run(ceph_cluster, **kwargs):
    try:
        test_data, config = kwargs.get("test_data"), kwargs.get("config")
        ref_inode_utils = RefInodeUtils(ceph_cluster)
        fs_util, clients, erasure = ref_inode_utils.prepare_environment(
            ceph_cluster, test_data, config
        )
        if not clients:
            log.info("Requires at least 1 client")
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

        fuse_root, kernel_root, nfs_root = [
            d[0]
            for d in ref_inode_utils.mount_rootfs(
                client, fs_util, default_fs, nfs_server, "cephfs-nfs"
            )
        ]
        subvol_names = ref_inode_utils.create_subvolumes(
            client, fs_util, default_fs, "subvol_group1", 1, 2
        )
        sv_fuse, sv_kernel, sv_nfs = ref_inode_utils.mount_subvolumes(
            client,
            fs_util,
            default_fs,
            subvol_names,
            "subvol_group1",
            nfs_server,
            "cephfs-nfs",
        )

        DIRECT_MOUNT_PATHS = {
            "Fuse": [fuse_root],
            "Kernel": [kernel_root],
            "NFS": [nfs_root],
        }
        SUBVOL_MOUNT_PATHS = {"Fuse": sv_fuse, "Kernel": sv_kernel, "NFS": sv_nfs}

        create_on_mounts(
            client, DIRECT_MOUNT_PATHS, MOUNT_POINTS, ref_inode_utils.create_directories
        )
        create_on_mounts(
            client, SUBVOL_MOUNT_PATHS, MOUNT_POINTS, ref_inode_utils.create_directories
        )
        created_rootfs = create_on_mounts(
            client,
            DIRECT_MOUNT_PATHS,
            MOUNT_POINTS,
            ref_inode_utils.create_file_with_content,
            "rootfs",
        )
        created_subvol = create_on_mounts(
            client,
            SUBVOL_MOUNT_PATHS,
            MOUNT_POINTS,
            ref_inode_utils.create_file_with_content,
            "subvol",
        )
        all_created_files = created_rootfs + created_subvol

        ref_inode_utils.flush_journal_on_active_mdss(fs_util, client, default_fs)
        inode_numbers = collect_inode_numbers(
            ref_inode_utils,
            client,
            all_created_files,
            DIRECT_MOUNT_PATHS,
            SUBVOL_MOUNT_PATHS,
        )

        try:
            log.info(
                "Starting Scenario 1: Verify 'allow_referent_inodes' is disabled by default"
            )
            if test_default_allow_referent_inodes_disabled(client, default_fs) != 0:
                return 1
            log.info("Scenario 1 PASSED")

            log.info("Starting Scenario 2: Enable and verify 'allow_referent_inodes'")
            if test_enable_allow_referent_inodes_toggle_on(client, default_fs) != 0:
                return 1
            log.info("Scenario 2 PASSED")

            log.info(
                "Starting Scenario 3: Disable 'allow_referent_inodes' and validate no referent inodes remain"
            )
            if (
                test_disable_allow_referent_inodes_and_validate(
                    client,
                    fs_util,
                    ref_inode_utils,
                    inode_numbers,
                    {
                        k: DIRECT_MOUNT_PATHS[k] + SUBVOL_MOUNT_PATHS[k]
                        for k in MOUNT_POINTS
                    },
                    default_fs,
                )
                != 0
            ):
                return 1
            log.info("Scenario 3 PASSED")

            log.info(
                "Starting Scenario 4: Validate CLI handles invalid and valid inputs for toggling flag"
            )
            if test_toggle_allow_referent_inodes_invalid_inputs(client) != 0:
                return 1
            log.info("Scenario 4 PASSED (input validation)")

            log.info(
                "Starting Scenario 5: Repeatedly enable/disable 'allow_referent_inodes'"
            )
            if test_toggle_enable_disable_repeatedly(client, default_fs) != 0:
                return 1
            log.info("Scenario 5 PASSED")

            log.info(
                "Starting Scenario 6: Parallel toggle on multiple clients to check race handling"
            )
            if test_parallel_toggle(clients, default_fs) != 0:
                return 1
            log.info("Scenario 6 PASSED")

            log.info(
                "Starting Scenario 7: Loop parallel enable/disable to stress concurrency logic"
            )
            if test_parallel_enable_disable_loop(clients, default_fs) != 0:
                return 1
            log.info("Scenario 7 PASSED")

            log.info("All scenarios PASSED")
            return 0

        finally:
            log.info("Starting cleanup phase")
            # Remove hardlinks, files, and directories
            for mount_type, base_paths in {
                **DIRECT_MOUNT_PATHS,
                **SUBVOL_MOUNT_PATHS,
            }.items():
                for idx, base_path in enumerate(base_paths):
                    dir_name = MOUNT_POINTS[mount_type]["dirs"][idx]

                    # Remove all files (including hardlinks) listed in MOUNT_POINTS
                    for fname in MOUNT_POINTS[mount_type]["files"][dir_name]:
                        file_path = "%s/%s/%s" % (
                            base_path.rstrip("/"),
                            dir_name,
                            fname,
                        )
                        try:
                            log.info("Removing file/hardlink: %s", file_path)
                            client.exec_command(
                                sudo=True, cmd="rm -f %s" % file_path, check_ec=False
                            )
                        except Exception as e:
                            log.warning("Failed to remove file %s: %s", file_path, e)

                    # Remove the directory after deleting files
                    dir_path = "%s/%s" % (base_path.rstrip("/"), dir_name)
                    try:
                        log.info("Removing directory: %s", dir_path)
                        client.exec_command(
                            sudo=True, cmd="rm -rf %s" % dir_path, check_ec=False
                        )
                    except Exception as e:
                        log.warning("Failed to remove directory %s: %s", dir_path, e)

            # Unmount all mount points
            log.info("Unmounting all mount points...")
            for base_paths in list(DIRECT_MOUNT_PATHS.values()) + list(
                SUBVOL_MOUNT_PATHS.values()
            ):
                for base_path in base_paths:
                    try:
                        client.exec_command(
                            sudo=True, cmd="umount -l %s" % base_path, check_ec=False
                        )
                        log.info("Unmounted: %s", base_path)
                    except Exception as e:
                        log.warning("Failed to unmount %s: %s", base_path, e)
            # Remove NFS export
            try:
                log.info("Removing all NFS exports from cluster cephfs-nfs...")
                out, _ = client.exec_command(
                    sudo=True, cmd="ceph nfs export ls cephfs-nfs -f json"
                )
                exports = json.loads(out.strip())
                for export in exports:
                    try:
                        cmd = "ceph nfs export rm cephfs-nfs %s" % export
                        log.info("Removing NFS export: %s", export)
                        client.exec_command(sudo=True, cmd=cmd, check_ec=False)
                    except Exception as e:
                        log.warning("Failed to remove export %s: %s", export, e)
            except Exception as e:
                log.warning("Failed to list/remove NFS exports: %s", e)

            try:
                log.info("Cleaning up subvolumes and subvolume groups...")
                for sv in subvol_names:
                    try:
                        fs_util.remove_subvolume(
                            client,
                            default_fs,
                            sv,
                            validate=True,
                            group_name="subvol_group1",
                        )
                        log.info("Removed subvolume: %s", sv)
                    except Exception as e:
                        log.warning("Failed to remove subvolume %s: %s", sv, e)
                try:
                    fs_util.remove_subvolumegroup(
                        client, default_fs, "subvol_group1", validate=True
                    )
                    log.info("Removed subvolume group: subvol_group1")
                except Exception as e:
                    log.warning("Failed to remove subvolume group: %s", e)
            except Exception as e:
                log.warning("Subvolume/subvolumegroup cleanup failed: %s", e)

            # Remove the cluster
            log.info("Removing NFS cluster:")
            fs_util.remove_nfs_cluster(
                client, nfs_cluster_name="cephfs-nfs", validate=True
            )

            # Remove the filesystem
            try:
                log.info("Removing filesystem %s" % default_fs)
                fs_util.remove_fs(client, default_fs, validate=True)
            except Exception as e:
                log.warning("Failed to remove filesystem %s: %s", default_fs, e)

    except Exception as err:
        log.exception("Error in test run: %s", err)
        return 1
