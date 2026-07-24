import random
import string
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def write_large_file_and_verify_checksum(
    clients, file_path_writer, file_path_reader, size_mb=10
):
    """
    Write a large file from one client/mount and verify checksum from another.
    Args:
        clients: List of clients (writer first, reader second)
        file_path_writer: Path where writer writes
        file_path_reader: Path from where reader verifies
        size_mb: Size of file in MB
    """
    writer = clients[0]
    reader = clients[1]

    # Write large file from writer
    cmd = f"dd if=/dev/urandom of={file_path_writer} bs=1M count={size_mb}"
    writer.exec_command(sudo=True, cmd=cmd)

    # Calculate checksum from writer
    checksum_writer, _ = writer.exec_command(
        sudo=True, cmd=f"sha256sum {file_path_writer} | awk '{{print $1}}'"
    )

    # Read and calculate checksum from reader
    checksum_reader, _ = reader.exec_command(
        sudo=True, cmd=f"sha256sum {file_path_reader} | awk '{{print $1}}'"
    )

    if checksum_writer.strip() != checksum_reader.strip():
        raise CommandFailed(
            f"Checksum mismatch: Writer ({file_path_writer}) vs Reader ({file_path_reader})"
        )


def write_and_verify_data(client, file_path, data):
    client.exec_command(sudo=True, cmd=f"echo '{data}' > {file_path}")
    out, _ = client.exec_command(sudo=True, cmd=f"cat {file_path}")
    if out.strip() != data:
        log.error("Data mismatch at %s,", file_path)
        raise CommandFailed(f"Data mismatch at {file_path}")


def perform_file_operations(client, mount_dir):
    test_file = f"{mount_dir}/test_ops.txt"
    client.exec_command(sudo=True, cmd=f"echo 'original' > {test_file}")
    client.exec_command(sudo=True, cmd=f"echo 'modified' >> {test_file}")
    client.exec_command(sudo=True, cmd=f"cat {test_file}")
    client.exec_command(sudo=True, cmd=f"rm -f {test_file}")
    out, rc = client.exec_command(sudo=True, cmd=f"ls {test_file}", check_ec=False)
    if rc == 0:
        log.error("File was not deleted in %s,", mount_dir)
        raise CommandFailed(f"File was not deleted in {mount_dir}")


def run_concurrent_ops(clients, file_path):
    cmds = [f"echo 'client_{i}' >> {file_path}" for i in range(len(clients))]
    with parallel() as p:
        for i, client in enumerate(clients):
            p.spawn(client.exec_command, sudo=True, cmd=cmds[i], long_running=True)


def verify_checksum_from_all_clients(clients, file_path):
    checksums = []
    for client in clients:
        checksum, _ = client.exec_command(
            sudo=True, cmd=f"sha256sum {file_path} | awk '{{print $1}}'"
        )
        checksums.append((client.node.hostname, checksum.strip()))

    reference = checksums[0][1]
    log.info("Checksums from all clients: %s", checksums)
    for hostname, checksum in checksums:
        if checksum != reference:
            log.error(
                f"Checksum mismatch detected!\nExpected: {reference}\nFound on {hostname}: "
                f"{checksum}\nAll Checksums: {checksums}"
            )
            raise CommandFailed(
                f"Checksum mismatch detected!\nExpected: {reference}\nFound on {hostname}: "
                f"{checksum}\nAll Checksums: {checksums}"
            )


def run(ceph_cluster, **kw):
    try:
        test_data = kw.get("test_data")
        fs_util_v1 = FsUtilsV1(ceph_cluster, test_data=test_data)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")
        if len(clients) < 1:
            log.warning(
                "This test requires minimum 1 client nodes. Found %d", len(clients)
            )
            return 1

        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        default_fs = "cephfs_new"
        subvolume_group_name = "subvol_group1"
        subvolume_name = "subvol"
        fs_util_v1.create_fs(clients[0], vol_name=default_fs)
        fs_util_v1.create_subvolumegroup(
            clients[0], vol_name=default_fs, group_name=subvolume_group_name
        )
        subvolume_list = [
            {
                "vol_name": default_fs,
                "subvol_name": f"{subvolume_name}_1",
                "group_name": subvolume_group_name,
            },
        ]
        for subvolume in subvolume_list:
            fs_util_v1.create_subvolume(clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"

        mon_node_ips = fs_util_v1.get_mon_node_ips()
        subvol_path, _ = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} {subvolume_name}_1 {subvolume_group_name}",
        )
        fs_util_v1.kernel_mount(
            [clients[0], clients[1]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=subvol_path.strip(),
            extra_params=f",fs={default_fs}",
        )

        fs_util_v1.fuse_mount(
            [clients[0], clients[1]],
            fuse_mounting_dir_1,
            extra_params=f" -r {subvol_path.strip()} --client_fs {default_fs}",
        )

        log.info("Running Acceptance Test: Large File and Checksum Verification")
        write_large_file_and_verify_checksum(
            clients,
            f"{kernel_mounting_dir_1}/kernel_largefile.bin",
            f"{fuse_mounting_dir_1}/kernel_largefile.bin",
            size_mb=10,
        )
        write_large_file_and_verify_checksum(
            clients,
            f"{fuse_mounting_dir_1}/fuse_largefile.bin",
            f"{kernel_mounting_dir_1}/fuse_largefile.bin",
            size_mb=10,
        )

        log.info("Running Functional Test: Cross-Mount Data Consistency")
        fuse_to_kernel_file = f"{fuse_mounting_dir_1}/cross_mount_test.txt"
        kernel_to_fuse_file = f"{kernel_mounting_dir_1}/cross_mount_test.txt"
        write_and_verify_data(clients[0], fuse_to_kernel_file, "written from fuse")
        out, _ = clients[0].exec_command(sudo=True, cmd=f"cat {kernel_to_fuse_file}")
        if out.strip() != "written from fuse":
            raise CommandFailed("Cross-mount data mismatch (fuse to kernel)")

        write_and_verify_data(clients[0], kernel_to_fuse_file, "updated from kernel")
        out, _ = clients[0].exec_command(sudo=True, cmd=f"cat {fuse_to_kernel_file}")
        if out.strip() != "updated from kernel":
            raise CommandFailed("Cross-mount data mismatch (kernel to fuse)")

        log.info("Running Functional Test: File Operations")
        perform_file_operations(clients[0], kernel_mounting_dir_1)
        perform_file_operations(clients[0], fuse_mounting_dir_1)

        if len(clients) >= 2:
            log.info("Running Functional Test: Concurrent Access")
            concurrent_kernel_file = (
                f"{kernel_mounting_dir_1}/shared_concurrent_kernel.txt"
            )
            concurrent_fuse_file = f"{fuse_mounting_dir_1}/shared_concurrent_fuse.txt"
            run_concurrent_ops(clients[:2], concurrent_kernel_file)
            run_concurrent_ops(clients[:2], concurrent_fuse_file)
            verify_checksum_from_all_clients(clients[:2], concurrent_fuse_file)
            verify_checksum_from_all_clients(clients[:2], concurrent_kernel_file)
        else:
            log.warning("Skipping concurrent test due to insufficient clients.")
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Test Cleanup: Unmounting and Deleting Subvolumes")
        fs_util_v1.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
        fs_util_v1.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        for subvolume in subvolume_list:
            fs_util_v1.remove_subvolume(clients[0], **subvolume)
        fs_util_v1.remove_subvolumegroup(
            clients[0], default_fs, subvolume_group_name, force=True
        )
