import random
import string
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.exceptions import ValueMismatchError
from utility.log import Log
from utility.retry import retry

log = Log(__name__)

"""
Testing description:

Verify cephfs-top package installation and its functionality(metrics)

Steps to Reproduce:
1. Try to test install cephfs-top package
2. Enable stats in mgr module
3. Create cephfs-top client user and verify
4. Mount CephFS using ceph-fuse and kernel
5. Get the cephfs top dump
6. Open files and verify the count
7. Close files and verify the count
8. Verify iocaps
9. Verify client counts
10. Verify total read and write IO
10. Create multiple volumes and verify it.
"""


@retry(ValueMismatchError, tries=5, delay=30)
def verify_open_files_count(expected_count):
    """
    Fetch cephfs-top dump and verify open file count.
    Retries if count does not match the expected value.
    """
    out_open = fs_util.get_cephfs_top_dump(client1)["filesystems"][fs_name][client_id][
        "ofiles"
    ]
    log.info("Output of {} top dump after opening files: {}".format(fs_name, out_open))

    if int(out_open) != expected_count:
        raise ValueMismatchError(
            "Open files did not match. Expected/Before: {}, Found/After: {}".format(
                expected_count, out_open
            )
        )

    log.info("Open files count matched")


@retry(ValueMismatchError, tries=5, delay=30)
def verify_closed_files_count():
    """
    Fetch cephfs-top dump and verify closed file count is 0.
    Retries if the count is not zero.
    """
    out_closed = fs_util.get_cephfs_top_dump(client1)["filesystems"][fs_name][
        client_id
    ]["ofiles"]

    if int(out_closed) != 0:
        raise ValueMismatchError(
            "Closed files count mismatch. Expected: 0, Found: {}.".format(out_closed)
        )

    log.info("Closed files count matched")


@retry(ValueMismatchError, tries=5, delay=30)
def verify_io_caps_increment(iocaps_value_before, num_files):
    """
    Verify that IO Caps value increased by the expected number of files.
    Retries if the value does not match expected increment.
    """
    iocaps_value_after = fs_util.get_cephfs_top_dump(client1)["filesystems"][fs_name][
        client_id
    ]["oicaps"]

    expected_value = int(iocaps_value_before) + num_files
    if int(iocaps_value_after) != expected_value:
        raise ValueMismatchError(
            "IO Caps value mismatch. Expected: {}, Found: {}".format(
                expected_value, iocaps_value_after
            )
        )

    log.info("IO Caps value matched")


@retry(ValueMismatchError, tries=5, delay=30)
def verify_io_caps_decrease(iocaps_value_before):
    """
    Verify that IO Caps value remains unchanged.
    Retries if the value changes unexpectedly.
    """
    iocaps_value_after = fs_util.get_cephfs_top_dump(client1)["filesystems"][fs_name][
        client_id
    ]["oicaps"]

    if int(iocaps_value_after) != int(iocaps_value_before):
        raise ValueMismatchError(
            "IO Caps value mismatch: Expected/Before: {}, Found/After: {}".format(
                iocaps_value_before, iocaps_value_after
            )
        )

    log.info("IO Caps value matched")


@retry(ValueMismatchError, tries=7, delay=30)
def verify_fuse_client_count_increment(current_fuse_count):
    """
    Verify that the fuse client count increased by a specific increment.
    Retries if the value does not match.
    """
    new_fuse_count = fs_util.get_cephfs_top_dump(client1)["client_count"]["fuse"]
    expected_fuse_count = current_fuse_count + 2

    if new_fuse_count != expected_fuse_count:
        raise ValueMismatchError(
            "Fuse client count mismatch. Expected/Before: {}, Found/After: {}".format(
                expected_fuse_count, new_fuse_count
            )
        )

    log.info("Fuse client count matched")


@retry(ValueMismatchError, tries=7, delay=30)
def verify_kernel_client_count_increment(current_kernel_count):
    """
    Verify that the kernel client count increased by the expected increment.
    Retries if the value does not match.
    """
    new_kernel_count = fs_util.get_cephfs_top_dump(client1)["client_count"]["kclient"]
    expected_kernel_count = current_kernel_count + 1

    if new_kernel_count != expected_kernel_count:
        raise ValueMismatchError(
            "Kernel client count mismatch. Expected/Before: {}, Found/After: {}".format(
                expected_kernel_count, new_kernel_count
            )
        )

    log.info("Kernel client count matched")


@retry(ValueMismatchError, tries=7, delay=30)
def verify_total_client_count_increment(current_total_count):
    """
    Verify that the total client count increased by the expected increment.
    Retries if the value does not match.
    """
    new_total_count = fs_util.get_cephfs_top_dump(client1)["client_count"][
        "total_clients"
    ]
    expected_total_count = current_total_count + 3

    if new_total_count != expected_total_count:
        raise ValueMismatchError(
            "Total client count mismatch. Expected/Before: {}, Found/After: {}".format(
                expected_total_count, new_total_count
            )
        )

    log.info("Total client count matched")


@retry(ValueMismatchError, tries=5, delay=30)
def verify_filesystems_present(fs_names):
    """
    Verify that all specified CephFS names are present in the filesystems dump.
    Retries if any filesystem name is missing.
    """
    fs_name_dump = fs_util.get_cephfs_top_dump(client1)["filesystems"]

    for fs_name in fs_names:
        if fs_name not in fs_name_dump:
            raise ValueMismatchError("{} not found in filesystems dump".format(fs_name))
        log.info("{} found in filesystems dump".format(fs_name))


@retry(ValueMismatchError, tries=5, delay=30)
def verify_read_write_io_progress(before_total_read, before_total_write):
    """
    Verify that read and write IO (rtio and wtio) have not decreased.
    Retries if either value is less than before.
    """
    dump = fs_util.get_cephfs_top_dump(client1)["filesystems"][fs_name][client_id]
    after_total_read = dump["rtio"]
    after_total_write = dump["wtio"]

    if int(after_total_read) < int(before_total_read):
        raise ValueMismatchError(
            "Total read IO mismatch. Expected/Before: {}, Found/After: {}".format(
                before_total_read, after_total_read
            )
        )
    log.info("Total read IO matched")

    if int(after_total_write) < int(before_total_write):
        raise ValueMismatchError(
            "Total write IO mismatch. Expected/Before: {}, Found/After: {}".format(
                before_total_write, after_total_write
            )
        )
    log.info("Total write IO matched")


def run(ceph_cluster, **kw):
    try:
        global fs_util, client1, fs_name, client_id

        tc = "CEPH-83573848"
        log.info(f"Running CephFS tests for ceph {tc}")
        # Initialize the utility class for CephFS
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        # Get the client nodes
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        # Authenticate the clients
        fs_util.auth_list(clients)
        build = config.get("build", config.get("rhbuild"))
        # Prepare the clients
        fs_util.prepare_clients(clients, build)
        client1 = clients[0]
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        log.info("Install cephfs-top by dnf install cephfs-top")
        client1.exec_command(
            sudo=True,
            cmd="dnf install cephfs-top -y",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="dnf list cephfs-top",
        )
        if "cephfs-top" not in out:
            log.error("cephfs-top package could not be installed")
            return 1

        log.info("Enable stats in mgr module")
        client1.exec_command(
            sudo=True,
            cmd="ceph mgr module enable stats",
        )
        log.info("Create cephfs-top client user and verify")
        client_user = "client.fstop"
        cmd = f"ceph auth get-or-create {client_user} mon 'allow r' mds 'allow r' osd 'allow r' mgr 'allow r'"
        cmd += " > /etc/ceph/ceph.client.fstop.keyring"
        client1.exec_command(
            sudo=True,
            cmd=cmd,
        )
        out, _ = client1.exec_command(
            sudo=True,
            cmd="ceph auth ls | grep fstop",
        )
        if client_user not in out:
            log.error(f"{client_user} user not created")
            return 1
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        # Define mount directories
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{rand}"
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel_{rand}"
        # Mount CephFS using ceph-fuse and kernel
        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir_1,
            extra_params=f" --client_fs {fs_name}",
        )
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={fs_name}",
        )
        output = fs_util.get_cephfs_top_dump(client1)
        log.info(f"Output of cephfs top dump: {output}")
        # get client ID using mounted directory
        client_id = fs_util.get_client_id(client1, fs_name, fuse_mounting_dir_1)
        # open files
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\n    Usecase 1: Verify total client counts, increment for fuse and kernel clients"
            "\n---------------***************-----------------------------------"
        )
        log.info("Verifying client counts")
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse_{rand}_2"
        fuse_mounting_dir_3 = f"/mnt/cephfs_fuse_{rand}_3"
        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel_{rand}_2"
        current_fuse_count = fs_util.get_cephfs_top_dump(client1)["client_count"][
            "fuse"
        ]
        current_kernel_count = fs_util.get_cephfs_top_dump(client1)["client_count"][
            "kclient"
        ]
        current_total_count = fs_util.get_cephfs_top_dump(client1)["client_count"][
            "total_clients"
        ]
        log.info(
            "Current FUSE count: {}, Current Kernel Count: {}, Current Total Count: {}".format(
                current_fuse_count, current_kernel_count, current_total_count
            )
        )
        log.info("mounting clients")
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir_2,
            ",".join(mon_node_ips),
            extra_params=f",fs={fs_name}",
        )

        time.sleep(5)
        fs_util.fuse_mount(
            [client1], fuse_mounting_dir_2, extra_params=f" --client_fs {fs_name}"
        )
        time.sleep(5)
        fs_util.fuse_mount(
            [client1], fuse_mounting_dir_3, extra_params=f" --client_fs {fs_name}"
        )

        verify_fuse_client_count_increment(current_fuse_count)

        verify_kernel_client_count_increment(current_kernel_count)

        verify_total_client_count_increment(current_total_count)

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\n    Usecase 2: Verify open and close files functionality        "
            "\n---------------***************-----------------------------------"
        )
        file_list = []
        opened_files = 10
        for i in range(opened_files):
            file_name = f"file_{rand}_{i}"
            file_list.append(file_name)
        pids = fs_util.open_files(client1, fuse_mounting_dir_1, file_list)
        verify_open_files_count(opened_files)

        fs_util.close_files(client1, pids)
        verify_closed_files_count()

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\n    Usecase 3: Verify iocaps increment and decrement          "
            "\n---------------***************-----------------------------------"
        )
        log.info("verifying iocaps")
        num_files = 10
        iocaps_value_before = fs_util.get_cephfs_top_dump(client1)["filesystems"][
            "cephfs"
        ][client_id]["oicaps"]
        for i in range(num_files):
            file_name = f"files_{rand}_{i}"
            client1.exec_command(
                sudo=True, cmd=f"touch {fuse_mounting_dir_1}/{file_name}"
            )
        verify_io_caps_increment(iocaps_value_before, num_files)

        log.info("Decrease the iocaps value")
        for i in range(num_files):
            file_name = f"files_{rand}_{i}"
            client1.exec_command(
                sudo=True, cmd=f"rm -rf {fuse_mounting_dir_1}/{file_name}"
            )
        verify_io_caps_decrease(iocaps_value_before)

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\n    Usecase 4: Create multiple filesystems and verify client metrics   "
            "\n---------------***************-----------------------------------"
        )
        log.info("Create multiple filesystems and verify client metrics")
        cephfs_name1 = f"cephfs_top_name_{rand}_1"
        cephfs_name2 = f"cephfs_top_name_{rand}_2"
        fs_util.create_fs(client1, cephfs_name1)
        fs_util.create_fs(client1, cephfs_name2)

        verify_filesystems_present([cephfs_name1, cephfs_name2])

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\n    Usecase 5: Create increase read and write IO"
            "\n---------------***************-----------------------------------"
        )
        log.info("create increase read and write IO")
        log.info("create a file 4GB file with dd")
        file_name = f"file_{rand}"
        before_total_read = fs_util.get_cephfs_top_dump(client1)["filesystems"][
            fs_name
        ][client_id]["rtio"]
        before_total_write = fs_util.get_cephfs_top_dump(client1)["filesystems"][
            fs_name
        ][client_id]["wtio"]
        client1.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={fuse_mounting_dir_1}/{file_name} bs=1M count=4096",
        )
        log.info("copy the file to other directories")
        client1.exec_command(
            sudo=True,
            cmd=f"cp {fuse_mounting_dir_1}/{file_name} {fuse_mounting_dir_1}/{file_name}_copy",
        )

        verify_read_write_io_progress(before_total_read, before_total_write)
        return 0

    except ValueMismatchError as e:
        log.error("Value mismatch error: {}".format(e))
        log.error(traceback.format_exc())
        return 1

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\n              Cleaning up the environment                        "
            "\n---------------***************-----------------------------------"
        )
        log.info("Disable mgr stats")
        client1.exec_command(sudo=True, cmd="ceph mgr module disable stats")

        for mount_dir in [
            fuse_mounting_dir_1,
            fuse_mounting_dir_2,
            fuse_mounting_dir_3,
        ]:
            fs_util.client_clean_up(
                "umount", fuse_clients=[client1], mounting_dir=mount_dir
            )

        fs_util.client_clean_up(
            "umount", kernel_clients=[client1], mounting_dir=kernel_mounting_dir_1
        )

        for fs_delete in [cephfs_name1, cephfs_name2]:
            fs_util.remove_fs(client1, fs_delete)
