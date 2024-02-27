import json
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def validate_pool_snapshots(client, pool, snap_count):
    """
    Validates the snapshot count in a given pool.
    Args:
        client (object): Client object for executing commands.
        pool (str): Name of the pool to validate.
        snap_count (int): Expected number of snapshots in the pool.
    Returns:
        str: Validation result message.
    """
    output, _ = client.exec_command(
        f"rados lssnap -p {pool} | tail -n 1 | grep -o [0-9]"
    )
    output = int(output.strip()) if output.strip().isdigit() else 0
    if output != snap_count:
        return f"Snapshot count mismatch in {pool}: Expected {snap_count}, found {output})."
        raise CommandFailed("Snapshot count mismatch. Pool Snapshot Validation Failed")
    elif snap_count == 0:
        return f"No snapshots found in {pool}."
    else:
        return f"{snap_count} snapshots found in {pool}:\n{output}"


def run(ceph_cluster, **kw):
    """
    Executes the CEPH-83575622 test case :
    Validate if creation of pool-level snaps for pools actively associated with a filesystem is disallowed.

    Pre-requisites:
    - Requires minimum 1 client node.

    Test operation:
    Scenario 1 : Creation of snap on existing pools attached to an FS should Fail.
    - Creates snapshots on existing pools attached to a filesystem.
    - Validates snapshot counts in pools is 0
    - Capture the Errors and validate the failures
    - Cleans up filesystems and pools.

    Scenario 2 : Creation of snap on new pools and adding to an existing filesystem should Fail
    - Creates new pools and snaps on the pools.
    - Validates snapshot counts in pools
    - Attach the pools with snaps to an existing filesystem
    - Capture the Errors and validate the failures
    - Cleans up pools.

    Scenario 3 : Creation of Filesystem using the pools which already have snaps should Fail
    - Creates new pools and snaps on the pools.
    - Validates snapshot counts in pools
    - Create new filesystem using the pools with snaps
    - Capture the Errors and validate the failures
    - Cleans up pools.

    Returns:
        Pass is error code and snap count matches, else Fail.
    """
    try:
        tc = "CEPH-83575622"
        log.info(f"Running cephfs {tc} test case")
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")
        if len(clients) < 1:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        log.info(
            "\n ==========================================================================="
            "\n Scenario 1 : Creation of snap on existing pools attached to an FS"
            "\n ==========================================================================="
        )
        new_fs = "newfs"
        log.info("Create new filesystem")
        fs_details = fs_util_v1.get_fs_info(clients[0], new_fs)
        if not fs_details:
            fs_util_v1.create_fs(clients[0], vol_name=new_fs)
            fs_util_v1.wait_for_mds_process(clients[0], new_fs)
        log.info(f"Fetch the Pools part of {new_fs}")
        output, _ = clients[0].exec_command(
            sudo=True, cmd="ceph osd lspools --format json-pretty"
        )
        pool_info = json.loads(output)
        metadata_pool = next(
            (
                pool["poolname"]
                for pool in pool_info
                if pool["poolname"].startswith(f"cephfs.{new_fs}.meta")
            ),
            None,
        )
        data_pool = next(
            (
                pool["poolname"]
                for pool in pool_info
                if pool["poolname"].startswith(f"cephfs.{new_fs}.data")
            ),
            None,
        )
        log.info(f"Metadata Pool : {metadata_pool}")
        log.info(f"Data Pool : {data_pool}")

        log.info("Method 1: 'ceph osd pool mksnap' is used to create a snap")
        expected_output = "Error ENOTSUP: (95) Operation not supported"
        errors = []
        pool_snap_names = [(metadata_pool, "snap1"), (data_pool, "snap2")]
        for pool_name, snap_name in pool_snap_names:
            error, return_code = clients[0].exec_command(
                f"ceph osd pool mksnap {pool_name} {snap_name}", check_ec=False
            )
            if expected_output in return_code:
                log.info(f"Snap Creation on {pool_name} is disallowed")
                errors.append(return_code)
            else:
                log.error(
                    "Snapshot Creation is allowed on Pools associated with Filesystem, Test Failed"
                )
                raise CommandFailed(
                    "Snapshot Creation is allowed on Pools associated with Filesystem, Test Failed"
                )
        method1_test_status = (
            "Method 1 - Test Passed." if errors else "Method 1 - Test Failed."
        )
        log.info(method1_test_status)

        log.info("Method 2: 'rados mksnap' is used to create a snap")
        errors = []
        pool_snap_names = [(metadata_pool, "snap3"), (data_pool, "snap4")]
        for pool_name, snap_name in pool_snap_names:
            expected_output = f"error creating pool {pool_name} snapshot {snap_name}: (95) Operation not supported"
            error, return_code = clients[0].exec_command(
                f"rados mksnap -p {pool_name} {snap_name}", check_ec=False
            )
            if expected_output in return_code:
                log.info(f"Snap Creation on {pool_name} is disallowed")
                errors.append(return_code)
            else:
                log.error(
                    "Snapshot Creation is allowed on Pools associated with Filesystem, Test Failed"
                )
                raise CommandFailed(
                    "Snapshot Creation is allowed on Pools associated with Filesystem, Test Failed"
                )

        method2_test_status = (
            "Method 2 - Test Passed." if errors else "Method 2 - Test Failed."
        )
        log.info(method2_test_status)

        log.info(
            f"Validate if no snapshots are created on {metadata_pool} and {data_pool}"
        )
        snap_count = 0
        log.info("Validate snapshots for metadata pool")
        metadata_validation_result = validate_pool_snapshots(
            clients[0], metadata_pool, snap_count
        )
        log.info(metadata_validation_result)

        log.info("Validate snapshots for data pool")
        data_validation_result = validate_pool_snapshots(
            clients[0], data_pool, snap_count
        )
        log.info(data_validation_result)

        log.info(f"Cleaning up the Filesystem {new_fs}")
        fs_util_v1.remove_fs(clients[0], new_fs, validate=True)

        log.info(
            "\n ==========================================================================="
            "\n Scenario 2: Creation of snap on pools and adding to an existing filesystem"
            "\n ==========================================================================="
        )
        default_fs = "cephfs"
        pool_snap_names = [
            (f"{data_pool}_1", "snap1"),
            (f"{metadata_pool}_1", "snap2"),
            (f"{data_pool}_1", "snap3"),
            (f"{metadata_pool}_1", "snap4"),
        ]
        snap_commands = {
            "snap1": "rados mksnap -p",
            "snap2": "rados mksnap -p",
            "snap3": "ceph osd pool mksnap",
            "snap4": "ceph osd pool mksnap",
        }
        for pool_name, snap_name in pool_snap_names:
            clients[0].exec_command(f"ceph osd pool create {pool_name}")
            snap_command = snap_commands.get(snap_name)
            clients[0].exec_command(f"{snap_command} {pool_name} {snap_name}")

        log.info(
            f"Validate if snapshots are created on {metadata_pool}_1 and {data_pool}_1"
        )
        snap_count = 2
        log.info("Validate snapshots for metadata pool")
        metadata_validation_result = validate_pool_snapshots(
            clients[0], f"{metadata_pool}_1", snap_count
        )
        log.info(metadata_validation_result)

        log.info("Validate snapshots for data pool")
        data_validation_result = validate_pool_snapshots(
            clients[0], f"{data_pool}_1", snap_count
        )
        log.info(data_validation_result)

        log.info("Execute ceph fs add_data_pool commands")
        out1, rc1 = clients[0].exec_command(
            f"ceph fs add_data_pool " f"{default_fs} {metadata_pool}_1", check_ec=False
        )
        out2, rc2 = clients[0].exec_command(
            f"ceph fs add_data_pool " f"{default_fs} {data_pool}_1", check_ec=False
        )

        log.info("Check errors and validate pass status")
        if (
            "ENOTSUP"
            and "already has mon-managed snaps; can't attach pool to fs" in rc1
            and rc2
        ):
            log.info(f"Error captured: {rc1} and {rc2}")
            log.info("Test status: Passed")
        else:
            log.error("Error captured: Unknown")
            log.error("Test status: Failed")
            raise CommandFailed(
                "No Errors Seen, Adding a Pool with snap is Allowed, Test Failed"
            )

        log.info(f"Cleaning up the Pools {metadata_pool}_1 and {data_pool}_1")
        clients[0].exec_command(
            f"ceph osd pool rm {metadata_pool}_1 {metadata_pool}_1 --yes-i-really-really-mean-it"
        )
        clients[0].exec_command(
            f"ceph osd pool rm {data_pool}_1 {data_pool}_1 --yes-i-really-really-mean-it"
        )

        log.info(
            "\n ==========================================================================="
            "\n Scenario 3 : Creation of Filesystem using the pools which already have snaps"
            "\n ==========================================================================="
        )
        pool_snap_names = [
            (f"{data_pool}", "snap1"),
            (f"{metadata_pool}", "snap2"),
            (f"{data_pool}", "snap3"),
            (f"{metadata_pool}", "snap4"),
        ]
        snap_commands = {
            "snap1": "rados mksnap -p",
            "snap2": "rados mksnap -p",
            "snap3": "ceph osd pool mksnap",
            "snap4": "ceph osd pool mksnap",
        }
        for pool_name, snap_name in pool_snap_names:
            clients[0].exec_command(f"ceph osd pool create {pool_name}")
            snap_command = snap_commands.get(snap_name)
            clients[0].exec_command(f"{snap_command} {pool_name} {snap_name}")

        log.info(
            f"Validate if snapshots are created on {metadata_pool} and {data_pool}"
        )
        snap_count = 2
        log.info("Validate snapshots for metadata pool")
        metadata_validation_result = validate_pool_snapshots(
            clients[0], f"{metadata_pool}", snap_count
        )
        log.info(metadata_validation_result)

        log.info("Validate snapshots for data pool")
        data_validation_result = validate_pool_snapshots(
            clients[0], f"{data_pool}", snap_count
        )
        log.info(data_validation_result)

        log.info("Create Ceph Filesystem using the pools which has Snaps")
        out, rc = clients[0].exec_command(
            f"ceph fs new {new_fs} {metadata_pool} {data_pool}", check_ec=False
        )

        log.info("Check errors and validate pass status")
        if "ENOTSUP" and "already has mon-managed snaps; can't attach pool to fs" in rc:
            log.info(f"Error captured: {rc}")
            log.info("Test status: Passed")
        else:
            log.error("Error captured: Unknown")
            log.error("Test status: Failed")
            raise CommandFailed(
                "No Errors Seen, Creation of FS using a Pool with snap is Allowed, Test Failed"
            )

        log.info(f"Cleaning up the Pools {metadata_pool} and {data_pool}")
        clients[0].exec_command(
            f"ceph osd pool rm {metadata_pool} {metadata_pool} --yes-i-really-really-mean-it"
        )
        clients[0].exec_command(
            f"ceph osd pool rm {data_pool} {data_pool} --yes-i-really-really-mean-it"
        )

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
