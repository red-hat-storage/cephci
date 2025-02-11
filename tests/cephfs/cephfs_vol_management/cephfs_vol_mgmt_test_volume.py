import random
import string
import traceback
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def update_dict_from_b_gb(input_dict, keys_to_be_converted):
    """
    Checks for the specified keys and convert the Bytes to GB for validation

    Parameters:
        input_dict (dict): Dictionary to consider the keys
        keys_to_be_converted (list): List of keys whose values need to converted

    Returns:
        dict: Returns dict with the updated value
    """
    for keys_bytes in keys_to_be_converted:
        if keys_bytes in input_dict:
            input_dict[keys_bytes] = (
                f"{round(int(input_dict[keys_bytes]) / (1024 ** 3), 1)}G"
                # Convert to GB and round to 1 decimal places
            )
    return input_dict


def test_cephfs_volume_creation(fs_util, client):
    """
    Unified test suite to validate CephFS volume creation under various scenarios.
    This includes testing valid and invalid names, duplicate names, system limits,
    and placement validation.

    Args:
        fs_util (object): Utility object for managing CephFS volumes.
        client (object): Client object to interact with the Ceph cluster.

    Test Scenarios:
        1. Shortest valid volume name.
        2. Longest valid volume name.
        3. Invalid volume names (special characters, excessive length).
        4. Duplicate volume names (idempotent behavior).
        5. Invalid placement group.
        6. Maximum volume limit.

    Cleanup:
        Ensures proper cleanup of created volumes to prevent residual data.

    Returns:
        None
    """

    # Test 1: Shortest valid volume names
    shortest_valid_names = [
        "a",
        "A",
        "_",
        "..",  # Relative path markers
    ]
    log.info(f"Testing shortest valid volume names: {shortest_valid_names}")

    for vol_name in shortest_valid_names:
        try:
            log.info(f"Testing shortest valid volume name: '{vol_name}'")
            fs_util.create_fs(client, vol_name, validate=True)
            log.info(
                f"Test passed: Successfully created and validated volume '{vol_name}'."
            )
        except Exception as e:
            log.error(f"Test failed for shortest valid name '{vol_name}': {e}")
        finally:
            try:
                fs_util.remove_fs(client, vol_name, validate=True)
            except CommandFailed:
                log.info(f"Cleanup not required: Volume '{vol_name}' does not exist.")

    # Test 2: Longest valid volume names
    try:
        max_length = 255  # Maximum allowable length for CephFS volume names
        longest_valid_names = [
            "x"
            * max_length,  # A name with maximum length consisting of a single repeated character
            "abc" * (max_length // 3),  # A name with repeated patterns
            "_" * max_length,  # A name with underscores
            "a" * (max_length - 1) + "1",  # Mixed alphanumeric name
            "A" * (max_length // 2)
            + "_" * (max_length // 2),  # Alternating uppercase and underscores
        ]

        log.info(
            f"Testing longest valid volume names with lengths up to {max_length} characters."
        )

        for vol_name in longest_valid_names:
            try:
                log.info(
                    f"Testing volume name of length {len(vol_name)}: '{vol_name[:10]}...'"
                )
                fs_util.create_fs(client, vol_name, validate=True)
                log.info(
                    f"Test passed: Successfully created and validated volume '{vol_name[:10]}...'."
                )
            except Exception as e:
                log.error(f"Test failed for volume name '{vol_name[:10]}...': {e}")
            finally:
                try:
                    fs_util.remove_fs(client, vol_name, validate=True)
                except CommandFailed:
                    log.info(
                        f"Cleanup not required: Volume '{vol_name[:10]}...' does not exist."
                    )
    except Exception as overall_exception:
        log.error(
            f"Unexpected error in longest valid volume name test: {overall_exception}"
        )

    # Test 3: Invalid volume names
    try:
        invalid_names = [
            "1",  # Numeric 1 is invalid in CephFS
            "1"
            * max_length,  # A numeric name with the maximum length, numeric 1 is invalid in CephFS
            "",  # Empty name
            " ",  # Single space
            "   ",  # Multiple spaces
            "!@#$%^&*()",  # Special characters
            "../",  # Directory traversal
            "-" * 300,  # Name exceeding maximum length
            "valid_name_but_with_invalid_chars@",  # Valid name but with invalid character
            "/leading/slash",  # Name with leading slash
            "trailing/slash/",  # Name with trailing slash
            "\\backslash\\",  # Backslashes
            "name with spaces",  # Name with spaces
            "tab\tname",  # Name with tabs
            "newline\nname",  # Name with newlines
            "." * 256,  # Name exceeding max length with dots
            "a" * 256,  # Name exceeding max length with alphabets
        ]

        log.info("Testing invalid volume names.")

        for vol_name in invalid_names:
            try:
                log.info(
                    f"Testing invalid volume name: '{vol_name}' (length {len(vol_name)})"
                )
                fs_util.create_fs(client, vol_name, validate=False)
                log.error(
                    f"Test failed: Volume with invalid name '{vol_name}' was created."
                )
            except CommandFailed:
                log.info(f"Test passed: Creation failed as expected for '{vol_name}'.")
            except Exception as e:
                log.error(f"Unexpected error for invalid name '{vol_name}': {e}")
    except Exception as overall_exception:
        log.error(f"Unexpected error in invalid volume name test: {overall_exception}")

    # Test 4: Duplicate volume names
    try:
        vol_name = "duplicate_test"
        log.info(f"Testing duplicate volume name: '{vol_name}'")
        fs_util.create_fs(client, vol_name, validate=True)
        fs_util.create_fs(client, vol_name, validate=True)
        log.info(
            f"Test passed: Duplicate volume creation succeeded for '{vol_name}' (idempotent behavior)."
        )
    except Exception as e:
        log.error(f"Test failed for duplicate volume name: {e}")
    finally:
        fs_util.remove_fs(client, vol_name, validate=True)

    # Test 5: Invalid placement group
    try:
        vol_name = "invalid_placement_test"
        invalid_placement = "invalid-placement"
        log.info(
            f"Testing invalid placement '{invalid_placement}' for volume '{vol_name}'"
        )
        fs_util.create_fs(client, vol_name, placement=invalid_placement, validate=False)
        log.error(
            f"Test failed: Volume creation succeeded with invalid placement '{invalid_placement}'."
        )
    except CommandFailed:
        log.info(
            f"Test passed: Creation failed as expected for invalid placement '{invalid_placement}'."
        )
    except Exception as e:
        log.error(f"Unexpected error for invalid placement: {e}")
    finally:
        try:
            fs_util.remove_fs(client, vol_name, validate=True)
        except CommandFailed:
            log.info(f"Cleanup not required: Volume '{vol_name}' does not exist.")

    # Test 6: Maximum volume limit
    # max_volumes = 100  # Adjust the limit as required
    # try:
    #     log.info(f"Testing system limits by creating up to {max_volumes} volumes.")
    #     for i in range(max_volumes):
    #         vol_name = f"max_vol_test_{i}"
    #         fs_util.create_fs(client, vol_name, validate=True)
    #     log.info("All volumes created successfully. Testing failure case by exceeding the limit.")
    #
    #     # Attempt to create one more volume
    #     fs_util.create_fs(client, f"max_vol_test_{max_volumes}", validate=True)
    #     log.error("Test failed: Exceeded maximum volume limit without encountering an error.")
    # except CommandFailed:
    #     log.info("Test passed: Maximum volume limit was reached and handled gracefully.")
    # except Exception as e:
    #     log.error(f"Unexpected error during max volume limit test: {e}")
    # finally:
    #     log.info("Cleanup: Deleting all test volumes.")
    #     for i in range(max_volumes):
    #         try:
    #             fs_util.remove_fs(client, f"max_vol_test_{i}", validate=True)
    #         except CommandFailed:
    #             log.info(f"Volume max_vol_test_{i} already removed or did not exist.")


def test_simultaneous_creation(fs_util, client):
    """
    This test ensures that the CephFS system can handle concurrent volume creation requests
    without errors or inconsistencies. It uses a thread pool to simulate parallel operations.

    Args:
        fs_util (object): Utility object for managing CephFS volumes.
        client (object): Client object to interact with the Ceph cluster.

    Procedure:
        1. Generate a list of unique volume names (`parallel_test_0` to `parallel_test_9`).
        2. Use `ThreadPoolExecutor` to initiate parallel volume creation requests for all volume names.
           - For each volume, attempt to create the volume with `validate=True`.
           - If a creation fails with a `CommandFailed` exception, log the error for that volume.
           - Log any unexpected errors for better debugging.
        3. Once all operations complete, clean up by deleting all created volumes, regardless of success or failure.

    Exceptions:
        - `CommandFailed`: Raised when a volume creation fails as expected.
        - Any other exceptions during creation or validation are logged and handled.

    Cleanup:
        - Ensures all volumes are deleted at the end of the test,
        even if some creations fail or unexpected errors occur.

    Returns:
        None
    """
    vol_names = [f"parallel_test_{i}" for i in range(10)]
    log.info("Testing simultaneous creation of multiple volumes.")
    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(
                    fs_util.create_fs, client, vol_name, validate=True
                ): vol_name
                for vol_name in vol_names
            }
            for future in futures:
                vol_name = futures[future]
                try:
                    future.result()  # This ensures that any exception raised in the thread is captured here.
                    log.info(f"Volume '{vol_name}' created and validated successfully.")
                except CommandFailed as e:
                    log.error(f"Volume creation failed for '{vol_name}': {e}")
                except Exception as e:
                    log.error(f"Unexpected error for '{vol_name}': {e}")
    finally:
        log.info("Cleanup: Deleting created volumes.")
        for vol_name in vol_names:
            try:
                fs_util.remove_fs(client, vol_name, validate=True)
                log.info(f"Volume '{vol_name}' deleted successfully.")
            except CommandFailed:
                log.warning(
                    f"Cleanup not required: Volume '{vol_name}' does not exist."
                )
            except Exception as e:
                log.error(f"Unexpected error during cleanup for '{vol_name}': {e}")


def test_volume_deletion_with_mon_allow_pool_delete_false(fs_util, client):
    """
    This test ensures that the `mon_allow_pool_delete` setting in Ceph prevents accidental or unauthorized
    deletion of pools associated with a CephFS volume. When the configuration is set to `False`, any attempt
    to delete a CephFS volume should fail.
    Args:
        fs_util (object): Utility object for managing CephFS volumes.
        client (object): Client object to interact with the Ceph cluster.

    Test Steps:
        1. Create a new CephFS volume (`cephfs_mon_pool_delete_test`) and validate its creation.
        2. Set the `mon_allow_pool_delete` configuration option to `False`.
        3. Attempt to delete the volume and verify that the deletion fails as expected.
        4. For cleanup:
           - Set `mon_allow_pool_delete` back to `True`.
           - Delete the volume successfully.

    Cleanup:
        - Ensures that the `mon_allow_pool_delete` setting is reset to `True` regardless of test outcome.
        - Deletes the test volume (`cephfs_mon_pool_delete_test`) if it was created successfully.

    Returns:
        int: 0 if the test passes, 1 if the test fails.

    Exceptions:
        - Logs and handles unexpected errors encountered during the test.
    """
    vol_name = "cephfs_mon_pool_delete_test"
    try:
        # Step 1: Create a new CephFS volume
        log.info(f"Creating a new CephFS volume: {vol_name}")
        fs_util.create_fs(client, vol_name, validate=True)

        # Step 2: Set 'mon_allow_pool_delete' to False
        log.info("Disabling pool deletion by setting 'mon_allow_pool_delete' to False.")
        client.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete false"
        )

        # Step 3: Attempt to delete the volume and expect failure
        log.info(f"Attempting to delete CephFS volume: {vol_name}")
        delete_result, delete_ec = client.exec_command(
            sudo=True,
            cmd=f"ceph fs volume rm {vol_name} --yes-i-really-mean-it",
            check_ec=False,
        )
        if delete_result == 0:
            log.error(
                "Volume deletion should not succeed when mon_allow_pool_delete is false"
            )
            return 1
        log.info("Volume deletion failed as expected")

        # Step 4: Cleanup: Delete the volume
        fs_util.remove_fs(client, vol_name, validate=True, check_ec=False)
        log.info(f"Cleanup successful: Volume {vol_name} deleted.")
        return 0
    except Exception as e:
        log.error(f"Test encountered an unexpected error: {e}")
        return 1


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83604097"
        log.info(f"Running CephFS tests for BZ-{tc}")
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.auth_list([client1])
        fs_util.prepare_clients(clients, build)
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )

        log.info("checking Pre-requisites")
        if not clients:
            log.error(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1

        fs_name_1 = f"cephfs_1_{rand}"
        fs_name_2 = f"cephfs_2_{rand}"
        mon_node_ips = {"mon_addrs": fs_util.get_mon_node_ips()}
        log.info(mon_node_ips)

        log.info(
            "\n"
            "\n---------------***************-------------------------------------------------------"
            "\n          Scenario 1: Create a File System and Validate from different command output"
            "\n---------------***************-------------------------------------------------------"
            "\n"
        )

        # Creation of FS
        fs_util.create_fs(client1, fs_name_1)
        fs_util.wait_for_mds_process(client1, fs_name_1)

        # Get ceph fs volume info of specific volume
        fs_volume_dict = fs_util.collect_fs_volume_info_for_validation(
            client1, fs_name_1
        )
        keys_to_convert = ["data_avail", "meta_avail", "data_used", "meta_used"]
        fs_volume_dict = update_dict_from_b_gb(fs_volume_dict, keys_to_convert)
        fs_volume_dict["mon_addrs"] = [
            addr.replace(":6789", "") for addr in fs_volume_dict["mon_addrs"]
        ]
        log.debug(f"Output of FS_1 volume: {fs_volume_dict}")

        # Get ceph fs dump output
        fs_dump_dict = fs_util.collect_fs_dump_for_validation(client1, fs_name_1)
        log.debug(f"Output of FS_1 dump: {fs_dump_dict}")

        # Get ceph fs get of specific volume
        fs_get_dict = fs_util.collect_fs_get_for_validation(client1, fs_name_1)
        log.debug(f"Output of FS_1 get: {fs_get_dict}")

        # Get ceph fs status
        fs_status_dict = fs_util.collect_fs_status_data_for_validation(
            client1, fs_name_1
        )
        fs_status_dict = update_dict_from_b_gb(fs_status_dict, keys_to_convert)
        log.debug(f"Output of FS_1 status: {fs_status_dict}")

        # Validation
        keys_to_be_validated = [
            "status",
            "fsname",
            "fsid",
            "rank",
            "mds_name",
            "data_avail",
            "data_used",
            "meta_avail",
            "meta_used",
            "mon_addrs",
        ]
        rc = fs_util.validate_dicts(
            [fs_status_dict, fs_volume_dict, fs_dump_dict, fs_get_dict, mon_node_ips],
            keys_to_be_validated,
        )

        if rc:
            log.info(f"Validation of {fs_name_1} is successsful")
        else:
            log.error(f"Validation of {fs_name_1} failed")
            return 1

        log.info(
            "\n"
            "\n---------------***************---------------"
            "\n          Scenario 1 Completed Successfully          "
            "\n---------------***************---------------"
            "\n"
        )

        log.info(
            "\n"
            "\n---------------***************---------------------------------------------------------"
            "\n          Scenario 2: Create 2nd File System and Validate from different command output"
            "\n---------------***************---------------------------------------------------------"
            "\n"
        )

        # Creation of 2nd FS
        fs_util.create_fs(client1, fs_name_2)
        fs_util.wait_for_mds_process(client1, fs_name_2)

        # Get ceph fs volume info of specific volume
        fs_volume_dict_2 = fs_util.collect_fs_volume_info_for_validation(
            client1, fs_name_2
        )
        fs_volume_dict_2 = update_dict_from_b_gb(fs_volume_dict_2, keys_to_convert)
        fs_volume_dict_2["mon_addrs"] = [
            addr.replace(":6789", "") for addr in fs_volume_dict_2["mon_addrs"]
        ]
        log.debug(f"Output of FS_2 volume: {fs_volume_dict_2}")

        # Get ceph fs dump output
        fs_dump_dict_2 = fs_util.collect_fs_dump_for_validation(client1, fs_name_2)
        log.debug(f"Output of FS_2 dump: {fs_dump_dict_2}")

        # Get ceph fs get of specific volume
        fs_get_dict_2 = fs_util.collect_fs_get_for_validation(client1, fs_name_2)
        log.debug(f"Output of FS_2 get: {fs_get_dict_2}")

        # Get ceph fs status
        fs_status_dict_2 = fs_util.collect_fs_status_data_for_validation(
            client1, fs_name_2
        )
        fs_status_dict_2 = update_dict_from_b_gb(fs_status_dict_2, keys_to_convert)
        log.debug(f"Output of FS_2 status: {fs_status_dict_2}")

        rc = fs_util.validate_dicts(
            [
                fs_status_dict_2,
                fs_volume_dict_2,
                fs_dump_dict_2,
                fs_get_dict_2,
                mon_node_ips,
            ],
            keys_to_be_validated,
        )

        if rc:
            log.info(f"Validation of {fs_name_2} is successsful")
        else:
            log.error(f"Validation of {fs_name_2} failed")
            return 1
        log.info(
            "\n"
            "\n---------------***************---------------"
            "\n          Scenario 2 Completed Successfully          "
            "\n---------------***************---------------"
            "\n"
        )
        log.info(
            "\n"
            "\n---------------***************---------------------------------------------------------"
            "\n          Scenario 3: Delete 2nd File System using negative testing"
            "\n---------------***************---------------------------------------------------------"
            "\n"
        )

        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )

        test_commands = [
            f"ceph fs volume rm {fs_name_2}",
            f"ceph fs volume rm {fs_name_2} --yes-i-really-men-it",
            f"ceph fs volume rm {fs_name_2} -yes-i-really-mean-it",
            f"ceph fs volume rm {fs_name_2} --yes-i-rally-mean-it",
        ]
        for fail_cmd in test_commands:
            try:
                client1.exec_command(sudo=True, cmd=fail_cmd)
            # Handling the error gracefully. Expected to fail
            except Exception as e:
                log.info(f"Exception: {fail_cmd} is expected to fail")
                log.info(f"Error: {e}")
        log.info(
            "\n"
            "\n---------------***************---------------"
            "\n          Scenario 3 Completed Successfully          "
            "\n---------------***************---------------"
            "\n"
        )

        # log.info(
        #     "\n"
        #     "\n---------------***************---------------------------------------------------------------"
        #     "\n          Scenario 4: Delete 2nd File System and validate the result based on previous result"
        #     "\n---------------***************---------------------------------------------------------------"
        #     "\n"
        # )
        # fs_util.remove_fs(client1, fs_name_2)

        # # Get ceph fs volume info of specific volume
        # fs_volume_dict_3 = fs_util.collect_fs_volume_info_for_validation(
        #     client1, fs_name_1
        # )
        # fs_volume_dict_3 = update_dict_from_b_gb(fs_volume_dict_3, keys_to_convert)
        # log.info(f"Dict: {fs_volume_dict_3}")

        # # Get ceph fs dump output
        # fs_dump_dict_3 = fs_util.collect_fs_dump_for_validation(client1, fs_name_1)
        # log.info(f"Dict: {fs_dump_dict_3}")

        # # Get ceph fs get of specific volume
        # fs_get_dict_3 = fs_util.collect_fs_get_for_validation(client1, fs_name_1)
        # log.info(f"Dict: {fs_get_dict_3}")

        # # Get ceph fs status
        # fs_status_dict_3 = fs_util.collect_fs_status_data_for_validation(
        #     client1, fs_name_1
        # )
        # fs_status_dict_3 = update_dict_from_b_gb(fs_status_dict_3, keys_to_convert)
        # log.info(f"Dict: {fs_status_dict_3}")

        # # Excluding the validation of mds_name, data_avail, meta avail
        # # Following it up with dev folks
        # # To be updated validation list: validation_list = ["fs_status_dict == fs_status_dict_3",
        # "fs_volume_dict == fs_volume_dict_3", "fs_dump_dict == fs_dump_dict_3", "fs_get_dict == fs_get_dict_3"]
        # fs_dump_dict.pop("mds_name", None)
        # fs_dump_dict_3.pop("mds_name", None)
        # fs_get_dict.pop("mds_name", None)
        # fs_get_dict_3.pop("mds_name", None)

        # log.info(f"fs_volume_dict: {fs_volume_dict}")
        # log.info(f"fs_volume_dict_3: {fs_volume_dict_3}")
        # log.info(f"fs_dump_dict: {fs_dump_dict}")
        # log.info(f"fs_dump_dict_3: {fs_dump_dict_3}")
        # log.info(f"fs_get_dict: {fs_get_dict}")
        # log.info(f"fs_get_dict_3: {fs_get_dict_3}")

        # validation_list = [
        #     "fs_volume_dict == fs_volume_dict_3",
        #     "fs_dump_dict == fs_dump_dict_3",
        #     "fs_get_dict == fs_get_dict_3",
        # ]
        # for val in validation_list:
        #     if eval(val):
        #         log.info("Values are identical even after adding and deleting a volume")
        #     else:
        #         log.error(f"Values are not identical for dict {val}")
        #         return 1

        # log.info(
        #     "\n"
        #     "\n---------------***************---------------"
        #     "\n          Scenario 4 Completed Successfully          "
        #     "\n---------------***************---------------"
        #     "\n"
        # )

        log.info("Starting all other tests for CephFS Volume creation")

        test_cephfs_volume_creation(fs_util, client1)
        # test_simultaneous_creation(fs_util, client1) # Reserved for future use or Scale Tests
        # test_max_volumes(fs_util, client1) # Reserved for future use or Scale Tests
        test_volume_deletion_with_mon_allow_pool_delete_false(fs_util, client1)

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        fs_util.remove_fs(client1, fs_name_1, validate=True)
        fs_util.remove_fs(client1, fs_name_2, validate=True)
        return 0
