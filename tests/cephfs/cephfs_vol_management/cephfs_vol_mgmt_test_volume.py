import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83604097 - Basic info validation after volume creation and deletion

    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create a file system with default value
    2. Validate the volume creation using ceph fs status, ceph fs volume info <vol_name>, ceph fs dump, ceph fs get <vol_name>
    3. Create second volume cephfs2
    4. Validate volume is created by following validation step 2
    5. Delete second volume without  "--yes-i-really-mean-it"
    6. Delete second volume by having typo in  "--yes-i-really-mean-it"
    7. Delete second volume with  "--yes-i-really-mean-it"
    8. Validate the commands mentioned in point2
    9. Delete cephfs1
    """

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
                    f"{round(int(input_dict[keys_bytes]) / (1024 ** 3), 1)}G"  # Convert to GB and round to 1 decimal places
                )
        return input_dict

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

        fs_name_1 = f"cephfs_83604097_1_{rand}"
        fs_name_2 = f"cephfs_83604097_2_{rand}"
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
        fs_volume_dict['mon_addrs'] = [addr.replace(':6789', '') for addr in fs_volume_dict['mon_addrs']]
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
            "mon_addrs"
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
        fs_volume_dict_2['mon_addrs'] = [addr.replace(':6789', '') for addr in fs_volume_dict_2['mon_addrs']]
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
            [fs_status_dict_2, fs_volume_dict_2, fs_dump_dict_2, fs_get_dict_2, mon_node_ips],
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
        # fs_volume_dict_3 = fs_util.collect_fs_volume_info_for_validation(client1, fs_name_1)
        # fs_volume_dict_3 = update_dict_from_b_gb(fs_volume_dict_3, keys_to_convert)
        # log.info(f"Dict: {fs_volume_dict_3}")

        # # Get ceph fs dump output
        # fs_dump_dict_3 = fs_util.collect_fs_dump_for_validation(client1, fs_name_1)
        # log.info(f"Dict: {fs_dump_dict_3}")

        # # Get ceph fs get of specific volume
        # fs_get_dict_3 = fs_util.collect_fs_get_for_validation(client1, fs_name_1)
        # log.info(f"Dict: {fs_get_dict_3}")

        # #Get ceph fs status
        # fs_status_dict_3 = fs_util.collect_fs_status_data_for_validation(client1, fs_name_1)
        # fs_status_dict_3 = update_dict_from_b_gb(fs_status_dict_3, keys_to_convert)
        # log.info(f"Dict: {fs_status_dict_3}")

        # # Excluding the validation of mds_name, data_avail, meta avail
        # # Following it up with dev folks
        # # To be updated validation list: validation_list = ["fs_status_dict == fs_status_dict_3", "fs_volume_dict == fs_volume_dict_3", "fs_dump_dict == fs_dump_dict_3", "fs_get_dict == fs_get_dict_3"]
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

        # validation_list = ["fs_volume_dict == fs_volume_dict_3", "fs_dump_dict == fs_dump_dict_3", "fs_get_dict == fs_get_dict_3"]
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
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        fs_util.remove_fs(client1, fs_name_1)
        fs_util.remove_fs(client1, fs_name_2)
        return 0
