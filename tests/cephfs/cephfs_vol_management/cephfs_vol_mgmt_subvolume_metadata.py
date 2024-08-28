import json
import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    pre-requisites:
    1. Subvolume created
    2. A set of test candidates for metadata
    Test operation:
    1. Create subvolume
    2. Set the metadata from the set
    3. Try to set the metadta with same key
    4. Try to remove the metadata key
    5. Check if the metadata key and value are deleted
    """
    try:
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        fs_util.auth_list([client1])
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={fs_name}",
        )
        subvol_random = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )

        subvol_name = "subvol_" + subvol_random

        fs_util.create_subvolume(client1, f"{fs_name}", subvol_name)

        key_value = {}

        number_of_values = 20

        # Creation Test

        for i in range(number_of_values):
            key_random = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(10))
            )
            value_random = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(10))
            )
            key_value[key_random] = value_random
            client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume metadata set {fs_name} {subvol_name} {key_random} {value_random}",
            )

        metadata_result_out, ec1 = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume metadata ls {fs_name} {subvol_name} --format plain",
        )
        metadata_result = json.loads(metadata_result_out)
        log.info("Metadata CREATION testing is done")

        for k1, v1 in key_value.items():
            if k1 in metadata_result and metadata_result[k1] == v1:
                pass
            else:
                log.error(
                    f"Metadata ls result have should have {k1} and value should be {v1}"
                )
                return 1
        # Update Test
        update_list = []
        for k1, v1 in key_value.items():
            update_value = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(10))
            )
            update_list.append(update_value)
            key_value[k1] = update_value
        log.info("Metadata UPDATE testing is done")
        # Metadata Get

        for k1, v1 in key_value.items():
            get_dict_out, ec1 = client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume metadata ls {fs_name} {subvol_name} --format plain",
            )
            get_dict = json.loads(get_dict_out)
            if k1 in get_dict and get_dict[k1] == v1:
                log.error(
                    f"ceph fs subvolume metadata get is not working properly, it should get {v1} value"
                )
                return 1
        log.info("Metadata GET testing is done")

        # Remove Testing

        for k1, v1 in key_value.items():
            client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume metadata rm {fs_name} {subvol_name} {k1}",
            )

            rm_dict_out, ec1 = client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume metadata ls {fs_name} {subvol_name} --format plain",
            )
            rm_dict = json.loads(rm_dict_out)
            if k1 in rm_dict:
                log.error("Deleted metadata key is still in the dict")
                return 1

        log.info("Metadata RM testing is done")

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
