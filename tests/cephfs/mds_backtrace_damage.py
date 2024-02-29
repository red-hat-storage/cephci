import random
import string
import time
import traceback

from tests.cephfs.cephfs_basic_tests import mount_test_case
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test steps:
    1. mount a directory in specific client using fuse in a pool
    2. mount a directory in specific client using kernel in the same pool
    3. create directories in the mounted directory for both clients
    4. create files in the mounted directory for both clients
    5. list out all the objects in the pool
    6. find all the objects in the pool ending with ".00000000"
    7. If the objects have attribute "parent" then it is a directory
    8. delete the "parent" attribute from the objects
    9. run scrub with no option
    10. check if the cluster is damaged state after the scrub.
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        num_dir = 20
        num_files = 10
        random_pool = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        pool_name = f"test_data_pool_{random_pool}"
        pool_name_meta = f"test_metadata_pool_{random_pool}"
        fs_name = f"cephfs_{random_pool}"
        client1.exec_command(sudo=True, cmd=f"ceph osd pool create {pool_name} 32")
        client1.exec_command(sudo=True, cmd=f"ceph osd pool create {pool_name_meta} 32")
        client1.exec_command(
            sudo=True, cmd=f"ceph fs new {fs_name} {pool_name_meta} {pool_name}"
        )
        client1.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} max_mds 2")
        fs_util.prepare_clients(clients, fs_name)
        fs_util.auth_list([client1])
        client1.exec_command(
            sudo=True, cmd=f'ceph orch apply mds {fs_name} --placement="2"'
        )
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{mounting_dir}_name1/"
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={fs_name}",
        )
        fs_util.fuse_mount(
            [client1], fuse_mounting_dir_1, extra_params=f" --client-fs={fs_name}"
        )
        dir_name = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        mount_test_case(clients, fuse_mounting_dir_1)
        mount_test_case(clients, kernel_mounting_dir_1)
        for i in range(num_dir):
            client1.exec_command(
                sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_1}/{dir_name}_{i}"
            )
            client1.exec_command(
                sudo=True, cmd=f"mkdir -p {kernel_mounting_dir_1}/{dir_name}_{i}"
            )
            for k in range(num_files):
                client1.exec_command(
                    sudo=True,
                    cmd=f"touch {fuse_mounting_dir_1}/{dir_name}_{i}/file_{k}",
                )
                client1.exec_command(
                    sudo=True,
                    cmd=f"touch {kernel_mounting_dir_1}/{dir_name}_{i}/file_{k}",
                )
        time.sleep(30)
        fs_util.run_ios(client1, fuse_mounting_dir_1, ["dd", "smallfile"])
        fs_util.run_ios(client1, kernel_mounting_dir_1, ["dd", "smallfile"])
        time.sleep(30)
        out_fs, _ = client1.exec_command(sudo=True, cmd="ceph fs status")
        print(out_fs)
        # remove the "parent" attribute from the objects
        object_list, _ = client1.exec_command(
            sudo=True, cmd=rf'rados -p {pool_name_meta} ls | grep  "\.00000000"'
        )
        log.info(object_list)
        object_list = object_list.split("\n")
        number_of_objects = len(object_list)
        log.info("number of objects: " + str(number_of_objects))
        for obj in object_list:
            if str(obj) != "":
                log.info("obj name:" + str(obj))
                xttr, _ = client1.exec_command(
                    sudo=True, cmd=f"rados -p {pool_name_meta} listxattr {str(obj)}"
                )
                log.info(xttr)
                if "parent" in xttr:
                    client1.exec_command(
                        sudo=True, cmd=f"rados -p {pool_name_meta} rmxattr {obj} parent"
                    )
        # run scrub
        client1.exec_command(sudo=True, cmd=f"ceph tell mds.{fs_name}:0 scrub start /")
        # check if the cluster is in damaged state
        time.sleep(30)
        out1, _ = client1.exec_command(sudo=True, cmd="ceph -s")
        print(out1)
        out2, _ = client1.exec_command(sudo=True, cmd="ceph fs status")
        print(out2)
        if "HEALTH_ERR" in out1:
            log.info("Cluster is in damaged state")
            return 0
        else:
            log.error("Cluster is not in damaged state")
            return 1
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        # to delete
        # 1, two pools, 2. volumes, 3. clients, 4. mounting dirs

        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )

        client1.exec_command(
            sudo=True, cmd=f"ceph fs volume rm {fs_name} --yes-i-really-mean-it"
        )
        # remove pools
        client1.exec_command(
            sudo=True,
            cmd=f"ceph osd pool delete {pool_name_meta} --yes-i-really-really-mean-it",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph osd pool delete {pool_name} --yes-i-really-really-mean-it",
        )
