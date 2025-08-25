import json
import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)
"""
pre-requisites:
1. The cluster should be up and remove all the existing filesystem if any
Test operation:
1.Create a new FS with custom name using cmd "fs new <filesystem name> <metadata pool name> <data pool name>"
2.List the existing FS using cmd "fs ls"
3.Remove the FS created in Step 2 using cmd "fs rm <filesystem name> [--yes-i-really-mean-it]"
4.Do Step 2 and reset the FS using cmd "fs reset <filesystem name>"-> pacific 없음
5.Get attributes of the FS using cmd "fs get <filesystem name>"
6.Set attributed of the FS using cmd "fs set <filesystem name> <var> <val>"
7.Add one more data pool to the existing FS using cmd "fs add_data_pool <filesystem name> <pool name/id>"
8.Remove a data-pol from the existing FS using cmd "fs rm_data_pool <filesystem name> <pool name/id>"
"""


def run(ceph_cluster, **kw):
    pool_list = []
    try:
        tc = "CEPH-11333"
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
        out1, ec1 = client1.exec_command(sudo=True, cmd="ceph fs ls --format json")
        output1 = json.loads(out1)
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        for output in output1:
            log.info(output)
            exist_fs = output["name"]
            client1.exec_command(sudo=True, cmd=f"ceph fs fail {exist_fs}")
            client1.exec_command(
                sudo=True, cmd=f"ceph fs rm {exist_fs} --yes-i-really-mean-it"
            )

        pool_data = f"cephfs_data_{rand}"
        pool_meta = f"cephfs_metadata_{rand}"
        pool_list.append(pool_meta)
        pool_list.append(pool_data)
        client1.exec_command(sudo=True, cmd=f"ceph osd pool create {pool_data}")
        client1.exec_command(sudo=True, cmd=f"ceph osd pool create {pool_meta}")

        fs_name = f"cephfs_{rand}"

        create_cmd = f"ceph fs new {fs_name} {pool_meta} {pool_data}"

        client1.exec_command(sudo=True, cmd=create_cmd)
        client1.exec_command(sudo=True, cmd=f"ceph fs fail {fs_name}")

        client1.exec_command(
            sudo=True, cmd=f"ceph fs rm {fs_name} --yes-i-really-mean-it"
        )

        client1.exec_command(sudo=True, cmd=f"ceph osd pool create {pool_data}_1")
        client1.exec_command(sudo=True, cmd=f"ceph osd pool create {pool_meta}_1")
        pool_list.append(f"{pool_data}_1")
        pool_list.append(f"{pool_meta}_1")
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        create_cmd = f"ceph fs new {fs_name} {pool_meta}_1 {pool_data}_1"

        client1.exec_command(sudo=True, cmd=create_cmd)
        retry_mount = retry(CommandFailed, tries=3, delay=30)(fs_util.kernel_mount)
        retry_mount([client1], kernel_mounting_dir_1, ",".join(mon_node_ips))

        out7, ec7 = client1.exec_command(sudo=True, cmd=f"ceph fs get {fs_name}")
        log.info(out7)
        log.info(ec7)
        fs_util.run_ios(
            client1, kernel_mounting_dir_1, ["dd", "smallfile"], file_name=rand
        )

        out8, ec8 = client1.exec_command(
            sudo=True, cmd=f"ceph fs set {fs_name} max_mds 3"
        )
        log.info(out8)
        # Adding addiotional pool in the cephfs since we can not remove the default pool
        pool_name_remove = "remove_pool"
        client1.exec_command(sudo=True, cmd=f"ceph osd pool create {pool_name_remove}")
        client1.exec_command(
            sudo=True, cmd=f"ceph fs add_data_pool {fs_name} {pool_name_remove}"
        )
        client1.exec_command(
            sudo=True, cmd=f"ceph fs rm_data_pool {fs_name} {pool_name_remove}"
        )

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        if kernel_mounting_dir_1:
            log.info(f"Cleaning up: {kernel_mounting_dir_1}")

            try:
                client1.exec_command(sudo=True, cmd=f"rm -rf {kernel_mounting_dir_1}/*")
            except Exception as e:
                log.warning(
                    f"Failed to delete contents of {kernel_mounting_dir_1}: {e}"
                )

            try:
                client1.exec_command(
                    sudo=True, cmd=f"umount -f {kernel_mounting_dir_1}"
                )
            except Exception as e:
                log.warning(f"Failed to unmount {kernel_mounting_dir_1}: {e}")

            try:
                client1.exec_command(sudo=True, cmd=f"rm -rf {kernel_mounting_dir_1}")
            except Exception as e:
                log.warning(f"Failed to remove directory {kernel_mounting_dir_1}: {e}")
