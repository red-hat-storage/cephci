import json
import secrets
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        """
         CEPH-83583721 - Validate the details of all MDS servers are displayed with command `ceph mds metadata`.

        Steps:
        1. Create Filesystem with 2 active and 1 standby
        2. validate mds metadata info
        3. Make 1 active and 2 standby and enable standby reply
        4. validate mds metadata info
        5. Stop active MDS and Validate metadata info
        6. Stop Standby replay MDs and Validate metadata info

        Cleanup:
        1. unmount the FS
        2. Remove FS

        """
        tc = "CEPH-83583721"
        log.info("Running cephfs %s test case" % (tc))
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_name = "cephfs_mds_metadata"
        fs_util.create_fs(client1, fs_name)
        mds_nodes = ceph_cluster.get_nodes("mds")
        host_list = [node.hostname for node in mds_nodes]
        hosts = " ".join(host_list)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph orch apply mds {fs_name} --placement='3 {hosts}'",
            check_ec=False,
        )
        client1.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} max_mds 2")
        fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util.fuse_mount(
            [client1],
            fuse_mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs_name}",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"cd {fuse_mount_dir};"
            f"mkdir subdir;"
            f"dd if=/dev/urandom of=subdir/sixmegs bs=1M conv=fdatasync count=6 seek=0;",
        )
        log.info("Scenario 1: Validate Active MDS and check metadata")
        validate_metadata_fs_status(
            client1,
            fs_name,
        )

        log.info("set allow_standby_replay to true ")
        client1.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} max_mds 1")
        validate_metadata_fs_status(client1, fs_name)

        log.info("Scenario 2: Validate standby replay MDS and check metadata")
        client1.exec_command(
            sudo=True, cmd=f"ceph fs set {fs_name} allow_standby_replay 1"
        )
        fs_util.wait_for_standby_replay_mds(client1, fs_name)
        validate_metadata_fs_status(client1, fs_name)

        log.info("Scenario 3: Fail Active MDS and check metadata")
        mds_ls = fs_util.get_active_mdss(client1, fs_name=fs_name)
        mds_active_node = ceph_cluster.get_node_by_hostname(mds_ls[0].split(".")[1])
        service_name = fs_util.deamon_name(mds_active_node, service=fs_name)
        fs_util.deamon_op(
            mds_active_node, service_name=service_name, op="stop", service=fs_name
        )
        validate_metadata_fs_status(client1, fs_name)

        fs_util.deamon_op(
            mds_active_node, service_name=service_name, op="start", service=fs_name
        )

        log.info("Scenario 4: Fail standby MDS and check metadata")
        mds_ls = fs_util.get_standby_replay_mdss(client1, fs_name=fs_name)
        mds_standby_node = ceph_cluster.get_node_by_hostname(mds_ls[0].split(".")[1])
        service_name = fs_util.deamon_name(mds_standby_node, service=fs_name)
        fs_util.deamon_op(
            mds_standby_node, service_name=service_name, op="stop", service=fs_name
        )
        validate_metadata_fs_status(client1, fs_name)
        fs_util.deamon_op(
            mds_standby_node, service_name=service_name, op="start", service=fs_name
        )
        time.sleep(60)
        return 0

    except CommandFailed as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        client1.exec_command(sudo=True, cmd=f"umount {fuse_mount_dir}", check_ec=False)
        client1.exec_command(sudo=True, cmd=f"rm -rf {fuse_mount_dir}", check_ec=False)
        client1.exec_command(
            sudo=True, cmd=f"ceph fs set {fs_name} allow_standby_replay 0"
        )
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        fs_util.remove_fs(client1, fs_name)


def get_info(json_output, fs_name):
    for i in json_output["filesystems"]:
        if i["mdsmap"]["fs_name"] == fs_name:
            return i["mdsmap"]["info"]


def get_mds_metadata(json_output, fs_name):
    filtered_data = [
        item for item in json_output if item["name"].startswith(f"{fs_name}.")
    ]
    return filtered_data


@retry(CommandFailed, tries=7, delay=30)
def validate_metadata_fs_status(client1, fs_name):
    out, rc = client1.exec_command(sudo=True, cmd="ceph mds metadata -f json")
    mds_info = get_mds_metadata(json.loads(out), fs_name)
    mds_names_in_metadata = [i["name"] for i in mds_info]
    result, rc = client1.exec_command(
        sudo=True, cmd=f"ceph fs status {fs_name} --format json-pretty"
    )
    result_json = json.loads(result)
    mds_names_in_status = [i["name"] for i in result_json["mdsmap"]]

    if not all(item in mds_names_in_status for item in mds_names_in_metadata):
        log.error(
            f"metadata has differnet mds than the fs status.\n Metadata in mds is {mds_names_in_metadata}"
            f"\n Metadata in Status is {mds_names_in_status}"
        )
        raise CommandFailed(
            f"metadata has differnet mds than the fs status.\n Metadata in mds is {mds_names_in_metadata}"
            f"\n Metadata in Status is {mds_names_in_status}"
        )
