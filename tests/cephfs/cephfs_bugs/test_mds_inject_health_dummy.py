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
         CEPH-83586730 - validate standby replay node not removed after setting mds_inject_health_dummy

        Steps:
        1. Set max_mds 1
        2. allow standby replay
        3. check the gid of the standby replay
        4. set mds_inject_health_dummy on stanby replay node
        5. check the gid again. expectation is  There should not be any change in gid of standby-replay node

        Cleanup:
        1. unmount the FS
        2. Remove FS

        """
        tc = "CEPH-83586730"
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_name = "cephfs_mds_inject"
        fs_util.create_fs(client1, fs_name)
        mds_nodes = ceph_cluster.get_nodes("mds")
        host_list = [node.hostname for node in mds_nodes]
        hosts = " ".join(host_list)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph orch apply mds {fs_name} --placement='3 {hosts}'",
            check_ec=False,
        )
        client1.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} max_mds 1")
        client1.exec_command(
            sudo=True, cmd=f"ceph fs set {fs_name} allow_standby_replay 1"
        )
        fs_util.wait_for_standby_replay_mds(client1, fs_name)

        stanby_replay_before, rc = client1.exec_command(
            sudo=True, cmd="ceph fs dump -f json"
        )
        info_before = get_info(json.loads(stanby_replay_before), fs_name)
        standby_replay_dict_before = {
            key: value
            for key, value in info_before.items()
            if "standby-replay" in value["state"]
        }

        standby_replay_mds = retry_get_standby_reply(fs_util, client1, fs_name=fs_name)
        log.info(f"standby_replay_mds : {standby_replay_mds}")
        standby_replay_mdss_obj = [
            ceph_cluster.get_node_by_hostname(i.split(".")[1])
            for i in standby_replay_mds
        ]
        log.info(f"standby_replay_mdss_obj : {standby_replay_mdss_obj}")
        out, rc = client1.exec_command(sudo=True, cmd="ceph orch ps -f json")
        output = json.loads(out)
        deamon_dict = fs_util.filter_daemons(output, "mds", fs_name)
        for node in deamon_dict:
            for hostname, deamon in node.items():
                if hostname in standby_replay_mdss_obj[0].hostname:
                    standby_replay_mdss_obj[0].exec_command(
                        sudo=True,
                        cmd=f"cephadm shell -- ceph daemon {deamon} config set mds_inject_health_dummy true",
                    )
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
        time.sleep(60)

        stanby_replay_after, rc = client1.exec_command(
            sudo=True, cmd="ceph fs dump -f json"
        )

        info_after = get_info(json.loads(stanby_replay_after), fs_name)
        standby_replay_dict_after = {
            key: value
            for key, value in info_after.items()
            if "standby-replay" in value["state"]
        }
        log.info(standby_replay_dict_before)
        log.info(standby_replay_dict_after)
        gid_before = [value["gid"] for value in standby_replay_dict_before.values()]
        gid_after = [value["gid"] for value in standby_replay_dict_after.values()]

        if gid_before != gid_after:
            log.error(
                f"gid values are not matching before : {gid_before} and after : {gid_after}"
            )
            return 1

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


@retry(CommandFailed, tries=3, delay=60)
def retry_get_standby_reply(fs_util, client1, fs_name):
    standby_replay_mds = fs_util.get_standby_replay_mdss(client1, fs_name=fs_name)
    if not standby_replay_mds:
        out, rc = client1.exec_command(sudo=True, cmd=f"ceph fs status {fs_name}")
        log.info(out)
        raise CommandFailed("Unable to get standby reply MDS")
    return standby_replay_mds
