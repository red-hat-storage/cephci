import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        """
         CEPH-83575624 - when a standby-replay node is laggy it should be thrown out from the MDS map
                        if the beacon grace period reaches 15sec.

        Steps:
        1. Create filesystem
        2. mount the Fs and set mas mds 2
        3. set allow_standby_replay to true/1
        4. write data
        5. collect standby reply nodes
        6. Stop the network for 10 sec on the first stnadby reply node
        7. verify if there is change in stnadby nodes
        8. Stop the network for 60 sec
        9. Verify if there change in standby reply nodes. Expectations is to have change in standby node

        Cleanup:
        1. unmount the FS
        2. Remove FS

        """
        tc = "CEPH-83575781"
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_name = "cephfs_mds_laggy"
        file = "network_disconnect.sh"
        fs_util.create_fs(client1, fs_name)
        mds_nodes = ceph_cluster.get_nodes("mds")
        host_list = [node.hostname for node in mds_nodes]
        hosts = " ".join(host_list)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph orch apply mds {fs_name} --placement='5 {hosts}'",
            check_ec=False,
        )
        client1.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} max_mds 2")
        client1.exec_command(
            sudo=True, cmd=f"ceph fs set {fs_name} allow_standby_replay 1"
        )
        fs_util.wait_for_mds_process(client1, fs_name)

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

        standby_reply_mdss = fs_util.get_standby_replay_mdss(client1, fs_name=fs_name)
        standby_reply_mdss_objs = [
            ceph_cluster.get_node_by_hostname(i.split(".")[1])
            for i in standby_reply_mdss
        ]
        standby_reply_mdss_objs[0].upload_file(
            sudo=True,
            src="tests/cephfs/cephfs_bugs/network_disconnect.sh",
            dst=f"/root/{file}",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs status {fs_name}",
        )
        log.info(out)
        standby_reply_mdss_objs[0].exec_command(
            sudo=True,
            cmd=f"bash /root/{file} {standby_reply_mdss_objs[0].ip_address} 10",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs status {fs_name}",
        )
        log.info(out)
        standby_reply_mdss_10 = fs_util.get_standby_replay_mdss(
            client1, fs_name=fs_name
        )
        standby_reply_mdss_objs_10 = [
            ceph_cluster.get_node_by_hostname(i.split(".")[1])
            for i in standby_reply_mdss_10
        ]
        if standby_reply_mdss != standby_reply_mdss_10:
            if standby_reply_mdss_objs[0].hostname not in standby_reply_mdss_objs_10:
                log.info(
                    f"Standby nodes has changed\n "
                    f"Before : {standby_reply_mdss} \n After : {standby_reply_mdss_10}"
                )
                return 0
            raise CommandFailed(
                f"Standby nodes has changed\n Before : {standby_reply_mdss} \n After : {standby_reply_mdss_10}"
            )
        standby_reply_mdss_objs[0].exec_command(
            sudo=True,
            cmd=f"bash /root/{file} {standby_reply_mdss_objs[0].ip_address} 90",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs status {fs_name}",
        )
        log.info(out)
        standby_reply_mdss_20 = fs_util.get_standby_replay_mdss(
            client1, fs_name=fs_name
        )

        if standby_reply_mdss == standby_reply_mdss_20:
            raise CommandFailed(
                f"Standby nodes did not change\n Before : {standby_reply_mdss} \n After : {standby_reply_mdss_20}"
            )

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
        standby_node_obj = ceph_cluster.get_ceph_objects()
        for node in standby_node_obj:
            if node.node.hostname == standby_reply_mdss_objs[0].hostname:
                fs_util.wait_for_host_online(client1, node)
                break
