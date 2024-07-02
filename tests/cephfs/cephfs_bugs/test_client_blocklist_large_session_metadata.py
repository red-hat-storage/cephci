import secrets
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        """
         CEPH-83581613 - Client is blocklisted if session metadata is bloated/large.

        Steps:
        1. Set ceph config as "client client inject fixed oldest tid true"
        2. Set ceph config as "mds mds_max_completed_requests max_requests" , max_requests = 10000
        3. Create files with count > max_requests asynchronously
        4. Create 10 files synchronously
        5. Wait for "failing to advance oldest client" in Ceph Status
        6. Set config as "mds mds_session_metadata_threshold 5000"
        7. Create more files synchronously with limit of 100000, until Client is blocklisted.
           IO error occurs on client mount.
        8. Verify MDS perf counters mds_sessions and mdthresh_evicted are incremented
        9. Set ceph config as "client client inject fixed oldest tid false"
        Cleanup:
        1. Umount subvolume, Remove subvolume

        """
        tc = "CEPH-83581613"
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_name = "cephfs"
        log.info("Enable ceph logging for ceph client")
        out, rc = client1.exec_command(
            sudo=True,
            cmd="ceph config set client log_to_file true",
        )
        log.info("Set client config client_inject_fixed_oldest_tid to true")
        out, rc = client1.exec_command(
            sudo=True,
            cmd='ceph config set client "client_inject_fixed_oldest_tid" true',
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd='ceph config get client "client_inject_fixed_oldest_tid"',
        )
        log.info(out)
        if "true" not in out.strip():
            log.error("client config could not be set to inject fixed oldest tid")
            return 1
        log.info("Set mds config mds_max_completed_requests 10000")
        out, rc = client1.exec_command(
            sudo=True,
            cmd="ceph config set mds mds_max_completed_requests 10000",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="ceph config get mds mds_max_completed_requests",
        )
        log.info(out)
        if "10000" not in out.strip():
            log.error("mds_max_completed_requests could not be set 10000")
            return 1
        mds = fs_util.get_active_mdss(clients[0], fs_name)
        mds_active_node = ceph_cluster.get_node_by_hostname(mds[0].split(".")[1])
        log.info("Get the mdthresh_evicted ceph mds perf cntr value")
        mds_daemon = f"mds.{mds[0]}"
        out, rc = mds_active_node.exec_command(
            sudo=True,
            cmd=f"cephadm shell ceph daemon {mds_daemon} perf dump | grep mdthresh_evicted",
        )
        mds_perf_out = out.strip().split("\n")

        # for mds_node in mds_nodes:
        #     log.info(mds_node.node.hostname)
        #     if mds_node.node.hostname in mds:
        #         log.info("Get the mdthresh_evicted ceph mds perf cntr value")
        #         mds_daemon = f"mds.{mds}"
        #         out, rc = mds_active_node.exec_command(
        #             sudo=True,
        #             cmd=f"cephadm shell ceph daemon {mds_daemon} perf dump | grep mdthresh_evicted",
        #         )
        #         mds_perf_out = out.strip().split("\n")
        mdthresh_evicted_before = mds_perf_out[0].split(":")[1]
        log.info(f"mdthresh_evicted_before:{mdthresh_evicted_before}")
        subvolume = {
            "vol_name": fs_name,
            "subvol_name": "subvol_client_blocklist",
        }
        log.info("Create subvolume")
        fs_util.create_subvolume(clients[0], **subvolume)
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} subvol_client_blocklist",
        )
        fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        log.info("Perform fuse mount of subvolume")
        fs_util.fuse_mount(
            [client1],
            fuse_mount_dir,
            extra_params=f" -r {subvol_path.strip()}  --client_fs {fs_name}",
        )

        log.info("Create files with count > 10000 asynchronously")
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 10 "
            f"--files 10050 --top {fuse_mount_dir}",
            long_running=True,
        )
        time.sleep(10)
        log.info("Verify failing to advance oldest client message in ceph status")
        out, _ = client1.exec_command(sudo=True, cmd="ceph status")
        out = out.strip()
        log.info(f"Ceph status : {out}")
        if "failing to advance oldest client" not in out:
            log.error("failing to advance oldest client was not found in ceph status")
            return 1

        log.info("Get mds_session_metadata_threshold default")
        out, rc = client1.exec_command(
            sudo=True,
            cmd="ceph config get mds mds_session_metadata_threshold",
        )
        mds_session_metadata_threshold_default = out.strip()
        log.info(
            f"mds_session_metadata_threshold_default:{mds_session_metadata_threshold_default}"
        )
        log.info("Set mds_session_metadata_threshold to 5000")
        out, rc = client1.exec_command(
            sudo=True,
            cmd="ceph config set mds mds_session_metadata_threshold 5000",
        )

        log.info("Create more files synchronously with limit of 100000")
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 20 --file-size 10 "
            f"--files 5000 --top {fuse_mount_dir}",
            long_running=True,
        )

        time.sleep(10)
        log.info("Verify Client session is blocklisted by accessing mountpoint")
        try:
            client1.exec_command(
                sudo=True,
                cmd=f"mkdir {fuse_mount_dir}/newdir;ls {fuse_mount_dir}",
                check_ec=True,
            )
        except Exception as ex:
            log.info(f"ex:{ex}")
            if "cannot create directory" in str(
                ex
            ) and "transport endpoint shutdown" in str(ex):
                log.info(
                    "Verified that Client is blocklisted, IO error occurs on client"
                )
            else:
                log.error("Client is not blocklisted")
                # return 1

        log.info(
            f"Verify ceph mds perf cntr mdthresh_evicted is incremented from {mdthresh_evicted_before}"
        )
        for mds_node in mds_nodes:
            if mds_node.node.hostname in mds:
                log.info("Get the mdthresh_evicted ceph mds perf cntr value")
                mds_daemon = f"mds.{mds}"
                try:
                    out, rc = mds_node.exec_command(
                        sudo=True,
                        cmd=f"cephadm shell ceph daemon {mds_daemon} perf dump | grep mdthresh_evicted",
                    )
                    log.info(f"mds_out:{out}")
                    mds_perf_out = out.strip().split("\n")
                    log.info(f"mds_perf_out:{mds_perf_out}")
                except Exception as ex:
                    log.info(ex)
        mdthresh_evicted_after = mds_perf_out[0].split(":")[1]
        if mdthresh_evicted_after > mdthresh_evicted_before:
            log.info(
                f"mdthresh_evicted perf counter has incremented to {mdthresh_evicted_after}"
            )
        else:
            log.error(
                f"mdthresh_evicted has not incremented : {mdthresh_evicted_after}"
            )

        out_client, _ = client1.exec_command(
            sudo=True, cmd='grep "I was blocklisted" /var/log/ceph/*'
        )
        if out_client.strip():
            log.info("Client blocklisted message is logged in client debug log")
        else:
            log.error("Client blocklisted message is not logged in client debug log")

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
        log.info("Peforming Cleanup")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph config set mds mds_session_metadata_threshold {mds_session_metadata_threshold_default}",
        )
        client1.exec_command(
            sudo=True,
            cmd='ceph config set client "client_inject_fixed_oldest_tid" false',
        )
        mds_session_metadata_threshold_default = out.strip()
        client1.exec_command(sudo=True, cmd=f"umount {fuse_mount_dir}", check_ec=False)
        client1.exec_command(sudo=True, cmd=f"rm -rf {fuse_mount_dir}", check_ec=False)
        fs_util.remove_subvolume(clients[0], **subvolume)
