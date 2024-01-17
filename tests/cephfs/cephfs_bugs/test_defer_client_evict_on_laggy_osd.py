import json
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
         CEPH-83581592 - Defer eviction of client if OSD is laggy.

        Steps:
        1. ceph config set mds defer_client_eviction_on_laggy_osds false
        2. Make OSD laggy: send SIGSTOP, wait 120 secs, send SIGCONT
            a. grep the osds
                dparmar:~$ pgrep ceph-osd -a
                483404 /../ceph/build/bin/ceph-osd -i 0 -c /home/dparmar/CephRepo0/ceph/build/ceph.conf
                484700 /../ceph/build/bin/ceph-osd -i 1 -c /home/dparmar/CephRepo0/ceph/build/ceph.conf
                485987 /../ceph/build/bin/ceph-osd -i 2 -c /home/dparmar/CephRepo0/ceph/build/ceph.conf
            b. send SIGSTOP(used to pause the process) to an OSD, let us pick first one
                dparmar:~$ kill -SIGSTOP 483404
            c. wait for 120 seconds
            d. send SIGCONT to resume the paused OSD process
                dparmar:~$ kill -SIGCONT 483404
        3. Make client laggy by bringing down network interface, waiting for session_timeout + 30 secs and
          bring up network interface
        4. Wait for 6mins, the below mentioned lines should not be visible in mon log because some OSD(s) is/are laggy
            Health check failed: 1 client(s) laggy due to laggy OSDs (MDS_CLIENTS_LAGGY)
        Cleanup:
        1. unmount the FS

        """
        tc = "CEPH-83581592"
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        mon_node = ceph_cluster.get_ceph_objects("mon")[0]
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_name = "cephfs"
        file = "network_disconnect.sh"
        osd_nodes = ceph_cluster.get_nodes("osd")

        log.info("Enable ceph logging for mon daemon")
        out, rc = client1.exec_command(
            sudo=True,
            cmd="ceph config set mon log_to_file true",
        )
        log.info("Set mds config defer_client_eviction_on_laggy_osds to false")
        out, rc = client1.exec_command(
            sudo=True,
            cmd="ceph config set mds defer_client_eviction_on_laggy_osds false",
        )
        subvolume = {
            "vol_name": fs_name,
            "subvol_name": "subvol_osd_laggy",
        }
        log.info("Create subvolume")
        fs_util.create_subvolume(clients[0], **subvolume)
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} subvol_osd_laggy",
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
        log.info("Run IO on subvolume")
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
            f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top "
            f"{fuse_mount_dir}",
            long_running=True,
        )
        log.info("Make OSD laggy")
        mds_list = fs_util.get_active_mdss(clients[0], fs_name)
        for mds in mds_list:
            for osd_node in osd_nodes:
                if osd_node.hostname in mds:
                    log.info("Get the OSD daemon ID")
                    out, rc = osd_node.exec_command(
                        sudo=True,
                        cmd="pgrep ceph-osd -a",
                    )
                    osd_list = out.split("\n")
                    osd_ps_id = osd_list[0].split(" ")[0]
                    log.info(
                        f"Stop the OSD Daemon with ID {osd_ps_id}, wait for 120 secs and start"
                    )
                    out, rc = osd_node.exec_command(
                        sudo=True,
                        cmd=f"kill -SIGSTOP {osd_ps_id};sleep 120;kill -SIGCONT {osd_ps_id}",
                    )
        out, _ = client1.exec_command(sudo=True, cmd="ceph status")
        out = out.strip()
        log.info(f"Ceph status : {out}")

        log.info("Get client session_timeout value")
        out, rc = client1.exec_command(
            sudo=True, cmd="ceph fs dump | grep session_timeout"
        )
        out_list = out.split("\n")
        for out in out_list:
            if "session_timeout" in out:
                session_timeout = out.split("\t")[1]

        client1.upload_file(
            sudo=True,
            src="tests/cephfs/cephfs_bugs/network_disconnect.sh",
            dst=f"/root/{file}",
        )
        nw_down_time = int(session_timeout) + 30
        log.info(
            f"Bring down Client network for {nw_down_time}secs to make client laggy"
        )
        out, _ = client1.exec_command(sudo=True, cmd="hostname -I | awk '{print $1}'")
        client_ip_addr = out.strip()
        log.info(f"{out},{client_ip_addr}")
        client1.exec_command(
            sudo=True,
            cmd=f"bash /root/{file} {client_ip_addr} {nw_down_time}",
        )
        log.info(
            "Verify if Client laggy due to OSD laggy message appears in ceph status or mon log"
        )
        unexp_str = '"client(s) laggy due to laggy OSDs"'
        out, _ = mon_node.exec_command(sudo=True, cmd="cephadm ls")
        data = json.loads(out)
        fsid = data[0]["fsid"]
        out_mon, _ = mon_node.exec_command(
            sudo=True, cmd=f"cd /var/log/ceph/{fsid}/;ls *mon*log"
        )
        mon_file = out_mon.strip()
        out, _ = mon_node.exec_command(
            sudo=True,
            check_ec=False,
            cmd=f"cat /var/log/ceph/{fsid}/{mon_file}|grep {unexp_str}",
        )
        if out:
            log.error(
                f"Found unexpected Client laggy due to OSD laggy messages in mon log file : {out}"
            )

        out, _ = client1.exec_command(sudo=True, cmd="ceph status")
        out = out.strip()
        log.info(f"Ceph status : {out}")
        if unexp_str in out.strip():
            log.error(
                "Found unexpected Client laggy due to OSD laggy messages in ceph status"
            )

        log.info("Verify if existing mountpoint is still accessible")
        out, _ = client1.exec_command(
            sudo=True, cmd=f"ls {fuse_mount_dir}|grep file_srcdir", check_ec=False
        )
        log.info(out)
        if "file_srcdir" not in out:
            log.error("Client mountpoint is NOT active, session could be evicted")
            out, _ = client1.exec_command(
                sudo=True, cmd="ceph tell mds.0 session ls --format json"
            )
            out = json.loads(out)
            for session_item in out:
                if client_ip_addr in session_item["entity"]["addr"]["addr"]:
                    log.info(f"Client session details for debugging:{session_item}")
            return 1

        log.info("Verify client sessions were not evicted")
        out, _ = client1.exec_command(
            sudo=True, cmd="ceph tell mds.0 session ls --format json"
        )
        out = json.loads(out)
        for session_item in out:
            if client_ip_addr in session_item["entity"]["addr"]["addr"]:
                if session_item["state"] == "open":
                    log.info(
                        f"Client session {session_item['id']} is open, not evicted, as expected"
                    )
                else:
                    log.error(
                        f"Client session {session_item['id']} is evicted:{session_item}"
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
        client1.exec_command(
            sudo=True,
            cmd="ceph config set mds defer_client_eviction_on_laggy_osds true",
        )
        client1.exec_command(sudo=True, cmd=f"umount {fuse_mount_dir}", check_ec=False)
        client1.exec_command(sudo=True, cmd=f"rm -rf {fuse_mount_dir}", check_ec=False)
        fs_util.remove_subvolume(clients[0], **subvolume)
