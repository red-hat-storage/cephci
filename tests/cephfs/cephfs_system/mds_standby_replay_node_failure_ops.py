import json
import secrets
import string
import traceback
from datetime import datetime, timedelta
from time import sleep

from ceph.parallel import parallel
from ceph.utils import check_ceph_healthly
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)

global stop_flag
stop_flag = False


def mds_standby_replay_setup(client, default_fs, mds_nodes, fs_util):
    log.info(
        "Configure/Verify 5 MDS for FS Volume, 2 Active, 2 Standby-Replay, 1 Standby"
    )
    out, rc = client.exec_command(sudo=True, cmd=f"ceph fs get {default_fs}")
    out_list = out.split("\n")
    for line in out_list:
        if "max_mds" in line:
            str, max_mds_val = line.split()
            log.info(f"max_mds before test {max_mds_val}")
    out, rc = client.exec_command(
        sudo=True, cmd=f"ceph fs status {default_fs} --format json"
    )
    output = json.loads(out)
    mds_cnt = len(output["mdsmap"])
    if mds_cnt < 5:
        cmd = f'ceph orch apply mds {default_fs} --placement="5'
        for mds_nodes_iter in mds_nodes:
            cmd += f" {mds_nodes_iter.node.hostname}"
        cmd += '"'
        log.info("Adding 5 MDS to cluster")
        out, rc = client.exec_command(sudo=True, cmd=cmd)
    client.exec_command(
        sudo=True,
        cmd=f"ceph fs set {default_fs} max_mds 2",
    )

    if wait_for_healthy_ceph(client, fs_util, 300) == 0:
        return 1

    log.info(f"Configure 2 standby-replay MDS in {default_fs}")
    client.exec_command(
        sudo=True,
        cmd=f"ceph fs set {default_fs} allow_standby_replay true",
    )

    if wait_for_healthy_ceph(client, fs_util, 300) == 0:
        return 1


def validate_standby_replay_mds(
    client, fs_name, fs_util, initial_mds_config, initial_mds_pair
):
    mds_config = fs_util.get_mds_config(client, fs_name)
    initial_standby_replay_mds = {}
    for mds in initial_mds_config:
        if mds.get("state") == "standby-replay":
            initial_standby_replay_mds.update({mds["name"]: {"events": mds["events"]}})
    log.info(f"initial_standby_replay_mds:{initial_standby_replay_mds}")
    standby_replay_mds = {}
    for mds in mds_config:
        if mds.get("state") == "standby-replay":
            standby_replay_mds.update({mds["name"]: {"events": mds["events"]}})
    log.info(f"standby_replay_mds:{standby_replay_mds}")
    log.info(f"initial_standby_replay_mds.keys():{initial_standby_replay_mds.keys()}")
    log.info(f"standby_replay_mds.keys():{standby_replay_mds.keys()}")
    if len(initial_standby_replay_mds.keys()) == len(standby_replay_mds.keys()):
        log.info("Standby-Replay MDS count before and after test are same")
    else:
        str = f"Before : {initial_standby_replay_mds}, After : {standby_replay_mds}"
        log.error(f"StandbyReplay MDS count before and after is not same,{str}")
        return 1
    sleep(5)
    mds_config1 = fs_util.get_mds_config(client, fs_name)
    standby_replay_mds1 = {}
    mds_pair = fs_util.get_mds_standby_replay_pair(client, fs_name, mds_config)
    log.info(f"mds_pair before:{initial_mds_pair},mds_pair after: {mds_pair}")
    for mds in mds_config1:
        if mds.get("state") == "standby-replay":
            standby_replay_mds1.update({mds["name"]: {"events": mds["events"]}})
            log.info(f"Events before:{standby_replay_mds[mds['name']]['events']}")
            log.info(f"Events now:{standby_replay_mds1[mds['name']]['events']}")
            if (
                standby_replay_mds1[mds["name"]]["events"]
                != standby_replay_mds[mds["name"]]["events"]
            ):
                log.info(f"StandbyReplay MDS {mds['name']} is operational")
                return 0
    return 1


def wait_for_healthy_ceph(client1, fs_util, wait_time_secs):
    # Returns 1 if healthy, 0 if unhealthy
    ceph_healthy = 0
    end_time = datetime.now() + timedelta(seconds=wait_time_secs)
    while ceph_healthy == 0 and (datetime.now() < end_time):
        try:
            fs_util.get_ceph_health_status(client1)
            ceph_healthy = 1
        except Exception as ex:
            log.info(ex)
            log.info(
                f"Wait for sometime to check if Cluster health can be OK, current state : {ex}"
            )
            sleep(5)
    if ceph_healthy == 0:
        return 0
    return 1


def start_io_time(fs_util, client1, mounting_dir, timeout=300):
    global stop_flag
    iter = 0
    if timeout:
        stop = datetime.now() + timedelta(seconds=timeout)
    else:
        stop = 0
    while True:
        if stop and datetime.now() > stop:
            log.info("Timed out *************************")
            break
        client1.exec_command(sudo=True, cmd=f"mkdir -p {mounting_dir}/run_ios_{iter}")
        fs_util.run_ios(
            client1, f"{mounting_dir}/run_ios_{iter}", io_tools=["smallfile"]
        )
        iter = iter + 1
        if stop_flag:
            log.info("Exited as stop flag is set to True")
            break


def run(ceph_cluster, **kw):
    """
    Verify after node failure or daemon restart, Standby Replay MDS reattach correctly, and
    the system resume normal operations
    Test Steps:
    Configure standby replay MDS and run IO on FS volume
    Perform a system restart of the standby-replay node
    Perform a daemon restart of the standby-replay node
    In each case, Verify that other standby-replay daemons are reattached to the active MDSs and
    the system resume normal operations.

    """
    try:
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        osp_cred = config.get("osp_cred")
        num_of_osds = config.get("num_of_osds")
        build = config.get("build", config.get("rhbuild"))
        print(osp_cred)
        fs_name = "cephfs"
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        mon_node_ip = fs_util_v1.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        kernel_mount_dir = "/mnt/kernel_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        if mds_standby_replay_setup(clients[0], fs_name, mds_nodes, fs_util_v1):
            log.error("MDS StandbyReplay configuration failed")
            return 1
        fs_util_v1.kernel_mount(
            [clients[0]],
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname="admin",
            extra_params=f",fs={fs_name}",
        )
        fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )

        fs_util_v1.fuse_mount(
            [clients[0]],
            fuse_mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs_name}",
        )
        initial_mds_config = fs_util_v1.get_mds_config(clients[0], fs_name)
        log.info(f"Initial MDS config before node reboot test: {initial_mds_config}")
        initial_mds_pair = fs_util_v1.get_mds_standby_replay_pair(
            clients[0], fs_name, initial_mds_config
        )
        standby_replay_nodes = []
        for mds in mds_nodes:
            for mds_iter in initial_mds_config:
                if (mds.node.hostname in mds_iter["name"]) and (
                    mds_iter["state"] == "standby-replay"
                ):
                    standby_replay_nodes.append(mds)
                    log.info(f"standby replay node: {mds.node.hostname}")
        test_fail = 0
        global stop_flag
        with parallel() as p:
            global stop_flag
            p.spawn(
                start_io_time,
                fs_util_v1,
                clients[0],
                fuse_mount_dir,
                timeout=0,
            )
            p.spawn(
                start_io_time,
                fs_util_v1,
                clients[0],
                kernel_mount_dir,
            )

            for mds in standby_replay_nodes:
                cluster_health_beforeIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                try:
                    fs_util_v1.reboot_node(ceph_node=mds)
                except Exception as e:
                    stop_flag = True
                    log.error(e)
                    log.error(traceback.format_exc())

                cluster_health_afterIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                if cluster_health_afterIO == cluster_health_beforeIO:
                    log.info("cluster is healthy")
                else:
                    log.error("cluster is not healty")

            log.info("Rebooted all Standby-Replay MDS nodes and cluster is Healthy")

            mds_config = fs_util_v1.get_mds_config(clients[0], fs_name)
            log.info(f"MDS config after node reboot: {mds_config}")
            log.info("Validate Standby-Replay node configuration after node reboot")
            if validate_standby_replay_mds(
                clients[0], fs_name, fs_util_v1, initial_mds_config, initial_mds_pair
            ):
                test_fail += 1
                log.error(
                    "StandbyReplay MDS configuration validate after node reboot test failed"
                )

            initial_mds_config = fs_util_v1.get_mds_config(clients[0], fs_name)
            log.info(
                f"Initial MDS config before daemon restart test: {initial_mds_config}"
            )
            initial_mds_pair = fs_util_v1.get_mds_standby_replay_pair(
                clients[0], fs_name, initial_mds_config
            )
            if validate_standby_replay_mds(
                clients[0], fs_name, fs_util_v1, initial_mds_config, initial_mds_pair
            ):
                test_fail += 1
            standby_replay_nodes = []
            for mds in mds_nodes:
                for mds_iter in initial_mds_config:
                    if (mds.node.hostname in mds_iter["name"]) and (
                        mds_iter["state"] == "standby-replay"
                    ):
                        standby_replay_nodes.append(mds)
                        log.info(f"standby replay node: {mds.node.hostname}")
            for mds in standby_replay_nodes:
                cluster_health_beforeIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                try:
                    fs_util_v1.deamon_op(mds, rf"mds\.{fs_name}\.", "restart")
                except Exception as e:
                    stop_flag = True
                    log.error(e)
                    log.error(traceback.format_exc())

                cluster_health_afterIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                if cluster_health_afterIO == cluster_health_beforeIO:
                    log.info("cluster is healthy")
                else:
                    log.error("cluster is not healty")
            log.info("Restarted Standby-Replay mds services and cluster is Healthy")

            mds_config = fs_util_v1.get_mds_config(clients[0], fs_name)
            log.info(f"MDS config after daemon restart: {mds_config}")
            log.info("Validate Standby-Replay node configuration after daemon restart")
            if validate_standby_replay_mds(
                clients[0], fs_name, fs_util_v1, initial_mds_config, initial_mds_pair
            ):
                test_fail += 1
            initial_mds_config = fs_util_v1.get_mds_config(clients[0], fs_name)
            log.info(
                f"Initial MDS config before daemon kill test: {initial_mds_config}"
            )
            initial_mds_pair = fs_util_v1.get_mds_standby_replay_pair(
                clients[0], fs_name, initial_mds_config
            )
            standby_replay_nodes = []
            for mds in mds_nodes:
                for mds_iter in initial_mds_config:
                    if (mds.node.hostname in mds_iter["name"]) and (
                        mds_iter["state"] == "standby-replay"
                    ):
                        standby_replay_nodes.append(mds)
                        log.info(f"standby replay node: {mds.node.hostname}")
            for mds in standby_replay_nodes:
                cluster_health_beforeIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                try:
                    fs_util_v1.pid_kill(mds, "mds")
                except Exception as e:
                    stop_flag = True
                    log.error(e)
                    log.error(traceback.format_exc())

                cluster_health_afterIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
                if cluster_health_afterIO == cluster_health_beforeIO:
                    log.info("cluster is healthy")
                else:
                    log.error("cluster is not healty")
            log.info("killed Standby-Replay mds services and cluster is Healthy")
            mds_config = fs_util_v1.get_mds_config(clients[0], fs_name)
            log.info(f"MDS config after daemon kill: {mds_config}")
            log.info("Validate Standby-Replay node configuration after daemon kill")
            if validate_standby_replay_mds(
                clients[0], fs_name, fs_util_v1, initial_mds_config, initial_mds_pair
            ):
                test_fail += 1
            for mds in mds_nodes:
                cluster_health_beforeIO = check_ceph_healthly(
                    clients[0],
                    num_of_osds,
                    len(mds_nodes),
                    build,
                    None,
                    30,
                )
            log.info("Setting stop flag")
            stop_flag = True
        if test_fail >= 1:
            return 1
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        stop_flag = True
        return 1
    finally:
        log.info("Cleaning up the system")
        stop_flag = True
        fs_util_v1.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mount_dir
        )
        fs_util_v1.client_clean_up(
            "umount",
            kernel_clients=[clients[0]],
            mounting_dir=kernel_mount_dir,
        )
