"""
This file contains the  methods for the verification of Mclock feature.
The main aim to test the various profiles of the Mclock.Those are
   1. higs_client_ops profile
   2.high_recovery_ops profile
   3.balanced
In addtion to the above profiles we are verifying the various configuration options  that are introduced for Mclock
"""

import random
import threading
import time
from configparser import ConfigParser
from statistics import mean
from threading import Thread

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_bench import RadosBench
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

# global variables used in various methods in the file
get_config_cmd = "ceph config get osd  "
set_config_cmd = "ceph config set osd  "
ini_file = "tests/rados/rados_config_parameters.ini"
fio_cmd = (
    "fio --directory=/mnt/cephfs_Qos  -direct=1 -iodepth 64 -thread -rw=randwrite  --end_fsync=0 "
    "-ioengine=libaio -bs=4096 -size=16384M --norandommap -numjobs=1 -runtime=600 --time_based --invalidate=0 "
    "-group_reporting -name=ceph_fs_Qos_4M --write_iops_log=/tmp/cephfs/Fio/output.0 "
    "--write_bw_log=/tmp/cephfs/Fio/output.0 --write_lat_log=/tmp/cephfs/Fio/output.0 --log_avg_msec=100 "
    "--write_hist_log=/tmp/cephfs/Fio/output.0 --output-format=json,normal > /tmp/cephfs/Fio/output.0 "
)

# Varible used to get the return value from the Thread
fio_return = None


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    config = kw["config"]

    # Delete
    # global rados_utils_obj
    #####
    global rados_obj
    global rados_bench_obj
    global client

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    # Initilizing the rados bench object
    client = ceph_cluster.get_nodes(role="client")[0]
    clients = ceph_cluster.get_nodes(role="client")
    rados_bench_obj = RadosBench(mon_node=client, clients=clients)

    # TO-DO for the next test cases
    # cmd = "ceph config get osd osd_mclock_profile"
    # mclock_profiles = ["high_client_ops", "balanced", "high_recovery_ops", "custom"]
    pool_names = ["iops_pool", "backgroud_pool", "scrub_pool"]
    pool_time = [1000, 1000, 1000]

    # Getting the client
    clients = ceph_cluster.get_ceph_objects("client")
    client = clients[0]

    # Create a directory in the client
    mount_dir = "/mnt/cephfs_Qos"

    #  Kernal mount the directory
    fs_util = FsUtils(ceph_cluster)
    mon_node_ips = fs_util.get_mon_node_ips()
    fs_util.kernel_mount(
        [client],
        mount_dir,
        ",".join(mon_node_ips),
        extra_params=",fs=cephfs_Qos",
        new_client_hostname="admin",
    )

    # creating log directory
    log_dir_cmd = "mkdir -p /tmp/cephfs/Fio"
    client.exec_command(cmd=log_dir_cmd, sudo=True)
    log.info("/tmp/cephfs/Fio directory created for the Fio logs")

    # Set the noscrub, nodeep-scrub, nobackfill and norecover flags
    if not set_flags(rados_obj):
        log.error("noscrub, nodeep-scrub, nobackfill and norecover flags are unset")
        return 1
    log.info("noscrub, nodeep-scrub, nobackfill and norecover flags are set")

    # Creating pools
    for pool_name in pool_names:
        if not (rados_obj.create_pool(pool_name)):
            log.error(f"Cannot able to create the {pool_name}")
            return 1
        log.info(f" {pool_name} pool created")

    # Get the average IOPS with out scrub and backfill
    avg_iops = get_iops(client)
    log.info(
        f"The average IOPS with out background and scrub operations are: {avg_iops}"
    )

    # Creating objects in to the pools
    rados_bench_obj.parallel_thread_write(client, pool_names, pool_time)

    # Usetting the flags
    if not unset_flags(rados_obj):
        log.error(
            "Failed to unset the noscrub, nodeep-scrub, nobackfill and norecover flags"
        )
        return 1
    log.info("noscrub, nodeep-scrub, nobackfill and norecover flags are unset")

    # Getting PG_list to deep scrub at PG level
    rados_obj.get_pgid_list()

    # Perform scrubbing at pg level
    rados_obj.start_all_pg_scrub()

    # perform deep scrub on PGs
    rados_obj.start_all_pg_deepscrub()

    if config.get("profile") == "high_client_ops":
        log.info(f'{"Verifying the Mclock with the High_client_ops profile"}')

        # Get the IOPS values after startting the backgroud,scrub,fio and rados up and down process
        highclient_iops_count = get_avg_iops_withOps(
            ceph_cluster, client, pool_names, pool_time
        )
        log.info(
            f"The ideal cluster average IOPS is {avg_iops} and average IOPS when the cluster is running with all "
            f"services is {highclient_iops_count}"
        )

        # Get the average
        highClient_percentage = is_what_percent(highclient_iops_count, avg_iops)
        log.info(
            f"For high_client_ops profile the IOPS percentage compare with the ideal cluster IOPS is "
            f"{highClient_percentage}"
        )
        if highClient_percentage < 50:
            log.error(
                "For the high_clent_ops profile the client reservation percentage is lses than 50%"
            )
            return 1
        else:
            log.error(
                "For the high_clent_ops profile the client reservation percentage is greater than 50%"
            )
            return 0


def config_change_check(rados_obj: RadosOrchestrator, profile):

    # Read Config paramters
    config_info = ConfigParser()
    config_info.read(ini_file)
    try:
        # Read values from mclock section
        mclock_parameters = config_info.items("Mclock")
        for param in mclock_parameters:
            cmd = f"{get_config_cmd}{param[0]}"
            status = rados_obj.run_ceph_command(cmd=cmd)
            if type(status) == int:
                new_status = status + 1
            set_cmd = f"{set_config_cmd}{param[0]}  {new_status}"
            rados_obj.run_ceph_command(cmd=set_cmd)
            time.sleep(2)
            get_cmd = f"{get_config_cmd}{param[0]}"
            status = rados_obj.run_ceph_command(cmd=get_cmd)
            if profile != "custom":
                if str(status).strip() != str(param[-1]).strip():
                    log.error(
                        f" Able to modify the configuration parameters for the {profile} profile"
                    )
                    return 1
            else:
                if str(status).strip() == str(param[-1]).strip():
                    log.error(
                        f" Not able to modify the configuration parameters for the {profile} profile"
                    )
                    return 1
        return 0
    except Exception as err:
        log.error("Failed to set or get configuration parameteers from the cluster")
        log.error(err)
        return 1
    finally:
        set_default_config(rados_obj)


def set_default_config(rados_obj: RadosOrchestrator):
    log.info(" Setting the all Mclock configration parameters to the default values")
    # Read Config paramters
    config_info = ConfigParser()
    # set_config_cmd = "ceph config set osd  "
    config_info.read(ini_file)
    # Read values from mclock section
    mclock_parameters = config_info.items("Mclock")
    for param in mclock_parameters:
        set_cmd = f"{set_config_cmd}{param[0]}  {param[-1]}"
        rados_obj.run_ceph_command(cmd=set_cmd)
        time.sleep(2)


def get_iops(client):
    """
    Used to get the average IOPS.
    Args:
        Client node
        Returns :  mean_iops and  fio_return(Thread)
    """

    # fio_return variable is using to return value to the thread
    global fio_return
    iops_avg_values = []

    # To get the average IOPS executing the five time
    for count in range(5):
        client.exec_command(cmd=fio_cmd, sudo=True, long_running=True)
        get_iops_cmd = (
            'grep "iops" /tmp/cephfs/Fio/output.0 | grep "avg=" | awk  \'{print $7}\''
        )
        iops_value = "".join(client.exec_command(cmd=get_iops_cmd, sudo=True))
        iops_list = iops_value.split("=")
        # TO-DO why the value some time is None
        if len(iops_list) == 0:
            iops = 0
        else:
            iops = ((iops_list[-1]).rstrip())[:-1]
        iops_avg_values.append(float(iops))
    mean_iops = mean(iops_avg_values)
    fio_return = mean_iops
    return mean_iops


def set_flags(rados_obj: RadosOrchestrator):
    """
    Used to set the noscrub, nodeep-scrub, nobackfill and norecover flags
    Args:
        Rados object
    Returns : True -> pass, False -> fail
    """
    try:
        # setting the noscrub, nodeep-scrub, nobackfill and norecover
        rados_obj.set_rados_flag("noscrub")
        rados_obj.set_rados_flag("nodeep-scrub")
        rados_obj.set_rados_flag("nobackfill")
        rados_obj.set_rados_flag("norecover")
        return True
    except Exception:
        log.error("Error while setting the flags")
        return False


def unset_flags(rados_obj: RadosOrchestrator):
    """
    Used to unset the noscrub, nodeep-scrub, nobackfill and norecover flags
    Args:
        Rados object
    Returns : True -> pass, False -> fail
    """
    try:
        # setting the noscrub, nodeep-scrub, nobackfill and norecover
        rados_obj.unset_rados_flag("noscrub")
        rados_obj.unset_rados_flag("nodeep-scrub")
        rados_obj.unset_rados_flag("nobackfill")
        rados_obj.unset_rados_flag("norecover")
        return True
    except Exception:
        log.error("Error while un-setting the flags")
        return False


def restart_random_osd(ceph_cluster):
    """
    Used to restart teh random OSD
    Args:
        ceph cluster
    """

    # Get the osd clients
    clients = ceph_cluster.get_ceph_objects("osd")

    # get the OSD list as a dictionary.Node name as key and osd numbers as values
    rados_list = rados_obj.get_osd_list()

    # Randomly pick the osd object.
    host_object = random.choice(clients)

    # Get the osd host name
    host_name = host_object.exec_command(sudo=True, cmd="hostname")
    host_name = ("".join(host_name)).strip()

    # From the osd node picking the random osd number
    osd_list = rados_list[host_name]
    osd = str(random.choice(osd_list))

    rados_obj.change_osd_state("stop", osd)
    time.sleep(180)
    rados_obj.change_osd_state("start", osd)


def thread_generate_backfill(ceph_cluster):
    """
    This method is used as a thread to bring the cluser in to backfill or recovery state
    Args:
        ceph cluster
    """

    # To get the list of threads running in the process
    list_threads = threading.enumerate()

    # Run untill the fio thread is in running
    while any("fio" in str(str1) for str1 in list_threads):
        # time.sleep(2)
        cluster_state = rados_obj.get_pg_states()

        # If the pg states are not in backfill or recovery state then stopping and starting the OSD
        if any(not ("backfill" or "recovery") in state for state in cluster_state):
            restart_random_osd(ceph_cluster)
        list_threads = threading.enumerate()


def thread_chk_iops_write(client, pool_names, time):
    """
    This method is used as a thread to push the data in to the cluster.
    Args:
        client-> cline node
        pool_names-> pool list
        time -> time to push the data
    """
    list_threads = threading.enumerate()
    while any("fio" in str(str1) for str1 in list_threads):
        rados_bench_obj.parallel_thread_write(client, pool_names, time)
        list_threads = threading.enumerate()


def get_avg_iops_withOps(ceph_cluster, client, pool_names, time):
    """
    This method is used to create three threads.
    These threads perform the following-
    1. To push the objects in to the cluster
    2. To bring the cluster in to backfill/recovery state
    3.Get the average IOPS value
    Args:
        ceph_cluster -> Ceph cluster object
        client-> cline node
        pool_names-> pool list
        time -> time to push the data
    """

    # 1. start all iops in the pools
    rados_bench_thread = Thread(
        target=thread_chk_iops_write, args=[client, pool_names, time]
    )

    # 2. check recovery operations and scrub if not create
    backfill_gen_thread = Thread(target=thread_generate_backfill, args=[ceph_cluster])

    # 3.Calculate the fio
    get_iops_thread = Thread(target=get_iops, args=[client])

    # Setting the thread names
    rados_bench_thread.name = "iops"
    backfill_gen_thread.name = "backfill"
    get_iops_thread.name = "fio"

    # start th threads
    get_iops_thread.start()
    rados_bench_thread.start()
    backfill_gen_thread.start()

    get_iops_thread.join()
    iops_count_withOps = fio_return
    rados_bench_thread.join()
    backfill_gen_thread.join()

    return iops_count_withOps


def is_what_percent(num_a, num_b):
    """
    To calculate the percentage
    Args:
        num_a and num_b are the average IOPS values
    """
    try:
        return (num_a / num_b) * 100
    except ZeroDivisionError:
        return 0
