import datetime
import json
import logging
import os
import random
import threading
import time
import traceback

from tests.cephfs.cephfs_system.cephfs_system_utils import CephFSSystemUtils
from utility.log import Log

log = Log(__name__)
logger = logging.getLogger("run_log")

logging_thread = None
mds_logging_thread = None
stop_event = threading.Event()


def run(ceph_cluster, **kw):
    """
    CephFs Configu update : To modify below cephfs config to different supported values,
    Reference:
    MDS : https://docs.ceph.com/en/reef/cephfs/mds-config-ref/
    Client : https://docs.ceph.com/en/reef/cephfs/client-config-ref/
    1. Balancer: On/Off balancer.
    2.

    """
    try:
        sys_lib = CephFSSystemUtils(ceph_cluster)
        config = kw.get("config")
        config_name = config.get("config_name", "all")
        set_def = config.get("set_default", 0)
        update_interval = config.get(
            "conf_update_interval", 1
        )  # hrs,set to '0' if NO iteration required
        run_time = config.get("run_time_hrs", 4)
        chk_clus = config.get(
            "check_cluster", 1
        )  # Required for system testing only,0 for other tests

        client = ceph_cluster.get_ceph_objects("client")[0]
        mds_config = {
            "mds_cache_memory_limit": {
                "default": "4G",
                "val_list": ["8G", "16G", "32G"],
            },
            # "mds_cache_reservation": {
            #    "default": "0.05",
            #    "val_list": list(range(0.06, 0.5)),
            # },
            "mds_cache_mid": {
                "default": "0.7",
                "val_list": [0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
            },
            "mds_dir_max_commit_size": {
                "default": "90",
                "val_list": list(range(10, 150)),
            },
            "mds_decay_halflife": {"default": "5", "val_list": list(range(3, 7))},
            "mds_beacon_interval": {"default": "4", "val_list": list(range(2, 10))},
            "mds_tick_interval": {"default": "5", "val_list": list(range(3, 8))},
            "mds_client_prealloc_inos": {
                "default": "1000",
                "val_list": list(range(500, 3000)),
            },
            # "mds_log_max_segments": {"default": "128", "val_list": list(range(10, 129))},
            "mds_bal_sample_interval": {"default": "3", "val_list": [2.0, 4.0, 5.0]},
            "mds_bal_replicate_threshold": {
                "default": "8000",
                "val_list": list(range(4000, 9000)),
            },
            "mds_bal_split_size": {
                "default": "10000",
                "val_list": list(range(1000, 10000)),
            },
            "mds_bal_split_rd": {
                "default": "25000",
                "val_list": list(range(5000, 25000)),
            },
            "mds_bal_split_wr": {
                "default": "10000",
                "val_list": list(range(1000, 10000)),
            },
            "mds_bal_merge_size": {"default": "50", "val_list": list(range(10, 100))},
            "mds_bal_interval": {"default": "10", "val_list": list(range(5, 15))},
            "mds_bal_fragment_interval": {
                "default": "5",
                "val_list": list(range(3, 10)),
            },
            "mds_bal_mode": {"default": "0", "val_list": list(range(1, 4))},
            "mds_thrash_fragments": {"default": "0", "val_list": [1, 0]},
            "mds_dump_cache_on_map": {
                "default": "false",
                "val_list": ["true", "false"],
            },
            "mds_dump_cache_after_rejoin": {
                "default": "false",
                "val_list": ["true", "false"],
            },
        }
        if config_name in mds_config:
            config_list = [config_name]
        else:
            config_list = mds_config.keys()

        test_status = mds_config_test(
            client,
            set_def,
            chk_clus,
            run_time,
            update_interval,
            config_list,
            mds_config,
            sys_lib,
        )

        if test_status == 1:
            return 1

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Reset MDS config to defaults")
        if (set_def == 0) and (update_interval != 0):
            for config_name in mds_config:
                conf_value = mds_config[config_name]["default"]
                cmd = f"ceph config set mds {config_name} {conf_value}"
                client.exec_command(sudo=True, cmd=cmd)
                cmd = f"ceph config get mds {config_name}"
                out, _ = client.exec_command(sudo=True, cmd=cmd)
                log.info(f"{config_name}:{out}")


def mds_config_test(
    client,
    set_def,
    check_cluster,
    run_time,
    update_interval,
    config_list,
    mds_config,
    fs_system_utils,
):
    # Log configuration
    log_name = "mds_config_test"
    log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
    log_path = f"{log_base_dir}/config_subtests"
    try:
        os.mkdir(log_path)
    except BaseException as ex:
        log.info(ex)
        if "File exists" not in str(ex):
            return 1
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    log1.info(f"Start {log_name}")

    config_interval_secs = int(update_interval) * 60 * 60
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=run_time)
    cluster_healthy = 1

    while datetime.datetime.now() < end_time and cluster_healthy:
        for config_name in config_list:
            if set_def:
                conf_value = mds_config[config_name]["default"]
            else:
                conf_val_list = mds_config[config_name]["val_list"]
                conf_value = random.choice(conf_val_list)
            cmd = f"ceph config set mds {config_name} {conf_value}"
            client.exec_command(sudo=True, cmd=cmd)
            cmd = f"ceph config get mds {config_name}"
            out, _ = client.exec_command(sudo=True, cmd=cmd)
            log1.info(f"{config_name}:{out}")
        if (set_def == 1) or (update_interval == 0):
            break
        time.sleep(config_interval_secs)
        if check_cluster:
            cluster_healthy = is_cluster_healthy(client)
            log1.info(f"cluster_health {cluster_healthy}")

    log1.info("mds_test_workflow_1 completed")
    return 0


def is_cluster_healthy(client):
    """
    returns '0' if failed, '1' if passed
    """
    file = "cephfs_systest_data.json"
    f = client.remote_file(
        sudo=True,
        file_name=f"/home/cephuser/{file}",
        file_mode="r",
    )
    cephfs_config = json.load(f)
    if cephfs_config.get("CLUS_MONITOR"):
        if cephfs_config["CLUS_MONITOR"] == "fail":
            return 0
    return 1
