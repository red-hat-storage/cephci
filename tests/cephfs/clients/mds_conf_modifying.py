import json
import random
import string
import traceback
from distutils.version import LooseVersion

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.utils import get_ceph_version_from_cluster

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Steps:
    1. set the mds config values
    2. verify the mds config values
    """
    try:
        tc = "CEPH-11329"
        log.info(f"Running CephFS tests for -{tc}")
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        ceph_version = get_ceph_version_from_cluster(clients[0])
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        fs_util.auth_list([client1])
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        client1.exec_command(sudo=True, cmd="yum install -y --nogpgcheck ceph-fuse")
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount([client1], fuse_mounting_dir_1)
        fs_util.run_ios(client1, fuse_mounting_dir_1, ["dd", "smallfile"])

        log.info("Setting the client config values")
        conf_set = "ceph config set mds"
        conf_get = "ceph config get mds"
        if LooseVersion(ceph_version) >= LooseVersion("18.1"):
            output = client1.exec_command(
                sudo=True, cmd=f"{conf_get} defer_client_eviction_on_laggy_osds"
            )
            defer_client_eviction_on_laggy_osds = output[0].strip()
            if defer_client_eviction_on_laggy_osds.lower() != "false":
                log.error(
                    f"defer_client_eviction_on_laggy_osds values is expected to be disabled by default.\n "
                    f"but the value is {defer_client_eviction_on_laggy_osds}"
                )

                return 1

        if LooseVersion(ceph_version) >= LooseVersion("18.2"):
            set_balance_automate_and_validate(client1)

        conf_default = [
            ["mds_cache_mid", ""],
            ["mds_dir_max_commit_size", ""],
            ["mds_dir_max_entries", ""],
            ["mds_decay_halflife", ""],
            ["mds_beacon_interval", ""],
            ["mds_beacon_grace", ""],
            ["mon_mds_blocklist_interval", ""],
            ["mds_reconnect_timeout", ""],
            ["mds_tick_interval", ""],
            ["mds_dirstat_min_interval", ""],
            ["mds_scatter_nudge_interval", ""],
            ["mds_client_prealloc_inos", ""],
            ["mds_early_reply", ""],
            ["mds_default_dir_hash", ""],
            ["mds_log_skip_corrupt_events", ""],
            ["mds_log_max_events", ""],
            ["mds_log_max_segments", ""],
            ["mds_bal_sample_interval", ""],
            ["mds_bal_replicate_threshold", ""],
            ["mds_bal_unreplicate_threshold", ""],
            ["mds_bal_split_size", ""],
            ["mds_bal_split_rd", ""],
            ["mds_bal_split_wr", ""],
            ["mds_bal_split_bits", ""],
            ["mds_bal_merge_size", ""],
            ["mds_bal_interval", ""],
            ["mds_bal_fragment_interval", ""],
            ["mds_bal_fragment_fast_factor", ""],
            ["mds_bal_fragment_size_max", ""],
            ["mds_bal_idle_threshold", ""],
            ["mds_bal_max", ""],
            ["mds_bal_max_until", ""],
            ["mds_bal_mode", ""],
            ["mds_bal_min_rebalance", ""],
            ["mds_bal_min_start", ""],
            ["mds_bal_need_min", ""],
            ["mds_bal_need_max", ""],
            ["mds_bal_midchunk", ""],
            ["mds_bal_minchunk", ""],
            ["mds_replay_interval", ""],
            ["mds_shutdown_check", ""],
            ["mds_thrash_exports", ""],
            ["mds_thrash_fragments", ""],
            ["mds_dump_cache_on_map", ""],
            ["mds_dump_cache_after_rejoin", ""],
            ["mds_verify_scatter", ""],
            ["mds_debug_scatterstat", ""],
            ["mds_debug_frag", ""],
            ["mds_debug_auth_pins", ""],
            ["mds_debug_subtrees", ""],
            ["mds_kill_mdstable_at", ""],
            ["mds_kill_export_at", ""],
            ["mds_kill_import_at", ""],
            ["mds_kill_link_at", ""],
            ["mds_kill_rename_at", ""],
            ["mds_inject_skip_replaying_inotable", ""],
            ["mds_kill_skip_replaying_inotable", ""],
            ["mds_wipe_sessions", ""],
            ["mds_wipe_ino_prealloc", ""],
            ["mds_skip_ino", ""],
            ["mds_min_caps_per_client", ""],
        ]
        for conf in conf_default:
            output = client1.exec_command(sudo=True, cmd=f"{conf_get} {conf[0]}")
            conf[1] = output[0].strip()
        conf_target = [
            ("mds_cache_mid", "0.800000"),
            ("mds_dir_max_commit_size", "11"),
            ("mds_dir_max_entries", "1"),
            ("mds_decay_halflife", "6.000000"),
            ("mds_beacon_interval", "5.000000"),
            ("mds_beacon_grace", "16.000000"),
            ("mon_mds_blocklist_interval", "3600.000000"),
            ("mds_reconnect_timeout", "46.000000"),
            ("mds_tick_interval", "4.000000"),
            ("mds_dirstat_min_interval", "2.000000"),
            ("mds_scatter_nudge_interval", "6.000000"),
            ("mds_client_prealloc_inos", "1001"),
            ("mds_early_reply", "false"),
            ("mds_default_dir_hash", "3"),
            ("mds_log_skip_corrupt_events", "true"),
            ("mds_log_max_events", "0"),
            ("mds_log_max_segments", "256"),
            ("mds_bal_sample_interval", "4.000000"),
            ("mds_bal_replicate_threshold", "8001.000000"),
            ("mds_bal_unreplicate_threshold", "0.100000"),
            ("mds_bal_split_size", "10001"),
            ("mds_bal_split_rd", "25001.000000"),
            ("mds_bal_split_wr", "10001.000000"),
            ("mds_bal_split_bits", "6"),
            ("mds_bal_merge_size", "51"),
            ("mds_bal_interval", "11"),
            ("mds_bal_fragment_interval", "6"),
            ("mds_bal_fragment_fast_factor", "1.700000"),
            ("mds_bal_fragment_size_max", "100001"),
            ("mds_bal_idle_threshold", "0.100000"),
            ("mds_bal_max", "1"),
            ("mds_bal_max_until", "1"),
            ("mds_bal_mode", "1"),
            ("mds_bal_min_rebalance", "0.200000"),
            ("mds_bal_min_start", "0.300000"),
            ("mds_bal_need_min", "0.900000"),
            ("mds_bal_need_max", "1.500000"),
            ("mds_bal_midchunk", "0.500000"),
            ("mds_bal_minchunk", "0.005000"),
            ("mds_replay_interval", "1.500000"),
            ("mds_shutdown_check", "1"),
            ("mds_thrash_exports", "1"),
            ("mds_thrash_fragments", "1"),
            ("mds_dump_cache_on_map", "true"),
            ("mds_dump_cache_after_rejoin", "true"),
            ("mds_verify_scatter", "true"),
            ("mds_debug_scatterstat", "true"),
            ("mds_debug_frag", "true"),
            ("mds_debug_auth_pins", "true"),
            ("mds_debug_subtrees", "true"),
            ("mds_kill_mdstable_at", "1"),
            ("mds_kill_export_at", "1"),
            ("mds_kill_import_at", "1"),
            ("mds_kill_link_at", "1"),
            ("mds_kill_rename_at", "1"),
            ("mds_inject_skip_replaying_inotable", "true"),
            ("mds_kill_skip_replaying_inotable", "true"),
            ("mds_wipe_sessions", "true"),
            ("mds_wipe_ino_prealloc", "true"),
            ("mds_skip_ino", "1"),
            ("mds_min_caps_per_client", "101"),
        ]
        for target in conf_target:
            client_conf = target[0]
            value = target[1]
            client1.exec_command(sudo=True, cmd=f"{conf_set} {client_conf} {value}")
        for target in conf_target:
            client_conf = target[0]
            output = client1.exec_command(
                sudo=True, cmd=f"ceph config get mds {client_conf}"
            )
            if output[0].rstrip() != target[1]:
                raise CommandFailed(f"Failed to set {client_conf} to {value}")
        log.info("Successfully set all the client config values")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Resetting the client config values to default")
        for default in conf_default:
            client_conf = default[0]
            value = default[1]
            client1.exec_command(sudo=True, cmd=f"{conf_set} {client_conf} {value}")
        log.info("Successfully reset all the client config values to default")


def set_balance_automate_and_validate(client):
    log.info(
        "Validate if the Balance Automate is set to False by default and reset after toggling the values"
    )
    out, rc = client.exec_command(sudo=True, cmd="ceph fs get cephfs -f json")
    json_data = json.loads(out)
    default_balance_automate_value = json_data["mdsmap"]["flags_state"][
        "balance_automate"
    ]

    if default_balance_automate_value.lower() != "false":
        log.error(
            f"balance automate values is expected to be disabled by default.\n "
            f"but the value is {default_balance_automate_value}"
        )
        raise CommandFailed("Set Balance Automate value is not set to default value")

    log.info("Set Balance Automate to True and Validate")
    client.exec_command(sudo=True, cmd="ceph fs set cephfs balance_automate true")
    out, rc = client.exec_command(sudo=True, cmd="ceph fs get cephfs -f json")
    json_data = json.loads(out)
    balance_automate = json_data["mdsmap"]["flags_state"]["balance_automate"]

    if balance_automate != "true":
        log.error(
            f"balance automate values is unable to set to True.\n "
            f"and the current value is {balance_automate}"
        )
        raise CommandFailed("Unable to set Balance Automate value to True")

    log.info("Set Balance Automate back to Default Value")
    client.exec_command(sudo=True, cmd="ceph fs set cephfs balance_automate false")

    out, rc = client.exec_command(sudo=True, cmd="ceph fs get cephfs -f json")
    json_data = json.loads(out)
    balance_automate = json_data["mdsmap"]["flags_state"]["balance_automate"]
    if balance_automate.lower() != "false":
        log.error(
            f"balance automate values is unable to set to default value.\n "
            f"but the value is {balance_automate}"
        )
        raise CommandFailed("Unable to set Balance Automate value to default value")
