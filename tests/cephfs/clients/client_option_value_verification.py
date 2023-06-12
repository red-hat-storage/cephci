import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
        Steps:
    1. set the config values
    2. verify the config values
    """
    try:
        tc = "CEPH-11330"
        log.info(f"Running CephFS tests for -{tc}")
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        conf_set = "ceph config set client"
        conf_get = "ceph config get client"
        conf_default = [
            ["debug_client", ""],
            ["client_acl_type", ""],
            ["client_cache_mid", ""],
            ["client_cache_size", ""],
            ["client_caps_release_delay", ""],
            ["client_debug_force_sync_read", ""],
            ["client_dirsize_rbytes", ""],
            ["client_max_inline_size", ""],
            ["client_metadata", ""],
            ["client_mount_gid", ""],
            ["client_mount_timeout", ""],
            ["client_mountpoint", ""],
            ["client_oc", ""],
            ["client_oc_max_dirty", ""],
            ["client_oc_max_dirty_age", ""],
            ["client_oc_max_objects", ""],
            ["client_oc_size", ""],
            ["client_oc_target_dirty", ""],
            ["client_permissions", ""],
            ["client_quota_df", ""],
            ["client_readahead_max_bytes", ""],
            ["client_readahead_max_periods", ""],
            ["client_readahead_min", ""],
            ["client_reconnect_stale", ""],
            ["client_snapdir", ""],
            ["client_tick_interval", ""],
            ["client_use_random_mds", ""],
            ["fuse_default_permissions", ""],
            ["fuse_max_write", ""],
            ["fuse_disable_pagecache", ""],
            ["client_debug_getattr_caps", ""],
            ["client_debug_inject_tick_delay", ""],
            ["client_inject_fixed_oldest_tid", ""],
            ["client_inject_release_failure", ""],
            ["client_trace", ""],
        ]
        for conf in conf_default:
            output = client1.exec_command(sudo=True, cmd=f"{conf_get} {conf[0]}")
            conf[1] = output[0].strip()
        print(conf_default)
        conf_target = [
            ("debug_client", "20/20"),
            ("client_acl_type", "posix_acl"),
            ("client_cache_mid", "0.76"),
            ("client_cache_size", "16Ki"),
            ("client_caps_release_delay", "5"),
            ("client_debug_force_sync_read", "false"),
            ("client_dirsize_rbytes", "true"),
            ("client_max_inline_size", "4Ki"),
            ("client_metadata", "test"),
            ("client_mount_gid", "-1"),
            ("client_mount_timeout", "301"),
            ("client_mountpoint", "/"),
            ("client_oc", "true"),
            ("client_oc_max_dirty", "100Mi"),
            ("client_oc_max_dirty_age", "5.0"),
            ("client_oc_max_objects", "1000"),
            ("client_oc_size", "200Mi"),
            ("client_oc_target_dirty", "8Mi"),
            ("client_permissions", "true"),
            ("client_quota_df", "true"),
            ("client_readahead_max_bytes", "0"),
            ("client_readahead_max_periods", "4"),
            ("client_readahead_min", "128Ki"),
            ("client_reconnect_stale", "false"),
            ("client_snapdir", ".snap"),
            ("client_tick_interval", "1"),
            ("client_use_random_mds", "false"),
            ("fuse_default_permissions", "false"),
            ("fuse_max_write", "0B"),
            ("fuse_disable_pagecache", "false"),
            ("client_debug_getattr_caps", "true"),
            ("client_debug_inject_tick_delay", "10"),
            ("client_inject_fixed_oldest_tid", "true"),
            ("client_inject_release_failure", "true"),
            ("client_trace", "test"),
        ]
        conf_expected = [
            ("debug_client", "20/20"),
            ("client_acl_type", "posix_acl"),
            ("client_cache_mid", "0.760000"),
            ("client_cache_size", "16384"),
            ("client_caps_release_delay", "5"),
            ("client_debug_force_sync_read", "false"),
            ("client_dirsize_rbytes", "true"),
            ("client_max_inline_size", "4096"),
            ("client_metadata", "test"),
            ("client_mount_gid", "-1"),
            ("client_mount_timeout", "301"),
            ("client_mountpoint", "/"),
            ("client_oc", "true"),
            ("client_oc_max_dirty", "104857600"),
            ("client_oc_max_dirty_age", "5.000000"),
            ("client_oc_max_objects", "1000"),
            ("client_oc_size", "209715200"),
            ("client_oc_target_dirty", "8388608"),
            ("client_permissions", "true"),
            ("client_quota_df", "true"),
            ("client_readahead_max_bytes", "0"),
            ("client_readahead_max_periods", "4"),
            ("client_readahead_min", "131072"),
            ("client_reconnect_stale", "false"),
            ("client_snapdir", ".snap"),
            ("client_tick_interval", "1"),
            ("client_use_random_mds", "false"),
            ("fuse_default_permissions", "false"),
            ("fuse_max_write", "0"),
            ("fuse_disable_pagecache", "false"),
            ("client_debug_getattr_caps", "true"),
            ("client_debug_inject_tick_delay", "10"),
            ("client_inject_fixed_oldest_tid", "true"),
            ("client_inject_release_failure", "true"),
            ("client_trace", "test"),
        ]
        for target, expected in zip(conf_target, conf_expected):
            client_conf = target[0]
            value = target[1]
            client1.exec_command(sudo=True, cmd=f"{conf_set} {client_conf} {value}")
            output = client1.exec_command(
                sudo=True, cmd=f"ceph config get client {client_conf}"
            )
            if output[0].rstrip() != expected[1]:
                raise CommandFailed(f"Failed to set {client_conf} to {value}")
        log.info("Successfully set all the client config values")
        conf_default[1][1] = '" "'
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
