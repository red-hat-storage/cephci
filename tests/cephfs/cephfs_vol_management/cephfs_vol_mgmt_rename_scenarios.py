import json
import re
import secrets
import string
import time
import traceback
from distutils.version import LooseVersion

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from cli.cephadm.cephadm import CephAdm
from cli.utilities.utils import get_service_id
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log
from utility.utils import get_ceph_version_from_cluster

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    pre-requisites:
    1. Create a Volume (cephfs_x)
    2.Create 1 Subvolume Group
    3.Create few Subvolumes when needed as part of each workflows.
    4.Mount Subvolume on all clients types Kernel, Fuse, NFS
    5.Continue IO’s on all 3 mounts while all of the below operations.
    workflow1:
    1.Create 3 + 3 + 3 + 3 subvolumes - 12 - mount each subvolume to a client type
    2.Configure 3 subvolumes with Manual Snapshots, 3 -
     Clones of all subvolumes mount, and quotas[ 3 - bytes and 3 - Files]
      of Subvolumes. Continue IOs on all subvolumes for some time and stop it.
    3.Rename the Volume.
    4.ensure all the snapshots are present, the file and byte quota that was set are intact.
    5.Create more new manual Snapshots, create more Clones, modify the byte level
     and file level quotas, and continue the IOs for some time.
    workflow2:
    1.Capture metrics using cephfs-top
    2.Create a snapshot schedule on the 3 subvolumes mounted across all 3 clients.
     Schedule it for 5 minutes at an interval of 1 min.
    3.Stop IO’s here and Rename the Volume
    4.Once remounted - validate the schedules remain active [check the status].
    5.If required create new schedules using the new volume name.
    6.Recapture metrics using cephfs-top on newly renamed volumes and ensure the metrics are intact.
     - it should be incremented values from the previous top output.
    workflow3:
    1.Adding additional DataPool to the newly renamed Volumes.
    2.Validate the newly added datapools in osd lspools, ceph fs status, ceph fs dump and all other places.
    3.Continue the IOs
    4.Run these tests tests/cephfs/clients/client_option_value_verification.py
     & tests/cephfs/clients/mds_conf_modifying.py  and capture the values set
    5.Stop the IOs
    6.Rename the Volume and validate that all the values set are intact.
    7.Rerun the tests - tests tests/cephfs/clients/client_option_value_verification.py
     & tests/cephfs/clients/mds_conf_modifying.py
    workflow4:
    1. Multifs scenario - Create a new Volume along with the existing one. Total 2 subvolumes
    2. Run IOs on both Volumes.
    3. rename 1 of the Volume
    4. Rename with the name of another volume that already exists.[Negative]
    5. Failover Scenarios before and after renaming should behave the same
    6. onfiguring the standby-replay on primary volume and after renaming the standby-replay should function properly.
    """
    try:
        tc = "CEPH-83607848"
        log.info(f"Running CephFS tests{tc}")
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs_1")
        fs_util.auth_list([client1])
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(client1, build)
        client1.exec_command(sudo=True, cmd="dnf install jq -y", check_ec=False)
        client1.exec_command(
            sudo=True,
            cmd="git clone https://github.com/distributed-system-analysis/smallfile.git",
            check_ec=False,
        )
        client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        rand = "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        fs_name = "cephfs_1"
        out, ec = client1.exec_command(sudo=True, cmd="ceph fs ls")
        log.info(out)
        if f"name: {fs_name}" not in out:
            fs_util.create_fs(client1, fs_name)
        log.info("####### Scenario 1 #######")
        fs_util.create_subvolumegroup(client1, fs_name, "subvol_group_name_1")
        log.info(
            "Create 12 subvolumes: 3-> Manual Snapshot, 3 -> clones for subvolume mount"
        )
        log.info("3 -> for quota bytes, 3-> quota Files")
        subvol_path = []
        for i in range(1, 13):
            fs_util.create_subvolume(
                client1, fs_name, f"subvol_{i}", group_name="subvol_group_name_1"
            )
            out, ec = client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {fs_name} subvol_{i} --group_name=subvol_group_name_1",
            )
            subvol_path.append(out.rstrip())
        log.info("Subvolumes are created")
        log.info("Mount the subvolumes")
        subvol_fuse_mount = [
            subvol_path[0],
            subvol_path[3],
            subvol_path[6],
            subvol_path[9],
        ]
        subvol_kernel_mount = [
            subvol_path[1],
            subvol_path[4],
            subvol_path[7],
            subvol_path[10],
        ]
        subvol_nfs_mount = [
            subvol_path[2],
            subvol_path[5],
            subvol_path[8],
            subvol_path[11],
        ]
        log.info("Fuse Mount")
        fuse_mount_dir = []
        kernel_mount_dir = []
        nfs_mount_dir = []
        rand = "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        for i in range(4):
            fuse_mount_dir.append(f"/mnt/fuse_{rand}_{i}")
            kernel_mount_dir.append(f"/mnt/kernel_{rand}_{i}")
            nfs_mount_dir.append(f"/mnt/nfs_{rand}_{i}")

        mon_node_ips = fs_util.get_mon_node_ips()
        export_list = []
        mount_dic = {}
        for i in range(len(subvol_fuse_mount)):
            fs_util.fuse_mount(
                [client1],
                fuse_mount_dir[i],
                extra_params=f" -r {subvol_fuse_mount[i]} --client_fs {fs_name}",
            )
            mount_dic[subvol_fuse_mount[i]] = fuse_mount_dir[i]
        for i in range(len(subvol_kernel_mount)):
            fs_util.kernel_mount(
                [client1],
                kernel_mount_dir[i],
                ",".join(mon_node_ips),
                sub_dir=subvol_kernel_mount[i],
                extra_params=f",fs={fs_name}",
            )
            mount_dic[subvol_kernel_mount[i]] = kernel_mount_dir[i]
        for i in range(len(subvol_nfs_mount)):
            nfs_export_name = "/export_" + "".join(
                secrets.choice(string.digits) for i in range(3)
            )
            export_list.append(nfs_export_name)
            cmd = f"ceph nfs export create cephfs {nfs_name} {nfs_export_name} {fs_name} --path={subvol_nfs_mount[i]}"
            log.info(cmd)
            client1.exec_command(sudo=True, cmd=cmd)
            fs_util.cephfs_nfs_mount(
                client1, nfs_server, nfs_export_name, nfs_mount_dir[i]
            )
            mount_dic[subvol_nfs_mount[i]] = nfs_mount_dir[i]
        for i in range(3):
            print(fuse_mount_dir[i], subvol_fuse_mount[i])
            print(kernel_mount_dir[i], subvol_kernel_mount[i])
            print(nfs_mount_dir[i], subvol_nfs_mount[i])
        io_list = fuse_mount_dir + kernel_mount_dir + nfs_mount_dir
        with parallel() as p:
            p.spawn(
                fs_util.run_ios(client1, f"{io_list[0]}/", ["dd"])
            )  # Add trailing slash
            p.spawn(fs_util.run_ios(client1, f"{io_list[1]}/", ["dd"]))
            p.spawn(fs_util.run_ios(client1, f"{io_list[2]}/", ["dd"]))
            p.spawn(fs_util.run_ios(client1, f"{io_list[3]}/", ["dd"]))
            p.spawn(fs_util.run_ios(client1, f"{io_list[4]}/", ["dd"]))
            p.spawn(fs_util.run_ios(client1, f"{io_list[5]}/", ["dd"]))
            p.spawn(fs_util.run_ios(client1, f"{io_list[6]}/", ["dd"]))
            p.spawn(fs_util.run_ios(client1, f"{io_list[7]}/", ["dd"]))
            p.spawn(fs_util.run_ios(client1, f"{io_list[8]}/", ["dd"]))

        log.info("Create Snapshot on subvolumes")
        snap_list1 = [
            {
                "vol_name": f"{fs_name}",
                "subvol_name": "subvol_1",
                "snap_name": "snap_1",
                "group_name": "subvol_group_name_1",
            },
            {
                "vol_name": f"{fs_name}",
                "subvol_name": "subvol_2",
                "snap_name": "snap_2",
                "group_name": "subvol_group_name_1",
            },
            {
                "vol_name": f"{fs_name}",
                "subvol_name": "subvol_3",
                "snap_name": "snap_3",
                "group_name": "subvol_group_name_1",
            },
        ]
        for snapshot in snap_list1:
            fs_util.create_snapshot(client1, **snapshot)
        snap_list2 = [
            {
                "vol_name": f"{fs_name}",
                "subvol_name": "subvol_4",
                "snap_name": "snap_4",
                "group_name": "subvol_group_name_1",
            },
            {
                "vol_name": f"{fs_name}",
                "subvol_name": "subvol_5",
                "snap_name": "snap_5",
                "group_name": "subvol_group_name_1",
            },
            {
                "vol_name": f"{fs_name}",
                "subvol_name": "subvol_6",
                "snap_name": "snap_6",
                "group_name": "subvol_group_name_1",
            },
        ]
        clone_list = [
            {
                "vol_name": fs_name,
                "subvol_name": "subvol_4",
                "snap_name": "snap_4",
                "target_subvol_name": "clone4",
                "group_name": "subvol_group_name_1",
                "target_group_name": "subvol_group_name_1",
            },
            {
                "vol_name": fs_name,
                "subvol_name": "subvol_5",
                "snap_name": "snap_5",
                "target_subvol_name": "clone5",
                "group_name": "subvol_group_name_1",
                "target_group_name": "subvol_group_name_1",
            },
            {
                "vol_name": fs_name,
                "subvol_name": "subvol_6",
                "snap_name": "snap_6",
                "target_subvol_name": "clone6",
                "group_name": "subvol_group_name_1",
                "target_group_name": "subvol_group_name_1",
            },
        ]
        for snapshot in snap_list2:
            fs_util.create_snapshot(client1, **snapshot)
        for clone in clone_list:
            fs_util.create_clone(client1, **clone)
        file_quota = 200
        byte_quota = 10000000
        file_quota_list = [fuse_mount_dir[2], kernel_mount_dir[2]]
        byte_quota_list = [fuse_mount_dir[3], kernel_mount_dir[3]]
        for directory in file_quota_list:
            log.info(f"Setting File Quota on {subvol_path[i]}")
            fs_util.set_quota_attrs(client1, file_quota, "", f"{directory}")
        for directory in byte_quota_list:
            log.info(f"Setting Byte Quota on {directory}")
            fs_util.set_quota_attrs(client1, "", byte_quota, f"{directory}")
        log.info("Quotas befroe rename")
        file_quotas = []
        byte_quotas = []
        for directory in file_quota_list:
            log.info(f"Getting File Quota on {directory}")
            out = fs_util.get_quota_attrs(client1, directory)
            log.info(out)
            file_quotas.append(out)
        for directory in byte_quota_list:
            log.info(f"Getting Byte Quota on {directory}")
            out = fs_util.get_quota_attrs(client1, directory)
            log.info(out)

            byte_quotas.append(out)
        log.info(file_quotas)
        log.info(byte_quotas)
        fs_util.rename_volume(client1, fs_name, "cephfs_2")
        fs_name = "cephfs_2"
        for i in range(len(file_quotas)):
            if file_quotas[i]["files"] != 200:
                log.error("File quota has changed")
                raise Exception("File Quota is not set")
            if byte_quotas[i]["bytes"] != 10000000:
                log.error("Byte quota has changed")
                raise Exception("Byte Quota is not set")
        clone_list_to_check = ["clone4", "clone5", "clone6"]
        snap_list3 = [
            {
                "vol_name": f"{fs_name}",
                "subvol_name": "subvol_7",
                "snap_name": "snap_7",
                "group_name": "subvol_group_name_1",
            },
            {
                "vol_name": f"{fs_name}",
                "subvol_name": "subvol_8",
                "snap_name": "snap_8",
                "group_name": "subvol_group_name_1",
            },
            {
                "vol_name": f"{fs_name}",
                "subvol_name": "subvol_9",
                "snap_name": "snap_9",
                "group_name": "subvol_group_name_1",
            },
        ]
        log.info("####Snapshot after rename####")
        snap_list_to_check = [
            "snap_1",
            "snap_2",
            "snap_3",
            "snap_4",
            "snap_5",
            "snap_6",
            "snap_7",
            "snap_8",
            "snap_9",
        ]
        subvol_name_list = [
            "subvol_1",
            "subvol_2",
            "subvol_3",
            "subvol_4",
            "subvol_5",
            "subvol_6",
            "subvol_7",
            "subvol_8",
            "subvol_9",
        ]
        log.info("###########################################")
        log.info("##############Snapshot check###############")
        log.info("###########################################")
        for snapshot in snap_list3:
            fs_util.create_snapshot(client1, **snapshot, validate=True)
        for i, (subv, snap) in enumerate(zip(subvol_name_list, snap_list_to_check)):
            out, ec = client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume snapshot ls {fs_name} {subv} --group_name subvol_group_name_1",
            )
            if snap not in out:
                log.error(f"Snapshot {snap} is not present")
                raise Exception(f"Snapshot {snap} is not present")
        log.info("####clone check####")
        for clone in clone_list_to_check:
            out, ec = client1.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume ls {fs_name}  --group_name subvol_group_name_1",
            )
            log.info(out)
            if clone not in out:
                log.error(f"Clone {clone} is not present")
                raise Exception(f"Clone {clone} is not present")
        log.info("####Clone,quota,snapshot are verified after rename####")
        client1.exec_command(sudo=True, cmd="dnf install jq -y")
        log.info("###########################################")
        log.info("##############Scenario 2###################")
        log.info("###########################################")
        log.info("####compare metrics####")
        snaputil = SnapUtils(ceph_cluster)
        snaputil.enable_snap_schedule(client1)
        metric_dir = [fuse_mount_dir[0], kernel_mount_dir[0], nfs_mount_dir[0]]
        daemons_value = {"mds": 10}
        fs_util.enable_logs(client1, daemons_value)
        metrics_before = fs_util.get_mds_metrics(
            client1, 0, metric_dir[0], fs_name=fs_name
        )
        fs_util.rename_volume(client1, fs_name, "cephfs_3")
        fs_name = "cephfs_3"
        time.sleep(10)
        metrics_after = fs_util.get_mds_metrics(
            client1, 0, metric_dir[0], fs_name=fs_name
        )
        rw_list = [
            "total_read_ops",
            "total_read_size",
            "total_write_ops",
            "total_write_size",
        ]
        for read in rw_list:
            log.info(
                f"Metrics before rename {read} : {metrics_before['counters'][read]}"
            )
            log.info(f"Metrics after rename {read} : {metrics_after['counters'][read]}")
            if metrics_before["counters"][read] > metrics_after["counters"][read]:
                log.error(f"{read} is not same")
                raise Exception(f"{read} is not same")
        log.info("####Metrics are validated after rename####")
        # log.info("####Snapshot Schedule####")
        # log.info("Creating snapshot schedules for two paths with a 1-minute interval")
        # Commenting this due to snap_schdeule issue for now
        # interval = "3M"
        # snap_path1 = f"{fuse_mount_dir[0]}"
        # snap_path2 = f"{kernel_mount_dir[0]}"
        # snap_params_1 ={}
        # # snap_params_1["sched"] = interval
        # # snap_params_1["fs_name"] = fs_name
        # # snap_params_1["validate"] = True
        # # snap_params_1["group_name"] = "subvol_group_name_1"
        # # snap_params_1["path"] = f"{snap_path1}"
        # # snap_params_1["client"] = client1
        # # #pdb.set_trace()
        # # snaputil.create_snap_schedule(snap_params_1)
        # # snaputil.create_snap_schedule(**snap_params_2)
        # snaputil.check_snap_sched_active(client1, snap_path1,state="True",fs_name=fs_name)
        log.info("###########################################")
        log.info("##############Scenario 3###################")
        log.info("###########################################")
        log.info("Add additional Data-pool")
        pool_name = "cephfs-new-data-pool"
        fs_util.create_osd_pool(client1, pool_name=pool_name, pg_num=32, pgp_num=32)
        cmd = f"ceph fs add_data_pool {fs_name} {pool_name}"
        client1.exec_command(sudo=True, cmd=cmd)
        log.info("####Data pool is added####")
        log.info("####Checking if the data-pool name is added####")
        out, ec = client1.exec_command(sudo=True, cmd="ceph fs ls")
        if pool_name not in out:
            log.error(f"{pool_name} is not present")
            raise Exception(f"{pool_name} is not present")
        out, ec = client1.exec_command(sudo=True, cmd="ceph osd lspools")
        if pool_name not in out:
            log.error(f"{pool_name} is not present")
            raise Exception(f"{pool_name} is not present")
        fs_util.rename_volume(client1, fs_name, "cephfs_4")
        fs_name = "cephfs_4"
        conf_set = "ceph config set client"
        conf_get = "ceph config get client"
        conf_default1 = [
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
        for conf in conf_default1:
            output = client1.exec_command(sudo=True, cmd=f"{conf_get} {conf[0]}")
            conf[1] = output[0].strip()
        log.info(f"\n conf default values: {conf_default1}")
        conf_target1 = [
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
        conf_expected1 = [
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
        for conf in conf_default1:
            client_conf = conf[0]
            output = client1.exec_command(
                sudo=True, cmd=f"ceph config get client {client_conf}"
            )
            conf[1] = output[0].strip()

        for target, expected in zip(conf_target1, conf_expected1):
            client_conf = target[0]
            value = target[1]
            client1.exec_command(sudo=True, cmd=f"{conf_set} {client_conf} {value}")
        fs_util.rename_volume(client1, fs_name, "cephfs_5")
        fs_name = "cephfs_5"
        for target, expected in zip(conf_target1, conf_expected1):
            client_conf = target[0]
            output = client1.exec_command(
                sudo=True, cmd=f"ceph config get client {client_conf}"
            )
            if output[0].rstrip() != expected[1]:
                raise Exception(f"Failed to set {client_conf} to {value}")
        for conf in conf_default1:
            client_conf = conf[0]
            output = client1.exec_command(
                sudo=True,
                cmd=f"ceph config set client {client_conf} {conf[1]}",
                check_ec=False,
            )
        log.info("Successfully set all the client config values")

        ceph_version = get_ceph_version_from_cluster(clients[0])
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
            set_balance_automate_and_validate(client1, fs_name=fs_name)
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
            (
                ["mds_kill_skip_replaying_inotable", ""]
                if LooseVersion(ceph_version) <= LooseVersion("19.1.1")
                else ["mds_kill_after_journal_logs_flushed", ""]
            ),
            ["mds_wipe_sessions", ""],
            ["mds_wipe_ino_prealloc", ""],
            ["mds_skip_ino", ""],
            ["mds_min_caps_per_client", ""],
        ]
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
            (
                ("mds_kill_skip_replaying_inotable", "true")
                if LooseVersion(ceph_version) <= LooseVersion("19.1.1")
                else ("mds_kill_after_journal_logs_flushed", "true")
            ),
            ("mds_wipe_sessions", "true"),
            ("mds_wipe_ino_prealloc", "true"),
            ("mds_skip_ino", "1"),
            ("mds_min_caps_per_client", "101"),
        ]
        conf_set = "ceph config set mds"
        conf_get = "ceph config get mds"
        for conf in conf_default:
            output = client1.exec_command(sudo=True, cmd=f"{conf_get} {conf[0]}")
            conf[1] = output[0].strip()
        for target in conf_target:
            client_conf = target[0]
            value = target[1]
            client1.exec_command(sudo=True, cmd=f"{conf_set} {client_conf} {value}")
        for target in conf_target:
            client_conf = target[0]
            output = client1.exec_command(
                sudo=True, cmd=f"ceph config get mds {client_conf}"
            )
            log.info(f"{client_conf} = {output[0].rstrip()}")
            log.info(f"{target[1]} = {target[1]}")
            if output[0].rstrip() != target[1]:
                raise CommandFailed(f"Failed to set {client_conf} to {value}")
        for conf in conf_default:
            client_conf = conf[0]
            value = conf[1]
            client1.exec_command(sudo=True, cmd=f"{conf_set} {client_conf} {value}")
        log.info("###########################################")
        log.info("##############Scenario 4###################")
        log.info("###########################################")
        log.info("####Volume Rename in multi fs scenario####")
        new_fs_name = "cephfs_multi"
        fs_util.create_fs(client1, new_fs_name)
        rand = "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        multi_mount_dir = f"/mnt/fuse_{rand}_multi"
        fs_util.fuse_mount(
            [client1], multi_mount_dir, extra_params=f" --client_fs {new_fs_name}"
        )
        log.info("####Fail mgr scenario####")
        node = ceph_cluster.get_nodes(role="mgr")[0]
        services = get_service_id(node, "mgr")
        print(services)
        regex, mgr_service = r"(?<=@mgr\.)[\w.-]+(?=\.service)", None
        mgr_service = None
        for service in services:
            mgr_service = re.findall(regex, service)
            if mgr_service:
                break
        # Fail mgr service
        if CephAdm(node).ceph.mgr.fail(mgr=mgr_service[0]):
            raise Exception("Failed to fail mgr service")
        mgrs = ceph_cluster.get_ceph_objects("mgr")
        for node in mgrs:
            for service in services:
                node.exec_command(
                    sudo=True, cmd=f"systemctl restart {service}", check_ec=False
                )
        time.sleep(10)
        log.info("####Waiting for mgr daemon is active####")
        mgr_out, ec = client1.exec_command(
            sudo=True, cmd="ceph orch ps --daemon_type=mgr -f json-pretty"
        )
        mgr_out = json.loads(mgr_out)
        log.info(mgr_out[0])
        mgr_active = {}
        log.info(f"mgr type: {type(mgr_out)}")
        for item in mgr_out:
            log.info(item)
            mgr_active[item["daemon_name"]] = item["is_active"]
        for key, value in mgr_active.items():
            if value == "False":
                log.error(f"{key} is not active")
                raise Exception(f"{key} is not active")
        log.info("####Mgr is active####")
        log.info("####Volume Rename####")
        names_after_mgr_fail, _ = client1.exec_command(sudo=True, cmd="ceph fs ls")
        if new_fs_name not in names_after_mgr_fail:
            log.error(f"{new_fs_name} is not present")
            raise Exception(f"{new_fs_name} is not present")
        log.info("####Volume Rename is validated when mgr fail over case####")
        log.info(
            "####Configuring the standby-replay on primary volume and after renaming "
            "the standby-replay should function properly####"
        )
        log.info("setting standby-replay on renamed volume")
        client1.exec_command(
            sudo=True, cmd=f"ceph fs set {fs_name} allow_standby_replay true"
        )
        time.sleep(10)
        log.info("Check if the standby-replay is set")
        out, ec = client1.exec_command(sudo=True, cmd=f"ceph fs status {fs_name}")
        log.info(out)
        if "standby-replay" not in out:
            log.error("standby-replay is not set")
            raise Exception("standby-replay is not set")
        log.info("####Fail rank 0 mds and check if the standby mds replace it####")
        mds_status = json.loads(
            client1.exec_command(
                sudo=True, cmd=f"ceph fs status {fs_name} -f json-pretty"
            )[0]
        )
        log.info(mds_status)
        rank0_mds = next(
            (mds["name"] for mds in mds_status["mdsmap"] if mds["rank"] == 0), None
        )
        if not rank0_mds:
            raise Exception("No active rank 0 MDS found")
        log.info(f"Failing rank 0 MDS: {rank0_mds}")
        client1.exec_command(sudo=True, cmd=f"ceph mds fail {rank0_mds}")
        log.info(
            "####Fail the rank0 mds and ensure that the standby-replay node becomes active and takes over rank0.####"
        )
        # Verify failover
        time.sleep(30)  # Allow time for failover
        new_status = json.loads(
            client1.exec_command(
                sudo=True, cmd=f"ceph fs status {fs_name} -f json-pretty"
            )[0]
        )
        new_rank0 = next(
            (mds["name"] for mds in new_status["mdsmap"] if mds["rank"] == 0), None
        )
        new_mds_info = next(
            (mds for mds in new_status["mdsmap"] if mds["name"] == new_rank0), None
        )
        log.info(f"failed mds info {rank0_mds}")
        log.info(f"new mds info {new_rank0}")
        if new_rank0 == rank0_mds:
            log.error(f"[FAIL] MDS failover - Same rank 0 active: {rank0_mds}")
            raise Exception("MDS failover failed - same rank 0 MDS still active")

        log.info(
            f"[PASS] New active MDS: {new_rank0} (State: {new_mds_info['state'].upper()})"
        )

        log.info("Standby-replay successfully replaced failed MDS")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        new_fs_name = "cephfs_multi"
        client1.exec_command(
            sudo=True,
            cmd="ceph config set mon mon_allow_pool_delete true",
            check_ec=False,
        )
        log.info("####cleanup####")
        client1.exec_command(
            sudo=True, cmd="ceph nfs cluster delete cephfs-nfs", check_ec=False
        )
        fs_util.remove_fs(client1, new_fs_name, check_ec=False)
        # clean up data pool
        client1.exec_command(
            sudo=True,
            cmd=f"ceph osd pool rm {pool_name} {pool_name} --yes-i-really-really-mean-it",
            check_ec=False,
        )
        fs_util.remove_snapshot(
            client1,
            fs_name,
            "subvol_4",
            "snap_4",
            group_name="subvol_group_name_1",
            check_ec=False,
        )
        fs_util.remove_snapshot(
            client1,
            fs_name,
            "subvol_5",
            "snap_5",
            group_name="subvol_group_name_1",
            check_ec=False,
        )
        fs_util.remove_snapshot(
            client1,
            fs_name,
            "subvol_6",
            "snap_6",
            group_name="subvol_group_name_1",
            check_ec=False,
        )
        fs_util.remove_fs(client1, fs_name, check_ec=False)
        for export in export_list:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export delete {nfs_name} {export}",
                check_ec=False,
            )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} ",
            check_ec=False,
        )


def validate_renamed_volume(client, renamed):
    log.info("####ceph fs ls#####")
    out, ec = client.exec_command(sudo=True, cmd="ceph fs ls")
    if renamed not in out:
        log.error(f"{renamed} is not present")
        raise Exception(f"{renamed} is not present")
    log.info("####ceph fs status#####")
    out, ec = client.exec_command(sudo=True, cmd=f"ceph fs status {renamed}")
    if renamed not in out:
        log.error(f"{renamed} is not ceph fs status")
        raise Exception(f"{renamed} is not status")
    log.info("####ceph fs dump#####")
    out, ec = client.exec_command(sudo=True, cmd="ceph fs dump")
    if renamed not in out:
        log.error(f"{renamed} is not present in ceph fs dump")
        raise Exception(f"{renamed} is not present in ceph fs dump")
    log.info("####ceph fs volume info#####")
    out, ec = client.exec_command(sudo=True, cmd=f"ceph fs volume info {renamed}")
    if not out:
        log.error(f"{renamed} is not present in ceph fs volume info")
        raise Exception(f"{renamed} is not present in ceph fs volume info")
    log.info("####cephfs-top --dumpfs#####")
    out, ec = client.exec_command(sudo=True, cmd=f"cephfs-top --dumpfs {renamed}")
    if renamed not in out:
        log.error(f"{renamed} is not present in cephfs-top --dumpfs")
        raise Exception(f"{renamed} is not present in cephfs-top --dumpfs")


def set_balance_automate_and_validate(client, fs_name="cephfs"):
    log.info(
        "Validate if the Balance Automate is set to False by default and reset after toggling the values"
    )
    out, rc = client.exec_command(sudo=True, cmd=f"ceph fs get {fs_name} -f json")
    json_data = json.loads(out)
    default_balance_automate_value = json_data["mdsmap"]["flags_state"][
        "balance_automate"
    ]

    if str(default_balance_automate_value).lower() != "false":
        log.error(
            f"balance automate values is expected to be disabled by default.\n "
            f"but the value is {default_balance_automate_value}"
        )
        raise CommandFailed("Set Balance Automate value is not set to default value")

    log.info("Set Balance Automate to True and Validate")
    client.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} balance_automate true")
    out, rc = client.exec_command(sudo=True, cmd=f"ceph fs get {fs_name} -f json")
    json_data = json.loads(out)
    balance_automate = json_data["mdsmap"]["flags_state"]["balance_automate"]

    if str(balance_automate).lower() != "true":
        log.error(
            f"balance automate values is unable to set to True.\n "
            f"and the current value is {balance_automate}"
        )
        raise CommandFailed("Unable to set Balance Automate value to True")

    log.info("Set Balance Automate back to Default Value")
    client.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} balance_automate false")

    out, rc = client.exec_command(sudo=True, cmd=f"ceph fs get {fs_name} -f json")
    json_data = json.loads(out)
    balance_automate = json_data["mdsmap"]["flags_state"]["balance_automate"]
    if str(balance_automate).lower() != "false":
        log.error(
            f"balance automate values is unable to set to default value.\n "
            f"but the value is {balance_automate}"
        )
        raise CommandFailed("Unable to set Balance Automate value to default value")
