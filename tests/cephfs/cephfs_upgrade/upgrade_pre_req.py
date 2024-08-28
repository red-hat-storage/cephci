import json
import random
import string
import time
import traceback
from distutils.version import LooseVersion

import requests
import yaml

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log
from utility.retry import retry
from utility.utils import get_ceph_version_from_cluster, get_cephci_config

log = Log(__name__)

magna_server = "http://magna002.ceph.redhat.com"
magna_url = f"{magna_server}/cephci-jenkins/"
magna_rhcs_artifacts = f"{magna_server}/cephci-jenkins/latest-rhceph-container-info/"


def run(ceph_cluster, **kw):
    """
    Create multiple file systems(include EC-pool).
    Collect the information of MDS(like which is active mds node and standby mds nodes details)
    Collect the status of the cluster and version
    Create subvolumegroups, subvolumes
    Mount using fuse and kernel mount different subvolumes and also cephfs root folder on to different locations.
    Create a sample file, folder and get the stat details of it.
    Create NFS cluster and mount it. → 5.0 feature
    Taking snapshots and clones of the volume (Scheduled snapshots, retain snapshots → 5.0 feature).
    Client authorize and subvolume authorize feature
    Dir pinning and set quota to the subvolume
    Run IOs on all the mount points from a different client machine.
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        snap_util = SnapUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")
        if len(clients) < 2:
            log.info(
                f"This test requires minimum 2 client nodes.This has only {len(clients)} clients"
            )
            return 1
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        osd_cmds = {
            "osd_stats_update_period_not_scrubbing": 2,
            "osd_stats_update_period_scrubbing": 2,
            "osd_pg_stat_report_interval_max": 5,
        }
        for osd_cmd in osd_cmds:
            cmd = f"ceph config set osd {osd_cmd} {osd_cmds[osd_cmd]}"
            out, rc = clients[0].exec_command(sudo=True, cmd=cmd, check_ec=False)
            if not rc:
                cmd = f"ceph config get osd {osd_cmd}"
                out, _ = clients[0].exec_command(sudo=True, cmd=cmd, check_ec=False)
                log.info(out)
                if out:
                    if str(osd_cmds[osd_cmd]) not in str(out):
                        log.warning(
                            f"OSD config {osd_cmd} couldn't be set to {osd_cmds[osd_cmd]}"
                        )
        time.sleep(10)
        default_fs = "cephfs"
        version, rc = clients[0].exec_command(
            sudo=True, cmd="ceph version --format json"
        )
        ceph_version = json.loads(version)
        ceph_config = {}
        nfs_mounting_dir = "/mnt/nfs/"
        dir_name = "dir"
        list_cmds = [
            "ceph fs flag set enable_multiple true",
            "ceph osd pool create cephfs-data-ec 64 erasure",
            "ceph osd pool create cephfs-metadata 64",
            "ceph osd pool set cephfs-data-ec allow_ec_overwrites true",
            "ceph fs new cephfs-ec cephfs-metadata cephfs-data-ec --force",
        ]
        if fs_util.get_fs_info(clients[0], "cephfs_new"):
            default_fs = "cephfs_new"
            list_cmds.append("ceph fs volume create cephfs")
        for cmd in list_cmds:
            clients[0].exec_command(sudo=True, cmd=cmd)
        upgrade_config = None
        vol_list = [default_fs, "cephfs-ec"]
        ceph_config["CephFS"] = {}
        ceph_config["NFS"] = {}
        for vol_name in vol_list:
            ceph_config["CephFS"][vol_name] = {}
        with open(
            "tests/cephfs/cephfs_upgrade/config.json",
            "r",
        ) as f:
            upgrade_config = json.load(f)
        svg_list = [
            f"{upgrade_config.get('subvolume_group_prefix', 'upgrade_svg')}_{svg}"
            for svg in range(0, upgrade_config.get("subvolume_group_count", 3))
        ]
        subvolumegroup_list = [
            {"vol_name": v, "group_name": svg} for v in vol_list for svg in svg_list
        ]
        log.info(subvolumegroup_list)
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(clients[0], **subvolumegroup)

        for v in vol_list:
            for svg in svg_list:
                ceph_config["CephFS"][v][svg] = {}

        subvolume_list = [
            {
                "vol_name": v,
                "group_name": svg,
                "subvol_name": f"{upgrade_config.get('subvolume_prefix', 'upgrade_sv')}_{sv}",
            }
            for v in vol_list
            for svg in svg_list
            for sv in range(0, upgrade_config.get("subvolume_count", 3))
        ]

        for subvolume in subvolume_list:
            fs_util.create_subvolume(clients[0], **subvolume)
            ceph_config["CephFS"][subvolume["vol_name"]][subvolume["group_name"]][
                subvolume["subvol_name"]
            ] = {}

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        mount_points = {"kernel_mounts": [], "fuse_mounts": [], "nfs_mounts": []}
        for sv in subvolume_list[::2]:
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {sv['vol_name']} {sv['subvol_name']} {sv['group_name']}",
            )
            mon_node_ips = fs_util.get_mon_node_ips()
            kernel_mounting_dir_1 = (
                f"/mnt/cephfs_kernel{mounting_dir}_{sv['vol_name']}_{sv['group_name']}_"
                f"{sv['subvol_name']}/"
            )
            if "nautilus" not in ceph_version["version"]:
                fs_util.kernel_mount(
                    [clients[0]],
                    kernel_mounting_dir_1,
                    ",".join(mon_node_ips),
                    sub_dir=f"{subvol_path.strip()}",
                    extra_params=f",fs={sv['vol_name']}",
                )
                mount_points["kernel_mounts"].append(kernel_mounting_dir_1)
                mnt_params = {
                    "mnt_pt": kernel_mounting_dir_1,
                    "mnt_client": clients[0].node.hostname,
                }
                ceph_config["CephFS"][sv["vol_name"]][sv["group_name"]][
                    sv["subvol_name"]
                ].update(mnt_params)
            else:
                if sv["vol_name"] == default_fs:
                    fs_util.kernel_mount(
                        [clients[0]],
                        kernel_mounting_dir_1,
                        ",".join(mon_node_ips),
                        sub_dir=f"{subvol_path.strip()}",
                    )
                    mount_points["kernel_mounts"].append(kernel_mounting_dir_1)
                    mnt_params = {
                        "mnt_pt": kernel_mounting_dir_1,
                        "mnt_client": clients[0].node.hostname,
                    }
                    ceph_config["CephFS"][sv["vol_name"]][sv["group_name"]][
                        sv["subvol_name"]
                    ].update(mnt_params)

        for sv in subvolume_list[1::2]:
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {sv['vol_name']} {sv['subvol_name']} {sv['group_name']}",
            )
            fuse_mounting_dir_1 = (
                f"/mnt/cephfs_fuse{mounting_dir}_{sv['vol_name']}_{sv['group_name']}_"
                f"{sv['subvol_name']}/"
            )
            if "nautilus" not in ceph_version["version"]:
                fs_util.fuse_mount(
                    [clients[0]],
                    fuse_mounting_dir_1,
                    extra_params=f"-r {subvol_path.strip()} --client_fs {sv['vol_name']}",
                )
                mount_points["fuse_mounts"].append(fuse_mounting_dir_1)
                mnt_params = {
                    "mnt_pt": fuse_mounting_dir_1,
                    "mnt_client": clients[0].node.hostname,
                }
                ceph_config["CephFS"][sv["vol_name"]][sv["group_name"]][
                    sv["subvol_name"]
                ].update(mnt_params)
            else:
                if sv["vol_name"] == default_fs:
                    fs_util.fuse_mount(
                        [clients[0]],
                        fuse_mounting_dir_1,
                        extra_params=f"-r {subvol_path.strip()} ",
                    )
                    mount_points["fuse_mounts"].append(fuse_mounting_dir_1)
                    mnt_params = {
                        "mnt_pt": fuse_mounting_dir_1,
                        "mnt_client": clients[0].node.hostname,
                    }
                    ceph_config["CephFS"][sv["vol_name"]][sv["group_name"]][
                        sv["subvol_name"]
                    ].update(mnt_params)

        subvol_snap_clone = [subvolume_list[0]]
        # Get one more subvolume from diff svg but same cephfs volume
        for subvolume in subvolume_list:
            if subvolume["vol_name"] == subvol_snap_clone[0]["vol_name"]:
                if subvolume["group_name"] not in subvol_snap_clone[0]["group_name"]:
                    if (
                        subvolume["subvol_name"]
                        not in subvol_snap_clone[0]["subvol_name"]
                    ):
                        subvol_snap_clone.append(subvolume)
                        break

        mnt_list = {}
        for subvol in subvol_snap_clone:
            for mount in mount_points["kernel_mounts"]:
                if (
                    (subvol["subvol_name"] in mount)
                    and subvol["vol_name"] in mount
                    and subvol["group_name"] in mount
                ):
                    if "cephfs-ec" not in mount:
                        mnt_list[subvol["subvol_name"]] = mount
                        file_path = f"{mount}/dd_test_file"
            for mount in mount_points["fuse_mounts"]:
                if (
                    subvol["subvol_name"] in mount
                    and subvol["vol_name"] in mount
                    and subvol["group_name"] in mount
                ):
                    if "cephfs-ec" not in mount:
                        mnt_list[subvol["subvol_name"]] = mount
                        file_path = f"{mount}/dd_test_file"

            clients[0].exec_command(
                sudo=True,
                cmd=f"dd if=/dev/urandom of={file_path} bs=1M " f"count=100",
                long_running=True,
            )

            log.info(f"Create manual snapshots on subvolume - {subvol['subvol_name']}")
            snap_list = []
            for i in range(1, 3):
                snapshot = {
                    "vol_name": subvol["vol_name"],
                    "subvol_name": subvol["subvol_name"],
                    "snap_name": f"snap{i}_{subvol['subvol_name']}",
                    "group_name": subvol["group_name"],
                }
                fs_util.create_snapshot(clients[0], **snapshot)
                snap_list.append(f"snap{i}_{subvol['subvol_name']}")
            snap_params = {"snap_list": snap_list}
            ceph_config["CephFS"][subvol["vol_name"]][subvol["group_name"]][
                subvol["subvol_name"]
            ].update(snap_params)

            log.info(f"Create clone of snapshot on subvolume - {subvol['subvol_name']}")
            clone_name = f"Clone_{subvol['subvol_name']}"
            clone_config = {
                "vol_name": subvol["vol_name"],
                "subvol_name": subvol["subvol_name"],
                "snap_name": f"snap2_{subvol['subvol_name']}",
                "target_subvol_name": clone_name,
                "group_name": subvol["group_name"],
                "target_group_name": subvol["group_name"],
            }

            fs_util.create_clone(clients[0], **clone_config)
            fs_util.validate_clone_state(clients[0], clone_config)

            clone_params = {
                "clone": clone_name,
            }
            ceph_config["CephFS"][subvol["vol_name"]][subvol["group_name"]][
                subvol["subvol_name"]
            ].update(clone_params)
            ceph_config["CephFS"][subvol["vol_name"]][subvol["group_name"]][
                clone_name
            ] = {}
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {subvol['vol_name']} {clone_name} {subvol['group_name']}",
            )
            fuse_mounting_dir_1 = (
                f"/mnt/cephfs_fuse{mounting_dir}_{subvol['vol_name']}_{subvol['group_name']}_"
                f"{clone_name}/"
            )

            fs_util.fuse_mount(
                [clients[0]],
                fuse_mounting_dir_1,
                extra_params=f"-r {subvol_path.strip()} --client_fs {subvol['vol_name']}",
            )
            mnt_params = {
                "mnt_pt": fuse_mounting_dir_1,
                "mnt_client": clients[0].node.hostname,
            }
            ceph_config["CephFS"][subvol["vol_name"]][subvol["group_name"]][
                clone_name
            ].update(mnt_params)

        log.info("Create a snapshot-schedule and apply to 2 subvolumes")

        subvol_sched_snap = []

        for sv in subvolume_list:
            if sv["subvol_name"] not in [
                subvol_snap_clone[0]["subvol_name"],
                subvol_snap_clone[1]["subvol_name"],
            ]:
                if sv["vol_name"] == subvol_snap_clone[0]["vol_name"]:
                    subvol_sched_snap.append(sv)
                    if len(subvol_sched_snap) == 2:
                        break
        snap_util.allow_minutely_schedule(clients[0], allow=True)
        fs_util.validate_services(clients[0], service_name="mgr")
        time.sleep(60)
        for subvol in subvol_sched_snap:
            snap_params = {}
            snap_util.enable_snap_schedule(clients[0])
            snap_params["fs_name"] = subvol["vol_name"]
            snap_params["validate"] = True
            snap_params["client"] = clients[0]
            # sched_list = ["2M", "1h", "7d", "4w"]
            ceph_version_1 = get_ceph_version_from_cluster(clients[0])
            sched_list = (
                ["2m", "1h", "7d", "4w"]
                if LooseVersion(ceph_version_1) >= LooseVersion("17.2.6")
                else ["2M", "1h", "7d", "4w"]
            )
            snap_params["retention"] = "5M5h5d4w"

            cmd = f"ceph fs subvolume getpath {subvol['vol_name']} {subvol['subvol_name']} "
            cmd += f"{subvol['group_name']}"
            retry_exec = retry(CommandFailed, tries=3, delay=60)(
                clients[0].exec_command
            )
            subvol_path, rc = retry_exec(
                sudo=True,
                cmd=cmd,
            )
            snap_params["path"] = f"{subvol_path.strip()}/.."
            for sched_val in sched_list:
                snap_params["sched"] = sched_val
                snap_util.create_snap_schedule(snap_params)
            snap_util.create_snap_retention(snap_params)
            sched_params = {
                "sched_path": snap_params["path"],
                "sched_list": sched_list,
                "retention": snap_params["retention"],
                "fs_name": snap_params["fs_name"],
            }
            ceph_config["CephFS"][subvol["vol_name"]][subvol["group_name"]][
                subvol["subvol_name"]
            ].update(sched_params)

        log.info(f"Ceph upgrade config : {ceph_config}")

        log.info("Set Dir pinning")
        dir_name = "upgrade_pin_dir"
        for subvol in subvol_snap_clone:
            clients[0].exec_command(
                cmd="sudo mkdir -p %s%s_{1..4}"
                % (mnt_list[subvol["subvol_name"]], dir_name)
            )
            mounting_dir = mnt_list[subvol["subvol_name"]]
            log.info(f"mounting_dir:{mounting_dir}")
            for num in range(1, 4):
                clients[0].exec_command(
                    sudo=True,
                    cmd="setfattr -n ceph.dir.pin -v %s %s%s_%d"
                    % (0, mounting_dir, dir_name, num),
                )
        pin_dir_list = [f"{dir_name}_{i}" for i in range(1, 4)]
        pin_params = {"pinned_dir_list": pin_dir_list, "pin_rank": 0}
        ceph_config["CephFS"][subvol["vol_name"]][subvol["group_name"]][
            subvol["subvol_name"]
        ].update(pin_params)

        log.info("Set Client and subvolume authorize")
        client_name = "test_auth"
        subvol = subvol_snap_clone[0]
        subvol_path, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {subvol['vol_name']} {subvol['subvol_name']} {subvol['group_name']}",
        )
        dir_path = f"{subvol_path.strip()}/"
        fs_util.fs_client_authorize(
            clients[0],
            subvol["vol_name"],
            client_name,
            "/",
            "r",
            extra_params=f" {dir_path} rw",
        )
        ceph_config["CephFS"][subvol["vol_name"]].update(
            {
                "client_auth": {
                    "client_name": client_name,
                    "/": "r",
                    dir_path: "rw",
                    "subvol_name": subvol["subvol_name"],
                }
            }
        )

        log.info("Get active MDS config before upgrade")
        for vol_name in vol_list:
            mds_list = fs_util.get_active_mdss(clients[0], vol_name)
            standby_list = []
            out, rc = clients[0].exec_command(
                sudo=True, cmd=f"ceph fs status {vol_name} --format json"
            )
            output = json.loads(out)
            standby_list = [
                mds["name"] for mds in output["mdsmap"] if mds["state"] == "standby"
            ]
            ceph_config["CephFS"][vol_name].update(
                {"active_mds": mds_list, "standby_mds": standby_list}
            )

        if "nautilus" not in ceph_version["version"]:
            nfs_server = ceph_cluster.get_ceph_objects("nfs")
            nfs_client = ceph_cluster.get_ceph_objects("client")
            fs_util.auth_list(nfs_client, recreate=False)
            nfs_name = "cephfs-nfs"
            fs_name = default_fs
            nfs_export_name = "/export1"
            path = "/"
            nfs_server_name = nfs_server[0].node.hostname
            # Create ceph nfs cluster
            nfs_client[0].exec_command(sudo=True, cmd="ceph mgr module enable nfs")
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server_name}"
            )
            ceph_config["NFS"].update({nfs_name: {}})
            # Verify ceph nfs cluster is created
            if wait_for_process(
                client=nfs_client[0], process_name=nfs_name, ispresent=True
            ):
                log.info("ceph nfs cluster created successfully")
            else:
                raise CommandFailed("Failed to create nfs cluster")
            # Create cephfs nfs export
            if "5.0" in build:
                nfs_client[0].exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                    f"{nfs_export_name} path={path}",
                )
            else:
                nfs_client[0].exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export create cephfs {nfs_name} "
                    f"{nfs_export_name} {fs_name} path={path}",
                )

            # Verify ceph nfs export is created
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"ceph nfs export ls {nfs_name}"
            )
            if nfs_export_name in out:
                log.info("ceph nfs export created successfully")
            else:
                raise CommandFailed("Failed to create nfs export")
            # Mount ceph nfs exports
            nfs_client[0].exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
            assert fs_util.wait_for_cmd_to_succeed(
                nfs_client[0],
                cmd=f"mount -t nfs -o port=2049 {nfs_server_name}:{nfs_export_name} {nfs_mounting_dir}",
            )
            nfs_client[0].exec_command(
                sudo=True,
                cmd=f"mount -t nfs -o port=2049 {nfs_server_name}:{nfs_export_name} {nfs_mounting_dir}",
            )
            out, rc = nfs_client[0].exec_command(cmd="mount")
            mount_output = out.split()
            log.info("Checking if nfs mount is is passed of failed:")
            assert nfs_mounting_dir.rstrip("/") in mount_output

            ceph_config["NFS"][nfs_name].update(
                {
                    nfs_export_name: {
                        "export_path": path,
                        "nfs_mnt_pt": nfs_mounting_dir,
                        "nfs_mnt_client": nfs_client[0].node.hostname,
                        "vol_name": "cephfs",
                    },
                }
            )

            log.info("Creating Directory")
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"mkdir {nfs_mounting_dir}{dir_name}"
            )
            nfs_client[0].exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
                f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top "
                f"{nfs_mounting_dir}{dir_name}",
                long_running=True,
            )
            nfs_client[0].exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 "
                f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top "
                f"{nfs_mounting_dir}{dir_name}",
                long_running=True,
            )

        else:
            clients = ceph_cluster.get_ceph_objects("client")
            nfs_server = [clients[0]]
            nfs_client = [clients[1]]

            rc = fs_util.nfs_ganesha_install(nfs_server[0])
            if rc == 0:
                log.info("NFS ganesha installed successfully")
            else:
                raise CommandFailed("NFS ganesha installation failed")
            rc = fs_util.nfs_ganesha_conf(nfs_server[0], "admin")
            if rc == 0:
                log.info("NFS ganesha config added successfully")
            else:
                raise CommandFailed("NFS ganesha config adding failed")
            rc = fs_util.nfs_ganesha_mount(
                nfs_client[0], nfs_mounting_dir, nfs_server[0].node.hostname
            )
            if rc == 0:
                log.info("NFS-ganesha mount passed")
            else:
                raise CommandFailed("NFS ganesha mount failed")

            mounting_dir = nfs_mounting_dir + "ceph/"
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"mkdir -p {mounting_dir}{dir_name}"
            )
            nfs_client[0].exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
                f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top "
                f"{mounting_dir}{dir_name}",
                long_running=True,
            )
            nfs_client[0].exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 "
                f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top "
                f"{mounting_dir}{dir_name}",
                long_running=True,
            )
            ceph_config["NFS"].update({"nfs_ganesha": {}})
            ceph_config["NFS"]["nfs_ganesha"].update(
                {
                    nfs_server[0]: {
                        "nfs_mnt_pt": nfs_mounting_dir,
                        "nfs_mnt_client": nfs_client[0].node.hostname,
                    }
                }
            )

        log.info(f"Ceph upgrade config : {ceph_config}")
        f = clients[0].remote_file(
            sudo=True,
            file_name="/home/cephuser/cephfs_upgrade_config.json",
            file_mode="w",
        )
        f.write(json.dumps(ceph_config, indent=4))
        f.write("\n")
        f.flush()

        if config.get("client_upgrade", 0) == 1:
            log.info("Upgrade Clients before Cluster upgrade")
            for client in clients:
                rh_ceph_repo_update(client, config)
                cmd = "yum install -y --nogpgcheck ceph-common ceph-fuse"
                client.exec_command(sudo=True, cmd=cmd)

        for i in mount_points["kernel_mounts"] + mount_points["fuse_mounts"]:
            fs_util.run_ios(clients[0], i)

        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1


def rh_ceph_repo_update(client, config):
    build = config.get("rhbuild")
    version, platform = build.split("-", 1)
    recipe_url = get_cephci_config().get("build-url", magna_rhcs_artifacts)
    release = f"RHCEPH-{version}"
    url = f"{recipe_url}{release}.yaml"
    data = requests.get(url, verify=False)
    base_url = yaml.safe_load(data.text)["latest"]["composes"][platform]
    f = client.remote_file(
        sudo=True,
        file_name="/etc/yum.repos.d/rh_ceph.repo",
        file_mode="r",
    )
    base_url_data = config.get("rhs-ceph-repo", base_url)
    replace_str = f"baseurl={base_url_data}/compose/Tools/x86_64/os/"
    repo_data = f.readlines()
    f.close()
    for i in range(len(repo_data)):
        line = repo_data[i]
        if "baseurl" in line:
            repo_data[i] = f"{replace_str}\n"
    f = client.remote_file(
        sudo=True,
        file_name="/etc/yum.repos.d/rh_ceph.repo",
        file_mode="w",
    )
    for line in repo_data:
        f.write(line)
    f.close()
