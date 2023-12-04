"""
This is cephfs snapshot schedule utility module
It contains all the re-useable functions related to cephfs snapshot schedule and retention feature

"""
import datetime
import json
import random
import re
import string
import time

from ceph.ceph import CommandFailed
from utility.log import Log

log = Log(__name__)


class SnapUtils(object):
    def __init__(self, ceph_cluster):
        """
        FS Snapshot Utility object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """

        self.ceph_cluster = ceph_cluster
        self.mons = ceph_cluster.get_ceph_objects("mon")
        self.mgrs = ceph_cluster.get_ceph_objects("mgr")
        self.osds = ceph_cluster.get_ceph_objects("osd")
        self.mdss = ceph_cluster.get_ceph_objects("mds")
        self.clients = ceph_cluster.get_ceph_objects("client")

    def enable_snap_schedule(self, client):
        """
        Enables Snapshot schedule on Ceph
        Args:
            client: ceph client to run cmd
        Returns: None
        """
        out, rc = client.exec_command(
            sudo=True, cmd="ceph mgr module enable snap_schedule"
        )
        log.info(out)

    def allow_minutely_schedule(self, client, allow=True):
        """
        Modifies ceph config to allow minutely snapshots in snapshot schedule
        Args:
            client: ceph client to run cmd
            allow: Boolean option to allow modify or not, True|False , default=True
        Returns: None
        """
        out, rc = client.exec_command(
            sudo=True,
            cmd=f"ceph config set mgr mgr/snap_schedule/allow_m_granularity {allow}",
        )
        log.info(out)

    def create_snap_schedule(self, snap_params):
        """
        To create snapshot schedule on ceph FS and activate it. It also verifies that schedule is set.
        Args:
            client: ceph client to run cmd
            snap_params : a dict data type with below key-value pairs,
                Required:
                    path : a path for scheduled snapshot to be created, type - str
                    sched : a schedule to be applied to snapshot schedule ass, type - str, for eg., 2M1h
                    fs_name : cephfs volume name
                Optional:
                    start_time : a iso 8601 format time value, from when schedule is to be applied. type -str
        Returns: 0 : success, 1 : failure
        """
        client = snap_params["client"]
        sched_cmd = (
            f"ceph fs snap-schedule add {snap_params['path']} {snap_params['sched']}"
        )
        if snap_params.get("start_time"):
            sched_cmd += f" {snap_params['start_time']} "
        sched_cmd += f" --fs {snap_params['fs_name']}"

        # activate_cmd = f"ceph fs snap-schedule activate {snap_params['path']} --fs {snap_params['fs_name']}"
        # for cmd in [sched_cmd, activate_cmd]:
        out, rc = client.exec_command(sudo=True, cmd=sched_cmd)
        log.info(out)

        if snap_params["validate"] is True:
            cmd = f"ceph fs snap-schedule status {snap_params['path']} --fs {snap_params['fs_name']} -f json"
            out, rc = client.exec_command(sudo=True, cmd=cmd)
            sched_status = json.loads(out)
            for sched_item in sched_status:
                log.info(f"{sched_item}")
                if (
                    sched_item["path"] == snap_params["path"]
                    and sched_item["active"] is True
                    and sched_item["schedule"] == snap_params["sched"]
                ):
                    log.info("Snap schedule is verified.")
                    return 0
            log.error(f"Snap schedule verification failed : {out}")
            return 1
        return 0

    def get_snap_schedule_list(self, client, path):
        """
        To get snap-schedule list for path on ceph FS .
        Args:
            client: ceph client to run cmd
            path : path set for snap-schedule, type - str
        Returns: a list, each item referring to a specfic snap-schedule on a path
        """
        cmd = f"ceph fs snap-schedule list {path} --recursive=true"
        cmd_out, rc = client.exec_command(sudo=True, cmd=cmd)
        log.info(cmd_out)
        sched_list = cmd_out.split()
        return sched_list

    def get_scheduled_snapshots(self, client, client_snap_path):
        """
        To get list of scheduled snapshots from client mount path.
        Args:
            client: ceph client to run cmd
            client_snap_path : an absolute path of cephfs volume in client mount point
            where snap-schedule is set, type - str
            for eg., /mnt/cephfs_kernel/volumes/subvolumegroup1/subvolume1/ for schedule path
            "/volumes/subvolumegroup1/subvolume1/.." or /mnt/cephfs_fuse/ for schedule path "/"
        Returns: a list, each item referring to a scheduled snapshot name
        """
        cmd_out, rc = client.exec_command(
            sudo=True, cmd=f"ls {client_snap_path}/.snap/| grep scheduled"
        )
        snap_list = cmd_out.strip().split()
        return snap_list

    def activate_snap_schedule(self, client, path, **kw_args):
        """
        To activate a snap-schedule for given path
        Args:
        Required:
            client: ceph client to run cmd
            path : a snap-schedule path which needs to be activated
        Optional:
            sched_val : schedule , type - str, a schedule value that needs to be activated
        Returns: None
        for eg., snap_util.activate_snap_schedule(client,activate_path,sched_val=sched_val)
        """
        cmd = f"ceph fs snap-schedule activate {path}"
        if kw_args.get("sched_val"):
            cmd += f" {kw_args.get('sched_val')}"
        client.exec_command(sudo=True, cmd=cmd)

    def deactivate_snap_schedule(self, client, path, **kw_args):
        """
        To deactivate a snap-schedule for given path
        Args:
        Required:
            client: ceph client to run cmd
            path : a snap-schedule path which needs to be deactivated
        Optional:
            sched_val : schedule , type - str, a schedule value that needs to be deactivated
        Returns: None
        for eg., snap_util.deactivate_snap_schedule(client,deactivate_path,sched_val=sched_val)
        """
        cmd = f"ceph fs snap-schedule deactivate {path}"
        if kw_args.get("sched_val"):
            cmd += f" {kw_args.get('sched_val')}"
        client.exec_command(sudo=True, cmd=cmd)

    def remove_snap_schedule(self, client, path):
        """
        To remove a snap-schedule for given path
        Args:
        Required:
            client: ceph client to run cmd
            path : a snap-schedule path which needs to be removed, type - str
        Returns: None
        """
        cmd = f"ceph fs snap-schedule remove {path}"
        client.exec_command(sudo=True, cmd=cmd)

    def validate_snap_schedule(self, client, path, sched_val):
        """
        To validate snapshot schedule set on ceph FS path by verifying snapshots created.
        Args:
            client: ceph client to run cmd
            snap_path : an absolute path in client mount for snap-schedule path, type -str
            sched_val : a schedule value that needs to be validated, type - str
            Ex:
              path : /mnt/cephfs_kernel/ for snap-schedule path "/"
              sched_val : 1M , for a snapshot every 1 minute on path
        Returns: 0 : success, 1 : failure
        """
        log.info("Validate if scheduled snapshots are getting created")
        out, rc = client.exec_command(
            sudo=True, cmd=f'ls -lrt {path}/.snap/| grep "scheduled" | wc -l'
        )
        log.info(out)
        if not (int(out) > 0):
            raise CommandFailed("It has not created the scheduled snapshot")
        log.info("Verify if snapshots created are as per defined schedule")
        null_val, sched_num, sched_type = re.split(r"(\d+)", sched_val)
        out, rc = client.exec_command(
            sudo=True, cmd=f'ls {path}/.snap/| grep "scheduled"'
        )
        sched_snap_list = out.split()
        if sched_type not in ["M", "H"]:
            log.info(
                f"Schedule validation does not exist for {sched_val} type intervals"
            )
            return 0
        sched_verified = 0
        snap_count = 1
        for i in range(len(sched_snap_list)):
            for j in range(i + 1, len(sched_snap_list)):
                if sched_type == "M":
                    if int(sched_num) == abs(
                        int(
                            int(sched_snap_list[i].split("_")[1])
                            - int(sched_snap_list[j].split("_")[1])
                        )
                    ):
                        sched_verified = 1
                        snap_count += 1
                if sched_type == "h":
                    hour_val2 = sched_snap_list[j].split("-")[4].split("_")[0]
                    hour_val1 = sched_snap_list[i].split("-")[4].split("_")[0]
                    if int(sched_num) == abs(int(int(hour_val2) - int(hour_val1))):
                        sched_verified = 1
                        snap_count += 1
        if sched_verified == 0:
            log.info(out)
            log.info(
                f"Snapshots are NOT created as per schedule : schedule_value {sched_val} Snap Count {snap_count}"
            )
            return 1
        log.info(
            f"Snapshots are created as per schedule : schedule_value {sched_val} Snap Count {snap_count}"
        )
        return 0

    def create_snap_retention(self, snap_params):
        """
        Create Snap Retention policy and verify its set.
        Args:
            client: ceph client to run cmd
            snap_params : a dict data type with below key-value pairs,
                Required:
                    path : a path for retention policy to be created upon, type - str
                    retention policy : a policy defining retention of scheduled snaps in path, type - str, for eg., 5M2h
                    fs_name : cephfs volume name
        Returns: 0 : success, 1 : failure
        """
        client = snap_params["client"]
        sched_cmd = f"ceph fs snap-schedule retention add {snap_params['path']} {snap_params['retention']}"
        out, rc = client.exec_command(sudo=True, cmd=sched_cmd)
        log.info(out)
        if snap_params["validate"] is True:
            ret_verify = self.verify_snap_retention(
                client=snap_params["client"],
                sched_path=snap_params["path"],
                ret_val=snap_params["retention"],
            )
            return ret_verify
        return 0

    def verify_snap_retention(self, **kw_args):
        """
        Verify Retention is active and values are as expected
        Args:
            client: ceph client to run cmd
            snap_params : a dict data type with below key-value pairs,
                Required:
                    path : a path for retention policy to be created upon, type - str
                    retention policy : a retention policy to verify, type - str, for eg., 5M2h
                    fs_name : cephfs volume name
        Returns: 0 : success, 1 : failure
        """
        fs_name = kw_args.get("fs_name", "cephfs")
        client = kw_args.get("client")
        cmd = f"ceph fs snap-schedule status {kw_args.get('sched_path')} --fs {fs_name} -f json"
        out, rc = client.exec_command(sudo=True, cmd=cmd)
        sched_status = json.loads(out)
        for sched_item in sched_status:
            if sched_item["path"] == kw_args.get("sched_path") and sched_item.get(
                "retention"
            ):
                for key, value in sched_item["retention"].items():
                    if (f"{key} {value}" in {kw_args.get("ret_val")}) or (
                        f"{value}{key}" in {kw_args.get("ret_val")}
                    ):
                        log.info("Snap Retention is verified.")
                return 0
        log.error(f"Snap retention verification failed : {out}")
        return 1

    def remove_snap_retention(self, client, path, **kw_args):
        """
        To remove a snap-retention for given snap-schedule path
        Args:
        Required:
            client: ceph client to run cmd
            path : a snap-schedule path whose retention needs to be removed, type - str
        Optional:
            ret_val : a retention value that needs to be removed, for eg., "3M" or "M 3"
        Returns: None
        """
        cmd = f"ceph fs snap-schedule retention remove {path}"
        if kw_args.get("ret_val"):
            cmd += f" {kw_args.get('ret_val')}"
        client.exec_command(sudo=True, cmd=cmd)

    def add_snap_sched_data(self, client, io_path, run_time):
        """
        To add ~400M data to client io_path using dd and smallfile IO at an interval of 30secs
        Args:
            io_path: a client mount path where data needs to be added
            run_time: a io duration time in minutes, for eg., 3 for 3mins
            client : client to run IO
        Returns: None
        """
        dir_suffix = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        dir_name = f"snap_io_{dir_suffix}"
        io_path = io_path.strip()
        client.exec_command(sudo=True, cmd=f"mkdir {io_path}{dir_name};")
        end_time = datetime.datetime.now() + datetime.timedelta(minutes=run_time)
        i = 0
        while datetime.datetime.now() < end_time:
            log.info(f"Iteration : {i}")
            # DD IO
            out, rc = client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/random of={io_path}{dir_name}/test_{i}.txt bs=500k "
                "count=100",
            )
            log.info(out)
            small_file_dir_name = f"snap_io_{dir_suffix}_{i}"
            client.exec_command(sudo=True, cmd=f"mkdir {io_path}{small_file_dir_name};")
            # SmallFile IO
            client.exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 "
                f"--file-size 400 --files 100 --files-per-dir 10 --dirs-per-dir 2 --top "
                f"{io_path}{small_file_dir_name}",
                long_running=True,
            )
            i += 1
            time.sleep(30)

    def validate_snap_retention(
        self, client, client_path, sched_path, ret_type="M", fs_name="cephfs"
    ):
        """
        To validate snapshot retentionset on ceph FS path by verifying snapshots created.
        Args:
        Required:
            client: ceph client to run cmd
            client_path : an absolute path in client mount for snap-schedule path, type -str
            sched_path : a snap-schedule path been set, type - str
            Ex:
              client_path : /mnt/cephfs_kernel/ for snap-schedule path "/"
              sched_path : "/"
        Optional:
            ret_type : A retention type need to be validate, default is "M" for minutely
            fs_name : CephFS volume name, default is "cephfs"
        Returns: 0 : success, 1 : failure
        """
        log.info("Validate if scheduled snapshots are retained as per Retention policy")
        out, rc = client.exec_command(
            sudo=True, cmd=f'ls -lrt {client_path}/.snap/| grep "scheduled" | wc -l'
        )
        if not (int(out) > 0):
            raise CommandFailed("It has not created the scheduled snaphot")

        out, rc = client.exec_command(
            sudo=True, cmd=f'ls {client_path}/.snap/| grep "scheduled"'
        )
        sched_snap_list = out.split()
        ret_verified = 0
        if ret_type not in ["M", "h"]:
            log.info(
                f"Retention validation does not exist for {ret_type} type intervals"
            )
            return 0
        cmd = f"ceph fs snap-schedule status {sched_path} --fs {fs_name} -f json"
        out, rc = client.exec_command(sudo=True, cmd=cmd)
        sched_status = json.loads(out)
        for sched_item in sched_status:
            if sched_item["path"] == sched_path and sched_item.get("retention"):
                for key, value in sched_item["retention"].items():
                    if key == ret_type:
                        ret_num = value
                if (
                    sched_item["path"] == sched_path
                    and "M" in sched_item["schedule"]
                    and ret_type == "M"
                ):
                    ret_verified = abs(
                        int(sched_item["created_count"])
                        - int(sched_item["pruned_count"])
                    )
                elif (
                    sched_item["path"] == sched_path
                    and "h" in sched_item["schedule"]
                    and ret_type == "h"
                ):
                    ret_verified = abs(
                        int(sched_item["created_count"])
                        - int(sched_item["pruned_count"])
                    )

        log.info(
            f"Actual Snapshots : {ret_verified}, Expected Snapshots : {ret_num}, Snapshot list : {sched_snap_list}"
        )
        if ret_verified != int(ret_num):
            log.info("Snapshots are NOT retained as per retention policy")
            return 1
        log.info("Snapshots are retained as per retention policy")
        return 0
