import json
import logging
import os
import random
import threading
import time

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

logging_thread = None
mds_logging_thread = None
stop_event = threading.Event()

results = {}


class CephFSSystemUtils(object):
    def __init__(self, ceph_cluster):
        """
        CephFS System test Utility object
        It contains all the re-usable functions related to CephFS system tests
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """
        self.ceph_cluster = ceph_cluster
        self.mons = ceph_cluster.get_ceph_objects("mon")
        self.mgrs = ceph_cluster.get_ceph_objects("mgr")
        self.mdss = ceph_cluster.get_ceph_objects("mds")
        self.osds = ceph_cluster.get_ceph_objects("osd")
        self.clients = ceph_cluster.get_ceph_objects("client")
        self.fs_util = FsUtils(ceph_cluster)

    def get_test_object(self, cephfs_config, req_type="shared"):
        """
        This method is to obtain subvolume object including attributes in below format,
        sv_object = {
        'sv_name' : sv_name,
        'group_name' : group_name,
        'mnt_pt': mnt_pt,
        'mnt_client' : mnt_client,
        'type' : 'shared',
        'fs_name' : fs_name
        }
        """
        sv_objs = []
        for i in cephfs_config:
            if "CLUS_MONITOR" not in i:
                for j in cephfs_config[i]["group"]:
                    sv_info = cephfs_config[i]["group"][j][req_type]
                    for k in sv_info:
                        if k not in ["sv_prefix", "sv_cnt"]:
                            sv_obj = {}
                            sv_obj.update({k: sv_info[k]})
                            sv_obj[k].update({"fs_name": i})
                            if "default" not in j:
                                sv_obj[k].update({"group_name": j})
                            sv_objs.append(sv_obj)

        sv_obj = random.choice(sv_objs)
        if req_type == "unique":
            retry_cnt = 1
            while sv_obj["in_use"] == 1 and retry_cnt < 10:
                sv_obj = random.choice(sv_objs)
                retry_cnt += 1
            if sv_obj["in_use"] == 1:
                return 1
        for i in sv_obj:
            sv_obj[i].update({"fs_util": self.fs_util})

        # log.info(f"SV test object selected : {sv_obj}")
        return sv_obj

    def configure_logger(self, logdir, logname):
        """
        This utility generates new file handler with given logname at given path.
        Required params:
        logdir - Path at which file handler needs to be generated
        logname - Test log file name
        """
        full_log_name = f"{logname}.log"

        LOG_FORMAT = "%(asctime)s (%(name)s) [%(levelname)s] - %(message)s"
        log_format = logging.Formatter(LOG_FORMAT)

        test_logfile = os.path.join(logdir, full_log_name)
        log.info(f"Test logfile: {test_logfile}")

        _handler = logging.FileHandler(test_logfile)
        _handler = logging.handlers.RotatingFileHandler(
            test_logfile,
            maxBytes=10 * 1024 * 1024,  # Set the maximum log file size to 10 MB
            backupCount=20,  # Keep up to 20 old log files which will be 200 MB per test case
        )

        _handler.setFormatter(log_format)
        log1 = logging.Logger(logname)
        log1.addHandler(_handler)
        logdir_list = logdir.split("/", 2)
        magna_url = "http://magna002.ceph.redhat.com//"
        url_base = (
            magna_url + logdir_list[2] if "/ceph/cephci-jenkins" in logdir else logdir
        )
        log1_url = f"{url_base}/{full_log_name}"

        log.info(f"New log {logname} url:{log1_url}")
        return log1

    def get_mds_requests(self, fs_name, client):
        """
        This utility returns Activity/sec from output of ceph fs status.
        It returns max value if Activity/sec seen across MDSes.
        """
        out, rc = client.exec_command(
            cmd=f"ceph fs status {fs_name} -f json", client_exec=True
        )
        parsed_data = json.loads(out)
        mds_reqs = []
        for mds in parsed_data.get("mdsmap"):
            if mds.get("rate"):
                mds_reqs.append(mds["rate"])
        if len(mds_reqs) > 0:
            return max(mds_reqs)
        else:
            return 0

    def crash_setup(self, client, daemon_list=["mds"]):
        """
        Enable crash module, create crash user and copy keyring file to cluster nodes
        """
        cmd = "ceph mgr module enable crash"
        client.exec_command(sudo=True, cmd=cmd)
        daemon_nodes = {
            "mds": self.mdss,
            "mgr": self.mgrs,
            "mon": self.mons,
            "osd": self.osds,
        }
        log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)

        for file_name in ["ceph.conf", "ceph.client.admin.keyring"]:
            dst_path = f"{log_base_dir}/{file_name}"
            src_path = f"/etc/ceph/{file_name}"
            client.download_file(src=src_path, dst=dst_path, sudo=True)
        crash_ready_nodes = []
        for daemon in daemon_list:
            nodes = daemon_nodes[daemon]
            for node in nodes:
                if node.node.hostname not in crash_ready_nodes:
                    cmd = "ls /etc/ceph/ceph.client.crash.keyring"
                    try:
                        node.exec_command(sudo=True, cmd=cmd)
                        crash_ready_nodes.append(node.node.hostname)
                    except BaseException as ex:
                        if "No such file" in str(ex):
                            for file_name in ["ceph.conf", "ceph.client.admin.keyring"]:
                                src_path = f"{log_base_dir}/{file_name}"
                                dst_path = f"/etc/ceph/{file_name}"
                                node.upload_file(src=src_path, dst=dst_path, sudo=True)
                            node.exec_command(
                                sudo=True,
                                cmd="yum install  -y --nogpgcheck ceph-common",
                            )
                            cmd = "ceph auth get-or-create client.crash mon 'profile crash' mgr 'profile crash'"
                            cmd += " > /etc/ceph/ceph.client.crash.keyring"
                            node.exec_command(sudo=True, cmd=cmd)
                            crash_ready_nodes.append(node.node.hostname)
        return 0

    def crash_check(self, client, crash_copy=1, daemon_list=["mds"]):
        """
        Check if Crash dir exists in all daemon hosting nodes, save meta file if crash exists
        """
        daemon_nodes = {
            "mds": self.mdss,
            "mgr": self.mgrs,
            "mon": self.mons,
            "osd": self.osds,
        }

        out, _ = client.exec_command(sudo=True, cmd="ceph fsid")
        fsid = out.strip()
        crash_dir = f"/var/lib/ceph/{fsid}/crash"
        crash_data = {}
        crash_checked_nodes = []
        for daemon in daemon_list:
            nodes = daemon_nodes[daemon]
            for node in nodes:
                if node.node.hostname not in crash_checked_nodes:
                    crash_list = []
                    cmd = f"ls {crash_dir}"
                    out, _ = node.exec_command(sudo=True, cmd=cmd)
                    crash_items = out.split()
                    crash_items.remove("posted")
                    if len(crash_items) > 0:
                        for crash_item in crash_items:
                            crash_path = f"{crash_dir}/{crash_item}"
                            node.exec_command(
                                sudo=True, cmd=f"ceph crash post -i {crash_path}/meta"
                            )
                            crash_list.append(crash_item)
                        crash_data.update({node: crash_list})
                    crash_checked_nodes.append(node.node.hostname)

        log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
        crash_log_path = f"{log_base_dir}/crash_info/"
        try:
            os.mkdir(crash_log_path)
        except BaseException as ex:
            log.info(ex)
        log.info(f"crash_data:{crash_data}")

        if crash_copy == 1:
            for crash_node in crash_data:
                crash_list = crash_data[crash_node]
                node_name = crash_node.node.hostname
                tmp_path = f"{crash_log_path}/{node_name}"
                os.mkdir(tmp_path)
                for crash_item in crash_list:
                    crash_dst_path = f"{crash_log_path}/{node_name}/{crash_item}"
                    os.mkdir(crash_dst_path)
                    crash_path = f"{crash_dir}/{crash_item}"

                    out, _ = crash_node.exec_command(sudo=True, cmd=f"ls {crash_path}")
                    crash_files = out.split()
                    for crash_file in crash_files:
                        src_path = f"{crash_path}/{crash_file}"
                        dst_path = f"{crash_dst_path}/{crash_file}"
                        crash_node.download_file(src=src_path, dst=dst_path, sudo=True)
                    log.info(f"Copied {crash_path} to {crash_dst_path}")
        return 0

    def wait_for_two_active_mds(
        self, client1, fs_name, max_wait_time=180, retry_interval=10
    ):
        """
        Wait until two active MDS (Metadata Servers) are found or the maximum wait time is reached.

        Args:
            data (str): JSON data containing MDS information.
            max_wait_time (int): Maximum wait time in seconds (default: 180 seconds).
            retry_interval (int): Interval between retry attempts in seconds (default: 5 seconds).

        Returns:
            bool: True if two active MDS are found within the specified time, False if not.

        Example usage:
        ```
        data = '...'  # JSON data
        if wait_for_two_active_mds(data):
            print("Two active MDS found.")
        else:
            print("Timeout: Two active MDS not found within the specified time.")
        ```
        """

        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            out, rc = client1.exec_command(
                cmd=f"ceph fs status {fs_name} -f json", client_exec=True
            )
            log.info(out)
            parsed_data = json.loads(out)
            active_mds = [
                mds
                for mds in parsed_data.get("mdsmap", [])
                if mds.get("rank", -1) in [0, 1] and mds.get("state") == "active"
            ]
            if len(active_mds) == 2:
                return True  # Two active MDS found
            else:
                time.sleep(retry_interval)  # Retry after the specified interval

        return False

    def log_rotate_size(self, client, size_str="200M"):
        """
        This mutility will enable log rotation when debug log file size reached the limit mentioned in 'size_str'
        Required_param : size_str, it should be size with units recognised by Ceph Cluster such as,
        200M , 500M, 1G ...
        """
        out, rc = client.exec_command(sudo=True, cmd="ceph fsid -f json")
        fsid_out = json.loads(out)
        fsid = fsid_out["fsid"]
        mds_nodes = self.ceph_cluster.get_ceph_objects("mds")
        mgr_nodes = self.ceph_cluster.get_ceph_objects("mgr")
        osd_nodes = self.ceph_cluster.get_ceph_objects("osd")
        mon_nodes = self.ceph_cluster.get_ceph_objects("mon")
        log_rotate_file = f"/etc/logrotate.d/ceph-{fsid}"
        log_rotate_file_bkp = f"/etc/logrotate.d/ceph-{fsid}.backup"
        log_rotate_tmp = "/home/cephuser/log_rotate_tmp"
        crontab_str = (
            f'"2 * * * * /usr/sbin/logrotate {log_rotate_file} >/dev/null 2>&1"'
        )
        log_complete = []
        for log_node_list in [mds_nodes, mgr_nodes, osd_nodes, mon_nodes]:
            for log_node in log_node_list:
                if log_node.node.hostname not in log_complete:
                    # on each node
                    cmd = f"cp {log_rotate_file} {log_rotate_file_bkp}"
                    out, _ = log_node.exec_command(sudo=True, cmd=cmd)
                    cmd = rf"sed '/compress/i \    \size {size_str}' {log_rotate_file} > {log_rotate_tmp}"
                    out, _ = log_node.exec_command(sudo=True, cmd=cmd)
                    cmd = f"yes | cp {log_rotate_tmp} {log_rotate_file}"
                    out, _ = log_node.exec_command(sudo=True, cmd=cmd)
                    cmd = f"echo {crontab_str} > /home/cephuser/log_cron_file"
                    out, _ = log_node.exec_command(sudo=True, cmd=cmd)
                    cmd = "crontab /home/cephuser/log_cron_file"
                    out, _ = log_node.exec_command(sudo=True, cmd=cmd)
                    log_complete.append(log_node.node.hostname)
        return 0

    def log_parser(self, client, expect_list, unexpect_list, daemon="mds"):
        """
        This utility parsers through daemon debug logs mentioned in daemon_list and checks for
        expected and unexpected strings in logs.
        If expected strings found and unexpected strings not found, return pass as 0
        If unexpected strrings found and expected strings not found, return fail as 1
        Example usage:
        expect_list = ['issue_new_caps','get_allowed_caps','sending MClientCaps','client_caps(revoke']
        unexpect_list = ['Exception','assert']
        log_parser(expect_list,unexpect_list)
        """
        out, rc = client.exec_command(sudo=True, cmd="ceph fsid -f json")
        fsid_out = json.loads(out)
        fsid = fsid_out["fsid"]
        daemon_nodes = self.ceph_cluster.get_ceph_objects(daemon)
        log_path = f"/var/log/ceph/{fsid}"
        results = {"expect": {}, "unexpect": {}}
        for node in daemon_nodes:
            for search_str in expect_list:
                cmd = f"grep {search_str} {log_path}/*{daemon}*"
                try:
                    out = node.exec_command(sudo=True, cmd=cmd)
                    if len(out) > 0:
                        log.info(
                            f"Found {search_str} in {daemon} log in {log_path} on {node.node.hostname}:\n {out}"
                        )
                        results["expect"].update({search_str: node})
                except BaseException as ex:
                    log.info(ex)
            for search_str in unexpect_list:
                cmd = f"grep {search_str} {log_path}/*{daemon}*"
                try:
                    out, _ = node.exec_command(sudo=True, cmd=cmd)
                    if len(out) > 0:
                        log.error(
                            f"Found {search_str} in {daemon} log in {log_path} on {node.node.hostname}:\n {out}"
                        )
                        results["unexpect"].update({search_str: node})
                except BaseException as ex:
                    log.info(ex)
        expect_not_found = []
        unexpect_found = []
        for exp_str in expect_list:
            if exp_str not in results["expect"]:
                expect_not_found.append(exp_str)
        for unexp_str in unexpect_list:
            if unexp_str in results["expect"]:
                unexpect_found.append(unexp_str)
        test_status = 0
        if len(expect_not_found):
            log.error(
                f"Some of expected strings not found in debug logs for daemon {daemon}:{expect_not_found}"
            )
            test_status = 1
        if len(unexpect_found):
            log.error(
                f"Some of unexpected strings found in debug logs for daemon {daemon}:{unexpect_found}"
            )
            test_status = 1
        return test_status
