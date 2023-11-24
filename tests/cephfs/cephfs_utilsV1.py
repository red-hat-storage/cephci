"""
This is cephfs utility module
It contains all the re-useable functions related to cephfs
It installs all the pre-requisites on client nodes

"""
import argparse
import datetime
import itertools
import json
import os
import random
import re
import string
import time
from time import sleep

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from mita.v2 import get_openstack_driver
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def function_execution_time(func):
    """
    This decorator can be used on any function to collect time take for completion of the function
    """

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        with open(
            f"{log.logger.handlers[0].baseFilename}_execution_times.txt", "a"
        ) as file:
            file.write(
                f"Execution time for {func.__name__} and arrguments - {args} - {kwargs} : {execution_time} seconds\n"
            )
        log.info(f"Execution time for {func.__name__}: {execution_time} seconds")
        return result

    return wrapper


class FsUtils(object):
    def __init__(self, ceph_cluster):
        """
        FS Utility object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """
        self.result_vals = {}
        self.return_counts = {}
        self.ceph_cluster = ceph_cluster
        self.mons = ceph_cluster.get_ceph_objects("mon")
        self.mgrs = ceph_cluster.get_ceph_objects("mgr")
        self.osds = ceph_cluster.get_ceph_objects("osd")
        self.mdss = ceph_cluster.get_ceph_objects("mds")

    def prepare_clients(self, clients, build):
        """
        Installs all the required rpms and clones the tools required for running Tests on clients
        Args:
            clients:
            build:
        Returns:

        """
        for client in clients:
            pkgs = [
                "python3",
                "python3-pip",
                "fio",
                "fuse",
                "ceph-fuse",
                "attr",
                "gcc",
                "python3-devel",
                "git",
            ]
            if build.endswith("7") or build.startswith("3"):
                pkgs.extend(
                    [
                        "@development",
                        "rh-python36",
                        "rh-python36-numpy rh-python36-scipy",
                        "rh-test-python-tools rh-python36-python-six",
                        "libffi libffi-devel",
                    ]
                )
            cmd = "yum install -y --nogpgcheck " + " ".join(pkgs)
            client.node.exec_command(sudo=True, cmd=cmd, long_running=True)
            client.node.exec_command(
                sudo=True, cmd="pip3 install xattr numpy scipy", long_running=True
            )
            out, rc = client.node.exec_command(sudo=True, cmd="ls /home/cephuser")
            if "smallfile" not in out:
                client.node.exec_command(
                    cmd="git clone https://github.com/bengland2/smallfile.git"
                )

    @staticmethod
    def nfs_ganesha_install(ceph_demon):
        if ceph_demon.pkg_type == "rpm":
            ceph_demon.exec_command(
                sudo=True, cmd="yum install --nogpgcheck nfs-ganesha-ceph -y"
            )
            ceph_demon.exec_command(sudo=True, cmd="systemctl start rpcbind")
            ceph_demon.exec_command(sudo=True, cmd="systemctl stop nfs-server.service")
            ceph_demon.exec_command(
                sudo=True, cmd="systemctl disable nfs-server.service"
            )
            assert ceph_demon.node.exit_status == 0
        return 0

    @staticmethod
    def get_fs_info(client, fs_name="cephfs"):
        """
        Gets the fs info for the given filesystem.
        if fs_name not given.it fetches the default cephfs info
        Args:
            client:
            fs_name:

        Returns:
            dictonary with fs_name, metadata_pool,data_pool
        """
        out, rc = client.exec_command(sudo=True, cmd="ceph fs ls --format json-pretty")
        all_fs_info = json.loads(out)
        output_dict = {}
        for fs in all_fs_info:
            if fs_name == fs["name"]:
                output_dict["fs_name"] = fs["name"]
                output_dict["metadata_pool_name"] = fs["metadata_pool"]
                output_dict["data_pool_name"] = fs["data_pools"][0]
        return output_dict

    def enable_mds_logs(self, client, fs_name="cephfs", validate=True):
        """

        Args:
            fs_name:

        Returns:

        """
        out, rc = client.exec_command(sudo=True, cmd="ceph orch ps -f json")
        output = json.loads(out)
        deamon_dict = self.filter_daemons(output, "mds", fs_name)
        set_log_dict = {"log_to_file": "true", "debug_mds": "5", "debug_ms": "1"}
        for mds_nodes in deamon_dict:
            for hostname, deamon in mds_nodes.items():
                node = self.ceph_cluster.get_node_by_hostname(hostname)
                for k, v in set_log_dict.items():
                    node.exec_command(
                        sudo=True,
                        cmd=f"cephadm shell -- ceph daemon {deamon} config set {k} {v}",
                    )
            if validate:
                for k, v in set_log_dict.items():
                    out, rc = node.exec_command(
                        sudo=True,
                        cmd=f"cephadm shell -- ceph daemon {deamon} config get {k}",
                    )
                    if v not in out:
                        log.error("Unable to set the debug logs")
                        raise CommandFailed(f"Unable to set the debug logs : {out}")

    def disable_mds_logs(self, client, fs_name="cephfs", validate=True):
        """

        Args:
            fs_name:

        Returns:

        """
        out, rc = client.exec_command(sudo=True, cmd="ceph orch ps -f json")
        output = json.loads(out)
        deamon_dict = self.filter_daemons(output, "mds", fs_name)
        for mds_nodes in deamon_dict:
            for hostname, deamon in mds_nodes.items():
                node = self.ceph_cluster.get_node_by_hostname(hostname)
                set_log_dict = {
                    "log_to_file": "false",
                    "debug_mds": "1",
                    "debug_ms": "0",
                }
                for k, v in set_log_dict.items():
                    node.exec_command(
                        sudo=True,
                        cmd=f"cephadm shell -- ceph daemon {deamon} config set {k} {v}",
                    )
                if validate:
                    for k, v in set_log_dict.items():
                        out, rc = node.exec_command(
                            sudo=True,
                            cmd=f"cephadm shell -- ceph daemon {deamon} config get {k}",
                        )
                        if v not in out:
                            log.error("Unable to set the debug logs")
                            raise CommandFailed(f"Unable to set the debug logs : {out}")

    def filter_daemons(self, daemon_list, daemon_type, daemon_id_prefix):
        filtered_list = []

        for daemon in daemon_list:
            if daemon["daemon_type"] == daemon_type and daemon["daemon_id"].startswith(
                daemon_id_prefix
            ):
                filtered_list.append(
                    {
                        daemon["hostname"]: daemon["daemon_name"],
                    }
                )

        return filtered_list

    @staticmethod
    def deamon_op(node, service, op, **kwargs):
        """
        It performs given operation on service given
        Args:
            node:
            service:
            op:  start,stop,restart

        Returns:

        """
        if kwargs.get("service_name"):
            node.exec_command(
                sudo=True, cmd=f"systemctl {op} {kwargs.get('service_name')}"
            )
        else:
            service_deamon = FsUtils.deamon_name(node, service)
            node.exec_command(sudo=True, cmd=f"systemctl {op} {service_deamon}")

    @staticmethod
    def deamon_name(node, service):
        out, rc = node.exec_command(
            sudo=True,
            cmd=f"systemctl list-units --type=service | grep {service} | awk {{'print $1'}}",
        )
        service_deamon = out.strip().split()[0]
        return service_deamon

    @staticmethod
    @retry(CommandFailed, tries=5, delay=60)
    def check_deamon_status(node, service, expected_status):
        """
        Checks the deamon status.
        This retries till
        Args:
            node:
            service:
            expected_status:

        Returns:

        """
        out, rc = node.exec_command(
            sudo=True,
            cmd=f"systemctl list-units --type=service | grep {service}.ceph | awk {{'print $1'}}",
        )
        service_deamon = out.strip()
        node.exec_command(
            sudo=True, cmd=f"systemctl status {service_deamon} | grep {expected_status}"
        )

    @staticmethod
    def get_active_mdss(client, fs_name="cephfs"):
        """
        Get Active MDS service names
        Args:
            client:
            fs_name:

        Returns:

        """
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph fs status {fs_name} --format json"
        )
        output = json.loads(out)
        active_mds = [
            mds["name"] for mds in output["mdsmap"] if mds["state"] == "active"
        ]
        return active_mds

    @staticmethod
    def get_standby_replay_mdss(client, fs_name="cephfs"):
        """
        Get standby-replay MDS service names
        Args:
            client:
            fs_name:

        Returns:
            List of standby-replay mds daemon list

        """
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph fs status {fs_name} --format json"
        )
        output = json.loads(out)
        standby_reply_mds = [
            mds["name"] for mds in output["mdsmap"] if mds["state"] == "standby-replay"
        ]
        return standby_reply_mds

    def get_mds_info(self, active_mds_node_1, active_mds_node_2, **kwargs):
        """
        Collects info from mds nodes by executing on respective mds nodes cephadm shell
        Args:
            active_mds_node_1:
            active_mds_node_2:
            **kwargs:

        Returns:

        """
        active_mds_node_obj_1 = self.ceph_cluster.get_node_by_hostname(
            active_mds_node_1.split(".")[1]
        )
        active_mds_node_obj_2 = self.ceph_cluster.get_node_by_hostname(
            active_mds_node_2.split(".")[1]
        )
        for key, val in list(kwargs.items()):
            if val == "get subtrees":
                out_1, err_1 = active_mds_node_obj_1.exec_command(
                    sudo=True,
                    cmd="cephadm shell -- ceph --admin-daemon /var/run/ceph/ceph-mds.%s."
                    "asok %s | grep path" % (active_mds_node_1, val),
                )
                out_2, err_2 = active_mds_node_obj_2.exec_command(
                    sudo=True,
                    cmd="cephadm shell -- ceph --admin-daemon /var/run/ceph/ceph-mds.%s."
                    "asok %s| grep path" % (active_mds_node_2, val),
                )
                return (
                    out_1.rstrip("\n"),
                    out_2.rstrip("\n"),
                    0,
                )

            elif val == "session ls":
                out_1, err_1 = active_mds_node_obj_1.exec_command(
                    sudo=True,
                    cmd="cephadm shell -- ceph --admin-daemon /var/run/ceph/ceph-mds.%s."
                    "asok %s" % (active_mds_node_1, val),
                )
                out_2, err_2 = active_mds_node_obj_2.exec_command(
                    sudo=True,
                    cmd="cephadm shell -- ceph --admin-daemon /var/run/ceph/ceph-mds.%s."
                    "asok %s" % (active_mds_node_2, val),
                )
                return (
                    out_1.rstrip("\n"),
                    out_2.rstrip("\n"),
                    0,
                )

    @staticmethod
    def auto_evict(client_node, clients, rank):
        """
        Evict Client from the ceph cluster
        Args:
            client_node:
            clients:
            rank:

        Returns:

        """
        grep_pid_cmd = """sudo ceph tell mds.%d client ls | grep '"pid":'"""
        out, rc = client_node.exec_command(cmd=grep_pid_cmd % rank)
        client_pid = re.findall(r"\d+", out)
        successful_clients = 0
        while True:
            for client in clients:
                successful_clients += 1
                try:
                    for pid in client_pid:
                        client.exec_command(
                            sudo=True, cmd="kill -9 %s" % pid, container_exec=False
                        )
                        return 0
                except Exception as e:
                    print(e)
                    pass
            if successful_clients == len(clients):
                raise CommandFailed(
                    f"Not able to find the PID {client_pid} in any of the clients"
                )

    @staticmethod
    def manual_evict(client_node, rank):
        """
        Manual evict Client from the ceph cluster
        Args:
            client_node:
            rank:

        Returns:

        """
        grep_cmd = """ sudo ceph tell mds.%d client ls | grep '"id":'"""
        out, rc = client_node.exec_command(cmd=grep_cmd % rank)
        client_ids = re.findall(r"\d+", out)
        grep_cmd = """ sudo ceph tell mds.%d client ls | grep '"inst":'"""
        log.info("Getting IP address of Evicted client")
        out, rc = client_node.exec_command(cmd=grep_cmd % rank)
        op = re.findall(r"\d+.+\d+.", out)
        ip_add = op[0]
        ip_add = ip_add.split(" ")
        ip_add = ip_add[1].strip('",')
        id_cmd = "sudo ceph tell mds.%d client evict id=%s"
        for client_id in client_ids:
            client_node.exec_command(cmd=id_cmd % (rank, client_id))
            break

        return ip_add

    @staticmethod
    def osd_blacklist_rm_client(client_node, ip_add):
        """
        Blacklist client ip from OSD
        Args:
            client_node:
            ip_add:

        Returns:

        """
        out, rc = client_node.exec_command(sudo=True, cmd="ceph osd blacklist ls")
        if ip_add in out:
            client_node.exec_command(sudo=True, cmd="ceph osd blacklist rm %s" % ip_add)
            if "listed 0 entries" in out:
                log.info("Evicted client %s unblacklisted successfully" % ip_add)
        return 0

    def config_blacklist_auto_evict(self, active_mds, rank, service_name, **kwargs):
        """
        Configure black list on osd and evict client
        Args:
            active_mds:
            rank:
            service_name:
            **kwargs:

        Returns:

        """
        if kwargs:
            active_mds.exec_command(
                sudo=True,
                cmd="cephadm shell -- ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok"
                " config set mds_session_blacklist_on_timeout true" % service_name,
            )
            return 0
        else:
            active_mds.exec_command(
                sudo=True,
                cmd="cephadm shell -- ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok "
                "config set mds_session_blacklist_on_timeout false" % service_name,
            )
            clients = self.ceph_cluster.get_ceph_objects("client")
            self.auto_evict(clients[0], clients[0:2], rank)
            log.info("Waiting 300 seconds for auto eviction---")
            sleep(300)
            return 0

    def config_blacklist_manual_evict(self, active_mds, rank, service_name, **kwargs):
        """
        Configure black list on osd and manually evict client
        Args:
            active_mds:
            rank:
            service_name:
            **kwargs:

        Returns:

        """
        if kwargs:
            active_mds.exec_command(
                sudo=True,
                cmd="cephadm shell -- ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok"
                " config set mds_session_blacklist_on_evict true" % service_name,
            )
            return 0
        else:
            active_mds.exec_command(
                sudo=True,
                cmd="cephadm shell -- ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok "
                "config set mds_session_blacklist_on_evict false" % service_name,
            )
            clients = self.ceph_cluster.get_ceph_objects("client")
            ip_add = self.manual_evict(clients[0], rank)
            out, rc = clients[0].exec_command(sudo=True, cmd="ceph osd blacklist ls")
            print(out)
            log.info(f"{ip_add} which is evicated manually")
            if ip_add not in out:
                return 0

    def validate_fs_info(self, client, fs_name="cephfs"):
        """
        Validates fs info command.
        ceph fs volume info
        if fs_name not given.it fetches the default cephfs info
        Checks the mon Ips based on Cluster configuration in CI
        Args:
            client:
            fs_name:
        """
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph fs volume info {fs_name} --format json"
        )
        fs_info = json.loads(out)
        mon_ips = self.get_mon_node_ips()
        if not set([f"{i}:6789" for i in mon_ips]).issubset(fs_info["mon_addrs"]):
            log.error("Mon IPs are not matching with FS Info IPs")
            return False
        return True

    def get_fs_details(self, client, **kwargs):
        """
        Gets all filesystems information
        Args:
            client:
        Returns:
            json object with all fs
        """
        fs_details_cmd = "ceph fs ls --format json"
        if kwargs.get("extra_params"):
            fs_details_cmd += f"{kwargs.get('extra_params')}"
        out, rc = client.exec_command(sudo=True, cmd=fs_details_cmd)
        all_fs_info = json.loads(out)
        return all_fs_info

    def auth_list(self, clients, **kwargs):
        """
        Creates ceph.client.<hostname>.keyring file for the given clients
        Args:
            clients:
            **kwargs:

        Returns:

        """
        fs_info = self.get_fs_info(clients[0])
        for client in clients:
            log.info("Giving required permissions for clients:")
            client.exec_command(
                sudo=True,
                cmd=f"ceph auth get client.{client.node.hostname}",
                check_ec=False,
            )
            if client.node.exit_status == 0:
                client.exec_command(
                    sudo=True, cmd=f"ceph auth del client.{client.node.hostname}"
                )
            if kwargs.get("mds"):
                client.exec_command(
                    sudo=True,
                    cmd="ceph auth get-or-create client.%s_%s"
                    " mon 'allow r' mds "
                    "'allow %s path=/%s' osd 'allow "
                    "rw pool=%s'"
                    " -o /etc/ceph/ceph.client.%s_%s.keyring"
                    % (
                        client.node.hostname,
                        kwargs.get("path"),
                        kwargs.get("permission"),
                        kwargs.get("path"),
                        fs_info.get("data_pool_name"),
                        client.node.hostname,
                        kwargs.get("path"),
                    ),
                )
                client.exec_command(
                    sudo=True,
                    cmd=f"chmod 644 /etc/ceph/ceph.client.{client.node.hostname}_{kwargs.get('path')}.keyring",
                )
            elif kwargs.get("osd"):
                client.exec_command(
                    sudo=True,
                    cmd="ceph auth get-or-create client.%s_%s"
                    " mon 'allow r' mds "
                    "'allow r, allow rw  path=/' osd 'allow "
                    "%s pool=%s'"
                    " -o /etc/ceph/ceph.client.%s_%s.keyring"
                    % (
                        client.node.hostname,
                        kwargs.get("path"),
                        kwargs.get("permission"),
                        fs_info.get("data_pool_name"),
                        client.node.hostname,
                        kwargs.get("path"),
                    ),
                )
                client.exec_command(
                    sudo=True,
                    cmd=f"chmod 644 /etc/ceph/ceph.client.{client.node.hostname}_{kwargs.get('path')}.keyring",
                )

            elif kwargs.get("layout_quota"):
                p_flag = kwargs.get("layout_quota")
                client.exec_command(
                    sudo=True,
                    cmd="ceph auth get-or-create client.%s_%s"
                    " mon 'allow r' mds "
                    "'allow %s' osd 'allow rw"
                    " tag cephfs data=cephfs'"
                    " -o /etc/ceph/ceph.client.%s_%s.keyring"
                    % (
                        client.node.hostname,
                        kwargs.get("path"),
                        p_flag,
                        client.node.hostname,
                        kwargs.get("path"),
                    ),
                )
                client.exec_command(
                    sudo=True,
                    cmd=f"chmod 644 /etc/ceph/ceph.client.{client.node.hostname}_{kwargs.get('path')}.keyring",
                )
            else:
                client.exec_command(
                    sudo=True,
                    cmd=f"ceph auth get-or-create client.{client.node.hostname}"
                    f" mon 'allow *' mds "
                    f"'allow *, allow * path=/' osd 'allow *'"
                    f" -o /etc/ceph/ceph.client.{client.node.hostname}.keyring",
                )
                client.exec_command(
                    sudo=True,
                    cmd=f"chmod 644 /etc/ceph/ceph.client.{client.node.hostname}.keyring",
                )
        return 0

    @retry(CommandFailed, tries=3, delay=60)
    def fuse_mount(self, fuse_clients, mount_point, **kwargs):
        """
        Mounts the drive using Fuse mount
        Args:
            fuse_clients:
            mount_point:
            **kwargs:
                extra_params : we can include extra parameters that needs to be passed to ceph fs fuse mount
        Returns:

        Exceptions:
            assertion error will occur if the device is not mounted
        """
        for client in fuse_clients:
            log.info("Creating mounting dir:")
            client.exec_command(sudo=True, cmd="mkdir -p %s" % mount_point)
            log.info("Mounting fs with ceph-fuse on client %s:" % client.node.hostname)
            if kwargs.get("new_client_hostname"):
                client.exec_command(
                    sudo=True,
                    cmd=f"ceph auth get "
                    f"client.{kwargs.get('new_client_hostname')} "
                    f"-o /etc/ceph/ceph.client"
                    f".{kwargs.get('new_client_hostname')}.keyring",
                )
            fuse_cmd = f"ceph-fuse -n client.{kwargs.get('new_client_hostname', client.node.hostname)} {mount_point} "
            if kwargs.get("extra_params"):
                fuse_cmd += f"{kwargs.get('extra_params')}"
            if kwargs.get("wait_for_mount"):
                if not self.wait_for_cmd_to_succeed(
                    client, cmd=fuse_cmd, timeout=300, interval=60
                ):
                    raise CommandFailed("Failed to mount Filesystem even after 5 min")
            else:
                client.exec_command(sudo=True, cmd=fuse_cmd, long_running=True)
            if not self.wait_until_mount_succeeds(client, mount_point):
                raise CommandFailed("Failed to appear in mount cmd even after 5 min")
            if kwargs.get("fstab"):
                mon_node_ips = self.get_mon_node_ips()
                mon_node_ip = ",".join(mon_node_ips)
                try:
                    client.exec_command(sudo=True, cmd="ls -lrt /etc/fstab.backup")
                except CommandFailed:
                    client.exec_command(
                        sudo=True, cmd="cp /etc/fstab /etc/fstab.backup"
                    )
                fstab = client.remote_file(
                    sudo=True, file_name="/etc/fstab", file_mode="a+"
                )
                fstab_entry = (
                    f"{mon_node_ip}    {mount_point}    fuse.ceph    "
                    f"ceph.name=client.{kwargs.get('new_client_hostname', client.node.hostname)},"
                )
                if kwargs.get("extra_params"):
                    arguments = self.convert_string_args(kwargs.get("extra_params"))
                    if arguments.client_fs:
                        fstab_entry += f"ceph.client_fs={arguments.client_fs},"
                    if arguments.r:
                        fstab_entry += f"ceph.client_mountpoint={arguments.r},"
                    if arguments.id:
                        fstab_entry += f"ceph.id={arguments.id},"
                fstab_entry += "_netdev,defaults      0       0"
                fstab.write(fstab_entry + "\n")
                fstab.flush()
                fstab.close()

    def convert_string_args(self, str_args):
        """
        This is support fucntion for adding fstab entry for fuse mounts.
        it takes extra params given to fuse mount command and
        returns it as Namespace where we can access then as variable
        """
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--client_fs")
        parser.add_argument("-r")
        parser.add_argument("--id")
        args = parser.parse_args(str_args.split())
        return args

    def wait_for_nfs_process(
        self,
        client,
        process_name,
        timeout=180,
        interval=5,
        ispresent=True,
        desired_state="running",
    ):
        """
        Checks for the proccess and returns the status based on ispresent
        :param client:
        :param process_name:
        :param timeout:
        :param interval:
        :param ispresent:
        :return:
        """
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        log.info("Wait for the process to start or stop")
        while end_time > datetime.datetime.now():
            out, rc = client.exec_command(
                sudo=True,
                cmd="ceph orch ps --daemon_type=nfs --format json",
                check_ec=False,
            )
            nfs_hosts = json.loads(out.read().decode())
            for nfs in nfs_hosts:
                log.info(nfs)
                if process_name in nfs["daemon_id"] and ispresent:
                    if nfs["status_desc"] == desired_state:
                        log.info(nfs)
                        return True
                if process_name not in nfs["daemon_id"] and not ispresent:
                    return True
            sleep(interval)
        return False

    def wait_for_mds_deamon(
        self,
        client,
        process_name,
        timeout=180,
        interval=5,
        ispresent=True,
        host="host_name",
        desired_state="running",
    ):
        """
        Checks for the mds deamon and returns the status based on ispresent
        :param client:
        :param process_name:
        :param timeout:
        :param interval:
        :param ispresent:
        :return:
        """
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        log.info("Wait for the process to start or stop")
        while end_time > datetime.datetime.now():
            out, rc = client.exec_command(
                sudo=True,
                cmd=f"ceph orch ps --hostname {host} --daemon_type=mds --format json",
                check_ec=False,
            )
            mds_hosts = json.loads(out)
            if ispresent:
                for mds in mds_hosts:
                    if process_name in mds["daemon_id"]:
                        if mds["status_desc"] == desired_state:
                            return True
            else:
                if not any(process_name in mds["daemon_id"] for mds in mds_hosts):
                    return True
            sleep(interval)
        return False

    def wait_for_mds_process(
        self,
        client,
        process_name,
        timeout=180,
        interval=5,
        ispresent=True,
        desired_state="running",
    ):
        """
        Checks for the proccess and returns the status based on ispresent
        :param client:
        :param process_name:
        :param timeout:
        :param interval:
        :param ispresent:
        :return:
        """
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        log.info("Wait for the process to start or stop")
        while end_time > datetime.datetime.now():
            out, rc = client.exec_command(
                sudo=True,
                cmd="ceph orch ps --daemon_type=mds --format json",
                check_ec=False,
            )
            mds_hosts = json.loads(out)
            for mds in mds_hosts:
                log.info(mds)
                if process_name in mds["daemon_id"] and ispresent:
                    if mds["status_desc"] == desired_state:
                        log.info(mds)
                        return True
                if process_name not in mds["daemon_id"] and not ispresent:
                    return True
            sleep(interval)
        return False

    def wait_until_mount_succeeds(self, client, mount_point, timeout=180, interval=5):
        """
        Checks for the mount point and returns the status based on mount command
        :param client:
        :param mount_point:
        :param timeout:
        :param interval:
        :return: boolean
        """
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        log.info("Wait for the mount to appear")
        while end_time > datetime.datetime.now():
            out, rc = client.exec_command(sudo=True, cmd="mount", check_ec=False)
            log.info(out)
            mount_output = out.rstrip("\n").split()
            log.info("Validate Fuse Mount:")
            if mount_point.rstrip("/") in mount_output:
                return True
            sleep(interval)
        return False

    def kernel_mount(self, kernel_clients, mount_point, mon_node_ip, **kwargs):
        """
        Mounts the drive using kernel mount
        Args:
            kernel_clients:
            mount_point:
            mon_node_ip:
            **kwargs:
                extra_params : we can include extra parameters that needs to be passed to ceph fs fuse mount
                sub_dir: if you want to mount specific directory
        Returns:

        Exceptions:
            assertion error will occur if the device is not mounted
        """
        for client in kernel_clients:
            log.info("Creating mounting dir:")
            client.exec_command(sudo=True, cmd="mkdir -p %s" % mount_point)
            client.exec_command(
                sudo=True,
                cmd=f"ceph auth get-key client.{kwargs.get('new_client_hostname', client.node.hostname)} -o "
                f"/etc/ceph/{kwargs.get('new_client_hostname', client.node.hostname)}.secret",
            )

            kernel_cmd = (
                f"mount -t ceph {mon_node_ip}:{kwargs.get('sub_dir','/')} {mount_point} "
                f"-o name={kwargs.get('new_client_hostname', client.node.hostname)},"
                f"secretfile=/etc/ceph/{kwargs.get('new_client_hostname', client.node.hostname)}.secret"
            )

            if kwargs.get("extra_params"):
                kernel_cmd += f"{kwargs.get('extra_params')}"
            cmd_rc = client.exec_command(
                sudo=True,
                cmd=kernel_cmd,
                long_running=True,
            )
            log.info("validate kernel mount:")
            if kwargs.get("validate", True):
                if not self.wait_until_mount_succeeds(client, mount_point):
                    raise CommandFailed(
                        "Failed to appear in mount cmd even after 5 min"
                    )
            if kwargs.get("fstab"):
                try:
                    client.exec_command(sudo=True, cmd="ls -lrt /etc/fstab.backup")
                except CommandFailed:
                    client.exec_command(
                        sudo=True, cmd="cp /etc/fstab /etc/fstab.backup"
                    )
                fstab = client.remote_file(
                    sudo=True, file_name="/etc/fstab", file_mode="a+"
                )
                fstab_entry = (
                    f"{mon_node_ip}:{kwargs.get('sub_dir', '/')}    {mount_point}    ceph    "
                    f"name={kwargs.get('new_client_hostname', client.node.hostname)},"
                    f"secretfile=/etc/ceph/{kwargs.get('new_client_hostname', client.node.hostname)}.secret"
                )
                if kwargs.get("extra_params"):
                    fstab_entry += f"{kwargs.get('extra_params')}"
                fstab_entry += ",_netdev,noatime      0       0"
                print(dir(fstab))
                fstab.write(fstab_entry + "\n")
                fstab.flush()
                fstab.close()
        return cmd_rc

    def get_mon_node_ips(self):
        """
        Returns:
            All the mon ips as a list
        """
        return [node.ip_address for node in self.ceph_cluster.get_nodes(role="mon")]

    # @staticmethod

    @staticmethod
    def client_clean_up(
        *args, fuse_clients=[], kernel_clients=[], mounting_dir="", **kwargs
    ):
        """
        This method cleans up all the client nodes and mount points
        Args:
            fuse_clients:
            kernel_clients:
            mounting_dir:
            *args:
                umount : if this argument is passed this will unmounts all the devices
            **kwargs:

        Returns:
            0 if all the clean up is passed
        """
        clients = fuse_clients + kernel_clients
        for client in clients:
            log.info("Removing files:")
            client.exec_command(
                sudo=True,
                cmd=f"rm -rf {mounting_dir}*",
                long_running=True,
                timeout=3600,
            )

            if "umount" in args:
                if client in fuse_clients:
                    log.info("Unmounting fuse client:")
                    cmd = f"fusermount -u {mounting_dir} -z"
                else:
                    log.info("Unmounting Kernel client:")
                    cmd = f"umount {mounting_dir} -l"
                client.exec_command(sudo=True, cmd=cmd)
                log.info("Removing mounting directory:")
                client.exec_command(sudo=True, cmd=f"rmdir {mounting_dir}")
                log.info("Removing keyring file:")
                client.exec_command(
                    sudo=True,
                    cmd=f"rm -rf /etc/ceph/ceph.client.{kwargs.get('client_name', client.node.hostname)}.keyring",
                )
                log.info("Removing permissions:")
                client.exec_command(
                    sudo=True,
                    cmd=f"ceph auth del client.{kwargs.get('client_name', client.node.hostname)}",
                )
                client.exec_command(
                    cmd="find /home/cephuser -type f -not -name 'authorized_keys' "
                    " -name 'Crefi' -name 'smallfile' -delete",
                    long_running=True,
                    timeout=3600,
                )
                client.exec_command(
                    cmd="cd /home/cephuser && ls -a | grep -v 'authorized_keys' |"
                    "xargs sudo rm -f",
                    long_running=True,
                    timeout=3600,
                )
                client.exec_command(sudo=True, cmd="iptables -F", check_ec=False)

        return 0

    def get_all_subvolumes(self, client, fs_list):
        """
        it gets all the subvolume details in fs provided
        if fs_list is empty it will iterate through all the fs present and gets back the list
        Args:
            client:
            fs_list:

        Returns:
            list of all sub volumes present in the group
        """
        if not fs_list:
            out, rc = client.exec_command(sudo=True, cmd="ceph fs ls --format json")
            all_fs_info = json.loads(out)
            fs_list = [i["name"] for i in all_fs_info]
        subvolumes = []
        for fs in fs_list:
            out, rc = client.exec_command(
                sudo=True, cmd=f"ceph fs subvolume ls {fs} --format json"
            )
            all_sub_info = json.loads(out)
            subvolumes.extend([i["name"] for i in all_sub_info])
        return subvolumes

    @function_execution_time
    def create_nfs(self, client, nfs_cluster_name, validate=True, **kwargs):
        """
        Create an NFS cluster with the given parameters.

        Args:
            client (str): The client name or identifier.
            nfs_cluster_name (str): The name of the NFS cluster.
            validate (bool): Flag indicating whether to validate the cluster creation.
            **kwargs: Additional keyword arguments for customization.

        Returns:
            tuple: A tuple containing the Ceph command output and command return code.
        """
        nfs_cmd = f"ceph nfs cluster create {nfs_cluster_name}"
        if kwargs.get("placement"):
            nfs_cmd += f" --placement '{kwargs.get('placement')}'"

        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=nfs_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            out, rc = client.exec_command(sudo=True, cmd="ceph nfs cluster ls")
            nfscluster_ls = out.split("\n")
            if nfs_cluster_name not in nfscluster_ls:
                raise CommandFailed(
                    f"Creation of NFS cluster: {nfs_cluster_name} failed"
                )
        return cmd_out, cmd_rc

    @function_execution_time
    @retry(CommandFailed, tries=5, delay=60)
    def remove_nfs_cluster(self, client, nfs_cluster_name, validate=True, **kwargs):
        """
        Remove an NFS cluster and retry for 5 times with a delay of 60 seconds.
        Args:
            client:
            nfs_cluster_name:
            validate:
            **kwargs:

        Returns:
            cmd_out, cmd_rc
        """

        rmnfscluster_cmd = f"ceph nfs cluster rm {nfs_cluster_name}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=rmnfscluster_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            listnfscluster_cmd = "ceph nfs cluster ls"
            out, rc = client.exec_command(sudo=True, cmd=f"{listnfscluster_cmd}")
            nfscluster_ls = out.split("\n")
            if nfs_cluster_name in nfscluster_ls:
                raise CommandFailed(f"Deletion of nfs cluster : {nfscluster_ls} failed")
        return cmd_out, cmd_rc

    @function_execution_time
    def create_nfs_export(
        self, client, nfs_cluster_name, binding, fs_name, validate=True, **kwargs
    ):
        """
        Create an NFS export for a specific file system within an NFS cluster.

        Args:
            client (str): The name or identifier of the client.
            nfs_cluster_name (str): The name of the NFS cluster.
            binding (str): The binding information for the NFS export.
            fs_name (str): The name of the file system to be exported.
            validate (bool): Flag indicating whether to validate the export creation.
            **kwargs: Additional keyword arguments for customization.

        Returns:
            tuple: A tuple containing the output of the command and the command's return code.
        """
        export_nfs_cmd = (
            f"ceph nfs export create cephfs {nfs_cluster_name} {binding} {fs_name}"
        )
        if kwargs.get("path"):
            export_nfs_cmd += f" --path={kwargs.get('path')} "
        if kwargs.get("readonly"):
            export_nfs_cmd += " --readonly"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=export_nfs_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            out, rc = client.exec_command(
                sudo=True, cmd=f"ceph nfs export ls {nfs_cluster_name} --format json"
            )
            export_ls = json.loads(out)
            if binding not in export_ls:
                raise CommandFailed(
                    f"Creation of export failed: {export_nfs_cmd} failed"
                )
        return cmd_out, cmd_rc

    @function_execution_time
    def remove_nfs_export(
        self, client, nfs_cluster_name, binding, validate=True, **kwargs
    ):
        """
        Remove an NFS export from a specific NFS cluster.

        Args:
            client (str): The name or identifier of the client.
            nfs_cluster_name (str): The name of the NFS cluster.
            binding (str): The binding information for the NFS export.
            validate (bool): Flag indicating whether to validate the export removal.
            **kwargs: Additional keyword arguments for customization.

        Returns:
            tuple: A tuple containing the output of the command and the command's return code.
        """
        remove_export_nfs_cmd = f"ceph nfs export rm {nfs_cluster_name} {binding}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=remove_export_nfs_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            out, rc = client.exec_command(
                sudo=True, cmd=f"ceph nfs export ls {nfs_cluster_name}"
            )
            if out:
                export_ls = out.split("\n")
                if binding in export_ls:
                    raise CommandFailed(
                        f"Removal of export failed: {remove_export_nfs_cmd} failed"
                    )
        return cmd_out, cmd_rc

    def create_fs(self, client, vol_name, validate=True, **kwargs):
        """
        This Function creates the cephfs volume with vol_name given
        It validates the creation operation by default.
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            validate:
            **kwargs:
                check_ec = True
        Returns:

        """
        fs_cmd = f"ceph fs volume create {vol_name}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=fs_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            out, rc = client.exec_command(sudo=True, cmd="ceph fs ls --format json")
            volname_ls = json.loads(out)
            if vol_name not in [i["name"] for i in volname_ls]:
                raise CommandFailed(f"Creation of filesystem: {vol_name} failed")
        return cmd_out, cmd_rc

    def create_subvolumegroup(
        self, client, vol_name, group_name, validate=True, **kwargs
    ):
        """
        Create subvolume group with vol_name, group_name
        It supports below optional arguments also
        Args:
            vol_name:
            group_name:
            **kwargs:
                pool_layout
                uid
                gid
                mode
                validate = True
                check_ec = True
        Returns:
            Returns the cmd_out and cmd_rc for Create cmd
        """
        subvolumegroup_cmd = f"ceph fs subvolumegroup create {vol_name} {group_name}"
        if kwargs.get("pool_layout"):
            subvolumegroup_cmd += f" --pool_layout {kwargs.get('pool_layout')}"
        if kwargs.get("uid"):
            subvolumegroup_cmd += f" --uid {kwargs.get('uid')}"
        if kwargs.get("gid"):
            subvolumegroup_cmd += f" --gid {kwargs.get('gid')}"
        if kwargs.get("mode"):
            subvolumegroup_cmd += f" --mode {kwargs.get('mode')}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=subvolumegroup_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            out, rc = client.exec_command(
                sudo=True, cmd=f"ceph fs subvolumegroup ls {vol_name} --format json"
            )
            subvolumegroup_ls = json.loads(out)
            if group_name not in [i["name"] for i in subvolumegroup_ls]:
                raise CommandFailed(f"Creation of subvolume group: {group_name} failed")
        return cmd_out, cmd_rc

    def create_subvolume(self, client, vol_name, subvol_name, validate=True, **kwargs):
        """
        Creates Subvolume with given arguments
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            subvol_name:
            validate:
            **kwargs:
                size : str in mb
                group_name : str
                pool_layout : str
                uid : str
                gid : str
                mode : str
                namespace_isolated : boolean
                check_ec = True
        Returns:
            Returns the cmd_out and cmd_rc for Create cmd
        """
        subvolume_cmd = f"ceph fs subvolume create {vol_name} {subvol_name}"
        if kwargs.get("size"):
            subvolume_cmd += f" --size {kwargs.get('size')}"
        if kwargs.get("group_name"):
            subvolume_cmd += f" --group_name {kwargs.get('group_name')}"
        if kwargs.get("pool_layout"):
            subvolume_cmd += f" --pool_layout {kwargs.get('pool_layout')}"
        if kwargs.get("uid"):
            subvolume_cmd += f" --uid {kwargs.get('uid')}"
        if kwargs.get("gid"):
            subvolume_cmd += f" --gid {kwargs.get('gid')}"
        if kwargs.get("mode"):
            subvolume_cmd += f" --mode {kwargs.get('mode')}"
        if kwargs.get("namespace_isolated"):
            subvolume_cmd += " --namespace-isolated"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=subvolume_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            listsubvolumes_cmd = f"ceph fs subvolume ls {vol_name}"
            if kwargs.get("group_name"):
                listsubvolumes_cmd += f" --group_name {kwargs.get('group_name')}"
            out, rc = client.exec_command(
                sudo=True, cmd=f"{listsubvolumes_cmd} --format json"
            )
            subvolume_ls = json.loads(out)
            if subvol_name not in [i["name"] for i in subvolume_ls]:
                raise CommandFailed(f"Creation of subvolume : {subvol_name} failed")
        return cmd_out, cmd_rc

    def create_snapshot(
        self, client, vol_name, subvol_name, snap_name, validate=True, **kwargs
    ):
        """
        Create snapshot with vol_name, subvol_name, snap_name
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            subvol_name:
            snap_name:
            validate:
            **kwargs:
                group_name : str
                check_ec = True
        Returns:
            Returns the cmd_out and cmd_rc for Create cmd
        """
        snapshot_cmd = (
            f"ceph fs subvolume snapshot create {vol_name} {subvol_name} {snap_name}"
        )
        if kwargs.get("group_name"):
            snapshot_cmd += f" --group_name {kwargs.get('group_name')}"
        if kwargs.get("time"):
            snapshot_cmd = f"time {snapshot_cmd}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=snapshot_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            listsnapshot_cmd = f"ceph fs subvolume snapshot ls {vol_name} {subvol_name}"
            if kwargs.get("group_name"):
                listsnapshot_cmd += f" --group_name {kwargs.get('group_name')}"
            out, rc = client.exec_command(
                sudo=True, cmd=f"{listsnapshot_cmd} --format json"
            )
            snapshot_ls = json.loads(out)
            if snap_name not in [i["name"] for i in snapshot_ls]:
                raise CommandFailed(f"Creation of subvolume : {snap_name} failed")
        return cmd_out, cmd_rc

    def set_snapshot_metadata(
        self, client, vol_name, subvol_name, snap_name, metadata_dict, **kwargs
    ):
        """
        Create snapshot metadata with vol_name, subvol_name, snap_name
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            subvol_name:
            snap_name:
            metadata_dict:
            validate:
            **kwargs:
                group_name : str
                check_ec = True
        Returns:
            Returns the cmd_out and cmd_rc for Create cmd
        """
        snapshot_cmd = f"ceph fs subvolume snapshot metadata set {vol_name} {subvol_name} {snap_name} "
        for k, v in metadata_dict.items():
            snapshot_cmd += f"'{k}' '{v}' "
        if kwargs.get("group_name"):
            snapshot_cmd += f"--group_name {kwargs.get('group_name')}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=snapshot_cmd, check_ec=kwargs.get("check_ec", True)
        )
        return cmd_out, cmd_rc

    def get_snapshot_metadata(
        self, client, vol_name, subvol_name, snap_name, metadata_key, **kwargs
    ):
        """
        Get snapshot metadata with vol_name, subvol_name, snap_name
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            subvol_name:
            snap_name:
            metadata_key:
            validate:
            **kwargs:
                group_name : str
                check_ec = True
        Returns:
            Returns the cmd_out and cmd_rc for Create cmd
        """
        snapshot_cmd = f"ceph fs subvolume snapshot metadata get {vol_name} {subvol_name} {snap_name} {metadata_key} "
        if kwargs.get("group_name"):
            snapshot_cmd += f"--group_name {kwargs.get('group_name')}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=snapshot_cmd, check_ec=kwargs.get("check_ec", True)
        )
        return cmd_out, cmd_rc

    def remove_snapshot_metadata(
        self, client, vol_name, subvol_name, snap_name, metadata_list, **kwargs
    ):
        """
        Remove snapshot's metadata with vol_name, subvol_name, snap_name
        It supports below arguments
        Args:
            client:
            vol_name:
            subvol_name:
            snap_name:
            metadata_list:
            validate:
            **kwargs:
                group_name : str
                check_ec = True
        Returns:
            Returns the cmd_out and cmd_rc for Create cmd
        """
        snapshot_cmd = f"ceph fs subvolume snapshot metadata rm {vol_name} {subvol_name} {snap_name} "
        for i in metadata_list:
            snapshot_cmd += f"{i} "
        if kwargs.get("group_name"):
            snapshot_cmd += f"--group_name {kwargs.get('group_name')}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=snapshot_cmd, check_ec=kwargs.get("check_ec", True)
        )
        return cmd_out, cmd_rc

    def list_snapshot_metadata(
        self, client, vol_name, subvol_name, snap_name, **kwargs
    ):
        """
        List metadata of the snapshot with vol_name, subvol_name, snap_name
        It supports below arguments
        Args:
            client:
            vol_name:
            subvol_name:
            snap_name:
            **kwargs:
                group_name : str
                check_ec = True
        Returns:
            Returns the cmd_out and cmd_rc for Create cmd
        """
        snapshot_cmd = f"ceph fs subvolume snapshot metadata ls {vol_name} {subvol_name} {snap_name} "
        if kwargs.get("group_name"):
            snapshot_cmd += f"--group_name {kwargs.get('group_name')}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=snapshot_cmd, check_ec=kwargs.get("check_ec", True)
        )
        return cmd_out, cmd_rc

    def create_clone(
        self,
        client,
        vol_name,
        subvol_name,
        snap_name,
        target_subvol_name,
        validate=True,
        timeout=600,
        **kwargs,
    ):
        """
        Creates clone based on the arguments vol_name,subvol_name,snap_name,target_subvol_name
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            subvol_name:
            snap_name:
            target_subvol_name:
            validate:
            **kwargs:
                group_name
                target_group_name
                pool_layout
                check_ec = True
        Returns:
            Returns the cmd_out and cmd_rc for Create cmd
        """
        clone_cmd = f"ceph fs subvolume snapshot clone {vol_name} {subvol_name} {snap_name} {target_subvol_name}"
        if kwargs.get("group_name"):
            clone_cmd += f" --group_name {kwargs.get('group_name')}"
        if kwargs.get("target_group_name"):
            clone_cmd += f" --target_group_name {kwargs.get('target_group_name')}"
        if kwargs.get("pool_layout"):
            clone_cmd += f" --pool_layout {kwargs.get('pool_layout')}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True,
            cmd=clone_cmd,
            check_ec=kwargs.get("check_ec", True),
            timeout=timeout,
        )
        if validate:
            listsubvolumes_cmd = f"ceph fs subvolume ls {vol_name}"
            if kwargs.get("target_group_name"):
                listsubvolumes_cmd += f" --group_name {kwargs.get('target_group_name')}"
            out, rc = client.exec_command(
                sudo=True, cmd=f"{listsubvolumes_cmd} --format json"
            )
            subvolume_ls = json.loads(out)
            if target_subvol_name not in [i["name"] for i in subvolume_ls]:
                raise CommandFailed(f"Creation of clone : {target_subvol_name} failed")
        return cmd_out, cmd_rc

    def remove_snapshot(
        self, client, vol_name, subvol_name, snap_name, validate=True, **kwargs
    ):
        """
        Removes the snapshot by taking snap_name,vol_name, subvol_name
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            subvol_name:
            snap_name:
            validate:
            **kwargs:
                group_name : str
                force : boolean
                check_ec : boolean

        Returns:
            Returns the cmd_out and cmd_rc for remove cmd
        """
        rmsnapshot_cmd = (
            f"ceph fs subvolume snapshot rm {vol_name} {subvol_name} {snap_name}"
        )
        if kwargs.get("group_name"):
            rmsnapshot_cmd += f" --group_name {kwargs.get('group_name')}"
        if kwargs.get("force"):
            rmsnapshot_cmd += " --force"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=rmsnapshot_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            listsnapshot_cmd = f"ceph fs subvolume snapshot ls {vol_name} {subvol_name}"
            if kwargs.get("group_name"):
                listsnapshot_cmd += f" --group_name {kwargs.get('group_name')}"
            out, rc = client.exec_command(
                sudo=True, cmd=f"{listsnapshot_cmd} --format json"
            )
            snapshot_ls = json.loads(out)
            if snap_name in [i["name"] for i in snapshot_ls]:
                raise CommandFailed(f"Remove of snapshot : {snap_name} failed")
        return cmd_out, cmd_rc

    def remove_subvolume(
        self, client, vol_name, subvol_name, validate=True, timeout=600, **kwargs
    ):
        """
        Removes the subvolume based subvol_name,vol_name
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            subvol_name:
            validate:
            **kwargs:
                group_name : str
                retain_snapshots : boolean
                force : boolean
                check_ec : boolean
        Returns:
            Returns the cmd_out and cmd_rc for remove cmd
        """
        rmsubvolume_cmd = f"ceph fs subvolume rm {vol_name} {subvol_name}"
        if kwargs.get("group_name"):
            rmsubvolume_cmd += f" --group_name {kwargs.get('group_name')}"
        if kwargs.get("retain_snapshots"):
            rmsubvolume_cmd += " --retain-snapshots"
        if kwargs.get("force"):
            rmsubvolume_cmd += " --force"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True,
            cmd=rmsubvolume_cmd,
            check_ec=kwargs.get("check_ec", True),
            timeout=timeout,
        )
        if validate:
            listsubvolumes_cmd = f"ceph fs subvolume ls {vol_name}"
            if kwargs.get("group_name"):
                listsubvolumes_cmd += f" --group_name {kwargs.get('group_name')}"
            out, rc = client.exec_command(
                sudo=True, cmd=f"{listsubvolumes_cmd} --format json", timeout=timeout
            )
            subvolume_ls = json.loads(out)
            if subvol_name in [i["name"] for i in subvolume_ls]:
                raise CommandFailed(f"Deletion of clone : {subvol_name} failed")
        return cmd_out, cmd_rc

    def remove_subvolumegroup(
        self, client, vol_name, group_name, validate=True, **kwargs
    ):
        """
        Removes the sub volume group with the group_name,vol_name as argument
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            group_name:
            validate:
            **kwargs:
                force
                check_ec : boolean

        Returns:
            Returns the cmd_out and cmd_rc for remove cmd
        """
        rmsubvolumegroup_cmd = f"ceph fs subvolumegroup rm {vol_name} {group_name}"
        if kwargs.get("force"):
            rmsubvolumegroup_cmd += " --force"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=rmsubvolumegroup_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            out, rc = client.exec_command(
                sudo=True, cmd=f"ceph fs subvolumegroup ls {vol_name} --format json"
            )
            subvolumegroup_ls = json.loads(out)
            if group_name in [i["name"] for i in subvolumegroup_ls]:
                raise CommandFailed(f"Deletion of subvolume group: {group_name} failed")
        return cmd_out, cmd_rc

    def remove_fs(self, client, vol_name, validate=True, **kwargs):
        """
        Removes the filesystem with the vol_name as argument
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            validate:
            **kwargs:
                check_ec : boolean

        Returns:
            Returns the cmd_out and cmd_rc for remove cmd
        """
        rmvolume_cmd = f"ceph fs volume rm {vol_name} --yes-i-really-mean-it"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=rmvolume_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            out, rc = client.exec_command(sudo=True, cmd="ceph fs ls --format json")
            volname_ls = json.loads(out)
            if vol_name in [i["name"] for i in volname_ls]:
                raise CommandFailed(f"Creation of filesystem: {vol_name} failed")
        return cmd_out, cmd_rc

    def fs_client_authorize(
        self, client, fs_name, client_name, dir_name, permission, **kwargs
    ):
        """
        We can create ceph clients for cephfs using this module.
        We can create client with permissions on directories in cephfs.

        Args:
            client: Client_node
            fs_name: cephfs name
            client_name: ceph client
            dir_name: Directory in cephfs
            permission: r/rw (read-only/read-write)
            **kwargs:
                extra_params : we can include extra parameters as more directories & permissions

        Returns:

        """
        command = (
            f"ceph fs authorize {fs_name} client.{client_name} {dir_name} {permission} "
        )
        if kwargs.get("extra_params"):
            command += f"{kwargs.get('extra_params')}"
        out, rc = client.exec_command(sudo=True, cmd=command)
        return 0

    def activate_multiple_mdss(self, clients):
        """
        Activate Multiple MDS for ceph filesystem
        Args:
            clients: Client_nodes
        """
        for client in clients:
            fs_info = self.get_fs_info(client)
            fs_name = fs_info.get("fs_name")
            log.info("Activating Multiple MDSs:")
            client.exec_command(cmd="ceph -v | awk {'print $3'}")
            command = f"ceph fs set {fs_name} max_mds 2"
            client.exec_command(sudo=True, cmd=command)
            return 0

    def mds_cleanup(self, nodes, dir_fragmentation):
        """
        Deactivating multiple mds activated, by setting it to single mds server
        Args:
            nodes: Client_nodes
            dir_fragmentation: fragmentation directory
        """
        log.info("Deactivating Multiple MDSs")
        for node in nodes:
            fs_info = self.get_fs_info(node)
            fs_name = fs_info.get("fs_name")
            log.info("Deactivating Multiple MDSs")
            log.info("Setting Max mds to 1:")
            command = f"ceph fs set {fs_name} max_mds 1"
            node.exec_command(sudo=True, cmd=command)
            if dir_fragmentation is not None:
                log.info("Disabling directory fragmentation")
                node.exec_command(
                    sudo=True,
                    cmd="ceph fs set %s allow_dirfrags 0" % fs_info.get("fs_name"),
                )
            break
        return 0

    def mds_fail_over(self, node):
        """
        method for validating MDS fail-over functionality
        Args:
            node: Client_node
        """
        timeout = 120
        timeout = datetime.timedelta(seconds=timeout)
        start = datetime.datetime.now()
        while True:
            fs_info = self.get_fs_info(node)
            fs_name = fs_info.get("fs_name")
            out, rc = node.exec_command(
                sudo=True, cmd=f"ceph fs status {fs_name} --format json"
            )
            output = json.loads(out)
            active_mds = [
                mds["name"] for mds in output["mdsmap"] if mds["state"] == "active"
            ]
            if len(active_mds) == 2:
                log.info("Failing MDS 1")
                node.exec_command(sudo=True, cmd="ceph mds fail 1")
                break
            else:
                log.info("waiting for active-active mds state")
                if datetime.datetime.now() - start > timeout:
                    log.error("Failed to get active-active mds")
                    return 1
        return 0

    def io_verify(self, client):
        """
        Client IO Verification
        Args:
            client: Client_node
        """
        if client.node.exit_status == 0:
            self.return_counts.update({client.node.hostname: client.node.exit_status})
            log.info("Client IO is going on,success")
        else:
            self.return_counts.update({client.node.hostname: client.node.exit_status})
            print("------------------------------------")
            print(self.return_counts)
            print("------------------------------------")
            log.error("Client IO got interrupted")
        return self.return_counts

    def pinned_dir_io_mdsfailover(
        self,
        clients,
        mounting_dir,
        dir_name,
        range1,
        range2,
        num_of_files,
        mds_fail_over,
    ):
        """
        Pinnging directories on mds failover
        Args:
            clients: Client_nodes
            mounting_dir: mounted directory
            dir_name: dir name
            range1: range2: ranges
            num_of_files: number of files
            mds_fail_over: mds failover method
        """
        log.info("Performing IOs on clients")
        for client in clients:
            for num in range(int(range1), int(range2)):
                working_dir = dir_name + "_" + str(num)
                out, rc = client.exec_command(f"sudo ls {mounting_dir}")
                if working_dir not in out:
                    client.exec_command(cmd=f"mkdir {mounting_dir}{dir_name}_{num}")
                log.info("Performing MDS failover:")
                mds_fail_over(client)
                command = "python3 /home/cephuser/smallfile/smallfile_cli.py "
                f"--operation create --threads 1 --file-size 100  --files  {num_of_files} "
                f"--top {mounting_dir}{dir_name}_{num}"
                client.exec_command(
                    sudo=True, cmd=command, long_running=True, timeout=300
                )
                self.return_counts = self.io_verify(client)
            break
        return self.return_counts, 0

    def get_clone_status(self, client, vol_name, clone_name, **kwargs):
        """
        Returns the clone status
        Args:
            clients: Client_nodes
            vol_name:
            clone_name:
            **kwargs:
                group_name

        """
        clone_status_cmd = f"ceph fs clone status {vol_name} {clone_name}"
        if kwargs.get("group_name"):
            clone_status_cmd += f" --group_name {kwargs.get('group_name')}"
        clone_status_cmd += " --format json"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=clone_status_cmd, check_ec=kwargs.get("check_ec", True)
        )

        return cmd_out, cmd_rc

    def get_snapshot_info(self, client, vol_name, subvol_name, snap_name, **kwargs):
        """

        Args:
            client:
            vol_name:
            snap_name:
            **kwargs:

        Returns:

        """
        snap_info_cmd = (
            f"ceph fs subvolume snapshot info {vol_name} {subvol_name} {snap_name}"
        )
        if kwargs.get("group_name"):
            snap_info_cmd += f" --group_name {kwargs.get('group_name')}"
        snap_info_cmd += " --format json"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=snap_info_cmd, check_ec=kwargs.get("check_ec", True)
        )
        return cmd_out, cmd_rc

    def clone_cancel(self, client, vol_name, clone_name, **kwargs):
        """
        Cancels the clone operation
        Args:
            clients: Client_nodes
            vol_name:
            clone_name:
            **kwargs:
                group_name

        """
        clone_status_cmd = f"ceph fs clone cancel {vol_name} {clone_name}"
        if kwargs.get("group_name"):
            clone_status_cmd += f" --group_name {kwargs.get('group_name')}"
        clone_status_cmd += " --format json"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=clone_status_cmd, check_ec=kwargs.get("check_ec", True)
        )

        return cmd_out, cmd_rc

    def validate_clone_state(
        self, client, clone, expected_state="complete", timeout=300
    ):
        """
        Validates the clone status based on the expected_state
        Args:
            clients: Client_nodes
            clone_obj:
                "vol_name": Volume name where clone as been created Ex: cephfs,
                "subvol_name": sub Volume name from where this clone has been created,
                "snap_name": snapshot name,
                "target_subvol_name": "clone_status_1",
                "group_name": "subvolgroup_1",
        """
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        clone_transistion_states = []
        cmd_out, cmd_rc = self.get_clone_status(
            client,
            clone["vol_name"],
            clone["target_subvol_name"],
            group_name=clone.get("target_group_name", ""),
        )
        status = json.loads(cmd_out)
        if status["status"]["state"] not in clone_transistion_states:
            clone_transistion_states.append(status["status"]["state"])
        while status["status"]["state"] != expected_state:
            cmd_out, cmd_rc = self.get_clone_status(
                client,
                clone["vol_name"],
                clone["target_subvol_name"],
                group_name=clone.get("target_group_name", ""),
            )
            status = json.loads(cmd_out)
            log.info(
                f"Clone Status of {clone['vol_name']} : {status['status']['state']}"
            )
            if status["status"]["state"] not in [
                "in-progress",
                "complete",
                "pending",
                "canceled",
            ]:
                raise CommandFailed(f'{status["status"]["state"]} is not valid status')
            if end_time < datetime.datetime.now():
                raise CommandFailed(
                    f"Clone creation has not reached to Complete state even after {timeout} sec"
                    f'Current state of the clone is {status["status"]["state"]}'
                )
        return clone_transistion_states

    def reboot_node(self, ceph_node, timeout=300):
        ceph_node.exec_command(sudo=True, cmd="reboot", check_ec=False)
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        while datetime.datetime.now() < endtime:
            try:
                sleep(20)
                ceph_node.node.reconnect()
                return
            except BaseException:
                log.error(
                    "Failed to reconnect to the node {node} after reboot ".format(
                        node=ceph_node.node.ip_address
                    )
                )
        raise RuntimeError(
            "Failed to reconnect to the node {node} after reboot ".format(
                node=ceph_node.node.ip_address
            )
        )

    def pid_kill(self, node, daemon):
        out, rc = node.exec_command(cmd="pgrep %s " % daemon, container_exec=False)
        out = out.split("\n")
        log.info(out)
        out.pop()
        for pid in out:
            node.exec_command(
                sudo=True, cmd="kill -9 %s" % pid, container_exec=False, check_ec=False
            )
            sleep(10)
        out_2, rc = node.exec_command(
            cmd="pgrep %s " % daemon, container_exec=False, check_ec=False
        )
        if rc:
            return 0
        out_2 = out_2.split("\n")
        out_2.pop()
        if out == out_2:
            raise CommandFailed(
                f"PID Kill Operation Failed pid Before : {out}, PID After : {out_2}"
            )
        return 0

    def network_disconnect(self, ceph_object, sleep_time=20):
        script = f"""import time,os
os.system('sudo systemctl stop network')
time.sleep({sleep_time})
os.system('sudo systemctl start  network')
    """
        node = ceph_object.node
        nw_disconnect = node.remote_file(
            sudo=True, file_name="/home/cephuser/nw_disconnect.py", file_mode="w"
        )
        nw_disconnect.write(script)
        nw_disconnect.flush()

        log.info("Stopping the network..")
        node.exec_command(sudo=True, cmd="yum install -y python3")
        node.exec_command(sudo=True, cmd="python3 /home/cephuser/nw_disconnect.py")
        log.info("Starting the network..")
        return 0

    def node_power_failure(
        self,
        node,
        sleep_time=300,
        **kwargs,
    ):
        """
        This is used for node power failures.
        Limitation : This works only for Openstack Vms


        Args:
            sleep_time:
            node_1:
            **kwargs:

        Returns:

        """
        user = os.getlogin()
        log.info(f"{user} logged in")
        if kwargs.get("cloud_type") == "openstack":
            kwargs.pop("cloud_type")
            driver = get_openstack_driver(**kwargs)
            for node_obj in driver.list_nodes():
                if node.private_ip in node_obj.private_ips:
                    log.info("Doing power-off on %s" % node_obj.name)
                    driver.ex_stop_node(node_obj)
                    sleep(20)
                    op = driver.ex_get_node_details(node_obj)
                    if op.state == "stopped":
                        log.info("Node stopped successfully")
                    sleep(sleep_time)
                    log.info("Doing power-on on %s" % node_obj.name)
                    driver.ex_start_node(node_obj)
                    sleep(20)
                    op = driver.ex_get_node_details(node_obj)
                    if op.state == "running":
                        log.info("Node restarted successfully")
                    sleep(20)
        elif kwargs.get("cloud_type") == "ibmc":
            # To DO for IBM
            pass
        else:
            pass
        return 0

    def node_power_off(
        self,
        node,
        sleep_time=300,
        **kwargs,
    ):
        """
        This is used for node power failures.
        Limitation : This works only for Openstack Vms


        Args:
            sleep_time:
            node_1:
            **kwargs:

        Returns:

        """
        user = os.getlogin()
        log.info(f"{user} logged in")
        if kwargs.get("cloud_type") == "openstack":
            kwargs.pop("cloud_type")
            driver = get_openstack_driver(**kwargs)
            for node_obj in driver.list_nodes():
                if node.private_ip in node_obj.private_ips:
                    log.info("Doing power-off on %s" % node_obj.name)
                    driver.ex_stop_node(node_obj)
                    sleep(20)
                    op = driver.ex_get_node_details(node_obj)
                    if op.state == "stopped":
                        log.info("Node stopped successfully")
                    sleep(sleep_time)
        elif kwargs.get("cloud_type") == "ibmc":
            # To DO for IBM
            pass
        else:
            pass
        return 0

    def node_power_on(
        self,
        node,
        sleep_time=300,
        **kwargs,
    ):
        """
        This is used for node power failures.
        Limitation : This works only for Openstack Vms


        Args:
            sleep_time:
            node_1:
            **kwargs:

        Returns:

        """
        user = os.getlogin()
        log.info(f"{user} logged in")
        if kwargs.get("cloud_type") == "openstack":
            kwargs.pop("cloud_type")
            driver = get_openstack_driver(**kwargs)
            for node_obj in driver.list_nodes():
                if node.private_ip in node_obj.private_ips:
                    log.info("Doing power-on on %s" % node_obj.name)
                    driver.ex_start_node(node_obj)
                    sleep(20)
                    op = driver.ex_get_node_details(node_obj)
                    if op.state == "running":
                        log.info("Node restarted successfully")
                    sleep(20)
                    sleep(sleep_time)
        elif kwargs.get("cloud_type") == "ibmc":
            # To DO for IBM
            pass
        else:
            pass
        return 0

    def get_floating_ip(self, node, **kwargs):
        user = os.getlogin()
        log.info(f"{user} logged in")
        new_kwargs = kwargs.copy()
        if kwargs.get("cloud_type") == "openstack":
            new_kwargs.pop("cloud_type")
            driver = get_openstack_driver(**new_kwargs)
            node_obj = self.get_node_obj(node, **kwargs)
            network_pool = list(node_obj.extra.get("addresses").keys())
            floating_ip = driver.ex_create_floating_ip(network_pool[0])
            return floating_ip
        elif kwargs.get("cloud_type") == "ibmc":
            # To DO for IBM
            pass
        else:
            pass

    def get_node_obj(self, node, **kwargs):
        user = os.getlogin()
        log.info(f"{user} logged in")
        new_kwargs = kwargs.copy()
        if kwargs.get("cloud_type") == "openstack":
            new_kwargs.pop("cloud_type")
            driver = get_openstack_driver(**new_kwargs)
            for node_obj in driver.list_nodes():
                if node.private_ip in node_obj.private_ips:
                    return node_obj

    def remove_floating_ip(self, floating_ip, **kwargs):
        user = os.getlogin()
        log.info(f"{user} logged in")
        new_kwargs = kwargs.copy()
        if kwargs.get("cloud_type") == "openstack":
            new_kwargs.pop("cloud_type")
            driver = get_openstack_driver(**new_kwargs)
            driver.ex_delete_floating_ip(floating_ip)
        elif kwargs.get("cloud_type") == "ibmc":
            # To DO for IBM
            pass
        else:
            pass

    def create_file_data(self, client, directory, no_of_files, file_name, data):
        """
        This function will write files to the directory with the data given
        :param client:
        :param directory:
        :param no_of_files:
        :param file_name:
        :param data:
        :return:
        """
        files = [f"{file_name}_{i}" for i in range(0, no_of_files)]
        client.exec_command(
            sudo=True,
            cmd=f"cd {directory};echo {data * random.randint(100, 500)} | tee {' '.join(files)}",
        )

    def get_files_and_checksum(self, client, directory):
        """
        This will collect the filenames and their respective checksums and returns the dictionary
        :param client:
        :param directory:
        :return:
        """
        out, rc = client.exec_command(
            sudo=True, cmd=f"cd {directory};ls -lrt |  awk {{'print $9'}}"
        )
        file_list = out.strip().split()
        file_dict = {}
        for file in file_list:
            out, rc = client.exec_command(sudo=True, cmd=f"md5sum {directory}/{file}")
            md5sum = out.strip().split()
            file_dict[file] = md5sum[0]
        return file_dict

    def set_quota_attrs(self, client, file, bytes, directory, **kwargs):
        """
        Args:
            file: sets the value as total number of files limit
            bytes: sets the value as total number of bytes limit
            directory: sets the above limits to this directory
            **kwargs:

        Returns:

        """
        if file:
            quota_set_cmd = f"setfattr -n ceph.quota.max_files -v {file}"
            client.exec_command(sudo=True, cmd=f"{quota_set_cmd} {directory}")
        if bytes:
            quota_set_cmd = f"setfattr -n ceph.quota.max_bytes -v {bytes}"
            client.exec_command(sudo=True, cmd=f"{quota_set_cmd} {directory}")

    def get_quota_attrs(self, client, directory, **kwargs):
        """
        Gets the quota of the given directory
        Args:
            client:
            directory: Gets the quota values for the given directory
            **kwargs:

        Returns:
            quota_dict
            will have values in this format {file : 0, bytes: 0}
        """
        quota_dict = {}
        file_quota, rc = client.exec_command(
            f"getfattr --only-values -n ceph.quota.max_files {directory}"
        )
        quota_dict["files"] = int(file_quota)
        byte_quota, rc = client.exec_command(
            f"getfattr --only-values -n ceph.quota.max_bytes {directory}"
        )
        quota_dict["bytes"] = int(byte_quota)
        return quota_dict

    def file_quota_test(self, client, mounting_dir, quota_attrs):
        """
        Validates the files quota that has been set on mounting dir
        it collects the quota_attrs from mounting dir.
        it checks if we are able to create with in the set limit and
        Also we are not able to create outside the limit
        Args:
            client:
            mounting_dir: Gets the quota values for the given directory
            quota_attrs : set quota values in dict
            **kwargs:
        Raises CommandFailed Exception anything fails
        """
        total_files = quota_attrs.get("files")
        temp_str = "".join([random.choice(string.ascii_letters) for _ in range(3)])
        files_in_dir = int(self.get_total_no_files(client, mounting_dir))
        if files_in_dir >= total_files:
            files = 1
        else:
            files = total_files - files_in_dir
        for i in range(1, files + 15):
            rc = client.exec_command(
                sudo=True,
                cmd=f"cd {mounting_dir};touch file_{temp_str}_{i}.txt",
                long_running=True,
                check_ec=False,
            )
            if rc == 1 and i < files:
                raise CommandFailed(
                    f"total allowed files {files} and current iteration {i}"
                )
            log.info(
                f"Return value for file_{temp_str}_{i}.txt is {rc} and total_allowed_files: {files}"
            )
        if rc == 0 and total_files != 0:
            raise CommandFailed("We are able to create more files than what we set")
        elif rc == 1 and total_files == 0:
            raise CommandFailed(
                f"File Attribute is set to {total_files} still we are not able to create files"
            )
        else:
            pass

    def get_total_no_files(self, client, directory):
        """
        Returns the total number files in the directory
        Args:
            client:
            directory: Gets the quota values for the given directory
            **kwargs:
        """
        total_files, rc = client.exec_command(
            f"find {directory} -type f -print | wc -l"
        )
        total_dir, rc = client.exec_command(f"find {directory} -type d -print | wc -l")
        print(f"total dir : {total_dir}")
        return total_files

    def byte_quota_test(self, client, mounting_dir, quota_attrs):
        """
        Validates the bytes quota that has been set on mounting dir
        it collects the quota_attrs from mounting dir.
        it checks if we are able to create with in the set limit and
        Also we are not able to create outside the limit
        Args:
            client:
            mounting_dir: Gets the quota values for the given directory
            quota_attrs : set quota values in dict
            **kwargs:
        Raises CommandFailed Exception anything fails
        """
        total_bytes = quota_attrs.get("bytes")
        temp_str = "".join([random.choice(string.ascii_letters) for _ in range(3)])
        bytes_in_dir = int(self.get_total_no_bytes(client, mounting_dir))
        if bytes_in_dir >= total_bytes:
            bytes = 100000
        else:
            bytes = total_bytes - bytes_in_dir
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={mounting_dir}/bytes_{temp_str}.txt bs=10M count={int(bytes / 10240)}",
            long_running=True,
        )
        rc = client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={mounting_dir}/bytes_{temp_str}.txt bs=10M count={int((bytes * 3) / 10240)}",
            check_ec=False,
            long_running=True,
        )
        log.info(
            f"Return value for cmd: "
            f"dd if=/dev/zero of={mounting_dir}/bytes_{temp_str}.txt bs=10M count={int((bytes * 3) / 10240)}"
            f" is {rc}"
        )
        if total_bytes != 0:
            if rc == 0:
                raise CommandFailed(
                    "We are able to write more bytes than bytes quota set"
                )

    def get_total_no_bytes(self, client, directory):
        total_bytes, rc = client.exec_command(
            f"du -sb  {directory}| awk '{{ print $1}}'"
        )
        return total_bytes

    def file_byte_quota_test(self, client, mounting_dir, quota_attrs):
        total_bytes = quota_attrs.get("bytes")
        temp_str = "".join([random.choice(string.ascii_letters) for _ in range(3)])
        bytes_in_dir = int(self.get_total_no_bytes(client, mounting_dir))
        if bytes_in_dir >= total_bytes:
            bytes = 1073741824
        else:
            bytes = total_bytes - bytes_in_dir

        total_files = quota_attrs.get("files")
        temp_str = "".join([random.choice(string.ascii_letters) for _ in range(3)])
        files_in_dir = int(self.get_total_no_files(client, mounting_dir))
        if files_in_dir >= total_files:
            files = 1
        else:
            files = total_files - files_in_dir

        for i in range(1, files + 15):
            rc = client.exec_command(
                sudo=True,
                cmd=f"cd {mounting_dir};touch file_bytes{temp_str}_{i}.txt;"
                f"dd if=/dev/zero of={mounting_dir}/file_bytes{temp_str}_{i}.txt bs=1 "
                f"count={int(int(bytes / files) / 10)}",
                long_running=True,
                check_ec=False,
            )
            if rc == 1 and i < files:
                raise CommandFailed(
                    f"total allowed files {files} and current iteration {i}"
                )
            log.info(
                f"Return value for file_{temp_str}_{i}.txt is {rc} and total_allowed_files: {files}"
            )
        if rc == 0 and total_files != 0:
            raise CommandFailed("We are able to create more files than what we set")
        elif rc == 1 and total_files == 0:
            raise CommandFailed(
                f"File Attribute is set to {total_files} still we are not able to create files"
            )
        else:
            pass

    def subvolume_authorize(self, client, vol_name, subvol_name, client_name, **kwargs):
        """
        Create client with permissions on cephfs subvolume
        Args:
            client: client node
            vol_name: Cephfs volume name
            subvol_name: Cephfs subvolume name
            client_name: Name of client to be created
            **kwargs:
                extra_params : subvolumegroup name, access level etc.
        """
        command = f"ceph fs subvolume authorize {vol_name} {subvol_name} {client_name}"
        if kwargs.get("extra_params"):
            command += f" {kwargs.get('extra_params')}"
            client.exec_command(sudo=True, cmd=command)

    def get_mds_status(self, client, num_of_mds, expected_status="active", **kwargs):
        """

        Args:
            client:
            num_of_mds:
            expected_status:

        Returns:

        """
        fs_status_cmd = "ceph fs status"
        if kwargs.get("vol_name"):
            fs_status_cmd += f" {kwargs.get('vol_name')}"
        fs_status_cmd += " --format json"
        out, rc = client.exec_command(sudo=True, cmd=fs_status_cmd)
        fs_status = json.loads(out)
        mds_map = fs_status["mdsmap"]
        active_list = [i["name"] for i in mds_map if i["state"] == expected_status]
        log.info(f"{mds_map}")
        if len(active_list) >= num_of_mds:
            return active_list
        log.info(f"{active_list}")
        raise CommandFailed(
            f"expected {num_of_mds} in {expected_status} status but only {len(active_list)} "
            f"are in {expected_status}"
        )

    def get_pool_df(self, client, pool_name, **kwargs):
        """
        Gets the pool Avaialble space and used space details
        Args:
            client: client node
            pool_name: Name of the pool
            **kwargs:
                vol_name : Name of the fs volume for which we need the status
        Return:
            returns pool details if present else returns None
        sample pool Return dictonary:
            {
            "avail": 33608753152,
            "id": 5,
            "name": "cephfs-metadata",
            "type": "metadata",
            "used": 278888448
        },

        """
        fs_status_cmd = "ceph fs status"
        if kwargs.get("vol_name"):
            fs_status_cmd += f" {kwargs.get('vol_name')}"
        fs_status_cmd += " --format json"
        out, rc = client.exec_command(sudo=True, cmd=fs_status_cmd)
        fs_status = json.loads(out)
        pool_status = fs_status["pools"]
        for pool in pool_status:
            if pool["name"] == pool_name:
                return pool
        else:
            return None

    def heartbeat_map(self, mds):
        """
        Verify "heartbeat_map" timeout is not in mds log
        Args:
            mds: mds node
        """
        try:
            mds.exec_command(
                sudo=True,
                cmd=f"grep heartbeat_map /var/log/ceph/ceph-mds.{mds.node.shortname}.log",
            )
            log.error("heartbeat map timeout seen")
            return 1
        except CommandFailed as e:
            log.info(e)
            log.info("heartbeat map timeout not found")
            return 0

    def get_subvolume_info(
        self, client, vol_name, subvol_name, validate=True, **kwargs
    ):
        """
        Gets the info of subvolume.
        Args:
            client: client node
            fs_name: name of thefilesystem or volume
            subvol_name: name of the subvolume which is required to collect the info
            **kwargs:
                group_name : Name of the subvoumegroup
        Return:
            returns pool details if present else returns None
            Sample output :

            {
            "atime": "2021-12-07 09:13:12",
            "bytes_pcent": "0.00",
            "bytes_quota": 2147483648,
            "bytes_used": 0,
            "created_at": "2021-12-07 09:13:12",
            "ctime": "2021-12-07 10:00:45",
            "data_pool": "cephfs.cephfs.data",
            "features": [
                "snapshot-clone",
                "snapshot-autoprotect",
                "snapshot-retention"
            ],
            "gid": 0,
            "mode": 16877,
            "mon_addrs": [
                "10.0.208.74:6789",
                "10.0.209.3:6789",
                "10.0.208.178:6789"
            ],
            "mtime": "2021-12-07 09:13:12",
            "path": "/volumes/subvolgroup_1/subvol_1/d4b4d20e-b8ed-4831-bb83-cf2bff91d109",
            "pool_namespace": "",
            "state": "complete",
            "type": "subvolume",
            "uid": 0

        """

        subvolume_info_cmd = f"ceph fs subvolume info {vol_name} {subvol_name}"
        if kwargs.get("group_name"):
            subvolume_info_cmd += f" --group_name {kwargs.get('group_name')}"
        out, rc = client.exec_command(sudo=True, cmd=subvolume_info_cmd)
        subvolume_info = json.loads(out)
        return subvolume_info

    def get_stats(self, client, file_path, validate=True, **kwargs):
        """
        Gets the stat of a file.
        Args:
            client: client node
            file_path: path of a file to collec the stats
            **kwargs:
                --printf : option to print a particular value
        Return:
            returns stat of a path(file or directory)
            If no option added to the cmd, it will return a dictionary consisted of
            "File", "Access", "Blocks", "Birth", "Access", "Uid", "Gid", "Size".
            Sample output :

            [root@ceph-hyelloji-9etpcf-node8 ~]# stat /mnt/cephfs_kernelqcnquvmv84_1/volumes/subvolgroup_1/
              File: /mnt/cephfs_kernelqcnquvmv84_1/volumes/subvolgroup_1/
              Size: 2           Blocks: 0          IO Block: 65536  directory
            Device: 31h/49d Inode: 1099511627829  Links: 4
            Access: (0755/drwxr-xr-x)  Uid: (   20/ UNKNOWN)   Gid: (   30/ UNKNOWN)
            Context: system_u:object_r:cephfs_t:s0
            Access: 2021-12-13 06:14:55.204505533 -0500
            Modify: 2021-12-13 06:15:16.335938236 -0500
            Change: 2021-12-13 06:15:16.335938236 -0500
             Birth: -

        """
        standard_format = " --printf='%n,%a,%A,%b,%w,%x,%u,%g,%s,%h'"
        stat_cmd = f"stat {file_path} "
        if kwargs.get("format"):
            stat_cmd += f" --printf {kwargs.get('format')}"
        else:
            stat_cmd += standard_format
            out, rc = client.exec_command(sudo=True, cmd=stat_cmd)
            key_list = [
                "File",
                "Octal_Permission",
                "Permission",
                "Blocks",
                "Birth",
                "Access",
                "Uid",
                "Gid",
                "Size",
                "Links",
            ]
            output_dic = dict(zip(key_list, out.split(",")))
            print(log.info(output_dic))
            return output_dic
        out, rc = client.exec_command(sudo=True, cmd=stat_cmd)
        return out

    def wait_for_cmd_to_succeed(self, client, cmd, timeout=180, interval=5):
        """
        Checks for the mount point and returns the status based on mount command
        :param client:
        :param mount_point:
        :param timeout:
        :param interval:
        :return: boolean
        """
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        log.info("Wait for the command to pass")
        while end_time > datetime.datetime.now():
            try:
                client.exec_command(sudo=True, cmd=cmd)
                return True
            except CommandFailed:
                sleep(interval)
        return False

    def run_ios(self, client, mounting_dir, io_tools=["dd", "smallfile"], file_name=""):
        def smallfile():
            client.exec_command(
                sudo=True,
                cmd=f"for i in create read append read delete create overwrite rename delete-renamed mkdir rmdir "
                f"create symlink stat chmod ls-l delete cleanup  ; "
                f"do python3 /home/cephuser/smallfile/smallfile_cli.py "
                f"--operation $i --threads 8 --file-size 10240 "
                f"--files 10 --top {mounting_dir} ; done",
                long_running=True,
            )

        def file_extract():
            client.exec_command(
                sudo=True,
                cmd=f"cd {mounting_dir};wget -O linux.tar.gz http://download.ceph.com/qa/linux-5.4.tar.gz",
            )
            client.exec_command(
                sudo=True,
                cmd="tar -xzf linux.tar.gz tardir/ ; sleep 10 ; rm -rf  tardir/",
            )

        def wget():
            client.exec_command(
                sudo=True,
                cmd=f"cd {mounting_dir};wget -O linux.tar.gz http://download.ceph.com/qa/linux-5.4.tar.gz",
            )

        def dd():
            client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={mounting_dir}{client.node.hostname}_dd_{file_name} bs=100M "
                f"count=5",
                long_running=True,
            )

        io_tool_map = {
            "dd": dd,
            "smallfile": smallfile,
            "wget": wget,
            "file_extract": file_extract,
        }
        io_tools = [io_tool_map.get(i) for i in io_tools if io_tool_map.get(i)]
        f = random.choice(io_tools)
        f()
        return f

    def run_ios_V1(self, client, mounting_dir, io_tools=["dd", "smallfile"], **kwargs):
        """
        Run the IO module, one of IO modules dd,small_file,wget and file_extract or all for given duration
        Args:
            client: client node , type - client object
            mounting_dir: mount path in client to run IO, type - str
            io_tools : List of IO tools to run, default : dd,smallfile, type - list
            run_time : run duration in minutes till when the IO would run, default - 1min
            **kwargs:
                run_time : duration in mins for test execution time, default - 1, type - int
                smallfile_params : A dict type io_params data to define small file io_tool params as below,
                    smallfile_params = {
                    "testdir_prefix": "smallfile_io_dir",
                    "threads": 8,
                    "file-size": 10240,
                    "files": 10,
                    }
                dd_params : A dict type io_params data to define dd io_tool params as below,
                dd_params = {
                    "file_name": "dd_test_file",
                    "input_type": "random",
                    "bs": "10M",
                    "count": 10,
                    }
                NOTE: If default prams are being over-ridden,
                    smallfile params : Both file-size and files params SHOULD to be passed
                    dd_params : Both bs and count SHOULD be passed
                for a logically correct combination of params in IO.

                example on method usage: fs_util.run_ios_V1(test_params["client"],io_path,io_tools=["dd"],
                run_time=duration_min,dd_params=test_io_params)
                    test_io_params = {
                        "file_name": "test_file",
                        "input_type": "zero",
                        "bs": "1M",
                        "count": 100,
                    }
        Return: None

        """

        def smallfile():
            log.info("IO tool scheduled : SMALLFILE")
            io_params = {
                "testdir_prefix": "smallfile_io_dir",
                "threads": 8,
                "file-size": 10240,
                "files": 10,
            }
            if kwargs.get("smallfile_params"):
                smallfile_params = kwargs.get("smallfile_params")
                for io_param in io_params:
                    if smallfile_params.get(io_param):
                        io_params[io_param] = smallfile_params[io_param]
            dir_suffix = "".join(
                [random.choice(string.ascii_letters) for _ in range(2)]
            )
            io_path = f"{mounting_dir}/{io_params['testdir_prefix']}_{dir_suffix}"
            client.exec_command(sudo=True, cmd=f"mkdir {io_path}")
            client.exec_command(
                sudo=True,
                cmd=f"for i in create read append read delete create overwrite rename delete-renamed mkdir rmdir "
                f"create symlink stat chmod ls-l delete cleanup  ; "
                f"do python3 /home/cephuser/smallfile/smallfile_cli.py "
                f"--operation $i --threads {io_params['threads']} --file-size {io_params['file-size']} "
                f"--files {io_params['files']} --top {io_path} ; done",
                long_running=True,
            )

        def file_extract():
            log.info("IO tool scheduled : FILE_EXTRACT")
            io_params = {"testdir_prefix": "file_extract_dir"}
            if kwargs.get("file_extract_params"):
                file_extract_params = kwargs.get("file_extract_params")
                for io_param in io_params:
                    if file_extract_params.get(io_param):
                        io_params[io_param] = file_extract_params[io_param]

            dir_suffix = "".join(
                [random.choice(string.ascii_letters) for _ in range(2)]
            )
            io_path = f"{mounting_dir}/{io_params['testdir_prefix']}_{dir_suffix}"
            client.exec_command(sudo=True, cmd=f"mkdir {io_path}")
            client.exec_command(
                sudo=True,
                cmd=f"cd {io_path};wget -O linux.tar.gz http://download.ceph.com/qa/linux-5.4.tar.gz",
            )
            client.exec_command(
                sudo=True,
                cmd=f"cd {io_path};tar -xzf linux.tar.gz tardir/ ; sleep 10 ; rm -rf  tardir/",
            )

        def wget():
            log.info("IO tool scheduled : WGET")
            io_params = {"testdir_prefix": "wget_dir"}
            if kwargs.get("wget_params"):
                wget_params = kwargs.get("wget_params")
                for io_param in io_params:
                    if wget_params.get(io_param):
                        io_params[io_param] = wget_params[io_param]
            dir_suffix = "".join(
                [random.choice(string.ascii_letters) for _ in range(2)]
            )
            io_path = f"{mounting_dir}/{io_params['testdir_prefix']}_{dir_suffix}"
            client.exec_command(sudo=True, cmd=f"mkdir {io_path}")
            client.exec_command(
                sudo=True,
                cmd=f"cd {io_path};wget -O linux.tar.gz http://download.ceph.com/qa/linux-5.4.tar.gz",
            )

        def dd():
            log.info("IO tool scheduled : DD")
            io_params = {
                "file_name": "dd_test_file",
                "input_type": "random",
                "bs": "10M",
                "count": 10,
            }
            if kwargs.get("dd_params"):
                dd_params = kwargs.get("dd_params")
                for io_param in io_params:
                    if dd_params.get(io_param):
                        io_params[io_param] = dd_params[io_param]
            suffix = "".join([random.choice(string.ascii_letters) for _ in range(2)])
            file_path = f"{mounting_dir}/{io_params['file_name']}_{suffix}"
            client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/{io_params['input_type']} of={file_path} bs={io_params['bs']} "
                f"count={io_params['count']}",
                long_running=True,
            )

        io_tool_map = {
            "dd": dd,
            "smallfile": smallfile,
            "wget": wget,
            "file_extract": file_extract,
        }

        log.info(f"IO tools planned to run : {io_tools}")

        io_tools = [io_tool_map.get(i) for i in io_tools if io_tool_map.get(i)]

        run_time = kwargs.get("run_time", 1)
        end_time = datetime.datetime.now() + datetime.timedelta(minutes=run_time)
        i = 0
        while datetime.datetime.now() < end_time:
            log.info(f"Iteration : {i}")
            with parallel() as p:
                for io_tool in io_tools:
                    p.spawn(io_tool)
            i += 1
            time.sleep(30)

    @retry(CommandFailed, tries=3, delay=60)
    def cephfs_nfs_mount(self, client, nfs_server, nfs_export, nfs_mount_dir, **kwargs):
        """
        Mount cephfs nfs export
        :param client:
        :param nfs_server:
        :param nfs_export:
        :param nfs_mount_dir:
        """
        client.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mount_dir}")
        command = f"mount -t nfs -o port={kwargs.get('port','2049')} {nfs_server}:{nfs_export} {nfs_mount_dir}"
        if kwargs.get("fstab"):
            try:
                client.exec_command(sudo=True, cmd="ls -lrt /etc/fstab.backup")
            except CommandFailed:
                client.exec_command(sudo=True, cmd="cp /etc/fstab /etc/fstab.backup")
            fstab = client.remote_file(
                sudo=True, file_name="/etc/fstab", file_mode="a+"
            )
            fstab_entry = (
                f"{nfs_server}:{nfs_export}    {nfs_mount_dir}    nfs4    "
                f"port={kwargs.get('port','2049')},"
                f"defaults,seclabel,vers=4.2,proto=tcp"
            )
            if kwargs.get("extra_params"):
                fstab_entry += f"{kwargs.get('extra_params')}"
            fstab_entry += "      0       0"
            print(dir(fstab))
            fstab.write(fstab_entry + "\n")
            fstab.flush()
            fstab.close()

        client.exec_command(sudo=True, cmd=command)
        rc = self.wait_until_mount_succeeds(client, nfs_mount_dir)
        return rc

    @staticmethod
    def nfs_ganesha_conf(node, nfs_client_name):
        out, rc = node.exec_command(
            sudo=True, cmd="ceph auth get-key client.%s" % nfs_client_name
        )
        secret_key = out.rstrip("\n")

        conf = """
        NFS_CORE_PARAM
        {
            Enable_NLM = false;
            Enable_RQUOTA = false;
            Protocols = 4;
        }

        NFSv4
        {
            Delegations = true;
            Minor_Versions = 1, 2;
        }

        CACHEINODE {
            Dir_Max = 1;
            Dir_Chunk = 0;
            Cache_FDs = true;
            NParts = 1;
            Cache_Size = 1;
        }

        EXPORT
        {
            Export_ID=100;
            Protocols = 4;
            Transports = TCP;
            Path = /;
            Pseudo = /ceph/;
            Access_Type = RW;
            Attr_Expiration_Time = 0;
            Delegations = R;
            Squash = "None";

            FSAL {
                Name = CEPH;
                User_Id = "%s";
                Secret_Access_key = "%s";
            }

        }
        CEPH
        {
            Ceph_Conf = /etc/ceph/ceph.conf;
        }
             """ % (
            nfs_client_name,
            secret_key,
        )
        conf_file = node.remote_file(
            sudo=True, file_name="/etc/ganesha/ganesha.conf", file_mode="w"
        )
        conf_file.write(conf)
        conf_file.flush()
        node.exec_command(sudo=True, cmd="systemctl enable nfs-ganesha")
        node.exec_command(sudo=True, cmd="systemctl start nfs-ganesha")
        return 0

    @staticmethod
    def nfs_ganesha_mount(client, mounting_dir, nfs_server):
        if client.pkg_type == "rpm":
            client.exec_command(sudo=True, cmd="yum install nfs-utils -y")
            client.exec_command(sudo=True, cmd="mkdir %s" % mounting_dir)
            client.exec_command(
                sudo=True,
                cmd="mount -t nfs -o nfsvers=4,sync,noauto,soft,proto=tcp %s:/ %s"
                % (nfs_server, mounting_dir),
            )

        return 0

    @staticmethod
    def generate_id_rsa_root(node):
        """
        generate id_rsa key files for the new vm node
        """
        # remove any old files
        node.exec_command(
            sudo=True,
            cmd="test -f ~/.ssh/id_rsa.pub && rm -f ~/.ssh/id*",
            check_ec=False,
        )
        node.exec_command(
            sudo=True, cmd="ssh-keygen -b 2048 -f ~/.ssh/id_rsa -t rsa -q -N ''"
        )
        id_rsa_pub, _ = node.exec_command(sudo=True, cmd="cat ~/.ssh/id_rsa.pub")
        return id_rsa_pub

    @staticmethod
    def setup_ssh_root_keys(clients):
        """
        Generate and distribute ssh keys within cluster
        """
        keys = "\n"
        hosts = ""
        hostkeycheck = (
            "Host *\n\tStrictHostKeyChecking no\n\tServerAliveInterval 2400\n"
        )

        for ceph in clients:
            keys = keys + FsUtils.generate_id_rsa_root(ceph)
            hosts = (
                hosts
                + ceph.node.ip_address
                + "\t"
                + ceph.node.hostname
                + "\t"
                + ceph.node.shortname
                + "\n"
            )
        for ceph in clients:
            keys_file = ceph.remote_file(
                sudo=True, file_name=".ssh/authorized_keys", file_mode="a"
            )
            hosts_file = ceph.remote_file(
                sudo=True, file_name="/etc/hosts", file_mode="a"
            )
            ceph.exec_command(
                sudo=True,
                cmd="[ -f ~/.ssh/config ] && chmod 700 ~/.ssh/config",
                check_ec=False,
            )
            ssh_config = ceph.remote_file(
                sudo=True, file_name=".ssh/config", file_mode="a"
            )
            keys_file.write(keys)
            hosts_file.write(hosts)
            ssh_config.write(hostkeycheck)
            keys_file.flush()
            hosts_file.flush()
            ssh_config.flush()
            ceph.exec_command(sudo=True, cmd="chmod 600 ~/.ssh/authorized_keys")
            ceph.exec_command(sudo=True, cmd="chmod 400 ~/.ssh/config")

    # MDS Pinning Support Functions
    def get_clients(self, build):
        log.info("Getting Clients")

        self.clients = self.ceph_cluster.get_ceph_objects("client")
        self.mon_node_ip = self.get_mon_node_ips()
        # self.mon_node_ip = ",".join(self.mon_node_ip_list)
        for client in self.clients:
            node = client.node
            if node.pkg_type == "rpm":
                out, rc = node.exec_command(
                    sudo=True, cmd="rpm -qa | grep -w 'attr\\|xattr'"
                )
                if "attr" not in out:
                    node.exec_command(sudo=True, cmd="yum install -y attr")
                node.exec_command(
                    sudo=True, cmd="yum install -y gcc python3-devel", check_ec=False
                )

                if "xattr" not in out:
                    pkgs = [
                        "@development",
                        "rh-python36",
                        "rh-python36-numpy rh-python36-scipy",
                        "rh-python36-python-tools rh-python36-python-six",
                        "libffi libffi-devel",
                    ]
                    if build.endswith("7") or build.startswith("3"):
                        cmd = "yum install -y " + " ".join(pkgs)
                        node.exec_command(sudo=True, cmd=cmd, long_running=True)

                    node.exec_command(
                        sudo=True, cmd="pip3 install xattr", long_running=True
                    )

                out, rc = node.exec_command(sudo=True, cmd="ls /home/cephuser")
                log.info("ls /home/cephuser : {}".format(out))

                if "Crefi" not in out:
                    self._setup_crefi(node)

                if "smallfile" not in out:
                    node.exec_command(
                        cmd="git clone https://github.com/bengland2/" "smallfile.git"
                    )

                out, rc = node.exec_command(sudo=True, cmd="rpm -qa")
                if "fio" not in out:
                    node.exec_command(sudo=True, cmd="yum install -y fio")
                if "fuse-2" not in out:
                    node.exec_command(sudo=True, cmd="yum install -y fuse")
                if "ceph-fuse" not in out:
                    node.exec_command(
                        sudo=True, cmd="yum install -y --nogpgcheck ceph-fuse"
                    )

            elif node.pkg_type == "deb":
                node.exec_command(sudo=True, cmd="pip3 install --upgrade pip3")
                out, rc = node.exec_command(sudo=True, cmd="apt list libattr1-dev")
                out = out.split()
                if "libattr1-dev/xenial,now" not in out:
                    node.exec_command(sudo=True, cmd="apt-get install -y libattr1-dev")
                out, rc = node.exec_command(sudo=True, cmd="apt list attr")
                out = out.split()
                if "attr/xenial,now" not in out:
                    node.exec_command(sudo=True, cmd="apt-get install -y attr")
                out, rc = node.exec_command(sudo=True, cmd="apt list fio")
                out = out.split()
                if "fio/xenial,now" not in out:
                    node.exec_command(sudo=True, cmd="apt-get install -y fio")
                out, rc = node.exec_command(sudo=True, cmd="pip3 list")
                if "crefi" not in out:
                    node.exec_command(sudo=True, cmd="pip3 install crefi")

                out, rc = node.exec_command(sudo=True, cmd="ls /home/cephuser")
                if "smallfile" not in out:
                    node.exec_command(
                        cmd="git clone " "https://github.com/bengland2/smallfile.git"
                    )
        self.mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        self.mounting_dir = "/mnt/cephfs_" + self.mounting_dir + "/"
        # separating clients for fuse and kernel
        self.fuse_clients = self.clients[0:2]
        self.kernel_clients = self.clients[2:4]
        self.result_vals.update({"clients": self.clients})
        self.result_vals.update({"fuse_clients": self.fuse_clients})
        self.result_vals.update({"kernel_clients": self.kernel_clients})
        self.result_vals.update({"mon_node_ip": self.mon_node_ip})
        self.result_vals.update({"mon_node": self.mons})
        self.result_vals.update({"osd_nodes": self.osds})
        self.result_vals.update({"mds_nodes": self.mdss})
        self.result_vals.update({"mgr_nodes": self.mgrs})
        self.result_vals.update({"mounting_dir": self.mounting_dir})

        return self.result_vals, 0

    @staticmethod
    def _setup_crefi(node):
        """
        Setup crefi using Crefi repository
        """
        # clone crefi repository
        node.exec_command(
            cmd="cd /home/cephuser/; git clone {}".format(
                "https://github.com/yogesh-mane/Crefi.git"
            ),
            long_running=True,
        )

        # Setup Crefi pre-requisites : pyxattr
        node.exec_command(sudo=True, cmd="pip3 install pyxattr", long_running=True)

    def generate_all_combinations(
        self, client, ioengine, mount_dir, workloads, sizes, iodepth_values, numjobs
    ):
        """
        Generates all the combimations of workloads sizes, iodepth as per the fio_config
        Sample fio_config:
        fio_config:
          global_params:
            ioengine: libaio
            direct: "1"
            size: ["1G"]
            time_based: ""
            runtime: "60"
          workload_params:
            random_rw:
              rw: ["read","write"]
              rwmixread: "70"
              bs: 8k
              numjobs: "4"
              iodepth: ["4","8","16","32"]
        """

        def generate_fio_config(ioengine, mount_dir, workload, size, iodepth, numjobs):
            fio_config = f"""
        [global]
        ioengine={ioengine}
        direct=1
        time_based
        runtime=60
        size={size}
        [{workload}]
        rw={workload}
        directory={mount_dir}
        bs= 4k
        numjobs={int(numjobs)}
        iodepth= {iodepth}
        """
            return fio_config

        fio_configs = []
        fio_filenames = []
        for combo in itertools.product(workloads, sizes, iodepth_values):
            workload, size, iodepth = combo
            filename = f"file_{workload}_{size}_{iodepth}.fio"
            fio_configs.append(
                (
                    filename,
                    generate_fio_config(
                        ioengine, mount_dir, workload, size, iodepth, numjobs=numjobs
                    ),
                )
            )
        for filename, config in fio_configs:
            remote_file = client.remote_file(
                sudo=True, file_name=filename, file_mode="w"
            )
            remote_file.write(config)
            remote_file.flush()
            fio_filenames.append(filename)
        log.info("Generated all the configs")
        return fio_filenames

    @retry(CommandFailed, tries=3, delay=60)
    def validate_services(self, client, service_name):
        """
        Validate if the Service is up and if it's not up, rety based on the
        count with a delay of 60 sec.
        Args:
            client : client node.
            service_name : name of the service which needs to be validated.
        Return:
            If the service is not up - with an interval of 60 sec, retry for 3 times before failing.
        """
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph orch ls --service_name={service_name} --format json"
        )
        service_ls = json.loads(out)
        log.info(service_ls)
        if service_ls[0]["status"]["running"] != service_ls[0]["status"]["size"]:
            raise CommandFailed(f"All {service_name} are Not UP")
        return True

    def validate_ports(self, client, service_name, port):
        json_data, rc = client.exec_command(
            sudo=True, cmd=f"ceph orch ls --service_name={service_name} --format json"
        )
        data = json.loads(json_data)
        log.info(data)
        spec_frontend_port = data[0]["spec"]["frontend_port"]
        status_ports = data[0].get("status").get("ports", [])
        log.info(f"Port specified for frontend_port is {spec_frontend_port}")
        log.info(f"Port deployed for ingress is {status_ports}")

        if spec_frontend_port not in status_ports:
            raise CommandFailed(f"The frontend port{port} is not matching")

    def get_ceph_health_status(self, client, validate=True):
        """
        Validate if the Ceph Health is OK or in ERROR State.
        Args:
            client : client node.
        Return:
            Status of the Ceph Health.
        """
        out, rc = client.exec_command(sudo=True, cmd="ceph -s -f json")
        log.info(out)
        health_status = json.loads(out)["health"]["status"]

        if health_status != "HEALTH_OK":
            raise CommandFailed(f"Ceph Cluster is in {health_status} State")
        log.info("Ceph Cluster is Healthy")

    @retry(CommandFailed, tries=10, delay=30)
    def wait_for_host_online(self, client1, node):
        out, rc = client1.exec_command(sudo=True, cmd="ceph orch host ls -f json")
        hosts = json.loads(out)
        for host in hosts:
            if host["hostname"] == node.node.hostname:
                hostname_status = host["status"]
                if hostname_status == "Offline":
                    raise CommandFailed("Host is in Offline state")

    @retry(CommandFailed, tries=5, delay=30)
    def wait_for_host_offline(self, client1, node):
        out, rc = client1.exec_command(sudo=True, cmd="ceph orch host ls -f json")
        hosts = json.loads(out)
        for host in hosts:
            if host["hostname"] == node.node.hostname:
                hostname_status = host["status"]
                if hostname_status != "Offline":
                    raise CommandFailed("Host is in online state")

    @retry(CommandFailed, tries=5, delay=30)
    def wait_for_service_to_be_in_running(self, client1, node):
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph orch ps {node.node.hostname} -f json"
        )
        services = json.loads(out)
        for service in services:
            if service["status"] != 1:
                raise CommandFailed(f"service : {service['service_name']} is not UP")

    def get_active_nfs(self, nfs_servers, virtual_ip):
        for nfs in nfs_servers:
            out, rc = nfs.exec_command(sudo=True, cmd="ip a")
            if virtual_ip in out:
                return nfs

    def get_active_nfs_server(self, client, nfs_cluster, ceph_cluster):
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph nfs cluster info {nfs_cluster} -f json"
        )
        output = json.loads(out)
        log.info(output)
        backend_servers = [
            server["hostname"] for server in output[nfs_cluster]["backend"]
        ]
        ceph_nodes = ceph_cluster.get_ceph_objects(role="nfs")
        server_list = []
        for node in ceph_nodes:
            if node.node.hostname in backend_servers:
                log.info(f"{node.node.hostname} is added to server list")
                server_list.append(node)
        return server_list

    def get_daemon_status(self, client, daemon_name):
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph orch ps --daemon-type={daemon_name} --format json"
        )
        daemon_ls = json.loads(out)
        log.info(daemon_ls)
        return daemon_ls

    def write_io(self, client1, mount_dir, timeout=600):
        dir_name = "smallfile_dir"
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        while datetime.datetime.now() < end_time:
            for i, mount in enumerate(mount_dir):
                log.info(f"Iteration : {i}")
                out, rc = client1.exec_command(
                    sudo=True,
                    cmd=f"dd if=/dev/zero of={mount}{dir_name}_{i}/test_{i}.txt bs=100M "
                    "count=100",
                    timeout=timeout + 300,
                )
                log.info(out)

    def nfs_mount_and_io(self, nfs_client, nfs_server, export_name, mount_path):
        """
        To perform nfs mount and run smallfile IO

        Args:
            nfs_client: client where volume is mounted
            nfs_server: nfs server name
            export_name : export to be mounted
            mount path : mount path
        Returns:
            None
        Raises:
            AssertionError
        """
        dirname_suffix = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(2))
        )
        dir_name = f"dir_{dirname_suffix}"
        nfs_client.exec_command(sudo=True, cmd=f"mkdir -p {mount_path}")
        assert self.wait_for_cmd_to_succeed(
            nfs_client,
            cmd=f"mount -t nfs -o port=2049 {nfs_server}:{export_name} {mount_path}",
        )
        out, rc = nfs_client.exec_command(cmd="mount")
        mount_output = out.split()
        log.info("Checking if nfs mount is is passed of failed:")
        assert mount_path.rstrip("/") in mount_output

        log.info("Creating Directory")
        out, rc = nfs_client.exec_command(
            sudo=True, cmd=f"mkdir {mount_path}/{dir_name}"
        )
        # Run IO on existing exports
        nfs_client.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
            f"--files 10 --files-per-dir 10 --dirs-per-dir 2 --top "
            f"{mount_path}/{dir_name}",
            long_running=True,
        )

    @retry(CommandFailed, tries=3, delay=60)
    def cephfs_nfs_mount_ingress(
        self,
        client,
        nfs_version,
        virtual_ip,
        psuedo_path,
        nfs_mounting_dir,
        port,
        **kwargs,
    ):
        """
        Mount cephfs nfs export
        :param client:
        :param nfs_version:
        :param virtual_ip:
        :param psuedo_path:
        :param mounting_dir:
        :param port:
        """
        client.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
        command = f"mount -t nfs -o port={port},nfsvers={nfs_version} {virtual_ip}:{psuedo_path} {nfs_mounting_dir}"
        client.exec_command(sudo=True, cmd=command, check_ec=False)

    def mount_ceph(self, mnt_type, mount_params):
        """
        To perform cephfs mount of path "/" with specified mount type
        Args:
            mnt_type: one of mount types- nfs, kernel or fuse
            mount_params : a dict type input for mount details including,
            fs_util : a fs_util testlib object created in test script
            client : client to perform mount
            fs_name : a cephfs volume name
            nfs_name : a nfs cluster name for nfs mount type
            nfs_export_name : an export name to be created for nfs mount
            Ex:
              path : /mnt/cephfs_kernel/ for snap-schedule path "/"
              sched_val : 1M , for a snapshot every 1 minute on path
        Returns: mount_path,export_created
                 export_created : 1, if export was created in module else 0
        """
        client = mount_params["client"]
        fs_vol_path = "/"
        mount_suffix = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        fs_util = mount_params["fs_util"]

        if mnt_type == "kernel":
            mounting_dir = f"/mnt/cephfs_kernel_{mount_suffix}/"
            client.exec_command(sudo=True, cmd=f"mkdir -p {mounting_dir}")
            mon_node_ips = fs_util.get_mon_node_ips()
            retry_mount = retry(CommandFailed, tries=3, delay=30)(fs_util.kernel_mount)
            retry_mount(
                [client],
                mounting_dir,
                ",".join(mon_node_ips),
                sub_dir=f"{fs_vol_path}",
                extra_params=f",fs={mount_params['fs_name']}",
            )
        if mnt_type == "fuse":
            mounting_dir = f"/mnt/cephfs_fuse_{mount_suffix}/"
            client.exec_command(sudo=True, cmd=f"mkdir -p {mounting_dir}")
            fs_util.fuse_mount(
                [client],
                mounting_dir,
                extra_params=f" -r {fs_vol_path} --client_fs {mount_params['fs_name']}",
            )
        if mnt_type == "nfs":
            mounting_dir = f"/mnt/cephfs_nfs_{mount_suffix}/"
            if mount_params["export_created"] == 0:
                client.exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export create cephfs {mount_params['nfs_name']} "
                    f"{mount_params['nfs_export_name']} {mount_params['fs_name']} path={fs_vol_path}",
                )
                mount_params["export_created"] = 1
            fs_util.cephfs_nfs_mount(
                client,
                mount_params["nfs_server"],
                mount_params["nfs_export_name"],
                mounting_dir,
            )
        return mounting_dir, mount_params["export_created"]
