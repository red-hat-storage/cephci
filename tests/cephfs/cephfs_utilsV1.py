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
import subprocess
import time
import traceback
from json import JSONDecodeError
from threading import Thread
from time import sleep

import paramiko

from ceph.ceph import CommandFailed, SSHConnectionManager
from ceph.parallel import parallel
from ceph.utils import check_ceph_healthly
from cli.cephadm.cephadm import CephAdm
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
    def __init__(self, ceph_cluster, **kwargs):
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
        self.clients = ceph_cluster.get_ceph_objects("client")
        self.installer = ceph_cluster.get_nodes(role="installer")[0]
        self.test_data = kwargs.get("test_data")
        enable_log = (
            FsUtils.get_custom_config_value(self.test_data, "enable_log")
            if self.test_data
            else False
        )
        if enable_log:
            if isinstance(enable_log, dict):
                self.enable_logs(client=self.clients[0], daemons_value=enable_log)
            else:
                self.enable_logs(
                    client=self.clients[0]
                )  # daemons_value will use the default value

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
                "postgresql postgresql-server postgresql-contrib",
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
            if "iozone" not in out:
                cmd = "cd /home/cephuser;wget http://www.iozone.org/src/current/iozone3_506.tar;"
                cmd += "tar xvf iozone3_506.tar;cd iozone3_506/src/current/;make;make linux"
                client.node.exec_command(cmd=cmd)
            out, rc = client.node.exec_command(
                sudo=True, cmd="rpm -qa | grep -w 'dbench'", check_ec=False
            )
            if "dbench" not in out:
                log.info("Installing dbench")
                client.node.exec_command(
                    sudo=True,
                    cmd=(
                        "rhel_version=$(rpm -E %rhel) && "
                        "dnf config-manager --add-repo="
                        "https://download.fedoraproject.org/pub/epel/${rhel_version}/Everything/x86_64/"
                    ),
                )
                client.node.exec_command(
                    sudo=True,
                    cmd="dnf install dbench -y --nogpgcheck",
                )
        if (
            hasattr(clients[0].node, "vm_node")
            and clients[0].node.vm_node.node_type == "baremetal"
        ):
            cmd = "dnf clean all;dnf -y install ceph-common --nogpgcheck;dnf -y update ceph-common --nogpgcheck"
            for client in clients:
                client.exec_command(sudo=True, cmd=cmd)

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

    def get_pool_num(self, client, pool_name):
        # Execute the command to get the pool list in JSON format
        out, rc = client.exec_command(sudo=True, cmd="ceph osd lspools --format json")

        # Parse the JSON output
        pools = json.loads(out)

        # Search for the pool name and return the pool number
        for pool in pools:
            if pool["poolname"] == pool_name:
                return pool["poolnum"]
        return None

    def get_mds_nodes(self, client, fs_name="cephfs"):
        """
        Gets the mds node objects based on the filesystem name provided
        Args:
            client:
            fs_name:

        Returns:
            List of MDS node objects
        """
        out, rc = client.exec_command(sudo=True, cmd="ceph orch ps -f json")
        output = json.loads(out)
        deamon_dict = self.filter_daemons(output, "mds", fs_name)

        mds_nodes = []
        for node in deamon_dict:
            for hostname, deamon in node.items():
                node = self.ceph_cluster.get_node_by_hostname(hostname)
            mds_nodes.append(node)
        mds_nodes_hostnames = [i.hostname for i in mds_nodes]
        ceph_nodes = self.ceph_cluster.get_ceph_objects()
        server_list = []

        for node in ceph_nodes:
            if node.node.hostname in mds_nodes_hostnames:
                log.info(f"{node.node.hostname} is added to server list")
                server_list.append(node)
        seen_hosts = set()

        # Filter the list based on the surname attribute
        filtered_list = [
            mds_deamon
            for mds_deamon in server_list
            if mds_deamon.node.hostname not in seen_hosts
            and not seen_hosts.add(mds_deamon.node.hostname)
        ]

        return list(set(filtered_list))

    def get_fsid(self, client):
        """
        Returns the FSID of the cluster
        Args:
            client:

        Returns:

        """
        out, rc = client.exec_command(sudo=True, cmd="ceph fsid -f json")
        output = json.loads(out)
        return output.get("fsid", "")

    def enable_mds_logs(self, client, fs_name="cephfs", validate=True):
        """

        Args:
            fs_name:

        Returns:

        """
        out, rc = client.exec_command(sudo=True, cmd="ceph orch ps -f json")
        output = json.loads(out)
        daemon_dict = self.filter_daemons(output, "mds", fs_name)
        set_log_dict = {"log_to_file": "true", "debug_mds": "5", "debug_ms": "1"}
        for mds_nodes in daemon_dict:
            for hostname, daemon in mds_nodes.items():
                node = self.ceph_cluster.get_node_by_hostname(hostname)
                for k, v in set_log_dict.items():
                    node.exec_command(
                        sudo=True,
                        cmd=f"cephadm shell -- ceph daemon {daemon} config set {k} {v}",
                    )
            if validate:
                for k, v in set_log_dict.items():
                    out, rc = node.exec_command(
                        sudo=True,
                        cmd=f"cephadm shell -- ceph daemon {daemon} config get {k}",
                    )
                    if v not in out:
                        log.error("Unable to set the debug logs")
                        raise CommandFailed(f"Unable to set the debug logs : {out}")

    def disable_mds_logs(self, client, fs_name="cephfs", validate=True):
        """
        Disable MDS logs for all MDS nodes in the cluster.

        Args:
            client: The client node to execute commands.
            fs_name: The name of the file system (default is "cephfs").
            validate: Whether to validate the log configuration (default is True).

        Returns:
            None

        Raises:
            CommandFailed: If unable to unset the debug logs.
        """
        out, rc = client.exec_command(sudo=True, cmd="ceph orch ps -f json")
        output = json.loads(out)
        daemon_dict = self.filter_daemons(output, "mds", fs_name)
        unset_log_keys = ["log_to_file", "log_file"]
        set_log_dict = {
            "log_to_file": "false",
            "debug_mds": "1",
            "debug_ms": "0",
        }
        for mds_nodes in daemon_dict:
            for hostname, daemon in mds_nodes.items():
                node = self.ceph_cluster.get_node_by_hostname(hostname)
                # Unset existing log configurations
                for key in unset_log_keys:
                    try:
                        out, rc = node.exec_command(
                            sudo=True,
                            cmd=f"cephadm shell -- ceph daemon {daemon} config unset {key}",
                        )
                        log.info(f"Unset {key} on {daemon}: {out}")
                    except Exception as e:
                        log.error(f"Failed to unset {key} on {daemon}: {e}")
                        raise CommandFailed(f"Failed to unset {key} on {daemon}: {e}")

                for k, v in set_log_dict.items():
                    node.exec_command(
                        sudo=True,
                        cmd=f"cephadm shell -- ceph daemon {daemon} config set {k} {v}",
                    )

                if validate:
                    for k, v in set_log_dict.items():
                        out, rc = node.exec_command(
                            sudo=True,
                            cmd=f"cephadm shell -- ceph daemon {daemon} config get {k}",
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
            return kwargs.get("service_name")
        else:
            service_deamon = FsUtils.deamon_name(node, service)
            node.exec_command(sudo=True, cmd=f"systemctl {op} {service_deamon}")
        return service_deamon

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
        grep_pid_cmd = f"""sudo ceph tell mds.{rank} client ls | grep '"pid":'"""
        out, rc = client_node.exec_command(cmd=grep_pid_cmd)
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
        grep_cmd = f""" sudo ceph tell mds.{rank} client ls | grep '"id":'"""
        out, rc = client_node.exec_command(cmd=grep_cmd)
        client_ids = re.findall(r"\d+", out)
        grep_cmd = f""" sudo ceph tell mds.{rank} client ls | grep '"inst":'"""
        log.info("Getting IP address of Evicted client")
        out, rc = client_node.exec_command(cmd=grep_cmd)
        op = re.findall(r"\d+.+\d+.", out)
        ip_add = op[0]
        ip_add = ip_add.split(" ")
        ip_add = ip_add[1].strip('",')
        for client_id in client_ids:
            client_node.exec_command(
                cmd=f"sudo ceph tell mds.{rank} client evict id={client_id}"
            )
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

    @retry(CommandFailed, tries=3, delay=10)
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
                cmd=f"cephadm shell -- ceph --admin-daemon /var/run/ceph/ceph-mds.{service_name}.asok"
                " config set mds_session_blacklist_on_evict true",
            )
            return 0
        else:
            active_mds.exec_command(
                sudo=True,
                cmd=f"cephadm shell -- ceph --admin-daemon /var/run/ceph/ceph-mds.{service_name}.asok "
                "config set mds_session_blacklist_on_evict false",
            )
            clients = self.ceph_cluster.get_ceph_objects("client")
            ip_add = self.manual_evict(clients[0], rank)
            out, rc = clients[0].exec_command(sudo=True, cmd="ceph osd blacklist ls")
            log.info(out)
            log.info(f"{ip_add} which is evicated manually")
            if ip_add not in out:
                return 0
            else:
                raise CommandFailed(
                    f"{ip_add} which is evicated manually not yet blocklisted . {out}"
                )

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

    def get_fs_info_dump(self, client, fs_name, **kwargs):
        """
        Dumps output of fs info command.
        Command: ceph fs volume info
        Args:
            fs_name : Name of the fs volume for which we need the status
            human_readable: If the output needs to be human readble set it to True
        Return:
            returns ceph fs volume info dump in json format
        """
        fs_info_cmd = f"ceph fs volume info {fs_name} --format json"
        if kwargs.get("human_readable"):
            fs_info_cmd += " --human-readable"

        out, _ = client.exec_command(sudo=True, cmd=fs_info_cmd)
        fs_info = json.loads(out)
        return fs_info

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
            recreate : if passed as false then will not create user if already exists or
                any kwargs like mds, osd are passed then it will create irrespective of this filed
        Returns:

        """
        fs_info = self.get_fs_info(clients[0], fs_name=kwargs.get("fs_name", "cephfs"))
        for client in clients:
            log.info("Giving required permissions for clients:")
            out, rc = client.exec_command(
                sudo=True,
                cmd=f"ceph auth get client.{client.node.hostname}",
                check_ec=False,
            )
            if not rc and kwargs.get("recreate", True):
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
                if kwargs.get("recreate", True):
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
            rc = client.exec_command(sudo=True, cmd=fuse_cmd, long_running=True)
            if rc:
                raise CommandFailed(f"Ceph Fuse Command failed with error: {rc}")
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

    @retry(CommandFailed, tries=3, delay=60)
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
                f"mount -t ceph {mon_node_ip}:{kwargs.get('sub_dir', '/')} {mount_point} "
                f"-o name={kwargs.get('new_client_hostname', client.node.hostname)},"
                f"secretfile=/etc/ceph/{kwargs.get('new_client_hostname', client.node.hostname)}.secret"
            )

            if kwargs.get("extra_params"):
                kernel_cmd += f"{kwargs.get('extra_params')}"

            cmd_rc = client.exec_command(sudo=True, cmd=kernel_cmd, long_running=True)

            log.info("validate kernel mount:")
            if kwargs.get("validate", True):
                if cmd_rc:
                    raise CommandFailed(
                        f"Ceph Kernel Command failed with error: {cmd_rc}"
                    )
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

    def create_osd_pool(
        self,
        client,
        pool_name,
        pg_num=None,
        pgp_num=None,
        erasure=False,
        validate=True,
        **kwargs,
    ):
        """
        Creates an OSD pool with given arguments.
        It supports the following optional arguments:
        Args:
            client:
            pool_name:
            pg_num: int
            pgp_num: int
            erasure: bool
            validate: bool
            **kwargs:
                erasure_code_profile: str
                crush_rule_name: str
                expected_num_objects: int
                autoscale_mode: str (on, off, warn)
                check_ec: bool (default: True)
        Returns:
            Returns the cmd_out and cmd_rc for the create command.
        """
        if erasure:
            pool_cmd = f"ceph osd pool create {pool_name} {pg_num or ''} {pgp_num or ''} erasure"
            if kwargs.get("erasure_code_profile"):
                pool_cmd += f" {kwargs.get('erasure_code_profile')}"
        else:
            pool_cmd = f"ceph osd pool create {pool_name} {pg_num or ''} {pgp_num or ''} replicated"

        if kwargs.get("crush_rule_name"):
            pool_cmd += f" {kwargs.get('crush_rule_name')}"
        if kwargs.get("expected_num_objects"):
            pool_cmd += f" {kwargs.get('expected_num_objects')}"
        if erasure and kwargs.get("autoscale_mode"):
            pool_cmd += f" --autoscale-mode={kwargs.get('autoscale_mode')}"

        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=pool_cmd, check_ec=kwargs.get("check_ec", True)
        )

        if validate:
            out, rc = client.exec_command(
                sudo=True, cmd="ceph osd pool ls --format json"
            )
            pool_ls = json.loads(out)
            if pool_name not in pool_ls:
                raise CommandFailed(f"Creation of OSD pool: {pool_name} failed")

        return cmd_out, cmd_rc

    @staticmethod
    def str_to_bool(value):
        """
        Convert a string representation of a boolean to an actual boolean.

        Args:
            value (str): The string representation of the boolean value.

        Returns:
            bool: The corresponding boolean value.

        Raises:
            ValueError: If the string does not represent a boolean value.
        """
        if isinstance(value, str):
            value = value.strip().lower()
            if value in {"true", "1", "t", "yes", "y"}:
                return True
            elif value in {"false", "0", "f", "no", "n"}:
                return False
        raise ValueError(f"Cannot convert {value} to bool")

    @staticmethod
    def parse_value(value):
        """
        Parse the value to return it in the appropriate format.

        Args:
            value (str): The string value to parse.

        Returns:
            bool, dict, list, str: The parsed value, which could be a boolean,
                                   a dictionary, a list, or a single string.
        """
        # Check if the value should be treated as a boolean
        try:
            return FsUtils.str_to_bool(value)
        except ValueError:
            pass

        # Check if the value can be parsed as a dictionary
        if value.startswith("{") and value.endswith("}"):
            try:
                # Attempt to convert the string to a dictionary
                return eval(value)
            except (SyntaxError, ValueError):
                raise ValueError(f"Cannot convert {value} to dict")

        # Check if the value is a comma-separated list
        if "," in value:
            return [item.strip() for item in value.split(",")]

        # Return as a single string if none of the above
        return value

    @staticmethod
    def get_custom_config_value(test_data, key_name):
        """
        Retrieve the custom configuration value based on the specified key name.

        Args:
            test_data (dict): The test data containing configuration.
            key_name (str): The key to look for in the configuration.

        Returns:
            bool, dict, list, str: The configuration value in the required format.
        """
        if "custom-config" in test_data and isinstance(
            test_data["custom-config"], list
        ):
            for config in test_data["custom-config"]:
                key, value = config.split("=")
                if key == key_name:
                    return FsUtils.parse_value(value.strip())
        return False

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

        erasure = (
            FsUtils.get_custom_config_value(self.test_data, "erasure")
            if self.test_data
            else False
        )
        if erasure:
            self.create_osd_pool(
                client, f"cephfs.{vol_name}.data-ec", pg_num=64, erasure=True
            )
            self.create_osd_pool(
                client, f"cephfs.{vol_name}.meta", pg_num=64, erasure=False
            )
            client.exec_command(
                sudo=True,
                cmd=f"ceph osd pool set cephfs.{vol_name}.data-ec allow_ec_overwrites true",
            )
            fs_cmd = f"ceph fs new {vol_name} cephfs.{vol_name}.meta cephfs.{vol_name}.data-ec --force"
        else:
            fs_cmd = f"ceph fs volume create {vol_name}"
            if kwargs.get("placement"):
                fs_cmd += f" --placement='{kwargs.get('placement')}'"

        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=fs_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if kwargs.get("check_ec", True) and validate:
            out, rc = client.exec_command(sudo=True, cmd="ceph fs ls --format json")
            volname_ls = json.loads(out)
            if vol_name not in [i["name"] for i in volname_ls]:
                raise CommandFailed(f"Creation of filesystem: {vol_name} failed")

        log.info("Validating the creation of pools")
        outpools, rcpools = client.exec_command(
            sudo=True,
            cmd=f"ceph osd lspools | grep -E 'cephfs.{vol_name}.data|cephfs.{vol_name}.meta' | wc -l",
        )
        if outpools.strip() != "2":
            raise CommandFailed(
                f"Creation of pools: {vol_name} failed. Actual Output: {outpools}"
            )
        log.info(f"Creation of pools for {vol_name} is successful")

        apply_cmd = f"ceph orch apply mds {vol_name}"
        if erasure and validate:
            if kwargs.get("placement"):
                apply_cmd += f" --placement='{kwargs.get('placement')}'"
            client.exec_command(
                sudo=True,
                cmd=f"{apply_cmd}",
            )
            time.sleep(20)
            retry_mds_status = retry(CommandFailed, tries=3, delay=30)(
                self.get_mds_status
            )
            retry_mds_status(
                client,
                1,
                vol_name=vol_name,
                expected_status="active",
            )
            # self.wait_for_mds_process(client,process_name=vol_name)

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
                earmark : starts with smd or nfs
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
        if kwargs.get("earmark"):
            subvolume_cmd += f" --earmark {kwargs.get('earmark')}"
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
        delete_mon_allow_cmd = "ceph config set mon mon_allow_pool_delete true"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=delete_mon_allow_cmd, check_ec=kwargs.get("check_ec", True)
        )
        rmvolume_cmd = f"ceph fs volume rm {vol_name} --yes-i-really-mean-it"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=rmvolume_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            out, rc = client.exec_command(sudo=True, cmd="ceph fs ls --format json")
            volname_ls = json.loads(out)
            if vol_name in [i["name"] for i in volname_ls]:
                raise CommandFailed(f"Creation of filesystem: {vol_name} failed")

            log.info("Validating the deletion of pools")
            outpools, rcpools = client.exec_command(
                sudo=True,
                cmd=f"ceph osd lspools | grep -E 'cephfs.{vol_name}.data|cephfs.{vol_name}.meta' | wc -l",
            )
            if outpools.strip() != "0":
                raise CommandFailed(
                    f"Deletion of pools: {vol_name} failed. Actual Output: {outpools}"
                )
            log.info(f"Deletion of pools for {vol_name} is successful")

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

    def activate_multiple_mdss(self, clients, fs_name="cephfs"):
        """
        Activate Multiple MDS for ceph filesystem
        Args:
            clients: Client_nodes
        """
        for client in clients:
            fs_info = self.get_fs_info(client, fs_name)
            fs_name = fs_info.get("fs_name")
            log.info("Activating Multiple MDSs:")
            client.exec_command(cmd="ceph -v | awk {'print $3'}")
            command = f"ceph fs set {fs_name} max_mds 2"
            client.exec_command(sudo=True, cmd=command)
            self.check_active_mds_count(client, fs_name, 2)
            return 0

    @retry(CommandFailed, tries=3, delay=60)
    def check_active_mds_count(self, client, fs_name, expected_active_count):
        # Parse the JSON string

        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph fs status {fs_name} --format json"
        )
        # Extract the MDS map
        data = json.loads(out)
        mdsmap = data.get("mdsmap", [])

        # Count the number of active MDS entries
        active_count = sum(1 for mds in mdsmap if mds.get("state") == "active")
        # Check if the count matches the expected value
        if active_count == expected_active_count:
            return True
        else:
            raise CommandFailed(
                f"MDS count {active_count} is not matching the expected {expected_active_count}"
            )

    def mds_cleanup(self, nodes, dir_fragmentation, **kwargs):
        """
        Deactivating multiple mds activated, by setting it to single mds server
        Args:
            nodes: Client_nodes
            dir_fragmentation: fragmentation directory
        """
        log.info("Deactivating Multiple MDSs")
        for node in nodes:
            fs_name = kwargs.get("fs_name", "cephfs")
            fs_info = self.get_fs_info(node, fs_name)
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
        node.exec_command(sudo=True, cmd="yum install -y python3 --nogpgcheck")
        node.exec_command(sudo=True, cmd="python3 /home/cephuser/nw_disconnect.py")
        log.info("Starting the network..")
        return 0

    def network_disconnect_v1(self, node, sleep_time=60):
        """
        Disconnects the network on a specified node using a custom Bash script.
        Args:
            node:
            sleep_time:

        Returns:

        """
        file = "network_disconnect.sh"
        node.upload_file(
            sudo=True,
            src="tests/cephfs/cephfs_bugs/network_disconnect.sh",
            dst=f"/root/{file}",
        )
        node.exec_command(
            sudo=True,
            cmd=f"bash /root/{file} {node.node.ip_address} {sleep_time}",
        )

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

    def set_xattrs(
        self,
        client,
        directory,
        key,
        value,
    ):
        set_cmd = f"setfattr -n {key} -v {value}"
        client.exec_command(sudo=True, cmd=f"{set_cmd} {directory}")

    def get_xattrs(self, client, directory, key):
        get_cmd = f"getfattr --only-values -n {key} {directory}"
        out, rc = client.exec_command(sudo=True, cmd=f"{get_cmd}")

        return out, rc

    def rm_xattrs(
        self,
        client,
        directory,
        key,
    ):
        set_cmd = f"setfattr -x {key}"
        client.exec_command(sudo=True, cmd=f"{set_cmd} {directory}")

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
                [
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in range(4)
                ]
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
            io_params = {"testdir_prefix": "file_extract_dir", "compile_test": 0}
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
                cmd=f"cd {io_path};wget -O linux.tar.xz http://download.ceph.com/qa/linux-6.5.11.tar.xz",
            )
            client.exec_command(
                sudo=True,
                cmd=f"cd {io_path};tar -xJf linux.tar.xz",
                long_running=True,
                timeout=3600,
            )
            log.info(f"untar suceeded on {mounting_dir}")
            if io_params["compile_test"] == 1:
                log.info("Install dependent packages")
                cmd = "yum install -y --nogpgcheck flex bison bc elfutils-libelf-devel openssl-devel"
                client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
                out, rc = client.exec_command(
                    sudo=True,
                    cmd="grep -c processor /proc/cpuinfo",
                )
                cpu_cnt = out.strip()
                log.info(f"cpu_cnt:{cpu_cnt},out:{out}")
                cmd = f"cd {io_path}/; cd linux-6.5.11;make defconfig;make -j {cpu_cnt}"
                client.exec_command(sudo=True, cmd=cmd, timeout=3600)
                log.info(f"Compile test passed on {mounting_dir}")

            # cleanup
            client.exec_command(
                sudo=True,
                cmd=f"rm -rf  {io_path}",
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

        def dbench():
            log.info("IO tool scheduled : dbench")
            io_params = {
                "clients": random.choice(
                    range(8, 33, 8)  # Randomly selects 8, 16, 24, or 32 for clients
                ),
                "duration": random.choice(
                    range(60, 601, 60)
                ),  # Randomly selects 60, 120, ..., 600 seconds
                "testdir_prefix": "dbench_io_dir",
            }
            if kwargs.get("dbench_params"):
                dbench_params = kwargs.get("dbench_params")
                for io_param in io_params:
                    if dbench_params.get(io_param):
                        io_params[io_param] = dbench_params[io_param]

            dir_suffix = "".join(
                [
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in range(4)
                ]
            )
            io_path = f"{mounting_dir}/{io_params['testdir_prefix']}_{dir_suffix}"
            client.exec_command(sudo=True, cmd=f"mkdir {io_path}")

            client.exec_command(
                sudo=True,
                cmd=f"dbench {io_params['clients']} -t {io_params['duration']} -D {io_path}",
                long_running=True,
            )

        def postgresIO():
            log.info("IO tool scheduled : PostgresIO")

            io_params = {
                "scale": random.choice(
                    range(
                        40, 101, 10
                    )  # The size of the database(test data) increases linearly with the scale factor
                ),
                "workers": random.choice(range(4, 17, 4)),
                "clients": random.choice(
                    range(8, 56, 8)  # Randomly selects 8, 16, 24, 32,.. for clients
                ),
                "duration": random.choice(
                    range(60, 601, 60)
                ),  # Randomly selects 60, 120, ..., 600 seconds
                "testdir_prefix": "postgres_io_dir",
                "db_name": "",
            }

            if kwargs.get("postgresIO_params"):
                postgresIO_params = kwargs.get("postgresIO_params")
                for io_param in io_params:
                    if postgresIO_params.get(io_param):
                        io_params[io_param] = postgresIO_params[io_param]

            dir_suffix = "".join(
                [
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in range(4)
                ]
            )
            io_path = f"{mounting_dir}/{io_params['testdir_prefix']}_{dir_suffix}"
            client.exec_command(sudo=True, cmd=f"mkdir {io_path}")

            # Initialize mode
            client.exec_command(
                sudo=True,
                cmd=f"pgbench -i --scale={io_params['scale']} -U postgres -d {io_params['db_name']}",
                long_running=True,
            )

            # Creating tables and populating data based on the scale factor
            client.exec_command(
                sudo=True,
                cmd=(
                    f"pgbench -c {io_params['clients']} "
                    f"-j {io_params['workers']} "
                    f"-T {io_params['duration']} "
                    f"-U postgres -d {io_params['db_name']}"
                ),
                long_running=True,
            )

        io_tool_map = {
            "dd": dd,
            "smallfile": smallfile,
            "wget": wget,
            "file_extract": file_extract,
            "dbench": dbench,
            "postgresIO": postgresIO,
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
        command = f"mount -t nfs -o port={kwargs.get('port', '2049')} {nfs_server}:{nfs_export} {nfs_mount_dir}"
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
                f"port={kwargs.get('port', '2049')},"
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

    def setup_postgresql_IO(self, client, mount_dir, db_name):
        """
        Setup Steps:
        1. Create the PostgreSQL data directory as Mount dir
        2. Set permissions and owners for Mount dir
        3. Initialise the Postgres Service
        4. Create DB
        """

        log.info("Stopping PostgresSQL")
        client.exec_command(sudo=True, cmd="systemctl stop postgresql", check_ec=False)

        log.info("Setting up PostgresSQL")
        client.exec_command(sudo=True, cmd=f"mkdir -p {mount_dir}")

        log.debug(f"Setting up postgres persmission and user for the dir {mount_dir}")
        client.exec_command(sudo=True, cmd=f"chown -R postgres:postgres {mount_dir}")
        client.exec_command(sudo=True, cmd=f"chmod 700 {mount_dir}")

        try:
            log.debug("Initialise the Postgres Service")
            client.exec_command(
                sudo=True, cmd=f"sudo -u postgres /usr/bin/initdb -D {mount_dir}"
            )
        except Exception as e:
            log.info(
                f"Initialising the Postgres Service failed: {e}. Applying the Recovery.."
            )
            client.exec_command(sudo=True, cmd=f"rm -rf {mount_dir}/*")
            client.exec_command(
                sudo=True, cmd=f"sudo -u postgres /usr/bin/initdb -D {mount_dir}"
            )

        config_file = "/usr/lib/systemd/system/postgresql.service"
        env_var = f"Environment=PGDATA={mount_dir}"

        # Updates the postgres config to point to the correct backend dir
        update_dir_command = (
            f"sudo sed -i '/^Environment=PGDATA/c\\{env_var}' {config_file} || "
            f"echo '{env_var}' | sudo tee -a {config_file} > /dev/null"
        )
        client.exec_command(sudo=True, cmd=update_dir_command)

        client.exec_command(sudo=True, cmd="systemctl daemon-reload")

        client.exec_command(sudo=True, cmd="sestatus")
        client.exec_command(sudo=True, cmd="setenforce 0")
        client.exec_command(sudo=True, cmd="systemctl restart postgresql")
        client.exec_command(sudo=True, cmd=f"chcon -R -t postgresql_db_t {mount_dir}")

        client.exec_command(sudo=True, cmd="setenforce 1")
        client.exec_command(sudo=True, cmd="systemctl restart postgresql")

        out, _ = client.exec_command(sudo=True, cmd="systemctl status postgresql")
        log.debug(out)
        if "active (running)" in out:
            log.info("PostgreSQL is running")
        else:
            log.error("PostgreSQL is not running")

        log.info(f"Creating the DB - {db_name}")
        client.exec_command(
            sudo=True, cmd=f"sudo -u postgres psql -c 'CREATE DATABASE {db_name};'"
        )
        log.info("DB creation is successful")

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

    @retry(CommandFailed, tries=3, delay=60)
    def validate_fs_services(self, client, service_name, is_present=True):
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
        try:
            service_ls = json.loads(out)
            log.info(service_ls)
            if is_present:
                if (
                    service_ls[0]["status"]["running"]
                    != service_ls[0]["status"]["size"]
                ):
                    raise CommandFailed(f"All {service_name} are Not UP")
                return True
            else:
                raise CommandFailed(f"All {service_name} are still UP")
        except JSONDecodeError:
            if "No services reported" not in out:
                raise CommandFailed(f"All Services are not down.. {out}")
        return True

    @retry(CommandFailed, tries=3, delay=60)
    def validate_services_placements(self, client, service_name, placement_list):
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph orch ls --service_name={service_name} --format json"
        )
        try:
            service_ls = json.loads(out)
        except JSONDecodeError:
            raise CommandFailed("No services are UP")
        log.info(service_ls)
        if service_ls[0]["status"]["running"] != service_ls[0]["status"]["size"]:
            raise CommandFailed(f"All {service_name} are Not UP")
        if service_ls[0]["placement"]["hosts"] != placement_list:
            raise CommandFailed(
                f"Services did not come up on the hosts: {placement_list} which "
                f"has been specifed but came up on {service_ls[0]['placement']['hosts']}"
            )

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

    @retry(CommandFailed, tries=4, delay=30)
    def get_ceph_health_status(self, client, validate=True, status=["HEALTH_OK"]):
        """
        Validate if the Ceph Health is OK or in WARN/ERROR State.
        Args:
            client : client node.
        Return:
            Status of the Ceph Health.
        """
        out, rc = client.exec_command(sudo=True, cmd="ceph -s -f json")
        log.info(out)
        health_status = json.loads(out)["health"]["status"]

        if health_status not in status:
            raise CommandFailed(f"Ceph Cluster is in {health_status} State")
        log.info("Ceph Cluster is Healthy")
        return health_status

    def monitor_ceph_health(self, client, retry, interval):
        """
        Monitors Ceph health and prints ceph status at regular intervals
        Args:
            client : client node.
            retry  : Number of times to retry the command
            intervals: Time duration between the retries (in seconds)
        Return:
            Prints Status of the Ceph Health.
        """
        for i in range(1, retry + 1):
            log.info(f"Running health status: Iteration: {i}")
            self.get_ceph_health_status(client)
            fs_status_info = self.get_fs_status_dump(client)
            log.info(f"FS Status: {fs_status_info}")
            time.sleep(interval)

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
        fs_vol_path = mount_params.get("mnt_path", "/")
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

    def config_set_runtime(self, client, service_name, key, value):
        """
        Set a runtime configuration for a Ceph service.

        Args:
            client (Client): The client object used to execute commands.
            service_name (str): The name of the Ceph service to configure.
            key (str): The configuration key to set.
            value (str): The value to set for the configuration key.

        Returns:
            int: 0 if the configuration is successfully set, 1 otherwise.
        """
        cmd_set = f"ceph config set {service_name} {key} {value}"
        cmd_get = f"ceph config get {service_name} {key}"

        client.exec_command(sudo=True, cmd=cmd_set)
        out, rc = client.exec_command(sudo=True, cmd=cmd_get)
        log.info(f"Value of {key} is {out}")
        if str(value) not in out.strip():
            log.error(f"Unable to set {key}")
            return 1
        return 0

    @retry(CommandFailed, tries=3, delay=30)
    def wait_for_standby_replay_mds(self, client1, fs_name):
        standby_reply_mds = self.get_standby_replay_mdss(client1, fs_name=fs_name)
        if not standby_reply_mds:
            raise CommandFailed("No Stanby Replay MDS")

    def wait_for_stable_fs(self, client, standby_replay, timeout=180, interval=5):
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        log.info("Wait for the command to pass")
        while end_time > datetime.datetime.now():
            try:
                out1, rc = client.exec_command(sudo=True, cmd="ceph fs status")
                print(out1)
                out, rc = client.exec_command(
                    sudo=True, cmd="ceph fs status | awk '{print $2}'"
                )
                output = out.splitlines()
                if (
                    "active" in output[3]
                    and "standby-replay" in output[4]
                    and standby_replay == "true"
                ):
                    return 0
                if "standby-replay" not in output[4] and standby_replay == "false":
                    return 0
                sleep(interval)
            except Exception as e:
                log.info(e)
                raise CommandFailed

    def get_crash_ls_new(self, client):
        """
        To get crash ls-new output in json format
        Args:
            client : To run ceph crash cmd
        Returns: dict type data having crash ls-new output
        """
        out, _ = client.exec_command(sudo=True, cmd="ceph crash ls-new -f json")
        crash_info = json.loads(out)
        return crash_info

    @retry(JSONDecodeError, tries=3, delay=30)
    def get_mds_metrics(self, client, rank=0, mounted_dir="", fs_name="cephfs"):
        """
        returns the metrics for the MDS rank and the client mounted directory

        Args:
            client (Client): The client object used to execute commands.
            rank (str): rank of the MDS.
            mounted_dir (str): where the client is mounted.

        Returns:
            str: The value of the metric.
        """
        ranked_mds, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs status {fs_name} -f json | jq '.mdsmap[] | select(.rank == {rank}) | .name'",
        )
        log.info(f"Executing MDS name with rank command: {ranked_mds}")
        ranked_mds = ranked_mds.replace('"', "").replace("\n", "")
        client_id_cmd = (
            f"ceph tell mds.{ranked_mds} session ls | jq '.[] | select(.client_metadata.mount_point"
            f' != null and (.client_metadata.mount_point | contains("{mounted_dir}"))) | .id\''
        )
        log.info(f"Executing Client ID Command : {client_id_cmd}")
        client_id, _ = client.exec_command(sudo=True, cmd=client_id_cmd)
        client_id = client_id.replace('"', "").replace("\n", "")
        if client_id == "":
            log.error(f"Client not found for Mounted Directory : {mounted_dir}")
            return 1
        log.info(f"Client ID :[{client_id}] for Mounted Directory : [{mounted_dir}]")
        cmd = f""" ceph tell mds.{ranked_mds} counter dump 2>/dev/null | \
            jq -r '. | to_entries | map(select(.key | match("mds_client_metrics"))) | \
            .[].value[] | select(.labels.client != null and (.labels.client | contains("{client_id}"))
            and (.labels.rank == "0"))'
            """
        metrics_out, _ = client.exec_command(sudo=True, cmd=cmd)
        log.info(
            f"Metrics for MDS : {ranked_mds} Mounted Directory: {mounted_dir} and Client : {client_id} is {metrics_out}"
        )
        if metrics_out == "":
            log.error(f"Metrics not found for MDS : {ranked_mds}")
            return 1
        metrics_out = json.loads(str(metrics_out))

        return metrics_out

    def enable_logs(
        self,
        client,
        daemons_value={"mds": 10, "mon": 5, "client": 5, "osd": 5, "mgr": 5},
        validate=True,
    ):
        """
        To Enable debug logs for daemons - mds,mgr,osd,mon,client
        Args:
            client: to run ceph cmds , type - client object
            daemons_value : to get daemons whose debug logging to be enabled, with dbg_level, in type - dict
                            example : {'mds':5,'mgr':5}
        Returns: 0 if completed succesfully, else 1.
        """
        cephadm = CephAdm(self.installer)
        cephadm.ceph.config.set(key="log_to_file", value="true", daemon="global")
        cephadm.ceph.config.set(key="debug_mds", value="5", daemon="mds")
        for k in daemons_value.keys():
            cephadm.ceph.config.set(key="log_to_file", value="true", daemon=k)
        for k, v in daemons_value.items():
            cephadm.ceph.config.set(key=f"debug_{k}", value=v, daemon=k)
            if k == "mds":
                cephadm.ceph.config.set(key="debug_ms", value=1, daemon=k)

        if validate:
            for k, v in daemons_value.items():
                out = cephadm.ceph.config.get(key="log_to_file", who=k)
                actual_value = out[0].strip() if isinstance(out, tuple) else out.strip()
                if "true" not in actual_value:
                    log.error(f"Unable to set the debug logs : {out}")
                    return 1
                out = cephadm.ceph.config.get(key=f"debug_{k}", who=k)
                actual_value = out[0].strip() if isinstance(out, tuple) else out.strip()
                if str(v) not in actual_value:
                    log.error(f"Unable to set the debug logs : {actual_value}")
                    return 1
        return 0

    def disable_logs(
        self, client, daemons=["mds", "mon", "mgr", "osd", "client"], validate=True
    ):
        """
        To Disable debug logs for daemons - mds,mgr,osd,mon,client
        Args:
            client: to run ceph cmds , type - client object
            daemons : to get daemons whose debug logging to be disabled,type - list
                      example : ['mds','mon','mgr']
        Returns: 0 if completed succesfully, else 1.
        """
        for k in daemons:
            client.exec_command(
                sudo=True,
                cmd=f"ceph config set {k} log_to_file false",
            )
        if validate:
            for k in daemons:
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=f"ceph config get {k} log_to_file",
                )
                if "false" not in out.strip():
                    log.error(f"Unable to disable the debug logs : {out}")
                    return 1
        return 0

    def upload_logs(self, client, log_dir_dst, daemons=["mds", "mgr", "osd"]):
        """
        To upload debug logs to test log directory.
        Args:
        client: to run ceph cmds , type - client object
        daemons : to get daemons whose debug logs to be uploaded,type - list
                  example : ['mds','mon','mgr']
        Returns: 0 if completed succesfully, else 1.
        """
        logs_dict = {}
        result = ""
        try:
            cmd = f'grep "> failed" {log_dir_dst}/*'
            result = subprocess.check_output(cmd, shell=True, text=True)
            log.info(result)
        except Exception as ex:
            log.info(ex)

        if "failed" in result:
            try:
                for k in daemons:
                    logs_dict.update({k: {}})
                    for node in self.ceph_cluster.get_nodes(role=k):
                        logs_dict[k].update({node.hostname: node.ip_address})
                uname = "root"
                pword = "passwd"
                fsid = self.get_fsid(client)
                node_info = {}
                for k in daemons:
                    node_info_tmp = logs_dict[k]
                    node_info.update(node_info_tmp)
                log.info(f"node_info:{node_info}")
                log.info("Collect logs in each node to tmp/<node_hostname> directory")
                for node in node_info:
                    ssh_install = SSHConnectionManager(
                        node_info[node], uname, pword
                    ).get_client()
                    ssh_install.exec_command(f"sudo mkdir -p tmp/{node}")
                    for k in daemons:
                        if node in logs_dict[k].keys():
                            ssh_install.exec_command(
                                f"sudo cp /var/log/ceph/{fsid}/*{k}* tmp/{node}/"
                            )
                            ssh_install.exec_command(
                                f"tar -czvf tmp/{node}.tar.gz tmp/{node}"
                            )

                # create dir in logdir for system logs
                log_path = f"{log_dir_dst}/cephfs-dbg-logs"
                os.mkdir(log_path)
                log.info(f"Upload logs to {log_dir_dst}/cephfs-dbg-logs")
                ssh_d = paramiko.SSHClient()
                ssh_d.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                for node in node_info:
                    nodeip = node_info[node]
                    ssh_d.connect(nodeip, username=uname, password=pword)
                    source_file = f"tmp/{node}.tar.gz"
                    dir_exist = os.path.exists(log_path)
                    if not dir_exist:
                        os.makedirs(log_path)
                    ftp_client = ssh_d.open_sftp()
                    ftp_client.get(f"{source_file}", f"{log_path}/{node}.tar.gz")
                    ftp_client.close()
                    ssh_d.exec_command(f"sudo rm -rf {source_file}")
                    ssh_d.close()
                log.info(f"Sucessfully copied debug logs to {log_path}")
                return 0
            except Exception as ex:
                log.info(ex)
                return 1
        else:
            log.info("There is no failed test, skipping debug logs upload")
            return 0

        # Ceph Client Support Functions

    def create_ceph_client(
        self,
        client,
        ceph_client_name,
        mon_caps,
        osd_caps,
        mds_caps,
        keyring=True,
        validate=True,
    ):
        """
        Creates a new client
        Args:
            client:
            ceph_client_name:
            mon_caps:
            osd_caps:
            mds_caps:
            validate:
        Example :
            create_ceph_client(client, "admin", "allow r", "allow *", "allow rwx")
        Returns:

        """
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph auth get client.{ceph_client_name}", check_ec=False
        )
        if not rc:
            log.error(f"user with name client.{ceph_client_name} Already exits")
            raise CommandFailed(
                f"user with name client.{ceph_client_name} Already exits with caps as {out}"
            )
        client_create_cmd = f"ceph auth add client.{ceph_client_name}"
        if mon_caps:
            client_create_cmd += f" mon '{mon_caps}'"
        if osd_caps:
            client_create_cmd += f" osd '{osd_caps}'"
        if mds_caps:
            client_create_cmd += f" mds '{mds_caps}'"

        if keyring:
            client_create_cmd += f" -o /etc/ceph/ceph.client.{ceph_client_name}.keyring"

        client.exec_command(
            sudo=True,
            cmd=client_create_cmd,
        )
        if validate:
            out, rc = client.exec_command(
                sudo=True, cmd=f"ceph auth get client.{ceph_client_name}"
            )
            log.info(f"User has been created successfully with below caps {out}")

        expected_caps = {
            "mon": mon_caps.strip("'"),
            "osd": osd_caps.strip("'"),
            "mds": mds_caps.strip("'"),
        }
        actual_caps = self.parse_caps(out)

        # Validate the caps
        for cap_type, expected_value in expected_caps.items():
            if actual_caps.get(cap_type) != expected_value:
                log.error(
                    f"Validation failed for {cap_type} caps: Expected {expected_value}, got {actual_caps.get(cap_type)}"
                )
                raise CommandFailed(
                    f"Validation failed for {cap_type} caps: Expected {expected_value}, got {actual_caps.get(cap_type)}"
                )

        log.info("Caps validation successful")

    def update_ceph_client(
        self, client, ceph_client_name, mon_caps, osd_caps, mds_caps, validate=True
    ):
        """
        Updates the existing client
        Args:
            client:
            ceph_client_name:
            mon_caps:
            osd_caps:
            mds_caps:
            validate:

        Returns:

        """
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph auth get client.{ceph_client_name}", check_ec=False
        )
        if rc:
            log.error(f"user with name client.{ceph_client_name} does not exits")
            raise CommandFailed(
                f"user with name client.{ceph_client_name} does not exits {out}"
            )
        client_update_cmd = f"ceph auth caps client.{ceph_client_name}"
        if mon_caps:
            client_update_cmd += f" mon '{mon_caps}'"
        if osd_caps:
            client_update_cmd += f" osd '{osd_caps}'"
        if mds_caps:
            client_update_cmd += f" mds '{mds_caps}'"

        client.exec_command(
            sudo=True,
            cmd=client_update_cmd,
        )
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph auth get client.{ceph_client_name}", check_ec=False
        )
        expected_caps = {
            "mon": mon_caps.strip("'"),
            "osd": osd_caps.strip("'"),
            "mds": mds_caps.strip("'"),
        }
        actual_caps = self.parse_caps(out)

        # Validate the caps
        for cap_type, expected_value in expected_caps.items():
            if actual_caps.get(cap_type) != expected_value:
                log.error(
                    f"Validation failed for {cap_type} caps: Expected {expected_value}, got {actual_caps.get(cap_type)}"
                )
                raise CommandFailed(
                    f"Validation failed for {cap_type} caps: Expected {expected_value}, got {actual_caps.get(cap_type)}"
                )

        log.info("Caps validation successful")

    def delete_ceph_client(self, client, ceph_client_name, validate=True):
        """
        Deletes the client
        Args:
            client:
            ceph_client_name:
            validate:

        Returns:

        """
        # Check if the client exists
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph auth get client.{ceph_client_name}", check_ec=False
        )
        if rc != 0:
            log.error(f"User with name client.{ceph_client_name} does not exist")
            raise CommandFailed(
                f"User with name client.{ceph_client_name} does not exist {out}"
            )

        # Delete the client
        client.exec_command(sudo=True, cmd=f"ceph auth del client.{ceph_client_name}")

        # Validate the deletion if required
        if validate:
            out, rc = client.exec_command(
                sudo=True,
                cmd=f"ceph auth get client.{ceph_client_name}",
                check_ec=False,
            )
            if rc == 0:
                log.error(f"User with name client.{ceph_client_name} not deleted")
                raise CommandFailed(
                    f"User with name client.{ceph_client_name} not deleted"
                )

    def parse_caps(self, output):
        caps = {}
        for line in output.splitlines():
            if line.strip().startswith("caps "):
                parts = line.strip().split(" = ")
                cap_type = parts[0].split()[-1]
                cap_value = parts[1].strip().strip('"')
                caps[cap_type] = cap_value
        return caps

    def enable_distributed_pin_on_subvolumes(
        self,
        client,
        fs_name,
        subvolumegroup_name,
        subvolume_name,
        pin_type,
        pin_setting,
    ):
        """
        Enable distributed pinning on a specified subvolume in a Ceph file system.

        Parameters:
        self: object
            The instance of the class that this method belongs to.
        client: object
            The client object used to execute commands on the Ceph cluster.
        fs_name: str
            The name of the Ceph file system.
        subvolumegroup_name: str
            The name of the subvolume group that contains the subvolume.
        subvolume_name: str
            The name of the subvolume on which to enable distributed pinning.
        pin_type: str
            The type of pinning to apply (e.g., "random" or "distributed").
        pin_setting: boolean
            The pinning setting to apply (e.g., 1, 0).

        Returns:
        None
        """
        out, rc = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume pin {fs_name} {subvolume_name} {pin_type} "
            f"{pin_setting} {subvolumegroup_name}",
        )

    def set_and_validate_mds_standby_replay(self, clients, fs_name, boolean):
        """
        Set and validate the MDS standby-replay configuration for a Ceph file system.

        This method configures the allow_standby_replay setting for the specified Ceph file system,
        and then validates that the setting has been applied correctly by examining the MDS map.

        Args:
            clients (object): The client object used to execute commands.
            fs_name (str): The name of the Ceph file system.
            boolean (bool): The desired state for the allow_standby_replay setting.

        Returns:
            dict: A dictionary containing the result of the validation, including:
                - active_mds (list): A list of tuples with active MDS information.
                - standby_replay_mds (list): A list of tuples with standby-replay MDS information.
                - standby_replay_enabled (bool): The state of the allow_standby_replay setting.
                - max_mds (int): The max_mds value from the MDS map.
                - rank_mismatch (bool): Whether there is a mismatch between active and standby-replay MDS ranks.
            If an error occurs, returns a dictionary with an "error" key.

        Raises:
            Exception: If any unexpected error occurs during the process.
        """
        try:
            # Set standby-replay
            clients.exec_command(
                sudo=True,
                cmd=f"ceph fs set {fs_name} allow_standby_replay {boolean}",
                check_ec=False,
            )

            # Retrieve the MDS map to validate
            out = clients.exec_command(
                sudo=True,
                cmd=f"ceph fs get {fs_name} -f json",
                check_ec=False,
            )

            json_data = json.loads(out[0])

            log.info(f"Get command output: {json_data}")

            mds_map = json_data.get("mdsmap", {})

            active_mds = []
            standby_replay_mds = []

            # Check if allow_standby_replay is set to the desired value
            standby_replay_enabled = mds_map.get("flags_state", {}).get(
                "allow_standby_replay", False
            )
            log.info(f"Enabled Values : {standby_replay_enabled}")
            if standby_replay_enabled != boolean:
                log.error(
                    f"Failed to validate allow_standby_replay. Expected: {boolean}, Found: {standby_replay_enabled}"
                )
                return 1
            else:
                log.info(
                    f"Found: {standby_replay_enabled} - allow_standby_replay for {fs_name} set to {boolean}"
                )

            # Get max_mds value
            max_mds = mds_map.get("max_mds", None)

            # Classify MDS by state
            for info in mds_map.get("info", {}).values():
                if "active" in info["state"]:
                    active_mds.append((info["name"], info["rank"], info["state"]))
                elif "standby-replay" in info["state"]:
                    standby_replay_mds.append(
                        (info["name"], info["rank"], info["state"])
                    )

            # Validation
            active_rank_set = {mds[1] for mds in active_mds}
            standby_replay_rank_set = {mds[1] for mds in standby_replay_mds}

            rank_mismatch = not (
                active_rank_set == standby_replay_rank_set
                and len(active_mds) == len(standby_replay_mds)
            )

            result = {
                "active_mds": active_mds,
                "standby_replay_mds": standby_replay_mds,
                "standby_replay_enabled": standby_replay_enabled,
                "max_mds": max_mds,
                "rank_mismatch": rank_mismatch,
            }

            # Log the result in JSON format
            log.info(json.dumps(result, indent=4))

            log.info(
                f"Successfully set and validated allow_standby_replay to {boolean} for {fs_name}"
            )
            return result

        except Exception as e:
            log.error(f"An unexpected error occurred: {e}")
            return {"error": "An unexpected error occurred"}

    def create_files_in_path(self, clients, path, num_of_files, batch_size):
        """
        Creates a specified number of files in the given path on the client.

        This method ensures the specified directory exists and then creates the
        desired number of files in batches to avoid overwhelming the filesystem.

        Args:
            clients (object): The client object used to execute commands.
            path (str): The directory where files will be created.
            num_of_files (int): The total number of files to create.
            batch_size (int): The number of files to create per batch to avoid overwhelming the filesystem.

        Returns:
            None

        Raises:
            Exception: If an error occurs during file creation.

        Example:
            To create 100 files in batches of 10 in the directory '/mnt/test':
            create_files_in_path(clients, '/mnt/test', 100, 10)
        """
        try:
            log.info(f"Checking if path exists: {path}")
            check_path_cmd = f"sudo test -d {path}"
            _, rc = clients.exec_command(sudo=True, cmd=check_path_cmd, check_ec=False)

            if rc != 0:
                log.info(f"Path doesn't exist, creating path: {path}")
                create_path_cmd = f"sudo mkdir -p {path}"
                clients.exec_command(sudo=True, cmd=create_path_cmd)

            log.info(f"Path exists or created successfully: {path}")
            file = "create_files.sh"
            clients.upload_file(
                sudo=True,
                src="tests/cephfs/cephfs_multi_mds/create_files.sh",
                dst=f"/root/{file}",
            )

            clients.exec_command(
                sudo=True,
                cmd=f"bash /root/{file} {path} {num_of_files} {batch_size}",
                timeout=3600,
            )
            log.info(f"Successfully created {num_of_files} files in {path}")
        except Exception as e:
            log.error(f"An error occurred: {e}")

    def get_mds_states_active_standby_replay(self, fs_name, clients):
        """
        Extracts and returns the MDS nodes and their states for the given filesystem.

        This method retrieves the MDS map for the specified Ceph filesystem and
        constructs a dictionary that maps each active MDS node to its associated
        standby-replay nodes.

        Args:
            fs_name (str): The name of the Ceph filesystem.
            clients (object): The client object used to execute commands.

        Returns:
            dict: A dictionary where keys are MDS ranks and values are dictionaries
                  containing 'active' MDS nodes and their associated 'standby-replay' nodes.

        Example:
            {
                0: {'active': 'mds.a', 'standby-replay': ['mds.b', 'mds.c']},
                1: {'active': 'mds.d', 'standby-replay': ['mds.e']}
            }
        """
        # Retrieve the MDS map to validate
        out = clients.exec_command(
            sudo=True,
            cmd=f"ceph fs get {fs_name} -f json",
            check_ec=False,
        )

        json_data = json.loads(out[0])

        mds_info = json_data.get("mdsmap", {}).get("info", {})
        mds_dict = {}

        for gid, mds in mds_info.items():
            if "active" in mds["state"]:
                rank = mds["rank"]
                active_node = mds["name"]
                mds_dict[rank] = {"active": active_node, "standby-replay": []}

        for gid, mds in mds_info.items():
            if "standby-replay" in mds["state"]:
                rank = mds["rank"]
                standby_replay_node = mds["name"]
                if rank in mds_dict:
                    mds_dict[rank]["standby-replay"].append(standby_replay_node)
        return mds_dict

    def runio_reboot_active_mds_nodes(
        self,
        fs_util,
        ceph_cluster,
        fs_name,
        clients,
        num_of_osds,
        build,
        mount_paths,
    ):
        """
        Processes the active MDS nodes by running IO operations and rebooting the active MDS nodes.

        This method performs the following steps:
        1. Identifies the active MDS nodes.
        2. Runs IO operations in parallel on the specified mount paths.
        3. Reboots the active MDS nodes one by one.
        4. Checks the cluster health before and after each reboot.
        5. Ensures the cluster remains healthy throughout the process.

        Args:
            fs_util: An instance of a filesystem utility class to run IO operations.
            ceph_cluster: An instance representing the Ceph cluster.
            fs_name (str): The name of the filesystem.
            clients (list): List of clients to execute commands.
            num_of_osds (int): Number of OSDs in the cluster.
            build (str): Build information.
            mount_paths (list): List of directories for mounts (either kernel or fuse).
            mount_type (str): Type of mount paths provided ("kernel" or "fuse").

        Returns:
            None
        """
        # Get active MDS nodes
        mds = self.get_active_mdss(clients, fs_name)
        active_mds_hostnames = [i.split(".")[1] for i in mds]
        log.info(f"Active MDS Nodes are: {active_mds_hostnames}")

        ceph_nodes = ceph_cluster.get_nodes()
        log.info(f"Ceph Nodes : {ceph_nodes}")
        server_list = []
        for node in ceph_nodes:
            if node.hostname in active_mds_hostnames:
                log.info(f"{node.hostname} is added to server list")
                server_list.append(node)
        active_nodes = list(set(server_list))
        log.info(f"New set of Active MDS Nodes are: {active_nodes}")

        global stop_flag
        stop_flag = False

        with parallel() as p:
            for mount_dir in mount_paths:
                p.spawn(
                    self.start_io_time,
                    fs_util,
                    clients,
                    mount_dir,
                    timeout=0,
                )

            for mds in active_nodes:
                cluster_health_beforeIO = check_ceph_healthly(
                    clients,
                    num_of_osds,
                    len(active_nodes),
                    build,
                    None,
                    30,
                )
                try:
                    self.reboot_node_v1(ceph_node=mds)
                except Exception as e:
                    stop_flag = True
                    log.error(e)
                    log.error(traceback.format_exc())

                cluster_health_afterIO = check_ceph_healthly(
                    clients,
                    num_of_osds,
                    len(active_nodes),
                    build,
                    None,
                    30,
                )
                if cluster_health_afterIO == cluster_health_beforeIO:
                    log.info("Cluster is healthy")
                else:
                    log.error("Cluster is not healthy")
            log.info("Rebooted all the nodes and cluster is healthy")
            log.info("Setting stop flag")
            stop_flag = True

    def runio_reboot_s_replay_mds_nodes(
        self,
        fs_util,
        ceph_cluster,
        fs_name,
        clients,
        num_of_osds,
        build,
        mount_paths,
    ):
        """
        Processes the standby-replay MDS nodes by running IO operations and rebooting the standby-replay MDS nodes.

        This method performs the following steps:
        1. Identifies the standby-replay MDS nodes.
        2. Runs IO operations in parallel on the specified mount paths.
        3. Reboots the standby-replay MDS nodes one by one.
        4. Checks the cluster health before and after each reboot.
        5. Ensures the cluster remains healthy throughout the process.

        Args:
            fs_util: An instance of a filesystem utility class to run IO operations.
            ceph_cluster: An instance representing the Ceph cluster.
            fs_name (str): The name of the filesystem.
            clients (list): List of clients to execute commands.
            num_of_osds (int): Number of OSDs in the cluster.
            build (str): Build information.
            mount_paths (list): List of directories for mounts (either kernel or fuse).
            mount_type (str): Type of mount paths provided ("kernel" or "fuse").

        Returns:
            None
        """
        # Get standby-replay MDS nodes
        mds = self.get_standby_replay_mdss(clients, fs_name)
        s_replay_mds_hostnames = [i.split(".")[1] for i in mds]
        log.info(f"standby-replay MDS Nodes are: {s_replay_mds_hostnames}")

        ceph_nodes = ceph_cluster.get_nodes()
        log.info(f"Ceph Nodes : {ceph_nodes}")
        server_list = []
        for node in ceph_nodes:
            if node.hostname in s_replay_mds_hostnames:
                log.info(f"{node.hostname} is added to server list")
                server_list.append(node)
        s_replay_nodes = list(set(server_list))
        log.info(f"Set of standby-replay MDS Nodes are: {s_replay_nodes}")

        global stop_flag
        stop_flag = False

        with parallel() as p:
            for mount_dir in mount_paths:
                p.spawn(
                    self.start_io_time,
                    fs_util,
                    clients,
                    mount_dir,
                    timeout=0,
                )

            for mds in s_replay_nodes:
                cluster_health_beforeIO = check_ceph_healthly(
                    clients,
                    num_of_osds,
                    len(s_replay_nodes),
                    build,
                    None,
                    30,
                )
                try:
                    self.reboot_node_v1(ceph_node=mds)
                except Exception as e:
                    stop_flag = True
                    log.error(e)
                    log.error(traceback.format_exc())

                cluster_health_afterIO = check_ceph_healthly(
                    clients,
                    num_of_osds,
                    len(s_replay_nodes),
                    build,
                    None,
                    30,
                )
                if cluster_health_afterIO == cluster_health_beforeIO:
                    log.info("Cluster is healthy")
                else:
                    log.error("Cluster is not healthy")
            log.info("Rebooted all the nodes and cluster is healthy")
            log.info("Setting stop flag")
            stop_flag = True

    def start_io_time(self, fs_util, clients, mounting_dir, timeout=300):
        """
        Starts IO operations on the specified directory for a given duration.

        This method continuously runs IO operations using the 'smallfile' tool in a loop,
        creating new directories for each iteration.
        The loop runs until the specified
        timeout is reached or a global stop flag is set.

        Args:
            fs_util: An instance of a filesystem utility class to run IO operations.
            clients: The client node where the IO operations will be executed.
            mounting_dir (str): The directory path where IO operations will be performed.
            timeout (int): The duration (in seconds) to run the IO operations. Default is 300 seconds.

        Global Variables:
            stop_flag (bool): A global flag to stop the IO operations when set to True.

        Returns:
            None
        """
        global stop_flag
        iter = 0
        if timeout:
            stop = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        else:
            stop = 0
        while True:
            if stop and datetime.datetime.now() > stop:
                log.info("Timed out *************************")
                break
            clients.exec_command(
                sudo=True, cmd=f"mkdir -p {mounting_dir}/run_ios_{iter}"
            )
            fs_util.run_ios(
                clients, f"{mounting_dir}/run_ios_{iter}", io_tools=["smallfile"]
            )
            iter = iter + 1
            if stop_flag:
                log.info("Exited as stop flag is set to True")
                break

    def mds_mem_cpu_load(
        self,
        client,
        mem_target_usage_pct,
        cpu_target_usage_pct,
        mds_name,
        mds_node,
        subvol_list,
        mnt_info,
    ):
        """
        In this method,
          - mount subvolumes, captures initial mem and cpu usage by mds
          - start IO and track mem and cpu usage by mds
          - When usage is atleast target_usage_pct i.e., say 70%, returns as 0
          - If target_usage_pct could not be achived in timeout(60sec), returns 1
        Input params:
        client : Ceph client to run cmds
        target_usage_pct : Its minimum percentage usage of mem and cpu by mds
        to be achived in this method, type - int, value to be between 30-80
        mds_name : Active mds whose usage to be monitored
        subvol_list : subvolumes list to mount and run IO
        client_list: Client objects to mount subvolumes, as one subvolume per client
        """
        initial_mds_mem_used_pct, initial_mds_cpu_used_pct = self.get_mds_cpu_mem_usage(
            client, mds_name, mds_node
        )
        log.info(
            f"MDS MEM,CPU initial usage: MEM - {initial_mds_mem_used_pct},CPU - {initial_mds_cpu_used_pct}"
        )
        # Mount subvolumes and write dataset
        log.info("Write Dataset on subvolumes")
        io_type = "write"
        write_procs = []
        for sv in subvol_list:
            sv_name = sv["subvol_name"]
            client_sv = mnt_info[sv_name]["client"]
            path = mnt_info[sv_name]["path"]
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            io_path = f"{path}/smallfile_dir_{rand_str}"

            repeat_cnt = 1
            p = Thread(
                target=self.mds_systest_io,
                args=(client_sv, io_path, io_type, repeat_cnt),
            )
            p.start()
            write_procs.append(p)
            mnt_info[sv_name].update({"io_path": io_path})
        for p in write_procs:
            p.join()
        mds_mem_used_pct, mds_cpu_used_pct = self.get_mds_cpu_mem_usage(
            client, mds_name, mds_node
        )
        log.info(
            f"MDS MEM and CPU usage after Write op: MEM - {mds_mem_used_pct},CPU - {mds_cpu_used_pct}"
        )
        if float(mds_mem_used_pct) >= float(mem_target_usage_pct) and float(
            mds_cpu_used_pct
        ) >= float(cpu_target_usage_pct):
            return 0
        log.info("Start Read ops and track cpu and mem usage by mds in parallel")
        rw_procs = []
        repeat_cnt = 2
        io_type = "read"
        i = 0
        for sv in mnt_info:
            i += 1
            client_sv = mnt_info[sv]["client"]
            io_path = mnt_info[sv]["io_path"]
            if i == 3:
                io_type = "write"
                rand_str = "".join(
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in list(range(3))
                )
                io_path = f"{path}/smallfile_dir_{rand_str}"
            p = Thread(
                target=self.mds_systest_io,
                args=(client_sv, io_path, io_type, repeat_cnt),
            )
            p.start()
            rw_procs.append(p)

        # If CPU and MEM usage crosses target_usage_pct exit as 0
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
        while end_time > datetime.datetime.now():
            mds_mem_used_pct, mds_cpu_used_pct = self.get_mds_cpu_mem_usage(
                client, mds_name, mds_node
            )
            log.info(
                f"MDS MEM and CPU usage after Read op: MEM - {mds_mem_used_pct},CPU - {mds_cpu_used_pct}"
            )
            if float(mds_mem_used_pct) >= float(mem_target_usage_pct) and float(
                mds_cpu_used_pct
            ) >= float(cpu_target_usage_pct):
                return 0
            time.sleep(5)
        return 1

    def get_mds_cpu_mem_usage(self, client, mds_name, mds_node):
        # Get mem cache limit
        out, rc = client.exec_command(
            sudo=True,
            cmd="ceph config get mds mds_cache_memory_limit",
        )
        mds_mem_limit = float(out.strip())
        out, rc = client.exec_command(
            sudo=True,
            cmd=f"ceph orch ps --refresh| grep {mds_name}",
        )
        mds_info = out.strip()
        mds_mem_usage = mds_info.split()[7]
        mds_mem_used = self.convert_to_bytes(mds_mem_usage)
        mds_mem_used_pct = (mds_mem_used / mds_mem_limit) * 100
        out = ""
        retry_cnt = 20
        iter = 0
        while "ceph-mds" not in out and retry_cnt > 0:
            iter += 1
            out, rc = mds_node.exec_command(
                sudo=True,
                cmd="top -d 3 -n 5 -b | grep ceph-mds > top_out.txt;cat top_out.txt",
            )
            out = out.strip()
            retry_cnt -= 1
            log.info(f"top output:{out}")
            time.sleep(2)

        top_out = out.split("\n")
        log.info(f"top_out:{top_out}")
        mds_cpu_used_pct_list = []
        if len(top_out) <= 1:
            return mds_mem_used_pct, 0
        else:
            for line in top_out:
                log.info(f"line:{line}")
                mds_cpu_used_pct_tmp = line.split()[8]
                mds_cpu_used_pct_list.append(mds_cpu_used_pct_tmp)
            mds_cpu_used_pct = max(mds_cpu_used_pct_list)

            return mds_mem_used_pct, mds_cpu_used_pct

    def mds_systest_io(self, client, io_path, io_type, repeat_cnt):
        i = 0
        while i < repeat_cnt:
            if io_type == "read":
                cmd = "python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 1"
                cmd += f" --files 7000 --top {io_path}"
                client.exec_command(
                    sudo=True,
                    cmd=cmd,
                    long_running=True,
                    timeout=3600,
                )

                i += 1
            elif io_type == "write":
                cmd = f"mkdir {io_path}"
                try:
                    client.exec_command(
                        sudo=True,
                        cmd=cmd,
                    )
                except CommandFailed as ex:
                    log.info(ex)
                cmd = "python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 1"
                cmd += f" --files 7000 --top {io_path} "
                try:
                    client.exec_command(
                        sudo=True,
                        cmd=cmd,
                        long_running=True,
                        timeout=3600,
                    )
                except CommandFailed as ex:
                    log.info(ex)
                i += 1

    def convert_to_bytes(self, mem_size):
        m = re.findall(r"(\d+.*\d+)(\w+)", mem_size)[0]
        size_val = m[0]
        size_type = m[1]
        log.info(f"size_val:{size_val},size_type:{size_type}")
        if "G" in size_type:
            bytes_val = float(size_val) * 1024 * 1024 * 1024
        elif "M" in size_type:
            bytes_val = float(size_val) * 1024 * 1024
        elif "K" in size_type:
            bytes_val = float(size_val) * 1024
        return bytes_val

    def reboot_node_v1(self, ceph_node, timeout=300):
        """
        Reboots a specified Ceph node and attempts to reconnect within a given timeout period.

        Parameters:
        self: object
            The instance of the class that this method belongs to.
        ceph_node: object
            The Ceph node object to be rebooted.
        timeout: int, optional
            The maximum time (in seconds) to wait for the node to reconnect after reboot.
            Defaults to 300 seconds.

        Returns:
        None

        Raises:
        RuntimeError
            If the node fails to reconnect within the specified timeout period.
        """
        ceph_node.exec_command(sudo=True, cmd="reboot", check_ec=False)
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        while datetime.datetime.now() < endtime:
            try:
                sleep(20)
                ceph_node.reconnect()
                return
            except BaseException:
                log.error(
                    "Failed to reconnect to the node {node} after reboot ".format(
                        node=ceph_node.ip_address
                    )
                )
        raise RuntimeError(
            "Failed to reconnect to the node {node} after reboot ".format(
                node=ceph_node.ip_address
            )
        )

    def runio_modify_max_mds(
        self,
        fs_util,
        fs_name,
        clients,
        mount_paths,
        max_mds_value,
    ):
        """
        Run I/O operations in parallel on multiple CephFS mounts and modify the max_mds setting.

        Parameters:
        self: object
            The instance of the class that this method belongs to.
        fs_util: object
            The file system utility object used to perform operations on CephFS.
        fs_name: str
            The name of the Ceph file system.
        clients: list
            A list of client nodes that will run I/O operations.
        mount_paths: list
            A list of mount paths where I/O operations will be performed.
        mount_type: str
            The type of mount (e.g., kernel, fuse).
        max_mds_value: int
            The value to set for max_mds.

        Returns:
        None
        """

        global stop_flag
        stop_flag = False

        with parallel() as p:
            for mount_dir in mount_paths:
                p.spawn(
                    self.start_io_time,
                    fs_util,
                    clients,
                    mount_dir,
                    timeout=0,
                )
                try:
                    log.info(f"Setting max_mds to {max_mds_value} on fs {fs_name}")
                    set_result = self.set_and_validate_max_mds(
                        clients, fs_name, max_mds_value
                    )
                    log.info(
                        f"Set result of max_mds of {fs_name} to {max_mds_value} is {set_result}"
                    )
                except Exception as e:
                    stop_flag = True
                    log.error(e)
                    log.error(traceback.format_exc())

            log.info("Modification of max_mds were successful")
            log.info("Setting stop flag")
            stop_flag = True

    def set_and_validate_max_mds(self, clients, fs_name, max_mds_value):
        """
        Set the max_mds value for a specified Ceph file system and validate the change.

        Parameters:
        self: object
            The instance of the class that this method belongs to.
        clients: object
            The client object used to execute commands on the Ceph cluster.
        fs_name: str
            The name of the Ceph file system.
        max_mds_value: int
            The value to set for max_mds.

        Returns:
        bool
            True if the max_mds value was set and validated successfully, False otherwise.
        """
        clients.exec_command(
            sudo=True, cmd=f"ceph fs set {fs_name} max_mds {max_mds_value}"
        )

        out = clients.exec_command(
            sudo=True,
            cmd=f"ceph fs get {fs_name} -f json",
            check_ec=False,
        )

        json_data = json.loads(out[0])
        log.info(f"Get command output: {json_data}")

        current_max_mds = json_data.get("mdsmap", {}).get("max_mds")
        if current_max_mds == max_mds_value:
            log.info(f"Validation successful: max_mds is set to {max_mds_value}")
            return True
        else:
            log.error(
                f"Validation failed: expected max_mds {max_mds_value}, but got {current_max_mds}"
            )
            return False

    def get_fsmap(self, clients, fs_name):
        """
        Retrieve the file system map for a specified Ceph file system.

        Parameters:
        self: object
            The instance of the class that this method belongs to.
        clients: object
            The client object used to execute commands on the Ceph cluster.
        fs_name: str
            The name of the Ceph file system.

        Returns:
        dict
            A dictionary containing the file system map in JSON format.
        """
        out = clients.exec_command(
            sudo=True,
            cmd=f"ceph fs get {fs_name} -f json-pretty",
            check_ec=False,
        )

        json_data = json.loads(out[0])
        log.info(f"Get command output: {json_data}")
        return json_data

    def get_mds_config(self, client, fs_name):
        out, rc = client.exec_command(
            cmd=f"ceph fs status {fs_name} -f json", client_exec=True
        )
        log.info(out)
        parsed_data = json.loads(out)
        mds_config = parsed_data.get("mdsmap")
        return mds_config

    def get_fs_status_dump(self, client, **kwargs):
        """
        Gets the overall info about fs
        Args:
            client: client node
            **kwargs:
                vol_name : Name of the fs volume for which we need the status
        Return:
            returns ceph fs ls dump in json format
        """
        fs_status_cmd = "ceph fs status"
        if kwargs.get("vol_name"):
            fs_status_cmd += f" {kwargs.get('vol_name')}"
        fs_status_cmd += " --format json"
        out, rc = client.exec_command(sudo=True, cmd=fs_status_cmd)
        fs_status = json.loads(out)
        return fs_status

    def get_mds_standby_replay_pair(self, client, fs_name, mds_config):
        mds_pair_info = {}
        for mds in mds_config:
            if mds.get("state") == "active":
                active_mds = mds["name"]
                mds_pair_info.update({mds["rank"]: {}})
                out, rc = client.exec_command(
                    cmd=f"ceph fs status {fs_name}", client_exec=True
                )
                out_list = out.split("\n")
                for out_iter in out_list:
                    exp_str = f"{mds['rank']}-s"
                    if exp_str in out_iter:
                        standby_replay_mds = out_iter.split()[2]
                        mds_pair_info[mds["rank"]].update(
                            {active_mds: standby_replay_mds}
                        )
        log.info(f"MDS Standby Replay pair info : {mds_pair_info}")
        return mds_pair_info

    def open_files(self, client, mount_dir, file_list):
        """
        Open files in the specified directory on the client node.

        Parameters:
        client: object
            The client object used to execute commands on the Ceph cluster.
        files_list: list
            List of files to be opened on the client node.
        Returns: List
            List of open file PIDs
        """
        for file in file_list:
            client.exec_command(sudo=True, cmd=f"touch {mount_dir}/{file}")
        log.info("It will leave files remain open using tail -f")
        for file in file_list:
            open_cmd = f"nohup tail -f {mount_dir}/{file} > /dev/null 2>&1 &"
            client.exec_command(sudo=True, cmd=open_cmd)
        pid_list = []
        for file in file_list:
            pid_cmd = f"ps aux | grep 'tail -f {mount_dir}/{file}' | grep -v grep | awk '{{print $2}}'"
            pid = client.exec_command(sudo=True, cmd=pid_cmd)[0].strip()
            if pid:
                pid_list.append(pid)
        log.info(f"PID list of open files: {pid_list}")
        return pid_list

    def close_files(self, client, pid_list):
        """
        Close the files on the client node.

        Parameters:
        client: object
            The client object used to execute commands on the Ceph cluster.
        pid_list: list
            List of PIDs to close the files on the client node.
        """
        try:
            for pid in pid_list:
                client.exec_command(sudo=True, cmd=f"kill {pid}")
            log.info("all the PIDs are killed and files are closed")
        except Exception as e:
            log.error(f"Failed to close the files: {e}")
            raise e

    @retry(CommandFailed, tries=10, delay=5)
    def get_cephfs_top_dump(self, client):
        """
        Get the cephfs-top dump for the Ceph file system.
        """
        try:
            dump_out, _ = client.exec_command(sudo=True, cmd="cephfs-top --dump")
            log.info(f"cephfs-top dump: {dump_out}")
            if "chit" not in dump_out:
                raise CommandFailed("Failed to get cephfs-top dump")
            dump = json.loads(dump_out)
            return dump

        except CommandFailed as e:
            log.error(f"Failed to get cephfs-top dump: {e}")
            raise e

    def get_client_id(self, client, fs_name, mounted_dir):
        """
        Get the client ID for the mounted directory on the Ceph file system.
        """
        ranked_mds, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs status {fs_name} -f json | jq '.mdsmap[] | select(.rank == 0) | .name'",
        )
        ranked_mds = ranked_mds.replace('"', "").replace("\n", "")
        client_id_cmd = (
            f"ceph tell mds.{ranked_mds} session ls | jq '.[] | select(.client_metadata.mount_point"
            f' != null and (.client_metadata.mount_point | contains("{mounted_dir}"))) | .id\''
        )
        client_id, _ = client.exec_command(sudo=True, cmd=client_id_cmd)
        client_id = client_id.replace('"', "").replace("\n", "")
        log.info(f"Client ID : {client_id} for Mounted Directory : {mounted_dir}")
        return client_id

    def set_subvolume_earmark(
        self, client, vol_name, subvol_name, earmark, group_name=None
    ):
        """
        Set earmark for a subvolume and verify the earmark is set
        Args:
            vol_name (str): Volume name
            subvol_name (str): Subvolume name
            earmark (str): Earmark name -> either starts with nfs or smb
        Returns:
            tuple: output, error
        """
        command = f"ceph fs subvolume earmark set {vol_name} {subvol_name} --earmark {earmark}"
        if group_name:
            command += f" --group_name={group_name}"
        log.info(f"Setting earmark for subvolume: {command}")
        out, err = client.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Failed to set earmark for subvolume: {err}")
            return 1
        # verify the earmark is set
        command = f"ceph fs subvolume earmark get {vol_name} {subvol_name}"
        if group_name:
            command += f" --group_name={group_name}"
        log.info(f"Getting earmark for subvolume: {command}")
        out, err = client.exec_command(sudo=True, cmd=command)
        if earmark not in out:
            log.error(f"Earmark not set for subvolume: {err}")
            return 1
        log.info(f"Earmark set for subvolume: {out}")
        log.info(f"Eearmark set for subvolume:[{subvol_name}] as [{earmark}]successful")
        return 0

    def get_subvolume_earmark(self, client, vol_name, subvol_name, group_name=None):
        """
        Get earmark for a subvolume
        Returns: Earmark name
        """
        command = f"ceph fs subvolume earmark get {vol_name} {subvol_name}"
        if group_name:
            command += f" --group_name={group_name}"
        log.info(f"Getting earmark for subvolume: {command}")
        out, err = client.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Failed to get earmark for subvolume: {err}")
            return 1
        log.info(f"Earmark for subvolume: {out}")
        return out

    def remove_subvolume_earmark(self, client, vol_name, subvol_name, group_name=None):
        """
        Remove earmark for a subvolume and verify the earmark is removed
        Args:
            client:
            vol_name:
            subvol_name:
            group_name:

        Returns:

        """
        command = f"ceph fs subvolume earmark rm {vol_name} {subvol_name}"
        if group_name:
            command += f" --group_name={group_name}"
        log.info(f"Removing earmark for subvolume: {command}")
        out, err = client.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Failed to remove earmark for subvolume: {err}")
            return 1
        # verify the earmark is removed
        command = f"ceph fs subvolume earmark get {vol_name} {subvol_name}"
        if group_name:
            command += f" --group_name={group_name}"
        log.info(f"Getting earmark for subvolume: {command}")
        out, err = client.exec_command(sudo=True, cmd=command)
        if "Error" in err:
            log.error(f"Earmark not removed for subvolume: {err}")
            return 1
        log.info(f"Earmark removed for subvolume: {out}")
        log.info(f"Eearmark removed for subvolume:[{subvol_name}] successful")
        return 0

    def get_fs_dump(self, client):
        """
        Gets the dump output of fs
        Args:
            client: client node
        Return:
            returns ceph fs dump in json format
        """
        fs_dump_cmd = "ceph fs dump --format json"
        out, _ = client.exec_command(sudo=True, cmd=fs_dump_cmd)
        fs_dump_output = json.loads(out)
        return fs_dump_output

    def fetch_value_from_json_output(self, **kwargs):
        """
        Recursive function to iteratively check the nested list of dictionaries
        Args:
            search_list: Accepts lists, list of dictionary
            match_key: Provide a match_key, match_value pair to select the list that is expected from multiple list
            match_value: Provide a match_key, match_value pair to select the list that is expected from multiple list
            target_key: To figure out the value of particular key
        Return:
            If success, returns the value of the target_key
            If no key found, return None
        Usage:
            fetch_value_from_json_output(search_list=[{node1: "node1", node2: "node2"}],
                                        match_key="name", match_value="cephfs1", target_key="status")
            Explanation: This will basically try to fetch the list where the value of "name" matches with
                        ".*cephfs1.*" from the search_list and gives us the value of key "status" from the list
        """
        if all(value is not None for value in kwargs.values()):
            for item in kwargs.get("search_list"):
                if isinstance(item, dict):
                    # If 'name' exists and matches partially, return the desired key
                    if kwargs.get("match_value") in item.get(
                        kwargs.get("match_key"), ""
                    ):
                        return item.get(kwargs.get("target_key"))

                    if (
                        kwargs.get("target_key") == "fs_name"
                        and kwargs.get("target_key") in item
                        and any(
                            kwargs.get("match_value")
                            in info_item.get(kwargs.get("match_key"), "")
                            for info_item in item["info"].values()
                        )
                    ):
                        return item["fs_name"]

                    if (
                        kwargs.get("target_key") == "id"
                        and kwargs.get("target_key") in item
                        and any(
                            kwargs.get("match_value")
                            in info_item.get(kwargs.get("match_key"), "")
                            for info_item in item["mdsmap"]["info"].values()
                        )
                    ):
                        return item["id"]

                    # Traverse nested dictionaries
                    for value in item.values():
                        if isinstance(value, (dict, list)):
                            result = self.fetch_value_from_json_output(
                                search_list=(
                                    [value] if isinstance(value, dict) else value
                                ),
                                match_key=kwargs.get("match_key"),
                                match_value=kwargs.get("match_value"),
                                target_key=kwargs.get("target_key"),
                            )
                            if result is not None:
                                return result
            return None
        else:
            return log.error(
                "One or more values are none. Expected Key-Value pair: search_list, match_key, match_value, target_key"
            )

    def collect_fs_status_data_for_validation(self, client, fs_name):
        """
        Gets the output using fs status and collect required info
        Args:
            client: client node
            fs_name: File system name
        Return:
            returns status,data_avail,data_used,meta_avail,meta_used and mds name from ceph fs status in dict format
        """
        fs_status_dict = {}
        fs_status_info = self.get_fs_status_dump(client)
        log.debug(f"Output: {fs_status_info}")

        status = self.fetch_value_from_json_output(
            search_list=fs_status_info["mdsmap"],
            match_key="name",
            match_value=fs_name,
            target_key="state",
        )
        data_avail = self.fetch_value_from_json_output(
            search_list=fs_status_info["pools"],
            match_key="name",
            match_value=fs_name + ".data",
            target_key="avail",
        )
        data_used = self.fetch_value_from_json_output(
            search_list=fs_status_info["pools"],
            match_key="name",
            match_value=fs_name + ".data",
            target_key="used",
        )
        meta_avail = self.fetch_value_from_json_output(
            search_list=fs_status_info["pools"],
            match_key="name",
            match_value=fs_name + ".meta",
            target_key="avail",
        )
        meta_used = self.fetch_value_from_json_output(
            search_list=fs_status_info["pools"],
            match_key="name",
            match_value=fs_name + ".meta",
            target_key="used",
        )
        for entry in fs_status_info["mdsmap"]:
            for key, value in entry.items():
                if key == "state" and value == "active" and fs_name in entry["name"]:
                    mds_name = entry["name"]

        fs_status_dict.update(
            {
                "status": status,
                "data_avail": data_avail,
                "data_used": data_used,
                "meta_avail": meta_avail,
                "meta_used": meta_used,
                "mds_name": mds_name,
            }
        )
        return fs_status_dict

    def collect_fs_volume_info_for_validation(self, client, fs_name, **kwargs):
        """
        Gets the output using fs volume info and collected required info
        Args:
            client: client node
            fs_name: File system name
        Return:
            returns data_avail,data_used,meta_avail,meta_used and mon addrs from ceph fs volume info in dict format
        """
        fs_volume_info_dict = {}

        fs_vol_info = self.get_fs_info_dump(
            client, fs_name, human_readable=kwargs.get("human_readable", False)
        )
        log.debug(f"Output: {fs_vol_info}")

        data_avail = self.fetch_value_from_json_output(
            search_list=[fs_vol_info],
            match_key="name",
            match_value=fs_name + ".data",
            target_key="avail",
        )
        data_used = self.fetch_value_from_json_output(
            search_list=[fs_vol_info],
            match_key="name",
            match_value=fs_name + ".data",
            target_key="used",
        )
        meta_avail = self.fetch_value_from_json_output(
            search_list=[fs_vol_info],
            match_key="name",
            match_value=fs_name + ".meta",
            target_key="avail",
        )
        meta_used = self.fetch_value_from_json_output(
            search_list=[fs_vol_info],
            match_key="name",
            match_value=fs_name + ".meta",
            target_key="used",
        )
        mon_addrs = fs_vol_info["mon_addrs"]

        fs_volume_info_dict.update(
            {
                "data_avail": data_avail,
                "data_used": data_used,
                "meta_avail": meta_avail,
                "meta_used": meta_used,
                "mon_addrs": mon_addrs,
            }
        )
        return fs_volume_info_dict

    def collect_fs_dump_for_validation(self, client, fs_name):
        """
        Gets the output using fs dump and collected required info
        Args:
            client: client node
            fs_name: File system name
        Return:
            returns status, fs_name, fsid, rank and mds name from ceph fs dump in dict format
        """
        fs_dump_info_dict = {}
        fs_dump = self.get_fs_dump(client)
        log.debug(fs_dump)

        status = (
            self.fetch_value_from_json_output(
                search_list=fs_dump["filesystems"],
                match_key="name",
                match_value=fs_name,
                target_key="state",
            )
        ).split(":", 1)[1]
        fsname = self.fetch_value_from_json_output(
            search_list=fs_dump["filesystems"],
            match_key="name",
            match_value=fs_name,
            target_key="fs_name",
        )
        fsid = self.fetch_value_from_json_output(
            search_list=fs_dump["filesystems"],
            match_key="name",
            match_value=fs_name,
            target_key="id",
        )
        rank = self.fetch_value_from_json_output(
            search_list=fs_dump["filesystems"],
            match_key="name",
            match_value=fs_name,
            target_key="rank",
        )
        mds_name = self.fetch_value_from_json_output(
            search_list=fs_dump["filesystems"],
            match_key="name",
            match_value=fs_name,
            target_key="name",
        )

        fs_dump_info_dict.update(
            {
                "status": status,
                "fsname": fsname,
                "fsid": fsid,
                "rank": rank,
                "mds_name": mds_name,
            }
        )
        return fs_dump_info_dict

    def collect_fs_get_for_validation(self, client, fs_name):
        """
        Gets the output using fs get and collected required info
        Args:
            client: client node
            fs_name: File system name
        Return:
            returns status, fs_name, fsid, rank and mds name from ceph fs get in dict format
        """
        fs_get_info_dict = {}
        fs_get_output = self.get_fsmap(client, fs_name)
        log.debug(fs_get_output)

        status = (
            self.fetch_value_from_json_output(
                search_list=[fs_get_output],
                match_key="name",
                match_value=fs_name,
                target_key="state",
            )
        ).split(":", 1)[1]
        fsname = self.fetch_value_from_json_output(
            search_list=[fs_get_output],
            match_key="name",
            match_value=fs_name,
            target_key="fs_name",
        )
        fsid = self.fetch_value_from_json_output(
            search_list=[fs_get_output],
            match_key="name",
            match_value=fs_name,
            target_key="id",
        )
        rank = self.fetch_value_from_json_output(
            search_list=[fs_get_output],
            match_key="name",
            match_value=fs_name,
            target_key="rank",
        )
        mds_name = self.fetch_value_from_json_output(
            search_list=[fs_get_output],
            match_key="name",
            match_value=fs_name,
            target_key="name",
        )

        fs_get_info_dict.update(
            {
                "status": status,
                "fsname": fsname,
                "fsid": fsid,
                "rank": rank,
                "mds_name": mds_name,
            }
        )
        return fs_get_info_dict

    def validate_dicts(self, dicts, keys_to_check):
        """
        Validate values of specific keys across multiple dictionaries and dictionary of list.

        Parameters:
            dicts (list): List of dictionaries to compare.
            keys_to_check (list): List of keys to check for validation.

        Returns:
            dict: Validation results for each key.
        """

        for key in keys_to_check:
            # Find dictionaries that contain the key
            dicts_with_key = [d for d in dicts if key in d]

            if len(dicts_with_key) == 0:
                log.error(f"Key '{key}' not found in any dictionary")
                return False
            elif len(dicts_with_key) == 1:
                log.error(f"Key '{key}' found in only one dictionary, cannot validate")
                return False
            else:
                # Collect values for the key
                values = [d[key] for d in dicts_with_key]

                # Check if values are lists
                if all(isinstance(v, list) for v in values):
                    # Compare list contents
                    if all(sorted(v) == sorted(values[0]) for v in values):
                        log.info(
                            f"Key '{key}' validated across {len(dicts_with_key)} \
                            dictionaries with the list value {values[0]}"
                        )
                    else:
                        log.error(f"Key '{key}' mismatch: List values = {values}")
                        return False
                else:
                    # Compare values of the key across the dictionaries
                    # Rewriting values using set for single value
                    values = set(d[key] for d in dicts_with_key)
                    if len(values) == 1:
                        log.info(
                            f"Key '{key}' validated across {len(dicts_with_key)} dictionaries with the value {values}"
                        )
                    else:
                        log.error(f"Key '{key}' mismatch: Values = {values}")
                        return False
        return True

    def rename_volume(self, client, old_name, new_name):
        log.info(f"[Fail {old_name} before renaming it]")
        client.exec_command(
            sudo=True, cmd=f"ceph fs fail {old_name} --yes-i-really-mean-it"
        )
        log.info("[Set refuse_client_session to true]")
        client.exec_command(
            sudo=True, cmd=f"ceph fs set {old_name} refuse_client_session true"
        )
        log.info("[Rename the volume]")
        rename_cmd = f"ceph fs rename {old_name} {new_name} --yes-i-really-mean-it"
        out, ec = client.exec_command(sudo=True, cmd=rename_cmd)
        if "renamed." not in ec:
            log.error(ec)
            log.error(f"Failed to rename the volume: {out}")
            return 1
        out, ec = client.exec_command(sudo=True, cmd="ceph fs ls")
        if new_name not in out:
            log.error(f"Volume not renamed: {out}")
            return 1
        log.info(f"Volume renamed successfully: {out}")
        log.info("Put it back to previous state")
        client.exec_command(
            sudo=True, cmd="ceph fs set " + new_name + " refuse_client_session false"
        )
        client.exec_command(sudo=True, cmd=f"ceph fs set {new_name} joinable true")
        timer = 10
        while timer > 0:
            out, ec = client.exec_command(sudo=True, cmd=f"ceph fs status {new_name}")
            if "active" in out:
                break
            time.sleep(5)
            timer -= 1
        log.info(f"Volume {new_name} is active now")
        log.info("Renaming and verification of volume successful")
        return 0
