"""
Module to perform Serviceability scenarios on mgr daemons
"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from utility.log import Log

log = Log(__name__)


class MgrWorkflows:
    """
    Module to perform Serviceability scenarios on mgr daemons
    """

    def __init__(self, node: CephAdmin):
        """
        Module to perform Serviceability scenarios on mgr daemons

        Args:
            node: CephAdmin object
        """
        self.rados_obj = RadosOrchestrator(node=node)
        self.mon_obj = MonitorWorkflows(node=node)
        self.cluster = node.cluster
        self.config = node.config
        self.client = node.cluster.get_nodes(role="client")[0]
        self.cluster_nodes = node.cluster.get_nodes()

    def add_mgr_service(self, **kwargs) -> bool:
        """
        Method to add the mgr service from the given host
        Args:
            kwargs: the accepted KW args for the method are:
                host: Cluster object of the host from where mgr needs to be Added
        returns:
            Pass -> True, Fail -> false
        """
        # Checking if mgr service exists before removing
        hostname = kwargs.get("host")
        log.info(f"The new mgr node to add the cluster is -{hostname}")
        if self.rados_obj.check_daemon_exists_on_host(host=hostname, daemon_type="mgr"):
            log.info(
                f"MGR daemon already present on the host {hostname}. Returning Pass"
            )
            return True

        log.info(f"Adding mgr daemon on the host : {hostname}")
        # Get the current mgr list
        current_mgrs = self.get_mgr_list()
        current_mgrs.append(hostname)
        new_mgr_list = " ".join(current_mgrs)
        log.info(f"The new mgr list before adding the the cluster is-{new_mgr_list}")
        cmd = f'ceph orch apply mgr --placement "{new_mgr_list}"'
        try:
            self.client.exec_command(sudo=True, cmd=cmd)
        except Exception as err:
            log.debug(f"Hit issue during command execution : {err}")

        endtime = datetime.datetime.now() + datetime.timedelta(seconds=180)
        while datetime.datetime.now() < endtime:
            if self.rados_obj.check_daemon_exists_on_host(
                host=hostname, daemon_type="mgr"
            ):
                log.info(f"Mgr {hostname} is add to the cluster.")
                return True
            log.info(
                "Mgr daemon still not added.sleeping for 5 seconds and checking again"
            )
            time.sleep(5)
        return False

    def add_mgr_with_label(self, hostname):
        """
        Method to add the mgr service from the given host
        Args:
            hostname:  Cluster hostname  where mgr needs to be configured
            returns:
                Pass -> True, Fail -> false
        """
        # Checking if mgr service exists before removing
        log.info(f"The new mgr node to add the cluster is -{hostname}")
        if self.rados_obj.check_daemon_exists_on_host(host=hostname, daemon_type="mgr"):
            log.info(
                f"MGR daemon already present on the host {hostname}. Returning Pass"
            )
            return True

        log.info(f"Adding mgr daemon on the host : {hostname}")
        # Get the current mgr list
        log.info(f"Adding mgr daemon on the host : {hostname}")
        labels = self.mon_obj.get_host_labels(host=hostname)
        if "mgr" not in labels:
            cmd = f"ceph orch host label add {hostname} mgr"
            self.client.exec_command(sudo=True, cmd=cmd)
            time.sleep(15)
            log.debug(f"Added mgr label for the host : {hostname}")
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=180)
        while datetime.datetime.now() < endtime:
            if self.rados_obj.check_daemon_exists_on_host(
                host=hostname, daemon_type="mgr"
            ):
                log.info(f"Mgr {hostname} is add to the cluster.")
                return True
            log.info(
                "Mgr daemon still not added.sleeping for 5 seconds and checking again"
            )
            time.sleep(5)
        return False

    def remove_mgr_service(self, hostname) -> bool:
        """
        Method to remove the mgr service from the given host

        Args:
            hostname: name of the host from where mgr needs to be removed

        returns:
            Pass -> True, Fail -> false
        """

        if not self.rados_obj.check_daemon_exists_on_host(
            host=hostname, daemon_type="mgr"
        ):
            log.info(f"Provided {hostname} mgr is not present in the  mgr list.")
            return False
        # Get the current mgr list
        current_mgrs = self.get_mgr_list()
        current_mgrs.remove(hostname)
        no_mgrs = len(current_mgrs)
        new_mgr_list = " ".join(current_mgrs)
        cmd = f'ceph orch apply mgr "{no_mgrs} {new_mgr_list}" '
        try:
            self.client.exec_command(sudo=True, cmd=cmd)
            time.sleep(120)
        except Exception as err:
            log.debug(f"Hit issue during command execution : {err}")

        mgr_exists = True
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=180)
        while datetime.datetime.now() < endtime:
            time.sleep(10)
            if not self.rados_obj.check_daemon_exists_on_host(
                host=hostname, daemon_type="mgr"
            ):
                log.info(f"Mgr daemon is removed on the host {hostname}.")
                mgr_exists = False
                break
            log.info(
                f"Mgr daemon still present on the host {hostname}. sleeping for 5 seconds and checking again"
            )
        if mgr_exists:
            log.error(
                f"Mgr daemon still present on the host {hostname}. Failed to remove"
            )
            return False
        log.debug(f"Mgr successfully removed from host : {hostname}")
        return True

    def remove_mgr_with_label(self, hostname):
        """
        Method to remove the mgr service from the given host
        Args:
            hostname:  Cluster hostname  where mgr needs to be remove
        returns:
                Pass -> True, Fail -> false
        """
        # Checking if mgr service exists before removing
        if not self.rados_obj.check_daemon_exists_on_host(
            host=hostname, daemon_type="mgr"
        ):
            log.info(f"Provided {hostname} mgr is not present in the  mgr list.")
            return False

        cmd = f"ceph orch host label rm {hostname} mgr"
        self.client.exec_command(sudo=True, cmd=cmd)
        time.sleep(120)
        log.debug(f"Removed mgr label from the host : {hostname}")

        mgr_exists = True
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=180)
        while datetime.datetime.now() < endtime:
            time.sleep(10)
            if not self.rados_obj.check_daemon_exists_on_host(
                host=hostname, daemon_type="mgr"
            ):
                log.info(f"Mgr daemon is removed on the host {hostname}.")
                mgr_exists = False
                break
            log.info(
                f"Mgr daemon still present on the host {hostname}. sleeping for 5 seconds and checking again"
            )
        if mgr_exists:
            log.error(
                f"Mgr daemon still present on the host {hostname}. Failed to remove"
            )
            return False
        log.debug(f"Mgr successfully removed from host : {hostname}")
        return True

    def remove_mgr_by_unmanaged(self, host) -> bool:
        """
        Method to remove the mgr service from the given host

        Args:
            host: name of the host from where mgr needs to be removed

        returns:
            Pass -> True, Fail -> false
        """
        # Code to get the exact mgr daemon name.For example: ceph-rados-auto-65guw8-node2.zgoyni
        mgr_daemon_list = self.get_mgr_daemon_list()
        new_mgr_daemon = ""
        for node in mgr_daemon_list:
            if host.hostname in node:
                new_mgr_daemon = node.strip()
                break

        host_name = host.hostname
        if not self.rados_obj.check_daemon_exists_on_host(
            host=host_name, daemon_type="mgr"
        ):
            log.info(f"MGR daemon not present on the host {host_name}. Returning Pass")
            return True

        # setting mgr daemon unmanaged to true
        unmanaged_chk = self.rados_obj.set_unmanaged_flag("mgr")
        if not unmanaged_chk:
            log.error("The unmanaged flag is not set")
            return 1

        cmd = f"ceph orch daemon rm mgr.{new_mgr_daemon} --force"
        try:
            self.client.exec_command(sudo=True, cmd=cmd)
        except Exception as err:
            log.debug(f"Hit issue during command execution : {err}")

        mgr_exists = True
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=180)
        while datetime.datetime.now() < endtime:
            time.sleep(10)
            if not self.rados_obj.check_daemon_exists_on_host(
                host=host_name, daemon_type="mgr"
            ):
                log.info(f"Mgr daemon is removed on the host {host_name}.")
                mgr_exists = False
                break
            log.info(
                f"Mgr daemon still present on the host {host_name}. sleeping for 5 seconds and checking again"
            )

        if mgr_exists:
            log.error(
                f"Mgr daemon still present on the host {host_name}. Failed to remove"
            )
            return False

        # Removing mgr label from host if present
        labels = self.mon_obj.get_host_labels(host=host_name)
        if "mgr" in labels:
            cmd = f"ceph orch host label rm {host_name} mgr"
            self.client.exec_command(sudo=True, cmd=cmd)
            log.debug(f"Removed mgr label from host : {host_name}")
            time.sleep(30)

        # Unsetting mgr daemon
        managed_chk = self.rados_obj.set_managed_flag("mgr")
        time.sleep(30)
        if not managed_chk:
            log.error("The unmanaged flag is  not unset")
            return False

        log.debug(f"Mgr successfully removed from host : {host_name}")
        return True

    def mgr_scale_up_down_bylist(self, mgr_node_list):
        """
        Method to scale up or down the mgr daemons to the cluster
        Args:
            mgr_node_list : This parameter contain the mgr daemon list.If mgr_node_list is greater than the
            current mgr list then the method performs scale up.If its less the method performs scale down.
        returns:
            Pass -> True, Fail -> false
        """

        current_mgr_daemons = self.rados_obj.get_daemon_list_fromCluster(
            daemon_type="mgr"
        )
        req_mgr_list = [item.split(".")[0] for item in mgr_node_list]
        log.info(f"The current mgr_daemons list is-{current_mgr_daemons}")
        log.info(f"The requested mgr list is -{req_mgr_list}")
        current_mgr_set = set(current_mgr_daemons)
        req_mgr_set = set(req_mgr_list)
        new_mgrs = req_mgr_set.difference(current_mgr_set)

        # case1 : The list is same as the old
        if len(current_mgr_daemons) == len(req_mgr_list) and len(new_mgrs) == 0:
            log.info("The current mgr and requested mgr list is same")
            return True
        # case2: The list contain new mgr but the cout is same
        if len(current_mgr_daemons) == len(req_mgr_list) and len(new_mgrs) != 0:
            log.info(
                f"The current mgr daemon count is same as the requested mgr list."
                f"New {new_mgrs} will get replace the old mgr"
            )
        # Case3: The list contain new mgr to scaleup

        if len(req_mgr_list) > len(current_mgr_daemons):
            log.info(
                f"Performing mgr scale up.The mgr is scale-up from {len(current_mgr_daemons)} to {len(req_mgr_list)}"
            )
            log.info(f"The new mgrs are adding to current mgrs are - {new_mgrs}")

        else:
            log.info(
                f"Performing MGR scale down.The mgr is scale-down from "
                f"{len(current_mgr_daemons)} to {len(req_mgr_list)}"
            )

        mgr_nodes = "  ".join(mgr_node_list)
        cmd = f'ceph orch apply mgr "{mgr_nodes}"'
        self.client.exec_command(sudo=True, cmd=cmd)
        time.sleep(120)
        mgr_count = 0
        for node in mgr_node_list:
            if self.rados_obj.check_daemon_exists_on_host(host=node, daemon_type="mgr"):
                log.info(f"Mgr {node} is up and running in the cluster.")
                log.info(f" In the {node} mgr is up and running")
                mgr_count = mgr_count + 1

        if mgr_count != len(mgr_node_list):
            log.info(f" The {mgr_node_list} are not up and running")
            return False
        return True

    def mgr_scale_up_down_byLabel(self, mgr_node_list):
        """
        Method to scale up or down the mgr daemons to the cluster
        Args:
            mgr_node_list : This parameter contain the mgr daemon list.If mgr_node_list is greater than the
            current mgr list then the method performs scale up.If its less the method performs scale down.
        returns:
            Pass -> True, Fail -> false
        """

        current_mgr_daemons = self.rados_obj.get_daemon_list_fromCluster(
            daemon_type="mgr"
        )
        # req_mgr_list = [item.split(".")[0] for item in mgr_node_list]

        log.info(f"The current mgr_daemons list is-{current_mgr_daemons}")
        log.info(f"The requested mgr list is -{mgr_node_list}")
        current_mgr_set = set(current_mgr_daemons)
        req_mgr_set = set(mgr_node_list)
        mgr_to_remove = current_mgr_set.difference(req_mgr_set)
        new_mgrs_to_add = req_mgr_set.difference(current_mgr_set)

        # case1 : The list is same as the old
        if len(mgr_to_remove) == 0 and len(new_mgrs_to_add) == 0:
            log.info("The current mgr and requested mgr list is same")
            return True
        # case2: The list contain new mgr but the cout is same
        if len(current_mgr_daemons) == len(mgr_node_list) and len(new_mgrs_to_add) != 0:
            log.info(
                f"The current mgr daemon count is same as the requested mgr list."
                f"New {new_mgrs_to_add} will get replace the {mgr_to_remove} mgr"
            )
        # Case3: The list contain new mgr to scaleup

        if len(mgr_node_list) > len(current_mgr_daemons):
            log.info(
                f"Performing mgr scale up.The mgr is scale-up from {len(current_mgr_daemons)} to {len(mgr_node_list)}"
            )
            log.info(f"The new mgrs are adding to current mgrs are - {new_mgrs_to_add}")

        else:
            log.info(
                f"Performing MGR scale down.The mgr is scale-down from "
                f"{len(current_mgr_daemons)} to {len(mgr_node_list)}"
            )

        if len(mgr_to_remove) > 0:
            for node in mgr_to_remove:
                result = self.remove_mgr_with_label(node)
                if not result:
                    log.error(f"Failed to remove the {node}")

        if len(new_mgrs_to_add) > 0:
            for node in new_mgrs_to_add:
                result = self.add_mgr_with_label(node)
                if not result:
                    log.error(f"Failed to add the {node}")

        mgr_count = 0
        for node in mgr_node_list:
            if self.rados_obj.check_daemon_exists_on_host(host=node, daemon_type="mgr"):
                log.info(f"Mgr {node} is up and running in the cluster.")
                log.info(f" In the {node} mgr is up and running")
                mgr_count = mgr_count + 1

        if mgr_count != len(mgr_node_list):
            log.info(f" The {mgr_node_list} are not up and running")
            return False
        return True

    def add_mgr_by_number(self, count):
        """
        Method to add the mgr daemons with the number
        Args:
            count : no of mgr to add to the cluster
        Return:
            True->mgrs are added False -> not added to the cluster
        """
        cmd_orch = "ceph orch host ls"
        host_output = self.rados_obj.run_ceph_command(cmd=cmd_orch, client_exec=True)
        if count > len(host_output):
            log.error("The count is more than the nodes")
            return False
        cmd_mgr_count = f" ceph orch apply mgr {count}"
        self.client.exec_command(sudo=True, cmd=cmd_mgr_count)
        time.sleep(120)
        current_mgr_daemons = self.rados_obj.get_daemon_list_fromCluster(
            daemon_type="mgr"
        )
        if count != len(current_mgr_daemons):
            log.error("The mgr daemons are not configured with the count")
            return False
        return True

    def get_mgr_stats(self):
        """
        Method to retrive the mgr daemon stats
        Args: none
        Return:
             mgr daemon stats in json format.
        """
        cmd = "ceph mgr stat"
        mgr_stats = self.rados_obj.run_ceph_command(cmd)
        return mgr_stats

    def set_mgr_fail(self, host):
        """
        Method to fail the mgr host
        Args:
         host : mgr host name
        Return:
             Return the output of the execution of the command
        """
        cmd = f"ceph mgr fail {host}"
        out_put = self.rados_obj.run_ceph_command(cmd)
        time.sleep(10)
        return out_put

    def get_mgr_list(self):
        """
        Method to get the mgr list from the cluster
        Args:
            none
        Returns:
            Current mgr nodes as a list
        """
        mgr_node_list = []
        cmd_orch = "ceph orch host ls"
        host_output = self.rados_obj.run_ceph_command(cmd=cmd_orch, client_exec=True)
        for item in host_output:
            host_name = item["hostname"]
            cmd_orch = f"ceph orch ps {host_name}"
            out = self.rados_obj.run_ceph_command(cmd=cmd_orch, client_exec=True)
            for entry in out:
                if entry["daemon_type"] == "mgr":
                    mgr_node_list.append(host_name)
        return mgr_node_list

    def get_mgr_daemon_list(self):
        """
        Method to retrive the mgr daemon list not the host name.
        For example the  mgr daemon name like
         - ceph-rados-tfa-otrj8b-node1-installer.agbrji, ceph-rados-tfa-otrj8b-node2.yplzgj
        Args:
             none
        Return: Mgr active and standby daemon list.
        """
        mgr_list = []
        out_put = self.rados_obj.run_ceph_command(cmd="ceph mgr dump")
        mgr_list.append(out_put["active_name"])
        for standby_mgr in out_put["standbys"]:
            mgr_list.append(standby_mgr["name"])
        log.info(f"The mgr daemon list is -{mgr_list}")
        return mgr_list
