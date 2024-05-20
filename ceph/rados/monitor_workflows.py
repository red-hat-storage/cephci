"""
Module to perform Serviceability scenarios on mon daemons
"""
import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def count(func):
    """
    Decorator method to check how many times a particular method has been invoked
    :param func: name of the function
    :return: wrapped method
    """

    def wrapped(*args, **kwargs):
        wrapped.calls += 1
        return func(*args, **kwargs)

    wrapped.calls = 0
    return wrapped


class MonitorWorkflows:
    """
    Module to perform Serviceability scenarios on mon daemons
    """

    def __init__(self, node: CephAdmin):
        """
        Module to perform Serviceability scenarios on mon daemons

        Args:
            node: CephAdmin object
        """
        self.rados_obj = RadosOrchestrator(node=node)
        self.cluster = node.cluster
        self.config = node.config
        self.client = node.cluster.get_nodes(role="client")[0]

    def get_host_labels(self, host) -> list:
        """
        Method to get the labels that are present on the

        Args:
            host: name of the host for which labels should be fetched

        Returns:
            list of labels persent on host
        """
        cmd = "ceph orch host ls"
        out = self.rados_obj.run_ceph_command(cmd=cmd)
        for entry in out:
            if entry["hostname"] == host:
                log.debug(f"Host entry found. details : {entry}")
                return entry["labels"]

    @count
    def set_mon_service_managed_type(self, unmanaged) -> bool:
        """
        Method to set the mon service to either managed or unmanaged

        Args:
            unmanaged: True or false, for the service management

        returns:
            Pass -> True, Fail -> false
        """
        cmd = "ceph orch ls mon --export"
        out = self.rados_obj.run_ceph_command(cmd=cmd, client_exec=True)[0]
        if unmanaged:
            log.debug(
                f"Setting the service as unmanaged by cephadm. current status : {out}"
            )
            out["unmanaged"] = "true"
        else:
            log.debug(
                f"Setting the service as unmanaged by cephadm. current status : {out}"
            )
            out["unmanaged"] = "false"

        file_name = f"/tmp/mon_spec_{self.set_mon_service_managed_type.calls}.yaml"
        # Creating mon config file
        self.client.exec_command(sudo=True, cmd=f"touch {file_name}")
        # Adding the spec rules into the file
        cmd = f"echo {out} > {file_name}"
        self.client.exec_command(cmd=cmd, sudo=True)

        log.debug(f"Contents of mon spec file : {out}")
        apply_cmd = f"ceph orch apply -i {file_name}"
        log.info(f"Applying the spec file via cmd : {apply_cmd}")
        self.client.exec_command(cmd=apply_cmd, sudo=True)

        time.sleep(10)
        # Checking for the unmanaged setting on mon service
        cmd = "ceph orch ls"
        out = self.rados_obj.run_ceph_command(cmd=cmd)
        for entry in out:
            if entry["service_name"] == "mon":
                log.debug(f"Service status : {entry}")
                status = entry.get("unmanaged", False)
                if status != unmanaged:
                    log.error(f"Service not in unmamaned={unmanaged} state. Fail")
                    return False
                else:
                    log.info(f"Service  in unmamaned={unmanaged} state. Pass")
                    return True

    def check_mon_exists_on_host(self, host) -> bool:
        """
        Method to check if the mon daemon exists on the given host
        Args:
            host: name of the host where existence of mon needs to be checked

        returns:
            Pass -> True, Fail -> false
        """
        cmd = f"ceph orch ps {host}"
        out = self.rados_obj.run_ceph_command(cmd=cmd, client_exec=True)
        for entry in out:
            if entry["daemon_type"] == "mon":
                log.debug("Mon daemon present on the host")
                return True
        log.debug("Mon daemon not present on the host")
        return False

    def remove_mon_service(self, host) -> bool:
        """
        Method to remove the mon service from the given host

        Args:
            host: name of the host from where mon needs to be removed

        returns:
            Pass -> True, Fail -> false
        """
        # Checking if mon service exists before removing

        if not self.check_mon_exists_on_host(host=host):
            log.info(f"Mon daemon not present on the host {host}. Returning Pass")
            return True
        cmd = f"ceph orch daemon rm mon.{host} --force"
        try:
            self.client.exec_command(sudo=True, cmd=cmd)
        except Exception as err:
            log.debug(f"Hit issue during command execution : {err}")
            log.debug("proceeding to check if the command execution was successful")

        # Sleeping for 5 seconds for the mon to be removed
        time.sleep(5)

        mon_exists = True
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=120)
        while datetime.datetime.now() < endtime:
            if not self.check_mon_exists_on_host(host=host):
                log.info(f"Mon daemon is removed on the host {host}.")
                mon_exists = False
                break
            log.info(
                f"Mon daemon still present on the host {host}. sleeping for 5 seconds and checking again"
            )
            time.sleep(5)
        if mon_exists:
            log.error(f"Mon daemon still present on the host {host}. Failed to remove")
            return False

        # Removing mon label from host if present
        labels = self.get_host_labels(host=host)
        if "mon" in labels:
            cmd = f"ceph orch host label rm {host} mon"
            self.client.exec_command(sudo=True, cmd=cmd)
            log.debug(f"Removed mon label from host : {host}")

        log.debug(f"Mon successfully removed from host : {host}")
        return True

    def add_mon_service(self, **kwargs) -> bool:
        """
        Method to remove the mon service from the given host

        Args:
            kwargs: the accepted KW args for the method are:
                host: Cluster object of the host from where mon needs to be Added
                add_label: Arg specifying if the mon label should be added to host or not.
                        Default is True.
                location_type: Name of the crush bucket type if needed (optional - Stretch mode)
                location_name: Name of the crush location type if needed (optional - Stretch mode)

        returns:
            Pass -> True, Fail -> false
        """
        # Checking if mon service exists before removing
        host = kwargs.get("host")
        add_label = kwargs.get("add_label", True)
        hostname = host.hostname
        host_ip = host.ip_address
        if self.check_mon_exists_on_host(host=hostname):
            log.info(
                f"Mon daemon Already present on the host {hostname}. Returning Pass"
            )
            return True

        log.debug(f"Adding mon daemon on the host : {hostname}")
        cmd = f"ceph mon add {hostname} {host_ip}"
        if kwargs.get("location_type", False):
            cmd = cmd + f" {kwargs['location_type']}={kwargs['location_name']}"
        log.debug(f"Adding new mon via cmd : {cmd}")
        self.client.exec_command(sudo=True, cmd=cmd)

        # Sleeping for 10 seconds for the mon to be added
        time.sleep(10)
        cmd = f"ceph orch daemon add mon {hostname}"
        self.client.exec_command(sudo=True, cmd=cmd)

        # Sleeping for 10 seconds for the mon to be added
        time.sleep(10)

        if not self.check_mon_exists_on_host(host=hostname):
            log.info(f"Mon daemon not present on the host {hostname}. Returning Fail")
            return False
        log.info(f"Mon service running on host {hostname} post addition")

        if add_label:
            # Adding mon label on host if not present
            labels = self.get_host_labels(host=hostname)
            if "mon" not in labels:
                cmd = f"ceph orch host label add {hostname} mon"
                self.client.exec_command(sudo=True, cmd=cmd)
                log.debug(f"Added mon label from host : {hostname}")

        log.debug(f"Mon successfully added on host : {hostname}")
        return True

    def get_mon_quorum_leader(self) -> str:
        """
        Fetches mon details and returns the name of quorum leader
        Returns: name of the leader mon in quorum
        """
        cmd = "ceph mon stat"
        quorum = self.rados_obj.run_ceph_command(cmd)
        return quorum["leader"]

    def get_mon_quorum_hosts(self) -> list:
        """
        Method to fetch the current hosts in mon quorum

        returns:
            List of hostnames that are present in the mon quorum
        """
        cmd = "ceph mon stat"
        quorum = self.rados_obj.run_ceph_command(cmd)
        return [entry["name"] for entry in quorum["quorum"]]

    def set_tiebreaker_mon(self, host) -> bool:
        """
        Sets the passed host mon as the new tiebreaker mon daemon in stretch mode

        Args:
            host: name of the host which should be set as the tiebreaker mon

        returns:
            Pass -> True, Fail -> false
        """
        cmd = f"ceph mon set_new_tiebreaker {host}"
        self.client.exec_command(sudo=True, cmd=cmd)

        # Checking if the tiebreaker mon is added successfully
        cmd = "ceph mon dump"
        out = self.rados_obj.run_ceph_command(cmd=cmd, client_exec=True)
        log.debug(f"The mon dump is : {out}")
        if host != out["tiebreaker_mon"]:
            log.error(
                f"New tiebreaker mon not set to {host}. mon set on cluster is : {out['tiebreaker_mon']}"
            )
            return False
        log.info("Successfully set the new tiebreaker mon on cluster")
        return True
