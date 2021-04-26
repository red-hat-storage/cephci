"""
Module that interacts with the orchestrator CLI.

Provide the interfaces to ceph orch and in turn manage the orchestration engine.
"""
import logging
from datetime import datetime, timedelta
from json import loads
from time import sleep

from ceph.ceph import ResourceNotFoundError

from .ceph import CephCLI
from .ls import LSMixin
from .ps import PSMixin
from .remove import RemoveMixin
from .upgrade import UpgradeMixin

LOG = logging.getLogger()


class Orch(LSMixin, PSMixin, RemoveMixin, UpgradeMixin, CephCLI):
    """Represent ceph orch command."""

    direct_calls = ["ls", "ps"]

    def get_hosts_by_label(self, label: str):
        """
        Fetch host object by label attached to it.
        Args:
            label: name of the label
        Returns:
            hosts
        """
        out, _ = self.shell(args=["ceph", "orch", "host", "ls", "--format=json"])
        return [node for node in loads(out) if label in node.get("labels")]

    def check_service_exists(
        self, service_name: str, timeout: int = 300, interval: int = 5
    ) -> bool:
        """
        Verify the provided service is running for the given list of ids.

        Args:
            service_name:   The name of the service to be checked.
            timeout:        In seconds, the maximum allowed time. By default 5 minutes
            interval:      In seconds, the polling interval time.

        Returns:
            True if the service and the list of daemons are running else False.
        """
        end_time = datetime.now() + timedelta(seconds=timeout)
        check_status_dict = {
            "base_cmd_args": {"format": "json"},
            "args": {"service_name": service_name, "refresh": True},
        }

        while end_time > datetime.now():
            sleep(interval)
            out, err = self.ls(check_status_dict)
            out = loads(out)[0]
            running = out["status"]["running"]
            count = out["status"]["size"]

            LOG.info("%s/%s %s daemon(s) up... retrying", running, count, service_name)

            if count == running:
                return True

        # Identify the failure
        out, err = self.ls(check_status_dict)
        LOG.error(f"{service_name} failed with \n{out[0]['events']}")

        return False

    def get_role_service(self, service_name: str) -> str:
        """
        Get service info by name.

        Args:
            service_name: service name

        Returns:
            service

        Raises:
            ResourceNotFound: when no resource with the provided is matched.
        """
        out, _ = self.ls()

        for svc in loads(out):
            if service_name in svc.get("service_name"):
                return svc

        raise ResourceNotFoundError(f"No service names matched {service_name}")

    def check_service(
        self, service_name: str, timeout: int = 300, interval: int = 5, exist=True
    ) -> bool:
        """
        check service existence based on the exist parameter

        if exist is set, then validate its presence.
        otherwise, for its removal.

        Args:
            service_name: service name
            timeout: timeout in seconds
            interval: interval in seconds
            exist: boolean
        Returns:
            service

        """
        end_time = datetime.now() + timedelta(seconds=timeout)

        while end_time > datetime.now():
            sleep(interval)
            out, err = self.ls({"base_cmd_args": {"format": "json"}})
            out = loads(out)
            service = [d for d in out if d.get("service_name") == service_name]

            if service_name not in service and not exist:
                return True
            elif service_name in service and exist:
                return True
            LOG.info("[%s] check for existence: %s, retrying" % (service_name, exist))

        return False
