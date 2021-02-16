"""
Module that interacts with the orchestrator CLI.

Provide the interfaces to ceph orch and in turn manage the orchestration engine.
"""
import logging
from datetime import datetime, timedelta
from json import loads
from time import sleep
from typing import List

from ceph.ceph import ResourceNotFoundError

from .ceph import CephCLI
from .ls import LSMixin
from .ps import PSMixin

LOG = logging.getLogger()


class Orch(LSMixin, PSMixin, CephCLI):
    """Represent ceph orch command."""

    direct_calls = ["ls", "ps"]

    def check_service_exists(
        self, service_name: str, ids: List[str], timeout: int = 300, interval: int = 5
    ) -> bool:
        """
        Verify the provided service is running for the given list of ids.

        Args:
            service_name:   The name of the service to be checked.
            ids:            The list of daemons to be checked for that service.
            timeout:        In seconds, the maximum allowed time. By default 5 minutes
            interval:      In seconds, the polling interval time.

        Returns:
            True if the service and the list of daemons are running else False.
        """
        end_time = datetime.now() + timedelta(seconds=timeout)

        while end_time > datetime.now():
            sleep(interval)
            out, err = self.ps({"base_cmd_args": {"format": "json"}})
            out = loads(out)
            daemons = [d for d in out if d.get("daemon_type") == service_name]

            count = 0
            for _id in ids:
                for daemon in daemons:
                    if (
                        _id in daemon["daemon_id"]
                        and daemon["status_desc"] == "running"
                    ):
                        count += 1

            LOG.info("%s/%s %s daemon(s) up... retrying", count, len(ids), service_name)

            if count == len(ids):
                return True

        # Identify the failure
        out, err = self.ls()
        for item in loads(out):
            if (
                service_name in item.get("service_type")
                and item["status"].get("running") == 0
            ):
                LOG.error("Service status(es): %s", item)
                LOG.error("Service event(s): %s", item["events"])

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
