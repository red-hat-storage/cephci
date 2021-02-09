"""
Module to deploy ceph role service(s) using orchestration command
"ceph orch apply <role> [options] --placement '<placements>' "

this module inherited where service deployed using "apply" operation.
and also validation using orchestration process list response
"""
import logging
from time import sleep

from ceph.ceph import ResourcesNotFoundError

logger = logging.getLogger(__name__)


class ServiceRemoveFailure(Exception):
    pass


class Apply:

    apply_cmd = ["ceph", "orch", "apply"]
    remove_cmd = ["ceph", "orch", "rm"]

    def apply(self, role, command, placements):
        """
        Cephadm service deployment using apply command

        Args:
            role: daemon name
            command: command to be executed
            placements: hosts/ID(s)
        """
        self.shell(
            remote=self.installer,
            args=command,
        )

        if placements:
            assert self.check_exist(
                daemon=role,
                ids=placements,
            )

    def orch_remove(self, service_name, check_status=True):
        """
        Remove service using "ceph orch rm <svc-id>" command
        Args:
            service_name: service name
            check_status: check command status
        """
        # check service existence
        if not self.get_role_service(service_name=service_name):
            raise ResourcesNotFoundError(
                f"Service {service_name} not found in services or Not provided"
            )

        # Remove service
        logger.info("Removing %s service", service_name)

        cmd = self.remove_cmd + [service_name]
        out, err = self.shell(
            remote=self.installer,
            args=cmd,
            check_status=check_status,
        )

        # Todo: need to validate expected response from mon, mgr
        if not check_status:
            return out, err

        interval = 5
        timeout = self.TIMEOUT
        checks = timeout / interval

        while checks:
            checks -= 1
            if not self.get_role_service(service_name):
                return True
            logger.info(
                "%s service is still up... Retries: %s" % (service_name, checks)
            )
            sleep(interval)

        raise ServiceRemoveFailure(f"{service_name} service did not get removed")
