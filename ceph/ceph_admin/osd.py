"""Manage OSD service via cephadm CLI."""
import json
import logging
from time import sleep
from typing import Dict

from .apply import ApplyMixin
from .orch import Orch

LOG = logging.getLogger()


class OSDServiceFailure(Exception):
    pass


class DevicesNotFound(Exception):
    pass


class OSD(ApplyMixin, Orch):

    """Interface to ceph orch osd."""

    SERVICE_NAME = "osd"

    def apply(self, config: Dict) -> None:
        """
        Deploy the ODS service using the provided configuration.

        Args:
            config (Dict): Key/value pairs provided by the test case to create the service.

        Example::

            config:
                command: apply
                service: osd
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                args:
                    all-available-devices: true
                    dry-run: true
                    unmanaged: true

        """
        cmd = ["ceph orch device ls -f json"]
        self.shell(args=["ceph orch device ls --refresh"])
        logging.info("Sleeping for 60 seconds for disks to be discovered")
        sleep(60)
        out, _ = self.shell(args=cmd)

        node_device_dict = dict()
        for node in json.loads(out):
            if not node.get("devices"):
                continue

            devices = list()
            for device in node.get("devices"):
                devices.append(device["path"])

            if devices:
                node_device_dict.update({node["addr"]: devices})

        if not node_device_dict:
            raise DevicesNotFound("No devices available to create OSD(s)")

        if not config.get("args", {}).get("all-available-devices"):
            config["args"]["all-available-devices"] = True

        super().apply(config)

        # validate of osd(s)
        interval = 5
        timeout = self.TIMEOUT
        checks = timeout / interval

        while checks:
            checks -= 1

            out, _ = self.shell(
                args=["ceph", "orch", "ps", "-f", "json-pretty"],
            )

            out = json.loads(out)
            daemons = [i for i in out if i.get("daemon_type") == "osd"]

            deployed = 0
            for node, devices in node_device_dict.items():
                count = 0
                for dmn in daemons:
                    if dmn["hostname"] == node and dmn["status_desc"] == "running":
                        count += 1

                LOG.info(
                    "%s %s/%s osd daemon(s) up... Retries: %s"
                    % (node, count, len(devices), checks)
                )
                if count == len(devices):
                    deployed += 1

            if deployed == len(node_device_dict):
                return

            sleep(interval)

        raise OSDServiceFailure("OSDs are not up and running in hosts")
