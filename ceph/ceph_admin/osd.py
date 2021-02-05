"""
Module to deploy osd service and daemon(s).

this module deploy OSD service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
import json
import logging
from time import sleep

from ceph.ceph_admin.apply import Apply
from ceph.ceph_admin.daemon_mixin import DaemonMixin

logger = logging.getLogger(__name__)


class OSDServiceFailure(Exception):
    pass


class DevicesNotFound(Exception):
    pass


class OSDRole(Apply, DaemonMixin):
    __ROLE = "osd"

    def __get_available_devices(self):
        """
        Fetch all available devices using "orch device ls" command

        Returns:
            device_list: list of nodes with available devices

        device_list:
            node1: ["/dev/sda", "/dev/sdb"]
            node2: ["/dev/sda"]
        """
        out, _ = self.shell(
            remote=self.installer,
            args=["ceph", "orch", "device", "ls", "-f", "json"],
        )

        node_device_list = dict()
        for node in json.loads(out):
            if not node.get("devices"):
                continue
            devices = []
            for device in node.get("devices"):
                if device["available"] is True:
                    devices.append(device["path"])

            if devices:
                node_device_list.update({node["addr"]: devices})

        return node_device_list

    def apply_osd(self):
        """
        Deploy all available OSDs in the ceph cluster using
        "ceph orch apply osd --all-available-devices"

        """
        node_device_list = self.__get_available_devices()

        if not node_device_list:
            raise DevicesNotFound("No devices available to create OSD(s)")

        cmd = Apply.apply_cmd + [self.__ROLE]
        cmd.append("--all-available-devices")

        Apply.apply(
            self,
            role=self.__ROLE,
            command=cmd,
            placements=[],
        )

        # validate of osd(s)
        interval = 5
        timeout = self.TIMEOUT
        checks = timeout / interval

        while checks:
            checks -= 1

            out, _ = self.shell(
                remote=self.installer,
                args=["ceph", "orch", "ps", "-f", "json-pretty"],
            )

            out = json.loads(out)
            daemons = [i for i in out if i.get("daemon_type") == "osd"]

            deployed = 0
            for node, devices in node_device_list.items():
                count = 0
                for dmn in daemons:
                    if dmn["hostname"] == node and dmn["status_desc"] == "running":
                        count += 1

                logger.info(
                    "%s %s/%s osd daemon(s) up... Retries: %s"
                    % (node, count, len(devices), checks)
                )
                if count == len(devices):
                    deployed += 1

            if deployed == len(node_device_list):
                return True
            sleep(interval)

        raise OSDServiceFailure("OSDs are not up and running in hosts")

    def daemon_add_osd(self):
        raise NotImplementedError
