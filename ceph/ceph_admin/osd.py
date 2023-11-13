"""Manage OSD service via cephadm CLI."""
import json
from time import sleep
from typing import Dict

from utility.log import Log

from .apply import ApplyMixin
from .common import config_dict_to_string
from .orch import Orch

LOG = Log(__name__)


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
        LOG.info("Sleeping for 120 seconds for disks to be discovered")
        sleep(120)
        out, _ = self.shell(args=cmd)

        node_device_dict = dict()
        for node in json.loads(out):
            if not node.get("devices"):
                continue

            devices = {"available": [], "unavailable": []}
            for device in node.get("devices"):
                # avoid considering devices which is less than 5GB
                if not device.get(
                    "rejected_reasons"
                ) or "Insufficient space (<5GB)" not in device.get(
                    "rejected_reasons", []
                ):
                    if device["available"]:
                        devices["available"].append(device["path"])
                        continue
                    devices["unavailable"].append(device["path"])

            if devices["available"]:
                node_device_dict.update({node["addr"]: devices})

        if config.get("verify", True):
            if not node_device_dict:
                raise DevicesNotFound("No devices available to create OSD(s)")

        if not config.get("args", {}).get("all-available-devices"):
            config["args"]["all-available-devices"] = True

        # print out discovered device list
        out, _ = self.shell(args=["ceph orch device ls -f yaml"])
        LOG.info(f"Node device list : {out}")

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

                count = count - len(devices["unavailable"])

                LOG.info(
                    "%s %s/%s osd daemon(s) up... Retries: %s"
                    % (node, count, len(devices["available"]), checks)
                )
                if count == len(devices["available"]):
                    deployed += 1

            if deployed == len(node_device_dict):
                return

            sleep(interval)

        raise OSDServiceFailure("OSDs are not up and running in hosts")

    def rm_status(self, config: Dict):
        """
        Execute the command ceph orch osd rm status.

        Args:
            config (Dict): OSD Remove status configuration parameters

        Returns:
          output, error   returned by the command.

        Example::

            config:
                command: rm status
                base_cmd_args:
                    verbose: true
                args:
                    format: json-pretty
        """

        base_cmd = ["ceph", "orch", "osd", "rm", "status"]
        if config.get("base_cmd_args"):
            base_cmd.append(config_dict_to_string(config["base_cmd_args"]))

        if config and config.get("args"):
            args = config.get("args")
            base_cmd.append(config_dict_to_string(args))

        return self.shell(args=base_cmd)

    def rm(self, config: Dict):
        """
        Execute the command ceph orch osd rm <OSD ID> .

        Args:
            config (Dict): OSD Remove configuration parameters

        Returns:
          output, error   returned by the command.

        Example::

            config:
                command: rm
                base_cmd_args:
                    zap: true
                pos_args:
                    - 1
        """
        base_cmd = ["ceph", "orch", "osd"]
        base_cmd.append("rm")
        osd_id = config["pos_args"][0]
        base_cmd.append(str(osd_id))
        if config.get("base_cmd_args"):
            base_cmd.append(config_dict_to_string(config["base_cmd_args"]))
        self.shell(args=base_cmd)

        check_osd_id_dict = {
            "args": {"format": "json"},
        }

        while True:
            # "ceph orch osd rm status -f json"
            # condition
            # continue loop if OSD_ID present
            # if not exit the loop
            out, _ = self.rm_status(check_osd_id_dict)

            try:
                status = json.loads(out)
                for osd_id_ in status:
                    if int(osd_id_["osd_id"]) == int(osd_id):
                        LOG.info(f"OSDs removal in progress: {osd_id_}")
                        sleep(2)
                        continue
                    else:
                        break

            except json.decoder.JSONDecodeError:
                break

        if config.get("validate", True):
            # validate OSD removal
            out, verify = self.shell(
                args=["ceph", "osd", "tree", "-f", "json"],
            )
            out = json.loads(out)
            for id_ in out["nodes"]:
                if int(id_["id"]) == int(osd_id):
                    LOG.error("OSD Removed ID found")
                    raise AssertionError("fail, OSD is present still after removing")
        LOG.info(f" OSD {osd_id} Removal is successful")

    def out(self, config: Dict):
        """
        Execute the command ceph osd out.
        Args:
            config (Dict): OSD 'out' configuration parameters
        Returns:
          output, error   returned by the command.
        Example::
            config:
                command: out
                base_cmd_args:
                    verbose: true
                pos_args:
                    - 4
        """
        base_cmd = ["ceph", "osd", "out"]
        if config.get("base_cmd_args"):
            base_cmd.append(config_dict_to_string(config["base_cmd_args"]))
        osd_id = config["pos_args"][0]
        base_cmd.append(str(osd_id))
        return self.shell(args=base_cmd)

    def osd_in(self, config: dict):
        """
        Execute the command ceph osd in.
        Args:
            config (Dict): OSD 'in' configuration parameters
        Returns:
          output, error   returned by the command.
        Example::
            config:
                command: in
                base_cmd_args:
                    verbose: true
                pos_args:
                    - 4
        """
        base_cmd = ["ceph", "osd", "in"]
        if config.get("base_cmd_args"):
            base_cmd.append(config_dict_to_string(config["base_cmd_args"]))
        osd_id = config["pos_args"][0]
        osd_ids = config["pos_args"][1]
        any_all = config["pos_args"][2]

        if any_all:
            base_cmd.append("all")
        else:
            if osd_id is not None and osd_id.is_integer():
                base_cmd.append(str(osd_id))
            if osd_ids is not None:
                base_cmd.extend([str(ele) for ele in osd_ids])
        return self.shell(args=base_cmd)

    def flag(self, config: dict):
        """
        Execute the command ceph osd set/unset flag.
        Args:
            config (Dict): OSD flag configuration parameters
                command: set/unset
                base_cmd_args:
                    verbose: true
                flag: value of flag
        """
        command = config.get("command")
        flag = config.get("flag")
        cmd = f"ceph osd {command} {flag}"
        return self.shell(args=[cmd])
