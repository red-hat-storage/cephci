"""Module that interfaces with ceph orch upgrade CLI."""

from datetime import datetime, timedelta
from json import loads
from time import sleep
from typing import Dict

from utility.log import Log

from .common import config_dict_to_string
from .typing_ import OrchProtocol

LOG = Log(__name__)


class UpgradeFailure(Exception):
    pass


class UpgradeMixin:
    """CLI that list daemons known to orchestrator."""

    def start_upgrade(self: OrchProtocol, config: Dict):
        """
        Execute the command ceph orch start <args>.

        Args:
            config (Dict): The key/value pairs passed from the test case.

        Example::

            Testing ceph orch upgrade

            config:
                service: upgrade
                command: start
                base_cmd_args:
                    verbose: true
                    format: json | json-pretty | xml | xml-pretty | plain | yaml
                args:
                    image: "latest"         # pick latest image from config
                    version: 16.0.0         # Not supported

        """
        cmd = ["ceph", "orch"]

        if config and config.get("base_cmd_args"):
            base_cmd_args = config_dict_to_string(config["base_cmd_args"])
            cmd.append(base_cmd_args)

        cmd.append("upgrade start")

        args = config.get("args")
        if "image" in args:
            cmd.append(f"--image {self.config.get('container_image')}")

        return self.shell(args=cmd)

    def upgrade_status(self: OrchProtocol):
        """
        Execute the command ceph orch status.

        Returns:
            upgrade Status (Dict)

        """
        out, _ = self.shell(args=["ceph", "orch", "upgrade", "status"])
        return loads(out)

    def upgrade_check(self: OrchProtocol, image):
        """
        Check the service versions available against targeted version

        Args:
            image (Str): image name

        Returns:
            status (Dict)
        """
        out, _ = self.shell(
            args=["ceph", "orch", "upgrade", "check", f"--image {image}"]
        )
        LOG.info("check upgrade : %s" % out)
        return loads(out)

    def monitor_upgrade_status(self, timeout=3600):
        """
        Monitor upgrade status

        Args:
            timeout (int):  Timeout in seconds (default:3600)

        """
        interval = 7  # ceph orch down while upgrading mgr daemon BZ2313471
        end_time = datetime.now() + timedelta(seconds=timeout)

        while end_time > datetime.now():
            sleep(interval)
            out = self.upgrade_status()

            if not out["in_progress"]:
                LOG.info("Upgrade Complete...")
                break

            LOG.info("Status : %s" % out)
        else:
            raise UpgradeFailure("Upgrade did not complete or failed")
