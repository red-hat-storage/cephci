"""Module that interfaces with ceph orch upgrade CLI."""

from datetime import datetime, timedelta
from json import JSONDecodeError, loads
from time import sleep
from typing import Dict

from ceph.ceph import CommandFailed
from ceph.waiter import WaitUntil
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
                    daemon_types: "mon,mgr" # Optional: comma-separated daemon types
                    hosts: "host1,host2"    # Optional: comma-separated host names
                    services: "mon,mgr"     # Optional: comma-separated service names

        """
        cmd = ["ceph", "orch"]

        if config and config.get("base_cmd_args"):
            base_cmd_args = config_dict_to_string(config["base_cmd_args"])
            cmd.append(base_cmd_args)

        cmd.append("upgrade start")

        args = config.get("args")
        if args and "image" in args:
            cmd.append(f"--image {self.config.get('container_image')}")

        # Add optional daemon_types argument
        if args and "daemon_types" in args:
            cmd.append(f"--daemon_types {args['daemon_types']}")

        # Add optional hosts argument
        if args and "hosts" in args:
            cmd.append(f"--hosts {args['hosts']}")

        # Add optional services argument
        if args and "services" in args:
            cmd.append(f"--services {args['services']}")

        return self.shell(args=cmd)

    def upgrade_status(self: OrchProtocol, timeout: int = 600, interval: int = 10):
        """
        Execute the command ceph orch status.

        Returns:
            upgrade Status (Dict)

        """
        # Retry upgrade command for interval time until it pass till timeout
        for w in WaitUntil(timeout=timeout, interval=interval):
            try:
                out, _ = self.shell(args=["ceph", "orch", "upgrade", "status"])
            except CommandFailed as e:
                msg = f"Upgrade command status failed due to error - {e},"
                msg += f"\n\nRetrying upgrade status after {interval}..."
                LOG.info(msg)

                # Continue in case command fails
                continue

            # Break in case no exception raised
            break

        # Check if command is even after timeout
        if w.expired:
            msg = f"Upgrade status failed even after {timeout} sec "
            msg += "due to mgr loading issue. "
            msg += "Refer Bzs 2314146, 2313471, 2361844."
            raise CommandFailed(msg)

        try:
            return loads(out)
        except JSONDecodeError:
            pass  # Not valid JSON, move to next check

        if "There are no upgrades in progress currently." in out:
            return {"in_progress": False}

        raise CommandFailed(
            "Command 'ceph orch upgrade status' returned unexpected output: '{}'".format(
                out
            )
        )

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
