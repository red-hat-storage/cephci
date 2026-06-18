"""Module that interfaces with ceph orch upgrade CLI."""

from datetime import datetime, timedelta
from json import JSONDecodeError, loads
from time import sleep
from typing import Dict

from looseversion import LooseVersion
from packaging.version import Version

from ceph.ceph import CommandFailed
from ceph.waiter import WaitUntil
from utility.log import Log

from ..utils import get_daemon_versions
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

        skip_license = None
        if (args and "skip_license" in args) or config.get("skip_license"):
            skip_license = True

        # IBM Storage Ceph 9.1 and greater would require license acceptance
        # There is '--automatically-accept-license' option
        if (
            config.get("product") == "ibm"
            and LooseVersion(str(config.get("release"))) >= LooseVersion("9.1")
            and not skip_license
        ):
            LOG.debug("Ceph product: %s", config.get("product"))
            LOG.debug("Target version: %s", config.get("release"))
            mgr_version_common, mgr_version_downstream = get_daemon_versions(
                node=self, daemon_type="mgr"
            )
            LOG.debug("MGR common version: %s", mgr_version_common)
            LOG.debug("MGR downstream version: %s", mgr_version_downstream)
            common_ver_check = mgr_version_common and Version(
                mgr_version_common
            ) >= Version("20.2.1")
            downstream_ver_check = mgr_version_downstream and Version(
                mgr_version_downstream
            ) >= Version("9.9.1.0")
            if common_ver_check or downstream_ver_check:
                cmd.append("--automatically-accept-license")

        return self.shell(args=cmd)

    def upgrade_status(self: OrchProtocol, timeout: int = 600, interval: int = 10):
        """
        Execute the command ceph orch status.

        Returns:
            upgrade Status (Dict)

        """
        cmd = ["ceph", "orch", "upgrade", "status"]

        # these additional checks are in place because the fix is currently
        # available in only 9.1, once back-ported to all relevant versions,
        # retry mechanism can be removed completely
        # determine mgr version to decide retry logic
        # check if active mgr daemon has upgraded to 9.1 or above
        out, _ = self.shell(args=["ceph", "mgr", "stat"])
        active_mgr = loads(out)["active_name"]
        LOG.debug("Active Mgr for the cluster: %s", active_mgr)
        mgr_version_common, mgr_version_downstream = get_daemon_versions(
            node=self, daemon_type="mgr", daemon_id=active_mgr
        )
        LOG.debug("MGR common version: %s", mgr_version_common)
        LOG.debug("MGR downstream version: %s", mgr_version_downstream)

        common_ver_check = mgr_version_common and Version(
            mgr_version_common
        ) >= Version("20.2.1")
        downstream_ver_check = mgr_version_downstream and Version(
            mgr_version_downstream
        ) >= Version("9.9.1.0")

        needs_retry = not (common_ver_check or downstream_ver_check)

        if not needs_retry:
            LOG.debug("------- Skipping retry workaround -------")
            out, _ = self.shell(args=cmd)
        else:
            # Retry upgrade command for interval time until it pass till timeout
            for w in WaitUntil(timeout=timeout, interval=interval):
                try:
                    out, _ = self.shell(args=cmd)
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
