"""Module that interfaces with ceph orch  CLI."""
from typing import Dict, Optional

from .common import config_dict_to_string
from .typing_ import OrchProtocol


def service_op(cls, op, config):
    """
       Execute the command ceph orch <start|stop|restart|reconfigure|redeploy> <service>.

      Args:
          cls: orchestration class
          config: command and service are passed from the test case.
          op: operation parameters ex: restart|start|stop|reconfigure|redeploy

      Example:
          Testing ceph orch restart mon

    config:
      command: restart
      service: mon

      Returns:
          output, error   returned by the command.
    """

    base_cmd = ["ceph", "orch"]

    if config.get("base_cmd_args"):
        base_cmd_args_str = config_dict_to_string(config.get("base_cmd_args"))
        base_cmd.append(base_cmd_args_str)

    base_cmd.append(op)
    base_cmd.append(config["service"])

    return cls.shell(args=base_cmd)


class ServiceOpsMixin:
    """CLI for service operations using orch commands."""

    def restart(self: OrchProtocol, config: Optional[Dict] = None):
        """
           Execute the command ceph orch restart  <args>.

          Args:
              config: command and service are passed from the test case.

          Example:
              Testing ceph orch restart mon

        config:
          command: restart
          service: mon

          Returns:
              output, error   returned by the command.
        """

        return service_op(self, "restart", config)

    def reconfigure(self: OrchProtocol, config: Optional[Dict] = None):
        """
           Execute the command ceph orch reconfigure  <args>.

          Args:
              config: command and service are passed from the test case.

          Example:
              Testing ceph orch reconfigure mon

        config:
          command: reconfigure
          service: mon

          Returns:
              output, error   returned by the command.

        """
        return service_op(self, "reconfigure", config)

    def redeploy(self: OrchProtocol, config: Optional[Dict] = None):
        """
           Execute the command ceph orch redeploy  <args>.

          Args:
              config: command and service are passed from the test case.

          Example:
              Testing ceph orch redeploy mon

        config:
          command: redeploy
          service: mon

          Returns:
              output, error   returned by the command.
        """
        return service_op(self, "redeploy", config)

    def start(self: OrchProtocol, config: Optional[Dict] = None):
        """
           Execute the command ceph orch start  <args>.

          Args:
              config: command and service are passed from the test case.

          Example:
              Testing ceph orch start mon

        config:
          command: start
          service: mon

          Returns:
              output, error   returned by the command.
        """

        return service_op(self, "start", config)

    def stop(self: OrchProtocol, config: Optional[Dict] = None):
        """
           Execute the command ceph orch stop  <args>.

          Args:
              config: command and service are passed from the test case.

          Example:
              Testing ceph orch stop mon

        config:
          command: stop
          service: mon

          Returns:
              output, error   returned by the command.
        """

        return service_op(self, "stop", config)
