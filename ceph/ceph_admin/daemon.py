"""CephADM orchestration Daemon operations."""
import logging

from .add import AddMixin
from .common import config_dict_to_string
from .orch import Orch

logger = logging.getLogger(__name__)


class Daemon(AddMixin, Orch):
    def op(self, op, config):
        """
        Execute the command ceph orch daemon <start|stop|restart|reconfigure|redeploy> <service>.
        Args:
            config: command and service are passed from the test case.
            op: operation parameters ex: restart|start|stop|reconfigure|redeploy
        Example:
            Testing ceph orch daemon restart mon
        config:
          command: restart
          service: mon
        Returns:
          output, error   returned by the command.

        example
            config:
                command: start
                base_cmd_args:
                    verbose: true
                pos_args:
                    - service_name
        """

        base_cmd = ["ceph", "orch", "daemon"]
        if config.get("base_cmd_args"):
            base_cmd.append(config_dict_to_string(config["base_cmd_args"]))

        base_cmd.append(op)
        base_cmd.extend(config.get("pos_args"))
        return self.shell(args=base_cmd)
