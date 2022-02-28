from typing import Dict, Union

from .typing_ import DaemonProtocol, OrchProtocol


class RestartMixin:
    def restart(self: Union[OrchProtocol, DaemonProtocol], config: Dict):
        """
        Execute the restart method using the object's service name.

        Args:
            config (Dict):     Key/value pairs passed from the test suite.

        Example::

            pos_args - List to be added as positional params

            config:
              service: mon
              command: restart
              base_cmd_args:
                verbose: true

        """
        return self.op("restart", config)
