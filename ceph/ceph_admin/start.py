from typing import Dict

from .typing_ import OrchProtocol


class StartMixin:
    def start(self: OrchProtocol, config: Dict):
        """
        Execute the start method using the object's service name.

        Args:
            config (Dict):     Key/value pairs passed from the test suite.

        Example::

            pos_args        - List to be added as positional params

            config:
              service: mon
              command: start
              base_cmd_args:
                verbose: true

        """
        return self.op("start", config)
