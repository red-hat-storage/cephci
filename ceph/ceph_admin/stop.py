from typing import Dict

from .typing_ import OrchProtocol


class StopMixin:
    def stop(self: OrchProtocol, config: Dict):
        """
        Execute the stop method using the object's service name.

        Args:
            config (Args):     Key/value pairs passed from the test suite.

        Example::

            pos_args        - List to be added as positional params

            config:
              service: mon
              command: stop
              base_cmd_args:
                verbose: true

        """
        return self.op("stop", config)
