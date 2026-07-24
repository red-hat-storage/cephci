from typing import Dict, Union

from .typing_ import DaemonProtocol, OrchProtocol


class RedeployMixin:
    def redeploy(self: Union[OrchProtocol, DaemonProtocol], config: Dict):
        """
        Execute the add method using the object's service name.

        Args:
            config (Dict):     Key/value pairs passed from the test suite.

        Example::

            pos_args        - List to be added as positional params

            config:
              service: mon
              command: reconfig
              base_cmd_args:
                verbose: true

        """
        return self.op("redeploy", config)
