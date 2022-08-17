import datetime
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
              args:
                service_name: mgr
                verify: true

        """
        restart_init_time = datetime.datetime.utcnow()
        rc = self.op("restart", config)
        args = config.get("args")
        verify = args.pop("verify", True)
        if verify:
            rc = self.check_service_restart(
                service_name=args.get("service_name"),
                restart_init_time=restart_init_time,
            )
        return rc
