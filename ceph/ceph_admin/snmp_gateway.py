"""Module to deploy SNMP-Gateway service and individual daemon(s)."""

from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class SnmpGateway(ApplyMixin, Orch):
    SERVICE_NAME = "snmp-gateway"

    def apply(self, config: Dict) -> None:
        """
        Deploy the SNMP-Gateway service using the provided configuration.

        Args:
            config (Dict): Key/value pairs provided by the test case to create the service.

        Example::

          - config:
              command: apply
              service: orch
              specs:
                - service_type: snmp-gateway
                  service_name: snmp-gateway
                  placement:
                    count: 1
                  spec:
                    credentials:
                      snmp_v3_auth_username: <username>>
                      snmp_v3_auth_password: <password>>
                    port: <port>
                    snmp_destination: <destination_node>>
                    snmp_version: <version>>

        """
        super().apply(config=config)
