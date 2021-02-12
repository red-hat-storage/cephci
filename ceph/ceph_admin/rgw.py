"""
Module to deploy RGW service and individual daemon(s).

this module deploy RGW service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
from .apply import ApplyMixin
from .orch import Orch


class RGW(ApplyMixin, Orch):
    SERVICE_NAME = "rgw"

    def apply(self, **config):
        """
        Deploy the RGW service using the provided data.

        Args:
            config: test arguments

        config:
            command: apply
            service: rgw
            prefix_args:
                realm: india
                zone: south
            args:
                label: rgw_south    # either label or node.
                nodes:
                    - node1
                limit: 3    # no of daemons
                sep: " "    # separator to be used for placements
        """
        prefix_args = config.pop("prefix_args")

        realm = prefix_args.get("realm", "realm")
        zone = prefix_args.get("zone", "zone")

        config[prefix_args] = [realm, zone]
        super().apply(**config)
