"""Cephadm orchestration client-keyring operations.

Syntax:
orch client-keyring ls [--format {plain|json|json-pretty|yaml}]
orch client-keyring rm <entity>
orch client-keyring set <entity> <placement> [<owner>] [<mode>]
"""

from .common import config_dict_to_string
from .orch import Orch


class ClientKeyringOpFailure(Exception):
    pass


class ClientKeyring(Orch):
    SERVICE_NAME = "client-keyring"

    def ls(self, config):
        """
        List the client keyrings available
        Args:
          config (Dict): list client-keyring configuration
        Example::
             config:
                service: client-keyring
                command: ls
                args:
                  format: json-pretty
        """
        cmd = ["ceph", "orch", "client-keyring", "ls"]

        if config.get("args"):
            cmd.append(config_dict_to_string(config["args"]))

        return self.shell(args=cmd)

    def rm(self, config):
        """
        Remove client-keyring from cluster
        Args:
          config (Dict): Remove client-keyring configuration
        Example::
             config:
                service: client-keyring
                command: rm
                entity: "client.1"
                nodes:
                    - "nodex"
        """
        cmd = ["ceph", "orch", "client-keyring", "rm"]

        entity = config.get("entity")
        cmd.append(entity)

        return self.shell(args=cmd)

    def set(self, config):
        """
        Put a keyring under management
        Args:
          config (Dict): set client-keyring configuration
        Example::
              config:
                service: "client-keyring"
                command: "set"
                entity: "client.rbd"
                placement: "rbd-client"
                owner: "107:107"
                mode: "640"
        """
        cmd = ["ceph", "orch", "client-keyring", "set"]

        entity = config.get("entity")
        placement = config.get("placement")
        owner = config.get("owner")
        mode = config.get("mode")

        cmd.extend([entity, f"label:{placement}", f"--owner {owner}", f"--mode {mode}"])

        return self.shell(args=cmd)
