"""Cephadm orchestration client-keyring operations.

Syntax:
orch client-keyring ls [--format {plain|json|json-pretty|yaml}]
orch client-keyring rm <entity>
orch client-keyring set <entity> <placement> [<owner>] [<mode>]
"""
import random

from .common import config_dict_to_string
from .orch import Orch


class ClientKeyringOpFailure(Exception):
    pass


class ClientKeyring(Orch):

    SERVICE_NAME = "client-keyring"

    def ls(self, config):
        """
        List the client keyrings available
        args:
            format: json-pretty
        Returns:
            json output of client-keyring list (List)
        """
        cmd = ["ceph", "orch", "client-keyring", "ls"]

        if config.get("args"):
            cmd.append(config_dict_to_string(config["args"]))

        return self.shell(args=cmd)

    def rm(self, config):

        """
        Remove client-keyring from cluster
        Args:
          config (Dict): Remove host configuration
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
          config (Dict): Remove host configuration
        Example::
              config:
                service: "client-keyring"
                command: "set"
                entity: "client.rbd"
                placement: "rbd-client"
                owner: "107:107"
                mode: "640"
                caps:
                    mon: "profile rbd"
                    osd: "profile rbd"
                    mgr: "profile rbd"
        """
        cmd = ["ceph", "orch", "client-keyring", "set"]

        entity = config.get("entity")
        placement = config.get("placement")
        owner = config.get("owner")
        mode = config.get("mode")

        self.get_or_create(config)
        cmd.extend([entity, f"label:{placement}", f"--owner {owner}", f"--mode {mode}"])

        return self.shell(args=cmd)

    def get_or_create(self, config):

        cmd = ["ceph", "auth", "get-or-create"]

        entity = config["entity"]
        pool_name = config.get("pool_name")
        pg_num = config.get("pg_num")
        pgp_num = config.get("pgp_num")

        cmd.append(entity)

        if not pool_name:
            pool_name = "test_pool" + str(random.randint(10, 999))
        if not pg_num:
            pg_num = 128
        if not pgp_num:
            pgp_num = 128

        pool_create_cmd = [f"sudo ceph osd pool create {pool_name} {pg_num} {pgp_num}"]
        self.shell(args=pool_create_cmd)

        for k, v in config.get("caps", {}).items():
            if k == "osd":
                cmd.append(f"{k} '{v} pool={pool_name}'")
            else:
                cmd.append(f"{k} '{v}'")

        return self.shell(args=cmd)
