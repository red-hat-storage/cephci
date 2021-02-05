"""
Module to deploy ceph role service(s) using orchestration command
"ceph orch apply <role> [options] --placement '<placements>' "

this module inherited where service deployed using "apply" operation.
and also validation using orchestration process list response
"""


class Apply:

    apply_cmd = ["ceph", "orch", "apply"]

    def apply(self, role, command, placements):
        """
        Cephadm service deployment using apply command

        Args:
            role: daemon name
            command: command to be executed
            placements: hosts/ID(s)
        """
        self.shell(
            remote=self.installer,
            args=command,
        )

        if placements:
            assert self.check_exist(
                daemon=role,
                ids=placements,
            )
