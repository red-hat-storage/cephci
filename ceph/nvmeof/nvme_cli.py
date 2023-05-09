from ceph.ceph_admin.common import config_dict_to_string
from cli import Cli


class NVMeCLI(Cli):
    """NVMe CLI commands.

    - Need to initialize Cli class with Ceph node object.
    - NQN: NVMe qualified name.
    """

    def configure(self):
        """Install NVME CLI."""
        configure_cmds = [
            ("yum install -y nvme-cli fio", True),
            ("modprobe nvme-fabrics", True),
        ]
        for cmd in configure_cmds:
            self.execute(*cmd)

    def discover(self, **kwargs):
        """Discover the subsystems.

        Example::

            kwargs:
                transport: tcp                  # Transport protocol
                traddr: IP address              # Transport address
                trsvcid: Transport port number  # Transport port number
                output-format: json
        """
        return self.execute(
            cmd=f"nvme discover {config_dict_to_string(kwargs)}",
            sudo=True,
        )

    def connect(self, **kwargs):
        """Connect to subsystem.

        Example::

            kwargs:
                transport: tcp                  # Transport protocol
                traddr: IP address              # Transport address
                trsvcid: Transport port number  # Transport port number
                nqn: Subsystem NQN Id           # Subsystem NQN
        """
        return self.execute(
            cmd=f"nvme connect {config_dict_to_string(kwargs)}",
            sudo=True,
        )

    def list(self, **kwargs):
        """List the NVMe Targets under subsystems.

        Example::

            kwargs:
                output-format: json             # output format
        """
        return self.execute(cmd=f"nvme list {config_dict_to_string(kwargs)}", sudo=True)
