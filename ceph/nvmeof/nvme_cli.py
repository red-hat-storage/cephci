import json

from ceph.ceph_admin.common import config_dict_to_string
from cli import Cli
from utility.log import Log

LOG = Log(__name__)


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

    def list_spdk_drives(self):
        """List the NVMe Targets only SPDK drives.

        Return:
            Dict: Dict of SPDK drives else empty list
        """
        json_kwargs = {"output-format": "json"}
        out, _ = self.list(**json_kwargs)
        devs = json.loads(out)["Devices"]
        return [
            dev for dev in devs if dev["ModelNumber"].startswith("Ceph bdev Controller")
        ]

    def disconnect(self, **kwargs):
        """Disconnect controller connected to the subsystem.

        Example::

            kwargs:
                nqn: Subsystem NQN id           # Subsystem NQN
        """
        return self.execute(
            cmd=f"nvme disconnect {config_dict_to_string(kwargs)}", sudo=True
        )

    def disconnect_all(self):
        """Disconnects all controllers connected to subsystems."""
        return self.execute(cmd="nvme disconnect-all", sudo=True)
