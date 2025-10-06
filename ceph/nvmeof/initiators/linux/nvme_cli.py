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

    def gen_dhchap_key(self, **kwargs):
        """Generates the TLS key.
        Example::
            kwargs:
                subsystem: NQN of subsystem
        """
        return self.execute(
            cmd=f"nvme gen-dhchap-key {config_dict_to_string(kwargs)}",
            sudo=True,
        )

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

    def list_subsys(self, **kwargs):
        """List the subsystems and its information.

        Example::

            kwargs:
                output-format: json             # output format
        """
        device = kwargs.pop("device", "")
        return self.execute(
            cmd=f"nvme list-subsys {device} {config_dict_to_string(kwargs)}", sudo=True
        )

    def list_spdk_drives(self, nsid_device_pair=None):
        """List the NVMe Targets only SPDK drives.
        Args: nsid_device_pair (optional)
        Return:
            Dict: Dict of SPDK drives if `nsid_device_pair` is None else empty list.
            list: If `nsid_device_pair` is passed, list of dicts each containing "Namespace" and "NSID".
        """
        # check if rhel version is 8 or 9
        out, _ = self.execute(sudo=True, cmd="cat /etc/os-release | grep VERSION_ID")
        rhel_version = out.split("=")[1].strip().strip('"')
        json_kwargs = {"output-format": "json"}
        out, _ = self.list(**json_kwargs)
        devs = json.loads(out)["Devices"]

        if not devs:
            LOG.debug("No NVMe devices found.")
            return []

        if rhel_version == "9.5":
            return [
                dev["DevicePath"]
                for dev in devs
                if dev["ModelNumber"].startswith("Ceph bdev Controller")
            ]

        elif rhel_version == "9.6":
            if nsid_device_pair:
                namespace_list = []
                for dev in devs:
                    for subsys in dev.get("Subsystems", []):
                        # Check if at least one controller in this subsystem is a Ceph bdev Controller
                        has_ceph_bdev = any(
                            ctrl.get("ModelNumber") == "Ceph bdev Controller"
                            for ctrl in subsys.get("Controllers", [])
                        )
                        if has_ceph_bdev:
                            for ns in subsys.get("Namespaces", []):
                                pair = {
                                    "Namespace": f"/dev/{ns['NameSpace']}",
                                    "NSID": ns.get("NSID"),
                                }
                                namespace_list.append(pair)
                return namespace_list

            devices = []
            for dev in devs:
                for subsys in dev.get("Subsystems", []):
                    # Check if at least one controller in this subsystem is a Ceph bdev Controller
                    has_ceph_bdev = any(
                        ctrl.get("ModelNumber") == "Ceph bdev Controller"
                        for ctrl in subsys.get("Controllers", [])
                    )
                    if has_ceph_bdev:
                        for ns in subsys.get("Namespaces", []):
                            devices.append(f"/dev/{ns['NameSpace']}")

            return devices

        return []

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

    def connect_all(self, **kwargs):
        """Connects all controllers connected to subsystems."""
        return self.execute(
            cmd=f"nvme connect-all {config_dict_to_string(kwargs)}", sudo=True
        )

    def register_reservation(self, **kwargs):
        """
        Register reservation for a namespace on an initiator.
        Mandatory arguments: device, namespace-id (nsid), nrkey.
        Optional arguments: as needed.
        """
        device = kwargs.pop("device")
        nsid = kwargs.pop("namespace-id")

        if not (device and nsid):
            raise ValueError("device, namespace-id must be provided")

        if kwargs.get("crkey"):
            cmd = (
                f"nvme resv-register {device} --namespace-id {nsid} "
                f"{config_dict_to_string(kwargs)} -v"
            )
        else:
            cmd = (
                f"nvme resv-register {device} --namespace-id {nsid} "
                f"{config_dict_to_string(kwargs)} -v"
            )
        return self.execute(cmd=cmd, sudo=True)

    def acquire_reservation(self, **kwargs):
        """
        Acquire reservation for a namespace on an initiator.
        Mandatory arguments: device, namespace-id (nsid), crkey for acquire or prkey for preempt.
        Optional arguments: as needed.
        """
        device = kwargs.pop("device")
        nsid = kwargs.pop("namespace-id")
        crkey = kwargs.pop("crkey", None)
        prkey = kwargs.pop("prkey", None)

        if not (device and nsid and (crkey is not None or prkey is not None)):
            raise ValueError(
                "device, namespace-id, and either crkey or prkey must be provided"
            )

        if prkey is not None:
            cmd = (
                f"nvme resv-acquire {device} --namespace-id {nsid} --prkey {prkey} "
                f"{config_dict_to_string(kwargs)} -v"
            )
        else:
            cmd = (
                f"nvme resv-acquire {device} --namespace-id {nsid} --crkey {crkey} "
                f"{config_dict_to_string(kwargs)} -v"
            )
        return self.execute(cmd=cmd, sudo=True)

    def report_reservation(self, **kwargs):
        """
        Report reservation for a namespace on an initiator.
        Mandatory arguments: device and namespace-id (nsid).
        """
        device = kwargs.pop("device")
        nsid = kwargs.pop("namespace-id")

        if not (device and nsid):
            raise ValueError("device and namespace-id must both be provided")

        cmd = f"nvme resv-report {device} --namespace-id {nsid} -o json"
        return self.execute(cmd=cmd, sudo=True)

    def release_reservation(self, **kwargs):
        """
        Release reservation for a namespace on an initiator.
        Mandatory arguments: device, namespace-id, crkey.
        Optional arguments: as needed.
        """
        device = kwargs.pop("device")
        nsid = kwargs.pop("namespace-id")
        crkey = kwargs.pop("crkey")

        if not (device and nsid and crkey):
            raise ValueError("device, namespace-id, and crkey must all be provided")

        cmd = (
            f"nvme resv-release {device} --namespace-id {nsid} --crkey {crkey} "
            f"{config_dict_to_string(kwargs)} -v"
        )
        return self.execute(cmd=cmd, sudo=True)
