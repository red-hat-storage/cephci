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
        """Install NVMe CLI and enable boot-time fabrics + autoconnect.

        Customer-like initiator reboot reconnect requires:
          1. ``nvme-fabrics`` loaded at boot (modules-load.d)
          2. ``/etc/nvme/discovery.conf`` populated (done on connect)
          3. ``nvmf-autoconnect.service`` enabled
          4. ``nvme connect-all --persistent`` (see ``connect_all``)
        """
        configure_cmds = [
            ("yum install -y nvme-cli fio", True),
            ("modprobe nvme-fabrics", True),
            (
                "bash -c 'echo nvme-fabrics > /etc/modules-load.d/nvme-fabrics.conf'",
                True,
            ),
            ("mkdir -p /etc/nvme", True),
        ]
        for cmd in configure_cmds:
            self.execute(*cmd)
        self.enable_nvmf_autoconnect()

    def enable_nvmf_autoconnect(self):
        """Enable systemd unit that runs connect-all at boot from discovery.conf."""
        host = getattr(self, "node", None) or getattr(self, "ctx", None)
        hostname = getattr(host, "hostname", host)
        try:
            self.execute(
                "systemctl enable nvmf-autoconnect.service",
                True,
            )
            LOG.info("Enabled nvmf-autoconnect.service on %s", hostname)
        except Exception as err:
            LOG.warning(
                "Could not enable nvmf-autoconnect.service on %s: %s",
                hostname,
                err,
            )

    def configure_discovery_conf(self, traddr, trsvcid=8009, transport="tcp"):
        """
        Persist discovery controller endpoint in /etc/nvme/discovery.conf.

        Used by ``nvmf-autoconnect.service`` (``nvme connect-all --context=autoconnect``)
        so namespaces reappear after initiator reboot without a manual discover/connect.

        Args:
            traddr: Discovery / gateway IP
            trsvcid: Discovery port (default 8009)
            transport: Fabric transport (default tcp)
        """
        if not traddr:
            raise ValueError("traddr is required for discovery.conf")
        line = f"--transport={transport} --traddr={traddr} --trsvcid={trsvcid}"
        self.execute("mkdir -p /etc/nvme", True)
        # Idempotent append of discovery controller endpoint
        self.execute(
            (
                'bash -c "'
                f"grep -qxF '{line}' /etc/nvme/discovery.conf 2>/dev/null || "
                f"echo '{line}' >> /etc/nvme/discovery.conf\""
            ),
            True,
        )
        LOG.info("Ensured /etc/nvme/discovery.conf contains: %s", line)
        return line

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
        kwargs.setdefault("persistent", True)
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
        json_kwargs = {"output-format": "json"}
        out, _ = self.list(**json_kwargs)
        LOG.debug(json.dumps(out, indent=4))
        devs = json.loads(out)["Devices"]

        if not devs:
            LOG.debug("No NVMe devices found.")
            return []

        ceph_model = "Ceph bdev Controller"
        devices, namespace_list = [], []
        for dev in devs:
            subsystems = dev.get("Subsystems", [])
            if subsystems:
                # --- New-style layout ---
                for subsys in subsystems:
                    if any(
                        ctrl.get("ModelNumber") == ceph_model
                        for ctrl in subsys.get("Controllers", [])
                    ):
                        for ns in subsys.get("Namespaces", []):
                            ns_path = f"/dev/{ns['NameSpace']}"
                            devices.append(ns_path)
                            namespace_list.append(
                                {"Namespace": ns_path, "NSID": ns.get("NSID")}
                            )
            elif dev.get("ModelNumber", "").startswith(ceph_model):
                # --- Old-style layout ---
                ns_path = dev.get("DevicePath")
                devices.append(ns_path)
                namespace_list.append(
                    {"Namespace": ns_path, "NSID": dev.get("NameSpace")}
                )

        return namespace_list if nsid_device_pair else devices

    def id_ctrl(self, device, **kwargs):
        """Identify controller.

        Example::

            kwargs:
                output-format: json             # output format
                human-readable: True            # human readable (-H)
        """
        return self.execute(
            cmd=f"nvme id-ctrl {device} {config_dict_to_string(kwargs)}",
            sudo=True,
        )

    def id_ns(self, device, **kwargs):
        """Identify namespace.

        Example::

            kwargs:
                namespace-id: 1                   # NSID
                output-format: json             # output format
                human-readable: True            # human readable (-H)
        """
        return self.execute(
            cmd=f"nvme id-ns {device} {config_dict_to_string(kwargs)}",
            sudo=True,
        )

    def copy(self, device, **kwargs):
        """Execute NVMe Copy (CNC) command.

        Destination is ``device``. Source ranges use ``slbs``, ``blocks``,
        ``snsids`` (comma-separated for multi-range). Use ``format=2`` for
        cross-namespace copy descriptors.

        Example::

            kwargs:
                sdlba: 1000
                slbs: 5000                      # or "5000,9000"
                blocks: 1255                    # or "99,199"
                snsids: 1                       # or "1,1"
                format: 2
        """
        check_ec = kwargs.pop("check_ec", True)
        return self.execute(
            cmd=f"nvme copy {device} {config_dict_to_string(kwargs)}",
            sudo=True,
            check_ec=check_ec,
        )

    def read(self, device, **kwargs):
        """Read logical blocks from an NVMe namespace.

        Example::

            kwargs:
                start-block: 1000
                block-count: 99                 # 0-based count (NLB - 1)
                data-size: 51200
                data: /tmp/region.bin
        """
        return self.execute(
            cmd=f"nvme read {device} {config_dict_to_string(kwargs)}",
            sudo=True,
        )

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
        """Connects all controllers to discovered subsystems.

        Always includes ``--persistent`` so connections are recorded for
        reconnect after initiator reboot / nvme connect-all -P semantics.
        Pass ``persistent=False`` to opt out for a specific call.

        When ``traddr`` is provided, also updates ``/etc/nvme/discovery.conf``
        and ensures ``nvmf-autoconnect.service`` is enabled for boot reconnect.
        """
        kwargs.setdefault("persistent", True)
        traddr = kwargs.get("traddr")
        if traddr:
            self.configure_discovery_conf(
                traddr=traddr,
                trsvcid=kwargs.get("trsvcid", 8009),
                transport=kwargs.get("transport", "tcp"),
            )
            self.enable_nvmf_autoconnect()
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

        cmd = f"nvme resv-report {device} --namespace-id {nsid} -e 1 -o json"
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
