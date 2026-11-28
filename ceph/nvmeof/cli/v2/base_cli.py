import re
from distutils.version import LooseVersion

from ceph.ceph import CommandFailed
from ceph.ceph_admin.common import config_dict_to_string
from ceph.nvmeof.cli.v2.common import (
    CONNECT_ALL_ADDRESSES_ERR,
    run_with_connect_retry,
    substitute_keys,
)
from utility.log import Log

LOG = Log(__name__)


KEY_MAP = {
    "subsystem": "nqn",
    "host": "host_nqn",
    "host-nqn": "host_nqn",
    "dhchap-ctrl-secret": "dhchap-controller-key",
    # NVMe/TCP TLS pre-shared key on host add
    "psk": "psk",
    "tls-psk": "psk",
    "network-mask": "network_mask",
    "serial-number": "serial_number",
    "max-namespaces": "max_namespaces",
    "no-group-append": "no-group-append",
    "rbd-pool": "rbd_pool",
    "rbd-image": "rbd_image_name",
    "rbd-data-pool": "rbd_data_pool",
    "rbd-create-image": "create-image",
    "load-balancing-group": "load_balancing_group",
    "rbd-trash-image-on-delete": "trash-image",
    "block-size": "block_size",
    "rados-namespace": "rados_namespace",
    "disable-auto-resize": "disable-auto-resize",
    "no-auto-visible": "no-auto-visible",
    "read-only": "read-only",
    "clear-alerts": "clear-alerts",
    "verify-host-name": "verify-host-name",
    "host-name": "host_name",
    "rw-ios-per-second": "rw_ios_per_second",
    "rw-megabytes-per-second": "rw_mbytes_per_second",
    "r-megabytes-per-second": "r_mbytes_per_second",
    "w-megabytes-per-second": "w_mbytes_per_second",
    "level": "log_level",
    "auto-visible": "auto_visible",
    # Hyphenated aliases used by callers / docs → underscore CLI flags
    "gw-group": "gw_group",
    "server-address": "server_address",
    "server-port": "server_port",
    "sort_by": "sort-by",
    "no_header": "no-header",
    "with_timestamp": "with-timestamp",
    # BYOK LUKS encryption (9.2+)
    # The CLI flags use hyphens: --encryption-format, --encryption-algorithm, --key-id
    # No translation needed — pass through as-is.
}


class BaseCLI:
    """Execute Command class runs NVMe CLI on Gateway Node."""

    BASE_CMD = "ceph nvmeof"

    def __init__(self, node, shell) -> None:
        """Initialize the Shell.

        Args:
            node: Gateway Node instance (CephNode)
            shell: Cephadm shell instance (orch.shell or cephadm.shell)
        """
        self.node = node
        self.shell = shell
        self.ceph_version = None

    def __local_mtls_cert_path(self) -> str:
        """Currently mtls is not supported in Ceph NVMe CLI."""
        return ""

    def get_ceph_version(self):
        if self.ceph_version is None:
            out, _ = self.shell(args=["ceph", "--format", "json", "version"])
            match = re.search(r"[0-9]+(\.[-0-9]+)*", out)
            if not match:
                raise ValueError("Ceph version not found.")

            self.ceph_version = match.group()

        return self.ceph_version

    @substitute_keys(KEY_MAP)
    def run_nvme_cli(self, entity, action, **kwargs):
        LOG.info(f"NVMeoF command - {entity} {action}")
        base_cmd_args = kwargs.get("base_cmd_args", {})

        # TODO: Currently mtls is not supported in Ceph NVMe CLI(Tentacle).
        #       Fix this once mtls is supported
        if self.mtls:
            pass

        cmd_args = kwargs.get("args", {})
        ceph_version = self.get_ceph_version()

        # Gateway group
        if not cmd_args.get("gw_group"):
            cmd_args["gw_group"] = self.gateway_group

        # Gateway address
        # TODO: Remove this once we have a proper way to determine right argument for the command.
        # ``top`` uses --server_address as an *output filter* (restrict to one GW),
        # not as the RPC target. Auto-injecting would break multi-GW top samples
        # and overwrite an explicit filter when traddr is absent (OR condition).
        # Inject only when neither address flag is set, and never for top entity.
        if entity != "top":
            if not cmd_args.get("traddr") and not cmd_args.get("server_address"):
                if self.get_ceph_version() > "20.1.0-145":
                    cmd_args["server_address"] = self.node.ip_address
                else:
                    cmd_args["traddr"] = self.node.ip_address

        # Image size flag: --rbd-image-size (9.2+ / >= 20.2.1); --size (older)
        if "rbd-image-size" in cmd_args:
            if LooseVersion(ceph_version) < LooseVersion("20.2.1"):
                cmd_args["size"] = cmd_args.pop("rbd-image-size")

        # Quote multi-word flag values (e.g. --sort-by 'Total IOPS')
        for key, val in list(cmd_args.items()):
            if (
                isinstance(val, str)
                and any(ch in val for ch in (" ", "µ"))
                and not (val.startswith("'") or val.startswith('"'))
            ):
                cmd_args[key] = f"'{val}'"

        command = [
            self.BASE_CMD,
            self.__local_mtls_cert_path(),
            entity,
            action,
            config_dict_to_string(cmd_args),
            config_dict_to_string(base_cmd_args),
        ]

        # Live-polling commands (e.g. ``top connection``) never exit on their
        # own. Wrap with GNU timeout so stdout is returned; Paramiko channel
        # timeout would raise SocketTimeoutException and discard output.
        # check_status=False is required (timeout exits 124), so surface the
        # IBMCEPH-16456 connect flake as CommandFailed for run_with_connect_retry.
        live_timeout = kwargs.get("live_timeout")
        if live_timeout:
            cmd_str = " ".join(part for part in command if part).strip()
            wrapped = (
                f"timeout --foreground --signal=TERM --kill-after=3 "
                f"{int(live_timeout)}s {cmd_str}"
            )

            def _live_shell():
                out, err = self.shell(
                    args=[wrapped],
                    pretty_print=True,
                    check_status=False,
                    timeout=int(live_timeout) + 60,
                )
                combined = f"{out or ''}{err or ''}"
                if CONNECT_ALL_ADDRESSES_ERR in combined:
                    raise CommandFailed(combined)
                return out, err

            return run_with_connect_retry(
                _live_shell,
                description=f"NVMeoF command - {entity} {action}",
            )

        # As a alternative to bug reported https://ibm-ceph.atlassian.net/browse/IBMCEPH-16456,
        # Which is intermittent and happens when the NVMe CLI is executed on a gateway node.
        # we are using a retry mechanism to handle the connection error.
        return run_with_connect_retry(
            lambda: self.shell(args=command, pretty_print=True),
            description=f"NVMeoF command - {entity} {action}",
        )
