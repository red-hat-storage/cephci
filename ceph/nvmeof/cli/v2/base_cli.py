import re
from distutils.version import LooseVersion

from ceph.ceph_admin.common import config_dict_to_string
from ceph.nvmeof.cli.v2.common import run_with_connect_retry, substitute_keys
from utility.log import Log

LOG = Log(__name__)


KEY_MAP = {
    "subsystem": "nqn",
    "host": "host_nqn",
    "host-nqn": "host_nqn",
    "dhchap-ctrl-secret": "dhchap-controller-key",
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

    def get_ceph_version(self):
        if self.ceph_version is None:
            out, _ = self.shell(args=["ceph", "--format", "json", "version"])
            match = re.search(r"[0-9]+(\.[-0-9]+)*", out)
            if not match:
                raise ValueError("Ceph version not found.")

            self.ceph_version = match.group()

        return self.ceph_version

    def build_command(self, entity, action, **kwargs):
        """Build a ``ceph nvmeof`` command.

        When mTLS is on, the mgr loads the client certificate, client key,
        and server certificate from the cephadm cert store and opens the
        gRPC channel itself. ``ceph nvmeof`` does not accept
        ``--client-key``, ``--client-cert``, or ``--server-cert`` (those
        flags belong to the container ``nvmeof-cli``).

        An injected ``--server_address`` can fail that handshake when it is
        not the address the dashboard registered for the gateway
        (IBMCEPH-16272). The helper therefore leaves the address unset while
        mTLS is on, unless the caller passed one. A plaintext command still
        targets this gateway node.
        """
        base_cmd_args = kwargs.get("base_cmd_args", {})
        cmd_args = kwargs.get("args", {})
        ceph_version = self.get_ceph_version()

        if not cmd_args.get("gw_group"):
            cmd_args["gw_group"] = self.gateway_group

        # Gateway address. While mTLS is on, ceph nvmeof selects the
        # dashboard's registered gateway and presents the stored client
        # certificate. Injecting this node's address can fail that
        # handshake (IBMCEPH-16272).
        # TODO: Remove the plaintext injection once we have a proper way to
        # determine the right argument for the command.
        if not self.mtls and (
            not cmd_args.get("traddr") or not cmd_args.get("server_address")
        ):
            if ceph_version > "20.1.0-145":
                cmd_args["server_address"] = self.node.ip_address
            else:
                cmd_args["traddr"] = self.node.ip_address

        # Image size flag: --rbd-image-size (9.2+ / >= 20.2.1); --size (older)
        if "rbd-image-size" in cmd_args:
            if LooseVersion(ceph_version) < LooseVersion("20.2.1"):
                cmd_args["size"] = cmd_args.pop("rbd-image-size")

        return [
            self.BASE_CMD,
            entity,
            action,
            config_dict_to_string(cmd_args),
            config_dict_to_string(base_cmd_args),
        ]

    @substitute_keys(KEY_MAP)
    def run_nvme_cli(self, entity, action, **kwargs):
        LOG.info(f"NVMeoF command - {entity} {action}")
        command = self.build_command(entity, action, **kwargs)
        # As a alternative to bug reported https://ibm-ceph.atlassian.net/browse/IBMCEPH-16456,
        # Which is intermittent and happens when the NVMe CLI is executed on a gateway node.
        # we are using a retry mechanism to handle the connection error.
        return run_with_connect_retry(
            lambda: self.shell(args=command, pretty_print=True),
            description=f"NVMeoF command - {entity} {action}",
        )
