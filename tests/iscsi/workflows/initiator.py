from ceph.iscsi.initiator import ISCSIInitiator
from tests.iscsi.workflows.iscsi_utils import fetch_tcmu_devices, format_iscsiadm_output
from utility.log import Log
from utility.utils import run_fio

LOG = Log(__name__)


class Initiator(ISCSIInitiator):

    def __init__(self, node, iqn, gateway):
        """Initialize with node name and ClientIQN.

        Args:
            node: Initiator node, Ceph node Object
            iqn: client iqn
            gateway: Gateway node object
        """
        super().__init__(node, iqn)
        self.gateway = gateway

    def connect_targets(self):
        """Connect iSCSI targets.

        Args:
            config: initiator config

            :: config:
                    - iqn: iqn.2025-01.com.redhat.iscsi-gw:rh-client
                      node: node7
                      type: linux
                      target: all
        """
        _args = {"type": "sendtargets", "portal": self.gateway.node.ip_address}
        targets, _ = self.iscsiadm.discover(**_args)
        targets = format_iscsiadm_output(targets)

        for target in targets:
            _args = {"targetname": target, "login": True}
            self.iscsiadm.node(**_args)

    def start_fio(self):
        """Start FIO on the all targets on client node."""
        self.node.exec_command(cmd="dnf install -y fio", sudo=True)
        targets = fetch_tcmu_devices(self.node)
        results = []
        io_args = {
            "cmd_timeout": 300,
            "io_type": "randrw",
            "iodepth": "2",
            "direct": "1",
            "run_time": 60,
        }

        for serial, target in targets.items():
            LOG.info(f"Starting FIO for Disk UUID {serial} and {target}....")
            _io_args = {}
            _io_args.update(
                {
                    "test_name": f"test-{target['mapper_path'].replace('/', '_')}",
                    "device_name": target["mapper_path"],
                    "client_node": self.node,
                    "long_running": True,
                }
            )
            _io_args = {**io_args, **_io_args}
            result = run_fio(**_io_args)
            results.append(result)
            LOG.info(f"Completed FIO for {target['mapper_path']}")

        LOG.info("Completed all IO executions.....")
        return results

    def logout(self, target=None):
        """Log out from all targets."""
        _args = {"logout": True}
        if target:
            _args["targetname"] = target
        LOG.info(f"Logging out from {self.node.ip_address} {_args}")
        return self.iscsiadm.node(**_args)

    def cleanup(self):
        """Cleanup method to purge all ISCSI initiator configuration."""
        self.logout()
        self.node.exec_command(
            cmd=f"rm -rf {self.MPATH_CONFIG_FILE_PATH} {self.ISCSID_CONFIG_FILE_PATH} {self.ISCSI_IQN_PATH}",
            sudo=True,
        )
        self.node.exec_command(
            cmd=f"dnf remove -y fio {self.ISCSI_PACKAGES}", sudo=True
        )
        LOG.info(f"[ {self.node.ip_address} ] Node cleanup done....")
