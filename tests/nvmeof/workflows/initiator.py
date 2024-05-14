import json

from ceph.nvmeof.initiator import Initiator
from ceph.parallel import parallel
from utility.log import Log
from utility.utils import log_json_dump, run_fio

LOG = Log(__name__)


class NVMeInitiator(Initiator):
    def __init__(self, node, gateway):
        super().__init__(node)
        self.gateway = gateway
        self.discovery_port = 8009

    def fetch_lsblk_nvme_devices(self):
        """Validate all devices at client side.

        Args:
            uuids: List of Namespace UUIDs
        Returns:
            boolean
        """
        out, _ = self.node.exec_command(
            cmd=" lsblk -I 8,259 -o name,wwn --json", sudo=True
        )
        out = json.loads(out)["blockdevices"]
        uuids = sorted(
            [i["wwn"].removeprefix("uuid.") for i in out if i["wwn"] is not None]
        )
        LOG.debug(f"[ {self.node.hostname} ] LSBLK UUIds : {log_json_dump(out)}")
        return uuids

    def connect_targets(self, config):
        if not config:
            config = self.config

        # Discover the subsystem endpoints
        cmd_args = {
            "transport": "tcp",
            "traddr": self.gateway.node.ip_address,
        }
        json_format = {"output-format": "json"}

        discovery_port = {"trsvcid": self.discovery_port}
        _disc_cmd = {**cmd_args, **discovery_port, **json_format}

        nqns_discovered, _ = self.discover(**_disc_cmd)
        LOG.debug(nqns_discovered)

        # connect-all
        connect_all = {}
        if config["nqn"] == "connect-all":
            connect_all = {"ctrl-loss-tmo": 3600}
            cmd = {**discovery_port, **cmd_args, **connect_all}
            self.connect_all(**cmd)
            self.list()
            return

        # Connect to individual targets of a subsystem
        subsystem = config["nqn"]
        sub_endpoints = []

        for nqn in json.loads(nqns_discovered)["records"]:
            if nqn["subnqn"] == subsystem and nqn["trsvcid"] == str(
                config["listener_port"]
            ):
                sub_endpoints.append(nqn)

        if not sub_endpoints:
            raise Exception(f"Subsystem not found -- {cmd_args}")

        for sub_endpoint in sub_endpoints:
            conn_port = {"trsvcid": config["listener_port"]}
            sub_args = {"nqn": sub_endpoint["subnqn"]}
            cmd_args.update({"traddr": sub_endpoint["traddr"]})
            _conn_cmd = {**cmd_args, **conn_port, **sub_args}
            LOG.debug(self.connect(**_conn_cmd))

    def list_devices(self):
        """List NVMe targets."""
        targets = self.list_spdk_drives()
        if not targets:
            raise Exception(f"NVMe Targets not found on {self.node.hostname}")
        LOG.debug(targets)
        return targets

    def start_fio(self):
        """Start FIO on the all targets on client node."""
        targets = self.list_devices()
        results = []
        io_args = {"size": "100%"}
        with parallel() as p:
            for target in targets:
                _io_args = {}
                if io_args.get("test_name"):
                    test_name = (
                        f"{io_args['test_name']}-"
                        f"{target['DevicePath'].replace('/', '_')}"
                    )
                    _io_args.update({"test_name": test_name})
                _io_args.update(
                    {
                        "device_name": target["DevicePath"],
                        "client_node": self.node,
                        "long_running": True,
                        "cmd_timeout": "notimeout",
                    }
                )
                _io_args = {**io_args, **_io_args}
                p.spawn(run_fio, **_io_args)
            for op in p:
                results.append(op)
        return results
