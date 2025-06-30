import json

from ceph.ceph import CommandFailed
from ceph.nvmeof.initiator import Initiator
from ceph.parallel import parallel
from utility.log import Log
from utility.retry import retry
from utility.utils import log_json_dump, run_fio

LOG = Log(__name__)


class NVMeInitiator(Initiator):
    def __init__(self, node, gateway, nqn=""):
        super().__init__(node)
        self.gateway = gateway
        self.discovery_port = 8009
        self.subsys_key = None
        self.host_key = None
        self.nqn = nqn
        self.auth_mode = ""

    def fetch_lsblk_nvme_devices_dict(self):
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
        return out

    def fetch_anastate(self, device):
        """
        Fetch the ANA state of the given device for each gateway
        Args:
            device: NVMe device path
        Returns:
            ANA state of the device
        """
        out, _ = self.list_subsys(**{"device": device, "output-format": "json"})
        subsystems = json.loads(out)[0].get("Subsystems")
        paths = {"optimized": [], "inaccessible": []}
        for subsys in subsystems:
            for path in subsys.get("Paths"):
                if path.get("State") == "live" and path.get("ANAState") == "optimized":
                    paths["optimized"].extend(
                        [path.get("Address").split("traddr=")[1].split(",")[0]]
                    )
                else:
                    paths["inaccessible"].extend(
                        [path.get("Address").split("traddr=")[1].split(",")[0]]
                    )
        return paths

    def fetch_lsblk_nvme_devices(self):
        """Validate all devices at client side.

        Args:
            uuids: List of Namespace UUIDs
        Returns:
            boolean
        """
        out = self.fetch_lsblk_nvme_devices_dict()
        uuids = sorted(
            [
                i["wwn"].removeprefix("uuid.")
                for i in out
                if i.get("wwn", "").startswith("uuid.")
            ]
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
        if config["nqn"] == "connect-all" or self.auth_mode == "unidirectional":
            connect_all = {"ctrl-loss-tmo": 3600}
            if self.auth_mode == "unidirectional":
                cmd_args.update({"dhchap-secret": self.host_key})
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

            if self.auth_mode == "bidirectional":
                sub_args.update(
                    {
                        "dhchap-secret": self.host_key,
                        "dhchap-ctrl-secret": self.subsys_key,
                    }
                )
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

        rhel_version = self.distro_version()
        if rhel_version.endswith("9.5"):
            paths = [target["DevicePath"] for target in targets]
        elif rhel_version == "9.6":
            paths = [
                f"/dev/{ns['NameSpace']}"
                for device in targets
                for subsys in device.get("Subsystems", [])
                for ns in subsys.get("Namespaces", [])
            ]

        results = []
        io_args = {"size": "100%"}
        # Use max_workers to ensure all FIO processes can start simultaneously
        with parallel(max_workers=len(paths) + 4) as p:
            for path in paths:
                _io_args = {}
                if io_args.get("test_name"):
                    test_name = f"{io_args['test_name']}-" f"{path.replace('/', '_')}"
                    _io_args.update({"test_name": test_name})
                _io_args.update(
                    {
                        "device_name": path,
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


@retry((IOError, TimeoutError, CommandFailed), tries=7, delay=2)
def fetch_gw_paths_for_namespaces(client, ns_device):
    """
    Fetch the optimized and inaccessible paths for the namespaces
    serviced by a particular gateway.

    Args:
        gateway: gateway object
        ana_id: ana group id of the namespaces
    """
    gw_paths = client.fetch_anastate(ns_device)
    LOG.info(f"Gateway paths : {log_json_dump(gw_paths)}")

    if not gw_paths.get("optimized"):
        raise IOError(f"Namespace is not optimized at {client} initiator")
    return {"namespace": ns_device, "paths": gw_paths}


def fetch_paths_for_namespaces(client, namespaces, devices):
    """
    Fetch the device path for the given namespace UUID
    Args:
        uuid: Namespace UUID
    Returns:
        NVMe device path
    """
    gw_paths = []
    wwn_to_name = {
        device.get("wwn", "").removeprefix("uuid."): device["name"]
        for device in devices
    }
    device_names = [
        wwn_to_name.get(ns.get("uuid"))
        for ns in namespaces
        if ns.get("uuid") in wwn_to_name
    ]
    with parallel() as p:
        for ns_device in device_names:
            p.spawn(fetch_gw_paths_for_namespaces, client, ns_device)
        for result in p:
            gw_paths.append(result)
    return gw_paths


def validate_initiator(clients, gateway, namespaces_gw, failed_gw=None):
    """Check whether all namespaces serviced by a particular gateway are optimized
    for that gateway at the initiator and also during failover, check if the failed
    gateway is inaccessible at the initiator.

    Args:
        gateway: gateway object
        namespaces_gw: namespaces related to the gateway
        failed_gw: failed gateway object
    """
    for client in clients:
        devices = client.fetch_lsblk_nvme_devices_dict()
        if not devices:
            raise Exception(f"NVMe devices are not available at {client} initiator")
        gw_paths = fetch_paths_for_namespaces(client, namespaces_gw, devices)
        for paths in gw_paths:
            if len(paths.get("paths").get("optimized")) > 1:
                raise Exception(
                    f"Namespace {paths.get('namespace')} has more than one at optimized paths \
                        {client} initiator"
                )
            gw_ip = gateway.node.ip_address
            if paths.get("paths").get("optimized")[0] != gw_ip:
                raise Exception(
                    f"Namespace {paths.get('namespace')} is not optimized for {gw_ip} at \
                        {client} initiator"
                )
            if failed_gw and failed_gw.node.ip_address not in paths.get("paths").get(
                "inaccessible"
            ):
                raise Exception(
                    f"Namespace {paths.get('namespace')} is not inaccessible for {failed_gw.node.ip_address} \
                    at {client} initiator"
                )
    LOG.info(
        f"All namespaces are optimized for all initiators for gateway {gateway.node.ip_address}"
    )
