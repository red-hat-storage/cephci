import json

from ceph.ceph import CommandFailed
from ceph.nvmeof.initiators.linux import Initiator
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.exceptions import NoDevicesFound
from utility.log import Log
from utility.retry import retry
from utility.utils import config_dict_to_string, log_json_dump, run_fio

LOG = Log(__name__)
Initiators = {}
Clients = []


class NVMeInitiator(Initiator):
    def __init__(self, node, nqn=""):
        super().__init__(node)
        self.discovery_port = 8009
        self.subsys_key = None
        self.host_key = None
        # TODO: Need to cosume initiator nqn rather than getting from outside
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

    def connect_targets(self, gateway, config):
        if not config:
            config = self.config

        # Discover the subsystem endpoints
        cmd_args = {
            "transport": "tcp",
            "traddr": gateway.node.ip_address,
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

    @retry((NoDevicesFound))
    def list_devices(self):
        """List NVMe targets."""
        targets = self.list_spdk_drives()
        if not targets:
            raise NoDevicesFound(f"NVMe Targets not found on {self.node.hostname}")
        LOG.debug(targets)
        return targets

    def start_fio(self, io_size="100%", runtime=None, paths=None, **kwargs):
        """Start FIO on the all targets on client node.

        Args:
            io_size: Size of the IO to be performed
            paths: List of paths to perform IO on
            **kwargs: Additional arguments for FIO
        """
        if not paths:
            LOG.info("No paths provided, fetching all devices")
            paths = self.list_devices()
            LOG.info(f"All devices found are {paths}")
        else:
            LOG.info(f"Paths provided are {paths}")

        if not paths:
            raise Exception("No paths found")

        results = []
        io_args = {}

        if runtime:
            io_args.update({"run_time": runtime})

        if io_size:
            io_args.update({"size": io_size})

        # Update io_args if test_name is provided
        if kwargs.get("test_name"):
            io_args.update({"test_name": kwargs.get("test_name")})

        # Update io_args if iodepth is provided
        if kwargs.get("iodepth"):
            io_args.update({"iodepth": kwargs.get("iodepth")})

        # Update io_args if time_based is provided
        if kwargs.get("time_based"):
            io_args.update({"time_based": kwargs.get("time_based")})

        # Update io_args if rwmixread is provided
        if kwargs.get("rwmixread"):
            io_args.update({"rwmixread": kwargs.get("rwmixread")})

        # Update io_args if io_type is provided
        if kwargs.get("io_type"):
            io_args.update({"io_type": kwargs.get("io_type")})

        # Check whether to execute blkdiscard
        # For read only namespaces, blkdiscard is not required
        blkdiscard_cmd = kwargs.get("execute_blkdiscard", True)

        # Use max_workers to ensure all FIO processes can start simultaneously
        with parallel(max_workers=len(paths) + 4) as p:
            for path in paths:
                _io_args = {}
                # TODO: blkdiscard is temporary workaround for same image usage
                #  in the IO progression tasks especially HA failover and failback.
                if blkdiscard_cmd:
                    self.node.exec_command(cmd=f"blkdiscard {path}", sudo=True)
                else:
                    LOG.info(f"Skipping blkdiscard for {path}")
                if io_args.get("test_name"):
                    test_name = f"{io_args['test_name']}-" f"{path.replace('/', '_')}"
                    _io_args.update({"test_name": test_name})
                _io_args.update(
                    {
                        "device_name": path,
                        "client_node": self.node,
                        "long_running": True,
                        "cmd_timeout": "notimeout",
                        "verbose": True,
                    }
                )
                if kwargs.get("output_dir"):
                    test_name = f"{kwargs['test_name']}-" f"{path.replace('/', '_')}"
                    _io_args.update(
                        {
                            "test_name": test_name,
                            "output_format": "json",
                            "output_dir": kwargs["output_dir"],
                        }
                    )
                _io_args = {**io_args, **_io_args}
                p.spawn(run_fio, **_io_args)
            for op in p:
                results.append(op)
        return results

    def register(self, base, register_args, nrkey, client_node):
        """
        Helper to perform the register_reservation and report_reservation on a client.
        Validate the report for nrkey
        Parameters:
        base: Common/base arguments needed for reservation that includes device name and NSID
        register_args: Additional args for registering from config file
        nrkey: New Reservation Key generated from test module
        client_node: initiator node on which commands are run
        """
        register_out = self.register_reservation(
            **{**base, **register_args, "nrkey": nrkey}
        )
        namespace = base.get("device")
        nsid = base.get("namespace-id")
        LOG.debug(
            f"Register ({namespace} nsid {nsid} on {client_node.hostname}): {register_out}"
        )
        report_out, report_err = self.report_reservation(**base)
        LOG.debug(
            f"Register Report for ({namespace} nsid {nsid} on {client_node.hostname}): {report_out}"
        )
        return register_out, report_out

    def acquire(self, base, acquire_args, crkey, client_node):
        """
        Helper to perform the acquire_reservation and report_reservation on a client.
        Validate the report for rtype and rcsts
        Parameters:
        base: Common/base arguments needed for reservation that includes device name and NSID
        acquire_args: Additional args for acquiring reservation from config file
        crkey: Current Reservation Key generated from test module
        client_node: initiator node on which commands are run
        """
        acquire_out = self.acquire_reservation(
            **{**base, **acquire_args, "crkey": crkey}
        )
        namespace = base.get("device")
        nsid = base.get("namespace-id")
        LOG.debug(
            f"Acquire ({namespace} nsid {nsid} on {client_node.hostname}): {acquire_out}"
        )

        report_out, report_err = self.report_reservation(**base)
        LOG.debug(
            f"Acquire Report for ({namespace} nsid {nsid} on {client_node.hostname}): {report_out}"
        )
        data = json.loads(report_out)
        regctlext = data.get("regctlext", [])
        first_entry = regctlext[0] if regctlext else {}
        rtype_value = data.get("rtype")
        rcsts_value = first_entry.get("rcsts") if first_entry else None
        if rtype_value == acquire_args.get("rtype") and rcsts_value == 1:
            LOG.info(
                f"Acquire Report validation successfull for ({namespace} nsid {nsid} on {client_node.hostname}: "
                f"rcsts is {rcsts_value}, rtype is {rtype_value})"
            )
        else:
            raise Exception(
                f"Acquire Report validation **failed** for ({namespace} nsid {nsid} on {client_node.hostname}): "
                f"rcsts is {rcsts_value}, rtype is {rtype_value})"
            )
        return acquire_out, report_out

    def release(self, base, release_args, crkey, client_node):
        """
        Helper to perform the release_reservation and report_reservation on a client.
        Validate the report for rcsts
        Parameters:
        base: Common/base arguments needed for reservation that includes device name and NSID
        release_args: Additional args for releasing NS
        crkey: Current Reservation Key generated from test module
        client_node: initiator node on which commands are run
        """
        release_out = self.release_reservation(
            **{**base, **release_args, "crkey": crkey}
        )
        namespace = base.get("device")
        nsid = base.get("namespace-id")
        LOG.debug(
            f"Release ({namespace} nsid {nsid} on {client_node.hostname}): {release_out}"
        )

        report_out, report_err = self.report_reservation(**base)
        LOG.debug(
            f"Release Report for ({namespace} nsid {nsid} on {client_node.hostname}): {report_out}"
        )
        data = json.loads(report_out)
        regctlext = data.get("regctlext", [])
        first_entry = regctlext[0] if regctlext else {}
        rcsts_value = first_entry.get("rcsts") if first_entry else None
        if rcsts_value == 0:
            LOG.info(
                f"Release Report validation successfull for ({namespace} nsid {nsid} on {client_node.hostname}:"
                f"rcsts value is {rcsts_value})"
            )
        else:
            raise Exception(
                f"Release Report validation **failed** for ({namespace} nsid {nsid} on {client_node.hostname}): "
                f"rcsts_value is ({rcsts_value}) which says {client_node.hostname} is still reservation holder"
            )
        return release_out, report_out

    def unregister(self, base, unregister_args, crkey, client_node):
        """
        Helper to perform the unregister_reservation and report_reservation on a client.
        Validate the report for crkey
        Parameters:
        base: Common/base arguments needed for reservation that includes device name and NSID
        unregister_args: Additional args for unregistering NS from config file
        crkey: Current Reservation Key generated from test module
        client_node: initiator node on which commands are run
        """
        unregister_out = self.register_reservation(
            **{**base, **unregister_args, "crkey": crkey}
        )
        namespace = base.get("device")
        nsid = base.get("namespace-id")
        LOG.debug(
            f"Unregister ({namespace} nsid {nsid} on {client_node.hostname}): {unregister_out}"
        )

        report_out, report_err = self.report_reservation(**base)
        LOG.debug(
            f"Unregister Report for ({namespace} nsid {nsid} on {client_node.hostname}): {report_out}"
        )
        data = json.loads(report_out)
        regctl_count = data.get("regctl")
        if regctl_count == 0:
            LOG.info("No registered controllers left; regctl is 0")
        else:
            raise Exception(f"Other registrants remain; regctl={regctl_count}")

        return unregister_out, report_out


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


def get_or_create_initiator(node_id, nqn, cluster):
    """Get existing NVMeInitiator or create a new one for each (node_id, nqn)."""
    key = (node_id, nqn)  # Use both as dictionary key

    if key not in Initiators:
        node = get_node_by_id(cluster, node_id)
        Initiators[key] = NVMeInitiator(node, nqn)

    return Initiators[key]


def prepare_io_execution(io_clients, gateways=None, cluster=None, return_clients=False):
    """Prepare FIO Execution.

    initiators:                             # Configure Initiators with all pre-req
        - nqn: connect-all
        listener_port: 4420
        node: node10
    """
    for io_client in io_clients:
        nqn = io_client.get("nqn")
        if io_client.get("subnqn"):
            nqn = io_client.get("subnqn")
        client = get_or_create_initiator(io_client["node"], nqn, cluster)
        client.connect_targets(gateways[0], io_client)
        if client not in Clients:
            Clients.append(client)
    if return_clients:
        return Clients


@retry(IOError, tries=3, delay=3)
def compare_client_namespace(clients, uuids, FEWR_NAMESPACES=False):
    lsblk_devs = []
    for client in clients:
        lsblk_devs.extend(client.fetch_lsblk_nvme_devices())

    LOG.info(
        f"Expected NVMe Targets : {set(list(uuids))} Vs LSBLK devices: {set(list(lsblk_devs))}"
    )
    if FEWR_NAMESPACES:
        if not set(uuids).issubset(lsblk_devs):
            raise IOError("Few Namespaces are missing!!!")
    else:
        if sorted(uuids) != sorted(set(lsblk_devs)):
            raise IOError("Few Namespaces are missing!!!")
    LOG.info("All namespaces are listed at Client(s)")
    return True
