import json
import threading
import time
from collections import defaultdict

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.initiator import NVMeInitiator, prepare_io_execution
from utility.log import Log
from utility.retry import retry

LOG = Log(__name__)


class NamespaceMasking:
    """
    Helper class containing the original methods.
    """

    def __init__(
        self,
        cluster,
        gateways,
        clients,
        orch,
    ):
        self.cluster = cluster
        self.gateways = gateways
        self.clients = clients
        self.orch = orch
        self.initiators = {}

    @retry((IOError, TimeoutError, CommandFailed), tries=7, delay=2)
    def validate_io(self, namespaces, negative=False):
        """Validate Continuous IO on namespaces."""

        def io_value(ns):
            sub_ns, pool, image = ns.rsplit("|", 2)
            count = 3
            samples = []
            for _ in range(count):
                out, _ = self.orch.shell(
                    args=[f"rbd --format json du {pool}/{image}"], timeout=600
                )
                out = json.loads(out)["images"][0]
                samples.append(out)
                time.sleep(3)
            return sub_ns, f"{pool}/{image}", samples

        def validate_incremetal_io(write_samples):
            for i in range(len(write_samples) - 1):
                if write_samples[i] >= write_samples[i + 1]:
                    return False
            return True

        with parallel() as p:
            for namespace in namespaces:
                p.spawn(io_value, namespace)

            for result in p:
                subsys, pool_img, samples = result
                res = [i["used_size"] for i in samples]

                LOG.info(
                    f"[ {subsys}|{pool_img} ] RBD DU Detailed - {json.dumps(samples, indent=2)}"
                )
                LOG.info(f"[ {subsys}|{pool_img} ] RBD DU samples - {res}")
                if not validate_incremetal_io(res):
                    if negative:
                        LOG.info(
                            f"[ {subsys}|{pool_img} ] IO is not progressing as expected - {res}"
                        )
                        continue
                    raise IOError(
                        f"[ {subsys}|{pool_img} ] IO is not progressing - {res}"
                    )
                if negative:
                    LOG.error(
                        f"[ {subsys}|{pool_img} ] IO is progressing as expected - {res}"
                    )
                    raise IOError(
                        f"[ {subsys}|{pool_img} ] IO is progressing as expected - {res}"
                    )
                LOG.info(f"IO validation for {subsys}|{pool_img} is successful.")

        LOG.info("IO Validation is Successfull on all RBD images..")

    def execute_io_ns_masking(self, init_nodes, images, negative=False):
        """
        Execute IO on the namespaces after validating the namespace masking.
        """
        for node in init_nodes:
            clients = prepare_io_execution(
                [{"nqn": "connect-all", "listener_port": 5500, "node": node}],
                gateways=self.gateways,
                cluster=self.cluster,
                return_clients=True,
            )
            if isinstance(images, dict):
                images_node = images.get(node, [])
            else:
                images_node = images
            initiator = [client for client in clients if client.node.id == node][0]
            t1 = threading.Thread(target=initiator.start_fio, kwargs={"io_size": "30%"})
            t2 = threading.Thread(
                target=self.validate_io,
                args=(images_node, negative),  # ← positional args
            )
            # start threads
            t1.start()
            t2.start()
            # wait for threads to complete
            t1.join()
            t2.join()

    def validate_init_namespace_masking(
        self,
        command,
        init_nodes,
        expected_visibility,
        validate_config=None,
    ):
        """Validate that the namespace visibility is correct from all initiators."""
        for node in init_nodes:
            initiator_node = get_node_by_id(self.cluster, node)
            client = NVMeInitiator(initiator_node)
            client.disconnect_all()  # Reconnect NVMe targets
            client.connect_targets(self.gateways[0], config={"nqn": "connect-all"})
            serial_to_namespace = defaultdict(set)

            @retry(
                IOError,
                tries=4,
                delay=3,
            )
            def execute_nvme_command(client_node):
                devices_json, _ = client_node.exec_command(
                    cmd="nvme list --output-format=json", sudo=True
                )
                return json.loads(devices_json)["Devices"]

            devices_json = execute_nvme_command(initiator_node)
            if not devices_json:
                LOG.info(f"No devices found on node {node}")
                continue

            for device in devices_json:
                # --- Case 1: RHEL 9.5 style fields ---
                # device["SerialNumber"] and device["NameSpace"]
                if "SerialNumber" in device and "NameSpace" in device:
                    key = device["NameSpace"]
                    serial_to_namespace[key].add(int(device["SerialNumber"]))
                    continue

                # --- Case 2: RHEL 9.6 style NVMe subsystem/namespace/controller tree ---
                for subsys in device.get("Subsystems", []):
                    for controller in subsys.get("Controllers", []):
                        if controller.get("ModelNumber") == "Ceph bdev Controller":
                            serial = controller.get("SerialNumber")
                            if serial is None:
                                continue

                            for ns in subsys.get("Namespaces", []):
                                nsid = ns.get("NSID")
                                if nsid is not None:
                                    LOG.info(f"NSID: {nsid} Serial: {serial}")
                                    serial_to_namespace[nsid].add(serial)

            def subsystem_nsid_found(dictionary, key, value):
                return key in dictionary and value in dictionary[key]

            if validate_config:
                args = (validate_config or {}).get("args", {})
                subsystem_to_nsid = {args["nsid"]: args["sub_num"]}
                init_node = args.get("init_node")
                ns_to_check, subsystem_to_check = next(iter(subsystem_to_nsid.items()))
                LOG.info(f"{subsystem_to_nsid} : {serial_to_namespace}")
                ns_subsys_found = subsystem_nsid_found(
                    serial_to_namespace, ns_to_check, subsystem_to_check
                )

                if command == "add_host":
                    if node == init_node:
                        if ns_subsys_found:
                            LOG.info(
                                f"Validated - Namespace:Subsystem pair {subsystem_to_nsid} is listed on {node}"
                            )
                        else:
                            LOG.error(
                                f"Namespace:Subsystem pair {subsystem_to_nsid} is not listed on {node}"
                            )
                            raise Exception(
                                f"Expected Namespace:Subsystem pair {subsystem_to_nsid} on {node} but did not find it"
                            )
                    else:
                        if ns_subsys_found:
                            LOG.error(
                                f"Namespace:Subsystem pair {subsystem_to_nsid} is listed on {node}"
                            )
                            raise Exception(
                                f"Did not expect Namespace:Subsystem pair {subsystem_to_nsid} on {node} but found it"
                            )
                        else:
                            LOG.info(
                                f"Validated - Namespace:Subsystem pair {subsystem_to_nsid} is not listed on {node}"
                            )
                elif command == "del_host":
                    if node == init_node:
                        if ns_subsys_found:
                            LOG.error(
                                f"Namespace:Subsystem pair {subsystem_to_nsid} is listed on {node}"
                            )
                            raise Exception(
                                f"Did not expect Namespace:Subsystem pair {subsystem_to_nsid} on {node} but found it"
                            )
                        else:
                            LOG.info(
                                f"Validated - Namespace:Subsystem pair {subsystem_to_nsid} is not listed on {node}"
                            )
                    else:
                        if ns_subsys_found:
                            LOG.error(
                                f"Namespace:Subsystem pair {subsystem_to_nsid} is listed on {node}"
                            )
                            raise Exception(
                                f"Did not expect Namespace:Subsystem pair {subsystem_to_nsid} on {node} but found it"
                            )
                        else:
                            LOG.info(
                                f"Validated - Namespace:Subsystem pair {subsystem_to_nsid} is not listed on {node}"
                            )
            else:
                if (
                    not expected_visibility
                ):  # If expected visibility is False, devices should be empty
                    # Determine if devices list is empty (no Namespaces in any Subsystem)
                    if devices_json:  # Check if Devices is not empty
                        LOG.error(
                            f"Expected no devices for initiator {node}, but found: {devices_json}"
                        )
                        raise Exception(
                            f"Initiator {node} has devices when NS visibility is restricted"
                        )
                    else:
                        LOG.info(f"Validated - no devices found on {node}")
                elif (
                    expected_visibility
                ):  # If expected visibility is True, devices should not be empty
                    if not devices_json:
                        LOG.error(
                            f"Expected devices to be visible for node {node}, but found none."
                        )
                        raise Exception(
                            f"Initiator {node} has no devices when NS visibility is not restricted"
                        )
                    else:
                        # Log Namespace and SerialNumber from each device
                        for device in devices_json:
                            # --- Case 1: RHEL 9.5 style fields ---
                            # device["SerialNumber"] and device["NameSpace"]
                            if "SerialNumber" in device and "NameSpace" in device:
                                key = device["NameSpace"]
                                serial_to_namespace[key].add(
                                    int(device["SerialNumber"])
                                )
                                continue

                            # --- Case 2: RHEL 9.6 style NVMe subsystem/namespace/controller tree ---
                            for subsys in device.get("Subsystems", []):
                                for controller in subsys.get("Controllers", []):
                                    if (
                                        controller.get("ModelNumber")
                                        == "Ceph bdev Controller"
                                    ):
                                        serial = controller.get("SerialNumber")
                                        if serial is None:
                                            continue

                                        for ns in subsys.get("Namespaces", []):
                                            nsid = ns.get("NSID")
                                            if nsid is not None:
                                                serial_to_namespace[nsid].add(serial)
                        LOG.info(
                            f"Validated - {len(devices_json)} devices found on {node}"
                        )

    @staticmethod
    def validate_namespace_masking(
        nsid,
        subnqn,
        namespaces_sub,
        hostnqn_dict,
        ns_visibility,
        command,
        expected_visibility,
    ):
        """Validate that the namespace visibility is correct."""

        if command == "add_host":
            LOG.info(command)
            num_namespaces_per_node = namespaces_sub // len(hostnqn_dict)

            # Determine the initiator node responsible for this nsid based on the calculated range
            node_index = (nsid - 1) // num_namespaces_per_node
            expected_host = list(hostnqn_dict.values())[node_index]

            # Log the visibility of the namespace
            if expected_host in ns_visibility:
                LOG.info(
                    f"Validated - Namespace {nsid} of {subnqn} has the correct nqn {ns_visibility}"
                )
            else:
                LOG.error(
                    f"Namespace {nsid} of {subnqn} has incorrect NQN. Expected {expected_host}, but got {ns_visibility}"
                )
                raise Exception(
                    f"Namespace {nsid} of {subnqn} has incorrect NQN. Expected {expected_host}, but got {ns_visibility}"
                )

        elif command == "del_host":
            LOG.info(command)
            num_namespaces_per_node = namespaces_sub // len(hostnqn_dict)

            # Determine the initiator node responsible for this nsid based on the calculated range
            node_index = (nsid - 1) // num_namespaces_per_node
            expected_host = list(hostnqn_dict.values())[node_index]

            # Log the visibility of the namespace
            if expected_host not in ns_visibility:
                LOG.info(
                    f"Validated - Namespace {nsid} of {subnqn} does not has {expected_host}"
                )
            else:
                LOG.error(
                    f"Namespace {nsid} of {subnqn} has {ns_visibility} which was removed"
                )
                raise Exception(
                    f"Namespace {nsid} of {subnqn} has incorrect NQN. Not expecting {ns_visibility} in {expected_host}"
                )
        else:
            # Validate visibility based on the expected value (for non-add/del host commands)
            if str(ns_visibility).lower() == str(expected_visibility).lower():
                LOG.info(
                    f"Validated - Namespace {nsid} has correct visibility: {ns_visibility}"
                )
            else:
                LOG.error(
                    f"NS {nsid} of {subnqn} has wrong visibility.Expected {expected_visibility} got {ns_visibility}"
                )
                raise Exception(
                    f"NS {nsid} of {subnqn} has wrong visibility.Expected {expected_visibility} got {ns_visibility}"
                )
