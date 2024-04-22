"""
NVMe High Availability Module.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from ceph.waiter import WaitUntil
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_gateway import NVMeGateway
from utility.log import Log

LOG = Log(__name__)


def log_json_dump(data):
    return json.dumps(data, indent=4)


def get_current_timestamp():
    return time.perf_counter(), time.asctime()


class HighAvailability:
    def __init__(self, ceph_cluster, gateways, **config):
        """Initialize NVMeoF Gateway High Availability class.

        Args:
            cluster: Ceph cluster
            gateways: Gateway node Ids
            config: HA config
        """
        self.cluster = ceph_cluster
        self.config = config
        self.gateways = []
        self.orch = Orch(cluster=self.cluster, **{})
        self.nvme_pool = config["rbd_pool"]
        self.clients = []

        for gateway in gateways:
            gw_node = get_node_by_id(self.cluster, gateway)
            self.gateways.append(NVMeGateway(gw_node))

        self.ana_ids = [i.ana_group_id for i in self.gateways]

    def check_gateway(self, node_id):
        """Check node is NVMeoF Gateway node.

        Args:
            node_id: Ceph node Id (ex., node6)
        """
        for gw in self.gateways:
            if gw.node.id == node_id:
                LOG.info(f"[{node_id}] {gw.node.hostname} is NVMeoF Gateway node.")
                return gw
        raise Exception(f"{node_id} doesn't match to any gateways provided...")

    def catogorize(self, gws):
        """Categorize to-be failed and running GWs.

        Args:
            gws: gateways to be failed/stopped

        Returns:
            list of,
             - to-be failed gateways
             - rest of the gateways
        """
        fail_gws = []
        running_gws = []

        # collect impending Gateways to be failed.
        if isinstance(gws, str):
            gws = [gws]
        for gw_id in gws:
            fail_gws.append(self.check_gateway(gw_id))

        # Collect rest of the Gateways
        for gw in self.gateways:
            if gw.node.id not in gws:
                running_gws.append(gw)

        return fail_gws, running_gws

    @staticmethod
    def string_to_dict(string):
        """Parse ANA states from the string."""
        states = string.replace(" ", "").split(",")
        dict = {}
        for state in states:
            if not state:
                continue
            _id, _state = state.split(":")
            dict[int(_id)] = _state
        return dict

    def ana_states(self, gw_group=""):
        """Fetch ANA states and convert into python dict."""

        out, _ = self.orch.shell(
            args=["ceph", "nvme-gw", "show", self.nvme_pool, repr(gw_group)]
        )
        states = {}
        for data in out.split("}"):
            data = data.strip()
            if not data:
                continue
            data = json.loads(f"{data}}}")
            if data.get("ana states"):
                gw = data["gw-id"]
                states[gw] = data
                states[gw].update(self.string_to_dict(data["ana states"]))

        return states

    def check_gateway_availability(self, ana_id, state="AVAILABLE", ana_states=None):
        """Check for failed ANA GW become unavailable.

        Args:
            ana_id: Gateway ANA group id.
            state: Gateway availability state
            ana_states: Overall ana state. (output from self.ana_states)
        Return:
            True if Gateway availability is in expected state, else False
        """
        # get ANA states
        if not ana_states:
            ana_states = self.ana_states()

        # Check Availability of ANA Group Gateway
        for _, _state in ana_states.items():
            if _state["anagrp-id"] == ana_id:
                if _state["Availability"] == state:
                    return True
                return False
        return False

    def get_optimized_state(self, failed_ana_id):
        """Fetch the Optimized ANA states for failed gateway.

        Args:
            gateway: The gateway which is operational.
            failed_ana_id: failed gateway ANA Group Id.

        Returns:
            gateways which shows ACTIVE state for failed ANA Group Id
        """
        # get ANA states
        ana_states = self.ana_states()

        # Fetch failed ANA Group Id in ACTIVE state
        found = []

        for ana_gw_id, state in ana_states.items():
            if (
                state["Availability"] == "AVAILABLE"
                and state.get(failed_ana_id) == "ACTIVE"
            ):
                found.append({ana_gw_id: state})

        return found

    def system_control(self, gateway, state="is_active"):
        # todo: handling failure injection via systemctl
        pass

    def ceph_daemon(self, gateway, state="running"):
        # todo: handling failure injection via systemctl
        pass

    def failover(self, gateway, fail_tool="systemctl"):
        """HA Failover on the NVMeoF Gateways.

        Initiate Failover
        - List the gateways which not have failures.
        - Initiate failures on GWs which has to be down systemctl/daemon stop
        - Validate the ANA states of failed GWs are optimized in one of the other working GWs.

        Post Failover Validation
        - List out namespaces associated with the failed Gateways using ANA group ids.
        - Check for 5 Consecutive times for the increments in write/read to validate IO continuation.
        """
        LOG.info(f"[ {gateway.hostname} ] Failing over Gateway")

        # Initiate Failover
        LOG.info(f"[ {gateway.hostname} ]: Failing NVMe Service {fail_tool} command")
        service = gateway.system_unit_id
        gateway.systemctl.stop(service)

        start_counter = float()
        start_time = str()
        end_counter = float()
        end_time = str()

        # Wait until 60 seconds
        for w in WaitUntil():
            # Check for service/daemon failure
            if not gateway.systemctl.is_active(service):
                LOG.info(f"[ {service} ] NVMeofGW service Stopped successfully.")
                if not start_counter:
                    start_counter, start_time = get_current_timestamp()

                # Check for gateway unavailability
                if self.check_gateway_availability(
                    gateway.ana_group_id, state="UNAVAILABLE"
                ):
                    LOG.info(f"[ {service} ] NVMeofGW service is UNAVAILABLE.")
                    active = self.get_optimized_state(gateway.ana_group_id)

                    # Find optimized path
                    # Condidtion to fail if multiple Active path exists for a gateway.
                    if active and 1 <= len(active) < 2:
                        end_counter, end_time = get_current_timestamp()
                        LOG.info(
                            f"{list(active[0])} is new and only Active GW for failed {gateway.hostname}"
                        )
                        break

                    if len(active) > 1:
                        raise Exception(
                            f"[ {gateway.hostname} ] Found more than one Active path - {log_json_dump(active)}"
                        )
                LOG.warning(f"[ {service} ] is still in AVAILABLE state..")
                continue

            LOG.warning(
                f"[ {service} ] Stopping NVMeofGW service Failed. try again!!!!"
            )

        if w.expired:
            raise TimeoutError(
                f"[ {gateway.hostname} ] Failover of NVMeofGW service failed after 60s timeout.."
            )

        LOG.info(
            f"[ {gateway.hostname} ] Total time taken to failover - {end_counter - start_counter}s"
        )
        return {
            "failover-start-time": start_time,
            "failover-end-time": end_time,
            "failover-start-counter-time": start_counter,
            "failover-end-counter-time": end_counter,
        }

    def failback(self, gateway, fail_tool="systemctl"):
        """Failback the Gateways.

        Args:
            gateway: Gateway to be fail-back.
            fail_tool: tool to fail the GW service
        """
        LOG.info(f"[ {gateway.hostname} ] Failback / Restore Gateway")

        # Initiate Fail-back
        LOG.info(f"[ {gateway.hostname} ]: Starting NVMe Service {fail_tool} command")
        service = gateway.system_unit_id
        gateway.systemctl.start(service)

        start_counter = float()
        start_time = str()
        end_counter = float()
        end_time = str()

        for w in WaitUntil():
            # Check for service/daemon running satte
            if gateway.systemctl.is_active(service):
                LOG.info(f"[ {service} ] NVMeofGW service Started successfully.")
                if not start_counter:
                    start_counter, start_time = get_current_timestamp()

                # Check for gateway availability
                if self.check_gateway_availability(gateway.ana_group_id):
                    LOG.info(f"[ {service} ] NVMeofGW service is AVAILABLE.")

                    active = self.get_optimized_state(gateway.ana_group_id)
                    if active and 1 <= len(active) < 2:
                        state = active[0]

                        # check gateway for its own original path.
                        if gateway.ana_group["name"] in state:
                            end_counter, end_time = get_current_timestamp()
                            LOG.info(
                                f"{gateway.hostname} restored to original path - {log_json_dump(state)}"
                            )
                            break

                    if len(active) > 1:
                        raise Exception(
                            f"[ {gateway.hostname} ] More than one Active path found - {log_json_dump(active)}"
                        )
                    LOG.warning(f"[ {gateway.hostname} ] No Active path found")
                    continue

                LOG.warning(f"[ {service} ] is still not in AVAILABLE state..")
                continue

            LOG.warning(
                f"[ {service} ] NVMeofGW service START still in Failed State. try again !!!!"
            )

        if w.expired:
            raise TimeoutError(
                f"[ {gateway.hostname} ] Fail-back of NVMeofGW service failed after 60s timeout.."
            )
        LOG.info(
            f"[ {gateway.hostname} ] Time taken to Failback - {end_counter - start_counter} seconds"
        )
        return {
            "failback-start-time": start_time,
            "failback-end-time": end_time,
            "failback-start-counter-time": start_counter,
            "failback-end-counter-time": end_counter,
        }

    def prepare_io_execution(self, io_clients):
        """Prepare FIO Execution.

        initiators:                             # Configure Initiators with all pre-req
          - nqn: connect-all
            listener_port: 4420
            node: node10
        """
        for io_client in io_clients:
            node = get_node_by_id(self.cluster, io_client["node"])
            client = NVMeInitiator(node, self.gateways[0])
            client.connect_targets(io_client)
            self.clients.append(client)

    def fetch_namespaces(self, gateway, failed_ana_grp_ids):
        """Fetch all namespaces for failed gateways.

        Args:
            gateway: Operational gateway
            failed_ana_grp_ids: Failed or to-be failed gateway ids
        Returns:
            list of namespaces
        """
        args = {"base_cmd_args": {"format": "json"}}
        _, subsystems = gateway.subsystem.list(**args)
        subsystems = json.loads(subsystems)

        namespaces = []
        for subsystem in subsystems["subsystems"]:
            sub_name = subsystem["nqn"]
            cmd_args = {"args": {"subsystem": subsystem["nqn"]}}
            _, nspaces = gateway.namespace.list(**{**args, **cmd_args})
            nspaces = json.loads(nspaces)

            for ns in nspaces["namespaces"]:
                if ns["load_balancing_group"] in failed_ana_grp_ids:
                    # <subsystem>|<nsid>|<pool_name>|<image>
                    ns_info = f"nsid-{ns['nsid']}|{ns['rbd_pool_name']}|{ns['rbd_image_name']}"
                    namespaces.append(f"{sub_name}|{ns_info}")

        LOG.info(
            f"Namespaces found for ANA-grp-id [{failed_ana_grp_ids}]: {log_json_dump(namespaces)}"
        )
        return namespaces

    def validate_io(self, namespaces):
        """Validate Continuous IO on namespaces.

        - Collect rbd disk usage info for each rbd image.
        - Validate written bytes value is incremental.

        Args:
            namespaces: list of namespaces
        """

        def io_value(ns):
            sub_ns, pool, image = ns.rsplit("|", 2)
            count = 3
            samples = []
            for _ in range(count):
                out, _ = self.orch.shell(args=[f"rbd --format json du {pool}/{image}"])
                out = json.loads(out)["images"][0]
                samples.append(out)
                time.sleep(3)
            return sub_ns, samples

        def validate_incremetal_io(write_samples):
            for i in range(len(write_samples) - 1):
                if write_samples[i] >= write_samples[i + 1]:
                    return False
            return True

        with parallel() as p:
            for namespace in namespaces:
                p.spawn(io_value, namespace)

            for result in p:
                subsys, samples = result
                res = [i["used_size"] for i in samples]

                LOG.info(f"[ {subsys} ] RBD Disk Usage samples - {res}")
                if not validate_incremetal_io(res):
                    raise Exception(f"[ {subsys} ] IO is not progressing - {res}")
                LOG.info(
                    f"IO validation for {subsys} is successful. {log_json_dump(samples)}"
                )

        LOG.info("IO Validation is Successfull on all RBD images..")

    def run(self):
        """Execute the HA failover and failback with IO validation."""
        fail_methods = self.config["fault-injection-methods"]
        initiators = self.config["initiators"]
        executor = ThreadPoolExecutor()
        io_tasks = []

        try:
            # Prepare FIO Execution
            self.prepare_io_execution(initiators)

            # Start IO Execution
            for initiator in self.clients:
                io_tasks.append(executor.submit(initiator.start_fio))
            time.sleep(20)  # time sleep for IO to Kick-in

            # Failover and Failback
            for fail_method in fail_methods:
                fail_tool = fail_method.get("fail_tool", "systemctl")
                nodes = fail_method["nodes"]
                if not isinstance(nodes, list):
                    nodes = [nodes]

                LOG.info(
                    f"Failover and Failback execution on {nodes} using {fail_tool}"
                )
                log_json_dump(fail_method)

                fail_gws, operational_gws = self.catogorize(nodes)
                fail_gw_ana_ids = [gw.ana_group_id for gw in fail_gws]
                gateway = operational_gws[0]

                # Primary check - validate IO continuation
                namespaces = self.fetch_namespaces(gateway, fail_gw_ana_ids)
                self.validate_io(namespaces)

                # Fail Over
                with parallel() as p:
                    for gw in fail_gws:
                        p.spawn(self.failover, gw, fail_tool)
                    for result in p:
                        LOG.info(log_json_dump(result))
                    self.validate_io(namespaces)

                # Fail Back
                with parallel() as p:
                    for gw in fail_gws:
                        p.spawn(self.failback, gw, fail_tool)
                    for result in p:
                        LOG.info(log_json_dump(result))
                    self.validate_io(namespaces)

        except BaseException as err:  # noqa
            raise Exception(err)
        finally:
            if io_tasks:
                executor.shutdown(wait=True, cancel_futures=True)
