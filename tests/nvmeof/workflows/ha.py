"""
NVMe High Availability Module.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph import CommandFailed
from ceph.ceph_admin.daemon import Daemon
from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from ceph.utils import get_node_by_id, get_openstack_driver
from ceph.waiter import WaitUntil
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_gateway import NVMeGateway
from utility.log import Log
from utility.retry import retry
from utility.utils import log_json_dump

LOG = Log(__name__)


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
        self.mtls = config.get("mtls")
        self.gateway_group = config.get("gw_group", "")
        self.orch = Orch(cluster=self.cluster, **{})
        self.daemon = Daemon(cluster=self.cluster, **{})
        self.nvme_pool = config["rbd_pool"]
        self.clients = []

        for gateway in gateways:
            gw_node = get_node_by_id(self.cluster, gateway)
            self.gateways.append(NVMeGateway(gw_node, self.mtls))

        self.ana_ids = [i.ana_group_id for i in self.gateways]
        self.fail_ops = {
            "systemctl": self.system_control,
            "daemon": self.ceph_daemon,
            "power_on_off": self.power_on_off,
        }

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
            args=["ceph", "nvme-gw", "show", self.nvme_pool, repr(self.gateway_group)]
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

    def system_control(self, gateway, action, wait_for_active_state=True):
        """SystemCtl methods to control nvme unit service states.

        Args:
            gateway: NVMe gateway object.
            action: systemctl "stop"|"start"
            wait_for_active_state: wait for the active using bool value.

        - wait_for_active_state:
            True: wait for the service unit becomes active.
            False: wait for the service unit becomes inactive.
            None: do not wait, return

        Returns:
            Boolean
        """
        ops = {
            "start": gateway.systemctl.start,
            "stop": gateway.systemctl.stop,
            "is-active": gateway.systemctl.is_active,
        }

        unit_service = gateway.system_unit_id
        op = ops[action]
        op(unit_service)

        if wait_for_active_state is None:
            return

        is_active = ops["is-active"]
        for w in WaitUntil(timeout=300):
            _active = is_active(unit_service)
            if _active == wait_for_active_state:
                LOG.info(
                    f"[ {unit_service} ] {action}ing NVMeofGW service is successfull."
                )
                return True
            LOG.warning(
                f"[ {unit_service} ] {action}ing NVMeofGW service is still not successfull. check again"
            )

        if w.expired:
            LOG.error(
                f"[ {unit_service} ] {action}ing NVMeofGW service failed even after 300s timeout.."
            )

        return False

    def nvmeof_daemon_state(self, name):
        """Return True if nvmeof daemon is running else False."""
        daemon_type, daemon_id = name.split(".", 1)
        ps_args = {
            "base_cmd_args": {"format": "json"},
            "args": {
                "daemon_type": daemon_type,
                "daemon_id": daemon_id,
                "refresh": True,
            },
        }
        out, _ = self.orch.ps(ps_args)
        out = json.loads(out)
        if out[0]["status"] == 1:
            return True
        elif out[0]["status"] in [-1, 0]:
            return False

    def ceph_daemon(self, gateway, action, wait_for_active_state=True):
        """Ceph daemon commands to control nvme daemon states.

        Args:
            gateway: NVMe gateway object.
            action: daemon orch "stop"|"start"
            wait_for_active_state: wait for the active using bool value.

        - wait_for_active_state:
            True: wait for the daemon becomes active.
            False: wait for the daemon becomes inactive.
            None: do not wait, return

        Returns:
            Boolean
        """
        ops = {
            "start": self.daemon.start,
            "stop": self.daemon.stop,
            "is-active": self.nvmeof_daemon_state,
        }
        daemon = gateway.daemon_name

        action_args = {"command": action, "pos_args": [daemon]}

        op = ops[action]
        op(action_args)

        if wait_for_active_state is None:
            return

        is_active = ops["is-active"]
        for w in WaitUntil(timeout=300):
            _active = is_active(daemon)
            if _active == wait_for_active_state:
                LOG.info(f"[ {daemon} ] {action}ing NVMeofGW Daemon is successfull.")
                return True
            LOG.warning(
                f"[ {daemon} ] {action}ing NVMeofGW Daemon is still not successfull. check again"
            )

        if w.expired:
            LOG.error(
                f"[ {daemon} ] {action}ing NVMeofGW Daemon failed even after 300s timeout.."
            )

        return False

    def is_node_active(self, driver, node):
        """Return true if the given node is powered on and false if powered off"""
        op = driver.ex_get_node_details(node)
        if op.state == "running":
            return True
        elif op.state == "stopped":
            return False

    def power_on_off(self, gateway, action, wait_for_active_state=None):
        """Power on and off nvme daemon nodes.

        Args:
            gateway: NVMe gateway object.
            action: "stop" | "start"
            wait_for_active_state: wait for the active using bool value.

        - wait_for_active_state:
            True: wait for the node to power on.
            False: wait for the node to power off.
            None: do not wait, return

        Returns:
            Boolean
        """
        osp_cred = self.config.get("osp_cred")
        driver = get_openstack_driver(osp_cred)
        ops = {
            "start": driver.ex_start_node,
            "stop": driver.ex_stop_node,
            "is-active": self.is_node_active,
        }
        nodename = gateway.node.hostname.lower()

        driver_node = next(
            (node for node in driver.list_nodes() if node.name.lower() == nodename),
            None,
        )

        op = ops[action]
        op(driver_node)

        if wait_for_active_state is None:
            return

        is_active = ops["is-active"]
        for w in WaitUntil(timeout=300):
            _active = is_active(driver, driver_node)
            if _active == wait_for_active_state:
                LOG.info(f"[ {nodename} ] {action} is successfull.")
                return True
            LOG.warning(
                f"[ {nodename} ] {action} is still not successfull. check again"
            )

        if w.expired:
            LOG.error(f"[ {nodename} ] {action} failed even after 300s timeout..")

        return False

    def failover(self, gateway, fail_tool):
        """HA Failover on the NVMeoF Gateways.

        Initiate Failover
        - List the gateways which not have failures.
        - Initiate failures on GWs which has to be down systemctl/daemon stop
        - Validate the ANA states of failed GWs are optimized in one of the other working GWs.

        Post Failover Validation
        - List out namespaces associated with the failed Gateways using ANA group ids.
        - Check for 5 Consecutive times for the increments in write/read to validate IO continuation.
        """
        hostname = gateway.hostname
        start_counter = float()
        start_time = str()
        end_counter = float()
        end_time = str()

        # Initiate Failover
        fail_op = self.fail_ops[fail_tool]
        LOG.info(f"[ {hostname} ]: Failing Over NVMe Service using {fail_tool} command")
        res = fail_op(gateway=gateway, action="stop", wait_for_active_state=False)
        if not res:
            raise Exception(
                f"[ {hostname} ]: Error in stopping NVMe Service using {fail_tool} command "
            )
        start_counter, start_time = get_current_timestamp()

        # Wait until 60 seconds
        for w in WaitUntil():
            # Check for gateway unavailability
            if self.check_gateway_availability(
                gateway.ana_group_id, state="UNAVAILABLE"
            ):
                LOG.info(f"[ {hostname} ] NVMeofGW service is UNAVAILABLE.")
                active = self.get_optimized_state(gateway.ana_group_id)

                # Find optimized path
                # Condidtion to fail if multiple Active path exists for a gateway.
                if active and 1 <= len(active) < 2:
                    end_counter, end_time = get_current_timestamp()
                    LOG.info(
                        f"{list(active[0])} is new and only Active GW for failed {hostname}"
                    )
                    break

                if len(active) > 1:
                    raise Exception(
                        f"[ {hostname} ] Found more than one Active path - {log_json_dump(active)}"
                    )
            LOG.warning(f"[ {hostname} ] is still in AVAILABLE state..")

        if w.expired:
            raise TimeoutError(
                f"[ {hostname} ] Failover of NVMeofGW service failed after 60s timeout.."
            )

        LOG.info(
            f"[ {hostname} ] Total time taken to failover - {end_counter - start_counter} seconds"
        )
        return {
            "failover-start-time": start_time,
            "failover-end-time": end_time,
            "failover-start-counter-time": start_counter,
            "failover-end-counter-time": end_counter,
        }

    def failback(self, gateway, fail_tool):
        """Failback the Gateways.

        Args:
            gateway: Gateway to be fail-back.
            fail_tool: tool to fail the GW service
        """
        hostname = gateway.hostname
        start_counter = float()
        start_time = str()
        end_counter = float()
        end_time = str()

        # Initiate Fail-back
        fail_op = self.fail_ops[fail_tool]
        LOG.info(
            f"[ {hostname} ]: Failback / Restore Gateway using {fail_tool} command"
        )
        res = fail_op(gateway=gateway, action="start", wait_for_active_state=True)
        if not res:
            raise Exception(
                f"[ {hostname} ]: Error in starting NVMe Service using {fail_tool} command "
            )
        start_counter, start_time = get_current_timestamp()

        for w in WaitUntil():
            # Check for gateway availability
            if self.check_gateway_availability(gateway.ana_group_id):
                LOG.info(f"[ {hostname} ] NVMeofGW service is AVAILABLE.")

                active = self.get_optimized_state(gateway.ana_group_id)
                if active and 1 <= len(active) < 2:
                    state = active[0]

                    # check gateway for its own original path.
                    if gateway.ana_group["name"] in state:
                        end_counter, end_time = get_current_timestamp()
                        LOG.info(
                            f"{hostname} restored to original path - {log_json_dump(state)}"
                        )
                        break

                if len(active) > 1:
                    raise Exception(
                        f"[ {hostname} ] More than one Active path found - {log_json_dump(active)}"
                    )
                LOG.warning(f"[ {hostname} ] No Active path found")
                continue

            LOG.warning(f"[ {hostname} ] is still not in AVAILABLE state..")
            continue

        if w.expired:
            raise TimeoutError(
                f"[ {hostname} ] Fail-back of NVMeofGW service failed even after 60s timeout.."
            )
        LOG.info(
            f"[ {hostname} ] Time taken to Failback - {end_counter - start_counter} seconds"
        )
        return {
            "failback-start-time": start_time,
            "failback-end-time": end_time,
            "failback-start-counter-time": start_counter,
            "failback-end-counter-time": end_counter,
        }

    @retry(IOError, tries=3, delay=3)
    def compare_client_namespace(self, uuids):
        lsblk_devs = []
        for client in self.clients:
            lsblk_devs.extend(client.fetch_lsblk_nvme_devices())

        LOG.info(f"Expcted NVMe Targets : {uuids} Vs LSBLK devices: {lsblk_devs}")
        if sorted(uuids) != sorted(lsblk_devs):
            raise IOError("Few Namespaces are missing!!!")
        LOG.info("All namespaces are listed at Client(s)")
        return True

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

    def fetch_namespaces(self, gateway, failed_ana_grp_ids=[]):
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
        all_ns = []
        for subsystem in subsystems["subsystems"]:
            sub_name = subsystem["nqn"]
            cmd_args = {"args": {"subsystem": subsystem["nqn"]}}
            _, nspaces = gateway.namespace.list(**{**args, **cmd_args})
            nspaces = json.loads(nspaces)["namespaces"]
            all_ns.extend(nspaces)

            if failed_ana_grp_ids:
                for ns in nspaces:
                    if ns["load_balancing_group"] in failed_ana_grp_ids:
                        # <subsystem>|<nsid>|<pool_name>|<image>
                        ns_info = f"nsid-{ns['nsid']}|{ns['rbd_pool_name']}|{ns['rbd_image_name']}"
                        namespaces.append(f"{sub_name}|{ns_info}")
        if not failed_ana_grp_ids:
            LOG.info(f"All namespaces : {log_json_dump(all_ns)}")
            return all_ns

        LOG.info(
            f"Namespaces found for ANA-grp-id [{failed_ana_grp_ids}]: {log_json_dump(namespaces)}"
        )
        return namespaces

    @retry((IOError, TimeoutError, CommandFailed), tries=3, delay=1)
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
                    f"[ {subsys}|{pool_img} ] RBD DU Detailed - {log_json_dump(samples)}"
                )
                LOG.info(f"[ {subsys}|{pool_img} ] RBD DU samples - {res}")
                if not validate_incremetal_io(res):
                    raise IOError(
                        f"[ {subsys}|{pool_img} ] IO is not progressing - {res}"
                    )
                LOG.info(f"IO validation for {subsys}|{pool_img} is successful.")

        LOG.info("IO Validation is Successfull on all RBD images..")

    def run(self):
        """Execute the HA failover and failback with IO validation."""
        fail_methods = self.config["fault-injection-methods"]
        initiators = self.config["initiators"]
        executor = ThreadPoolExecutor()
        io_tasks = []

        try:
            # Prepare FIO Execution
            namespaces = self.fetch_namespaces(self.gateways[0])
            self.prepare_io_execution(initiators)

            # Check for targets at clients
            self.compare_client_namespace([i["uuid"] for i in namespaces])

            repeat_ha_count = self.config.get("repeat_ha_count", 1)
            # Start IO Execution
            for initiator in self.clients:
                io_tasks.append(executor.submit(initiator.start_fio))
            time.sleep(20)  # time sleep for IO to Kick-in

            # Failover and Failback
            for i in range(0, repeat_ha_count):
                LOG.info(f"Failover and failback execution for iteration number {i}")
                for fail_method in fail_methods:
                    fail_tool = fail_method["tool"]
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

                    time.sleep(20)

        except BaseException as err:  # noqa
            raise Exception(err)
        finally:
            if io_tasks:
                LOG.info("Waiting for completion of IOs.")
                executor.shutdown(wait=True, cancel_futures=True)
