"""
NVMe High Availability Module.
"""

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph_admin.daemon import Daemon
from ceph.ceph_admin.host import Host
from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from ceph.utils import find_vm_node_by_hostname, get_openstack_driver
from ceph.waiter import WaitUntil
from tests.nvmeof.workflows.gateway_entities import fetch_namespaces
from tests.nvmeof.workflows.initiator import (
    compare_client_namespace,
    prepare_io_execution,
    validate_initiator,
)
from tests.nvmeof.workflows.nvme_utils import (
    ana_states,
    catogorize,
    check_gateway_availability,
    get_optimized_state,
    validate_io,
)
from utility.log import Log
from utility.utils import log_json_dump

LOG = Log(__name__)
NSEC_TO_SEC = 1e-9  # 1 nanosecond = 1e-9 seconds
USEC_TO_SEC = 1e-6  # 1 microsecond = 1e-6 seconds


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
        self.gw_list = gateways
        self.gateways = []
        self.mtls = config.get("mtls")
        self.gateway_group = config.get("gw_group", "")
        self.orch = Orch(cluster=self.cluster, **{})
        self.daemon = Daemon(cluster=self.cluster, **{})
        self.host = Host(cluster=self.cluster, **{})
        self.nvme_pool = config["rbd_pool"]
        self.nvme_service = config.get("nvme_service")
        self.clients = []
        self.fail_ops = {
            "systemctl": self.system_control,
            "daemon": self.ceph_daemon,
            "power_on_off": self.power_on_off,
            "maintanence_mode": self.maintanence_mode,
        }

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

    def daemon_redeploy(self, gateway):
        """Ceph daemon redeploy commands to control nvme daemon states.

        Args:
            gateway: NVMe gateway object.

        - wait_for_active_state:
            True: wait for the daemon becomes active.
            False: wait for the daemon becomes inactive.
            None: do not wait, return

        Returns:
            Boolean
        """
        daemon = gateway.daemon_name

        action_args = {"command": "redeploy", "pos_args": [daemon]}
        out = self.daemon.redeploy(action_args)
        if "Scheduled" not in out[0]:
            raise Exception(f"[ {daemon} ]: Error in redeploying NVMe Service ")

        # Wait for the daemon to become active
        for w in WaitUntil(timeout=300):
            _active = self.nvmeof_daemon_state(daemon)
            if _active:
                LOG.info(f"[ {daemon} ] redeploying NVMeofGW Daemon is successfull.")
                break
            else:
                LOG.warning(
                    f"[ {daemon} ] redeploying NVMeofGW Daemon is still not successfull. check again"
                )

        # If the loop times out, log an error and return False
        if w.expired:
            LOG.error(
                f"[ {daemon} ] redeploying NVMeofGW Daemon failed even after 300s timeout.."
            )
            return False

        # Only check the ANA states if the daemon is active
        states = ana_states(self.nvme_service, self.orch)

        # Validate the service state for each host
        for host, state in states.items():
            daemon_host = f"client.{daemon}"
            if host == daemon_host:
                if state["Availability"] == "AVAILABLE":
                    LOG.info(f"[ {daemon} ] NVMeofGW service is AVAILABLE.")
                    return True
                else:
                    raise Exception(f"[ {daemon} ] NVMeofGW service is UNAVAILABLE.")

    def is_node_active(self, driver, node):
        """Return true if the given node is powered on and false if powered off"""
        op = driver.ex_get_node_details(node)
        if op.state == "running":
            return True
        elif op.state == "stopped":
            return False

    def maintanence_mode(self, gateway, action, wait_for_active_state=None):
        """Ceph daemon commands to control nvme daemon states.

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
        ops = {
            "start": self.host.exit,
            "stop": self.host.enter,
            "is-active": self.nvmeof_daemon_state,
        }
        daemon = gateway.daemon_name
        gateway_node = daemon.split(".")[-2]

        action_args = {"command": action, "args": {"node": gateway_node}}

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
        # check if cloud_type is openstack then power on/off the node
        if self.config.get("cloud-type") == "openstack":
            LOG.info(
                f"Powering on/off {gateway.node.hostname} node in "
                f"environment {self.config.get('cloud-type')} in {action} mode"
            )
            osp_cred = self.config.get("osp_cred")
            driver = get_openstack_driver(osp_cred)
            ops = {
                "start": driver.ex_start_node,
                "stop": driver.ex_stop_node,
                "is-active": self.is_node_active,
            }
            nodename = gateway.node.hostname.lower().replace("-", "_")
            driver_node = next(
                (
                    node
                    for node in driver.list_nodes()
                    if node.name.lower().replace("-", "_") == nodename
                ),
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
        elif self.config.get("cloud-type") == "ibmc":
            LOG.info(
                f"Powering on/off {gateway.node.hostname} node in "
                f"environment {self.config.get('cloud-type')} in {action} mode"
            )
            vm_node = find_vm_node_by_hostname(self.cluster, gateway.node.hostname)
            if action == "start":
                vm_node.power_on()
                return True
            elif action == "stop":
                vm_node.shutdown(wait=True)
                return True
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

        try:
            # Initiate Failover
            fail_op = self.fail_ops[fail_tool]
            LOG.info(
                f"[ {hostname} ]: Failing Over NVMe Service using {fail_tool} command"
            )
            res = fail_op(gateway=gateway, action="stop", wait_for_active_state=False)
            if not res:
                raise Exception(
                    f"[ {hostname} ]: Error in stopping NVMe Service using {fail_tool} command "
                )

            # Wait until 60 seconds
            for w in WaitUntil():
                # Check for gateway unavailability
                if check_gateway_availability(
                    self.nvme_service,
                    gateway.ana_group_id,
                    self.orch,
                    state="UNAVAILABLE",
                ):
                    LOG.info(f"[ {hostname} ] NVMeofGW service is UNAVAILABLE.")
                    active = get_optimized_state(
                        self.nvme_service, self.orch, gateway.ana_group_id
                    )

                    # Find optimized path
                    # Condition to fail if multiple Active path exists for a gateway.
                    if active and 1 <= len(active) < 2:
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

            return {
                "failed-gw": gateway,
            }

        except BaseException as err:  # noqa
            raise Exception(err)

    def failback(self, gateway, fail_tool):
        """Failback the Gateways.

        Args:
            gateway: Gateway to be fail-back.
            fail_tool: tool to fail the GW service
        """
        hostname = gateway.hostname

        # Initiate Fail-back
        fail_op = self.fail_ops[fail_tool]
        LOG.info(
            f"[ {hostname} ]: Failback / Restore Gateway using {fail_tool} command"
        )
        try:
            res = fail_op(gateway=gateway, action="start", wait_for_active_state=True)
            if not res:
                raise Exception(
                    f"[ {hostname} ]: Error in starting NVMe Service using {fail_tool} command "
                )

            for w in WaitUntil():
                # Check for gateway availability
                if check_gateway_availability(
                    self.nvme_service, gateway.ana_group_id, self.orch
                ):
                    LOG.info(f"[ {hostname} ] NVMeofGW service is AVAILABLE.")

                    active = get_optimized_state(
                        self.nvme_service, self.orch, gateway.ana_group_id
                    )
                    if active and 1 <= len(active) < 2:
                        state = active[0]

                        # check gateway for its own original path.
                        if gateway.ana_group["name"] in state:
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

            return {
                "failed-gw": gateway,
            }

        except BaseException as err:  # noqa
            raise Exception(err)

    def _clat_value_to_base_units(self, value_str):
        """Parse FIO clat value (e.g. '6939.4k', '13843M', '235213') to base units (usec or nsec)."""
        value_str = value_str.strip()
        if value_str.endswith("k"):
            return float(value_str[:-1]) * 1000
        if value_str.endswith("M"):
            return float(value_str[:-1]) * 1_000_000
        return float(value_str)

    def nsec_to_sec(self, nsec_str):
        """Convert FIO clat max string (e.g. '218589k', '13843M') from nanosec to seconds."""
        return self._clat_value_to_base_units(nsec_str) * NSEC_TO_SEC

    def usec_to_sec(self, usec_str):
        """Convert FIO clat max string (e.g. '6939.4k', '13843M') from microsec to seconds."""
        return self._clat_value_to_base_units(usec_str) * USEC_TO_SEC

    def calculate_max_clat_time(self, fio_outputs):
        # Extract failover time: match both clat (usec) and clat (nsec), convert max to seconds
        try:
            clat_max_values = []
            # Match "clat (usec): ... max=VAL" or "clat (nsec): ... max=VAL"; capture unit and max value
            pattern = re.compile(
                r"clat \((usec|nsec)\): min=[^,]+, max=([0-9]+(?:\.[0-9]+)?[a-zA-Z]*)(?=[,\s])"
            )
            for each_device_output in fio_outputs[0]:
                for match in pattern.finditer(str(each_device_output)):
                    unit, value_str = match.group(1), match.group(2)
                    if unit == "usec":
                        sec_val = self.usec_to_sec(value_str)
                    else:
                        sec_val = self.nsec_to_sec(value_str)
                    clat_max_values.append(sec_val)
                    LOG.info(
                        f"Max clat value ({unit}): {value_str} -> {sec_val} seconds"
                    )
            if clat_max_values:
                LOG.info(
                    f"Failover time for namespaces is {max(clat_max_values)} seconds"
                )
        except Exception as e:
            LOG.error(f"Failed to parse FIO output: {e}")

    def run(self, FEWR_NAMESPACES=False):
        """Execute the HA failover and failback with IO validation."""
        fail_methods = self.config["fault-injection-methods"]
        initiators = self.config["initiators"]

        try:
            # Prepare FIO Execution
            LOG.info("Fetching namespaces from all gateways")
            namespaces = fetch_namespaces(self.gateways[0])

            LOG.info("Preparing IO execution")
            # Get pre-configured initiators if available
            pre_configured = self.config.get("pre_configured_initiators")
            clients = prepare_io_execution(
                initiators,
                gateways=self.gateways,
                cluster=self.cluster,
                return_clients=True,
                pre_configured_initiators=pre_configured,
            )
            # Raise exception if clients are not found
            if not clients:
                raise Exception("Clients not found")
            self.clients = clients

            LOG.info("Checking for targets at clients and validating namespaces")
            # Check for targets at clients
            if FEWR_NAMESPACES:
                compare_client_namespace(
                    clients, [i["uuid"] for i in namespaces], FEWR_NAMESPACES=True
                )
            else:
                compare_client_namespace(clients, [i["uuid"] for i in namespaces])

            repeat_ha_count = self.config.get("repeat_ha_count", 1)
            LOG.info(f"Repeat HA count is {repeat_ha_count}")
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

                    LOG.info("categorize failed gateways")
                    fail_gws, _ = catogorize(self.nvme_service, nodes)
                    fail_gw_ana_ids = []
                    namespaces = []
                    all_failed_ns = {}
                    for gw in fail_gws:
                        fail_gw_ana_ids.append(gw.ana_group_id)
                        namespaces_gw = fetch_namespaces(
                            gw, [gw.ana_group_id], get_list=True
                        )
                        ns_list = [ns.get("list") for ns in namespaces_gw]
                        ns_info = [ns.get("info") for ns in namespaces_gw]
                        LOG.info(
                            f"Namespaces for failed gateway {gw.node.ip_address} before failover are {ns_list}"
                        )
                        namespaces.extend(ns_info)
                        all_failed_ns.update({gw.ana_group_id: ns_list})
                        # Validate initiator for the failed gateway
                        LOG.info(
                            f"Validating initiator for the failed gateway {gw.node.ip_address}"
                        )
                        validate_initiator(self.clients, gw, ns_list)

                    # Fail Over
                    with parallel() as p:
                        initiators = self.config["initiators"]
                        io_tasks = []

                        # TODO: Move this outside of the run
                        #   keep it as separate function.
                        if len(namespaces) >= 1:
                            max_workers = (
                                len(initiators) * len(namespaces)
                                if initiators
                                else len(namespaces)
                            )  # 20 devices + 10 buffer per initiator
                            executor = ThreadPoolExecutor(
                                max_workers=max_workers,
                            )
                        else:
                            executor = ThreadPoolExecutor()

                        # Start IO Execution
                        LOG.info("Starting IO Execution")
                        for initiator in self.clients:
                            io_tasks.append(
                                executor.submit(initiator.start_fio, "100%", iodepth=16)
                            )
                        time.sleep(20)  # time sleep for IO to Kick-in

                        LOG.info("Validating IO before failover")
                        validate_io(self.orch, namespaces)

                        LOG.info("Failover started")
                        for gw in fail_gws:
                            if fail_tool == "daemon_redeploy":
                                p.spawn(self.daemon_redeploy, gw)
                            else:
                                p.spawn(self.failover, gw, fail_tool)
                        for result in p:
                            if not isinstance(result, dict):
                                raise Exception("Failover failed")
                            failed_gw = result.pop("failed-gw", None)
                            if not failed_gw:
                                raise Exception("Failover failed")
                        LOG.info("Failover Completed")

                        LOG.info("Validating IO after failover")
                        validate_io(self.orch, namespaces)

                        for gw in fail_gws:
                            LOG.info(
                                f"Getting optimized state for the failed gateway {gw.node.ip_address}"
                            )
                            active = get_optimized_state(
                                self.nvme_service, self.orch, gw.ana_group_id
                            )
                            active_gw = list(active[0])[0]
                            LOG.info(log_json_dump(active_gw))
                            active_gw_obj = [
                                gw
                                for gw in self.gateways
                                if gw.daemon_name in active_gw
                            ][0]
                            LOG.info(
                                f"Active gateway after failover for {gw.node.ip_address} is \
                                {active_gw_obj.node.ip_address}"
                            )
                            namespaces_gw = fetch_namespaces(
                                active_gw_obj, [gw.ana_group_id], get_list=True
                            )
                            ns_list = [ns.get("list") for ns in namespaces_gw]
                            LOG.info(
                                f"Namespaces for failed gateway {gw.node.ip_address} after \
                                    failover are {ns_list}"
                            )
                            LOG.info(
                                f"Validating initiator for the active gateway {active_gw_obj.node.ip_address}"
                            )
                            validate_initiator(self.clients, active_gw_obj, ns_list, gw)

                        # Wait for IO to complete and collect FIO outputs
                        fio_outputs = []
                        if io_tasks:
                            LOG.info("Waiting for completion of IOs.")
                            executor.shutdown(wait=True, cancel_futures=True)

                            for task in io_tasks:
                                try:
                                    fio_outputs.append(task.result())
                                except Exception as e:
                                    LOG.error(f"FIO execution failed: {e}")

                        self.calculate_max_clat_time(fio_outputs)

                    # Fail Back
                    if fail_tool != "daemon_redeploy":
                        with parallel() as p:
                            initiators = self.config["initiators"]
                            io_tasks = []
                            if len(namespaces) >= 1:
                                max_workers = (
                                    len(initiators) * len(namespaces)
                                    if initiators
                                    else len(namespaces)
                                )  # 20 devices + 10 buffer per initiator
                                executor = ThreadPoolExecutor(
                                    max_workers=max_workers,
                                )
                            else:
                                executor = ThreadPoolExecutor()

                            # Start IO Execution
                            LOG.info("Starting IO Execution for failback")
                            for initiator in self.clients:
                                io_tasks.append(
                                    executor.submit(
                                        initiator.start_fio, "100%", iodepth=16
                                    )
                                )
                            time.sleep(20)  # time sleep for IO to Kick-in

                            LOG.info("Validating IO before failback")
                            validate_io(self.orch, namespaces)

                            LOG.info("Failback started")
                            for gw in fail_gws:
                                p.spawn(self.failback, gw, fail_tool)
                            for result in p:
                                if not isinstance(result, dict):
                                    raise Exception("Failback failed")
                                failed_gw = result.pop("failed-gw", None)
                                LOG.info(log_json_dump(result))
                                if not failed_gw:
                                    raise Exception("Failback failed")
                                namespaces_gw = fetch_namespaces(
                                    failed_gw, [failed_gw.ana_group_id], get_list=True
                                )
                                ns_list = [ns.get("list") for ns in namespaces_gw]
                                LOG.info(
                                    f"Namespaces for failed gateway {failed_gw.node.ip_address} after \
                                        failback are {ns_list}"
                                )
                                LOG.info(
                                    f"Active gateway after failback is {failed_gw.node.ip_address}"
                                )
                                validate_initiator(self.clients, failed_gw, ns_list)
                            LOG.info("Failback completed")

                            LOG.info("Validating IO after failback")
                            validate_io(self.orch, namespaces)
                            # Wait for IO to complete and collect FIO outputs
                            fio_outputs = []
                            if io_tasks:
                                LOG.info("Waiting for completion of IOs.")
                                executor.shutdown(wait=True, cancel_futures=True)

                                for task in io_tasks:
                                    try:
                                        fio_outputs.append(task.result())
                                    except Exception as e:
                                        LOG.error(f"FIO execution failed: {e}")

                            self.calculate_max_clat_time(fio_outputs)

                        time.sleep(20)
        except BaseException as err:  # noqa
            raise Exception(err)
