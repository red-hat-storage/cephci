"""
NVMe High Availability Module.
"""

import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph import CommandFailed
from ceph.ceph_admin.daemon import Daemon
from ceph.ceph_admin.host import Host
from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from ceph.utils import get_node_by_id, get_openstack_driver
from ceph.waiter import WaitUntil
from tests.nvmeof.workflows.initiator import NVMeInitiator, validate_initiator
from tests.nvmeof.workflows.nvme import NVMeService
from tests.nvmeof.workflows.nvme_gateway import NVMeGateway
from tests.nvmeof.workflows.nvme_utils import deploy_nvme_service
from tests.nvmeof.workflows.utils import get_current_timestamp, string_to_dict
from utility.log import Log
from utility.retry import retry
from utility.utils import log_json_dump

LOG = Log(__name__)


class HighAvailability(NVMeService):
    def __init__(self, ceph_cluster, gateways, **config):
        """Initialize NVMeoF Gateway High Availability class.

        Args:
            ceph_cluster: Ceph cluster
            gateways: Gateway node Ids
            config: HA config
        """
        super().__init__(config=config, ceph_nodes=gateways, ceph_cluster=ceph_cluster)

        self.orch = Orch(cluster=self.ceph_cluster)
        self.daemon = Daemon(cluster=self.ceph_cluster)
        self.host = Host(cluster=self.ceph_cluster)
        self.clients = []
        self.initiators = {}

        self.ana_ids = [gw.ana_group_id for gw in self.gateways]
        self.fail_ops = {
            "systemctl": self.system_control,
            "daemon": self.ceph_daemon,
            "power_on_off": self.power_on_off,
            "maintanence_mode": self.maintanence_mode,
        }

    def get_or_create_initiator(self, node_id, nqn):
        """Get existing NVMeInitiator or create a new one for each (node_id, nqn)."""
        key = (node_id, nqn)  # Use both as dictionary key

        if key not in self.initiators:
            node = get_node_by_id(self.cluster, node_id)
            self.initiators[key] = NVMeInitiator(node, self.gateways[0], nqn)

        return self.initiators[key]

    def catogorize(self, gws):
        """Categorize to-be failed and running GWs.

        Args:
            gws: gateways to be failed/stopped/scaled-down

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
        states = self.ana_states()

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
                # Condition to fail if multiple Active path exists for a gateway.
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
            "failed-gw": gateway,
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
            "failed-gw": gateway,
        }

    @retry(IOError, tries=3, delay=3)
    def compare_client_namespace(self, uuids):
        lsblk_devs = []
        for client in self.clients:
            lsblk_devs.extend(client.fetch_lsblk_nvme_devices())

        LOG.info(
            f"Expected NVMe Targets : {set(list(uuids))} Vs LSBLK devices: {set(list(lsblk_devs))}"
        )
        if sorted(uuids) != sorted(set(lsblk_devs)):
            raise IOError("Few Namespaces are missing!!!")
        LOG.info("All namespaces are listed at Client(s)")
        return True

    def prepare_io_execution(self, io_clients, return_clients=False):
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
            client = self.get_or_create_initiator(io_client["node"], nqn)
            client.connect_targets(io_client)
            if client not in self.clients:
                self.clients.append(client)
        if return_clients:
            return self.clients

    def fetch_namespaces(self, gateway, failed_ana_grp_ids=[], get_list=False):
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
                        if get_list:
                            namespaces.append(
                                {"list": ns, "info": f"{sub_name}|{ns_info}"}
                            )
                        else:
                            namespaces.append(f"{sub_name}|{ns_info}")
        if not failed_ana_grp_ids:
            LOG.info(f"All namespaces : {log_json_dump(all_ns)}")
            return all_ns

        LOG.info(
            f"Namespaces found for ANA-grp-id [{failed_ana_grp_ids}]: {log_json_dump(namespaces)}"
        )
        return namespaces

    @retry((IOError, TimeoutError, CommandFailed), tries=7, delay=2)
    def validate_io(self, namespaces, negative=False):
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
                time.sleep(6)
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

    def run(self):
        """Execute the HA failover and failback with IO validation."""
        fail_methods = self.config["fault-injection-methods"]
        initiators = self.config["initiators"]
        io_tasks = []

        try:
            # Prepare FIO Execution
            namespaces = self.fetch_namespaces(self.gateways[0])
            if len(namespaces) >= 1:
                max_workers = (
                    len(initiators) * len(namespaces) if initiators else len(namespaces)
                )  # 20 devices + 10 buffer per initiator
                executor = ThreadPoolExecutor(
                    max_workers=max_workers,
                )
            else:
                executor = ThreadPoolExecutor()

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

                    fail_gws, _ = self.catogorize(nodes)
                    fail_gw_ana_ids = []
                    namespaces = []
                    all_failed_ns = {}
                    for gw in fail_gws:
                        fail_gw_ana_ids.append(gw.ana_group_id)
                        namespaces_gw = self.fetch_namespaces(
                            gw, [gw.ana_group_id], get_list=True
                        )
                        ns_list = [ns.get("list") for ns in namespaces_gw]
                        ns_info = [ns.get("info") for ns in namespaces_gw]
                        LOG.info(
                            f"Namespaces for failed gateway {gw.node.ip_address} before failover are {ns_list}"
                        )
                        namespaces.extend(ns_info)
                        all_failed_ns.update({gw.ana_group_id: ns_list})
                        validate_initiator(self.clients, gw, ns_list)

                    self.validate_io(namespaces)

                    # Fail Over
                    with parallel() as p:
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
                                raise Exception("Faileover failed")
                        for gw in fail_gws:
                            active = self.get_optimized_state(gw.ana_group_id)
                            active_gw = list(active[0])[0]
                            LOG.info(log_json_dump(result))
                            active_gw_obj = [
                                gw
                                for gw in self.gateways
                                if gw.daemon_name in active_gw
                            ][0]
                            LOG.info(
                                f"Active gateway after failover for {gw.node.ip_address} is \
                                    {active_gw_obj.node.ip_address}"
                            )
                            namespaces_gw = self.fetch_namespaces(
                                active_gw_obj, [gw.ana_group_id], get_list=True
                            )
                            ns_list = [ns.get("list") for ns in namespaces_gw]
                            LOG.info(
                                f"Namespaces for failed gateway {gw.node.ip_address} after \
                                    failover are {ns_list}"
                            )
                            validate_initiator(self.clients, active_gw_obj, ns_list, gw)
                        self.validate_io(namespaces)

                    # Fail Back
                    if fail_tool != "daemon_redeploy":
                        with parallel() as p:
                            for gw in fail_gws:
                                p.spawn(self.failback, gw, fail_tool)
                            for result in p:
                                if not isinstance(result, dict):
                                    raise Exception("Failback failed")
                                failed_gw = result.pop("failed-gw", None)
                                LOG.info(log_json_dump(result))
                                if not failed_gw:
                                    raise Exception("Failback failed")
                                namespaces_gw = self.fetch_namespaces(
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
                        time.sleep(20)
        except BaseException as err:  # noqa
            raise Exception(err)
        finally:
            if io_tasks:
                LOG.info("Waiting for completion of IOs.")
                executor.shutdown(wait=True, cancel_futures=True)
