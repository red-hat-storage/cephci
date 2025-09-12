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
from tests.io.io_utils import get_max_clat_from_fio_output
from tests.nvmeof.workflows.initiator import NVMeInitiator, validate_initiator
from tests.nvmeof.workflows.nvme_gateway import create_gateway
from tests.nvmeof.workflows.nvme_utils import (
    deploy_nvme_service,
    nvme_gw_cli_version_adapter,
)
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
        self.gw_list = gateways
        self.gateways = []
        self.mtls = config.get("mtls")
        self.gateway_group = config.get("gw_group", "")
        self.orch = Orch(cluster=self.cluster, **{})
        self.daemon = Daemon(cluster=self.cluster, **{})
        self.host = Host(cluster=self.cluster, **{})
        self.nvme_pool = config["rbd_pool"]
        self.clients = []
        self.initiators = {}
        self.ana_ids = []
        self.fail_ops = {
            "systemctl": self.system_control,
            "daemon": self.ceph_daemon,
            "power_on_off": self.power_on_off,
            "maintanence_mode": self.maintanence_mode,
        }

    def initialize_gateways(self):
        """Initialize Gateways."""
        version = nvme_gw_cli_version_adapter(self.cluster)

        for gateway in self.gw_list:
            gw_node = get_node_by_id(self.cluster, gateway)
            self.gateways.append(
                create_gateway(
                    version,
                    gw_node,
                    mtls=self.mtls,
                    shell=getattr(self.orch, "shell"),
                    gw_group=self.gateway_group,
                )
            )
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

    def get_or_create_initiator(self, node_id, nqn):
        """Get existing NVMeInitiator or create a new one for each (node_id, nqn)."""
        key = (node_id, nqn)  # Use both as dictionary key

        if key not in self.initiators:
            node = get_node_by_id(self.cluster, node_id)
            self.initiators[key] = NVMeInitiator(node, nqn)

        return self.initiators[key]

    def create_dhchap_key(self, config, update_host_key=False):
        """Generate DHCHAP key for subsystem (once) and unique keys for each initiator host."""
        subnqn = config["subnqn"]

        for host_config in config["hosts"]:
            node_id = host_config["node"]
            initiator = self.get_or_create_initiator(node_id, subnqn)

            # Case 1: Subsystem key (generate once and reuse for all hosts)
            if not update_host_key:
                if "subsys_key" not in config or config.get(
                    "update_dhchap_key", False
                ):  # only generate once
                    key, _ = initiator.gen_dhchap_key(n=subnqn)
                    config["subsys_key"] = key.strip()
                    LOG.info(
                        f"Generated subsystem key {config['subsys_key']} for {subnqn}"
                    )
                initiator.subsys_key = config["subsys_key"]
                config["dhchap-key"] = config["subsys_key"]  # backward compatibility

            # Case 2: Host key (unique per initiator)
            else:
                key, _ = initiator.gen_dhchap_key(n=initiator.initiator_nqn())
                host_config["dhchap-key"] = key.strip()
                initiator.host_key = key.strip()
                LOG.info(
                    f"Generated host key {host_config['dhchap-key']} for host {node_id}"
                )

            initiator.nqn = subnqn
            initiator.auth_mode = config.get("auth_mode")
            self.clients.append(initiator)

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
        if self.cluster.rhcs_version >= "8":
            out = json.loads(out)
            for gateway in out.get("Created Gateways:"):
                gw = gateway["gw-id"]
                states[gw] = gateway
                states[gw].update(self.string_to_dict(gateway["ana states"]))
        else:
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

    def scale_down(self, gateway_nodes_to_be_scaleddown):
        """Scaling down of the NVMeoF Gateways.

        Initiate scale-down
        - List the gateways which has to be scaled down.
        - Validate the ANA states of scaled down GWs are optimized in one of the other working GWs.

        Post scale-down Validation
        - List out namespaces associated with the scaled down Gateways using ANA group ids.
        - Check for 5 Consecutive times for the increments in write/read to validate IO continuation.
        """
        start_counter = float()
        start_time = str()
        end_counter = float()
        end_time = str()
        LOG.info(f"{gateway_nodes_to_be_scaleddown}: Scaling down NVMe Service")

        if not isinstance(gateway_nodes_to_be_scaleddown, list):
            gateway_nodes_to_be_scaleddown = [gateway_nodes_to_be_scaleddown]

        to_be_scaledown_gws, operational_gws = self.catogorize(
            gateway_nodes_to_be_scaleddown
        )
        ana_ids = [gw.ana_group_id for gw in to_be_scaledown_gws]
        gateway = operational_gws[0]

        # Validate IO and scale operation
        old_namespaces = self.fetch_namespaces(gateway, ana_ids)
        self.validate_io(old_namespaces)

        # Scale down
        gwnodes_to_be_deployed = list(
            set(self.config["gw_nodes"]) - set(gateway_nodes_to_be_scaleddown)
        )
        self.config["gw_nodes"] = gwnodes_to_be_deployed
        deploy_nvme_service(self.cluster, self.config)

        self.gateways = []
        for gateway in self.config["gw_nodes"]:
            gw_node = get_node_by_id(self.cluster, gateway)
            self.gateways.append(
                create_gateway(
                    nvme_gw_cli_version_adapter(self.cluster),
                    gw_node,
                    mtls=self.mtls,
                    shell=getattr(self.orch, "shell"),
                    gw_group=self.gateway_group,
                )
            )

        start_counter, start_time = get_current_timestamp()
        for gateway in to_be_scaledown_gws:
            hostname = gateway.hostname

            if self.cluster.rhcs_version == "8":
                # Wait until 60 seconds
                for w in WaitUntil():
                    # Check for gateway unavailability
                    if self.check_gateway_availability(
                        gateway.ana_group_id, state="DELETING"
                    ):
                        LOG.info(f"[ {gateway} ] NVMeofGW service is UNAVAILABLE.")
                        active = self.get_optimized_state(gateway.ana_group_id)

                        # Find optimized path
                        if active:
                            LOG.info(
                                f"{list(active[0])} is new and only Active GW for failed {hostname}"
                            )
                            break

                    LOG.warning(f"[ {hostname} ] is still in AVAILABLE state..")

                if w.expired:
                    raise TimeoutError(
                        f"[ {hostname} ] Scale down of NVMeofGW service failed after 60s timeout.."
                    )

            end_counter, end_time = get_current_timestamp()
            LOG.info(
                f"[ {hostname} ] Total time taken to scale down - {end_counter - start_counter} seconds"
            )

            result = {
                "scale-down-start-time": start_time,
                "scale-down-end-time": end_time,
                "scale-down-start-counter-time": start_counter,
                "scale-down-end-counter-time": end_counter,
            }
            LOG.info(log_json_dump(result))

            # Validate auto load balance if rhcs version is 8.1
            if self.cluster.rhcs_version == "8.1":
                time.sleep(60)
                validate_ns_balance = self.validate_auto_loadbalance()
                LOG.info(f"Validated namespaces in each GW:{validate_ns_balance}")
            # Validate IO post scale down
            self.validate_io(set(list(old_namespaces)))
            return result

    def validate_scaleup(self, scaleup_nodes, namespaces):
        """
        - List out namespaces associated with the new Gateways using ANA group ids.
        - Check for 5 Consecutive times for the increments in write/read to validate IO continuation.

        Args:
            scaleup_nodes (list): A list of gateway nodes to be scaled up.
        """
        start_counter = float()
        start_time = str()
        end_counter = float()
        end_time = str()
        new_gws = []

        for gateway_node in scaleup_nodes:
            gw = get_node_by_id(self.cluster, gateway_node)
            new_gws.append(
                create_gateway(
                    nvme_gw_cli_version_adapter(self.cluster),
                    gw,
                    mtls=self.mtls,
                    shell=getattr(self.orch, "shell"),
                    gw_group=self.gateway_group,
                )
            )

        start_counter, start_time = get_current_timestamp()
        for gateway in new_gws:
            hostname = gateway.hostname

            # Wait until 60 seconds
            for w in WaitUntil(timeout=60):
                # Check for gateway availability
                if self.check_gateway_availability(gateway.ana_group_id):
                    LOG.info(f"[ {gateway} ] NVMeofGW service is AVAILABLE.")
                    state = self.get_optimized_state(gateway.ana_group_id)

                    # check gateway for its own original path.
                    if gateway.ana_group["name"] in state[0]:
                        end_counter, end_time = get_current_timestamp()
                        LOG.info(
                            f"{hostname} restored to original path - {log_json_dump(state)}"
                        )
                        break

                LOG.warning(f"[ {hostname} ] is still not in AVAILABLE state..")

            if w.expired:
                raise TimeoutError(
                    f"[ {hostname} ] Scale up of NVMeofGW service failed after 120s timeout.."
                )

            LOG.info(
                f"[ {hostname} ] Total time taken to scale up - {end_counter - start_counter} seconds"
            )
            result = {
                "scale-up-start-time": start_time,
                "scale-up-end-time": end_time,
                "scale-up-start-counter-time": start_counter,
                "scale-up-end-counter-time": end_counter,
            }
            LOG.info(log_json_dump(result))
            # Validate auto load balance if rhcs version is 8.1
            if self.cluster.rhcs_version == "8.1":
                time.sleep(60)
                validate_ns_balance = self.validate_auto_loadbalance()
                LOG.info(f"Validated namespaces in each GW:{validate_ns_balance}")
            # Validate IO post scale up
            self.validate_io(set(list(namespaces)))
            return result

    def scale_up(self, scaleup_nodes, gw_nodes, existing_namespaces):
        """Scaling up of the NVMeoF Gateways.

        Initiate scale-up
        - Spin up the new gateways.
        - Validate the ANA states of new GWs are optimized.

        Pre scale-up Validation
        - List out namespaces associated with the new Gateways using ANA group ids.
        - Check for 5 Consecutive times for the increments in write/read to validate IO continuation.

        Post scale-up Validation
        - Check if Ana group ids of replaced GWs took over the original ANA group ids
        """
        existing_namespaces = []
        LOG.info(f"{scaleup_nodes}: Scaling up NVMe Service")

        if not isinstance(scaleup_nodes, list):
            scaleup_nodes = [scaleup_nodes]

        # Validate IO before scale up operation
        self.validate_io(set(list(existing_namespaces)))

        # Scale up
        gwnodes_to_be_deployed = list(set(self.config["gw_nodes"] + scaleup_nodes))
        self.config["gw_nodes"] = gwnodes_to_be_deployed
        deploy_nvme_service(self.cluster, self.config)

        self.gateways = []
        for gateway in gwnodes_to_be_deployed:
            gw_node = get_node_by_id(self.cluster, gateway)
            self.gateways.append(
                create_gateway(
                    nvme_gw_cli_version_adapter(self.cluster),
                    gw_node,
                    mtls=self.mtls,
                    shell=getattr(self.orch, "shell"),
                    gw_group=self.gateway_group,
                )
            )

        # Validate ana_grp_ids post scale up
        for scaleup_node in scaleup_nodes:
            for gw_node in gw_nodes:
                if gw_node.node.id == scaleup_node:
                    gw = self.check_gateway(gw_node.node.id)
                    scaleup_gw = self.check_gateway(scaleup_node)
                    if gw.ana_group_id == scaleup_gw.ana_group_id:
                        LOG.info("Scaleup nodes took over the previous anagrpids")
                    else:
                        raise Exception("anagrpids are not matching after scaleup")

    def validate_auto_loadbalance(self, gw_group=""):
        """
        Fetch the namespace count on each Gateway and compare them.
        Ensure that the number of namespaces for each GW is within the range [num_namespaces_per_gw + or - len(GWs)].
        """
        out, _ = self.orch.shell(
            args=["ceph", "nvme-gw", "show", self.nvme_pool, repr(self.gateway_group)]
        )
        out = json.loads(out)
        total_num_namespaces = out.get("num-namespaces")
        gateways = out.get("Created Gateways:", [])
        total_gateways = len(gateways)
        if total_gateways == 0:
            raise Exception("No gateways found in the output.")

        num_namespaces_per_gw = total_num_namespaces / total_gateways
        namespaces = {}
        LOG.info(f"Total namespace in GW group : {total_num_namespaces}")
        LOG.info(f"Total GWs: {total_gateways}")
        LOG.info(f"Namespaces per GW : {num_namespaces_per_gw}")

        for gateway in gateways:
            gw_id = gateway["gw-id"]
            num_namespaces = gateway["num-namespaces"]
            lower_range = num_namespaces_per_gw - total_gateways
            upper_range = num_namespaces_per_gw + total_gateways
            LOG.info(
                f"namespace per GW must be in range between {lower_range} and {upper_range}"
            )

            if not (lower_range <= num_namespaces <= upper_range):
                raise Exception(
                    f"Gateway '{gw_id}' has an invalid num-namespaces: {num_namespaces}. "
                    f"It must be between {lower_range} and {upper_range}."
                )

            namespaces[gw_id] = gateway
            namespaces[gw_id]["num-namespaces"] = num_namespaces

        return namespaces

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
                if self.check_gateway_availability(
                    gateway.ana_group_id, state="UNAVAILABLE"
                ):
                    LOG.info(f"[ {hostname} ] NVMeofGW service is UNAVAILABLE.")
                    active = self.get_optimized_state(gateway.ana_group_id)

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
                if self.check_gateway_availability(gateway.ana_group_id):
                    LOG.info(f"[ {hostname} ] NVMeofGW service is AVAILABLE.")

                    active = self.get_optimized_state(gateway.ana_group_id)
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
            client.connect_targets(self.gateways[0], io_client)
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
        subsystems, _ = gateway.subsystem.list(**args)
        subsystems = json.loads(subsystems)

        namespaces = []
        all_ns = []
        for subsystem in subsystems["subsystems"]:
            sub_name = subsystem["nqn"]
            cmd_args = {"args": {"subsystem": subsystem["nqn"]}}
            nspaces, _ = gateway.namespace.list(**{**args, **cmd_args})
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

            out, _ = initiator_node.exec_command(
                sudo=True, cmd="cat /etc/os-release | grep VERSION_ID"
            )
            rhel_version = out.split("=")[1].strip().strip('"')

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
                if rhel_version == "9.5":
                    key = device["NameSpace"]
                    value = int(device["SerialNumber"])
                    serial_to_namespace[key].add(value)
                elif rhel_version == "9.6":
                    for subsys in device.get("Subsystems", []):
                        for controller in subsys.get("Controllers", []):
                            if controller.get("ModelNumber") == "Ceph bdev Controller":
                                serial = controller.get("SerialNumber", "")
                                value = int(serial)
                                for ns in subsys.get("Namespaces", []):
                                    key = ns.get("NSID")
                                    serial_to_namespace[key].add(value)

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
                    devices_json_empty = (
                        all(
                            not subsys.get("Namespaces")  # True if empty or missing
                            for device in devices_json
                            for subsys in device.get("Subsystems", [])
                        )
                        if rhel_version == "9.6"
                        else not devices_json
                    )
                    if not devices_json_empty:  # Check if Devices is not empty
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
                    devices_json_empty = (
                        all(
                            not subsys.get("Namespaces")  # True if empty or missing
                            for device in devices_json
                            for subsys in device.get("Subsystems", [])
                        )
                        if rhel_version == "9.6"
                        else not devices_json
                    )
                    if devices_json_empty:
                        LOG.error(
                            f"Expected devices to be visible for node {node}, but found none."
                        )
                        raise Exception(
                            f"Initiator {node} has no devices when NS visibility is restricted"
                        )
                    else:
                        # Log Namespace and SerialNumber from each device
                        for device in devices_json:
                            if rhel_version == "9.5":
                                namespace = device.get("NameSpace", None)
                                serial_number = device.get("SerialNumber", None)
                            elif rhel_version == "9.6":
                                for subsys in device.get("Subsystems", []):
                                    for controller in subsys.get("Controllers", []):
                                        if (
                                            controller.get("ModelNumber")
                                            == "Ceph bdev Controller"
                                        ):
                                            serial_number = int(
                                                controller.get("SerialNumber", "")
                                            )
                                            for ns in subsys.get("Namespaces", []):
                                                namespace = ns.get("NSID")
                                                LOG.info(
                                                    f"Namespace: {namespace}, SerialNumber: {serial_number}"
                                                )
                        LOG.info(
                            f"Validated - {len(devices_json)} devices found on {node}"
                        )

    def validate_namespace_masking(
        self,
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

    def calculate_max_clat_time(self, fio_outputs):
        # Extract failover time
        # TODO: Current approach of extracting CLAT using regex will not work with different FIO
        #  workload and clat duration calculation is invalid which needs modification using right approach.
        #  Instead of CLI repsonse, modify the fio command to provide output json file
        #  to parse and calculate the Completion latency.
        for idx, output in enumerate(fio_outputs):
            try:
                max_clat_in_ms = get_max_clat_from_fio_output(output[0][0])
                max_clat_in_sec = max_clat_in_ms / 1000
                LOG.info(f"Failover time for {max_clat_in_sec} ms")
            except Exception as e:
                LOG.error(f"Failed to parse FIO output: {e}")

    def run(self):
        """Execute the HA failover and failback with IO validation."""
        fail_methods = self.config["fault-injection-methods"]
        initiators = self.config["initiators"]

        try:
            # Prepare FIO Execution
            namespaces = self.fetch_namespaces(self.gateways[0])

            self.prepare_io_execution(initiators)

            # Check for targets at clients
            self.compare_client_namespace([i["uuid"] for i in namespaces])

            repeat_ha_count = self.config.get("repeat_ha_count", 1)

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
                        for initiator in self.clients:
                            io_tasks.append(
                                executor.submit(initiator.start_fio, "100%")
                            )
                        time.sleep(20)  # time sleep for IO to Kick-in

                        self.validate_io(namespaces)

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

                        self.validate_io(namespaces)

                        for gw in fail_gws:
                            active = self.get_optimized_state(gw.ana_group_id)
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
                            namespaces_gw = self.fetch_namespaces(
                                active_gw_obj, [gw.ana_group_id], get_list=True
                            )
                            ns_list = [ns.get("list") for ns in namespaces_gw]
                            LOG.info(
                                f"Namespaces for failed gateway {gw.node.ip_address} after \
                                    failover are {ns_list}"
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
                            for initiator in self.clients:
                                io_tasks.append(
                                    executor.submit(initiator.start_fio, "100%")
                                )
                            time.sleep(20)  # time sleep for IO to Kick-in

                            self.validate_io(namespaces)

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
                            LOG.info("Failback completed started")

                            self.validate_io(namespaces)
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
