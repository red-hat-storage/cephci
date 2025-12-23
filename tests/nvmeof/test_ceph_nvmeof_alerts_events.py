"""
Test suite that verifies NVMe alerts and NVMe Health checks.
"""

from copy import deepcopy
from json import dumps, loads
from random import choice
from urllib.parse import urljoin, urlparse

import requests

from ceph.ceph import Ceph
from ceph.ceph_admin.common import fetch_method
from ceph.waiter import WaitUntil
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.ha import HighAvailability, get_node_by_id
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    delete_nvme_service,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.retry import retry
from utility.utils import generate_unique_id

LOG = Log(__name__)

rbd_obj = None
nvme_service = None
cleanup = False


class NVMeAlertFailure(Exception):
    pass


class PrometheusAlerts:
    """
    Interface to interact with the Prometheus instance integrated with the Ceph dashboard.
    """

    def __init__(self, orch):
        """
        Initialize the Prometheus wrapper.

        Args:
            orch: Orchestrator object that interfaces with the Ceph cluster.
        """
        self.orch = orch
        self._baseurl = self._fetch_prometheus_endpoint()

    @property
    def baseurl(self):
        """Base URL of the Prometheus instance."""
        return self._baseurl

    @baseurl.setter
    def baseurl(self, _):
        """
        Refresh the base Prometheus URL.
        This setter discards any input and fetches the endpoint again.
        """
        self._baseurl = self._fetch_prometheus_endpoint()

    def _fetch_prometheus_endpoint(self):
        """
        Fetch the Prometheus endpoint URL from the Ceph dashboard configuration.

        Returns:
            str: The full URL to the Prometheus server.
        """
        url, _ = self.orch.shell(args=["ceph dashboard get-prometheus-api-host"])
        url = url.strip()
        hostname = urlparse(url).hostname
        server = get_node_by_id(self.orch.cluster, hostname)
        return url.replace(hostname, server.ip_address)

    def fetch_prometheus_alerts(self):
        """
        Fetch all alerts from Prometheus.

        Returns:
            dict or None: The JSON response from Prometheus containing alert rules, or None on failure.
        """
        uri = "api/v1/rules?type=alert"
        try:
            response = requests.get(urljoin(self.baseurl, uri), timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            LOG.error(f"[ERROR] Failed to fetch NVMe-oF alerts: {e}")
            return {}

    def get_nvme_alerts(self):
        """
        Fetch all NVMeoF related alerts from Prometheus.

        Returns:
            dict or None: The JSON response from Prometheus containing alert rules, or None on failure.
        """
        alerts = self.fetch_prometheus_alerts()
        for alert in alerts["data"]["groups"]:
            if alert["name"] != "nvmeof":
                continue
            return alert
        return False

    def get_nvme_alert_by_name(self, alert_name):
        """Get a specific NVMe alert by its name.

        Args:
            alert_name: NVMeoF alert name
        """
        for alert in self.get_nvme_alerts()["rules"]:
            if alert.get("name") == alert_name:
                return alert
        raise Exception(f"[ {alert_name} ] alert not found.")

    def calculate_timeout_window(self, timeout: int, interval: int):
        """Calculate Time duration based on the interval.

        Here interval remains same as it is provided by the user,
        but timeout should be added with tolerance of interval.

        for example,
            timeout = 60 s, interval = 10s
            then, timeout with tolerance level = 70s
            so alert should fire in this range, 60s < fire < 70

        Note: inetrval is the key here to calculate the alerting window
        """
        return timeout + (3 * interval)

    def monitor_alert(
        self, alert_name, state="firing", msg="", timeout=60, interval=10
    ):
        """Monitor NVMe alert."""
        _alert = str()
        timeout = self.calculate_timeout_window(timeout, interval)
        for w in WaitUntil(timeout=timeout, interval=interval):
            _alert = self.get_nvme_alert_by_name(alert_name)
            if _alert["state"] == state:
                LOG.info(
                    f"[ {alert_name} ] is in Expected {state} state  - \n{dumps(_alert)}"
                )
                if msg:
                    for alrt in _alert["alerts"]:
                        if alrt["annotations"]["summary"] == msg:
                            LOG.info(f"[ {alert_name} ] alert message found {msg}")
                            return _alert
                    LOG.warning(f"[ {alert_name} ] msg not found - ( EXPECTED: {msg} )")
                return _alert
            LOG.warning(f"[ {alert_name} ] is not in expected {state} state ")
        if w.expired:
            raise NVMeAlertFailure(
                f"[ {alert_name} ] - ( Expected: {state} )\n{dumps(_alert)}"
            )


# Special case for adding listeners to the gateway
def configure_listeners(ha_obj, nodes, config, action="add"):
    """Configure Listeners on subsystem.

    Args:
        ha_obj: HA object
        nodes: List of GW nodes
        config: listener config
        action: listener add, del
    """
    lb_group_ids = {}
    for node in nodes:
        nvmegwcli = ha_obj.check_gateway(node)
        hostname = nvmegwcli.fetch_gateway_hostname()
        listener_config = {
            "args": {
                "subsystem": config["nqn"],
                "traddr": nvmegwcli.node.ip_address,
                "trsvcid": config["listener_port"],
                "host-name": hostname,
            }
        }
        method = fetch_method(nvmegwcli.listener, action)
        method(**listener_config)
        lb_group_ids.update({hostname: nvmegwcli.ana_group_id})
    return lb_group_ids


def test_ceph_83611097(ceph_cluster, config):
    """[CEPH-83611097] NVMeoFNVMeoFMissingListener alert.

    NVMeoFMissingListener alert helps user to identify the missing listener
      which is important for HA. where initiators could get connected to
      multipath data paths, ensuring IO is uninterupted when GW(s) fails.

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    time_to_fire = config.get("time_to_fire", 60)
    rbd_obj = config["rbd_obj"]
    interval = config.get("interval", 50)
    alert = "NVMeoFMissingListener"
    msg = "No listener added for {GW} NVMe-oF Gateway to {NQN} subsystem"

    # Deploy Services
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()

    # Initialize High Availability
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()

    # Create RBD image and multiple NS with that image.
    nvmegwcli = ha.gateways[0]
    events = PrometheusAlerts(ha.orch)
    sub1_args = {"subsystem": f"nqn.2016-06.io.spdk:cnode{generate_unique_id(4)}"}
    nvmegwcli.subsystem.add(**{"args": {**sub1_args, **{"no-group-append": True}}})
    LOG.info(f"{alert} should be INACTIVE.")
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        state="inactive",
        interval=interval,
    )

    # Add a single listener and NVMeoFNVMeoFMissingListener alert should be firing
    gw1, gw2 = ha.gateways
    listener_args = {"nqn": sub1_args["subsystem"], "listener_port": 4420}
    configure_listeners(ha, [gw1.node.id], listener_args)
    LOG.info(f"{alert} should be FIRING GW as listener(s) are missing.")
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        interval=interval,
        msg=msg.format(NQN=sub1_args["subsystem"], GW=gw2.node.hostname),
    )

    # Add all listener and alert should be inactive
    configure_listeners(ha, [gw2.node.id], listener_args)
    LOG.info(f"{alert} should be INACTIVE.")
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        state="inactive",
        interval=interval,
    )

    # Delete a listener and alert should be firing
    configure_listeners(ha, [gw1.node.id], listener_args, action="delete")
    LOG.info(f"{alert} should be FIRING GW as listener(s) are missing.")
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        interval=interval,
        msg=msg.format(NQN=sub1_args["subsystem"], GW=gw1.node.hostname),
    )

    # Add back GW2 listener and alert should be inactive
    configure_listeners(ha, [gw1.node.id], listener_args)
    LOG.info(f"{alert} should be INACTIVE.")
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )

    # Add another subsystem and with single listener
    sub2_args = {"subsystem": f"nqn.2016-06.io.spdk:cnode{generate_unique_id(4)}"}
    listener_args = {"nqn": sub2_args["subsystem"], "listener_port": 4420}
    nvmegwcli.subsystem.add(**{"args": {**sub2_args, **{"no-group-append": True}}})
    configure_listeners(ha, [gw1.node.id], listener_args)
    LOG.info(f"{alert} should be FIRING GW listener(s) are missing.")
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        interval=interval,
        msg=msg.format(NQN=sub2_args["subsystem"], GW=gw2.node.hostname),
    )

    # Add all listener and alert should be inactive
    configure_listeners(ha, [gw2.node.id], listener_args)
    LOG.info(f"{alert} should be INACTIVE.")
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )
    if config.get("cleanup"):
        global cleanup
        LOG.info("Cleaning up NVMeoF services and RBD pool for CEPH-83611097.")
        teardown(nvme_service, rbd_obj)
        cleanup = True
    LOG.info(f"CEPH-83611097 - {alert} alert validated successfully.")


def test_ceph_83610950(ceph_cluster, config):
    """[CEPH-83610950] NVMeoF Multiple Namespaces Of RBDImage alert.

    NVMeoFMultipleNamespacesOfRBDImage alerts the user if a RBD image
    is used for multiple namespaces. This is important alerts for cases
    where namespaces are created on same image for different gateway group.

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    rbd_pool = config["rbd_pool"]
    rbd_obj = config["rbd_obj"]
    time_to_fire = config.get("time_to_fire", 60)
    interval = config.get("interval", 50)
    alert = "NVMeoFMultipleNamespacesOfRBDImage"
    msg = "RBD image {image} cannot be reused for multiple NVMeoF namespace"
    svcs = []
    services = []

    # Deploy Services
    for svc in config["gw_groups"]:
        svc.update({"rbd_pool": rbd_pool})
        service = NVMeService(svc, ceph_cluster)
        service.deploy()
        service.init_gateways()
        services.append(service)
        ha = HighAvailability(ceph_cluster, svc["gw_nodes"], **svc)
        ha.initialize_gateways()
        svcs.append(ha)

    # Create RBD image and multiple NS with that image.
    ha1, ha2 = svcs
    nvmegwcl1 = ha1.gateways[0]
    sub1_args = {"subsystem": f"nqn.2016-06.io.spdk:cnode{generate_unique_id(4)}"}
    nvmegwcl1.subsystem.add(**{"args": {**sub1_args, **{"no-group-append": True}}})

    nvmegwcl2 = ha2.gateways[0]
    sub2_args = {"subsystem": f"nqn.2016-06.io.spdk:cnode{generate_unique_id(4)}"}
    nvmegwcl2.subsystem.add(**{"args": {**sub2_args, **{"no-group-append": True}}})

    image = f"image-{generate_unique_id(4)}"
    rbd_obj.create_image(rbd_pool, image, "10G")

    # Removing nsid because we have https://tracker.ceph.com/issues/72681
    # img_args = {"rbd-pool": _rbd_pool, "rbd-image": image, "nsid": 1}
    img_args = {"rbd-pool": rbd_pool, "rbd-image": image, "force": ""}
    nvmegwcl1.namespace.add(**{"args": {**sub1_args, **img_args}})
    nvmegwcl2.namespace.add(**{"args": {**sub2_args, **img_args}})
    events = PrometheusAlerts(ha1.orch)

    # Two namespaces created for single RBD image,
    # NVMeoFMultipleNamespacesOfRBDImage prometheus alert should be firing
    LOG.info(
        f"{alert} should be FIRING Since multiple namespaces are created for Single RBD image."
    )
    events.monitor_alert(
        alert, timeout=time_to_fire, interval=interval, msg=msg.format(image=image)
    )
    nvmegwcl2.namespace.delete(**{"args": {**sub2_args, **{"nsid": 1}}})

    # Alert should be inactive
    LOG.info(f"{alert} should be INACTIVE.")
    events.monitor_alert(
        alert, timeout=time_to_fire, interval=interval, state="inactive"
    )

    sub2_args = {"subsystem": f"nqn.2016-06.io.spdk:cnode{generate_unique_id(4)}"}
    nvmegwcl2.subsystem.add(**{"args": {**sub2_args, **{"no-group-append": True}}})
    nvmegwcl2.namespace.add(**{"args": {**sub2_args, **img_args}})

    # Alert should be active again, since image is used in new subsystem
    LOG.info(
        f"{alert} should be FIRING Since multiple namespaces are created for Single RBD image."
    )
    events.monitor_alert(
        alert, timeout=time_to_fire, interval=interval, msg=msg.format(image=image)
    )
    nvmegwcl1.namespace.delete(**{"args": {**sub1_args, **{"nsid": 1}}})

    # Namespace removed , alert should be inactive
    LOG.info(f"{alert} should be INACTIVE.")
    events.monitor_alert(
        alert, timeout=time_to_fire, interval=interval, state="inactive"
    )
    if config.get("cleanup"):
        LOG.info("Cleaning up NVMeoF services and RBD pool for CEPH-83610950.")
        for service in services:
            service.delete_nvme_service()
            rbd_obj.clean_up(pools=[rbd_pool])
    global cleanup
    cleanup = True

    LOG.info(f"CEPH-83610950 - {alert} alert validated successfully.")


def test_ceph_83610948(ceph_cluster, config):
    """[CEPH-83610948] Raise a healthcheck when a gateway is in UNAVAILABLE state.

    This test case validates the warning which would be get hit on any gateway unavailability
    and Raise a healthcheck warning under ceph status and ceph health.

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    rbd_obj = config["rbd_obj"]
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()

    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()

    configure_gw_entities(nvme_service, rbd_obj=rbd_obj)

    # Validate the Health warning states
    @retry(ValueError, tries=6, delay=5)
    def check_warning_status(gw, alert=True):
        health, _ = ha.orch.shell(args=["ceph", "health", "detail", "--format", "json"])
        health = loads(health)

        if not alert:
            if "NVMEOF_GATEWAY_DOWN" not in health["checks"]:
                LOG.info(
                    f"NVMEOF_GATEWAY_DOWN health check warning alerted disappeared - {health}."
                )
                if ha.check_gateway_availability(gw.ana_group_id, state="AVAILABLE"):
                    return True
            raise ValueError(f"NVMEOF_GATEWAY_DOWN warning still appears!!! - {health}")

        if alert and "NVMEOF_GATEWAY_DOWN" in health["checks"]:
            nvme_gw_down = health["checks"]["NVMEOF_GATEWAY_DOWN"]
            if HEALTH_CHECK_WARN == nvme_gw_down:
                LOG.info(
                    f"NVMEOF_GATEWAY_DOWN health check warning alerted SUCCESSFULLY - {nvme_gw_down}."
                )
                if ha.check_gateway_availability(gw.ana_group_id, state="UNAVAILABLE"):
                    return True
            raise ValueError(
                f"NVMEOF_GATEWAY_DOWN is alerted, but different messages - {nvme_gw_down}"
            )
        raise ValueError(f"NVMEOF_GATEWAY_DOWN health warning not alerted - {health}.")

    # Fail a Gateway
    fail_gw = choice(ha.gateways)
    HEALTH_CHECK_WARN = {
        "severity": "HEALTH_WARN",
        "summary": {
            "message": "1 gateway(s) are in unavailable state; gateway might be down, try to redeploy.",
            "count": 1,
        },
        "detail": [
            {
                "message": f"NVMeoF Gateway 'client.{fail_gw.daemon_name}' is unavailable."
            },
        ],
        "muted": False,
    }
    if not ha.system_control(fail_gw, action="stop", wait_for_active_state=False):
        raise Exception(f"[ {fail_gw.daemon_name} ] GW Stop failed..")
    check_warning_status(fail_gw)

    # Start GW back and alert should not be visible
    if not ha.system_control(fail_gw, action="start"):
        raise Exception(f"[ {fail_gw.daemon_name} ] GW Start failed..")
    check_warning_status(fail_gw, alert=False)

    if config.get("cleanup"):
        LOG.info("Cleaning up NVMeoF services and RBD pool for CEPH-83610948.")
        teardown(nvme_service, rbd_obj)
        global cleanup
        cleanup = True
    LOG.info(
        "CEPH-83610948 - GW Unavailability healthcheck warning validated successfully."
    )


def test_ceph_83611098(ceph_cluster, config):
    """[CEPH-83611098] Warning at Ceph cluster on zero listener in a subsystem.

    NVMeoFZeroListenerSubsystem alert helps user to add listeners to the subsystem,
    otherwise it would be not visible and able to connect from initators.

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    rbd_obj = config["rbd_obj"]
    time_to_fire = config.get("time_to_fire", 60)
    interval = config.get("interval", 60)
    alert = "NVMeoFZeroListenerSubsystem"
    msg = "No listeners added to {subsystem_name} subsystem"
    gateway_nodes = deepcopy(config.get("gw_nodes"))

    LOG.info("Deploy nvme service")
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()

    # Configure subsystems
    nvmegwcl1 = nvme_service.gateways[0]
    subsystem1 = f"nqn.2016-06.io.spdk:cnode{generate_unique_id(4)}"
    sub1_args = {"subsystem": subsystem1}
    config.update({"nqn": subsystem1})
    nvmegwcl1.subsystem.add(**{"args": {**sub1_args, **{"no-group-append": True}}})

    # Check for alert
    # NVMeoFZeroListenerSubsystem prometheus alert should be firing
    LOG.info(
        "NVMeoFZeroListenerSubsystem should be firing because no listener is configured"
    )
    events = PrometheusAlerts(ha.orch)
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(subsystem_name=subsystem1),
        interval=interval,
    )

    # Add the listener and check alert is inactive state or not
    LOG.info("Add the listener")
    configure_listeners(ha, [gateway_nodes[0]], config)
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )

    # Delete the listener and alert should be in firing state
    LOG.info("Delete the listener")
    listener_args = {"nqn": subsystem1, "listener_port": config["listener_port"]}
    configure_listeners(ha, [gateway_nodes[0]], listener_args, action="delete")
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(subsystem_name=subsystem1),
        interval=interval,
    )

    # Add the listener and check alert is inactive state or not
    LOG.info("Add the listener back")
    configure_listeners(ha, [gateway_nodes[0]], config)
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )

    # Add one more subsystem and ceph prometheus alert to be in firing state
    LOG.info("Add one more subsystem")
    subsystem2 = f"nqn.2016-06.io.spdk:cnode{generate_unique_id(4)}"
    sub2_args = {"subsystem": subsystem2}
    nvmegwcl1.subsystem.add(**{"args": {**sub2_args, **{"no-group-append": True}}})
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(subsystem_name=subsystem2),
        interval=interval,
    )

    if config.get("cleanup"):
        LOG.info("Cleaning up NVMeoF services and RBD pool for CEPH-83611098.")
        teardown(nvme_service, rbd_obj)
        global cleanup
        cleanup = True

    LOG.info("CEPH-83611098 - {alert} alert validated successfully.")


def test_ceph_83611099(ceph_cluster, config):
    """[CEPH-83611099] Warning message or Status on NVMe Service with single Gateway Node.

    NVMeoFSingleGateway alert indicates to user to add more gateways
    to the service in order to have High Availability.

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    time_to_fire = config.get("time_to_fire", 60)
    rbd_obj = config["rbd_obj"]
    interval = config.get("interval", 60)
    alert = "NVMeoFSingleGateway"
    msg = "The gateway group {gw_group} consists of a single gateway - HA is not possible on cluster"

    # Deploy nvmeof service
    LOG.info("deploy nvme service")
    gateway_nodes = deepcopy(config.get("gw_nodes"))
    random_gateway = choice(gateway_nodes)
    config.update({"gw_nodes": [random_gateway]})
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()

    # Check for alert
    # NVMeoFSingleGateway prometheus alert should be firing
    events = PrometheusAlerts(ha.orch)

    LOG.info("Check NVMeoFSingleGateway should be firing")
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(gw_group=config["gw_group"]),
        interval=interval,
    )

    # Add one more gateway and NVMeoFSingleGateway alert should be in inactive state
    LOG.info("Add one more gateway and NVMeoFSingleGateway should be in inactive state")
    config.update({"gw_nodes": gateway_nodes})
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )

    # scale down to one gateway and NVMeoFSingleGateway alert should be in firing state
    LOG.info(
        "scale down to one gateway and NVMeoFSingleGateway should be in firing state"
    )
    random_gateway = choice(gateway_nodes)
    config.update({"gw_nodes": [random_gateway]})
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    LOG.info(
        "scaled down to one gateway, NVMeoFSingleGateway should be in firing state"
    )
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(gw_group=config["gw_group"]),
        interval=interval,
    )

    # Scale up to 2 gateways and alert should be in inactive state
    LOG.info("Add one more gateway and NVMeoFSingleGateway should be in inactive state")
    config.update({"gw_nodes": gateway_nodes})
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )

    if config.get("cleanup"):
        teardown(nvme_service, rbd_obj)
        global cleanup
        cleanup = True

    LOG.info("CEPH-83611099 - NVMeoFSingleGateway validated successfully.")


def test_ceph_83611306(ceph_cluster, config):
    """[CEPH-83611306] Raise a Healthcheck warning for gateway in DELETING state

    This test case validates the warning which would be get hit on any gateway in deleting state
    and Raise a healthcheck warning under ceph status and ceph health.

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    rbd_obj = config["rbd_obj"]
    LOG.info("Deploy NVME service")
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()

    configure_gw_entities(nvme_service, rbd_obj=rbd_obj)

    # Validate the Health warning states
    @retry(ValueError, tries=10, delay=60)
    def check_warning_status(alert=True):
        health, _ = ha.orch.shell(args=["ceph", "health", "detail", "--format", "json"])
        health = loads(health)

        if not alert:
            if "NVMEOF_GATEWAY_DELETING" not in health["checks"]:
                LOG.info(
                    f"NVMEOF_GATEWAY_DELETING health check warning alerted disappeared - {health}."
                )
                return True
            raise ValueError(
                f"NVMEOF_GATEWAY_DELETING warning still appears!!! - {health}"
            )

        if alert and "NVMEOF_GATEWAY_DELETING" in health["checks"]:
            nvme_gw_deleting = health["checks"]["NVMEOF_GATEWAY_DELETING"]
            if HEALTH_CHECK_WARN == nvme_gw_deleting:
                LOG.info(
                    f"NVMEOF_GATEWAY_DELETING health check warning alerted SUCCESSFULLY - {nvme_gw_deleting}."
                )
                return True
            raise ValueError(
                f"NVMEOF_GATEWAY_DELETING is alerted, but different messages - {nvme_gw_deleting}"
            )
        raise ValueError(
            f"NVMEOF_GATEWAY_DELETING health warning not alerted - {health}."
        )

    # This test case can be tested by reducing the below parameter
    # ceph config set mon mon_nvmeofgw_delete_grace 5
    LOG.info("Set mon_nvmeofgw_delete_grace to 5 seconds")
    output, _ = ha.orch.shell(
        args=["ceph", "config", "set", "mon", "mon_nvmeofgw_delete_grace", "5"]
    )
    LOG.info("Check mon_nvmeofgw_delete_grace is set to 5 seconds")
    get_val, _ = ha.orch.shell(
        args=["ceph", "config", "get", "mon", "mon_nvmeofgw_delete_grace"]
    )
    if int(get_val.strip()) != 5:
        raise Exception("Failed to set mon_nvmeofgw_delete_grace value to 5 seconds..")
    # Scale down
    LOG.info("Scale down and check for NVMEOF_GATEWAY_DELETING warn message")
    fail_gw = ha.gateways[0]
    deleting_gw_daemon = ha.gateways[1].daemon_name
    config.update({"gw_nodes": [fail_gw.node.id]})
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    HEALTH_CHECK_WARN = {
        "severity": "HEALTH_WARN",
        "summary": {
            "message": "1 gateway(s) are in deleting state; namespaces are automatically "
            "balanced across remaining gateways, this should take a few minutes.",
            "count": 1,
        },
        "detail": [
            {
                "message": f"NVMeoF Gateway 'client.{deleting_gw_daemon}' is in deleting state."
            }
        ],
        "muted": False,
    }

    LOG.info("check for NVMEOF_GATEWAY_DELETING message")
    # Check the warning status
    check_warning_status(alert=True)
    # Once autoload balancing happens, automatically warning should disappear
    LOG.info(
        "Once autoload balancing completes, NVMEOF_GATEWAY_DELETING should disappeared so check the same"
    )
    check_warning_status(alert=False)

    if config.get("cleanup"):
        teardown(nvme_service, rbd_obj)
        global cleanup
        cleanup = True
    LOG.info(
        "CEPH-83611306 - Healthcheck warning for gateway in DELETING state validated successfully"
    )


def test_CEPH_83616917(ceph_cluster, config):
    """[CEPH-83616917] - Warning at maximum number of namespaces reached at subsystem.

    NVMeoFSubsystemNamespaceLimit alert indicates to user to maximum number of
    namespaces reached at subsystem

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    time_to_fire = config.get("time_to_fire", 60)
    interval = config.get("interval", 60)
    rbd_pool = config["rbd_pool"]
    rbd_obj = config["rbd_obj"]
    nqn_name = config["subsystems"][0]["nqn"]
    alert = "NVMeoFSubsystemNamespaceLimit"
    msg = "{nqn} subsystem has reached its maximum number of namespaces on cluster "

    # Deploy nvmeof service
    LOG.info("deploy nvme service")
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()

    # Configure subsystems, --max-namespaces is 10 and we are creating 10 namesapces
    configure_gw_entities(nvme_service, rbd_obj=rbd_obj)

    # Check for alert
    # NVMeoFSubsystemNamespaceLimit prometheus alert should be firing
    events = PrometheusAlerts(ha.orch)
    LOG.info("Check NVMeoFSubsystemNamespaceLimit should be firing")
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(nqn=nqn_name),
        interval=interval,
    )

    nvmegwcli = ha.gateways[0]
    sub1_args = {"subsystem": nqn_name}
    # Delete an namespace and check alert should be in inactive state
    LOG.info("Delete an namespace and check alert should be in inactive state")
    nvmegwcli.namespace.delete(**{"args": {**sub1_args, **{"nsid": 1}}})
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )

    # Add namespace back and check alert should be in active state
    LOG.info("Add namespace back and check alert should be in active state")
    image = f"image-{generate_unique_id(4)}"
    rbd_obj.create_image(rbd_pool, image, "10G")
    img_args = {"rbd-pool": rbd_pool, "rbd-image": image}
    # img_args = {"rbd-pool": rbd_pool, "rbd-image": image, "nsid": 1}
    nvmegwcli.namespace.add(**{"args": {**sub1_args, **img_args}})
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(nqn=nqn_name),
        interval=interval,
    )

    if config.get("cleanup"):
        LOG.info("Cleaning up NVMeoF services and RBD pool for CEPH-83616917.")
        teardown(nvme_service, rbd_obj)
        global cleanup
        cleanup = True

    LOG.info("CEPH-83616917 - NVMeoFSubsystemNamespaceLimit validated successfully.")


def test_ceph_83617544(ceph_cluster, config):
    """[CEPH-83617544] Warning at Max gateways within a gateway group exceeded on cluster.

    NVMeoFMaxGatewayGroupSize alert users to notify when user created
    more than 8 gateways within a group per cluster

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    time_to_fire = config.get("time_to_fire", 60)
    interval = config.get("interval", 60)
    alert = "NVMeoFMaxGatewayGroupSize"
    msg = "Max gateways within a gateway group ({gw_group}) exceeded on cluster "

    # Deploy nvmeof service
    rbd_obj = config["rbd_obj"]
    LOG.info("deploy nvme service")
    gateway_nodes = deepcopy(config.get("gw_nodes"))
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)

    # Check for alert
    # NVMeoFMaxGatewayGroupSize prometheus alert should be firing
    events = PrometheusAlerts(ha.orch)

    LOG.info("Check NVMeoFMaxGatewayGroupSize should be firing")
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(gw_group=config["gw_group"]),
        interval=interval,
    )

    # Deploy 8 gateways in group and NVMeoFMaxGatewayGroupSize alert should be in inactive state
    LOG.info(
        "Deploy 8 gateways in group and NVMeoFMaxGatewayGroupSize alert should be in inactive state"
    )
    config.update({"gw_nodes": gateway_nodes[0:8]})
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )

    # Add more than 8 gateways and NVMeoFMaxGatewayGroupSize alert should be in firing state
    LOG.info(
        "Add more than 8 gateways and NVMeoFMaxGatewayGroupSize alert should be in firing state"
    )
    config.update({"gw_nodes": gateway_nodes})
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(gw_group=config["gw_group"]),
        interval=interval,
    )

    if config.get("cleanup"):
        LOG.info("Cleaning up NVMeoF services and RBD pool for CEPH-83617544.")
        teardown(nvme_service, rbd_obj)
        global cleanup
        cleanup = True

    LOG.info("CEPH-83617544 - NVMeoFMaxGatewayGroupSize validated successfully.")


def test_ceph_83616916(ceph_cluster, config):
    """[CEPH-83616916] - Warning at subsystem defined without host level security on cluster.

    NVMeoFGatewayOpenSecurity alert helps user to add host security to the subsystem

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    time_to_fire = config.get("time_to_fire", 60)
    interval = config.get("interval", 60)
    rbd_obj = config["rbd_obj"]
    nqn_name = config["subsystems"][0]["nqn"]
    alert = "NVMeoFGatewayOpenSecurity"
    msg = "Subsystem {nqn} has been defined without host level security on cluster "

    # Deploy nvmeof service
    LOG.info("deploy nvme service")
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()
    nvmegwcli = ha.gateways[0]

    configure_gw_entities(nvme_service, rbd_obj=rbd_obj)

    # Check for alert
    # NVMeoFGatewayOpenSecurity prometheus alert should be firing
    events = PrometheusAlerts(ha.orch)
    LOG.info("Check NVMeoFGatewayOpenSecurity should be firing")
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(nqn=nqn_name),
        interval=interval,
    )

    # Delete allow open security
    sub_args = {"subsystem": nqn_name}
    nvmegwcli.host.delete(**{"args": {**sub_args, **{"host": repr("*")}}})
    # Add the host security with host nqn and check the alert is in inactive state
    LOG.info("Add the host security with host nqn and check alert is in inactive state")
    initiator_node = get_node_by_id(ceph_cluster, config.get("host"))
    initiator = NVMeInitiator(initiator_node)
    host_nqn = initiator.initiator_nqn()
    nvmegwcli.host.add(**{"args": {**sub_args, **{"host": host_nqn}}})
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )

    # Allow open security and check for alert
    LOG.info("Add open host access back and check alert should be in active state")
    nvmegwcli.host.add(**{"args": {**sub_args, **{"host": repr("*")}}})
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(nqn=nqn_name),
        interval=interval,
    )

    if config.get("cleanup"):
        LOG.info("Cleaning up NVMeoF services and RBD pool for CEPH-83616916.")
        teardown(nvme_service, rbd_obj)
        global cleanup
        cleanup = True

    LOG.info("CEPH-83616916 - NVMeoFGatewayOpenSecurity validated successfully.")


def test_ceph_83617404(ceph_cluster, config):
    """[CEPH-83617404] Warning at when created more than 4 gateway groups

    NVMeoFMaxGatewayGroups alert users to notify when user created
    more than 4 gateway groups per cluster

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    time_to_fire = config.get("time_to_fire", 60)
    interval = config.get("interval", 60)
    rbd_pool = config["rbd_pool"]
    rbd_obj = config["rbd_obj"]
    alert = "NVMeoFMaxGatewayGroups"
    original_gw_groups = deepcopy(config.get("gw_groups"))
    svcs = list()
    services = []
    gw_groups = deepcopy(config.get("gw_groups"))

    # Deploy nvmeof service
    LOG.info("deploy nvme service")
    # Deploy Services
    for svc in config["gw_groups"]:
        svc.update({"rbd_pool": rbd_pool})
        nvme_service = NVMeService(svc, ceph_cluster)
        nvme_service.deploy()
        nvme_service.init_gateways()
        services.append(nvme_service)
        ha = HighAvailability(ceph_cluster, svc["gw_nodes"], **svc)
        ha.initialize_gateways()
        svcs.append(ha)

    ha1 = svcs[0]
    # Check for alert
    # NVMeoFMaxGatewayGroups prometheus alert should be firing
    events = PrometheusAlerts(ha1.orch)

    LOG.info("Check NVMeoFMaxGatewayGroups should be firing")
    nvmegwcli = ha1.gateways[0]
    # Execute ceph -s --format json and get fsid
    cephs, _ = ha1.orch.shell(args=["ceph", "-s", "--format", "json"])
    cephs = loads(cephs)
    fsid = cephs["fsid"]
    # Check version of cluster if it is less than 20.0 then msg should be "Max gateway groups exceeded on cluster "
    if nvmegwcli.cli_version != "v2":
        msg = "Max gateway groups exceeded on cluster"
    else:
        msg = f"Max gateway groups exceeded on cluster {fsid}".format(fsid=fsid)

    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg,
        interval=interval,
    )

    # Remove one gateway and NVMeoFMaxGatewayGroups alert should be in inactive state
    LOG.info(
        "Remove one gateway group and NVMeoFMaxGatewayGroups alert should be in inactive state"
    )
    rm_add_gw_grp = gw_groups[0:1]
    config.update({"gw_groups": rm_add_gw_grp})
    delete_nvme_service(ceph_cluster, config)
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )

    # Add more than 4 gateway groups and NVMeoFMaxGatewayGroups alert should be in firing state
    LOG.info(
        "Add more than 4 gateway groups and NVMeoFMaxGatewayGroups alert should be in firing state"
    )
    # Deploy Services
    config.update({"gw_groups": original_gw_groups})
    for svc in config["gw_groups"]:
        svc.update({"rbd_pool": rbd_pool})
        nvme_service = NVMeService(svc, ceph_cluster)
        nvme_service.deploy()
        nvme_service.init_gateways()

    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg,
        interval=interval,
    )

    if config.get("cleanup"):
        LOG.info("Cleaning up NVMeoF services and RBD pool for CEPH-83617404.")
        for service in services:
            service.delete_nvme_service()
            rbd_obj.clean_up(pools=[rbd_pool])
    global cleanup
    cleanup = True
    LOG.info("CEPH-83617404 - NVMeoFMaxGatewayGroups validated successfully.")


def test_ceph_83617622(ceph_cluster, config):
    """[CEPH-83617622] - Warning at maximum number of subsystems reached in group

    NVMeoFTooManySubsystems  Prometheus alert users to notify when user created
    more than 128 subsystems in group

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """

    time_to_fire = config.get("time_to_fire", 60)
    interval = config.get("interval", 60)
    rbd_obj = config["rbd_obj"]
    nqn_name = config["subsystems"][0]["nqn"]
    alert = "NVMeoFTooManySubsystems"
    msg = "The number of subsystems defined to the gateway exceeds supported values on cluster "

    # Deploy nvmeof service
    LOG.info("deploy nvme service")
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()

    # Configure subsystems
    nvmegwcl1 = nvme_service.gateways[0]
    created_subsystems = list()
    for i in range(0, 128):
        subsystem = f"{nqn_name}.{i}"
        sub_args = {"subsystem": subsystem}
        nvmegwcl1.subsystem.add(**{"args": {**sub_args, **{"no-group-append": True}}})
        created_subsystems.append(subsystem)

    # Check for alert
    # NVMeoFTooManySubsystems prometheus alert should be firing
    LOG.info(
        "NVMeoFTooManySubsystems should be firing because we have created 128 subsystems in group"
    )
    events = PrometheusAlerts(ha.orch)
    events.monitor_alert(alert, timeout=time_to_fire, interval=interval, msg=msg)

    # Delete few subsystems and check alert is in inactive state
    LOG.info(
        "Delete few subsystems and check NVMeoFTooManySubsystems is in inactive state"
    )
    selected_nqs_to_delete = created_subsystems[-9:]
    for nqn in selected_nqs_to_delete:
        sub_args = {"subsystem": nqn}
        nvmegwcl1.subsystem.delete(**{"args": {**sub_args}})

    # Check for alert and it should be in inactive state
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )

    # Add the deleted nqns and check the alert is in firing state or not
    LOG.info(
        "Add 128 susbystems and check NVMeoFTooManySubsystems alert is in firing state"
    )
    for nqn in selected_nqs_to_delete:
        sub_args = {"subsystem": nqn}
        nvmegwcl1.subsystem.add(**{"args": {**sub_args, **{"no-group-append": True}}})

    events.monitor_alert(alert, timeout=time_to_fire, interval=interval, msg=msg)

    if config.get("cleanup"):
        LOG.info("Cleaning up NVMeoF services and RBD pool for CEPH-83617622.")
        teardown(nvme_service, rbd_obj)
        global cleanup
        cleanup = True

    LOG.info("CEPH-83617622 - NVMeoFTooManySubsystems validated successfully.")


def test_ceph_83617545(ceph_cluster, config):
    """[CEPH-83617545] - Warning at maximum number of namespaces reached in group

    NVMeoFTooManyNamespaces  Prometheus alert users to notify when user created
    more than allowed namespaces in group

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """

    time_to_fire = config.get("time_to_fire", 60)
    interval = config.get("interval", 60)
    rbd_pool = config["rbd_pool"]
    rbd_obj = config["rbd_obj"]
    nqn_name = config["subsystems"][0]["nqn"]
    alert = "NVMeoFTooManyNamespaces"
    msg = "The number of namespaces defined to the gateway exceeds supported values on cluster "

    # Deploy nvmeof service
    LOG.info("deploy nvme service")
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    nvmegwcl1 = nvme_service.gateways[0]

    # Configure subsystems
    LOG.info("Configure subsystems")
    configure_gw_entities(nvme_service, rbd_obj=rbd_obj)

    # Check for alert
    # NVMeoFTooManyNamespaces prometheus alert should be firing
    LOG.info(
        "NVMeoFTooManyNamespaces should be firing because we have created more than allowed namespaces"
    )
    events = PrometheusAlerts(ha.orch)
    events.monitor_alert(alert, timeout=time_to_fire, interval=interval, msg=msg)

    # Delete few namespaces and check alert is in inactive state
    LOG.info(
        "Delete few namespaces and check NVMeoFTooManyNamespaces is in inactive state"
    )

    sub1_args = {"subsystem": nqn_name}
    nvmegwcl1.namespace.delete(**{"args": {**sub1_args, **{"nsid": 1}}})

    # Check for alert and it should be in inactive state
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )

    # Add the namespaces more than limit and check alert is in firing state or not
    LOG.info(
        "Add the namespaces more than limit and check alert is in firing state or not"
    )
    image = f"image-{generate_unique_id(4)}"
    rbd_obj.create_image(rbd_pool, image, "10G")

    img_args = {"rbd-pool": rbd_pool, "rbd-image": image}
    # img_args = {"rbd-pool": rbd_pool, "rbd-image": image, "nsid": 1}
    nvmegwcl1.namespace.add(**{"args": {**sub1_args, **img_args}})

    #  Check for the alert
    events.monitor_alert(alert, timeout=time_to_fire, interval=interval, msg=msg)

    if config.get("cleanup"):
        LOG.info("Cleaning up NVMeoF services and RBD pool for CEPH-83617545.")
        teardown(nvme_service, rbd_obj)
        global cleanup
        cleanup = True

    LOG.info("CEPH-83617545 - NVMeoFTooManyNamespaces validated successfully.")


def test_ceph_83617640(ceph_cluster, config):
    """[CEPH-83617640] - Warning for having different nvme-of gateway releases active on cluster

    NVMeoFVersionMismatch alert will notify the user when user is having
    different nvme-of gateway releases active on cluster

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    time_to_fire = config.get("time_to_fire", 60)
    interval = config.get("interval", 60)
    rbd_obj = config["rbd_obj"]
    nvme_diff_version = config["nvme_diff_version"]
    alert = "NVMeoFVersionMismatch"
    msg = "Too many different NVMe-oF gateway releases active on cluster "

    # Deploy nvmeof service
    LOG.info("deploy nvme service")
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()

    # Configure subsystems
    LOG.info("Configure subsystems")
    configure_gw_entities(nvme_service, rbd_obj=rbd_obj)

    # Check for alert
    # NVMeoFVersionMismatch prometheus alert should be in inactive state
    LOG.info("NVMeoFVersionMismatch should in inactive state")
    events = PrometheusAlerts(ha.orch)
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=interval
    )

    # Get the image version of nvmeof daemon
    LOG.info("Get the current nvmeof image version")
    get_version, _ = ha.orch.shell(
        args=["ceph", "config", "get", "mgr", "mgr/cephadm/container_image_nvmeof"]
    )

    initial_verions_of_nvme = get_version.strip()
    LOG.info(f"Initial version of nvme is {initial_verions_of_nvme}")

    # Set the different nvmeof version so that alert will be active
    # ceph config set mgr mgr/cephadm/container_image_nvmeof  < container image >
    LOG.info(
        f"Set ceph config set mgr mgr/cephadm/container_image_nvmeof  {nvme_diff_version}"
    )
    output, _ = ha.orch.shell(
        args=[
            "ceph",
            "config",
            "set",
            "mgr",
            "mgr/cephadm/container_image_nvmeof",
            f"{nvme_diff_version}",
        ]
    )

    # Redeploy NVMeOF daemon
    LOG.info("Redeploy NVMeof daemon")
    ha.daemon_redeploy(ha.gateways[0])

    # Check if alert is in firing state or not
    LOG.info("Check for firing state of alert")
    events.monitor_alert(alert, timeout=time_to_fire, interval=interval, msg=msg)

    if config.get("cleanup"):
        LOG.info("Cleaning up NVMeoF services and RBD pool for CEPH-83617640.")
        teardown(nvme_service, rbd_obj)
        global cleanup
        cleanup = True

    LOG.info("CEPH-83617640 - NVMeoFVersionMismatch validated successfully.")


testcases = {
    "CEPH-83610948": test_ceph_83610948,
    "CEPH-83610950": test_ceph_83610950,
    "CEPH-83611097": test_ceph_83611097,
    "CEPH-83611098": test_ceph_83611098,
    "CEPH-83611099": test_ceph_83611099,
    "CEPH-83611306": test_ceph_83611306,
    "CEPH-83616917": test_CEPH_83616917,
    "CEPH-83617544": test_ceph_83617544,
    "CEPH-83616916": test_ceph_83616916,
    "CEPH-83617404": test_ceph_83617404,
    "CEPH-83617622": test_ceph_83617622,
    "CEPH-83617545": test_ceph_83617545,
    "CEPH-83617640": test_ceph_83617640,
}


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof HA test execution.

    - Configure Gateways
    - Configures Initiators and Run FIO on NVMe targets.
    - Perform failover and failback.
    - Validate the IO continuation prior and after to failover and failback

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).
    """
    global rbd_obj, nvme_service, cleanup
    config = kwargs["config"]
    kwargs["config"].update(
        {
            "do_not_create_image": True,
            "rep-pool-only": True,
            "rep_pool_config": {"pool": config["rbd_pool"]},
        }
    )

    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    try:
        # NVMe alert test case to run
        if config.get("test_case"):
            test_case_run = testcases[config["test_case"]]
            config.update({"rbd_obj": rbd_obj})
            test_case_run(ceph_cluster, config)
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup") and not cleanup:
            teardown(nvme_service, rbd_obj)

    return 1
