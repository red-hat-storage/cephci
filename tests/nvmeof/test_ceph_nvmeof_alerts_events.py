"""
Test suite that verifies NVMe alerts and NVMe Health checks.
"""

from copy import deepcopy
from json import dumps, loads
from random import choice
from urllib.parse import urljoin, urlparse

import requests

from ceph.ceph import Ceph
from ceph.nvmegw_cli import NVMeGWCLI
from ceph.parallel import parallel
from ceph.waiter import WaitUntil
from tests.nvmeof.test_ceph_nvmeof_high_availability import (
    HighAvailability,
    configure_listeners,
    configure_subsystems,
    deploy_nvme_service,
    get_node_by_id,
    teardown,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.retry import retry
from utility.utils import generate_unique_id

LOG = Log(__name__)


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
        return timeout + interval

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

    # def monitor_alert1(self, alert_name, state="firing", timeout=60, msg=""):
    #     """Monitor NVMe alert."""

    #     @retry(NVMeAlertFailure, tries=3, delay=timeout, backoff=1)
    #     def check():
    #         _alert = self.get_nvme_alert_by_name(alert_name)
    #         if _alert["state"] == state:
    #             LOG.info(
    #                 f"[ {alert_name} ] is in Expected {state} state  - \n{dumps(_alert)}"
    #             )
    #             if msg:
    #                 for alrt in _alert["alerts"]:
    #                     if alrt["annotations"]["summary"] == msg:
    #                         LOG.info(f"[ {alert_name} ] has expected msg {msg}")
    #                         return _alert
    #             return _alert
    #         raise NVMeAlertFailure(
    #             f"[ {alert_name} ] - ( Expected: {state} )\n{dumps(_alert)}"
    #         )

    #     return check()


def test_ceph_83611097(ceph_cluster, config):
    """[CEPH-83611097] NVMeoFNVMeoFMissingListener alert.

    NVMeoFMissingListener alert helps user to identify the missing listener
      which is important for HA. where initiators could get connected to
      multipath data paths, ensuring IO is uninterupted when GW(s) fails.

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    time_to_fire = 600
    interval = 30
    alert = "NVMeoFMissingListener"
    msg = "No listener added for {GW} NVMe-oF Gateway to {NQN} subsystem"

    # Deploy Services
    deploy_nvme_service(ceph_cluster, config)
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)

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
    _rbd_pool = config["rbd_pool"]
    _rbd_obj = config["rbd_obj"]
    time_to_fire = 60
    interval = 10
    alert = "NVMeoFMultipleNamespacesOfRBDImage"
    msg = "RBD image {image} cannot be reused for multiple NVMeoF namespace"
    svcs = []

    # Deploy Services
    for svc in config["gw_groups"]:
        svc.update({"rbd_pool": _rbd_pool})
        deploy_nvme_service(ceph_cluster, svc)
        svcs.append(HighAvailability(ceph_cluster, svc["gw_nodes"], **svc))

    # Create RBD image and multiple NS with that image.
    ha1, ha2 = svcs
    nvmegwcl1 = ha1.gateways[0]
    sub1_args = {"subsystem": f"nqn.2016-06.io.spdk:cnode{generate_unique_id(4)}"}
    nvmegwcl1.subsystem.add(**{"args": {**sub1_args, **{"no-group-append": True}}})

    nvmegwcl2 = ha2.gateways[0]
    sub2_args = {"subsystem": f"nqn.2016-06.io.spdk:cnode{generate_unique_id(4)}"}
    nvmegwcl2.subsystem.add(**{"args": {**sub2_args, **{"no-group-append": True}}})

    image = f"image-{generate_unique_id(4)}"
    _rbd_obj.create_image(_rbd_pool, image, "10G")

    img_args = {"rbd-pool": _rbd_pool, "rbd-image": image, "nsid": 1}
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
    LOG.info(f"CEPH-83610950 - {alert} alert validated successfully.")


def test_ceph_83610948(ceph_cluster, config):
    """[CEPH-83610948] Raise a healthcheck when a gateway is in UNAVAILABLE state.

    This test case validates the warning which would be get hit on any gateway unavailability
    and Raise a healthcheck warning under ceph status and ceph health.

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """
    rbd_pool = config["rbd_pool"]
    deploy_nvme_service(ceph_cluster, config)
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)

    with parallel() as p:
        for subsys_args in config["subsystems"]:
            subsys_args["ceph_cluster"] = ceph_cluster
            p.spawn(configure_subsystems, rbd_pool, ha, subsys_args)

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

    time_to_fire = 600
    intervel = 100
    alert = "NVMeoFZeroListenerSubsystem"
    msg = "No listeners added to {subsystem_name} subsystem"
    gateway_nodes = deepcopy(config.get("gw_nodes"))

    LOG.info("Deploy nvme service")
    deploy_nvme_service(ceph_cluster, config)
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)

    # Configure subsystems
    nvmegwcl1 = ha.gateways[0]
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
        interval=intervel,
    )

    # Add the listener and check alert is inactive state or not
    LOG.info("Add the listener")
    configure_listeners(ha, [gateway_nodes[0]], config)
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=intervel
    )

    # Delete the listener and alert should be in firing state
    LOG.info("Delete the listener")
    listener_args = {"nqn": subsystem1, "listener_port": config["listener_port"]}
    configure_listeners(ha, [gateway_nodes[0]], listener_args, action="delete")
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(subsystem_name=subsystem1),
        interval=intervel,
    )

    # Add the listener and check alert is inactive state or not
    LOG.info("Add the listener back")
    configure_listeners(ha, [gateway_nodes[0]], config)
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=intervel
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
        interval=intervel,
    )

    LOG.info("CEPH-83611098 - {alert} alert validated successfully.")


def test_ceph_83611099(ceph_cluster, config):
    """[CEPH-83611099] Warning message or Status on NVMe Service with single Gateway Node.

    NVMeoFSingleGateway alert indicates to user to add more gateways
    to the service in order to have High Availability.

    Args:
        ceph_cluster: Ceph cluster object
        config: test case config
    """

    time_to_fire = 300
    intervel = 60
    alert = "NVMeoFSingleGateway"
    msg = "The gateway group {gw_group} consists of a single gateway - HA is not possible on cluster"

    # Deploy nvmeof service
    LOG.info("deploy nvme service")
    gateway_nodes = deepcopy(config.get("gw_nodes"))
    random_gateway = choice(gateway_nodes)
    config.update({"gw_nodes": [random_gateway]})
    deploy_nvme_service(ceph_cluster, config)
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    # Check for alert
    # NVMeoFSingleGateway prometheus alert should be firing
    events = PrometheusAlerts(ha.orch)

    LOG.info("Check NVMeoFSingleGateway should be firing")
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(gw_group=config["gw_group"]),
        interval=intervel,
    )

    # Add one more gateway and NVMeoFSingleGateway alert should be in inactive state
    LOG.info("Add one more gateway and NVMeoFSingleGateway should be in inactive state")
    config.update({"gw_nodes": gateway_nodes})
    deploy_nvme_service(ceph_cluster, config)
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=intervel
    )

    # scale down to one gateway and NVMeoFSingleGateway alert should be in firing state
    LOG.info(
        "scale down to one gateway and NVMeoFSingleGateway should be in firing state"
    )
    random_gateway = choice(gateway_nodes)
    config.update({"gw_nodes": [random_gateway]})
    deploy_nvme_service(ceph_cluster, config)
    LOG.info(
        "scaled down to one gateway, NVMeoFSingleGateway should be in firing state"
    )
    events.monitor_alert(
        alert,
        timeout=time_to_fire,
        msg=msg.format(gw_group=config["gw_group"]),
        interval=intervel,
    )

    # Scale up to 2 gateways and alert should be in inactive state
    LOG.info("Add one more gateway and NVMeoFSingleGateway should be in inactive state")
    config.update({"gw_nodes": gateway_nodes})
    deploy_nvme_service(ceph_cluster, config)
    events.monitor_alert(
        alert, timeout=time_to_fire, state="inactive", interval=intervel
    )

    LOG.info("CEPH-83611099 - NVMeoFSingleGateway validated successfully.")


testcases = {
    "CEPH-83610948": test_ceph_83610948,
    "CEPH-83610950": test_ceph_83610950,
    "CEPH-83611097": test_ceph_83611097,
    "CEPH-83611098": test_ceph_83611098,
    "CEPH-83611099": test_ceph_83611099,
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
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")
    config = kwargs["config"]
    kwargs["config"].update(
        {
            "do_not_create_image": True,
            "rep-pool-only": True,
            "rep_pool_config": {"pool": config["rbd_pool"]},
        }
    )

    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeGWCLI.NVMEOF_CLI_IMAGE = value
            break

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
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)

    return 1
