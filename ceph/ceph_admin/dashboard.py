"""Manage the Ceph Dashboard service via ceph CLI."""
import json
import tempfile
from json import loads
from time import sleep

import requests

from utility.log import Log

LOG = Log(__name__)


class DaemonFailure(Exception):
    pass


def execute_commands(cls, commands):
    for cmd in commands:
        out, err = cls.shell(args=[cmd])
        LOG.info(f"Output:\n {out}")
        LOG.error(f"Error:\n {err}")


def validate_url(url):
    """Method to validate provided API URL.
    The HTTP 200 OK success status response code indicates that the request has succeeded.

    Args:
        url: URL of any API.
    """

    url = url.rstrip()
    resp = requests.get(url, verify=False)

    if resp.status_code == 200:
        LOG.info("API validation is successful")
    else:
        raise Exception(f"API validation fails with status code:{resp.status_code}")


def enable_dashboard(cls, config):
    """Method to enable the dashboard module.

    if user bootstrap with skip-dashboard option
    then enabling the dashboard module.

    Args:
        cls (CephAdmin object) : cephadm instance object.
        config (Dict): Key/value pairs passed from the test suite.

        Example::

            args:
                username: admin123
                password: admin@123
    """
    user = config.get("username")
    pwd = config.get("password")

    # To create password text file
    temp_file = tempfile.NamedTemporaryFile(suffix=".txt")
    passwd_file = cls.installer.node.remote_file(
        sudo=True, file_name=temp_file.name, file_mode="w"
    )
    passwd_file.write(pwd)
    passwd_file.flush()

    # To enable dashboard module
    DASHBOARD_ENABLE_COMMANDS = [
        "ceph mgr module enable dashboard",
        "ceph dashboard create-self-signed-cert",
    ]

    execute_commands(cls, DASHBOARD_ENABLE_COMMANDS)

    # command to create username and password to access dashboard as administrator
    cmd = [
        "ceph",
        "dashboard",
        "ac-user-create",
        user,
        "-i",
        temp_file.name,
        "administrator",
    ]

    out, err = cls.shell(
        args=cmd,
        base_cmd_args={"mount": "/tmp:/tmp"},
    )
    LOG.info(f"Output:\n {out}")
    LOG.error(f"Error:\n {err}")

    validate_enable_dashboard(cls, user, pwd)


def validate_enable_dashboard(cls, username, password):
    """Method to validate dashboard login.

    After enabling the dashboard module validating by API login.

    Args:
        cls (CephAdmin object): cephadm instance.
        username (str) : configured user name to login.
        password (str) : configured password to login.
    """
    LOG.info("wait for some seconds for mgr to setup dashboard")
    sleep(100)
    out, _ = cls.shell(args=["ceph", "mgr", "services"])
    formatted = loads(out)
    # validating dashboard URL
    url = formatted.get("dashboard")
    if not url:
        raise KeyError("Dashboard URL not found")
    LOG.info(f"Dashboard URL to login : {url}")
    LOG.info(
        f"Configured credentials to login username:{username} and password:{password}"
    )

    # Performing dasboard API login test
    data = json.dumps({"username": username, "password": password})
    url_ = f"{url}/api/auth" if not url.endswith("/") else f"{url}api/auth"

    session = requests.Session()
    session.headers = {
        "accept": "application/vnd.ceph.api.v1.0+json",
        "content-type": "application/json",
    }

    resp = session.post(url_, data=data, verify=False)
    if not resp.ok:
        LOG.error(
            f"Dashboard API login is not successful, Status code: \
            {resp.status_code}\nResponse: {resp.text}"
        )
    else:
        LOG.info(f"Dashboard API login is successful, status code : {resp.status_code}")


def enable_alertmanager(cls, config):
    """Method to enable alertmanager.
    Args:
        cls (CephAdmin object) : cephadm instance object.
        config (Dict): Key/value pairs passed from the test suite.
    """
    port = config.get("alertmanager-port")
    ssl = config.get("alertmanager_ssl_set")

    # check if alertmanager daemon exist
    out, _ = cls.shell(
        args=["ceph", "orch", "ps", "--daemon_type", "alertmanager", "-f", "json"]
    )
    daemon_obj = loads(out)

    if daemon_obj:

        # To get hostname in which daemon got deployed
        for daemon in daemon_obj:
            host = daemon["hostname"]

        # To get ip address of that host
        for node in cls.cluster.get_nodes():
            if node.hostname == host:
                node_ip = node.ip_address
                break

        ALERTMANAGER_ENABLE_COMMANDS = [
            f"ceph dashboard set-alertmanager-api-host 'http://{node_ip}:{port}'",
            f"ceph dashboard set-alertmanager-api-ssl-verify {ssl}",
        ]

        execute_commands(cls, ALERTMANAGER_ENABLE_COMMANDS)

        url, _ = cls.shell(args=["ceph", "dashboard", "get-alertmanager-api-host"])
        validate_url(url)
    else:
        raise DaemonFailure("Daemon alertmanager does not exists")


def enable_prometheus(cls, config):
    """Method to enable prometheus.
    Args:
        cls (CephAdmin object) : cephadm instance object.
        config (Dict): Key/value pairs passed from the test suite.
    """
    port = config.get("prometheus-port")
    ssl = config.get("prmoetheus-ssl-set")

    # check if prometheus daemon exist
    out, _ = cls.shell(
        args=["ceph", "orch", "ps", "--daemon_type", "prometheus", "-f", "json"]
    )
    daemon_obj = loads(out)

    if daemon_obj:
        # To get hostname in which daemon got deployed
        for daemon in daemon_obj:
            host = daemon["hostname"]

        # To get ip address of that host
        for node in cls.cluster.get_nodes():
            if node.hostname == host:
                node_ip = node.ip_address
                break

        PROMETHEUS_ENABLE_COMMANDS = [
            f"ceph dashboard set-prometheus-api-host 'http://{node_ip}:{port}'",
            f"ceph dashboard set-prometheus-api-ssl-verify {ssl}",
        ]

        execute_commands(cls, PROMETHEUS_ENABLE_COMMANDS)

        url, _ = cls.shell(args=["ceph", "dashboard", "get-prometheus-api-host"])
        validate_url(url)
    else:
        raise DaemonFailure("Daemon prometheus does not exists")


def enable_grafana(cls, config):
    """Method to enable grafana.
    Args:
        cls (CephAdmin object) : cephadm instance object.
        config (Dict): Key/value pairs passed from the test suite.
    """
    port = config.get("grafana-port")
    ssl = config.get("grafana-ssl-set")

    # check if grafana daemon exist
    out, _ = cls.shell(
        args=["ceph", "orch", "ps", "--daemon_type", "grafana", "-f", "json"]
    )
    daemon_obj = loads(out)

    if daemon_obj:
        # To get hostname in which daemon got deployed
        for daemon in daemon_obj:
            host = daemon["hostname"]

        # To get ip address of that host
        for node in cls.cluster.get_nodes():
            if node.hostname == host:
                node_ip = node.ip_address
                break

        GRAFANA_ENABLE_COMMANDS = [
            f"ceph dashboard set-grafana-api-url 'https://{node_ip}:{port}'",
            f"ceph dashboard set-grafana-api-ssl-verify {ssl}",
        ]

        execute_commands(cls, GRAFANA_ENABLE_COMMANDS)

        url, _ = cls.shell(args=["ceph", "dashboard", "get-grafana-api-url"])
        validate_url(url)
    else:
        raise DaemonFailure("Daemon grafana does not exists")
