"""Manage the Ceph Dashboard service via ceph CLI."""
import json
import logging
import tempfile
from time import sleep

import requests

LOG = logging.getLogger(__name__)


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

    for cmd in DASHBOARD_ENABLE_COMMANDS:
        out, err = cls.shell(args=[cmd])
        LOG.info("STDOUT:\n %s" % out)
        LOG.error("STDERR:\n %s" % err)

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
    LOG.info("STDOUT:\n %s" % out)
    LOG.error("STDERR:\n %s" % err)

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
    formatted = json.loads(out)
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
        LOG.info(
            "Dashboard API login is successful, status code : %s" % resp.status_code
        )
