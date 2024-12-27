"""
Test module that executes the external S3 Tests.

The purpose of this test module is to execute the test suites available @
https://github.com/ceph/s3-tests. Over here, we have three stages

    - Test Setup
    - Execute Tests
    - Test Teardown

In test setup stage, the test suite is cloned and the necessary steps to execute it is
carried out.

In execute test stage, the test suite is executed using tags based on the RHCS build
version.

In test teardown, the cloned repository is removed and the configuration change undone.

Requirement parameters
     ceph_nodes:    The list of node participating in the RHCS environment.
     config:        The test configuration
        branch      the s3test branch to be used.

Entry Point:
    def run(**args):
"""

import binascii
import json
import os
from json import loads
from time import sleep
from typing import Dict, Optional, Tuple

from jinja2 import Template

from ceph.ceph import Ceph, CephNode, CommandFailed
from utility.log import Log

log = Log(__name__)
S3CONF = """[DEFAULT]
host = {{ data.host }}
port = {{ data.port }}
is_secure = {{ data.secure }}
ssl_verify = false

[fixtures]
bucket_prefix = cephci-{random}-

[s3 main]
api_name = default
display_name = {{ data.main.name }}
user_id = {{ data.main.id }}
access_key = {{ data.main.access_key }}
secret_key = {{ data.main.secret_key }}
email = {{ data.main.email }}
{%- if data.main.kms_keyid %}
kms_keyid = {{ data.main.kms_keyid }}
{% endif %}

[s3 alt]
display_name = {{ data.alt.name }}
user_id = {{ data.alt.id }}
access_key = {{ data.alt.access_key }}
secret_key = {{ data.alt.secret_key }}
email = {{ data.alt.email }}

[s3 tenant]
display_name = {{ data.tenant.name }}
user_id = {{ data.tenant.id }}
access_key = {{ data.tenant.access_key }}
secret_key = {{ data.tenant.secret_key }}
email = {{ data.tenant.email }}

{%- if data.iam %}
[iam]
display_name = {{ data.iam.name }}
user_id = {{ data.iam.id }}
access_key = {{ data.iam.access_key }}
secret_key = {{ data.iam.secret_key }}
email = {{ data.iam.email }}
{% endif %}

{%- if data.webidentity %}
[webidentity]
token = {{ data.webidentity.token }}
aud = {{ data.webidentity.aud }}
thumbprint = {{ data.webidentity.thumbprint }}
KC_REALM = {{ data.webidentity.realm }}
{% endif %}

"""
S3CONF_ACC = """
{%- if data.iamroot %}
[iam root]
display_name = {{ data.iamroot.name }}
user_id = {{ data.iamroot.account_id }}
access_key = {{ data.iamroot.access_key }}
secret_key = {{ data.iamroot.secret_key }}
email = {{ data.iamroot.email }}
{% endif %}

{%- if data.iamrootalt %}
[iam alt root]
display_name = {{ data.iamrootalt.name }}
user_id = {{ data.iamrootalt.account_id }}
access_key = {{ data.iamrootalt.access_key }}
secret_key = {{ data.iamrootalt.secret_key }}
email = {{ data.iamrootalt.email }}
{% endif %}
"""


def run(**kw):
    log.info("Running s3-tests")

    cluster = kw["ceph_cluster"]
    config = kw.get("config")
    build = config.get("build", config.get("rhbuild"))
    host = config.get("host")
    secure = config.get("ssl", False)

    client_node = cluster.get_nodes(role="client")[0]

    execute_setup(cluster, config)

    if not host:
        _, secure, _ = get_rgw_frontend(cluster)

    exit_status = execute_s3_tests(client_node, build, secure)
    execute_teardown(cluster, build)

    log.info("Returning status code of %s", exit_status)
    return exit_status


def execute_setup(cluster: Ceph, config: dict) -> None:
    """
    Execute the prerequisites required to run the tests.

    It involves the following steps
        - install the required software (radosgw for CLI execution)
        - Clone the S3 tests repo in the client node
        - Install S3 pre-requisites in the client node
        - Open the firewall port on the RGW node
        - Add 'rgw_lc_debug_interval' key to the RGW node config
        - Restart the service

    Args:
        cluster: Ceph cluster participating in the test.
        config:  The key/value pairs passed by the tester.

    Raises:
        CommandFailed:  When a remote command returned non-zero value.
    """
    build = config.get("build", config.get("rhbuild"))
    secure = config.get("ssl", False)
    host = config.get("host")
    port = config.get("port")

    client_node = cluster.get_nodes(role="client")[0]
    rgw_node = cluster.get_nodes(role="rgw")[0]

    branch = config.get("branch", "ceph-quincy")
    clone_s3_tests(node=client_node, branch=branch)
    install_s3test_requirements(client_node, branch, os_ver=build[-1])

    if not all([host, port]):
        host = rgw_node.shortname
        lib, secure, port = get_rgw_frontend(cluster)

    kms_keyid = config.get("kms_keyid")
    create_s3_conf(cluster, build, host, port, secure, kms_keyid)

    if build.startswith("4"):
        rgw_node.open_firewall_port(port=port, protocol="tcp")

    add_lc_debug(cluster, build)


def execute_s3_tests(node: CephNode, build: str, encryption: bool = False) -> int:
    """
    Return the result of S3 test run.

    Args:
        node        The node from which the test execution is triggered.
        build       The RH build version
        encryption  include encryption test or not
    Returns:
        0 - Success
        1 - Failure
    """
    log.debug("Executing s3-tests")
    try:
        base_cmd = "cd s3-tests; S3TEST_CONF=config.yaml virtualenv/bin/nosetests -v"
        extra_args = "-a '!fails_on_rgw,!fails_strict_rfc2616,!encryption'"
        tests = "s3tests"

        if not build.split(".")[0] >= "7":
            extra_args = "-a '!fails_on_rgw,!fails_strict_rfc2616"

            if not encryption:
                extra_args += ",!encryption"

            extra_args += ",!test_of_sts,!s3select,!user-policy,!webidentity_test'"
            tests = "s3tests_boto3"

        else:
            base_cmd = "cd s3-tests; S3TEST_CONF=config.yaml virtualenv/bin/tox"
            extra_args = "-- -v -m 'not fails_on_rgw and not fails_strict_rfc2616"
            tests = "s3tests_boto3"

            if not encryption:
                extra_args += " and not encryption"
            extra_args += " and not user-policy and not webidentity_test'"

        cmd = f"{base_cmd} {extra_args} {tests}"
        return node.exec_command(cmd=cmd, long_running=True)
    except CommandFailed as e:
        log.warning("Received CommandFailed")
        log.warning(e)
        return 1


def execute_teardown(cluster: Ceph, build: str) -> None:
    """
    Execute the test teardown phase.
    """
    command = "sudo rm -rf s3-tests"

    node = cluster.get_nodes(role="client")[0]
    node.exec_command(cmd=command)

    del_lc_debug(cluster, build)


# Internal functions


def clone_s3_tests(node: CephNode, branch="ceph-luminous") -> None:
    """Clone the S3 repository on the given node."""
    repo_url = "https://github.com/ceph/s3-tests.git"
    node.exec_command(cmd="if test -d s3-tests; then sudo rm -r s3-tests; fi")
    node.exec_command(cmd=f"git clone -b {branch} {repo_url}")


def install_s3test_requirements(
    node: CephNode, branch: str, os_ver: Optional[str] = "8"
) -> None:
    """
    Install the required packages required by S3tests.

    The S3test requirements are installed via bootstrap however for RHCS it is a
    simulation of the manual steps.

    Args:
        node:   The node that is considered for running S3Tests.
        branch: The branch to be installed
        os_ver: The OS major version

    Raises:
        CommandFailed:  Whenever a command returns a non-zero value part of the method.
    """
    if branch in ["ceph-nautilus", "ceph-luminous"] or os_ver == "9":
        return _s3tests_req_install(node, os_ver)

    _s3tests_req_bootstrap(node)


def get_rgw_frontend(cluster: Ceph) -> Tuple:
    """
    Returns the RGW frontend information.

    The frontend information is found by
        - getting the config dump (or)
        - reading the config file

    Args:
         cluster     The cluster participating in the test

    Returns:
         Tuple(str, bool, int)
            lib         beast or civetweb
            secure      True if secure else False
            port        the configured port
    """
    frontend_value = None

    try:
        node = cluster.get_nodes(role="client")[0]
        out, err = node.exec_command(sudo=True, cmd="ceph config dump --format json")
        configs = loads(out)

        for config in configs:
            if config.get("name").lower() != "rgw_frontends":
                continue

            frontend_value = config.get("value").split()

        # the command would work but may not have the required values
        if not frontend_value:
            raise AttributeError("Config has no frontend information. Trying conf file")

    except BaseException as e:
        log.debug(e)

        # Process via config
        node = cluster.get_nodes(role="rgw")[0]

        # rgw frontends = civetweb port=192.168.122.199:8080 num_threads=100
        command = "grep -e '^rgw frontends' /etc/ceph/ceph.conf"
        out, err = node.exec_command(sudo=True, cmd=command)
        key, sep, value = out.partition("=")
        frontend_value = value.lstrip().split()

        if not frontend_value:
            raise AttributeError("RGW frontend details not found in conf.")

    lib = "beast" if "beast" in frontend_value else "civetweb"
    secure = False
    port = 80

    # Doublecheck the port number
    for value in frontend_value:
        # support values like endpoint=x.x.x.x:port ssl_port=443 port=x.x.x.x:8080
        if "port" in value or "endpoint" in value:
            sep = ":" if ":" in value else "="
            port = value.split(sep)[-1]
            continue

        if not secure and "ssl" in value.lower():
            secure = True
            continue

    return lib, secure, port


def create_s3_user(node: CephNode, user_prefix: str, data: Dict) -> None:
    """
    Create a S3 user with the given display_name.

    The other required information for creating a user is auto generated.

    Args:
        node: node in the cluster to create the user on
        user_prefix: Prefix to be added to the new user
        data: a reference to the payload that needs to be updated.

    Returns:
        user_info dict
    """
    uid = binascii.hexlify(os.urandom(32)).decode()
    display_name = f"{user_prefix}-user"
    log.info(f" the display name is {display_name} ")
    log.info("Creating user: {display_name}".format(display_name=display_name))
    if display_name == "iamrootalt-user":
        cmd = (
            f"radosgw-admin user create --uid=iamrootalt-user "
            f"--account-id RGW22222222222222222 --account-root "
            f"--display_name={display_name} --email={display_name}@foo.bar"
        )
    elif display_name == "iamroot-user":
        cmd = (
            f"radosgw-admin user create --uid=iamroot-user "
            f"--account-id RGW11111111111111111 --account-root "
            f"--display_name={display_name} --email={display_name}@foo.bar"
        )
    else:

        cmd = f"radosgw-admin user create --uid={uid} --display_name={display_name}"
        cmd += " --email={email}@foo.bar".format(email=uid)

    out, err = node.exec_command(sudo=True, cmd=cmd)
    user_info = json.loads(out)
    log.info(f" the user info for {display_name} is {user_info} ")
    data[user_prefix] = {
        "id": user_info["keys"][0]["user"],
        "access_key": user_info["keys"][0]["access_key"],
        "secret_key": user_info["keys"][0]["secret_key"],
        "name": user_info["display_name"],
        "email": user_info["email"],
    }
    if display_name in ("iamrootalt-user", "iamroot-user"):
        data[user_prefix] = {
            "id": user_info["keys"][0]["user"],
            "access_key": user_info["keys"][0]["access_key"],
            "secret_key": user_info["keys"][0]["secret_key"],
            "name": user_info["display_name"],
            "email": user_info["email"],
            "account_id": user_info["account_id"],
        }

    if user_prefix == "iam":
        log.info("Adding user-policy caps for IAM user")
        cmd = f'radosgw-admin caps add --uid={uid} --caps="user-policy=*"'
        out, err = node.exec_command(sudo=True, cmd=cmd)
        cmd = f'radosgw-admin caps add --uid={uid} --caps="roles=*"'
        out, err = node.exec_command(sudo=True, cmd=cmd)


def create_s3_conf(
    cluster: Ceph,
    build: str,
    host: str,
    port: str,
    secure: bool,
    kms_keyid: Optional[str] = None,
) -> None:
    """
    Generate the S3TestConf for test execution.

    Args:
        cluster     The cluster participating in the test
        build       The RHCS version string
        host        The RGW hostname to be set in the conf
        port        The RGW port number to be used in the conf
        secure      If the connection is secure or unsecure.
        kms_keyid   key to be used for encryption
    """
    log.info("Creating the S3TestConfig file")
    data = dict({"host": host, "port": int(port), "secure": secure})

    rgw_node = cluster.get_nodes(role="rgw")[0]
    client_node = cluster.get_nodes(role="client")[0]

    if not build.startswith("4"):
        rgw_node = client_node

    create_s3_user(node=rgw_node, user_prefix="main", data=data)
    create_s3_user(node=rgw_node, user_prefix="alt", data=data)
    create_s3_user(node=rgw_node, user_prefix="tenant", data=data)
    create_s3_user(node=rgw_node, user_prefix="iam", data=data)
    if build.split(".")[0] >= "8":
        create_s3_user(node=rgw_node, user_prefix="iamroot", data=data)
        create_s3_user(node=rgw_node, user_prefix="iamrootalt", data=data)

    if kms_keyid:
        data["main"]["kms_keyid"] = kms_keyid

    global S3CONF
    global S3CONF_ACC
    if build.split(".")[0] >= "8":
        S3CONF = S3CONF + S3CONF_ACC

    templ = Template(S3CONF)
    _config = templ.render(data=data)

    conf_file = client_node.remote_file(file_name="s3-tests/config.yaml", file_mode="w")
    conf_file.write(_config)
    conf_file.flush()


def add_lc_debug(cluster: Ceph, build: str) -> None:
    """
    Modifies the RGW conf files to support lifecycle actions.

    Args:
        cluster:    The cluster participating in the test.
        build:      The RHCS build version

    Raises:
        CommandFailed:  Whenever a command returns a non-zero value part of the method.
    """
    log.debug("Setting the lifecycle interval for all RGW daemons")
    if build.startswith("4"):
        _rgw_lc_debug_conf(cluster, add=True)
        return

    _rgw_lc_debug(cluster, add=True)


def del_lc_debug(cluster: Ceph, build: str) -> None:
    """
    Modifies the RGW conf files to support lifecycle actions.

    Args:
        cluster:    The cluster participating in the tests.
        build:      The RHCS build version

    Raises:
        CommandFailed:  Whenever a command returns a non-zero value part of the method.
    """
    log.debug("Removing the lifecycle configuration")
    if build.startswith("4"):
        _rgw_lc_debug_conf(cluster, add=False)
        return

    _rgw_lc_debug(cluster, add=False)


# Private functions


def _s3test_req_py3(node: CephNode) -> None:
    """
    Creates a python3 virtual environment and install the s3 prerequisite packages.

    Args:
        node (CephNode):    The system to be used for creating the venv.

    Raises:
        CommandFailed
    """
    packages = [
        "python3-devel",
        "libevent-devel",
        "libffi-devel",
        "libxml2-devel",
        "libxslt-devel",
        "zlib-devel",
    ]
    commands = [
        "python3 -m venv s3-tests/virtualenv",
        "s3-tests/virtualenv/bin/python -m pip install --upgrade pip setuptools",
        "s3-tests/virtualenv/bin/python -m pip install -r s3-tests/requirements.txt",
        "s3-tests/virtualenv/bin/python s3-tests/setup.py develop",
    ]
    node.exec_command(
        sudo=True, check_ec=False, cmd=f"yum install -y {' '.join(packages)}"
    )

    for cmd in commands:
        node.exec_command(cmd=cmd)


def _s3tests_req_install(node: CephNode, os_ver: str) -> None:
    """Install S3 prerequisites via pip."""
    if os_ver == "9":
        return _s3test_req_py3(node)

    packages = [
        "python2-virtualenv",
        "python2-devel",
        "libevent-devel",
        "libffi-devel",
        "libxml2-devel",
        "libxslt-devel",
        "zlib-devel",
    ]
    node.exec_command(
        sudo=True, cmd="yum groupinstall -y 'Development Tools'", check_ec=False
    )
    node.exec_command(
        sudo=True, cmd=f"yum install -y --nogpgcheck {' '.join(packages)}"
    )

    venv_cmd = "virtualenv" if os_ver == "7" else "virtualenv-2"
    commands = [
        f"{venv_cmd} -p python2 --no-site-packages --distribute s3-tests/virtualenv",
        "s3-tests/virtualenv/bin/pip install --upgrade pip setuptools",
        "s3-tests/virtualenv/bin/pip install -r s3-tests/requirements.txt",
        "s3-tests/virtualenv/bin/python s3-tests/setup.py develop",
    ]
    for cmd in commands:
        node.exec_command(cmd=cmd)


def _s3tests_req_bootstrap(node: CephNode) -> None:
    """Install the S3tests using bootstrap script."""
    node.exec_command(cmd="cd s3-tests; ./bootstrap")


def _rgw_lc_debug_conf(cluster: Ceph, add: bool = True) -> None:
    """
    Modifies the LC debug config entry based on add being set or unset.

    The LC debug value is set in ceph conf file

    Args:
        cluster     The cluster participating in the tests.
        add         If set, the config flag is added else removed

    Returns:
        None
    """
    if add:
        command = (
            r"sed -i '/\[global\]/a rgw lc debug interval = 10' /etc/ceph/ceph.conf"
        )
    else:
        command = "sed -i '/rgw lc debug interval/d' /etc/ceph/ceph.conf"

    command += " && systemctl restart ceph-radosgw@rgw.`hostname -s`.rgw0.service"

    for node in cluster.get_nodes(role="rgw"):
        node.exec_command(sudo=True, cmd=command)

    # Service restart can take time
    sleep(60)

    log.debug("Lifecycle dev configuration set to 10")


def _rgw_lc_debug(cluster: Ceph, add: bool = True) -> None:
    """
    Modifies the Lifecycle interval parameter used for testing.

    The configurable is enable if add is set else disabled. In case of CephAdm, the
    value has to set for every daemon.

    Args:
        cluster     The cluster participating in the test
        add         If set adds the configurable else unsets it.

    Returns:
        None
    """
    node = cluster.get_nodes(role="client")[0]

    out, err = node.exec_command(
        sudo=True, cmd="ceph orch ps --daemon_type rgw --format json"
    )
    rgw_daemons = [f"client.rgw.{x['daemon_id']}" for x in loads(out)]

    out, err = node.exec_command(
        sudo=True, cmd="ceph orch ls --service_type rgw --format json"
    )
    rgw_services = [x["service_name"] for x in loads(out)]

    # Set (or) Unset the lc_debug_interval for all daemons
    for daemon in rgw_daemons:
        if add:
            command = f"ceph config set {daemon} rgw_lc_debug_interval 10"
            command_1 = f"ceph config set {daemon} rgw_sts_key abcdefghijklmnop"
            command_2 = f"ceph config set {daemon} rgw_s3_auth_use_sts true"
        else:
            command = f"ceph config rm {daemon} rgw_lc_debug_interval"
            command_1 = f"ceph config rm {daemon} rgw_sts_key"
            command_2 = f"ceph config rm {daemon} rgw_s3_auth_use_sts"

        node.exec_command(sudo=True, cmd=command)
        node.exec_command(sudo=True, cmd=command_1)
        node.exec_command(sudo=True, cmd=command_2)

    for service in rgw_services:
        node.exec_command(sudo=True, cmd=f"ceph orch restart {service}")

    # Restart can take time
    sleep(30)
