"""
Test module that executes the external S3 Tests.

The purpose of this test module is to execute the test suites available @
https://github.com/ceph/s3-tests. Over here, we have three stages

    - Test Setup
    - Execute Tests
    - Test Teardown

In the test setup stage, the test suite is clone and the necessary steps to execute it
is carried out here.

In the execute test stage, the test suite is executed using tags based on the RHCS
build version.

In the test teardown, the cloned repository is removed and the configurations done are
reverted.

Requirement parameters
     ceph_nodes:    The list of node participating in the RHCS environment.
     config:        The test configuration
        branch      the s3test branch to be used.

Entry Point:
    def run(**args):
"""

import binascii
import json
import logging
import os

from ceph.ceph import Ceph, CephNode, CommandFailed
from ceph.utils import open_firewall_port

log = logging.getLogger(__name__)


def run(**kw):
    log.info("Running s3-tests")

    cluster = kw["ceph_cluster"]
    config = kw.get("config")
    build = config.get("build", config.get("rhbuild"))
    client_node = cluster.get_nodes(role="client")[0]

    execute_setup(cluster, config)
    exit_status = execute_s3_tests(client_node, build)
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
    client_node = cluster.get_nodes(role="client")[0]
    rgw_node = cluster.get_nodes(role="rgw")[0]

    branch = config.get("branch", "ceph-luminous")
    clone_s3_tests(node=client_node, branch=branch)
    install_s3test_requirements(client_node, branch)

    host = rgw_node.shortname
    secure = config.get("is_secure", "no")
    port = "443" if secure.lower() == "yes" else rgw_frontend_port(cluster, build)
    create_s3_conf(cluster, build, host, port, secure)

    if not build.startswith("5"):
        open_firewall_port(rgw_node, port=port, protocol="tcp")

    add_lc_debug(cluster, build)


def execute_s3_tests(node: CephNode, build: str) -> int:
    """
    Return the result of S3 test run.

    Args:
        node: The node from which the test execution is triggered.
        build: the RH build version

    Returns:
        0 - Success
        1 - Failure
    """
    log.info("Executing s3-tests")
    try:
        base_cmd = "cd s3-tests; S3TEST_CONF=config.yaml virtualenv/bin/nosetests -v"
        extra_args = "-a '!fails_on_rgw,!fails_strict_rfc2616,!encryption'"
        tests = "s3tests"

        if build.startswith("5"):
            extra_args = "-a '!fails_on_rgw,!fails_strict_rfc2616,!encryption"
            extra_args += ",!test_of_sts,!lifecycle,!s3select,!user-policy"
            extra_args += ",!webidentity_test'"
            tests = "s3tests_boto3"

        cmd = f"{base_cmd} {extra_args} {tests}"
        out, err = node.exec_command(cmd=cmd, timeout=3600)
        log.info(out.read().decode())
        log.error(err.read().decode())
    except CommandFailed as e:
        log.warning("Received CommandFailed")
        log.warning(e)
        return 1

    return 0


def execute_teardown(cluster: Ceph, build: str) -> None:
    """
    Execute the test teardown phase.
    """
    command = "rm -r s3-tests"

    node = cluster.get_nodes(role="client")[0]
    node.exec_command(cmd=command)

    del_lc_debug(cluster, build)


# Internal functions


def clone_s3_tests(node: CephNode, branch="ceph-luminous") -> None:
    """Clone the S3 repository on the given node."""
    repo_url = "https://github.com/ceph/s3-tests.git"
    node.exec_command(cmd="if test -d s3-tests; then rm -r s3-tests; fi")
    node.exec_command(cmd=f"git clone -b {branch} {repo_url}")


def install_s3test_requirements(node: CephNode, branch: str) -> None:
    """
    Install the required packages required by S3tests.

    The S3test requirements are installed via bootstrap however for RHCS it is a
    simulation of the manual steps.

    Args:
        node:   The node that is consider for running S3Tests.
        branch: The branch to be installed

    Raises:
        CommandFailed:  Whenever a command returns a non-zero value part of the method.
    """
    rhel8, err = node.exec_command(
        cmd="grep -i 'release 8' /etc/redhat-release", check_ec=False
    )
    rhel8 = rhel8.read().decode()

    if branch == "ceph-nautilus" and rhel8:
        return _s3tests_req_install(node)

    _s3tests_req_bootstrap(node)


def rgw_frontend_port(cluster: Ceph, build: str) -> str:
    """
    Return the configured port number of RadosGW.

    For prior versions of RHCS 5.0, the port number is determined using the ceph.conf
    and for the higher versions, the value is retrieved from cephadm.

    Note: In case of RHCS 5.0, we assume that the installer node is provided.

    Args:
        cluster:    The cluster participating in the test.
        build:      RHCS version string.

    Returns:
        port_number:    The configured port number
    """
    node = cluster.get_nodes(role="rgw")[0]

    if build.startswith("5"):
        node = cluster.get_nodes(role="client")[0]
        # Allow realm & zone variability hence using config dump instead of config-key
        cmd1 = "ceph config dump | grep client.rgw | grep port | head -n 1"
        cmd2 = "cut -d '=' -f 2 | cut -d ' ' -f 1"
        command = f"{cmd1} | {cmd2}"
    else:
        # To determine the configured port, the line must start with rgw frontends
        # in ceph.conf. An example is given below,
        #
        # rgw frontends = civetweb port=192.168.122.199:8080 num_threads=100
        cmd1 = "grep -e '^rgw frontends' /etc/ceph/ceph.conf"
        cmd2 = "cut -d ':' -f 2 | cut -d ' ' -f 1"
        command = f"{cmd1} | {cmd2}"

    out, _ = node.exec_command(sudo=True, cmd=command)
    return out.read().decode().strip()


def create_s3_user(node, display_name, email=False):
    """
    Create a S3 user with the given display_name.

    The other required information for creating an user is auto generated.

    Args:
        node: node in the cluster to create the user on
        display_name: display name for the new user
        email: (optional) generate fake email address for user

    Returns:
        user_info dict
    """
    uid = binascii.hexlify(os.urandom(32)).decode()
    log.info("Creating user: {display_name}".format(display_name=display_name))

    cmd = f"radosgw-admin user create --uid={uid} --display_name={display_name}"

    if email:
        cmd += " --email={email}@foo.bar".format(email=uid)

    out, err = node.exec_command(sudo=True, cmd=cmd)
    user_info = json.loads(out.read().decode())

    return user_info


def create_s3_conf(
    cluster: Ceph, build: str, host: str, port: str, secure: str
) -> None:
    """
    Generate the S3TestConf for test execution.

    Args:
        cluster:    The cluster participating in the test
        build:      The RHCS version string
        host:       The RGW hostname to be set in the conf
        port:       The RGW port number to be used in the conf
        secure:     If the connection is secure or unsecure.
    """
    log.info("Creating the S3TestConfig file")

    rgw_node = cluster.get_nodes(role="rgw")[0]
    client_node = cluster.get_nodes(role="client")[0]

    if build.startswith("5"):
        rgw_node = client_node

    main_user = create_s3_user(node=rgw_node, display_name="main-user", email=True)
    alt_user = create_s3_user(node=rgw_node, display_name="alt-user", email=True)
    tenant_user = create_s3_user(node=rgw_node, display_name="tenant", email=True)

    _config = """
[DEFAULT]
host = {host}
port = {port}
is_secure = {secure}

[fixtures]
bucket prefix = cephuser-{random}-

[s3 main]
user_id = {main_id}
display_name = {main_name}
access_key = {main_access_key}
secret_key = {main_secret_key}
email = {main_email}
api_name = default

[s3 alt]
user_id = {alt_id}
display_name = {alt_name}
email = {alt_email}
access_key = {alt_access_key}
secret_key = {alt_secret_key}

[s3 tenant]
user_id = {tenant_id}
display_name = {tenant_name}
email = {tenant_email}
access_key = {tenant_access_key}
secret_key = {tenant_secret_key}""".format(
        host=host,
        port=port,
        secure=secure,
        random="{random}",
        main_id=main_user["user_id"],
        main_name=main_user["display_name"],
        main_access_key=main_user["keys"][0]["access_key"],
        main_secret_key=main_user["keys"][0]["secret_key"],
        main_email=main_user["email"],
        alt_id=alt_user["user_id"],
        alt_name=alt_user["display_name"],
        alt_email=alt_user["email"],
        alt_access_key=alt_user["keys"][0]["access_key"],
        alt_secret_key=alt_user["keys"][0]["secret_key"],
        tenant_id=tenant_user["user_id"],
        tenant_name=tenant_user["display_name"],
        tenant_email=tenant_user["email"],
        tenant_access_key=tenant_user["keys"][0]["access_key"],
        tenant_secret_key=tenant_user["keys"][0]["secret_key"],
    )

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
    node = cluster.get_nodes(role="rgw")[0]
    commands = [
        "sed -i -e '$argw_lc_debug_interval = 10' /etc/ceph/ceph.conf",
        "systemctl restart ceph-radosgw.target",
    ]

    if build.startswith("5"):
        node = cluster.get_nodes(role="client")[0]
        rgw_service_name = _get_rgw_service_name(cluster)
        commands = [
            "ceph config set client.rgw.* rgw_lc_debug_interval 10",
            f"ceph orch restart {rgw_service_name}",
        ]

    for cmd in commands:
        node.exec_command(sudo=True, cmd=cmd)


def del_lc_debug(cluster: Ceph, build: str) -> None:
    """
    Modifies the RGW conf files to support lifecycle actions.

    Args:
        cluster:    The cluster participating in the tests.
        build:      The RHCS build version

    Raises:
        CommandFailed:  Whenever a command returns a non-zero value part of the method.
    """
    node = cluster.get_nodes(role="rgw")[0]
    commands = [
        "sed -i -e '$argw_lc_debug_interval = 10' /etc/ceph/ceph.conf",
        "systemctl restart ceph-radosgw.target",
    ]

    if build.startswith("5"):
        node = cluster.get_nodes(role="client")[0]
        rgw_service_name = _get_rgw_service_name(cluster)
        commands = [
            "ceph config rm client.rgw.* rgw_lc_debug_interval",
            f"ceph orch restart {rgw_service_name}",
        ]

    for cmd in commands:
        node.exec_command(sudo=True, cmd=cmd)


# Private functions


def _s3tests_req_install(node: CephNode) -> None:
    """Install S3 prerequisites via pip."""
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
        sudo=True, cmd=f"yum install -y --nogpgcheck {' '.join(packages)}"
    )

    commands = [
        "virtualenv -p python2 --no-site-packages --distribute s3-tests/virtualenv",
        "s3-tests/virtualenv/bin/pip install --upgrade pip setuptools",
        "s3-tests/virtualenv/bin/pip install -r s3-tests/requirements.txt",
        "s3-tests/virtualenv/bin/python s3-tests/setup.py develop",
    ]
    for cmd in commands:
        node.exec_command(cmd=cmd)


def _s3tests_req_bootstrap(node: CephNode) -> None:
    """Install the S3tests using bootstrap script."""
    node.exec_command(cmd="cd s3-tests; ./bootstrap")


def _get_rgw_service_name(cluster: Ceph) -> str:
    """Return the RGW service name."""
    node = cluster.get_nodes(role="client")[0]
    cmd_ = "ceph orch ls rgw --format json"
    out, err = node.exec_command(sudo=True, cmd=cmd_)

    json_out = json.loads(out.read().decode().strip())
    return json_out[0]["service_name"]
