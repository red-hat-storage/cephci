"""Test module that executes the external S3 Tests."""

import binascii
import json
import logging
import os
import time

from ceph.ceph import CephNode, CommandFailed
from ceph.utils import open_firewall_port

log = logging.getLogger(__name__)


def run(**kw):
    log.info("Running s3-tests")
    ceph_nodes = kw.get("ceph_nodes")
    config = kw.get("config")
    build = config.get("build", config.get("rhbuild"))
    test_data = kw.get("test_data")
    client_node = None
    rgw_node = None
    for node in ceph_nodes:
        if node.role == "client":
            client_node = node
            break

    for node in ceph_nodes:
        if node.role == "rgw":
            rgw_node = node
            break

    if client_node:
        if test_data.get("install_version", "").startswith("2"):
            client_node.exec_command(sudo=True, cmd="yum install -y ceph-radosgw")

        setup_s3_tests(client_node, rgw_node, config, build)
        exit_status = execute_s3_tests(client_node, build)
        cleanup(client_node)
        teardown_rgw_conf(rgw_node)

        log.info("Returning status code of {}".format(exit_status))
        return exit_status

    else:
        log.warning("No client node in cluster, skipping s3 tests.")
        return 0


def setup_s3_tests(client_node, rgw_node, config, build):
    """
    Performs initial setup and configuration for s3 tests on client node.

    Args:
        client_node: The node configured with 'client'
        rgw_node: The node configured with 'radosgw'
        config: test configuration
        build: rhcs4 build version

    Returns:
        None

    """
    log.info("Removing existing s3-tests directory if it exists")
    client_node.exec_command(cmd="if test -d s3-tests; then rm -r s3-tests; fi")

    log.info("Cloning s3-tests repository")
    branch = config.get("branch", "ceph-luminous")
    repo_url = "https://github.com/ceph/s3-tests.git"
    client_node.exec_command(
        cmd="git clone -b {branch} {repo_url}".format(branch=branch, repo_url=repo_url)
    )

    if build.startswith("4"):
        pkgs = [
            "python2-virtualenv",
            "python2-devel",
            "libevent-devel",
            "libffi-devel",
            "libxml2-devel",
            "libxslt-devel",
            "zlib-devel",
        ]
        client_node.exec_command(
            cmd="sudo yum install -y {pkgs}".format(pkgs=" ".join(pkgs)), check_ec=False
        )
        commands = [
            "virtualenv -p python2 --no-site-packages --distribute virtualenv",
            "~/virtualenv/bin/pip install --upgrade pip setuptools",
            "~/virtualenv/bin/pip install -r s3-tests/requirements.txt",
            "~/virtualenv/bin/python s3-tests/setup.py develop",
        ]
        for cmd in commands:
            client_node.exec_command(cmd=cmd)
    else:
        log.info("Running bootstrap")
        client_node.exec_command(
            cmd="cd s3-tests; ./bootstrap",
        )

    setup_rgw_conf(rgw_node)
    main_info = create_s3_user(client_node, "main-user")
    alt_info = create_s3_user(client_node, "alt-user", email=True)
    tenant_info = create_s3_user(client_node, "tenant", email=True)

    log.info("Creating configuration file")
    port = "8080"
    s3_config = """
[DEFAULT]
host = {host}
port = {port}
is_secure = no

[fixtures]
bucket prefix = cephuser-{random}-

[s3 main]
user_id = {main_id}
display_name = {main_name}
access_key = {main_access_key}
secret_key = {main_secret_key}
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
secret_key = {tenant_secret_key}
    """.format(
        host=rgw_node.shortname,
        port=port,
        random="{random}",
        main_id=main_info["user_id"],
        main_name=main_info["display_name"],
        main_access_key=main_info["keys"][0]["access_key"],
        main_secret_key=main_info["keys"][0]["secret_key"],
        alt_id=alt_info["user_id"],
        alt_name=alt_info["display_name"],
        alt_email=alt_info["email"],
        alt_access_key=alt_info["keys"][0]["access_key"],
        alt_secret_key=alt_info["keys"][0]["secret_key"],
        tenant_id=tenant_info["user_id"],
        tenant_name=tenant_info["display_name"],
        tenant_email=tenant_info["email"],
        tenant_access_key=tenant_info["keys"][0]["access_key"],
        tenant_secret_key=tenant_info["keys"][0]["secret_key"],
    )

    log.info("s3-tests configuration: {s3_config}".format(s3_config=s3_config))
    config_file = client_node.remote_file(
        file_name="s3-tests/config.yaml", file_mode="w"
    )
    config_file.write(s3_config)
    config_file.flush()

    log.info("Opening port on rgw node")
    open_firewall_port(rgw_node, port=port, protocol="tcp")


def create_s3_user(client_node, display_name, email=False):
    """
    Create an s3 user with the given display_name. The uid will be generated.

    Args:
        client_node: node in the cluster to create the user on
        display_name: display name for the new user
        email: (optional) generate fake email address for user

    Returns:
        user_info dict

    """
    uid = binascii.hexlify(os.urandom(32)).decode()
    log.info("Creating user: {display_name}".format(display_name=display_name))
    cmd = "radosgw-admin user create --uid={uid} --display_name={display_name}".format(
        uid=uid, display_name=display_name
    )
    if email:
        cmd += " --email={email}@foo.bar".format(email=uid)

    out, err = client_node.exec_command(sudo=True, cmd=cmd)
    user_info = json.loads(out.read().decode())

    return user_info


def execute_s3_tests(client_node, build):
    """
    Return the result of S3 test run.

    Args:
        client_node: node to execute tests from
        build: the RH build version

    Returns:
        0 - Success
        1 - Failure
    """
    log.info("Executing s3-tests")
    try:
        base_cmd = "cd s3-tests; S3TEST_CONF=config.yaml ~/virtualenv/bin/nosetests -v "
        if build.startswith("4"):
            extra_args = "-a '!fails_on_rgw,!fails_strict_rfc2616,!encryption'"
            cmd = base_cmd + extra_args
        else:
            extra_args = ["-a '!fails_on_rgw,!lifecycle'"]
            cmd = base_cmd + " ".join(extra_args)
        out, err = client_node.exec_command(cmd=cmd, timeout=3600)
        log.info(out.read().decode())
        log.info(err.read().decode())
        return 0
    except CommandFailed as e:
        log.warning("Received CommandFailed")
        log.warning(e)
        time.sleep(30)
        return 1


def setup_rgw_conf(node: CephNode) -> None:
    """
    Execute the pre-setup workflow on the provided node.

    Below are the steps executed
        - Stop the RadosGW service
        - Add `rgw_lc_debug_interval` with interval as 10 seconds
        - Start the RadosGW service

    Args:
        node:   The node object has the rgw role

    Returns:
        None

    Raises:
        CommandFailed: when any remote command execution has returned a non-zero
    """
    commands = [
        "sudo systemctl stop ceph-radosgw.target",
        "sudo sed -i -e '$argw_lc_debug_interval = 10' /etc/ceph/ceph.conf",
        "sudo systemctl start ceph-radosgw.target",
    ]

    for cmd in commands:
        node.exec_command(cmd=cmd)


def teardown_rgw_conf(node: CephNode) -> None:
    """
    Execute the pre-setup cleanup workflow on the given node.

    Below are the steps executed as part of the cleanup activity
        - Stop the RadosGW service
        - Remove the conf changes
        - Start the RadosGW service

    Args:
        node: The node that has the rgw role

    Returns:
        None

    Raises:
        CommandFailed: when any of the
    """
    commands = [
        "sudo systemctl stop ceph-radosgw.target",
        "sudo sed -i '/rgw_lc_debug_interval/d' /etc/ceph/ceph.conf",
        "sudo systemctl start ceph-radosgw.target",
    ]

    for cmd in commands:
        node.exec_command(cmd=cmd)


def cleanup(client_node):
    """
    Cleanup the test artifacts.

    Args:
        client_node: The node

    Returns:
        None
    """
    log.info("Removing s3-tests directory")
    client_node.exec_command(cmd="rm -r s3-tests virtualenv")
