"""
Test Module to test Java S3tests suite.
Steps to test this suite are described here https://github.com/ceph/java_s3tests/blob/master/README.md
- We will clone the suite
- We will create the necessary rgw config to test
- And then we will execute the tests
Polarion ID: CEPH-83586289

Requirement parameters
    config:     The test configuration
    branch      the s3test branch to be used.

Entry Point:
    def run(**args):
"""
import binascii
import json
import os
from json import loads
from typing import Dict, Optional, Tuple

from jinja2 import Template

from ceph.ceph import Ceph, CephNode
from utility.log import Log

log = Log(__name__)
S3CONF = """bucket_prefix : test-

s3main :
  access_key : {{ data.user1.access_key }}
  access_secret : {{ data.user1.secret_key }}
  region : us
  endpoint : {{ data.endpoint }}
  port : {{ data.port }}
  display_name : {{ data.user1.name }}
  email : {{ data.user1.email }}
  is_secure : {{ data.secure }}
  SSE : AES256

  s3dir : folder
  dir : data
"""


def run(**kw):
    log.info("Running Java S3tests suite")

    cluster = kw["ceph_cluster"]
    config = kw.get("config")
    host = config.get("host")

    client_node = cluster.get_nodes(role="client")[0]

    execute_setup(cluster, config)

    if not host:
        _, secure, _ = get_rgw_frontend(cluster)

    exit_status = execute_s3_tests(client_node, config)
    execute_teardown(cluster)

    log.info("Returning status code of %s", exit_status)
    return exit_status


def execute_setup(cluster: Ceph, config: dict) -> None:
    """
    Execute the prerequisites required to run the tests.

    It involves the following steps
        - install the required software (radosgw for CLI execution)
        - Clone the S3 tests repo in the client node
    Args:
        cluster: Ceph cluster participating in the test.
        config:  The key/value pairs passed by the tester.

    Raises:
        CommandFailed:  When a remote command returned non-zero value.
    """
    secure = config.get("ssl", False)
    host = config.get("host")
    port = config.get("port")

    client_node = cluster.get_nodes(role="client")[0]
    rgw_node = cluster.get_nodes(role="rgw")[0]

    clone_java_tests(node=client_node)
    install_requirements(client_node)

    if not all([host, port]):
        host = rgw_node.shortname
        lib, secure, port = get_rgw_frontend(cluster)

    kms_keyid = config.get("kms_keyid")
    create_s3_conf(cluster, host, port, secure, kms_keyid)


def execute_s3_tests(node: CephNode, config) -> int:
    """
    Return the result of S3 test run.

    Args:
        node        The node from which the test execution is triggered.
    Returns:
        0 - Success
        1 - Failure
    """
    log.info("Executing java s3tests")
    cmd = "cd /home/cephuser/java_s3tests; /opt/gradle/gradle/bin/gradle clean test"
    out, err = node.exec_command(cmd=cmd, sudo=True, check_ec=False)
    log.info(out)
    # Encryption failures are expected when SSL is false
    encrypt_failure = False
    for line in out:
        if "FAILED" in line and "testEncrypt" in line:
            encrypt_failure = True
        elif "FAILED" in line and "testEncrypt" not in line:
            log.info("Failure not related to encryption observed")
            return 1
    if encrypt_failure and config.get("ssl"):
        log.info("encryption failures are not expected when run with SSL")
        return 1
    return 0


def execute_teardown(cluster: Ceph) -> None:
    """
    Execute the test teardown phase.
    """
    command = "sudo rm -rf java_s3tests"

    node = cluster.get_nodes(role="client")[0]
    node.exec_command(cmd=command)


def create_s3_conf(
    cluster: Ceph,
    host: str,
    port: str,
    secure: bool,
    kms_keyid: Optional[str] = None,
) -> None:
    """
    Generate the S3 Conf for test execution.

    Args:
        cluster     The cluster participating in the test
        host        The RGW hostname to be set in the conf
        port        The RGW port number to be used in the conf
        secure      If the connection is secure or unsecure.
    """
    log.info("Creating the S3 Config file")
    rgw_node = cluster.get_nodes(role="rgw")[0]
    if secure:
        sec = "https"
    else:
        sec = "http"
    rgw_endpoint = f"{sec}://{rgw_node.shortname}:{port}"
    data = dict(
        {"host": host, "port": int(port), "secure": secure, "endpoint": rgw_endpoint}
    )

    client_node = cluster.get_nodes(role="client")[0]

    create_s3_user(node=client_node, data=data)
    if kms_keyid:
        data["kms_keyid"] = kms_keyid

    templ = Template(S3CONF)
    _config = templ.render(data=data)

    conf_file = client_node.remote_file(
        file_name="java_s3tests/config.properties", file_mode="w"
    )
    conf_file.write(_config)
    conf_file.flush()


def clone_java_tests(node: CephNode) -> None:
    """Clone the Java S3tests repository on the given node."""
    repo_url = "https://github.com/ceph/java_s3tests.git"
    node.exec_command(cmd="if test -d java_s3tests; then sudo rm -r java_s3tests; fi")
    node.exec_command(cmd=f"git clone {repo_url}")


def install_requirements(node: CephNode) -> None:
    """Install prerequisites for running the suite"""

    node.exec_command(cmd="cd /home/cephuser/java_s3tests; ./bootstrap.sh")
    node.exec_command(cmd="export PATH=/opt/gradle/gradle-6.0.1/bin:$PATH")


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


def create_s3_user(node: CephNode, data: Dict) -> None:
    """
    Create a S3 user with the given display_name.

    The other required information for creating a user is auto generated.

    Args:
        node: node in the cluster to create the user on
        data: a reference to the payload that needs to be updated.

    Returns:
        user_info dict
    """
    uid = binascii.hexlify(os.urandom(32)).decode()
    display_name = "Test-user"
    log.info(f"Creating user: {display_name}")

    cmd = f"radosgw-admin user create --uid={uid} --display_name={display_name}"
    cmd += " --email={email}@foo.bar".format(email=uid)

    out, err = node.exec_command(sudo=True, cmd=cmd)
    user_info = json.loads(out)

    data["user1"] = {
        "id": user_info["keys"][0]["user"],
        "access_key": user_info["keys"][0]["access_key"],
        "secret_key": user_info["keys"][0]["secret_key"],
        "name": user_info["display_name"],
        "email": user_info["email"],
    }
