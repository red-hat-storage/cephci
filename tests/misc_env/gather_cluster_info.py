"""Module that executes workflows required for ODF CI tests."""
import binascii
import os
from json import loads

import yaml

from utility.log import Log

LOG = Log(__name__)


def run(ceph_cluster, ceph_nodes, config, **kwargs) -> int:
    """
    Entry point to this module.

    At a high level, the below steps are executed

        - Install the required software packages by ODF CI team
        - Create the RBD pool
        - Retrieve the cluster's admin key
        - Create a RGW user
        - Gather information about the environment.

    Args:
        ceph_cluster:       Instance of Ceph
        ceph_nodes (list):  List of nodes participating in the cluster.
        config (dict):

    Returns:
        0 -> on success
        1 -> on failure

    Raises:
        CommandError
    """
    LOG.info("Gathering details about the system under test.")
    rhbuild = config["rhbuild"]

    installer = ceph_cluster.get_nodes(role="installer")[0]
    base_cmd = "sudo cephadm shell --"
    if rhbuild.startswith("4"):
        base_cmd = "sudo"

    # Install the software packages and
    installer.exec_command(
        sudo=True, cmd="yum install --nogpgcheck -y python-rados python-rbd"
    )

    # RBD pool is created in test_ansible hence the parity needs to be addressed for
    # higher RHCS versions
    if not rhbuild.startswith("4"):
        installer.exec_command(cmd=f"{base_cmd} ceph osd pool create rbd")
        installer.exec_command(cmd=f"{base_cmd} rbd pool init rbd")

    # Get admin key
    cmd = f"{base_cmd} ceph auth get client.admin --format json"
    out, err = installer.exec_command(cmd=cmd)
    cmd_resp = loads(out)
    admin_key = cmd_resp[0]["key"]

    # Create rgw-admin-ops-user
    uid = binascii.hexlify(os.urandom(16)).decode()
    cmd = f"{base_cmd} radosgw-admin user create --uid={uid}"
    cmd += " --display_name='rgw-admin-ops-user'"
    cmd += " --email rgw-admin-ops-user@foo.bar"

    out, err = installer.exec_command(cmd=cmd)
    cmd_resp = loads(out)
    access_key = cmd_resp["keys"][0]["access_key"]
    secret_key = cmd_resp["keys"][0]["secret_key"]

    sut_details = dict(
        {
            "access_key_rgw-admin-ops-user": access_key,
            "secret_key_rgw-admin-ops-user": secret_key,
            "login": {"username": "root", "password": ceph_nodes[0].root_passwd},
            "admin_keyring": {"key": admin_key},
            "external_cluster_node_roles": list(),
        }
    )
    for node in ceph_nodes:
        node_info = dict(
            {
                "hostname": node.vmname,
                "ip_address": node.ip_address,
                "roles": list(set(node.role.role_list)),
            }
        )
        sut_details["external_cluster_node_roles"].append(node_info)

    with open("sut.yaml", "w") as fh:
        yaml.dump(sut_details, fh)

    LOG.info("Successfully written the system details.")
    return 0
