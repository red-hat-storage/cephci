import json

from ceph.utils import get_nodes_by_ids
from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from utility.log import Log

log = Log(__name__)


class IncorrectIscsiServiceStatus(Exception):
    pass


def _verify_service(node, present=True):
    """
    Verifies iscsi service is deployed or removed
    Args:
        node (str): monitor node object
        should_be_present (bool): validation option for iscsi
    return: (bool)
    """
    # Get iscsi service status
    out = CephAdm(node).ceph.orch.ls(service_type="iscsi", format="json-pretty")

    # If ISCSI was deployed
    if present:
        service_details = json.loads(out)[0]
        return (
            False
            if "service was created" not in (service_details["events"][0])
            else True
        )
    else:
        # If ISCSI was removed, wait for a timeout to check whether its removed
        timeout, interval = 20, 2
        for w in WaitUntil(timeout=timeout, interval=interval):
            out = CephAdm(node).ceph.orch.ls(service_type="iscsi", format="json-pretty")
            if "No services reported" in out:
                return True
        if w.expired:
            log.info("Service iscsi is not removed after rm operation.")
    return False


def run(ceph_cluster, **kw):
    """
    Verifies multiple deployment and removal of ISCSI services

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data
            e.g:
            test:
              name: Deploy and Remove ISCSI Service multiple times
              desc: Deploys and remove ISCSI multiple times to verify no crash
              module: test_iscsi_multiple_removal_deployment.py
              polarion-id: CEPH-83575352
              config:
                command: apply
                service: iscsi
                base_cmd_args: # arguments to ceph orch
                  verbose: true
                pos_args:
                  - iscsi                 # name of the pool
                  - api_user              # name of the API user
                  - api_pass              # password of the api_user.
                args:
                  trusted_ip_list: #it can be used both as keyword/positional arg in 5.x
                    - node1
                    - node6
                  placement:
                    nodes:
                      - node1
                      - node6                   # either label or node.
              destroy-cluster: false
              abort-on-fail: true
    """
    config = kw.get("config")

    node = ceph_cluster.get_nodes(role="mon")[0]
    conf = {}  # Fetching required data from the test config

    # Get trusted-ip details
    args = config.get("args")
    trusted_ip_list = args.get("trusted_ip_list")
    if trusted_ip_list:
        node_ips = get_nodes_by_ids(ceph_cluster, trusted_ip_list)
        conf["trusted_ip_list"] = repr(" ".join([node.ip_address for node in node_ips]))

    # Get placement details
    conf["pos_args"] = config.get("pos_args")
    placement = args.get("placement")
    if placement:
        nodes = placement.get("nodes")
        node_ips = get_nodes_by_ids(ceph_cluster, nodes)
        conf["placement="] = repr(" ".join([node.hostname for node in node_ips]))

    # Deploy and Remove ISCSI 3 times
    for _ in range(3):
        log.info("Deploying ISCSI service")
        CephAdm(node).ceph.orch.apply(service_name="iscsi", **conf)

        # Verify if the service is deployed successfully
        if not _verify_service(node):
            raise IncorrectIscsiServiceStatus(
                "Error - ISCSI service is not running after being deployed"
            )
        log.info("ISCSI service is deployed successfully")

        # Remove ISCSI service
        log.info("Removing ISCSI service")
        CephAdm(node).ceph.orch.rm(service_name="iscsi.iscsi")

        # Verify if iscsi is removed
        if not _verify_service(node, False):
            raise IncorrectIscsiServiceStatus("Error - ISCSI service is not removed.")
        log.info("ISCSI service is removed successfully")

    return 0
