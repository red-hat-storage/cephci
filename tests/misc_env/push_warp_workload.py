"""
This module contains the workflows for pushing WARP workload to its client.
     It supports
    - Only write data into cluster
    - [RFE]Perform mixed operations such as read write delete and list object

Sample test script

    - test:
        abort-on-fail: true
        config:
          duration: 60m
          obj_size:256KiB
          operation: put/mixed
          bucket_count: 5
          warp_server:
            - node6
          haproxy_client:
            - node6
            - node7
          warp_client:
              - node6
              - node7
        desc: Write data into cluster
        module: push_warp_workload.py
        name: Configure WARP
"""

from datetime import date
from typing import List

from ceph.ceph import Ceph, CephNode
from ceph.utils import get_nodes_by_ids
from tests.misc_env.configure_warp import get_or_create_user
from utility.log import Log

LOG = Log(__name__)


def push_io(
    warp_server: List[CephNode],
    client_nodes: List[CephNode],
    ha_nodes: List[CephNode],
    client,
    bucket_count,
    run_duration,
    warp_operation,
) -> None:
    """
    Push WARP workload to its clients.

    Args:
        nodes (list):   The list of nodes on which the packages are installed.

    Returns:
        None

    Raises:
        CommandFailed
    """
    keys = get_or_create_user(client)
    access_key = keys["access_key"]
    secret_key = keys["secret_key"]
    current_date = date.today()
    date_number = int(current_date.strftime("%Y%m%d"))

    for bkt in range(bucket_count):
        bucket_name = bkt + 1
        cli = (
            f"nohup warp {warp_operation} --obj.size 256KiB --duration={run_duration} --concurrent=1 --obj.randsize "
            + "--host-select=roundrobin --noclear --no-color"
        )
        if warp_operation == "mixed":
            cli = (
                cli
                + " --put-distrib 35 --delete-distrib 5 --get-distrib 45 --stat-distrib 15"
            )

        cli_list = ""
        stop_flag = len(client_nodes)
        i = 0
        for node in client_nodes:
            port = 7000 + bucket_name
            cli_list = cli_list + str(node.ip_address) + ":" + str(port)
            if i != stop_flag - 1:
                cli_list = cli_list + ","
            i += 1
            port += 1
        cli = cli + " --warp-client=" + cli_list
        host_list = ""
        stop_flag = len(ha_nodes)
        i = 0
        for node in ha_nodes:
            port = 5000
            host_list = host_list + str(node.ip_address) + ":" + str(port)
            if i != stop_flag - 1:
                host_list = host_list + ","
            i += 1
        cli = cli + " --host=" + host_list
        cli = (
            cli
            + " --access-key="
            + access_key
            + " --secret-key="
            + secret_key
            + " --bucket mybucket-"
            + str(bucket_name)
            + " --benchdata "
            + str(date_number)
            + "_fill_"
            + str(bucket_name)
            + " &> "
            + str(date_number)
            + "_warp"
            + str(bucket_name)
            + "_results.out 2>&1 &"
        )
        LOG.info(cli)
        out, err = warp_server[0].exec_command(sudo=True, cmd=f"{cli}")
    LOG.info("Initiated workload on cluster!!!")


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Entry point to this module that executes the set of workflows.

    Here, Cloud Object Store Benchmark tool (WARP) is pushing workload to its client

    Args:
        ceph_cluster:   Cluster participating in the test.

    Returns:
        0 on Success and 1 on Failure.
    """
    LOG.info("Push workload to warp.")
    client = ceph_cluster.get_nodes(role="installer")[0]
    warp_server = get_nodes_by_ids(ceph_cluster, kwargs["config"]["warp_server"])
    warp_clients = get_nodes_by_ids(ceph_cluster, kwargs["config"]["warp_client"])
    ha_clients = get_nodes_by_ids(ceph_cluster, kwargs["config"]["haproxy_client"])
    run_duration = kwargs["config"].get("duration", "1m")
    warp_operation = kwargs["config"].get("operation", "put")
    bucket_count = kwargs["config"].get("bucket_count", 6)

    try:
        bucket_count = kwargs["config"].get("bucket_count", 6)
        push_io(
            warp_server,
            warp_clients,
            ha_clients,
            client,
            bucket_count,
            run_duration,
            warp_operation,
        )

    except BaseException as be:  # noqa
        LOG.error(be)
        return 1
    LOG.info("Workload initited successfully!!!")
    return 0
