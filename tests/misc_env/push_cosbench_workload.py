"""
This module contains the workflows for creating and pushing workloads to cosbench

Sample test script

    - test:
        abort-on-fail: true
        clusters:
          ceph-pri:
            config:
              controllers:
                - node5
              drivers: # if drivers are not specified will use one of the rgw node
                - node5
                - node6
              fill_percent: 30
              record_sync_on_site: ceph-sec
              record_sync_max_duration: 6 # in hours
              record_sync_sleep_time: 15 # in minutes
        desc: prepare and push cosbench fill workload
        module: push_cosbench_workload.py
        name: push cosbench fill workload
"""
import json
import math
import time
from datetime import datetime

from ceph.utils import get_nodes_by_ids
from tests.misc_env.cosbench import get_or_create_user
from utility import utils
from utility.log import Log

LOG = Log(__name__)


fill_workload = """<?xml version="1.0" encoding="UTF-8" ?>
<workload name="fillCluster-s3" description="RGW testing">
<!-- Initialization -->
  <storage type="s3" config="timeout=900000;accesskey=x;secretkey=y;endpoint=workload_endpoint;path_style_access=true"/>
  <auth type="none"/>
  <workflow>

<!-- Initialization -->
    <workstage name="init_containers">
        <work type="init" workers="1" config="cprefix=pri-bkt;containers=r(1,6)"/>
    </workstage>

    <workstage name="preparing_cluster">
        <work type="prepare" workers="1" config="cprefix=pri-bkt;containers=r(1,6);oprefix=pri-obj;
        objects=r(1,objects_count);sizes=h(1|5|25,5|50|40,50|256|25,256|512|5,512|1024|3,1024|5120|1,5120|51200|1)KB"/>
    </workstage>
  </workflow>
</workload>"""

avail_storage = 0
workload_file_id = ""
bucket_name_prefix = ""


def prepare_fill_workload(ceph_cluster, client, rgw, controller, config):
    """
    preparing cosbench workload from the template

    Args:
        ceph_cluster:   Cluster participating in the test.
        client: client node
        rgw: rgw node
        controller: controller node
        config: config from test

    Returns:
        workload xml file name which is created on controller node
    """
    global fill_workload, avail_storage, workload_file_id, bucket_name_prefix
    keys = get_or_create_user(client)
    fill_workload = fill_workload.replace(
        "accesskey=x", f"accesskey={keys['access_key']}"
    )
    fill_workload = fill_workload.replace(
        "secretkey=y", f"secretkey={keys['secret_key']}"
    )

    avail_storage = utils.calculate_available_storage(client)
    LOG.info(f"Total available storage: {avail_storage}")
    fill_percent = config.get("fill_percent", 30)
    bytes_to_fill = avail_storage / 100 * fill_percent
    LOG.info(f"no of bytes to fill {fill_percent} percent: {bytes_to_fill}")
    # these bytes have to be filled in 6 buckets, so finding bytes per bucket
    bytes_to_fill = bytes_to_fill / 6
    # 404.56 KB is the average size according to sizes range in workload
    # using the average size to find number of objects
    objects_count = math.floor(bytes_to_fill * 100 / (40456 * 1024))
    LOG.info(f"no of objects for an average of sizes in workload: {objects_count}")
    fill_workload = fill_workload.replace("objects_count", f"{objects_count}")

    workload_endpoint = "http://localhost:5000"
    if not config.get("drivers"):
        ip = rgw.ip_address
        out, err = rgw.exec_command(
            sudo=True, cmd="ceph orch ls --format json --service-type rgw"
        )
        rgw_service = json.loads(out)
        port = rgw_service[0]["status"]["ports"][0]
        workload_endpoint = f"http://{ip}:{port}"
    LOG.info(f"workload endpoint: {workload_endpoint}")
    fill_workload = fill_workload.replace("workload_endpoint", workload_endpoint)

    out, err = controller.exec_command(
        cmd="sh /opt/cosbench/cli.sh info | grep drivers | awk '{print $2}'"
    )
    LOG.info(out)
    drivers_count = int(out.strip())
    workers = drivers_count * 100
    fill_workload = fill_workload.replace(
        'work type="prepare" workers="1"', f'work type="prepare" workers="{workers}"'
    )

    workload_file_id = utils.generate_unique_id(length=4)
    workload_file_name = f"fill-workload-{workload_file_id}.xml"
    bucket_name_prefix = f"pri-bkt-{workload_file_id.lower()}-"
    fill_workload = fill_workload.replace("pri-bkt", bucket_name_prefix)

    LOG.info(fill_workload)
    controller.exec_command(cmd=f"touch {workload_file_name}")
    controller.exec_command(cmd=f"echo '{fill_workload}' > {workload_file_name}")
    return workload_file_name


def get_workload_status(controller, wid):
    """
    get workload status by its id
    like PROCESSING, FINISHED, TERMINATED...

    Args:
        controller: controller node
        wid: workload id

    Returns:
        workload status along with other details as a string
    """
    out, err = controller.exec_command(
        cmd=f"curl -d id={wid} 'http://127.0.0.1:19088/controller/cli/workload.action'"
    )
    LOG.info(out)
    return out.strip()


def push_workload(controller, client, workload_file_name):
    """
    push workload to cosbench

    Args:
        controller: controller node
        client: client node
        workload_file_name: workload file name present on controller node

    Returns:
        workload status
    """
    global avail_storage
    out, err = controller.exec_command(
        cmd=f"sh /opt/cosbench/cli.sh submit ~/{workload_file_name}"
    )
    LOG.info(out)
    wid = out.strip().split(": ")[1]

    sleep_time = 30
    retry_limit = 20
    while "PROCESSING" in get_workload_status(controller, wid):
        utils.check_ceph_status(client)
        stored_bytes_prev = utils.get_utilized_space(client)
        LOG.info(
            f"utilized space before sleep of {sleep_time} seconds in bytes:{stored_bytes_prev}"
        )
        for _ in range(retry_limit):
            LOG.info(f"sleeping for {sleep_time} seconds")
            time.sleep(sleep_time)
            stored_bytes_curr = utils.get_utilized_space(client)
            LOG.info(f"utilized space now in bytes:{stored_bytes_curr}")
            if stored_bytes_prev != stored_bytes_curr:
                LOG.info("utilized space changed as expected")
                break
            elif "PROCESSING" not in get_workload_status(controller, wid):
                LOG.info(
                    "Workload is not in PROCESSING state. stopping workload progress check"
                )
                break
            else:
                LOG.info("utilized space not changed, retrying again..")
        else:
            raise Exception(
                f"utilized space not changed even after waiting for {retry_limit * sleep_time} seconds"
            )
    workload_status = get_workload_status(controller, wid)
    if "FINISHED" in workload_status:
        LOG.info("workload completed successfully")
    elif "TERMINATED" in workload_status:
        raise Exception("workload failed")
    elif "CANCELLED" in workload_status:
        raise Exception("workload cancelled")
    elif "Not Found" in workload_status:
        raise Exception("workload id not found")
    else:
        raise Exception(f"workload status: {workload_status}")

    updated_avail_storage = utils.calculate_available_storage(client)
    actual_bytes_filled = avail_storage - updated_avail_storage
    fill_percentage = actual_bytes_filled / avail_storage * 100
    LOG.info(f"Available storage before workload: {avail_storage}")
    LOG.info(f"Available storage after workload: {updated_avail_storage}")
    LOG.info(f"Actual percentage of cluster filled by workload: {fill_percentage} %")
    LOG.info(f"Actual number of bytes filled by workload: {actual_bytes_filled}")
    return workload_status


def record_sync_status(record_sync_site_client, config):
    global workload_file_id, bucket_name_prefix
    LOG.info(
        "creating a file to record sync status and bucket sync status until data is caught up"
    )
    sync_record_file_name = f"sync_record_{workload_file_id}"
    record_sync_site_client.exec_command(cmd=f"touch {sync_record_file_name}")

    record_sync_max_hrs = config.get("record_sync_max_duration", 6)
    record_sync_max_min = record_sync_max_hrs * 60
    sleep_time = config.get("record_sync_sleep_time", 15)
    retry_limit = int(record_sync_max_min / sleep_time)
    LOG.info(
        f"waiting for sync to complete for a maximum of {record_sync_max_hrs} hours with {retry_limit} retries"
        + f" and a sleep of {sleep_time} minutes between each retry"
    )
    for _ in range(retry_limit):
        out = utils.get_sync_status(record_sync_site_client)
        if "data is caught up with source" in out:
            break
        sync_record_file = record_sync_site_client.remote_file(
            file_name=sync_record_file_name, file_mode="a"
        )
        sync_record_file.write(f"\n\n{datetime.now()}\n\n")
        sync_record_file.write(out)
        for bucket_index in range(1, 7):
            sync_record_file.write("\n")
            bucket_name = f"{bucket_name_prefix}{bucket_index}"
            out = utils.get_bucket_sync_status(record_sync_site_client, bucket_name)
            sync_record_file.write(out)
        sync_record_file.flush()
        LOG.info(f"sleeping for {sleep_time} minutes")
        time.sleep(sleep_time * 60)


def run(ceph_cluster, **kwargs) -> int:
    """
    preparing and pushing cosbench workload

    Args:
        ceph_cluster:   Cluster participating in the test.

    Returns:
        0 on Success and raises Exception on Failure.
    """
    LOG.info("preparing and pushing cosbench workload to fill 30% of the cluster")
    clusters = kwargs.get("ceph_cluster_dict")
    config = kwargs["config"]
    controller = get_nodes_by_ids(ceph_cluster, config["controllers"])[0]
    client = ceph_cluster.get_nodes(role="installer")[0]
    rgw = ceph_cluster.get_nodes(role="rgw")[0]
    record_sync_on_site = config.get("record_sync_on_site")

    workload_file_name = prepare_fill_workload(
        ceph_cluster, client, rgw, controller, config
    )
    push_workload(controller, client, workload_file_name)
    if record_sync_on_site:
        record_sync_site = clusters.get(record_sync_on_site)
        record_sync_site_client = record_sync_site.get_nodes(role="installer")[0]
        record_sync_site_client.exec_command(
            cmd="sudo yum install -y --nogpgcheck ceph-common"
        )
        record_sync_status(record_sync_site_client, config)

    LOG.info("Workload completed successfully!!!")
    return 0
