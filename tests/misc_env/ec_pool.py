"""
This module contains the workflows for configuring EC pool and create pool with defined pg count

Sample test script

- test:
    abort-on-fail: true
    config:
    data_pg_count: 8192
    index_pg_count: 512
    k_value_for_ec: 4
    m_value_for_ec: 2
    desc: Create pool with EC profile
    module: ec_pool.py
    name: Create pool with EC profile
"""
import json
import time

from ceph.ceph import Ceph
from utility.log import Log

LOG = Log(__name__)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Entry point to this module that configures EC pool.
    """
    LOG.info("Creating EC pools")
    client = ceph_cluster.get_ceph_object("installer")
    try:
        data_pg_count = kwargs["config"]["data_pg_count"]
        index_pg_count = kwargs["config"]["index_pg_count"]
        k = kwargs["config"]["k_value_for_ec"]
        m = kwargs["config"]["m_value_for_ec"]
        out, _ = client.exec_command(cmd="sudo radosgw-admin zone get --format json")
        out = json.loads(out)
        zone_name = out["name"]
        dt_pl = f"{zone_name}.rgw.buckets.data"
        idx_pl = f"{zone_name}.rgw.buckets.index"
        client.exec_command(
            cmd="sudo ceph tell mon.\\* injectargs '--mon-allow-pool-delete=true'"
        )
        client.exec_command(
            cmd=f"sudo ceph osd pool delete {dt_pl} {dt_pl} --yes-i-really-really-mean-it"
        )
        client.exec_command(
            cmd=f"sudo ceph osd pool delete {idx_pl} {idx_pl} --yes-i-really-really-mean-it"
        )
        client.exec_command(cmd=f"sudo ceph osd crush rule rm {dt_pl}")
        client.exec_command(cmd="sudo ceph osd erasure-code-profile rm myprofile")
        client.exec_command(
            cmd=f"sudo ceph osd erasure-code-profile set myprofile k={k} m={m}"
        )
        client.exec_command(
            cmd=f"sudo ceph osd crush rule create-erasure {dt_pl} myprofile"
        )
        client.exec_command(cmd=f"sudo ceph osd pool create {dt_pl} erasure myprofile")
        client.exec_command(
            cmd=f"sudo ceph osd pool set {dt_pl} pg_num {data_pg_count}"
        )
        client.exec_command(
            cmd=f"sudo ceph osd pool set {dt_pl} pg_num_min {data_pg_count}"
        )
        client.exec_command(
            cmd=f"sudo ceph osd pool set {dt_pl} pgp_num {data_pg_count}"
        )
        client.exec_command(cmd=f"sudo ceph osd pool create {idx_pl} replicated")
        client.exec_command(
            md=f"sudo ceph osd pool set {idx_pl} pg_num {index_pg_count}"
        )
        client.exec_command(
            cmd=f"sudo ceph osd pool set {idx_pl} pg_num_min {index_pg_count}"
        )
        client.exec_command(
            cmd=f"sudo ceph osd pool set {idx_pl} pgp_num {index_pg_count}"
        )
        out, _ = client.exec_command(
            cmd=f"sudo ceph df | grep {dt_pl} | awk '{{print $3}}'"
        )
        current_pg = int(out)
        retries = 0
        while current_pg < data_pg_count:
            time.sleep(10)
            out, _ = client.exec_command(
                cmd=f"sudo ceph df | grep {dt_pl} | awk '{{print $3}}'"
            )
            current_pg = int(out)
            retries += 1
            if retries == 500:
                raise AssertionError(f"Failed to create {dt_pl} pool with required PGs")
        out, _ = client.exec_command(
            cmd=f"sudo ceph df | grep {idx_pl} | awk '{{print $3}}'"
        )
        current_idx_pg = int(out)
        retries = 0
        while current_idx_pg < index_pg_count:
            time.sleep(10)
            out, _ = client.exec_command(
                cmd=f"sudo ceph df | grep {idx_pl} | awk '{{print $3}}'"
            )
            current_pg = int(out)
            retries += 1
            if retries == 500:
                raise AssertionError(
                    f"Failed to create {idx_pl} pool with required PGs"
                )

    except BaseException as be:  # noqa
        LOG.error(be)
        return 1

    LOG.info("Successfully created EC pools!!!")
    return 0
