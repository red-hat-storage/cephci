"""
This file contains various tests/ validations related to brownfield deployment.
Tests included:
1. Verification of upgraded version as reported by prometheus

"""

import json

from api import Api
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)

HEADER = {
    "Accept": "application/vnd.ceph.api.v1.0+json",
    "Content-Type": "application/json",
}


def run(ceph_cluster, **kw):
    """
    Performs various validation tests for brownfield deployment
    Returns:
        1 -> Fail, 0 -> Pass

    Current coverage:
        - CEPH-83581346: Prometheus metrics verification
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    prom_node = ceph_cluster.get_nodes(role="prometheus")[0]

    if config.get("prometheus_metrics"):
        doc = """
        # CEPH-83581346
        # BZ - 2008524
        Post cluster upgrade -
        1. Fetch ceph version from installer node
        2. Get daemon metadata metrics from Prometheus API
        3. Check whether cluster upgraded version is correctly reflected
        in prometheus output
        """
        log.info(doc)
        out, _ = cephadm.shell(["ceph version"])
        ceph_version = out.strip()
        log.info(f"Ceph version: {ceph_version}")

        try:
            # Get prometheus api host
            url, _ = cephadm.shell(["ceph dashboard get-prometheus-api-host"])
            log.info(f"Promethues api host: {url}")
            url = f"http://{prom_node.ip_address}:9095"

            daemon_list = config["prometheus_metrics"].get(
                "daemons", ["mon", "mgr", "osd"]
            )
            for daemon in daemon_list:
                log.info(
                    f"Verification start for Ceph version from Prometheus metric for {daemon}"
                )
                api_obj = Api(url=url, api=f"api/v1/query?query=ceph_{daemon}_metadata")
                response = api_obj.get(header=HEADER)
                metadata_dict = response.json()

                metric_list = metadata_dict["data"]["result"]
                log.info(f"Metric list for {daemon}:")
                log.info(json.dumps(response.json(), indent=4, sort_keys=False))

                # begin comparison of prometheus metadata with local metadata from the cluster
                log.info(
                    "Begin comparison of prometheus metadata with local metadata from the cluster"
                )
                for metric in metric_list:
                    data = metric["metric"]
                    daemon_name = data["ceph_daemon"]
                    log.info(f"comparison begun for {daemon_name}")
                    _daemon_id = ".".join(daemon_name.split(".")[1:])
                    local_metadata = rados_obj.get_daemon_metadata(
                        daemon_type=daemon, daemon_id=_daemon_id
                    )
                    log.info(
                        f"Cluster fetched metadata for {daemon_name}: \n {local_metadata}"
                    )
                    if (
                        local_metadata["ceph_version"] != data["ceph_version"]
                        or data["ceph_version"] != ceph_version
                    ):
                        log.error(
                            f"(Version mismatch between prometheus metric and local metadata for {daemon_name}"
                        )
                        return 1
                log.info(
                    f"Verification complete for {daemon}, prometheus report"
                    f" correct upgraded version for all {daemon} daemons"
                )
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        return 0
