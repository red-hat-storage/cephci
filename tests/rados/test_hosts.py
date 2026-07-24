"""
Module to Add/remove hosts
"""

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from ceph.rados.utils import get_cluster_timestamp
from ceph.utils import get_node_by_id
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Module to Add/remove hosts
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    serviceability_methods = ServiceabilityMethods(cluster=ceph_cluster, **config)
    nodes = config.get("nodes")

    if config.get("scenario") == "remove_host":
        try:
            for node in nodes:
                host_name = get_node_by_id(ceph_cluster, node).hostname
                log.info("Removing host from the cluster - " + host_name)
                serviceability_methods.remove_custom_host(
                    host_node_name=host_name, print_osd_list=False
                )
                log.info("Removed host from the cluster - " + host_name)
            log.info("Removed hosts from the cluster - ".join(nodes))
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info("*********** Execution of finally block starts ***********")

            # log cluster health
            rados_obj.log_cluster_health()

            # check for crashes after test execution
            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.debug(
                f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
            )
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("Verification of scrub_chunk_max parameter completed")
        return 0

    if config.get("scenario") == "add_host":
        try:
            log.info("Adding hosts to the cluster - ".join(nodes))
            serviceability_methods.add_new_hosts(
                add_nodes=nodes,
                crush_bucket_name=config.get("crush_bucket_name", None),
                crush_bucket_type=config.get("crush_bucket_type", None),
                crush_bucket_val=config.get("crush_bucket_val", None),
                deploy_osd=config.get("deploy_osd", True),
                osd_label=config.get("osd_label", "osd-bak"),
            )
            log.info("Successfully added hosts to the cluster - ".join(nodes))
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info("*********** Execution of finally block starts ***********")

            # log cluster health
            rados_obj.log_cluster_health()

            # check for crashes after test execution
            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.debug(
                f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
            )
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("Verification of scrub_chunk_max parameter completed")
        return 0
