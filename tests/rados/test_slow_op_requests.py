import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.rados_test_util import create_pools, get_slow_requests_log
from utility.log import Log
from utility.utils import method_should_succeed, should_not_be_empty

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Verify slow op requests
    - Capture the node time to start reading the logs - format %Y-%m-%d %H:%M:%S
    - Add pools to the cluster and write data to these pools
    - Capture the node time to stop reading the logs - format %Y-%m-%d %H:%M:%S
    - Query slow requests in the logs for the given time interval
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    installer = ceph_cluster.get_nodes(role="installer")[0]

    try:
        log.info("Running slow op requests tests")
        rados_obj.enable_file_logging()
        installer.exec_command(cmd="sudo timedatectl set-timezone UTC")
        start_time, err = installer.exec_command(
            cmd="sudo date -u '+%Y-%m-%d %H:%M:%S'"
        )
        osd_nodes = ceph_cluster.get_nodes(role="osd")

        pool = create_pools(config, rados_obj, client_node)
        should_not_be_empty(pool, "Failed to retrieve pool details")
        pools = config.get("create_pools")
        for each_pool in pools:
            cr_pool = each_pool["create_pool"]
            osd_services = rados_obj.list_orch_services(service_type="osd")
            for service in osd_services:
                client_node.exec_command(cmd=f"ceph orch restart {service}", sudo=True)
            for node in osd_nodes:
                log.info(f"Rebooting node : {node.hostname}")
                node.exec_command(sudo=True, cmd="reboot", check_ec=False)
            method_should_succeed(rados_obj.bench_write, **cr_pool)
        rados_obj.change_recovery_threads(config=pool, action="set")

        end_time, err = installer.exec_command(cmd="sudo date -u '+%Y-%m-%d %H:%M:%S'")
        logs = get_slow_requests_log(installer, start_time.strip(), end_time.strip())
        log.info(f"logs --- {logs}")

        return 0
    except Exception as e:
        log.info(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        rados_obj.change_recovery_threads(config=pool, action="rm")

        # removal of rados pools
        rados_obj.rados_pool_cleanup()

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
