import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.rados_test_util import (
    create_pools,
    get_slow_requests_log,
    write_to_pools,
)
from utility.log import Log
from utility.utils import should_not_be_empty

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Verify slow op requests
    - Capture the node time to start reading the logs - format %Y-%m-%d %H:%M:%S
    - Add pools to the cluster and write data to these pools
    - Capture the node time to stop reading the logs - format %Y-%m-%d %H:%M:%S
    - Query slow requests in the logs for the given time interval
    """
    try:
        log.info(run.__doc__)
        config = kw["config"]
        cephadm = CephAdmin(cluster=ceph_cluster, **config)
        rados_obj = RadosOrchestrator(node=cephadm)
        client_node = ceph_cluster.get_nodes(role="client")[0]
        installer = ceph_cluster.get_nodes(role="installer")[0]

        log.info("Running slow op requests tests")
        rados_obj.enable_file_logging()
        installer.exec_command(cmd="sudo timedatectl set-timezone UTC")
        start_time, err = installer.exec_command(
            cmd="sudo date -u '+%Y-%m-%d %H:%M:%S'"
        )

        pool = create_pools(config, rados_obj, client_node)
        should_not_be_empty(pool, "Failed to retrieve pool details")
        write_to_pools(config, rados_obj, client_node)
        rados_obj.change_recover_threads(config=pool, action="set")

        end_time, err = installer.exec_command(cmd="sudo date -u '+%Y-%m-%d %H:%M:%S'")
        logs = get_slow_requests_log(installer, start_time.strip(), end_time.strip())
        log.info(f"logs --- {logs}")

        return 0
    except Exception as e:
        log.info(e)
        log.error(traceback.format_exc())
        return 1
