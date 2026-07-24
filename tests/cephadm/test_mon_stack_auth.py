from api.cephapi.auth.auth import Auth
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.ops.cephadm import is_dashboard_enabled, is_prometheus_enabled
from utility.log import Log

log = Log(__name__)

HEADER = {
    "Accept": "application/vnd.ceph.api.v1.0+json",
    "Content-Type": "application/json",
}


def run(ceph_cluster, **kw):
    """Verify redeploy for a specific service
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config", {})
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get valid creds to access the mon stack
    username = config.get("username")
    password = config.get("password")

    if not (is_dashboard_enabled(installer) or is_prometheus_enabled(installer)):
        OperationFailedError("Dashboard plug-in not listed under enabled modules")

    # Get existing value of mgr/cephadm/secure_monitoring_stack
    mon_stack_status = CephAdm(installer).ceph.config.get(
        "mgr", "mgr mgr/cephadm/secure_monitoring_stack"
    )

    # If mon stack set to false, enable it
    if "false" in mon_stack_status:
        CephAdm(installer).ceph.config.set(
            daemon="mgr", key="mgr/cephadm/secure_monitoring_stack", value="true"
        )

    # Check if the value is update to true
    if "false" in CephAdm(installer).ceph.config.get(
        "mgr", "mgr mgr/cephadm/secure_monitoring_stack"
    ):
        raise OperationFailedError(
            "Failed to set secure_monitoring_stack value to true"
        )

    # Get dashboard api host
    kw = {"get-alertmanager-api-host": ""}
    dashboard = CephAdm(installer).ceph.dashboard(**kw)

    # Get prometheus api host
    kw = {"get-prometheus-api-host": ""}
    prometheus = CephAdm(installer).ceph.dashboard(**kw)

    # Try to access the urls without creds
    for mon_stack in [dashboard, prometheus]:
        log.info(f"url : {mon_stack}")
        try:
            Auth(url=mon_stack, header=HEADER).post(username="test", password="test")
        except Exception:
            log.info(f"Expected! {mon_stack} no accessible without valid credentials")

    # Now access the urls with creds
    for mon_stack in [dashboard, prometheus]:
        try:
            Auth(url=f"{mon_stack}", header=HEADER).post(
                username=username, password=password
            )
        except Exception as e:
            log.error(
                f"Unexpected! Failed to access {mon_stack} with valid credentials. {str(e)}"
            )
    log.info(f"Able to access {mon_stack} with valid credentials")
    return 0
