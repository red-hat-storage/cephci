from ceph.ceph_admin.client_keyring import ClientKeyring
from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import get_cluster_state
from utility.log import Log

log = Log(__name__)


CLUSTER_STATE = ["ceph orch client-keyring ls"]


def run(ceph_cluster, **kw):
    """
    Manage Cephadm client-keyring operations,
        - list
        - remove client-keyring
        - set client-keyring
    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    for example.,
            config:
                command: ls | rm | set
                service: client-keyring
                args:
                    format: json-pretty
                base_cmd_args:
                    nodes:
                        - "node3"
    """
    log.info("Running Cephadm client-keyring test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    service = config.pop("service", "")

    log.info("Executing %s %s" % (service, command))

    client_keyring = ClientKeyring(cluster=ceph_cluster, **config)
    try:
        method = fetch_method(client_keyring, command)
        method(config)
    finally:
        # Get cluster state
        get_cluster_state(client_keyring, CLUSTER_STATE)

    return 0
