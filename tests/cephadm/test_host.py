from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import get_cluster_state
from ceph.ceph_admin.host import Host
from utility.log import Log

log = Log(__name__)


CLUSTER_STATE = ["ceph orch host ls -f yaml"]


def run(ceph_cluster, **kw):
    """
    Cephadm Bootstrap, Managing hosts with options and
    full cluster deployment at single call are supported.

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    - Manage host operations like,
        - Add hosts with/without labels and IP address
        - Add/Remove labels to/from existing node
        - Set Address to node.
        - Remove hosts

        host_ops keys are definition names are defined under
        CephAdmin.HostMixin should be used to call that respective method.

        supported definition names for host_ops are host_add, attach_label,
        remove_label, set_address and host_remove.

        for example.,
        - test:
            name: Add host
            desc: Add new host node with IP address
            module: test_host.py
            config:
                service: host
                command: add | remove | label_add | label_remove | set_address
                base_cmd_args:
                  nodes:
                    - "node3"
                  attach_address: true
                  add_label: false

    """
    log.info("Running Cephadm Host test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    service = config.pop("service", "")

    log.info("Executing %s %s" % (service, command))

    host = Host(cluster=ceph_cluster, **config)
    try:
        method = fetch_method(host, command)
        method(config)
    finally:
        # Get cluster state
        get_cluster_state(host, CLUSTER_STATE)

    return 0
