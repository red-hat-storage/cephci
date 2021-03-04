import logging

from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.host import Host

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Cephadm Bootstrap, Managing hosts with options and
    full cluster deployment at single call are supported.

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    The test data should be framed as per the below support,

    - Bootstrap cluster with default or custom image and
      returns after cephadm.bootstrap. To use default image, set 'registry'.

        Example:
            config:
                command: bootstrap
                base_cmd_args:
                    verbose: true
                args:
                    custom_image: true | false
                    mon-ip: <node_name>
                    mgr-id: <mgr_id>
                    fsid: <id>

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
            module: test_cephadm.py
            config:
                command: host
                service: add | remove | label_add | label_remove | set_address
                base_cmd_args:
                  nodes:
                    - "node3"
                  attach_address: true
                  add_label: false

    """
    log.info("Running cephadm test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    if config.get("skip_setup") is True:
        log.info("Skipping setup of ceph cluster")
        return 0

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    service = config.pop("service", "")

    log.info("Executing %s %s" % (service, command))

    if command in CephAdmin.direct_calls:
        instance = CephAdmin(cluster=ceph_cluster, **config)
        method = fetch_method(instance, command)
    elif service in Host.SERVICE_NAME:
        instance = Host(cluster=ceph_cluster, **config)
        method = fetch_method(instance, command)
    else:
        raise NotImplementedError

    if "shell" in command:
        method(args=config["args"])
    else:
        method(config)

    if "get_cluster_details" in config:
        instance.get_cluster_state(config["get_cluster_details"])

    return 0
