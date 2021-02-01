import logging

from ceph.ceph_admin import CephAdmin

log = logging.getLogger(__name__)

OP_MAP = {
    "apply": {
        "mon": "apply_mon",
        "mgr": "apply_mgr",
        "osd": "apply_osd",
        "mds": "apply_mds",
        "rgw": "apply_rgw",
        "iscsi": "apply_iscsi",
        "node-exporter": "apply_node_exporter",
        "prometheus": "apply_prometheus",
        "alert-manager": "apply_alert_manager",
        "grafana": "apply_grafana",
        "nfs": "apply_nfs",
    },
    "add": {
        "mon": "daemon_add_mon",
        "mgr": "daemon_add_mgr",
        "osd": "daemon_add_osd",
        "mds": "daemon_add_mds",
        "rgw": "daemon_add_rgw",
        "iscsi": "daemon_add_iscsi",
        "node-exporter": "daemon_add_node_exporter",
        "prometheus": "daemon_add_prometheus",
        "alert-manager": "daemon_add_alert_manager",
        "grafana": "daemon_add_grafana",
        "nfs": "daemon_add_nfs",
    },
    "host": {
        "add": "host_add",
        "remove": "host_remove",
        "add_label": "attach_label",
        "remove_label": "remove_label",
        "set_address": "set_address",
    },
}


def run(ceph_cluster, **kw):
    """
    Cephadm Bootstrap, Managing hosts with options and
    full cluster deployment at single call are supported.

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    The test data should be framed as per the below support,

    - For full cluster deployment which includes below tasks and
      returns after completion of cephadm.deploy,
        - Enables ceph compose tool repo
        - SSH keys exchange between nodes
        - Bootstrap cluster
        - Add all Hosts to cluster
        - Add role services/daemons

      for example.,
        - test:
            module: test_cephadm.py
            config:
              deployment: true

    - Bootstrap cluster with default or custom image and
      returns after cephadm.bootstrap. To use default image, set 'registry'.

      for example.,
        - test:
            module: test_cephadm.py
            config:
              registry: false
              bootstrap: true

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
               module: test_cephadm.py
               config:
                 host_ops:
                   host_add:
                     nodes: ['node2']      # Node list
                     add_label: true       # Set to attach labels to Node
                     attach_address: true  # Set to attach Node address

    """
    log.info("Running test")
    log.info("Running cephadm test")
    config = kw.get("config")
    test_data = kw.get("test_data")
    ceph_cluster.custom_config = test_data.get("custom-config")
    ceph_cluster.custom_config_file = test_data.get("custom-config-file")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    if config.get("skip_setup") is True:
        log.info("Skipping setup of ceph cluster")
        return 0

    # Get CephAdmin object
    cephadm = CephAdmin(cluster=ceph_cluster, **config)

    # Deployment-only option which deploys whole cluster
    if config.get("deployment", False):
        cephadm.deploy()
        return 0

    # Execute cephadm shell commands
    if config.get("exec_shell", False):
        commands = config.get("exec_shell")
        ceph_installer = ceph_cluster.get_ceph_object("installer")
        for cmd in commands:
            cmd = cmd if isinstance(cmd, list) else [cmd]
            cephadm.shell(
                remote=ceph_installer,
                args=cmd,
            )
        return 0

    # Bootstrap cluster
    if config.get("bootstrap", False):
        cephadm.bootstrap()

    # Manage hosts
    if config.get("host_ops"):
        host_ops = config.get("host_ops")
        assert isinstance(host_ops, dict)

        for op, config in host_ops.items():

            hosts = config.pop("nodes", list())
            hosts = hosts if isinstance(hosts, list) else [hosts]

            # empty list wll pick all cluster nodes
            if not hosts:
                hosts = ceph_cluster.get_nodes()
            else:
                hosts = [
                    node
                    for node in ceph_cluster.get_nodes()
                    for name in hosts
                    if name in node.shortname
                ]

            config["nodes"] = hosts
            try:
                func = getattr(cephadm, op)
                func(**config)
            except AttributeError:
                raise NotImplementedError(f"Cls HostMixin Not implemented {op}")
    return 0
