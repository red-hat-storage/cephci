from cli.cephadm.cephadm import CephAdm


class RemoveServiceError(Exception):
    pass


def run(ceph_cluster, **kw):
    """Remove labels and host from cluster
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
        e.g
        test:
            name: Remove RHEL 8 installers and labels from cluster
            module: test_remove_labels_installer.py
            config:
            label:
                - installer
                - _admin
            desc: Remove RHEL 8 installers and labels from cluster
    """
    config = kw.get("config")
    node = ceph_cluster.get_nodes(role="mon")[0]
    host_name = [host.hostname for host in ceph_cluster.get_nodes()][0]
    labels = config.get("label")
    exp_out = "Removed label"
    # Remove label for host
    for label in labels:
        result = CephAdm(node).ceph.orch.label.rm(host_name, label)
        if exp_out not in result:
            raise RemoveServiceError("Fail to remove label")
    # Remove installer from cluster
    ceph_cluster.__delitem__(0)
    return 0
