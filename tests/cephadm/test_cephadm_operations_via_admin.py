import os

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm


class CephadmOperationsAdmin(Exception):
    pass


def run(ceph_cluster, **kw):
    """Verify cephadm operations via admin node for managing and expanding cluster
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
        e.g
        test:
            name: test_cephadm_operations_via_admin_node
            desc: Verify cephadm operations via admin node for managing and expanding cluster
            config:
                label: _admin
                files:
                - /etc/ceph/ceph.client.admin.keyring
                - /etc/ceph/ceph.conf
            polarion-id: CEPH-83573725
            module: test_cephadm_operations_via_admin.py
    """
    config = kw.get("config")
    label = config.get("label")
    if not label:
        raise CephadmOperationsAdmin("label value not present in config")
    files = config.get("files")
    if not files:
        raise CephadmOperationsAdmin("files value not present in config")
    installer = ceph_cluster.get_ceph_object("installer")
    # Fetching node without _admin label
    node = [
        node for node in ceph_cluster.get_nodes() if "_admin" not in node.role.role_list
    ][0]
    hostname = node.hostname
    # Adding _Admin label in node
    exp_out = f"Added label _admin to host {hostname}"
    result = CephAdm(installer).ceph.orch.label.add(hostname, label)
    if result != exp_out:
        raise CephadmOperationsAdmin(
            f"Failed to add label '{label}' on node '{hostname}'"
        )
    # Verify files in new admin node
    timeout, interval = 300, 6
    for w in WaitUntil(timeout=timeout, interval=interval):
        _files = [_f for _f in files if os.path.exists(_f)]
        if len(_files) == len(files):
            break
    if w.expired:
        raise CephadmOperationsAdmin(
            f"Failed to validate presence of '{files}' on node '{hostname}'"
        )
    # Operation using orch command in new admin node
    for w in WaitUntil(timeout=timeout, interval=interval):
        result = CephAdm(node).ceph.orch.ls()
        if "RUNNING" in result:
            break
    if w.expired:
        raise CephadmOperationsAdmin(f"Orch ls operation failed on node '{hostname}'")
    return 0
