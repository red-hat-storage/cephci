from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.utils import create_yaml_config


def run(ceph_cluster, **kw):
    """
    Validate the change in retention time for Prometheus
    """
    config = kw.get("config", {})
    installer = ceph_cluster.get_nodes(role="installer")[0]
    specs = config.get("specs")
    realm_name = specs.get("rgw_realm")

    # Enable MGR module
    CephAdm(installer).ceph.mgr.module.enable("rgw")

    # Bootstrap rgw using new spec file
    file = create_yaml_config(installer, specs)
    conf = {"in-file": file}
    out = CephAdm(installer, mount=file).ceph.rgw.realm.bootstrap(**conf)
    if "Realm(s) created correctly" not in out:
        raise OperationFailedError(
            "Failed to perform rgw realm bootstrap using spec file"
        )

    # Validate rgw realm tokens
    tokens = CephAdm(installer).ceph.rgw.realm.tokens()
    if not tokens:
        raise OperationFailedError(
            "Failed: No realm tokens are listed even after rgw bootstrap"
        )
    if realm_name not in tokens:
        raise OperationFailedError(
            "Failed: The recently created realm token is not listed"
        )

    return 0
