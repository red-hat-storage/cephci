from cli.cephadm.cephadm import CephAdm


class RgwOperationFailedError(Exception):
    pass


def run(ceph_cluster, **kw):
    """Verify Defining a zone-group when deploying RGW service with cephadm
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    node = ceph_cluster.get_nodes("installer")[0]
    realm_name = config.get("realm-name")
    zonegroup_name = config.get("zonegroup-name")
    zone_name = config.get("zone-name")

    # Enable MGR module
    CephAdm(node).ceph.mgr.module(action="enable", module="rgw")

    # Create a new RGW realm entity, a new zonegroup, and a new zone anf deploy RGW daemon
    # using RGW module's bootstrap command
    conf = {
        "realm-name": realm_name,
        "zonegroup-name": zonegroup_name,
        "zone-name": zone_name,
        "placement": node.hostname,
    }
    CephAdm(node).ceph.rgw.realm.bootstrap(**conf)

    # Check for rgw realm tokens
    tokens = CephAdm(node).ceph.rgw.realm.tokens()

    # Check 1: See if tokens are listing
    if not tokens:
        raise RgwOperationFailedError("No realm tokens are listed even after creating")

    # Check 2: See if the token created above is listed
    if realm_name not in tokens:
        raise RgwOperationFailedError("The recently created realm token is not listed")

    return 0
