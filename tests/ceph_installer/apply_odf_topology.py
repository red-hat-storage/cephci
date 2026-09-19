"""
Suite-callable wrapper to apply ODF topology after deploy.

Use when deploy is not via test_cephadm.py, or to re-apply topology::

    - test:
        module: apply_odf_topology.py
        config:
          # optional subset: zones, crush_rules, container_limits, msgr2, ssd_class
          # steps: [zones, msgr2]
"""

from ceph.ceph_admin import CephAdmin
from utility.log import Log
from utility.odf_defaults import APPLY_ODF_TOPOLOGY_KEY, overrides_enabled
from utility.odf_topology import apply_odf_topology, topology_status_snapshot

LOG = Log(__name__)


def run(ceph_cluster, **kwargs):
    config = kwargs.get("config") or {}
    overrides = kwargs.get("test_data", {}).get("custom_config_dict") or {}
    # Allow suite to force apply even without CLI flag
    force = config.get("apply_odf_topology", False)
    if not force and not overrides_enabled(overrides, APPLY_ODF_TOPOLOGY_KEY):
        LOG.info(
            "Skipping ODF topology (set --custom-config apply-odf-topology=true "
            "or config.apply_odf_topology: true)"
        )
        return 0

    # Ensure flag is seen by apply_odf_topology when forced from suite config
    if force:
        overrides = dict(overrides)
        overrides[APPLY_ODF_TOPOLOGY_KEY] = True

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    steps = config.get("steps")
    apply_odf_topology(
        ceph_cluster,
        cephadm.shell,
        installer_node=cephadm.installer,
        overrides=overrides,
        steps=steps,
    )
    LOG.info("Topology snapshot: %s", topology_status_snapshot(cephadm.shell))
    return 0
