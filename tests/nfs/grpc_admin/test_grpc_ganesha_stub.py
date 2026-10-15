"""
Placeholder module for Polarion test cases not yet automated.

Returns skip (rc=-1) so results post to Polarion without failing the suite
while automation is developed.
"""

from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Log and skip a planned gRPC admin Polarion test case."""
    config = kw.get("config", {})
    polarion_id = config.get("polarion-id") or config.get("polarion_id", "unknown")
    reason = config.get(
        "stub_reason",
        "Automation not yet implemented — see grpc_admin/POLARION.md",
    )
    log.info("SKIP Polarion %s: %s", polarion_id, reason)
    return -1
