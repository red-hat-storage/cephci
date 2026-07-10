"""
Ceph Health Warning Classification and Tracking for Upgrade Tests

Provides:
- HEALTH_WARNING_CATALOG: dict of 78 explicit health codes with severity + description
- classify_upgrade_error(msg): categorize UPGRADE_* error messages
- classify_health_warning(code): return severity level for a health check code
- HealthWarningTracker: lightweight state tracker for health snapshots during upgrades
"""

import re
import threading

from utility.log import Log

log = Log(__name__)

BLOCKING = "blocking"
CONCERNING = "concerning"
EXPECTED = "expected"
INFORMATIONAL = "informational"

HEALTH_WARNING_CATALOG = {
    "UPGRADE_EXCEPTION": {
        "severity": BLOCKING,
        "description": "Catch-all for unhandled exceptions in upgrade loop",
    },
    "UPGRADE_FAILED_PULL": {
        "severity": BLOCKING,
        "description": "Container image pull failure",
    },
    "UPGRADE_REDEPLOY_DAEMON": {
        "severity": BLOCKING,
        "description": "Daemon redeploy failed during upgrade",
    },
    "UPGRADE_NO_STANDBY_MGR": {
        "severity": BLOCKING,
        "description": "No standby MGR available for failover",
    },
    "CEPHADM_PAUSED": {
        "severity": BLOCKING,
        "description": "Cephadm background work paused -- blocks upgrade",
    },
    "RECENT_CRASH": {
        "severity": CONCERNING,
        "description": "Recent daemon crash detected",
    },
    "RECENT_MGR_MODULE_CRASH": {
        "severity": CONCERNING,
        "description": "Recent MGR module crash",
    },
    "MDS_CACHE_OVERSIZED": {
        "severity": CONCERNING,
        "description": "MDS cache exceeds memory limit",
    },
    "MDS_CLIENT_RECALL": {
        "severity": CONCERNING,
        "description": "Clients not responding to cache pressure",
    },
    "MGR_MODULE_ERROR": {
        "severity": CONCERNING,
        "description": "MGR module error",
    },
    "MGR_MODULE_DEPENDENCY": {
        "severity": CONCERNING,
        "description": "MGR module dependency issue",
    },
    "MDS_DAMAGE": {
        "severity": CONCERNING,
        "description": "Metadata damage detected",
    },
    "OSD_FULL": {
        "severity": CONCERNING,
        "description": "OSD is full -- blocks writes",
    },
    "OSD_NEARFULL": {
        "severity": CONCERNING,
        "description": "OSD approaching full capacity",
    },
    "OSD_BACKFILLFULL": {
        "severity": CONCERNING,
        "description": "OSD too full for backfill",
    },
    "POOL_FULL": {
        "severity": CONCERNING,
        "description": "Pool is full -- blocks writes",
    },
    "POOL_NEAR_FULL": {
        "severity": CONCERNING,
        "description": "Pool approaching full",
    },
    "PG_DAMAGED": {
        "severity": CONCERNING,
        "description": "PGs with damaged data",
    },
    "PG_RECOVERY_FULL": {
        "severity": CONCERNING,
        "description": "PGs cannot recover due to full OSDs",
    },
    "PG_BACKFILL_FULL": {
        "severity": CONCERNING,
        "description": "PGs cannot backfill due to full OSDs",
    },
    "OBJECT_UNFOUND": {
        "severity": CONCERNING,
        "description": "Objects cannot be found",
    },
    "OSD_SCRUB_ERRORS": {
        "severity": CONCERNING,
        "description": "Scrub errors detected",
    },
    "OSD_TOO_MANY_REPAIRS": {
        "severity": CONCERNING,
        "description": "Too many OSD read repairs",
    },
    "MDS_HEALTH_READ_ONLY": {
        "severity": CONCERNING,
        "description": "MDS in read-only mode",
    },
    "MDS_TRIM": {
        "severity": CONCERNING,
        "description": "MDS journal trim falling behind",
    },
    "MDS_SLOW_METADATA_IO": {
        "severity": CONCERNING,
        "description": "Slow metadata IO operations",
    },
    "MDS_CLIENTS_BROKEN_ROOTSQUASH": {
        "severity": CONCERNING,
        "description": "Client has broken root_squash implementation",
    },
    "FS_DEGRADED": {
        "severity": CONCERNING,
        "description": "Filesystem ranks failed or damaged",
    },
    "MON_NETSPLIT": {
        "severity": CONCERNING,
        "description": "MON network split detected",
    },
    "MON_CLOCK_SKEW": {
        "severity": CONCERNING,
        "description": "Clock drift between MON nodes",
    },
    "MON_DISK_CRIT": {
        "severity": CONCERNING,
        "description": "MON database critically large",
    },
    "MON_DISK_LOW": {
        "severity": CONCERNING,
        "description": "MON disk space low",
    },
    "CEPHADM_FAILED_DAEMON": {
        "severity": CONCERNING,
        "description": "A managed daemon has failed",
    },
    "CEPHADM_HOST_CHECK_FAILED": {
        "severity": CONCERNING,
        "description": "Host fails basic cephadm prerequisites",
    },
    "BLUESTORE_SLOW_OP_ALERT": {
        "severity": CONCERNING,
        "description": "BlueStore slow operations detected",
    },
    "AUTH_INSECURE_GLOBAL_ID_RECLAIM": {
        "severity": CONCERNING,
        "description": "Clients using insecure global_id reclaim",
    },
    "OSD_DOWN": {
        "severity": EXPECTED,
        "description": "OSD(s) down -- normal during rolling OSD upgrade",
    },
    "PG_DEGRADED": {
        "severity": EXPECTED,
        "description": "PGs degraded -- normal during OSD restarts",
    },
    "PG_AVAILABILITY": {
        "severity": EXPECTED,
        "description": "PGs with reduced availability",
    },
    "OBJECT_MISPLACED": {
        "severity": EXPECTED,
        "description": "Objects not in preferred location",
    },
    "DAEMON_OLD_VERSION": {
        "severity": EXPECTED,
        "description": "Mixed daemon versions -- normal during upgrade",
    },
    "MDS_UP_LESS_THAN_MAX": {
        "severity": EXPECTED,
        "description": "Active MDS ranks reduced -- normal during MDS upgrade",
    },
    "MDS_INSUFFICIENT_STANDBY": {
        "severity": EXPECTED,
        "description": "Fewer standby MDS daemons -- normal during MDS upgrade",
    },
    "MDS_ALL_DOWN": {
        "severity": EXPECTED,
        "description": "All MDS ranks offline -- brief during MDS upgrade",
    },
    "FS_WITH_FAILED_MDS": {
        "severity": EXPECTED,
        "description": "MDS rank failed, no standby -- brief during MDS upgrade",
    },
    "MGR_DOWN": {
        "severity": EXPECTED,
        "description": "No active MGR -- brief during MGR upgrade",
    },
    "MON_DOWN": {
        "severity": EXPECTED,
        "description": "MON(s) down -- brief during MON upgrade",
    },
    "SLOW_OPS": {
        "severity": EXPECTED,
        "description": "Slow OSD operations -- expected during restarts",
    },
    "OSD_UNREACHABLE": {
        "severity": EXPECTED,
        "description": "OSD unreachable via heartbeat -- brief during restarts",
    },
    "MDS_SLOW_REQUEST": {
        "severity": EXPECTED,
        "description": "Slow MDS requests -- expected during MDS failover",
    },
    "MDS_CLIENTS_LAGGY": {
        "severity": EXPECTED,
        "description": "Clients laggy due to OSD lag",
    },
    "MDS_ESTIMATED_REPLAY_TIME": {
        "severity": EXPECTED,
        "description": "MDS journal replay in progress",
    },
    "CEPHADM_CHECK_CEPH_RELEASE": {
        "severity": EXPECTED,
        "description": "Mixed Ceph releases -- bypassed during upgrade",
    },
    "OSDMAP_FLAGS": {
        "severity": EXPECTED,
        "description": "Global OSD map flags set (noout, noscrub) -- normal during upgrade",
    },
    "OSD_FLAGS": {
        "severity": EXPECTED,
        "description": "Per-OSD flags set -- may be intentional during upgrade",
    },
    "PG_NOT_SCRUBBED": {
        "severity": EXPECTED,
        "description": "PGs not scrubbed recently -- expected if noscrub flag set",
    },
    "PG_NOT_DEEP_SCRUBBED": {
        "severity": EXPECTED,
        "description": "PGs not deep-scrubbed recently -- expected if nodeep-scrub flag set",
    },
    "MDS_HEALTH_CLIENT_LATE_RELEASE": {
        "severity": EXPECTED,
        "description": "Client failing to release capabilities -- may occur during MDS upgrade",
    },
    "MDS_CLIENT_OLDEST_TID": {
        "severity": EXPECTED,
        "description": "Client not advancing oldest tid",
    },
    "CEPHADM_STRAY_DAEMON": {
        "severity": EXPECTED,
        "description": "Daemon not managed by cephadm -- may appear transiently during upgrade",
    },
    "CEPHADM_STRAY_HOST": {
        "severity": EXPECTED,
        "description": "Host has daemons but not registered with cephadm",
    },
    "NVMEOF_GATEWAY_DOWN": {
        "severity": EXPECTED,
        "description": "NVMe-oF gateway down -- expected during gateway upgrade",
    },
    "TOO_MANY_PGS": {
        "severity": CONCERNING,
        "description": "Too many PGs per OSD -- exceeds mon_max_pg_per_osd",
    },
    "TOO_FEW_PGS": {
        "severity": EXPECTED,
        "description": "Pool has fewer PGs than recommended",
    },
    "SMALLER_PGP_NUM": {
        "severity": EXPECTED,
        "description": "Pool pg_num > pgp_num -- PG splitting in progress",
    },
    "MANY_OBJECTS_PER_PG": {
        "severity": EXPECTED,
        "description": "Average objects per PG exceeds threshold",
    },
    "POOL_NO_REDUNDANCY": {
        "severity": CONCERNING,
        "description": "Pool has no redundancy (size=1)",
    },
    "POOL_TOO_FEW_PGS": {
        "severity": EXPECTED,
        "description": "Pool PG count is too low for data volume",
    },
    "TOO_FEW_OSDS": {
        "severity": CONCERNING,
        "description": "Fewer OSDs than osd_pool_default_size -- reduced redundancy",
    },
    "CEPHADM_REFRESH_FAILED": {
        "severity": CONCERNING,
        "description": "Cephadm failed to refresh host info",
    },
    "CEPHADM_DAEMON_PLACE_FAIL": {
        "severity": CONCERNING,
        "description": "Cephadm failed to place a daemon on a host",
    },
    "TELEMETRY_CHANGED": {
        "severity": INFORMATIONAL,
        "description": "Telemetry module has new collections available",
    },
    "OSD_SLOW_PING_TIME": {
        "severity": CONCERNING,
        "description": "OSD heartbeat ping latency too high",
    },
    "BLUEFS_SPILLOVER": {
        "severity": CONCERNING,
        "description": "BlueFS spilling to slow device",
    },
    "BLUESTORE_FRAGMENTATION": {
        "severity": CONCERNING,
        "description": "BlueStore fragmentation exceeds threshold",
    },
    "OSD_UP_LESS_THAN_IN": {
        "severity": EXPECTED,
        "description": "Fewer OSDs up than in -- normal during rolling OSD upgrade",
    },
    "MDS_DEGRADED": {
        "severity": EXPECTED,
        "description": "MDS cluster degraded -- normal during MDS rolling upgrade",
    },
    "IBM_LICENSE_NOT_ACCEPTED": {
        "severity": EXPECTED,
        "description": (
            "IBM license not yet accepted -- expected during/after upgrade "
            "before orch accept-license; not a never-licensed fresh cluster"
        ),
    },
}

_UPGRADE_ERROR_RE = re.compile(
    r"(UPGRADE_EXCEPTION|UPGRADE_FAILED_PULL|UPGRADE_REDEPLOY_DAEMON"
    r"|UPGRADE_NO_STANDBY_MGR)",
    re.IGNORECASE,
)

_UPGRADE_SUBCAUSE_PATTERNS = [
    ("mds_health_block", re.compile(r"fs set failed.*health warnings", re.I)),
    ("division_by_zero", re.compile(r"division by zero", re.I)),
    ("daemon_spec_unbound", re.compile(r"daemon_spec.*referenced before", re.I)),
    ("module_attr_missing", re.compile(r"module.*has no attribute", re.I)),
    ("image_pull_fail", re.compile(r"failed to pull", re.I)),
    ("json_parse_error", re.compile(r"Invalid control character", re.I)),
]


def classify_upgrade_error(msg: str) -> tuple:
    """Classify an upgrade status message into (error_code, subcause)."""
    m = _UPGRADE_ERROR_RE.search(msg)
    if not m:
        return ("", "")
    error_code = m.group(1)
    subcause = "unknown"
    for name, pat in _UPGRADE_SUBCAUSE_PATTERNS:
        if pat.search(msg):
            subcause = name
            break
    return (error_code, subcause)


def classify_health_warning(code: str) -> str:
    """Return severity level for a health check code."""
    entry = HEALTH_WARNING_CATALOG.get(code)
    if entry:
        return entry["severity"]
    return INFORMATIONAL


class HealthWarningTracker:
    """Tracks health warnings seen during upgrade for the report catalog."""

    def __init__(self):
        self._lock = threading.Lock()
        self.all_seen = {}

    def record_snapshot(self, timestamp: str, health_data: dict):
        """Record a ceph health detail snapshot."""
        if not isinstance(health_data, dict):
            return
        checks = health_data.get("checks", {})
        with self._lock:
            for code in checks:
                severity = classify_health_warning(code)
                if code not in self.all_seen:
                    desc = HEALTH_WARNING_CATALOG.get(code, {}).get(
                        "description", "Unknown health check"
                    )
                    self.all_seen[code] = {
                        "severity": severity,
                        "first_seen": timestamp,
                        "last_seen": timestamp,
                        "count": 1,
                        "description": desc,
                    }
                else:
                    self.all_seen[code]["last_seen"] = timestamp
                    self.all_seen[code]["count"] += 1

    def to_dict(self) -> dict:
        """Export for monitoring_data / report."""
        with self._lock:
            return {"all_health_checks_seen": dict(self.all_seen)}
