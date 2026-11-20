"""
TC9 — Cert-store / mgr config-key size guard under agent scale.

Fails if any mgr/cephadm/cert* (or related) config-key dump entry exceeds the
configured byte limit (default 65536 — mon entry size).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

from cephadm_agent.helpers import log, shell
from cephadm_agent.scale_regression_ops import run_scale_case


def execute_tc9(ceph_cluster, installer, scale: Dict[str, Any], timings: List):
    log.info("=== TC9: Cert-store / config-key size check ===")
    limit = int(scale.get("cert_store_max_entry_bytes") or 65536)

    # Prefer structured dump; fall back to listing keys then probing sizes.
    oversized = []
    checked = 0
    try:
        out, _ = shell(installer, "ceph config-key dump -f json")
        dump = json.loads(out) if out.strip() else {}
        if isinstance(dump, dict):
            for key, val in dump.items():
                k = str(key)
                if not any(
                    s in k.lower()
                    for s in ("cert", "cephadm", "agent", "tls", "keyring")
                ):
                    continue
                size = len(val) if isinstance(val, str) else len(json.dumps(val))
                checked += 1
                if size > limit:
                    oversized.append({"key": k, "bytes": size})
                    log.error(
                        "OVERSIZE config-key {0} bytes={1} limit={2}".format(
                            k, size, limit
                        )
                    )
    except Exception as exc:
        log.warning("config-key dump failed ({0}); trying ls + get sample".format(exc))
        out, _ = shell(installer, "ceph config-key ls")
        keys = [
            k.strip().strip('"').strip("'")
            for k in (out or "").replace(",", "\n").splitlines()
            if k.strip()
        ]
        keys = [
            k for k in keys if any(s in k.lower() for s in ("cert", "cephadm", "agent"))
        ]
        for key in keys[:200]:
            try:
                val, _ = shell(installer, "ceph config-key get {0}".format(key))
                size = len(val or "")
                checked += 1
                if size > limit:
                    oversized.append({"key": key, "bytes": size})
            except Exception:
                continue

    # Also surface related health checks if present
    health_out, _ = shell(installer, "ceph health detail -f json")
    health = json.loads(health_out) if health_out.strip() else {}
    checks = health.get("checks") or {}
    mon_related = {
        code: info.get("summary", {})
        for code, info in checks.items()
        if any(s in code.upper() for s in ("MON", "AUTH", "CEPHADM"))
    }

    result = {
        "checked": checked,
        "limit": limit,
        "oversized": oversized[:20],
        "health_related": mon_related,
    }
    log.info("TC9 result: checked={0} oversized={1}".format(checked, len(oversized)))
    if oversized:
        raise AssertionError(
            "{0} config-key entr(y/ies) exceed {1} bytes: {2}".format(
                len(oversized), limit, oversized[:5]
            )
        )
    log.info("PASS TC9: no oversized cert-store related config-keys")
    return result


def run(ceph_cluster, **kw):
    return run_scale_case(
        ceph_cluster,
        kw,
        case_id="tc9",
        execute=execute_tc9,
        watch_health=True,
        start_workloads=False,
        check_agent_down=True,
    )
