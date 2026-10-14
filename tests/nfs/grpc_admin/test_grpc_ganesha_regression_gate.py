"""
Tier0/Tier1 regression gate smoke tests with gRPC-enabled cephadm NFS.

Runs a representative subset of existing CephCI NFS modules rather than
re-executing full tier YAML (200+ tests).
"""

import importlib

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.grpc_admin.grpc_deploy import (
    apply_nfs_container_image,
    resolve_nfs_container_image,
)
from utility.log import Log

log = Log(__name__)

# Representative tier0 / tier1 checks (module name without .py).
TIER0_MODULES = (
    ("nfs_verify_file_lock", {"nfs_version": 4.1, "clients": 2}),
    ("nfs_verify_pynfs", {"nfs_version": 4.1, "clients": 2}),
)

TIER1_MODULES = (
    ("test_export_readonly", {"nfs_version": 4.1, "clients": 1}),
    ("nfs_validate_cmount_path_export_conf", {"nfs_version": 4.1, "clients": 1}),
)


def _merge_config(base, override):
    merged = dict(base)
    merged.update(override or {})
    return merged


def _run_module_chain(ceph_cluster, modules, suite_config):
    """Import and run each module; fail fast on first hard failure."""
    failures = []
    for mod_name, defaults in modules:
        cfg = _merge_config(defaults, suite_config)
        log.info("Regression gate running %s with config %s", mod_name, cfg)
        try:
            mod = importlib.import_module(mod_name)
        except ImportError as exc:
            raise OperationFailedError(f"Cannot import {mod_name}: {exc}") from exc
        rc = mod.run(ceph_cluster, config=cfg)
        if rc == -1:
            log.warning("%s returned skip (rc=-1)", mod_name)
            continue
        if rc != 0:
            failures.append(f"{mod_name} rc={rc}")
    if failures:
        raise OperationFailedError(
            f"Regression gate failed: {', '.join(failures)}"
        )
    return 0


def run(ceph_cluster, **kw):
    config = kw.get("config", {})
    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required (tier0_smoke | tier1_smoke)")

    installers = ceph_cluster.get_nodes("installer")
    if installers:
        image = resolve_nfs_container_image(config)
        if image:
            apply_nfs_container_image(installers[0], image)

    gate_config = {
        k: v
        for k, v in config.items()
        if k not in ("operation", "nfs_container_image", "container_image_nfs")
    }

    if operation == "tier0_smoke":
        return _run_module_chain(ceph_cluster, TIER0_MODULES, gate_config)
    if operation == "tier1_smoke":
        return _run_module_chain(ceph_cluster, TIER1_MODULES, gate_config)

    raise ConfigError(f"Unknown regression gate operation: {operation}")
