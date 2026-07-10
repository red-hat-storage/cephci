"""
IO lifecycle manager for the Ceph upgrade thrash test.

Handles integrity baseline writes, cluster pre-fill, background IO during
upgrade, process management (PID registry), and post-upgrade verification
across all client types: RADOS, RBD, CephFS, NFS, SMB, and NVMeoF.

Address-space separation:
    BASELINE: offset 0 to fio_baseline_size (or dedicated objects/dirs)
    BACKGROUND: offset fio_baseline_size onward (or separate pools/dirs)
    FILL: fill_data/ subdirectories or fill_obj_* prefix objects

IO Tiering:
    All IO intensity parameters (rate limits, thread counts, concurrency,
    file sizes) are configurable via the ``io_tier`` knob in the suite YAML.
    Six tiers are provided:

        none       -- Zero IO; test upgrade orchestration only.
        low        -- Canary IO; minimal tools at very low rates (~10 procs).
        medium     -- Balanced; moderate tools, s3cmd added for RGW (~30 procs).
        high       -- Full suite; warp added for concurrent S3 IO (~46 procs).
        extreme    -- Maximum stress; unlimited rates, high concurrency (~80+ procs).
        saturation -- Cluster saturation; all services maximized (~120+ procs).

    Tiers are rebalanced for service equity: RBD/RADOS/RGW get proportionally
    higher concurrency to compensate for fewer process instances (1 pool/device
    vs 4+ CephFS mount points).

    Each tier controls:
        - Which IO tools run per service (tools)
        - Background IO intensity: fio rate_iops/numjobs/bs, vdbench fwdrate/
          threads, smallfile threads/file_count, rados_bench threads, etc. (bg_params)
        - Fill IO intensity: fio numjobs/bs, vdbench threads, etc. (fill_params)
        - Integrity sizing: fio baseline size, RADOS object count, timeouts (integrity)
        - Resource scale: image counts, bucket counts, subvolumes, mounts (scale)

    Config precedence (highest to lowest):
        1. Explicit YAML values (always win)
        2. Tier profile defaults
        3. Code-level .get() fallback defaults

    When ``io_tier`` is omitted, behavior is identical to the pre-tiering codebase.
    See ``_IO_TIER_PROFILES`` and ``apply_io_tier()`` for details.
"""

import base64
import concurrent.futures
import json
import random
import re
import shlex
import threading
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from upgrade_thrashing.lifecycle_log import remote_exec_failure_tag

from utility.log import Log

log = Log(__name__)


def _vdbench_elapsed(seconds, interval=5):
    """vdbench requires elapsed to be a multiple of interval."""
    seconds = max(interval, int(seconds))
    return seconds - (seconds % interval)


try:
    from thrash_helpers import _INTEGRITY_FILE_SPECS
except ImportError:
    _INTEGRITY_FILE_SPECS = [
        ("integrity_4k", "4k", "4k"),
        ("integrity_64k", "64k", "4k"),
        ("integrity_256k", "256k", "4k"),
        ("integrity_1m", "1M", "16k"),
        ("integrity_4m", "4M", "64k"),
        ("integrity_8m", "8M", "64k"),
        ("integrity_16m", "16M", "128k"),
        ("integrity_32m", "32M", "128k"),
        ("integrity_48m", "48M", "256k"),
        ("integrity_64m", "64M", "256k"),
    ]

FILL_DIR_PREFIX = "fill_data"
FILL_OBJ_PREFIX = "fill_obj"
INTEGRITY_DIR = "thrash_integrity"
BACKGROUND_DIR = "background_io"

# ---------------------------------------------------------------------------
#  IO Tier Profiles
# ---------------------------------------------------------------------------
# Each tier defines a complete set of IO parameters. Tiers are rebalanced
# for service equity: RBD/RADOS/RGW get proportionally higher concurrency
# to compensate for fewer process instances.
#
# Parameter reference (bg_params):
#   fio_rate_iops      -- --rate_iops for fio background jobs (0 = unlimited)
#   fio_numjobs        -- --numjobs for fio background jobs
#   fio_size           -- --size for fio mount-based background jobs
#   fio_bs             -- --bs for fio (or --bsrange if value contains '-')
#   fio_block_size     -- --size for fio block-device (RBD/NVMeoF) bg jobs
#   vdbench_fwdrate    -- fwdrate= in vdbench param file (int or "max")
#   vdbench_threads    -- threads= in vdbench param file
#   smallfile_threads  -- --threads for smallfile_cli
#   smallfile_files    -- --files for smallfile_cli
#   rados_bench_threads -- -t for rados bench
#   rados_bench_duration -- duration (seconds) for rados bench write
#   boto3_sleep        -- sleep between S3 put/get cycles (seconds)
#   boto3_obj_min      -- minimum random object size (bytes)
#   boto3_obj_max      -- maximum random object size (bytes)
#   warp_concurrent    -- --concurrent for warp (parallel S3 operations)
#   warp_obj_size      -- --obj.size for warp (e.g. "256KiB")
#   warp_duration      -- --duration for warp per-run (e.g. "30m")
#   dbench_clients     -- client count for dbench
#   dd_count           -- count= for dd (number of blocks)
#   dd_bs              -- bs= for dd (block size)
#   bonnie_size        -- -s for bonnie++ (MB)
#   fsstress_ops       -- -n for fsstress (operation count)
#   fsstress_procs     -- -p for fsstress (process count)
#   mdtest_files       -- -n for mdtest (file count)
#   smb_dd_count       -- count= for smbclient dd background IO
#
# Parameter reference (fill_params):
#   target_percent     -- cluster fill target (routed to cluster_fill config)
#   fio_numjobs        -- --numjobs for fill fio jobs
#   fio_bs             -- --bs for mount-based fill fio jobs
#   fio_default_size   -- --size default when chunk size is not specified
#   block_fio_bs       -- --bs for block-device fill fio jobs
#   vdbench_threads    -- threads= in fill vdbench param file
#   smallfile_threads  -- --threads for fill smallfile_cli
#   smb_dd_count       -- count= for fill smbclient dd
#
# Parameter reference (integrity):
#   fio_baseline_size     -- --size for block integrity writes
#   rados_objects_per_pool -- object count for RADOS integrity baseline
#   verify_timeout        -- timeout for fio verify and RADOS verify (seconds)
#   write_timeout         -- timeout for block baseline writes (seconds)
#   rados_batch_size      -- RADOS objects per SSH batch call
#   heavy_ssh_parallel    -- max concurrent long client SSH sessions (verify)
#   pool_clean_wait_timeout -- wait for rep_pool active+clean before RBD verify

_IO_TIER_PROFILES = {
    "none": {
        "tools": {},
        "bg_params": {},
        "fill_params": {"target_percent": 0},
        "integrity": {},
        "scale": {
            "rbd": {"image_count": 10, "fill_image_count": 0},
            "rgw": {"versioned_buckets": 1, "non_versioned_buckets": 1},
            "cephfs": {"subvolume_groups_per_fs": 1, "subvolumes_per_group": 1},
            "nfs": {"mounts_per_version": 1},
            "smb": {"share_count": 1},
        },
    },
    "low": {
        "tools": {
            "cephfs": ["fio"],
            "nfs": ["fio"],
            "rbd": ["fio"],
            "rados": ["rados_bench"],
            "rgw": ["boto3"],
        },
        "bg_params": {
            "fio_rate_iops": 50,
            "fio_numjobs": 1,
            "fio_size": "256M",
            "fio_bs": "4k",
            "fio_block_size": "512M",
            "vdbench_fwdrate": 30,
            "vdbench_threads": 1,
            "smallfile_threads": 1,
            "smallfile_files": 1000,
            "rados_bench_threads": 2,
            "rados_bench_duration": 30,
            "boto3_sleep": 0.5,
            "boto3_obj_min": 1024,
            "boto3_obj_max": 131072,
            "dbench_clients": 2,
            "dd_count": 5,
            "dd_bs": "1M",
            "bonnie_size": 64,
            "fsstress_ops": 5000,
            "fsstress_procs": 1,
            "mdtest_files": 2000,
            "smb_dd_count": 8,
        },
        "fill_params": {
            "target_percent": 5,
            "fio_numjobs": 1,
            "fio_bs": "1M",
            "fio_default_size": "1G",
            "block_fio_bs": "4M",
            "vdbench_threads": 1,
            "smallfile_threads": 1,
            "smb_dd_count": 128,
        },
        "integrity": {
            "fio_baseline_size": "512M",
            "rados_objects_per_pool": 500,
            "verify_timeout": 300,
            "write_timeout": 600,
            "rados_batch_size": 50,
        },
        "scale": {
            "rbd": {
                "image_count": 10,
                "image_size": "1G",
                "fill_image_count": 2,
                "fill_image_size": "10G",
            },
            "rgw": {"versioned_buckets": 2, "non_versioned_buckets": 2},
            "cephfs": {"subvolume_groups_per_fs": 1, "subvolumes_per_group": 5},
            "nfs": {"mounts_per_version": 1},
            "smb": {"share_count": 2},
        },
    },
    # "medium" provides balanced IO across all services. Rebalanced from
    # pre-tiering defaults to give RBD/RADOS/RGW proportionally higher
    # concurrency, compensating for fewer process instances.
    "medium": {
        "tools": {
            "cephfs": ["fio", "smallfile", "dd"],
            "nfs": ["fio", "smallfile"],
            "rbd": ["fio"],
            "rados": ["rados_bench"],
            "rgw": ["boto3", "s3cmd"],
        },
        "bg_params": {
            "fio_rate_iops": 200,
            "fio_numjobs": 1,
            "fio_size": "512M",
            "fio_bs": "4k",
            "fio_block_size": "2G",
            "vdbench_fwdrate": 100,
            "vdbench_threads": 4,
            "smallfile_threads": 4,
            "smallfile_files": 5000,
            "rados_bench_threads": 4,
            "rados_bench_duration": 30,
            "boto3_sleep": 0.1,
            "boto3_obj_min": 1024,
            "boto3_obj_max": 131072,
            "dbench_clients": 4,
            "dd_count": 10,
            "dd_bs": "1M",
            "bonnie_size": 128,
            "fsstress_ops": 10000,
            "fsstress_procs": 2,
            "mdtest_files": 5000,
            "smb_dd_count": 16,
        },
        "fill_params": {
            "target_percent": 10,
            "fio_numjobs": 4,
            "fio_bs": "1M",
            "fio_default_size": "4G",
            "block_fio_bs": "4M",
            "vdbench_threads": 4,
            "smallfile_threads": 4,
            "smb_dd_count": 256,
        },
        "integrity": {
            "fio_baseline_size": "1G",
            "rados_objects_per_pool": 1000,
            "verify_timeout": 300,
            "write_timeout": 600,
            "rados_batch_size": 50,
        },
        "scale": {
            "rbd": {
                "image_count": 10,
                "image_size": "1G",
                "fill_image_count": 4,
                "fill_image_size": "50G",
            },
            "rgw": {"versioned_buckets": 5, "non_versioned_buckets": 5},
            "cephfs": {"subvolume_groups_per_fs": 2, "subvolumes_per_group": 10},
            "nfs": {"mounts_per_version": 2},
            "smb": {"share_count": 5},
        },
    },
    "high": {
        "tools": {
            "cephfs": [
                "fio",
                "smallfile",
                "mdtest",
                "dd",
                "fsstress",
                "dbench",
                "vdbench",
            ],
            "nfs": [
                "fio",
                "smallfile",
                "bonnie",
                "dbench",
                "vdbench",
                "cthon",
            ],
            "rbd": ["fio"],
            "rados": ["rados_bench"],
            "rgw": ["boto3", "s3cmd", "warp"],
        },
        "bg_params": {
            "fio_rate_iops": 500,
            "fio_numjobs": 2,
            "fio_size": "1G",
            "fio_bs": "4k",
            "fio_block_size": "4G",
            "vdbench_fwdrate": 300,
            "vdbench_threads": 4,
            "smallfile_threads": 4,
            "smallfile_files": 5000,
            "rados_bench_threads": 8,
            "rados_bench_duration": 60,
            "boto3_sleep": 0.05,
            "boto3_obj_min": 1024,
            "boto3_obj_max": 262144,
            "warp_concurrent": 8,
            "warp_obj_size": "256KiB",
            "warp_duration": "30m",
            "dbench_clients": 8,
            "dd_count": 20,
            "dd_bs": "1M",
            "bonnie_size": 256,
            "cthon_iterations": 1,
            "cthon_sleep_sec": 60,
            "fsstress_ops": 20000,
            "fsstress_procs": 4,
            "mdtest_files": 10000,
            "smb_dd_count": 32,
        },
        "fill_params": {
            "target_percent": 25,
            "fio_numjobs": 4,
            "fio_bs": "4M",
            "fio_default_size": "4G",
            "block_fio_bs": "8M",
            "vdbench_threads": 4,
            "smallfile_threads": 4,
            "smb_dd_count": 512,
        },
        "integrity": {
            "fio_baseline_size": "2G",
            "rados_objects_per_pool": 2000,
            "verify_timeout": 600,
            "write_timeout": 900,
            "rados_batch_size": 100,
        },
        "scale": {
            "rbd": {
                "image_count": 10,
                "image_size": "2G",
                "fill_image_count": 6,
                "fill_image_size": "100G",
            },
            "rgw": {"versioned_buckets": 10, "non_versioned_buckets": 10},
            "cephfs": {"subvolume_groups_per_fs": 3, "subvolumes_per_group": 15},
            "nfs": {"mounts_per_version": 3},
            "smb": {"share_count": 8},
        },
    },
    "extreme": {
        "tools": {
            "cephfs": [
                "fio",
                "smallfile",
                "mdtest",
                "dd",
                "fsstress",
                "dbench",
                "vdbench",
            ],
            "nfs": [
                "fio",
                "smallfile",
                "bonnie",
                "dbench",
                "vdbench",
                "cthon",
            ],
            "rbd": ["fio"],
            "rados": ["rados_bench"],
            "rgw": ["boto3", "s3cmd", "warp"],
        },
        "bg_params": {
            "fio_rate_iops": 0,
            "fio_numjobs": 4,
            "fio_size": "2G",
            "fio_bs": "4k-128k",
            "fio_block_size": "8G",
            "vdbench_fwdrate": "max",
            "vdbench_threads": 8,
            "smallfile_threads": 8,
            "smallfile_files": 10000,
            "rados_bench_threads": 16,
            "rados_bench_duration": 60,
            "boto3_sleep": 0,
            "boto3_obj_min": 1024,
            "boto3_obj_max": 524288,
            "warp_concurrent": 16,
            "warp_obj_size": "512KiB",
            "warp_duration": "60m",
            "dbench_clients": 16,
            "dd_count": 50,
            "dd_bs": "4M",
            "bonnie_size": 512,
            "cthon_iterations": 1,
            "cthon_sleep_sec": 60,
            "fsstress_ops": 50000,
            "fsstress_procs": 4,
            "mdtest_files": 20000,
            "smb_dd_count": 64,
        },
        "fill_params": {
            "target_percent": 40,
            "fio_numjobs": 8,
            "fio_bs": "4M",
            "fio_default_size": "8G",
            "block_fio_bs": "8M",
            "vdbench_threads": 8,
            "smallfile_threads": 8,
            "smb_dd_count": 1024,
        },
        "integrity": {
            "fio_baseline_size": "4G",
            "rados_objects_per_pool": 5000,
            "verify_timeout": 900,
            "write_timeout": 1200,
            "rados_batch_size": 200,
        },
        "scale": {
            "rbd": {
                "image_count": 10,
                "image_size": "2G",
                "fill_image_count": 8,
                "fill_image_size": "100G",
            },
            "rgw": {"versioned_buckets": 15, "non_versioned_buckets": 15},
            "cephfs": {"subvolume_groups_per_fs": 4, "subvolumes_per_group": 20},
            "nfs": {"mounts_per_version": 4},
            "smb": {"share_count": 10},
        },
    },
    "saturation": {
        "tools": {
            "cephfs": [
                "fio",
                "smallfile",
                "mdtest",
                "dd",
                "fsstress",
                "dbench",
                "vdbench",
            ],
            "nfs": [
                "fio",
                "smallfile",
                "bonnie",
                "dbench",
                "vdbench",
                "cthon",
            ],
            "rbd": ["fio"],
            "rados": ["rados_bench"],
            "rgw": ["boto3", "s3cmd", "warp"],
        },
        "bg_params": {
            "fio_rate_iops": 0,
            "fio_numjobs": 16,
            "fio_size": "4G",
            "fio_bs": "4k-256k",
            "fio_block_size": "16G",
            "vdbench_fwdrate": "max",
            "vdbench_threads": 16,
            "smallfile_threads": 16,
            "smallfile_files": 50000,
            "rados_bench_threads": 32,
            "rados_bench_duration": 120,
            "boto3_sleep": 0,
            "boto3_obj_min": 1024,
            "boto3_obj_max": 1048576,
            "warp_concurrent": 32,
            "warp_obj_size": "1MiB",
            "warp_duration": "120m",
            "dbench_clients": 32,
            "dd_count": 100,
            "dd_bs": "16M",
            "bonnie_size": 1024,
            "cthon_iterations": 1,
            "cthon_sleep_sec": 60,
            "fsstress_ops": 100000,
            "fsstress_procs": 8,
            "mdtest_files": 50000,
            "smb_dd_count": 128,
        },
        "fill_params": {
            "target_percent": 10,
            "fio_numjobs": 18,
            "fio_bs": "16M",
            "fio_default_size": "16G",
            "block_fio_bs": "16M",
            "vdbench_threads": 18,
            "smallfile_threads": 18,
            "warp_fill_concurrent": 28,
            "smb_dd_count": 2048,
        },
        "integrity": {
            "fio_baseline_size": "8G",
            "rados_objects_per_pool": 10000,
            "verify_timeout": 1200,
            "write_timeout": 1800,
            "rados_batch_size": 200,
        },
        "scale": {
            "rbd": {
                "image_count": 10,
                "image_size": "4G",
                "fill_image_count": 10,
                "fill_image_size": "100G",
            },
            "rgw": {"versioned_buckets": 25, "non_versioned_buckets": 25},
            "cephfs": {"subvolume_groups_per_fs": 5, "subvolumes_per_group": 50},
            "nfs": {"mounts_per_version": 8},
            "smb": {"share_count": 15},
        },
    },
}


def apply_io_tier(config: dict, tier_name: str) -> None:
    """Merge IO tier profile defaults into config.

    Uses setdefault semantics so that explicit YAML values always take
    precedence over tier defaults.  Call this on the raw YAML config dict
    **before** ``_deep_merge(DEFAULT_CONFIG, ...)`` so that tier values
    sit between DEFAULT_CONFIG (lowest priority) and explicit YAML (highest).

    Tool-list merging converts the tier's per-service tool lists
    (e.g. ``["fio", "smallfile"]``) into ``{tool: True}`` entries and only
    sets tools not already present in config.

    ``fill_params.target_percent`` is routed to
    ``config["cluster_fill"]["target_percent"]`` since that is where the
    fill loop reads it.

    Args:
        config: Raw YAML config dict (modified in place).
        tier_name: One of "none", "low", "medium", "high", "extreme", "saturation".

    Raises:
        ValueError: If *tier_name* is not a recognised tier.
    """
    valid_tiers = set(_IO_TIER_PROFILES.keys())
    if tier_name not in valid_tiers:
        raise ValueError(
            f"Invalid io_tier '{tier_name}'. " f"Must be one of: {sorted(valid_tiers)}"
        )

    profile = _IO_TIER_PROFILES[tier_name]
    overrides: list = []

    # -- Tool selection -------------------------------------------------------
    if profile.get("tools"):
        io_tools = config.setdefault("io_tools", {})
        for svc, tool_list in profile["tools"].items():
            svc_tools = io_tools.setdefault(svc, {})
            for tool in tool_list:
                if tool in svc_tools:
                    overrides.append(f"io_tools.{svc}.{tool}")
                else:
                    svc_tools[tool] = True

    # -- Background IO intensity ----------------------------------------------
    if profile.get("bg_params"):
        bg = config.setdefault("bg_params", {})
        for key, val in profile["bg_params"].items():
            if key in bg:
                overrides.append(f"bg_params.{key}")
            else:
                bg[key] = val

    # -- Fill IO intensity ----------------------------------------------------
    if profile.get("fill_params"):
        fp = config.setdefault("fill_params", {})
        for key, val in profile["fill_params"].items():
            if key == "target_percent":
                fill_cfg = config.setdefault("cluster_fill", {})
                if "target_percent" not in fill_cfg:
                    fill_cfg["target_percent"] = val
                else:
                    overrides.append("cluster_fill.target_percent")
            elif key in fp:
                overrides.append(f"fill_params.{key}")
            else:
                fp[key] = val

    # -- Integrity sizing -----------------------------------------------------
    if profile.get("integrity"):
        integ = config.setdefault("integrity", {})
        for key, val in profile["integrity"].items():
            if key in integ:
                overrides.append(f"integrity.{key}")
            else:
                integ[key] = val

    # -- Resource scale -------------------------------------------------------
    if profile.get("scale"):
        cfg_scale = config.setdefault("scale", {})
        for svc, svc_defaults in profile["scale"].items():
            svc_cfg = cfg_scale.setdefault(svc, {})
            for key, val in svc_defaults.items():
                if key in svc_cfg:
                    overrides.append(f"scale.{svc}.{key}")
                else:
                    svc_cfg[key] = val

    if overrides:
        log.info(
            "IO tier '%s': YAML overrides detected for: %s",
            tier_name,
            ", ".join(overrides),
        )
    scale_summary = {
        svc: len(keys)
        for svc, keys in config.get("scale", {}).items()
        if isinstance(keys, dict)
    }
    log.info(
        "IO tier '%s' applied (tools=%d svcs, bg=%d keys, fill=%d keys, "
        "integrity=%d keys, scale=%s)",
        tier_name,
        len(profile.get("tools", {})),
        len(config.get("bg_params", {})),
        len(config.get("fill_params", {})),
        len(config.get("integrity", {})),
        scale_summary,
    )


class UpgradeIOManager:
    """Manages IO lifecycle across all client types for the upgrade thrash test.

    Maintains a PID registry for guaranteed process cleanup and tracks
    integrity checksums for post-upgrade verification.
    """

    def __init__(self, ceph_cluster, rados_obj, config: Dict[str, Any]):
        """
        Args:
            ceph_cluster: The Ceph cluster object (ceph.ceph.Ceph).
            rados_obj: RadosOrchestrator instance for cluster commands.
            config: Full suite config dict (includes io_tools, integrity,
                    cluster_fill, io_patterns, etc.).
        """
        self.ceph_cluster = ceph_cluster
        self.rados_obj = rados_obj
        self.config = config
        installers = ceph_cluster.get_nodes(role="installer")
        if not installers:
            raise RuntimeError("No installer node found in cluster topology")
        self.installer = installers[0]

        self._pid_registry: Dict[Tuple[Any, int], Dict[str, str]] = {}
        self._registry_lock = threading.Lock()
        self._integrity_checksums: Dict[str, Dict[str, str]] = {}
        # Per-client mount sample for integrity write/verify (same set both sides).
        self._integrity_mount_sample: Dict[str, List[Tuple[str, str]]] = {}
        self._io_error_counts: Dict[str, int] = {}
        self._bg_log_snapshot: List[Tuple[Any, Dict[str, str]]] = []
        self._fill_stop_event = threading.Event()
        self._fill_mount_idx = 0
        self._fill_dev_idx = 0
        self._rgw_tool_counter = 0
        self._cached_rgw_endpoints = None

        # IO tier parameters -- populated by apply_io_tier() before this
        # constructor runs, or left empty for pre-tiering backward compat.
        # Every command builder uses .get(key, hardcoded_default) so that
        # missing keys fall back to the original pre-tiering values.
        self._bg_params: Dict[str, Any] = config.get("bg_params", {})
        self._fill_params: Dict[str, Any] = config.get("fill_params", {})
        self._integrity: Dict[str, Any] = config.get("integrity", {})
        heavy_parallel = int(self._integrity.get("heavy_ssh_parallel", 4))
        self._heavy_ssh_sem = threading.Semaphore(max(1, heavy_parallel))

        # Quick-install only boto3/s3cmd now (needed by _setup_rgw).
        # Full tool install runs later via install_io_tools(), after pool
        # creation, so PG distribution overlaps with the slow compile phase.
        self._install_rgw_prerequisites()

    # ------------------------------------------------------------------
    #  IO tool installation
    # ------------------------------------------------------------------

    def _install_rgw_prerequisites(self) -> None:
        """Pre-install boto3 and s3cmd on all clients before service setup.

        _setup_rgw() needs boto3 on client[0] to create buckets, so these
        must be available before _setup_services() runs. The full tool
        install (install_io_tools) runs later, after pool creation, to
        overlap PG rebalancing with the slow tool compilation phase.

        Runs in parallel across all clients (~30s vs N*30s serial).
        """
        clients = self.ceph_cluster.get_nodes(role="client")
        if not clients:
            return

        def _prereq_on_client(client):
            tag = getattr(client, "hostname", str(client))
            try:
                client.exec_command(
                    sudo=True,
                    cmd="rpm -q python3-pip 2>/dev/null || "
                    "dnf install -y python3-pip 2>&1 || true",
                    timeout=60,
                )
                client.exec_command(
                    sudo=True,
                    cmd="pip3 install boto3 s3cmd 2>/dev/null || "
                    "python3 -m pip install boto3 s3cmd 2>/dev/null || true",
                    timeout=120,
                )
                log.debug(f"[{tag}] RGW prerequisites (boto3, s3cmd) installed")
            except Exception as e:
                log.warning(f"[{tag}] RGW prerequisite install failed: {e}")

        log.info("Installing RGW prerequisites (boto3, s3cmd) on all clients...")
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(len(clients), 10)
        ) as ex:
            list(ex.map(_prereq_on_client, clients))
        log.info("RGW prerequisites installed on all clients")

    # ------------------------------------------------------------------
    #  IO Tool Registry
    # ------------------------------------------------------------------
    # Each tool participates in one or both IO phases of the test:
    #
    #   FILL (Phase 2)  -- Write data to reach a target cluster fullness %.
    #       Requirements: writes must persist on disk (no self-cleanup),
    #       produce a predictable and controllable data volume, and
    #       terminate after writing a chunk so the adaptive fill loop
    #       can re-check cluster fullness.
    #
    #   BACKGROUND (Phase 3+) -- Continuous IO during upgrade/thrashing.
    #       No special constraints; just keeps the cluster busy.
    #
    # Tool classification:
    #
    #   Fill + Background:
    #     fio        -- Primary workhorse. CRC integrity, direct IO.
    #     vdbench    -- Java-based (Oracle). Param-file driven, controllable.
    #     smallfile  -- Many small files. Good for metadata + data fill.
    #     smbclient  -- dd on CIFS mount (only for SMB service).
    #
    #   Background only (_NON_FILL_TOOLS):
    #     bonnie     -- Deletes its own output files after each run.
    #     cthon      -- NFS protocol test suite; restart loop on mount root.
    #     dbench     -- Deletes its own output files after each run.
    #     dd         -- Writes too little per invocation (10MB) for fill.
    #     fsstress   -- Creates and deletes its own file tree.
    #     mdtest     -- Creates and deletes its own file tree.
    #     rados_bench-- Operates at RADOS pool level, not mount level.
    #     boto3      -- S3 API for RGW; not a filesystem tool.
    #     s3cmd      -- S3 CLI for RGW; rich op mix (multipart, list, del).
    #     warp       -- MinIO S3 benchmark; built-in concurrency.
    #
    # Maps io_tools config keys to the RPM/pip packages and binaries needed.
    _TOOL_PACKAGES = {
        "fio": {
            "rpms": ["fio"],
            "binary": "fio",
        },
        "smallfile": {
            "rpms": [],
            "check_cmd": "python3 -c 'import smallfile_cli'",
            "post_install": (
                "pip3 install git+https://github.com/distributed-system-analysis/"
                "smallfile.git 2>/dev/null || true"
            ),
        },
        "mdtest": {
            "rpms": ["openmpi-devel", "automake", "autoconf", "libtool"],
            "binary": "mdtest",
            "post_install": (
                "cd /tmp && rm -rf ior && "
                "git clone --depth 1 --branch 3.3.0 "
                "https://github.com/hpc/ior.git && cd ior && "
                "./bootstrap && "
                "source /etc/profile.d/modules.sh && "
                "module load mpi/openmpi-x86_64 && "
                "./configure && make -j4 && make install"
            ),
        },
        "dbench": {
            "rpms": ["dbench"],
            "binary": "dbench",
        },
        "fsstress": {
            "rpms": [
                "xfsprogs-devel",
                "libuuid-devel",
                "libattr-devel",
                "libacl-devel",
                "libaio-devel",
            ],
            "binary": "fsstress",
            "post_install": (
                "rm -f /usr/local/bin/fsstress && "
                "cd /tmp && rm -rf xfstests-dev && "
                "git clone --depth 1 "
                "https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git && "
                "cd xfstests-dev && make -j4 && "
                "cp ltp/fsstress /usr/local/bin/"
            ),
        },
        "vdbench": {
            # openjdk-headless RPM is rewritten per client RHEL major in
            # install_io_tools (9 → java-17, 10+ → java-21).
            "rpms": ["java-17-openjdk-headless", "unzip"],
            "binary": "vdbench",
            "post_install": (
                "mkdir -p /opt/vdbench && cd /tmp && "
                "curl -sLO https://github.com/ldua07/vdbench/raw/main/vdbench50407.zip && "
                "unzip -o vdbench50407.zip -d /opt/vdbench && "
                "chmod +x /opt/vdbench/vdbench && "
                "cat > /usr/local/bin/vdbench << 'WRAPPER'\n"
                "#!/bin/bash\n"
                'cd /opt/vdbench && exec ./vdbench "$@"\n'
                "WRAPPER\n"
                "chmod +x /usr/local/bin/vdbench"
            ),
        },
        "boto3": {
            "pip": ["boto3"],
            "check_cmd": "python3 -c 'import boto3'",
        },
        "s3cmd": {
            "pip": ["s3cmd"],
            "check_cmd": "s3cmd --version",
        },
        "warp": {
            "rpms": [],
            "binary": "warp",
            "post_install": (
                "curl -sLO https://dl.min.io/aistor/warp/release/"
                "linux-amd64/archive/warp-1.5.0-1.x86_64.rpm && "
                "rpm -i warp-1.5.0-1.x86_64.rpm && "
                "rm -f warp-1.5.0-1.x86_64.rpm"
            ),
        },
        "bonnie": {
            "rpms": ["bonnie++"],
            "binary": "bonnie++",
        },
        "cthon": {
            "rpms": ["git", "gcc", "nfs-utils", "time", "make", "libtirpc-devel"],
            "check_cmd": "test -x /root/cthon04/server",
            "post_install": (
                "rm -rf /root/cthon04 && "
                "git clone --depth 1 https://github.com/linux-nfs/cthon04.git "
                "/root/cthon04 && "
                "cd /root/cthon04 && make"
            ),
        },
        "dd": {
            "rpms": [],
            "binary": "dd",
        },
        "rados_bench": {
            "rpms": [],
            "binary": "rados",
        },
        # "smbclient" here means dd-based IO on CIFS mounts, not smbclient protocol ops
        "smbclient": {
            "rpms": ["samba-client"],
            "binary": "smbclient",
        },
    }

    def install_io_tools(self) -> None:
        """Install all IO tools on clients in parallel.

        Runs AFTER _setup_services() so that PG distribution from pool
        creation overlaps with tool compilation (mdtest, fsstress
        source builds take 5-15 min per client).  Parallel execution across
        clients via ThreadPoolExecutor reduces total wall time from
        N*15 min (serial) to ~15 min (parallel).

        boto3/s3cmd are skipped if already installed by
        _install_rgw_prerequisites().
        """
        clients = self.ceph_cluster.get_nodes(role="client")
        if not clients:
            log.warning("No client nodes found, skipping IO tool installation")
            return

        io_cfg = self.config.get("io_tools", {})
        enabled_tools: Set[str] = set()
        for _svc, tools in io_cfg.items():
            if isinstance(tools, dict):
                for tool, val in tools.items():
                    if val:
                        enabled_tools.add(tool)

        if not enabled_tools:
            log.info("No IO tools enabled in config, skipping installation")
            return

        # Pre-compute tool requirements (read-only, shared across threads)
        rpms_needed: Set[str] = set()
        pip_needed: Set[str] = set()
        checks: List[dict] = []

        for tool in enabled_tools:
            spec = self._TOOL_PACKAGES.get(tool)
            if not spec:
                continue
            if "rpms" in spec:
                rpms_needed.update(spec["rpms"])
            if "pip" in spec:
                pip_needed.update(spec["pip"])
            checks.append({"tool": tool, **spec})

        log.info(
            f"Installing IO tools on {len(clients)} clients in parallel: "
            f"{sorted(enabled_tools)}"
        )

        def _rhel_major(client) -> int:
            try:
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=(
                        'awk -F= \'/^VERSION_ID=/{gsub(/"/,"",$2); print $2; '
                        "exit}' /etc/os-release"
                    ),
                    timeout=15,
                )
                return int(float((out or "9").strip().split(".")[0] or "9"))
            except Exception:
                return 9

        def _rpms_for_client(client) -> Set[str]:
            """Resolve RPMs for this client (openjdk name depends on RHEL major)."""
            major = _rhel_major(client)
            openjdk = (
                "java-21-openjdk-headless"
                if major >= 10
                else "java-17-openjdk-headless"
            )
            resolved: Set[str] = set()
            for rpm in rpms_needed:
                if "openjdk-headless" in rpm:
                    resolved.add(openjdk)
                else:
                    resolved.add(rpm)
            return resolved

        def _install_on_client(client):
            """Per-client install logic -- runs in its own thread."""
            tag = getattr(client, "hostname", str(client))
            skipped_count = 0
            installed_count = 0
            client_rpms = _rpms_for_client(client) if rpms_needed else set()

            if client_rpms:
                missing_rpms = client_rpms
                try:
                    rpm_query = " ".join(sorted(client_rpms))
                    out, _ = client.exec_command(
                        sudo=True,
                        cmd=f"rpm -q {rpm_query} 2>/dev/null || true",
                        timeout=30,
                    )
                    installed_rpms: set = set()
                    for line in out.strip().splitlines():
                        line = line.strip()
                        if line and "not installed" not in line:
                            installed_rpms.add(line)
                    missing_rpms = set()
                    for rpm in client_rpms:
                        if not any(inst.startswith(rpm) for inst in installed_rpms):
                            missing_rpms.add(rpm)
                    if missing_rpms:
                        log.info(
                            f"  [{tag}] {len(missing_rpms)}/{len(client_rpms)}"
                            f" RPMs need install: {sorted(missing_rpms)}"
                        )
                    else:
                        log.info(
                            f"  [{tag}] All {len(client_rpms)} RPMs"
                            " already installed -- skipping"
                        )
                        skipped_count += len(client_rpms)
                except Exception:
                    missing_rpms = client_rpms

                if missing_rpms:
                    try:
                        client.exec_command(
                            sudo=True,
                            cmd=(
                                "rpm -q epel-release 2>/dev/null || "
                                "dnf install -y "
                                "https://dl.fedoraproject.org/pub/epel/"
                                "epel-release-latest-9.noarch.rpm "
                                "2>/dev/null || true"
                            ),
                            timeout=60,
                        )
                    except Exception:
                        log.debug(f"[{tag}] EPEL setup skipped or failed")

                    for rpm in sorted(missing_rpms):
                        try:
                            client.exec_command(
                                sudo=True,
                                cmd=f"dnf install -y {rpm} 2>&1 || true",
                                timeout=120,
                            )
                            installed_count += 1
                        except Exception as e:
                            log.warning(f"[{tag}] RPM install {rpm}: {e}")

            if pip_needed:
                try:
                    client.exec_command(
                        sudo=True,
                        cmd="rpm -q python3-pip 2>/dev/null || "
                        "dnf install -y python3-pip 2>&1 || true",
                        timeout=60,
                    )
                except Exception:
                    pass
                pip_list = " ".join(sorted(pip_needed))
                try:
                    client.exec_command(
                        sudo=True,
                        cmd=f"pip3 install {pip_list} 2>/dev/null || "
                        f"python3 -m pip install {pip_list} 2>/dev/null "
                        f"|| true",
                        timeout=120,
                    )
                except Exception as e:
                    log.warning(f"[{tag}] pip install: {e}")

            needs_build = any(chk.get("post_install") for chk in checks)
            if needs_build:
                try:
                    client.exec_command(
                        sudo=True,
                        cmd="dnf install -y gcc make git 2>&1 || true",
                        timeout=120,
                    )
                except Exception:
                    pass

            for chk in checks:
                post_cmd = chk.get("post_install")
                if post_cmd:
                    binary = chk.get("binary")
                    if binary:
                        try:
                            client.exec_command(cmd=f"which {binary}", timeout=10)
                            log.debug(
                                f"  [{tag}] {chk.get('tool', '?')}: already"
                                f" built ({binary} exists), skipping"
                            )
                            skipped_count += 1
                            continue
                        except Exception:
                            pass
                    try:
                        _encoded = base64.b64encode(post_cmd.encode()).decode()
                        client.exec_command(
                            sudo=True,
                            cmd=f"echo {_encoded} | base64 -d | bash",
                            timeout=300,
                            long_running=True,
                        )
                        installed_count += 1
                    except Exception as e:
                        log.warning(
                            f"  [{tag}] {chk.get('tool', '?')}: "
                            f"post_install failed: {e}"
                        )

            for chk in checks:
                tool = chk["tool"]
                if "binary" in chk:
                    try:
                        client.exec_command(cmd=f"which {chk['binary']}", timeout=10)
                        log.debug(f"  [{tag}] {tool}: OK ({chk['binary']})")
                    except Exception:
                        if chk.get("pip_fallback"):
                            try:
                                client.exec_command(
                                    sudo=True,
                                    cmd=f"pip3 install {chk['pip_fallback']}",
                                    timeout=60,
                                )
                            except Exception:
                                pass
                        log.warning(
                            f"  [{tag}] {tool}: binary '{chk['binary']}'"
                            f" not found after install attempt"
                        )
                elif "check_cmd" in chk:
                    try:
                        client.exec_command(cmd=chk["check_cmd"], timeout=10)
                        log.debug(f"  [{tag}] {tool}: OK")
                    except Exception:
                        log.warning(f"  [{tag}] {tool}: check failed")

            log.info(
                f"  [{tag}] IO tool install complete"
                f" (skipped={skipped_count}, installed={installed_count})"
            )

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(len(clients), 10)
        ) as executor:
            futures = {executor.submit(_install_on_client, c): c for c in clients}
            for future in concurrent.futures.as_completed(futures):
                client = futures[future]
                tag = getattr(client, "hostname", str(client))
                try:
                    future.result()
                except Exception as e:
                    log.error(f"[{tag}] IO tool install thread failed: {e}")

        log.info(f"IO tool installation complete on {len(clients)} clients")

    # ------------------------------------------------------------------
    #  Public: Phase 2 -- Integrity baseline
    # ------------------------------------------------------------------

    def write_baseline_with_integrity(
        self,
        clients: List[Any],
        deployed_services: Set[str],
    ) -> Dict[str, Any]:
        """Write CRC/MD5 integrity baselines in the BASELINE address space.

        For mount-based clients (CephFS, NFS, SMB): writes files with known
        content via FIO with --verify=crc32c --do_verify=0.
        For block devices (RBD, NVMeoF): FIO write with offset=0.
        For RADOS: rados put objects with MD5 stored in xattr.

        Args:
            clients: List of client node objects.
            deployed_services: Set of successfully deployed service names.

        Returns:
            Summary dict with files_written, objects_written, and any errors.
        """
        result = {"files_written": 0, "objects_written": 0, "errors": []}
        integrity_cfg = self.config.get("integrity", {})
        fio_baseline_size = integrity_cfg.get("fio_baseline_size", "1G")
        rados_obj_count = integrity_cfg.get("rados_objects_per_pool", 1000)

        mount_services = [
            svc for svc in ("cephfs", "nfs", "smb") if svc in deployed_services
        ]
        for client in clients:
            if mount_services:
                self._write_mount_baseline(client, mount_services, result)
            for svc in ("rbd", "nvmeof"):
                if svc in deployed_services:
                    self._write_block_baseline(client, svc, fio_baseline_size, result)

        if "rados" in deployed_services or "rbd" in deployed_services:
            self._write_rados_baseline(rados_obj_count, result)

        log.info(
            "[integrity] Baseline complete: %d files, %d objects, %d errors",
            result["files_written"],
            result["objects_written"],
            len(result["errors"]),
        )
        return result

    # ------------------------------------------------------------------
    #  Public: Phase 2.5 -- Cluster Pre-Fill
    # ------------------------------------------------------------------

    def fill_cluster(
        self,
        fill_config: Dict[str, Any],
        deployed_services: Set[str],
    ) -> Dict[str, Any]:
        """Fill the cluster to target_percent using a two-phase hybrid approach.

        Phase 1 (Weighted Parallel): When far from the target, discovers all
        fill targets (mount points + block devices), assigns IO tools via
        weighted round-robin, and runs batched fill with auto-scaled chunk
        sizes.  Polls ``ceph df`` between batches and transitions to Phase 2
        once remaining capacity drops below *precision_threshold_pct*.

        Phase 2 (Adaptive Batch): Calculates exact per-process write sizes,
        launches one batch at a time, waits for completion, re-checks usage,
        and repeats until the target is reached or max iterations exhausted.

        Args:
            fill_config: The cluster_fill config sub-dict.
            deployed_services: Set of deployed service names.

        Returns:
            Dict with keys: skipped, start_percent, end_percent,
            target_reached, timeout_hit, abort_hit.
        """
        if not fill_config.get("enabled", True):
            log.info("[fill] Cluster fill disabled, skipping")
            return {"skipped": True}

        target_pct = fill_config.get("target_percent", 35)
        abort_pct = fill_config.get("abort_at_percent", 75)
        poll_interval = fill_config.get("poll_interval_sec", 10)
        fill_timeout = fill_config.get("fill_timeout_sec", 0)
        precision_threshold_pct = fill_config.get("precision_threshold_pct", 5)
        safety_factor = fill_config.get("safety_factor", 2)

        start_usage, total_cluster_bytes = self._get_cluster_df_stats()
        start_usage = start_usage or 0.0
        total_cluster_tib = total_cluster_bytes / (1024**4)
        fill_tolerance_pct = 0.5 if total_cluster_tib > 50 else 0.2
        effective_target = target_pct - fill_tolerance_pct
        log.info(
            "[fill] Starting fill: current=%.1f%%, target=%d%%, "
            "effective_target=%.1f%% (tolerance=%.1f%%, cluster=%.0f TiB), "
            "abort=%d%%, precision_threshold=%d%%",
            start_usage,
            target_pct,
            effective_target,
            fill_tolerance_pct,
            total_cluster_tib,
            abort_pct,
            precision_threshold_pct,
        )

        if start_usage >= effective_target:
            log.info("[fill] Already at or above effective target, skipping fill")
            return {
                "skipped": False,
                "start_percent": start_usage,
                "end_percent": start_usage,
                "target_reached": True,
            }

        targets = self._discover_fill_targets(deployed_services)
        total_processes = (
            max(1, len(targets))
            if targets
            else max(1, self._count_fill_processes(fill_config, deployed_services))
        )
        if fill_timeout == 0:
            fill_timeout = self._calculate_fill_timeout(
                start_usage, target_pct, total_processes
            )
        log.info(
            "[fill] Computed timeout: %ds, total processes: %d",
            fill_timeout,
            total_processes,
        )

        deadline = time.time() + fill_timeout
        timeout_hit = False
        abort_hit = False
        remaining_pct = effective_target - start_usage

        # Scale precision threshold so Phase 2 only handles the last ~2 TiB
        remaining_abs_bytes = remaining_pct / 100.0 * total_cluster_bytes
        if remaining_abs_bytes > 2 * 1024**4:
            effective_threshold = min(
                precision_threshold_pct,
                2 * 1024**4 / total_cluster_bytes * 100,
            )
        else:
            effective_threshold = precision_threshold_pct

        # Start RGW fill in parallel (if warp available and rgw deployed)
        if "rgw" in deployed_services and "warp" in self._get_enabled_tools("rgw"):
            self._fill_rgw(deadline)

        # ---- Phase 1: Weighted parallel fill (far from target) ----
        # Discovers all mount points / block devices, assigns IO tools via
        # weighted round-robin (_FILL_TOOL_WEIGHTS), and runs batched fill
        # with auto-scaled chunk sizes across every target simultaneously.
        if remaining_pct > effective_threshold:
            log.info(
                "[fill] Phase 1: Weighted parallel fill "
                "(remaining=%.1f%% > threshold=%.2f%%)",
                remaining_pct,
                effective_threshold,
            )
            self._fill_stop_event.clear()
            assignments = (
                self._assign_fill_tools(targets, deployed_services, fill_config)
                if targets
                else []
            )

            if assignments:
                while True:
                    if time.time() >= deadline:
                        log.warning("[fill] Timeout in Phase 1 after %ds", fill_timeout)
                        timeout_hit = True
                        break

                    current_pct = self._get_cluster_usage_percent()
                    if current_pct is None:
                        log.warning(
                            "[fill] Usage query failed, retrying in %ds",
                            poll_interval,
                        )
                        time.sleep(poll_interval)
                        continue

                    if current_pct >= effective_target:
                        log.info(
                            "[fill] Target reached in Phase 1: %.1f%%",
                            current_pct,
                        )
                        break
                    if current_pct >= abort_pct:
                        log.warning(
                            "[fill] ABORT threshold in Phase 1: %.1f%% >= %d%%",
                            current_pct,
                            abort_pct,
                        )
                        abort_hit = True
                        break

                    remaining_pct = effective_target - current_pct
                    if remaining_pct <= effective_threshold:
                        log.info(
                            "[fill] Phase 1 -> Phase 2 transition: "
                            "remaining=%.1f%% <= threshold=%.2f%%",
                            remaining_pct,
                            effective_threshold,
                        )
                        break

                    remaining_bytes = remaining_pct / 100.0 * total_cluster_bytes
                    log.info(
                        "[fill] Phase 1: usage=%.1f%%, launching weighted batch "
                        "(%d processes, %.1f GB remaining)",
                        current_pct,
                        len(assignments),
                        remaining_bytes / (1024**3),
                    )
                    self._run_weighted_fill_batch(
                        assignments,
                        remaining_bytes,
                        deadline,
                        target_pct=effective_target,
                        abort_pct=abort_pct,
                        poll_interval=poll_interval,
                    )
            else:
                log.warning(
                    "[fill] No fill targets or tools available, skipping Phase 1"
                )

        # Kill any remaining fill PIDs (warp, Phase 1 stragglers) before
        # entering precision fill.  Idempotent if Phase 1 already cleaned up.
        self._cleanup_fill_pids()

        # ---- Phase 2: Adaptive batch mode (near target) ----
        self._fill_stop_event.clear()
        current_pct = self._get_cluster_usage_percent() or 0.0
        if not abort_hit and not timeout_hit and current_pct < effective_target:
            num_procs = self._count_fill_processes(fill_config, deployed_services)
            log.info(
                "[fill] Phase 2: Adaptive batch mode (current=%.1f%%, "
                "effective_target=%.1f%%, procs=%d)",
                current_pct,
                effective_target,
                num_procs,
            )

            remaining_bytes = (
                (effective_target - current_pct) / 100.0 * total_cluster_bytes
            )
            batch_size_mb_est = int(
                remaining_bytes / num_procs / safety_factor / (1024 * 1024)
            )
            per_proc_fair_share_mb = int(
                total_cluster_bytes / max(1, num_procs) / 10 / (1024**2)
            )
            dynamic_cap = max(4096, per_proc_fair_share_mb)
            batch_size_mb_est = max(10, min(batch_size_mb_est, dynamic_cap))

            estimated_batches = (
                int(
                    remaining_bytes
                    / max(1, batch_size_mb_est * num_procs * 1024 * 1024)
                )
                + 5
            )
            max_batch_iterations = max(30, min(estimated_batches, 200))
            stall_count = 0
            prev_pct = None

            for iteration in range(1, max_batch_iterations + 1):
                if time.time() >= deadline:
                    log.warning("[fill] Timeout in Phase 2 at iteration %d", iteration)
                    timeout_hit = True
                    break

                current_pct = self._get_cluster_usage_percent()
                if current_pct is None:
                    log.warning("[fill] Usage query failed in Phase 2, retrying")
                    time.sleep(10)
                    continue

                if current_pct >= effective_target:
                    log.info(
                        "[fill] Phase 2: target reached at iteration %d (%.1f%%)",
                        iteration,
                        current_pct,
                    )
                    break
                if current_pct >= abort_pct:
                    log.warning(
                        "[fill] ABORT in Phase 2: %.1f%% >= %d%%",
                        current_pct,
                        abort_pct,
                    )
                    abort_hit = True
                    break

                if (effective_target - current_pct) < 0.1:
                    log.info(
                        "[fill] Phase 2: within 0.1%% of target "
                        "(%.3f%%), declaring success",
                        current_pct,
                    )
                    break

                if prev_pct is not None and abs(current_pct - prev_pct) < 0.1:
                    stall_count += 1
                    if stall_count >= 3:
                        log.warning(
                            "[fill] Phase 2: stalled at %.1f%% for %d "
                            "iterations, stopping early",
                            current_pct,
                            stall_count,
                        )
                        break
                else:
                    stall_count = 0
                prev_pct = current_pct

                remaining_bytes = (
                    (effective_target - current_pct) / 100.0 * total_cluster_bytes
                )
                batch_size_bytes = remaining_bytes / num_procs / safety_factor
                batch_size_mb = int(batch_size_bytes / (1024 * 1024))
                batch_size_mb = max(10, min(batch_size_mb, dynamic_cap))

                log.info(
                    "[fill] Phase 2 iteration %d/%d: usage=%.1f%%, "
                    "batch_size=%dMB/process, processes=%d",
                    iteration,
                    max_batch_iterations,
                    current_pct,
                    batch_size_mb,
                    num_procs,
                )

                self._run_adaptive_batch(
                    fill_config,
                    deployed_services,
                    batch_size_mb,
                    deadline=deadline,
                    effective_target=effective_target,
                )
            else:
                log.warning(
                    "[fill] Phase 2: max iterations (%d) exhausted",
                    max_batch_iterations,
                )

        # Final cleanup: kill any Phase 2 orphans or lingering fill PIDs
        self._cleanup_fill_pids()

        end_usage = self._get_cluster_usage_percent() or 0.0
        log.info("[fill] Fill complete: %.1f%% -> %.1f%%", start_usage, end_usage)

        return {
            "skipped": False,
            "start_percent": start_usage,
            "end_percent": end_usage,
            "target_reached": end_usage >= effective_target,
            "timeout_hit": timeout_hit,
            "abort_hit": abort_hit,
        }

    # ------------------------------------------------------------------
    #  Public: Phase 3 -- Background IO
    # ------------------------------------------------------------------

    def start_background_io(
        self,
        clients: List[Any],
        deployed_services: Set[str],
    ) -> Dict[str, Any]:
        """Start background IO in the BACKGROUND address space on all clients.

        Launches one process per enabled io_tool per client type. FIO jobs
        use --rate_iops throttling, no --verify flag. All PIDs registered.

        Args:
            clients: List of client node objects.
            deployed_services: Set of deployed service names.

        Returns:
            Summary dict with process counts per service type.
        """
        io_patterns = self.config.get("io_patterns", {})
        rwmixread = io_patterns.get("rwmixread", 70)
        timing = self.config.get("phase_timing", {})
        upgrade_timeout = timing.get("upgrade_timeout_sec", 43200)
        fio_timeout = upgrade_timeout + 1800
        percentiles = io_patterns.get(
            "tail_latency_percentiles", "50:90:95:99:99.9:99.99"
        )

        self._cached_rgw_endpoints = None
        result = {}
        for client in clients:
            for svc in deployed_services:
                tools = self._get_enabled_tools(svc)
                if not tools:
                    continue

                if svc == "nfs":
                    # NFS: distribute tools round-robin across ALL mount
                    # points so every versioned mount gets IO coverage.
                    try:
                        count = self._launch_nfs_distributed_io(
                            client, tools, rwmixread, fio_timeout, percentiles
                        )
                    except Exception as e:
                        log.warning(
                            "[bg_io] NFS distributed IO failed on %s: %s",
                            getattr(client, "hostname", client),
                            e,
                        )
                        self._io_error_counts["nfs"] = (
                            self._io_error_counts.get("nfs", 0) + 1
                        )
                        count = 0
                else:
                    count = 0
                    for tool_idx, tool in enumerate(tools):
                        try:
                            self._launch_background_process(
                                client=client,
                                service=svc,
                                tool=tool,
                                rwmixread=rwmixread,
                                fio_timeout=fio_timeout,
                                percentiles=percentiles,
                                mount_index=tool_idx,
                            )
                            count += 1
                        except Exception as e:
                            log.warning(
                                "[bg_io] Failed to start %s/%s on %s: %s",
                                svc,
                                tool,
                                getattr(client, "hostname", client),
                                e,
                            )
                            self._io_error_counts[svc] = (
                                self._io_error_counts.get(svc, 0) + 1
                            )
                result[svc] = result.get(svc, 0) + count

        log.info("[bg_io] Started background IO: %s", result)
        return result

    # ------------------------------------------------------------------
    #  Public: Capacity Guard
    # ------------------------------------------------------------------

    def start_capacity_guard(self, max_percent: int = 70, check_interval: int = 120):
        """Start a background thread that kills IO if cluster usage exceeds max_percent.

        The guard polls ``_get_cluster_usage_percent()`` every *check_interval*
        seconds.  When usage breaches the threshold it SIGKILL-s all
        background-phase PIDs in the registry and sets ``_capacity_breach``.
        One-shot: after killing, the thread exits.
        """
        self._cap_guard_stop = threading.Event()
        self._capacity_breach = False
        self._cap_guard_max = max_percent
        self._cap_guard_thread = threading.Thread(
            target=self._capacity_guard_loop,
            args=(max_percent, check_interval),
            daemon=True,
        )
        self._cap_guard_thread.start()
        log.info(
            "[CAPACITY_GUARD] Started: kill bg IO if cluster >= %d%% "
            "(check every %ds)",
            max_percent,
            check_interval,
        )

    def stop_capacity_guard(self):
        """Signal the capacity guard thread to exit."""
        if hasattr(self, "_cap_guard_stop"):
            self._cap_guard_stop.set()
            log.info("[CAPACITY_GUARD] Stopped")

    def _capacity_guard_loop(self, max_percent: int, check_interval: int):
        while not self._cap_guard_stop.wait(timeout=check_interval):
            try:
                usage = self._get_cluster_usage_percent()
            except Exception:
                continue
            if usage is None:
                continue
            log.debug("[CAPACITY_GUARD] Cluster usage: %.1f%%", usage)
            if usage >= max_percent:
                killed = self._kill_background_pids()
                self._capacity_breach = True
                log.warning(
                    "[CAPACITY_GUARD] Cluster at %.1f%% >= %d%%, "
                    "killed %d background IO processes",
                    usage,
                    max_percent,
                    killed,
                )
                break

    def _kill_background_pids(self) -> int:
        """SIGKILL all PIDs registered with phase='background'."""
        with self._registry_lock:
            bg_entries = [
                (client, pid)
                for (client, pid), meta in self._pid_registry.items()
                if meta.get("phase") == "background"
            ]
        killed = 0
        pids_by_client: Dict[Any, List[int]] = {}
        for client, pid in bg_entries:
            pids_by_client.setdefault(client, []).append(pid)

        for client, pids in pids_by_client.items():
            kill_cmd = " ; ".join(
                f"kill -9 {p} 2>/dev/null && echo killed:{p}" for p in pids
            )
            try:
                out, _ = client.exec_command(
                    sudo=True, cmd=kill_cmd, timeout=30, check_ec=False
                )
                for line in (out or "").strip().split("\n"):
                    if line.strip().startswith("killed:"):
                        killed += 1
            except Exception:
                pass

        with self._registry_lock:
            for client, pid in bg_entries:
                self._pid_registry.pop((client, pid), None)
        return killed

    # ------------------------------------------------------------------
    #  Public: Process Management
    # ------------------------------------------------------------------

    def stop_io_processes(self) -> Dict[str, Any]:
        """Gracefully stop all registered IO processes.

        Sends SIGTERM, waits io_kill_timeout_sec (default 30s), then
        SIGKILL for any survivors. Does NOT unmount filesystems.
        Snapshots background-phase metadata for later summarisation
        via ``collect_io_outputs()``.

        Returns:
            Summary with terminated/killed/failed counts.
        """
        timing = self.config.get("phase_timing", {})
        kill_timeout = timing.get("io_kill_timeout_sec", 30)
        result = {"terminated": 0, "killed": 0, "failed": 0}

        with self._registry_lock:
            pids_by_client: Dict[Any, List[int]] = {}
            for client, pid in self._pid_registry:
                pids_by_client.setdefault(client, []).append(pid)
            self._bg_log_snapshot = [
                (client, dict(meta))
                for (client, _pid), meta in self._pid_registry.items()
                if meta.get("phase") == "background" and meta.get("log_path")
            ]

        for client, pids in pids_by_client.items():
            for pid in pids:
                try:
                    out, _ = client.exec_command(
                        sudo=True,
                        cmd=f"kill -TERM {pid} 2>/dev/null && echo ok || echo gone",
                        timeout=10,
                        check_ec=False,
                    )
                    if "ok" in str(out):
                        result["terminated"] += 1
                except Exception:
                    pass

        time.sleep(kill_timeout)

        for client, pids in pids_by_client.items():
            for pid in pids:
                try:
                    out, _ = client.exec_command(
                        sudo=True,
                        cmd=f"kill -0 {pid} 2>/dev/null && echo alive",
                        timeout=10,
                        check_ec=False,
                    )
                    if "alive" in str(out):
                        client.exec_command(
                            sudo=True,
                            cmd=f"kill -KILL {pid}",
                            timeout=10,
                            check_ec=False,
                        )
                        result["killed"] += 1
                except Exception:
                    result["failed"] += 1

        with self._registry_lock:
            self._pid_registry.clear()

        if any(self._io_error_counts.values()):
            log.warning(
                "[io] Error counts during background IO: %s",
                dict(self._io_error_counts),
            )

        log.info(
            "[stop_io] Terminated=%d, Killed=%d, Failed=%d",
            result["terminated"],
            result["killed"],
            result["failed"],
        )
        return result

    def kill_all_registered_processes(self) -> int:
        """Force-kill (SIGKILL) every process in the PID registry.

        Fallback method for the finally block. Silently ignores
        already-dead processes.

        Returns:
            Number of kill attempts made.
        """
        count = 0
        with self._registry_lock:
            entries = list(self._pid_registry.items())

        for (client, pid), meta in entries:
            try:
                client.exec_command(
                    sudo=True,
                    cmd=f"kill -KILL {pid}",
                    timeout=10,
                    check_ec=False,
                )
            except Exception:
                pass
            count += 1

        log.info("[kill_all] Force-killed %d registered processes", count)
        with self._registry_lock:
            self._pid_registry.clear()
        self._cleanup_all_by_prefix()
        return count

    def _pkill_client(self, client):
        """Kill ugt- processes on a single client (best-effort, non-fatal).

        Runs pkill -9 (immediate kill) with a tight timeout. If it fails or
        times out, processes will die naturally or be cleaned up by the OS.
        """
        try:
            client.exec_command(
                sudo=True,
                cmd="pkill -9 -f '[u]gt-' 2>/dev/null || true",
                timeout=10,
                check_ec=False,
            )
        except Exception:
            log.warning(
                "[_pkill_client] Timeout or error killing ugt- on %s "
                "(best-effort; processes will die naturally)",
                client.hostname,
            )

    def _cleanup_all_by_prefix(self):
        """Safety-net: kill any orphan processes matching the ugt- prefix.

        Parallel per-node cleanup: each client gets 10s exec timeout. Wall-clock
        capped at 15s; hung SSH threads are abandoned (wait=False) so one wedged
        transport cannot block the finally block. Best-effort / non-fatal.
        """
        clients = self.ceph_cluster.get_nodes(role="client")
        if not clients:
            log.debug("[_cleanup_all_by_prefix] No client nodes to clean up")
            return

        log.info(
            "[_cleanup_all_by_prefix] Starting parallel pkill on %d clients",
            len(clients),
        )
        done_count = 0
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(clients))
        try:
            futures = {
                executor.submit(self._pkill_client, client): client
                for client in clients
            }
            try:
                for future in concurrent.futures.as_completed(futures, timeout=15):
                    client = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        log.warning(
                            "[_cleanup_all_by_prefix] %s failed: %s",
                            client.hostname,
                            e,
                        )
                    done_count += 1
            except TimeoutError:
                pending = len(futures) - done_count
                log.warning(
                    "[_cleanup_all_by_prefix] 15s wall-clock cap hit; "
                    "abandoning %d remaining pkill(s)",
                    pending,
                )
        finally:
            # Do not join hung SSH workers; finally must return. Workers exit
            # when exec_command times out (~10s) or linger if transport is wedged.
            executor.shutdown(wait=False, cancel_futures=True)

        log.info(
            "[_cleanup_all_by_prefix] Parallel pkill finished: %d/%d clients done",
            done_count,
            len(clients),
        )

    # ------------------------------------------------------------------
    #  Public: IO Output Collection
    # ------------------------------------------------------------------

    def collect_io_outputs(self) -> Dict[str, Any]:
        """Build a tool-usage summary from the PID registry snapshot.

        Must be called AFTER ``stop_io_processes()``.  Uses the log-path
        snapshot taken at stop time to extract which tools actually ran
        on which clients and services.

        Returns:
            Dict with ``"tools_used"`` -- a flat list of
            ``{"tool", "service", "hostname"}`` dicts for rendering a
            Client x Service x Tool matrix in the report.
        """
        snapshot = self._bg_log_snapshot
        if not snapshot:
            log.info("[collect_io] No background IO log paths recorded")
            return {"tools_used": []}

        tools_used: List[Dict[str, str]] = []
        seen: Set[Tuple[str, str, str]] = set()
        for client, meta in snapshot:
            hostname = getattr(client, "hostname", str(client))
            tool = meta.get("tool", "")
            service = meta.get("service", "")
            if not tool:
                continue
            key = (hostname, service, tool)
            if key not in seen:
                seen.add(key)
                tools_used.append(
                    {"tool": tool, "service": service, "hostname": hostname}
                )

        log.info("[collect_io] %d unique IO tool instances recorded", len(tools_used))
        return {"tools_used": tools_used}

    def scrape_nfs_io_errors(self) -> Dict[str, Any]:
        """Grep nfs background IO logs for hard errors (post stop_io_processes)."""
        snapshot = self._bg_log_snapshot
        if not snapshot:
            return {"hits": [], "count": 0}

        hits: List[Dict[str, str]] = []
        err_pattern = r"Input/output error|ESTALE|Stale file handle|err=\s*[1-9][0-9]*"
        for client, meta in snapshot:
            if meta.get("service") != "nfs":
                continue
            log_path = meta.get("log_path", "")
            if not log_path:
                continue
            hostname = getattr(client, "hostname", str(client))
            tool = meta.get("tool", "")
            try:
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=(
                        "grep -iE "
                        f"{shlex.quote(err_pattern)} "
                        f"{shlex.quote(log_path)} 2>/dev/null | head -20 || true"
                    ),
                    timeout=30,
                    check_ec=False,
                )
            except Exception as err:
                log.warning(
                    "[nfs_io_errors] grep failed on %s %s: %s",
                    hostname,
                    log_path,
                    err,
                )
                continue
            for line in (out or "").strip().splitlines():
                line = line.strip()
                if not line:
                    continue
                hits.append(
                    {
                        "hostname": hostname,
                        "tool": tool,
                        "log_path": log_path,
                        "line": line[:500],
                    }
                )

        log.info("[nfs_io_errors] %d hard error line(s) in nfs IO logs", len(hits))
        return {"hits": hits, "count": len(hits)}

    # ------------------------------------------------------------------
    #  Public: Storage Teardown
    # ------------------------------------------------------------------

    # One SSH per client. Sequential per-mount SSH wedged cali015 under load.
    _CLIENT_TEARDOWN_SCRIPT = r"""
set +e
killall -9 fio dd smallfile iozone dbench bonnie++ mdtest vdbench rados 2>/dev/null
pkill -9 -f '[b]oto3|[s]3cmd|[w]arp|[s]mallfile_cli|[r]ados_bench|[u]gt-' 2>/dev/null
for mp in $(mount 2>/dev/null | awk '$3 ~ /^\/mnt\// {print $3}'); do
  fuser -km "$mp" 2>/dev/null
done
sleep 2
for mp in $(mount -t nfs,nfs4,cifs,ceph,fuse.ceph-fuse 2>/dev/null | awk '{print $3}') \
          $(mount 2>/dev/null | awk '$3 ~ /^\/mnt\// {print $3}'); do
  umount -lf "$mp" 2>/dev/null
done
rbd unmap --all 2>/dev/null
nvme disconnect-all 2>/dev/null
echo "__REMAINING_MOUNTS__"
mount -t nfs,nfs4,cifs,ceph,fuse.ceph-fuse 2>/dev/null || true
echo "__END_REMAINING__"
exit 0
"""

    def cleanup_mounts_and_connections(self) -> bool:
        """Hard-kill IO, lazy-umount, unmap — one remote script per client.

        Returns True when every *reachable* client has no nfs/ceph/fuse/cifs
        mounts left. Unreachable clients are abandoned (do not block). Incomplete
        scrape markers → unsafe (do not claim mounts cleared).
        """
        clients = self.ceph_cluster.get_nodes(role="client")
        mounts_remaining: Dict[str, List[str]] = {}
        unreachable: List[str] = []

        for client in clients:
            hostname = getattr(client, "hostname", str(client))
            try:
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=(
                        "bash -s <<'EOFTEARDOWN'\n"
                        f"{self._CLIENT_TEARDOWN_SCRIPT}"
                        "\nEOFTEARDOWN"
                    ),
                    timeout=180,
                    check_ec=False,
                )
            except Exception as e:
                log.warning(
                    "[cleanup] Client %s unreachable/failed (abandoning): %s",
                    hostname,
                    e,
                )
                unreachable.append(hostname)
                continue

            text = str(out or "")
            if "__REMAINING_MOUNTS__" not in text or "__END_REMAINING__" not in text:
                mounts_remaining[hostname] = ["(teardown output incomplete)"]
                log.warning(
                    "[cleanup] %s teardown output incomplete — treating as mounted",
                    hostname,
                )
                continue

            chunk = text.split("__REMAINING_MOUNTS__", 1)[1]
            chunk = chunk.split("__END_REMAINING__", 1)[0]
            remaining = [ln.strip() for ln in chunk.splitlines() if ln.strip()]
            if remaining:
                mounts_remaining[hostname] = remaining
                log.warning(
                    "[cleanup] %s still has %d storage mount(s): %s",
                    hostname,
                    len(remaining),
                    remaining[:5],
                )
            else:
                log.info("[cleanup] %s storage mounts cleared", hostname)

        if unreachable:
            log.warning(
                "[cleanup] Unreachable clients (abandoned for cluster cleanup): %s",
                unreachable,
            )
        if mounts_remaining:
            log.error(
                "[cleanup] Not safe for cluster cleanup; mounts remain: %s",
                mounts_remaining,
            )
        return not mounts_remaining

    # ------------------------------------------------------------------
    #  Public: Phase 6 -- Verification
    # ------------------------------------------------------------------

    def verify_all_integrity(
        self,
        clients: List[Any],
        deployed_services: Set[str],
    ) -> Dict[str, Any]:
        """Re-read baseline data and verify checksums match.

        Parallelizes verification across clients using ThreadPoolExecutor.
        Each thread handles one client, running a single batched SSH call for
        all mount-based services (reducing ~140 SSH round-trips per client to
        1) and individual calls for block devices.  Results from all threads
        are merged into the final result dict.

        Must be called AFTER stop_io_processes() and io_quiesce_sec wait.

        Args:
            clients: Client nodes with mounts still active.
            deployed_services: Set of deployed service names.

        Returns:
            Dict with total checked, mismatches, and errors per service.
        """
        result = {
            "total_checked": 0,
            "mismatches": [],
            "errors": [],
            "services": {},
        }

        mount_services = [
            svc for svc in ("cephfs", "nfs", "smb") if svc in deployed_services
        ]
        block_services = [svc for svc in ("rbd", "nvmeof") if svc in deployed_services]

        if "rbd" in block_services:
            # RBD integrity lives on rep_pool; waiting on the whole cluster
            # (RGW/CephFS remaps) false-skips verify when RBD itself is clean.
            clean_wait = int(self._integrity.get("pool_clean_wait_timeout", 600))
            log.info(
                "[verify] Waiting for rep_pool active+clean before RBD "
                "integrity (timeout=%ss)",
                clean_wait,
            )
            try:
                clean = self.rados_obj.wait_for_clean_pg_sets(
                    timeout=clean_wait,
                    sleep_interval=15,
                    test_pool="rep_pool",
                    recovery_thread=False,
                )
            except Exception as e:
                log.warning("[verify] rep_pool active+clean wait raised: %s", e)
                clean = False
            if not clean:
                log.warning(
                    "[verify] rep_pool not active+clean within %ss; "
                    "skipping RBD block integrity verify",
                    clean_wait,
                )
                result["errors"].append(
                    "cluster_not_ready:rep_pool_active+clean_timeout"
                )
                block_services = [s for s in block_services if s != "rbd"]

        def _verify_single_client(client):
            partial = {"total_checked": 0, "mismatches": [], "errors": []}
            if mount_services:
                self._verify_mount_integrity(client, mount_services, partial)
            for svc in block_services:
                self._verify_block_integrity(client, svc, partial)
            return partial

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, len(clients))
        ) as executor:
            futures = {executor.submit(_verify_single_client, c): c for c in clients}
            for fut in concurrent.futures.as_completed(futures):
                client = futures[fut]
                hostname = getattr(client, "hostname", str(client))
                try:
                    partial = fut.result()
                    result["total_checked"] += partial["total_checked"]
                    result["mismatches"].extend(partial["mismatches"])
                    result["errors"].extend(partial["errors"])
                except Exception as e:
                    log.error(
                        "[verify] Client %s verification failed: %s",
                        hostname,
                        e,
                    )
                    result["errors"].append(f"client:{hostname}:{e}")
                    result["verification_incomplete"] = True
                    result.setdefault("unreachable_clients", []).append(hostname)

        if "rados" in deployed_services or "rbd" in deployed_services:
            self._verify_rados_integrity(result)

        if result["total_checked"] == 0 and deployed_services:
            log.warning(
                "[verify] WARNING: 0 items checked despite deployed services %s. "
                "Mounts or block devices may have been lost.",
                deployed_services,
            )
            result["errors"].append(
                "zero_items_checked: no mounts/devices found for verification"
            )

        if result.get("verification_incomplete"):
            log.warning(
                "[verify] VERIFICATION_INCOMPLETE: %d client(s) unreachable: %s",
                len(result.get("unreachable_clients", [])),
                result.get("unreachable_clients"),
            )

        log.info(
            "[verify] Integrity check: %d checked, %d mismatches, %d errors",
            result["total_checked"],
            len(result["mismatches"]),
            len(result["errors"]),
        )
        return result

    def check_mount_health(
        self,
        clients: List[Any],
        deployed_services: Set[str],
        stat_timeout: int = 15,
    ) -> Dict[str, Any]:
        """Verify all mounts are accessible, attempt remount if stale.

        Phase 6 Step 1.5: Confirms mounts are not stale before running
        integrity verification. For NFS and CephFS stale mounts, captures
        mount info, performs lazy umount, then attempts a timed remount.
        Other stale mounts (SMB) get lazy umount only.

        Clients are checked in parallel (one thread per client) to reduce
        wall-clock time.  The initial stat probe uses *stat_timeout* (default
        15s); recovery remounts keep the longer 30s default via
        ``_attempt_remount``.

        Returns:
            Dict with healthy/stale_remounted/stale_unrecoverable/stale
            lists per service type.
        """

        def _check_single_client(
            client,
        ) -> Dict[str, Dict[str, List[str]]]:
            local: Dict[str, Dict[str, List[str]]] = {}
            hostname = getattr(client, "hostname", str(client))
            for svc in ("cephfs", "nfs", "smb"):
                if svc not in deployed_services:
                    continue
                mount_points = self._get_mount_points(client, svc)
                if not mount_points:
                    continue

                svc_result = local.setdefault(
                    svc,
                    {
                        "healthy": [],
                        "stale_remounted": [],
                        "stale_unrecoverable": [],
                        "stale": [],
                    },
                )
                for mp in mount_points:
                    if self._is_mount_healthy(client, mp, timeout=stat_timeout):
                        svc_result["healthy"].append(mp)
                        continue

                    log.warning(
                        "[mount_health] Stale mount detected: " "%s on %s (svc=%s)",
                        mp,
                        hostname,
                        svc,
                    )

                    if svc in ("nfs", "cephfs"):
                        mount_info = self._get_mount_info(client, mp, svc)
                        self._attempt_lazy_umount(client, mp)
                        if mount_info and self._attempt_remount(
                            client, mp, mount_info, svc
                        ):
                            svc_result["stale_remounted"].append(mp)
                            log.info(
                                "[mount_health] %s mount recovered "
                                "via remount: %s on %s",
                                svc.upper(),
                                mp,
                                hostname,
                            )
                            continue
                        log.error(
                            "[mount_health] %s mount UNRECOVERABLE:" " %s on %s",
                            svc.upper(),
                            mp,
                            hostname,
                        )
                        svc_result["stale_unrecoverable"].append(mp)
                    else:
                        self._attempt_lazy_umount(client, mp)
                        svc_result["stale"].append(mp)
            return local

        result: Dict[str, Dict[str, List[str]]] = {}
        workers = max(1, len(clients))
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_check_single_client, c): c for c in clients}
            for fut in concurrent.futures.as_completed(futures):
                client = futures[fut]
                hostname = getattr(client, "hostname", str(client))
                try:
                    local = fut.result()
                except Exception as e:
                    log.error(
                        "[mount_health] Health check failed for %s: %s",
                        hostname,
                        e,
                    )
                    continue
                for svc, buckets in local.items():
                    merged = result.setdefault(
                        svc,
                        {
                            "healthy": [],
                            "stale_remounted": [],
                            "stale_unrecoverable": [],
                            "stale": [],
                        },
                    )
                    for key in buckets:
                        merged[key].extend(buckets[key])

        return result

    def capture_background_pid_snapshot(self) -> Dict:
        """Return a copy of background-phase PID registry entries."""
        with self._registry_lock:
            return {
                key: dict(meta)
                for key, meta in self._pid_registry.items()
                if meta.get("phase") == "background"
            }

    def wait_for_background_io_continuity(
        self,
        clients: List[Any],
        deployed_services: Set[str],
        deadline_sec: int = 300,
        interval: int = 15,
        bg_snapshot: Dict = None,
    ) -> Dict[str, Any]:
        """Poll until *bg_snapshot* PIDs are alive and mounts are healthy.

        Uses ``check_mount_health()`` (stat + remount) — same path as Phase 6.
        *bg_snapshot* should be captured before disruptive work; defaults to a
        fresh snapshot when omitted.
        """
        snapshot = (
            bg_snapshot
            if bg_snapshot is not None
            else self.capture_background_pid_snapshot()
        )
        started = time.time()
        deadline = started + deadline_sec
        last: Dict[str, Any] = {}

        def _pids_alive() -> Tuple[bool, List[Dict[str, Any]]]:
            dead: List[Dict[str, Any]] = []
            for (client, pid), meta in snapshot.items():
                try:
                    client.exec_command(
                        sudo=True,
                        cmd=f"kill -0 {pid}",
                        timeout=10,
                        check_ec=False,
                    )
                    if client.exit_status != 0:
                        dead.append(
                            {
                                "pid": pid,
                                "client": getattr(client, "hostname", str(client)),
                                "service": meta.get("service"),
                                "tool": meta.get("tool"),
                            }
                        )
                except Exception as exc:
                    dead.append(
                        {
                            "pid": pid,
                            "client": getattr(client, "hostname", str(client)),
                            "error": str(exc),
                        }
                    )
            return not dead, dead

        while time.time() < deadline:
            pids_ok, dead = _pids_alive()
            mount_result = self.check_mount_health(
                clients, deployed_services, stat_timeout=15
            )
            mount_ok = not any(
                mount_result.get(svc, {}).get("stale_unrecoverable")
                or mount_result.get(svc, {}).get("stale")
                for svc in mount_result
            )
            last = {
                "snapshot_count": len(snapshot),
                "background_pids_alive": pids_ok,
                "mount_health_ok": mount_ok,
                "dead_pids": dead,
            }
            if pids_ok and mount_ok:
                last["ok"] = True
                last["recovery_sec"] = round(time.time() - started, 1)
                return last
            time.sleep(interval)

        last["ok"] = False
        last["recovery_sec"] = round(time.time() - started, 1)
        return last

    # ------------------------------------------------------------------
    #  Internal: PID Registry
    # ------------------------------------------------------------------

    def _register_pid(
        self,
        client: Any,
        pid: int,
        service: str,
        tool: str,
        phase: str,
        log_path: str = "",
    ):
        """Register a background process PID for lifecycle tracking.

        Args:
            client: Node handle where the process runs.
            pid: OS process ID.
            service: Service type (e.g., "cephfs", "rbd").
            tool: IO tool name (e.g., "fio", "smallfile").
            phase: Current test phase ("fill" or "background").
            log_path: Remote path to the process's stdout/stderr log file.
        """
        with self._registry_lock:
            self._pid_registry[(client, pid)] = {
                "service": service,
                "tool": tool,
                "phase": phase,
                "log_path": log_path,
                "registered_at": datetime.now().isoformat(),
            }
        log.debug("[pid] Registered PID %d (%s/%s) phase=%s", pid, service, tool, phase)

    def _fill_rgw(self, deadline: float) -> None:
        """Run warp put to push data to RGW during fill phase.

        Launches a time-limited ``warp put`` (write-only) process on each
        client node.  Duration is capped at ``deadline - now`` so the
        process stops with the fill phase.  Registered with phase="fill"
        so ``_cleanup_fill_pids()`` can kill it if needed.
        """
        endpoints = self._resolve_all_rgw_endpoints()
        if not endpoints:
            log.info("[fill] No RGW endpoint, skipping RGW fill")
            return
        access_key = self.config.get("rgw_access_key", "iokey")
        secret_key = self.config.get("rgw_secret_key", "iosecret")
        remaining_sec = max(60, int(deadline - time.time()))
        concurrent = self._fill_params.get(
            "warp_fill_concurrent", self._bg_params.get("warp_concurrent", 8)
        )
        obj_size = self._fill_params.get(
            "warp_fill_obj_size", self._bg_params.get("warp_obj_size", "1MiB")
        )
        clients = self.ceph_cluster.get_nodes(role="client")
        for ci, client in enumerate(clients):
            rgw_endpoint = endpoints[ci % len(endpoints)]
            hostname = getattr(client, "hostname", str(client))
            fill_bucket = f"ugt-fill-rgw-{hostname}"
            cmd = (
                f"warp put --host={rgw_endpoint} "
                f"--access-key={access_key} --secret-key={secret_key} "
                f"--bucket={fill_bucket} "
                f"--concurrent={concurrent} "
                f"--obj.size={obj_size} "
                f"--duration={remaining_sec}s "
                f"--noclear --no-color --stress"
            )
            pid, log_path = self._exec_background(client, cmd, log_tag="fill_rgw")
            if pid:
                self._register_pid(
                    client,
                    pid,
                    service="rgw",
                    tool="warp",
                    phase="fill",
                    log_path=log_path,
                )
                log.info(
                    "[fill] RGW fill launched on %s (pid=%d)",
                    getattr(client, "hostname", str(client)),
                    pid,
                )

    def _cleanup_fill_pids(self):
        """Remove fill-phase PIDs from registry to avoid stale PID reuse.

        Called between fill phases and after fill completion. Kills any
        survivors and purges their entries from the PID registry.
        """
        with self._registry_lock:
            fill_entries = [
                (key, meta)
                for key, meta in self._pid_registry.items()
                if meta.get("phase") == "fill"
            ]
        for (client, pid), _meta in fill_entries:
            try:
                client.exec_command(
                    sudo=True,
                    cmd=f"kill -KILL {pid} 2>/dev/null || true",
                    timeout=10,
                    check_ec=False,
                )
            except Exception:
                pass
        with self._registry_lock:
            for key, _meta in fill_entries:
                self._pid_registry.pop(key, None)
        if fill_entries:
            log.info("[fill] Cleaned up %d fill PIDs from registry", len(fill_entries))

    # ------------------------------------------------------------------
    #  Internal: Baseline Writes
    # ------------------------------------------------------------------

    def _batch_mount_ssh_timeout(self, n_mounts: int) -> int:
        """Outer SSH budget for sequential per-mount fio (write or verify)."""
        per = int(self._integrity.get("verify_timeout", 300))
        return n_mounts * (per + 30) + 120

    def _select_integrity_mounts(
        self, all_mounts: List[Tuple[str, str]], hostname: str
    ) -> List[Tuple[str, str]]:
        """Return mounts for integrity write/verify (same sample both sides).

        If a sample was already chosen for *hostname* (write phase), reuse it
        intersected with mounts still present. Otherwise sample up to
        ``integrity.max_mounts_to_check`` (default 25) and persist.
        """
        max_n = int(self._integrity.get("max_mounts_to_check", 25))
        stored = self._integrity_mount_sample.get(hostname)
        if stored is not None:
            present = set(all_mounts)
            chosen = [x for x in stored if x in present]
            if not chosen:
                log.warning(
                    "[integrity] Stored sample for %s has 0 mounts still present "
                    "(%d current); skipping",
                    hostname,
                    len(all_mounts),
                )
            else:
                log.info(
                    "[integrity] Reusing %d/%d sampled mounts on %s (max=%d)",
                    len(chosen),
                    len(all_mounts),
                    hostname,
                    max_n,
                )
            return chosen

        if len(all_mounts) <= max_n:
            chosen = list(all_mounts)
        else:
            chosen = random.sample(list(all_mounts), max_n)
        self._integrity_mount_sample[hostname] = chosen
        log.info(
            "[integrity] Using %d/%d mounts on %s (max=%d): %s",
            len(chosen),
            len(all_mounts),
            hostname,
            max_n,
            [m for _, m in chosen],
        )
        return chosen

    @staticmethod
    def _batched_fail_reason(exc: BaseException, timeout: int) -> str:
        """Non-empty reason for batched SSH failures (socket.timeout often '')."""
        return remote_exec_failure_tag(exc, timeout)

    def _heavy_client_ssh(self, client: Any, **kwargs):
        """Cap concurrent long-running client SSH sessions during verify."""
        with self._heavy_ssh_sem:
            return client.exec_command(**kwargs)

    def _write_mount_baseline(self, client: Any, services: List[str], result: Dict):
        """Write FIO integrity files for all mount-based services in one SSH call.

        Instead of issuing N*10 individual SSH+fio commands (N mounts, 10 file
        specs each), this builds a single bash script that:

        1. ``mkdir -p`` all integrity directories.
        2. Generates a fio job file per mount with ``[global]`` (rw=write,
           verify=crc32c, do_verify=0) and individual ``[job]`` sections for
           each of the 10 file specs from ``_INTEGRITY_FILE_SPECS``.
        3. Runs ``fio <jobfile>`` once per mount.
        4. Emits structured WRITTEN/FAIL lines delimited by markers for
           reliable parsing.

        Args:
            client: Client node object.
            services: Mount-based service names (e.g. ["cephfs", "nfs", "smb"]).
            result: Accumulator dict with 'files_written' and 'errors' keys.
        """
        all_mounts: List[Tuple[str, str]] = []
        for svc in services:
            for mp in self._get_mount_points(client, svc):
                all_mounts.append((svc, mp))

        if not all_mounts:
            return

        hostname = getattr(client, "hostname", str(client))
        all_mounts = self._select_integrity_mounts(all_mounts, hostname)
        if not all_mounts:
            return

        num_specs = len(_INTEGRITY_FILE_SPECS)

        script_lines = ['echo "###WRITE_START###"']

        for _svc, mp in all_mounts:
            integrity_dir = f"{mp}/{INTEGRITY_DIR}"
            sanitized = mp.replace("/", "_").strip("_")
            job_file = f"/tmp/write_{sanitized}.fio"

            script_lines.append(f'if ! timeout 15 stat "{mp}" >/dev/null 2>&1; then')
            script_lines.append(f'  echo "IO_CHECK_FAIL:{mp}:stale_or_hung"')
            script_lines.append(f'elif ! mkdir -p "{integrity_dir}"; then')
            script_lines.append(f'  echo "IO_CHECK_FAIL:{mp}:mkdir_failed"')
            script_lines.append("else")

            fio_content = self._build_fio_job_content(integrity_dir, "write", False)
            script_lines.append(f"  cat > {job_file} << 'FIOEOF'")
            script_lines.append(fio_content)
            script_lines.append("FIOEOF")

            fio_verify_tmout = self._integrity.get("verify_timeout", 300)
            script_lines.append(
                f"  fio_out=$(timeout {fio_verify_tmout} fio {job_file} 2>&1)"
            )
            script_lines.append("  fio_rc=$?")
            script_lines.append("  if [ $fio_rc -ne 0 ]; then")
            script_lines.append(f'    echo "IO_CHECK_FAIL:{mp}:fio_exit_code_$fio_rc"')
            script_lines.append("  else")
            script_lines.append(f'    echo "IO_CHECK_OK:{mp}:{num_specs}"')
            script_lines.append("  fi")
            script_lines.append(f"  rm -f {job_file}")
            script_lines.append("fi")

        script_lines.append('echo "###WRITE_END###"')
        script = "\n".join(script_lines)

        timeout = self._batch_mount_ssh_timeout(len(all_mounts))
        log.info(
            "[integrity] Writing baselines for %d mounts on %s (ssh_timeout=%ds)",
            len(all_mounts),
            hostname,
            timeout,
        )

        try:
            cmd = f"bash << 'WRITE_SCRIPT_EOF'\n{script}\nWRITE_SCRIPT_EOF"
            out, _ = client.exec_command(
                sudo=True, cmd=cmd, timeout=timeout, check_ec=False
            )
            self._parse_write_output(out, all_mounts, result, hostname)
        except Exception as e:
            reason = self._batched_fail_reason(e, timeout)
            log.error(
                "[integrity] Batched baseline write failed on %s: %s",
                hostname,
                reason,
            )
            for svc, mp in all_mounts:
                result["errors"].append(f"{svc}:{mp}:batched_failed:{reason}")

    def _parse_write_output(
        self,
        output: str,
        all_mounts: List[Tuple[str, str]],
        result: Dict,
        hostname: str,
    ):
        """Parse structured IO_CHECK_OK/IO_CHECK_FAIL output from the batched write script."""
        text = str(output)
        start = text.find("###WRITE_START###")
        end = text.find("###WRITE_END###")
        if start == -1 or end == -1:
            log.warning("[integrity] Missing markers in write output on %s", hostname)
            result["errors"].append(f"missing_write_markers:{hostname}")
            return

        body = text[start + len("###WRITE_START###") : end]
        reported_mounts: Set[str] = set()

        for line in body.strip().splitlines():
            line = line.strip()
            if line.startswith("IO_CHECK_OK:"):
                parts = line.split(":", 2)
                if len(parts) >= 3:
                    mp = parts[1]
                    count = int(parts[2]) if parts[2].isdigit() else 0
                    result["files_written"] += count
                    reported_mounts.add(mp)
                    log.info(
                        "[integrity] Written baseline: %s/%s",
                        mp,
                        INTEGRITY_DIR,
                    )
            elif line.startswith("IO_CHECK_FAIL:"):
                parts = line.split(":", 2)
                mp = parts[1] if len(parts) >= 2 else "unknown"
                detail = (parts[2] if len(parts) >= 3 else "") or "unknown"
                reported_mounts.add(mp)
                svc = next((s for s, m in all_mounts if m == mp), "unknown")
                log.warning("[integrity] Mount baseline failed at %s: %s", mp, detail)
                result["errors"].append(f"{svc}:{mp}:{detail}")

        for svc, mp in all_mounts:
            if mp not in reported_mounts:
                log.warning("[integrity] No output for mount %s on %s", mp, hostname)
                result["errors"].append(f"{svc}:{mp}:no_output")

    def _write_block_baseline(self, client: Any, service: str, size: str, result: Dict):
        """Write FIO integrity data to block devices (RBD/NVMeoF).

        Only writes baselines to devices dedicated to integrity testing,
        skipping any device whose image name contains "background" (those are
        actively written to by background IO during the upgrade and would
        produce false CRC mismatches at verification time).

        Records baselined targets as {pool, name} for RBD (device path is
        resolved again at verify) or {device} for NVMeoF.
        """
        entries = self._get_integrity_block_devices(client, service)
        block_write_timeout = self._integrity.get("write_timeout", 600)
        for entry in entries:
            dev = entry["device"]
            label = (
                f"{entry['pool']}/{entry['name']}"
                if entry.get("pool") and entry.get("name")
                else dev
            )
            try:
                fio_cmd = (
                    f"fio --name=ugt-baseline-{service}-fio --filename={dev} "
                    f"--offset=0 --size={size} --bs=4M --rw=write "
                    f"--numjobs=1 --verify=crc32c "
                    f"--do_verify=0 --ioengine=libaio --direct=1"
                )
                client.exec_command(sudo=True, cmd=fio_cmd, timeout=block_write_timeout)
                result["files_written"] += 1

                baselined = self._integrity_checksums.setdefault(
                    "block_baselined_devices", {}
                )
                hostname = getattr(client, "hostname", str(client))
                client_baselined = baselined.setdefault(hostname, [])
                if entry.get("pool") and entry.get("name"):
                    key = {"pool": entry["pool"], "name": entry["name"]}
                else:
                    key = {"device": dev}
                if key not in client_baselined:
                    client_baselined.append(key)
                log.info("[integrity] Block baseline written: %s (%s)", label, dev)
            except Exception as e:
                log.warning("[integrity] Block baseline failed %s: %s", label, e)
                result["errors"].append(f"{service}:{label}:{e}")

    def _write_rados_baseline(self, obj_count: int, result: Dict):
        """Write RADOS objects with MD5 stored in xattr."""
        pool_name = self._get_integrity_pool()
        installer = self.installer

        batch_size = self._integrity.get("rados_batch_size", 50)
        ts_tag = datetime.now().strftime("%Y%m%d%H%M%S")
        for batch_start in range(0, obj_count, batch_size):
            batch_end = min(batch_start + batch_size, obj_count)
            script_lines = []
            for i in range(batch_start, batch_end):
                obj = f"integrity_obj_{i:06d}"
                content = f"integrity_data_{i}_{ts_tag}"
                script_lines.append(
                    f"content='{content}'; "
                    f'echo -n "$content" | rados -p {pool_name} put {obj} - && '
                    f"md5=$(echo -n \"$content\" | md5sum | awk '{{print $1}}') && "
                    f"rados -p {pool_name} setxattr {obj} integrity_md5 $md5 && "
                    f"echo 'OK:{obj}'"
                )
            batch_script = "; ".join(script_lines)
            try:
                rados_verify_tmout = self._integrity.get("verify_timeout", 300)
                out, _ = installer.exec_command(
                    sudo=True,
                    cmd=(
                        f"cephadm shell -- bash -s <<'EOFVERIFY'\n"
                        f"{batch_script}\n"
                        f"EOFVERIFY"
                    ),
                    timeout=rados_verify_tmout,
                )
                ok_count = out.count("OK:integrity_obj_")
                result["objects_written"] += ok_count
                if ok_count < (batch_end - batch_start):
                    log.warning(
                        "[integrity] RADOS batch %d-%d: %d/%d succeeded",
                        batch_start,
                        batch_end,
                        ok_count,
                        batch_end - batch_start,
                    )
            except Exception as e:
                log.warning(
                    "[integrity] RADOS batch %d-%d failed: %s",
                    batch_start,
                    batch_end,
                    e,
                )
                result["errors"].append(f"rados:batch_{batch_start}-{batch_end}:{e}")
                if len(result["errors"]) > 10:
                    log.error("[integrity] Too many RADOS batch errors, stopping")
                    break

        self._integrity_checksums["rados_pool"] = pool_name
        self._integrity_checksums["rados_count"] = str(obj_count)

    # ------------------------------------------------------------------
    #  Internal: Fill Processes
    # ------------------------------------------------------------------

    # Services that have fill support (mount-based or block-based with fio)
    _FILLABLE_SERVICES = {"cephfs", "nfs", "smb", "rbd", "nvmeof"}

    # Background-only tools (excluded from fill). See tool classification
    # above _TOOL_PACKAGES for the rationale behind each exclusion.
    _NON_FILL_TOOLS = {
        "boto3",
        "s3cmd",
        "warp",
        "bonnie",
        "cthon",
        "dbench",
        "dd",
        "rados_bench",
        "fsstress",
        "mdtest",
    }

    def _get_fillable_tools(self, svc, tools):
        """Filter tools to those suitable for fill IO on *svc*."""
        if svc in ("rbd", "nvmeof"):
            return [t for t in tools if t == "fio"]
        return [t for t in tools if t not in self._NON_FILL_TOOLS]

    _FILL_TOOL_WEIGHTS = {
        "fio": 50,
        "vdbench": 15,
        "smallfile": 15,
        "smbclient": 5,
    }

    def _count_fill_processes(
        self, fill_config: Dict, deployed_services: Set[str]
    ) -> int:
        """Count total fill processes to launch based on enabled io_tools.

        Only counts services that have actual fill implementation. For block
        devices (RBD/NVMeoF), only fio is counted since non-fill tools
        are skipped during fill.
        """
        total = 0
        for svc in deployed_services:
            if svc not in self._FILLABLE_SERVICES:
                continue
            tools = self._get_enabled_tools(svc)
            if not tools:
                continue
            fillable = self._get_fillable_tools(svc, tools)
            if not fillable:
                continue
            total += len(fillable)
        return max(1, total)

    def _calculate_fill_timeout(
        self, current_pct: float, target_pct: float, num_processes: int
    ) -> int:
        """Auto-calculate fill timeout based on cluster capacity.

        Uses a conservative 30 MB/s per-process throughput estimate
        (mixed tools like smallfile/dd on partitioned mounts average
        10-30 MB/s, not 100 MB/s) and a 3x safety multiplier for
        process restart overhead and tool startup latency.
        """
        try:
            df_stats = self.rados_obj.run_ceph_command("ceph df")
            total_bytes = df_stats["stats"]["total_bytes"]
        except Exception:
            total_bytes = 10 * 1024**4  # 10 TiB fallback

        bytes_to_fill = (target_pct - current_pct) / 100.0 * total_bytes
        throughput_estimate = num_processes * 30 * 1024 * 1024  # 30 MB/s each
        if throughput_estimate <= 0:
            throughput_estimate = 30 * 1024 * 1024

        timeout = int((bytes_to_fill / throughput_estimate) * 3)
        timeout = max(3600, min(timeout, 43200))  # 1h to 12h
        return timeout

    def _launch_fill_process(
        self, client: Any, service: str, tool: str, size_mb: Optional[int] = None
    ) -> Optional[int]:
        """Launch one fill process and register its PID.

        Returns the PID so the caller can wait for completion.
        Block-device services (RBD, NVMeoF) always use FIO for fill regardless
        of the tool parameter; non-fio tools are skipped.

        Args:
            client: Node handle.
            service: Service type.
            tool: IO tool name.
            size_mb: When set, each process writes exactly this many MB.
                Used by adaptive batch mode for precision fills.
        """
        cmd = ""
        if service in ("cephfs", "nfs", "smb"):
            mount_points = self._get_mount_points(client, service)
            if not mount_points:
                return None
            mp = mount_points[self._fill_mount_idx % len(mount_points)]
            self._fill_mount_idx += 1
            target_dir = f"{mp}/{FILL_DIR_PREFIX}"
            client.exec_command(sudo=True, cmd=f"mkdir -p {target_dir}", timeout=15)
            cmd = self._build_fill_cmd(tool, target_dir, size_mb=size_mb)
        elif service in ("rbd", "nvmeof"):
            if tool != "fio":
                return None
            devices = self._get_block_devices(client, service)
            if not devices:
                return None
            integrity_size = self.config.get("integrity", {}).get(
                "fio_baseline_size", "1G"
            )
            device = devices[self._fill_dev_idx % len(devices)]
            self._fill_dev_idx += 1
            fio_size = f"{size_mb}m" if size_mb else "1G"
            fp = self._fill_params
            cmd = (
                f"fio --name=ugt-fill-{service}-fio "
                f"--filename={device} "
                f"--offset={integrity_size} "
                f"--size={fio_size} "
                f"--bs={fp.get('block_fio_bs', '4M')} --rw=randwrite "
                f"--numjobs={fp.get('fio_numjobs', 4)} "
                f"--ioengine=libaio --direct=1 --unlink=0"
            )
        else:
            return None

        if cmd:
            pid, log_path = self._exec_background(
                client, cmd, log_tag=f"fill_{service}_{tool}"
            )
            if pid:
                self._register_pid(
                    client, pid, service, tool, "fill", log_path=log_path
                )
                return pid
        return None

    def _build_fill_cmd(
        self, tool: str, target_dir: str, size_mb: Optional[int] = None
    ) -> str:
        """Build a fill command string for the given tool.

        Each invocation must produce a unique output file/dir so that
        repeated calls in the fill loop accumulate data on disk.

        All intensity parameters are read from ``self._fill_params`` with
        defaults matching the pre-tiering hardcoded values.

        Args:
            tool: IO tool name.
            target_dir: Target directory for writes.
            size_mb: Exact size in MB each process should write. When None,
                uses defaults from fill_params (fio_default_size, etc.).
        """
        fp = self._fill_params
        tag = "$((RANDOM))_$$"
        fio_size = f"{size_mb}m" if size_mb else fp.get("fio_default_size", "4G")
        smb_count = size_mb if size_mb else fp.get("smb_dd_count", 256)

        if tool == "fio":
            numjobs = fp.get("fio_numjobs", 4)
            per_job_size = f"{max(1, size_mb // numjobs)}m" if size_mb else fio_size
            return (
                f"fio --name=ugt-fill-fio --directory={target_dir} "
                f"--size={per_job_size} "
                f"--bs={fp.get('fio_bs', '1M')} --rw=write "
                f"--numjobs={numjobs} "
                f"--ioengine=libaio --direct=1 --unlink=0"
            )
        elif tool == "vdbench":
            vdb_size_kb = (size_mb * 1024) if size_mb else (1024 * 1024)
            vdb_depth, vdb_width = 1, 1
            num_files = max(1, vdb_size_kb // 64)
            return (
                f"bash -c '"
                f"TAG=$((RANDOM))_$$; "
                f"PARM={target_dir}/vdbench_fill_parm_$TAG.txt; "
                f"cat > $PARM << VDB_EOF\n"
                f"fsd=fsd1,anchor={target_dir}/vdbench_fill_$TAG,"
                f"depth={vdb_depth},width={vdb_width},files={num_files},size=64k\n"
                f"fwd=fwd1,fsd=fsd1,operation=write,"
                f"xfersize=64k,"
                f"threads={fp.get('vdbench_threads', 4)}\n"
                f"rd=rd1,fwd=fwd1,fwdrate=max,"
                f"format=restart,"
                f"elapsed={_vdbench_elapsed(max(120, size_mb // 10) if size_mb else 120)},"
                f"interval=5\n"
                f"VDB_EOF\n"
                f"vdbench -f $PARM -o {target_dir}/vdbench_fill_out_$TAG'"
            )
        elif tool == "smallfile":
            file_size_kb = 1024
            num_files = max(1, (size_mb * 1024 // file_size_kb)) if size_mb else 1000
            return (
                f"python3 -m smallfile_cli "
                f"--operation create "
                f"--threads {fp.get('smallfile_threads', 4)} "
                f"--file-size {file_size_kb} "
                f"--files {num_files} --top {target_dir}/smallfile_fill_{tag}"
            )
        elif tool == "smbclient":
            return (
                f"dd if=/dev/urandom of={target_dir}/smb_fill_{tag} "
                f"bs=1M count={smb_count}"
            )
        return ""

    # ------------------------------------------------------------------
    #  Internal: Weighted Fill Infrastructure
    # ------------------------------------------------------------------

    def _discover_fill_targets(
        self, deployed_services: Set[str]
    ) -> List[Tuple[Any, str, str]]:
        """Discover ALL fillable mount points and block devices on ALL clients.

        Consolidates mount/device discovery across all clients and services
        into a single list of (client_node, mount_path_or_device, service_type)
        tuples for use by the weighted fill pipeline.
        """
        clients = self.ceph_cluster.get_nodes(role="client")
        if not clients:
            return []

        targets: List[Tuple[Any, str, str]] = []
        for client in clients:
            for svc in deployed_services:
                if svc not in self._FILLABLE_SERVICES:
                    continue
                try:
                    if svc in ("rbd", "nvmeof"):
                        for dev in self._get_block_devices(client, svc):
                            targets.append((client, dev, svc))
                    else:
                        for mp in self._get_mount_points(client, svc):
                            targets.append((client, mp, svc))
                except Exception as e:
                    log.warning(
                        "[fill] Target discovery failed for %s on %s: %s",
                        svc,
                        getattr(client, "hostname", client),
                        e,
                    )

        log.info(
            "[fill] Discovered %d fill targets across %d clients",
            len(targets),
            len(clients),
        )
        return targets

    def _assign_fill_tools(
        self,
        targets: List[Tuple[Any, str, str]],
        deployed_services: Set[str],
        fill_config: Dict[str, Any],
    ) -> List[Tuple[Any, str, str, str]]:
        """Assign IO tools to fill targets via weighted round-robin.

        Builds a weighted tool roster from _FILL_TOOL_WEIGHTS (or override
        from _FILL_TOOL_WEIGHTS), redistributes weights of disabled tools
        proportionally, sorts targets by service priority (CephFS first,
        NFS second, block devices last), and assigns tools round-robin.

        RBD/NVMeoF targets always receive "fio" regardless of weights.

        Returns:
            List of (client, path, tool, service) assignment tuples.
        """
        if not targets:
            return []

        enabled_by_svc: Dict[str, List[str]] = {}
        all_enabled: Set[str] = set()
        for svc in deployed_services:
            if svc not in self._FILLABLE_SERVICES:
                continue
            tools = self._get_enabled_tools(svc)
            fillable = self._get_fillable_tools(svc, tools)
            if fillable:
                enabled_by_svc[svc] = fillable
                all_enabled.update(fillable)

        if not all_enabled:
            return []

        weights: Dict[str, int] = {}
        for tool, default_weight in self._FILL_TOOL_WEIGHTS.items():
            if tool in all_enabled:
                weights[tool] = default_weight

        if not weights:
            return []

        total_weight = sum(weights.values())
        if total_weight <= 0:
            return []

        num_targets = len(targets)
        weighted_tools: List[str] = []
        sorted_tools = sorted(weights.items(), key=lambda x: -x[1])
        if num_targets <= len(sorted_tools):
            weighted_tools = [t for t, _ in sorted_tools[:num_targets]]
        else:
            for tool, weight in sorted_tools:
                count = max(1, round(weight * num_targets / total_weight))
                weighted_tools.extend([tool] * count)

        svc_order = {"cephfs": 0, "nfs": 1, "smb": 2, "rbd": 3, "nvmeof": 4}
        targets_sorted = sorted(targets, key=lambda t: svc_order.get(t[2], 99))

        assignments: List[Tuple[Any, str, str, str]] = []
        for i, (client, path, svc) in enumerate(targets_sorted):
            if svc in ("rbd", "nvmeof"):
                tool = "fio"
            else:
                tool = weighted_tools[i % len(weighted_tools)]
                svc_fillable = enabled_by_svc.get(svc, [])
                if tool not in svc_fillable and svc_fillable:
                    tool = svc_fillable[i % len(svc_fillable)]
            assignments.append((client, path, tool, svc))

        tool_counts: Dict[str, int] = {}
        for _, _, t, _ in assignments:
            tool_counts[t] = tool_counts.get(t, 0) + 1
        log.info(
            "[fill] Assigned tools to %d targets: %s",
            len(assignments),
            tool_counts,
        )
        return assignments

    def _compute_fill_chunk(
        self, remaining_bytes: int, num_processes: int, tool: str
    ) -> int:
        """Compute per-process write size in MB, scaled to remaining capacity.

        Each process writes roughly half of its fair share of the remaining
        space, clamped to tool-specific bounds to avoid pathologically small
        or large writes.
        """
        if num_processes <= 0:
            num_processes = 1
        per_process_mb = int(remaining_bytes / num_processes / 2 / (1024 * 1024))
        tool_clamps = {
            "fio": (256, 8192),
            "vdbench": (128, 2048),
            "smallfile": (64, 2048),
            "smbclient": (32, 512),
        }
        lo, hi = tool_clamps.get(tool, (64, 2048))
        return max(lo, min(hi, per_process_mb))

    def _run_weighted_fill_batch(
        self,
        assignments: List[Tuple[Any, str, str, str]],
        remaining_bytes: int,
        deadline: float,
        target_pct: float = 0,
        abort_pct: float = 0,
        poll_interval: int = 10,
    ) -> List[Tuple[Any, int]]:
        """Launch a weighted fill batch and monitor until completion.

        Groups assignments by client node, builds a single bash script per
        client that launches all assigned fill processes via nohup, executes
        via one SSH call per client, then polls for PID completion and cluster
        usage until all processes finish, the target is reached, or the
        deadline expires.

        Args:
            assignments: Output of _assign_fill_tools().
            remaining_bytes: Current remaining capacity to fill.
            deadline: Absolute time.time() after which processes are killed.
            target_pct: If > 0, stop early when cluster usage reaches this.
            abort_pct: If > 0, stop early when cluster usage reaches this.
            poll_interval: Seconds between liveness/usage checks.

        Returns:
            List of (client, pid) tuples for all processes launched.
        """
        client_groups: Dict[Any, List[Tuple[str, str, str]]] = defaultdict(list)
        for client, path, tool, svc in assignments:
            client_groups[client].append((path, tool, svc))

        num_processes = len(assignments)
        all_pids: List[Tuple[Any, int]] = []
        launched_pids: List[Tuple[Any, int]] = []

        for client, tasks in client_groups.items():
            script_lines = ["#!/bin/bash"]
            for path, tool, svc in tasks:
                size_mb = self._compute_fill_chunk(remaining_bytes, num_processes, tool)
                tag = (
                    f"wfill_{svc}_{tool}"
                    f"_{int(time.time())}_{random.randint(1000, 9999)}"
                )
                log_path = f"/tmp/ugt_{tag}.log"

                if svc in ("rbd", "nvmeof"):
                    integrity_size = self._integrity.get("fio_baseline_size", "1G")
                    cmd = (
                        f"fio --name=ugt-fill-{svc}-fio --filename={path} "
                        f"--offset={integrity_size} --size={size_mb}m "
                        f"--bs={self._fill_params.get('block_fio_bs', '4M')} "
                        f"--rw=randwrite "
                        f"--numjobs={self._fill_params.get('fio_numjobs', 4)} "
                        f"--ioengine=libaio --direct=1 --unlink=0"
                    )
                else:
                    target_dir = f"{path}/{FILL_DIR_PREFIX}"
                    script_lines.append(f"mkdir -p {target_dir}")
                    cmd = self._build_fill_cmd(tool, target_dir, size_mb=size_mb)

                if not cmd:
                    continue
                script_lines.append(f'nohup {cmd} > {log_path} 2>&1 & echo "PID:$!"')

            if len(script_lines) <= 1:
                continue

            script = "\n".join(script_lines)
            b64 = base64.b64encode(script.encode()).decode()
            try:
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=f"echo {b64} | base64 -d | bash",
                    timeout=60,
                )
                for line in out.strip().split("\n"):
                    line = line.strip()
                    if line.startswith("PID:"):
                        pid_str = line[4:].strip()
                        if pid_str.isdigit():
                            pid = int(pid_str)
                            all_pids.append((client, pid))
                            launched_pids.append((client, pid))
                            self._register_pid(
                                client,
                                pid,
                                "mixed",
                                "weighted_fill",
                                "fill",
                                log_path="",
                            )
            except Exception as e:
                log.warning(
                    "[fill] Batch launch failed on %s: %s",
                    getattr(client, "hostname", client),
                    e,
                )

        log.info("[fill] Weighted batch: launched %d processes", len(launched_pids))

        while all_pids:
            time.sleep(poll_interval)

            if time.time() >= deadline:
                log.warning("[fill] Deadline reached during weighted batch")
                break

            if self._fill_stop_event.is_set():
                break

            if target_pct > 0:
                current = self._get_cluster_usage_percent()
                if current is not None:
                    if current >= target_pct:
                        log.info("[fill] Target %.1f%% reached during batch", current)
                        break
                    if abort_pct > 0 and current >= abort_pct:
                        log.warning("[fill] Abort %.1f%% hit during batch", current)
                        break

            all_pids = self._poll_and_reap_pids(all_pids)
            if not all_pids:
                break

        self._kill_remaining_pids(all_pids)

        return launched_pids

    # ------------------------------------------------------------------
    #  Internal: Background IO Processes
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    #  Internal: NFS distributed IO
    # ------------------------------------------------------------------

    def _build_nfs_cthon_bg_cmd(self, client, mount_point: str, bp: dict) -> str:
        """Deploy a restart-loop cthon04 script on the NFS mount root."""
        iterations = bp.get("cthon_iterations", 1)
        sleep_sec = bp.get("cthon_sleep_sec", 60)
        mount_q = mount_point.replace("'", "'\\''")
        tag = mount_point.strip("/").replace("/", "_") or "root"
        script_path = f"/tmp/ugt-bg-nfs-cthon-{tag}.sh"
        script = (
            "#!/bin/bash\n"
            f"MOUNT='{mount_q}'\n"
            'LOG="/tmp/ugt-bg-nfs-cthon-$(basename "$MOUNT").log"\n'
            "CTHON=/root/cthon04/server\n"
            f"ITER={iterations}\n"
            f"SLEEP={sleep_sec}\n"
            "while true; do\n"
            '  if [ ! -x "$CTHON" ]; then\n'
            "    sleep 30\n"
            "    continue\n"
            "  fi\n"
            '  line=$(mount | grep -F "on $MOUNT " | head -1)\n'
            '  if [ -z "$line" ]; then\n'
            "    sleep 30\n"
            "    continue\n"
            "  fi\n"
            "  spec=$(echo \"$line\" | awk '{print $1}')\n"
            "  SERVER=${spec%%:*}\n"
            "  EXPORT=${spec#*:}\n"
            "  PORT=$(echo \"$line\" | sed -n 's/.*port=\\([0-9]*\\).*/\\1/p')\n"
            "  PORT=${PORT:-2049}\n"
            "  cd /root/cthon04 || { sleep 30; continue; }\n"
            '  ./server -a -o port=$PORT -N "$ITER" '
            '-p "$EXPORT" -m "$MOUNT" "$SERVER" >> "$LOG" 2>&1\n'
            '  sleep "$SLEEP"\n'
            "done\n"
        )
        return self._deploy_script(client, script_path, script, "cthon script")

    def _launch_nfs_distributed_io(
        self,
        client: Any,
        tools: List[str],
        rwmixread: int,
        fio_timeout: int,
        percentiles: str,
    ) -> int:
        """Distribute IO tools round-robin across healthy NFS mount points.

        With versioned NFS mounts (v3, v4.0, v4.1, v4.2 x 2 mounts each
        x 2 clusters = 16 mounts per client), a single random-choice would
        leave most mounts idle. This method assigns one IO process per mount
        point, cycling through the enabled tool list so every mount gets
        continuous IO during the upgrade.

        Stale mounts (backed by crashed daemons, e.g. CQOS port conflicts)
        are detected via a fast ``timeout 3 stat`` batch probe and skipped
        to avoid 15-30s timeout cascades that exhaust SSH channels.
        """
        mount_points = self._get_mount_points(client, "nfs")
        if not mount_points:
            return 0

        healthy_mounts = self._filter_healthy_nfs_mounts(client, mount_points)
        skipped = len(mount_points) - len(healthy_mounts)
        if skipped:
            log.warning(
                "[bg_io] NFS: %d/%d mounts healthy on %s, skipping %d stale",
                len(healthy_mounts),
                len(mount_points),
                getattr(client, "hostname", client),
                skipped,
            )
        if not healthy_mounts:
            return 0

        count = 0
        for i, mp in enumerate(healthy_mounts):
            tool = tools[i % len(tools)]
            cmd = ""

            if tool == "cthon":
                cmd = self._build_nfs_cthon_bg_cmd(client, mp, self._bg_params)
            elif tool == "fio":
                active_mds = (
                    self.config.get("scale", {}).get("cephfs", {}).get("active_mds", 6)
                )
                shard_count = active_mds * 3
                if shard_count > 1:
                    shard_dirs = [
                        f"{mp}/bg_shard_{si:02d}" for si in range(shard_count)
                    ]
                    try:
                        client.exec_command(
                            sudo=True,
                            cmd="mkdir -p " + " ".join(shard_dirs),
                            timeout=30,
                        )
                    except Exception:
                        shard_dirs = []
                    if shard_dirs:
                        cmd = self._build_sharded_fio_cmd(
                            shard_dirs,
                            rwmixread,
                            fio_timeout,
                            percentiles,
                            "nfs",
                        )

            if not cmd:
                bg_dir = f"{mp}/{BACKGROUND_DIR}"
                try:
                    client.exec_command(sudo=True, cmd=f"mkdir -p {bg_dir}", timeout=15)
                except Exception:
                    pass
                cmd = self._build_mount_bg_cmd(
                    client,
                    tool,
                    bg_dir,
                    rwmixread,
                    fio_timeout,
                    percentiles,
                    "nfs",
                )

            if not cmd:
                continue
            pid, log_path = self._exec_background(
                client, cmd, log_tag=f"bg_nfs_{tool}_{i}"
            )
            if pid:
                self._register_pid(
                    client, pid, "nfs", tool, "background", log_path=log_path
                )
                count += 1
        return count

    def _launch_background_process(
        self,
        client: Any,
        service: str,
        tool: str,
        rwmixread: int = 70,
        fio_timeout: int = 12600,
        percentiles: str = "50:90:95:99:99.9:99.99",
        mount_index: int = 0,
    ):
        """Launch one background IO process and register its PID."""
        cmd = self._build_background_cmd(
            client,
            service,
            tool,
            rwmixread,
            fio_timeout,
            percentiles,
            mount_index=mount_index,
        )
        if not cmd:
            return

        pid, log_path = self._exec_background(
            client, cmd, log_tag=f"bg_{service}_{tool}"
        )
        if pid:
            self._register_pid(
                client, pid, service, tool, "background", log_path=log_path
            )

    def _poll_and_reap_pids(self, all_pids):
        """Check liveness of PIDs, reap done ones, return still-running list."""
        still_running = []
        check_groups = {}
        for client, pid in all_pids:
            check_groups.setdefault(client, []).append(pid)

        for client, client_pids in check_groups.items():
            check_parts = [
                f"(kill -0 {p} 2>/dev/null && echo alive:{p} || echo done:{p})"
                for p in client_pids
            ]
            check_cmd = " ; ".join(check_parts)
            try:
                out, _ = client.exec_command(sudo=True, cmd=check_cmd, timeout=30)
                for line in out.strip().split("\n"):
                    line = line.strip()
                    if line.startswith("alive:"):
                        try:
                            still_running.append((client, int(line[6:])))
                        except ValueError:
                            pass
                    elif line.startswith("done:"):
                        try:
                            with self._registry_lock:
                                self._pid_registry.pop((client, int(line[5:])), None)
                        except ValueError:
                            pass
            except Exception:
                for p in client_pids:
                    still_running.append((client, p))
        return still_running

    def _kill_remaining_pids(self, remaining):
        """Kill all PIDs in the list and remove from registry."""
        for client, pid in remaining:
            try:
                client.exec_command(
                    sudo=True,
                    cmd=f"kill -KILL {pid} 2>/dev/null || true",
                    timeout=10,
                    check_ec=False,
                )
            except Exception:
                pass
            with self._registry_lock:
                self._pid_registry.pop((client, pid), None)

    @staticmethod
    def _fio_bs_flag(bs_val):
        """Return --bs= or --bsrange= flag depending on whether bs_val is a range."""
        return f"--bsrange={bs_val}" if "-" in str(bs_val) else f"--bs={bs_val}"

    def _deploy_script(self, client, path: str, content: str, tag: str) -> str:
        """Write a script to *path* on *client* and return the run command."""
        try:
            client.exec_command(
                sudo=True,
                cmd=(
                    f"cat > {path} << 'SHEOF'\n" f"{content}SHEOF\n" f"chmod +x {path}"
                ),
                timeout=15,
            )
        except Exception as e:
            log.warning("[bg_io] Failed to write %s: %s", tag, e)
            return ""
        return f"bash {path}"

    def _build_background_cmd(
        self,
        client: Any,
        service: str,
        tool: str,
        rwmixread: int,
        fio_timeout: int,
        percentiles: str,
        mount_index: int = 0,
    ) -> str:
        """Build the background IO command for the given service/tool.

        All intensity parameters are read from ``self._bg_params`` with
        defaults matching the pre-tiering hardcoded values.
        """
        if service in ("cephfs", "nfs", "smb"):
            mount_points = self._get_mount_points(client, service)
            if not mount_points:
                return ""
            mp = mount_points[mount_index % len(mount_points)]

            if tool == "fio" and service in ("cephfs", "nfs"):
                active_mds = (
                    self.config.get("scale", {}).get("cephfs", {}).get("active_mds", 6)
                )
                shard_count = active_mds * 3
                if shard_count > 1:
                    shard_dirs = [f"{mp}/bg_shard_{i:02d}" for i in range(shard_count)]
                    try:
                        client.exec_command(
                            sudo=True,
                            cmd="mkdir -p " + " ".join(shard_dirs),
                            timeout=30,
                        )
                    except Exception:
                        shard_dirs = []
                    if shard_dirs:
                        return self._build_sharded_fio_cmd(
                            shard_dirs,
                            rwmixread,
                            fio_timeout,
                            percentiles,
                            service,
                        )

            bg_dir = f"{mp}/{BACKGROUND_DIR}"
            try:
                client.exec_command(sudo=True, cmd=f"mkdir -p {bg_dir}", timeout=15)
            except Exception:
                pass
            return self._build_mount_bg_cmd(
                client,
                tool,
                bg_dir,
                rwmixread,
                fio_timeout,
                percentiles,
                service,
            )

        elif service in ("rbd", "nvmeof"):
            if service == "rbd":
                try:
                    out, _ = client.exec_command(
                        sudo=True,
                        cmd="rbd device list --format json",
                        timeout=15,
                    )
                    all_devs = json.loads(out) if out.strip() else []
                    bg_devices = [
                        d["device"]
                        for d in all_devs
                        if "integrity" not in d.get("name", "").lower()
                    ]
                except Exception:
                    bg_devices = []
            else:
                bg_devices = self._get_block_devices(client, service)
            if not bg_devices:
                return ""
            integrity_size = self.config.get("integrity", {}).get(
                "fio_baseline_size", "1G"
            )
            if tool == "fio":
                bp = self._bg_params
                rate_iops = bp.get("fio_rate_iops", 200)
                rate_flag = f" --rate_iops={rate_iops}" if rate_iops else ""
                fio_bs = bp.get("fio_bs", "4k")
                bs_flag = self._fio_bs_flag(fio_bs)
                filenames = ":".join(bg_devices)
                return (
                    f"fio --name=ugt-bg-{service}-fio --filename={filenames} "
                    f"--offset={integrity_size} "
                    f"--size={bp.get('fio_block_size', '1G')} "
                    f"{bs_flag} --rw=randrw --rwmixread={rwmixread} "
                    f"--numjobs={bp.get('fio_numjobs', 1)} "
                    f"--ioengine=libaio --direct=1"
                    f"{rate_flag} --time_based --runtime=99999 "
                    f"--timeout={fio_timeout} "
                    f"--percentile_list={percentiles} "
                    f"--output-format=json --status-interval=5"
                )
            return ""

        elif service == "rados":
            if tool == "rados_bench":
                pools = [
                    p
                    for p in self._get_fill_pools()
                    if p.startswith(("rep_", "ec_")) and "quota" not in p
                ]
                if not pools:
                    pools = ["rep_pool"]
                pool_arr = " ".join(pools)
                rados_dur = self._bg_params.get("rados_bench_duration", 30)
                rados_thr = self._bg_params.get("rados_bench_threads", 2)
                rados_script_path = "/tmp/ugt-bg-rados-rados_bench.sh"
                rados_script = (
                    "#!/bin/bash\n"
                    f"POOLS=({pool_arr})\n"
                    "NUM_POOLS=${#POOLS[@]}\n"
                    "SEQ=0\n"
                    "while true; do\n"
                    "  POOL=${POOLS[$((SEQ % NUM_POOLS))]}\n"
                    f"  rados bench -p $POOL {rados_dur} write "
                    f"-t {rados_thr} "
                    "--run-name bg_$(hostname -s)_$SEQ\n"
                    "  rados -p $POOL cleanup "
                    "--run-name bg_$(hostname -s)_$SEQ 2>/dev/null\n"
                    "  SEQ=$((SEQ+1))\n"
                    "  sleep 2\n"
                    "done\n"
                )
                return self._deploy_script(
                    client, rados_script_path, rados_script, "rados bench script"
                )
            return ""

        elif service == "rgw":
            endpoints = self._cached_rgw_endpoints
            if endpoints is None:
                endpoints = self._resolve_all_rgw_endpoints()
                self._cached_rgw_endpoints = endpoints
            if not endpoints:
                log.warning(
                    "[bg_io] Cannot resolve RGW endpoint, skipping RGW background IO"
                )
                return ""
            idx = self._rgw_tool_counter
            self._rgw_tool_counter += 1
            rgw_endpoint = endpoints[idx % len(endpoints)]
            access_key = self.config.get("rgw_access_key", "iokey")
            secret_key = self.config.get("rgw_secret_key", "iosecret")

            if tool == "boto3":
                hostname = getattr(client, "hostname", str(client))
                scaled = self._get_scaled_rgw_buckets()
                bucket = (
                    scaled[idx % len(scaled)] if scaled else f"ugt-bg-boto3-{hostname}"
                )
                script_path = "/tmp/ugt-bg-rgw-boto3.py"
                script_lines = [
                    "import boto3, time, os, random",
                    (
                        f"s3 = boto3.client('s3', "
                        f"endpoint_url='http://{rgw_endpoint}', "
                        f"aws_access_key_id='{access_key}', "
                        f"aws_secret_access_key='{secret_key}')"
                    ),
                    "try:",
                    f"    s3.create_bucket(Bucket='{bucket}')",
                    "except Exception:",
                    "    pass",
                    "i = 0",
                    "while True:",
                    "    try:",
                    "        key = f'bg_obj_{i % 1000}'",
                    (
                        f"        data = os.urandom("
                        f"random.randint("
                        f"{self._bg_params.get('boto3_obj_min', 1024)}, "
                        f"{self._bg_params.get('boto3_obj_max', 65536)}))"
                    ),
                    (
                        f"        s3.put_object(Bucket='{bucket}', "
                        "Key=key, Body=data)"
                    ),
                    (f"        s3.get_object(Bucket='{bucket}', " "Key=key)"),
                    "        if i % 4 == 3:",
                    (
                        "            s3.delete_object("
                        f"Bucket='{bucket}', "
                        "Key=f'bg_obj_{(i - 2) % 1000}')"
                    ),
                    "        i += 1",
                    f"        time.sleep({self._bg_params.get('boto3_sleep', 0.1)})",
                    "    except Exception:",
                    "        time.sleep(5)",
                    "        continue",
                ]
                script = "\n".join(script_lines) + "\n"
                try:
                    client.exec_command(
                        sudo=True,
                        cmd=(f"cat > {script_path} << 'PYEOF'\n" f"{script}PYEOF"),
                        timeout=15,
                    )
                except Exception as e:
                    log.warning("[bg_io] Failed to write RGW IO script: %s", e)
                    return ""
                return f"python3 {script_path}"

            elif tool == "s3cmd":
                bp = self._bg_params
                obj_min = bp.get("boto3_obj_min", 1024)
                obj_max = bp.get("boto3_obj_max", 65536)
                sleep_sec = bp.get("boto3_sleep", 0.1)
                hostname = getattr(client, "hostname", str(client))
                scaled = self._get_scaled_rgw_buckets()
                s3cmd_bucket = (
                    scaled[idx % len(scaled)] if scaled else f"ugt-bg-s3cmd-{hostname}"
                )
                script_path = "/tmp/ugt-bg-rgw-s3cmd.sh"
                host_base = rgw_endpoint
                script = (
                    "#!/bin/bash\n"
                    "cat > ~/.s3cfg << 'S3CFG'\n"
                    "[default]\n"
                    f"host_base = {host_base}\n"
                    f"host_bucket = {host_base}\n"
                    f"access_key = {access_key}\n"
                    f"secret_key = {secret_key}\n"
                    "signature_v2 = True\n"
                    "use_https = False\n"
                    "S3CFG\n"
                    f"s3cmd mb s3://{s3cmd_bucket} 2>/dev/null || true\n"
                    "i=0\n"
                    "while true; do\n"
                    f"  SIZE=$(shuf -i {obj_min}-{obj_max} -n 1)\n"
                    "  dd if=/dev/urandom of=/tmp/s3obj bs=1 "
                    "count=$SIZE 2>/dev/null\n"
                    '  KEY="bg_obj_$((i % 1000))"\n'
                    "  s3cmd put /tmp/s3obj "
                    f"s3://{s3cmd_bucket}/$KEY 2>/dev/null\n"
                    f"  s3cmd get s3://{s3cmd_bucket}/$KEY "
                    "/tmp/s3get --force 2>/dev/null\n"
                    "  if [ $((i % 10)) -eq 9 ]; then\n"
                    '    DEL_KEY="bg_obj_$(( (i - 5) % 1000 ))"\n'
                    "    s3cmd del "
                    f"s3://{s3cmd_bucket}/$DEL_KEY 2>/dev/null\n"
                    "  fi\n"
                    "  if [ $((i % 20)) -eq 19 ]; then\n"
                    "    dd if=/dev/urandom of=/tmp/s3obj_large "
                    "bs=1M count=10 2>/dev/null\n"
                    "    s3cmd put --multipart-chunk-size-mb=5 "
                    "/tmp/s3obj_large "
                    f"s3://{s3cmd_bucket}/multipart_$KEY 2>/dev/null\n"
                    "  fi\n"
                    "  i=$((i + 1))\n"
                    f"  sleep {sleep_sec}\n"
                    "done\n"
                )
                return self._deploy_script(client, script_path, script, "s3cmd script")

            elif tool == "warp":
                bp = self._bg_params
                concurrent = bp.get("warp_concurrent", 8)
                obj_size = bp.get("warp_obj_size", "256KiB")
                duration = bp.get("warp_duration", "30m")
                hostname = getattr(client, "hostname", str(client))
                scaled = self._get_scaled_rgw_buckets()
                warp_bucket = (
                    scaled[idx % len(scaled)] if scaled else f"ugt-bg-warp-{hostname}"
                )
                warp_script_path = "/tmp/ugt-bg-rgw-warp.sh"
                warp_script = (
                    "#!/bin/bash\n"
                    "while true; do\n"
                    f"  warp mixed "
                    f"--host={rgw_endpoint} "
                    f"--access-key={access_key} --secret-key={secret_key} "
                    f"--bucket={warp_bucket} "
                    f"--concurrent={concurrent} "
                    f"--obj.size={obj_size} "
                    f"--duration={duration} "
                    f"--put-distrib 35 --get-distrib 35 "
                    f"--delete-distrib 25 --stat-distrib 5 "
                    f"--noclear --no-color --stress "
                    f"--benchdata /tmp/warp_bg 2>/dev/null\n"
                    "  sleep 2\n"
                    "done\n"
                )
                return self._deploy_script(
                    client, warp_script_path, warp_script, "warp script"
                )

            return ""

        return ""

    def _build_mount_bg_cmd(
        self,
        client: Any,
        tool: str,
        bg_dir: str,
        rwmixread: int,
        fio_timeout: int,
        percentiles: str,
        service: str = "cephfs",
    ) -> str:
        """Build background IO command for mount-point tools.

        Long-running tools (fio, dbench) run natively with extended runtimes.
        Short-lived tools (smallfile, bonnie, mdtest, fsstress) are
        wrapped in a restart loop so IO continues during the upgrade.

        All intensity parameters are read from ``self._bg_params`` with
        defaults matching the pre-tiering hardcoded values.
        """
        bp = self._bg_params

        if tool == "fio":
            rate_iops = bp.get("fio_rate_iops", 200)
            rate_flag = f" --rate_iops={rate_iops}" if rate_iops else ""
            fio_bs = bp.get("fio_bs", "4k")
            bs_flag = self._fio_bs_flag(fio_bs)
            return (
                f"fio --name=ugt-bg-{service}-fio --directory={bg_dir} "
                f"--size={bp.get('fio_size', '512M')} {bs_flag} "
                f"--rw=randrw --rwmixread={rwmixread} "
                f"--numjobs={bp.get('fio_numjobs', 1)} "
                f"--ioengine=libaio --direct=1"
                f"{rate_flag} --time_based --runtime=99999 "
                f"--timeout={fio_timeout} "
                f"--percentile_list={percentiles} "
                f"--output-format=json --status-interval=5"
            )
        elif tool == "smallfile":
            sf_script_path = f"/tmp/ugt-bg-{service}-smallfile.sh"
            sf_script = (
                "#!/bin/bash\n"
                "iter=0\n"
                "while true; do\n"
                f"  python3 -m smallfile_cli "
                f"--operation create "
                f"--threads {bp.get('smallfile_threads', 4)} "
                f"--file-size 64 "
                f"--files {bp.get('smallfile_files', 5000)} "
                f"--top {bg_dir}/smallfile_bg_$iter "
                f"--response-times Y\n"
                f"  find {bg_dir}/smallfile_bg_$iter -type f 2>/dev/null "
                f"| head -2500 | xargs rm -f 2>/dev/null\n"
                "  iter=$((iter+1))\n"
                "  sleep 2\n"
                "done\n"
            )
            return self._deploy_script(
                client, sf_script_path, sf_script, "smallfile script"
            )
        elif tool == "dd":
            dd_script_path = f"/tmp/ugt-bg-{service}-dd.sh"
            dd_script = (
                "#!/bin/bash\n"
                "while true; do\n"
                f"  dd if=/dev/urandom of={bg_dir}/dd_bg_file "
                f"bs={bp.get('dd_bs', '1M')} "
                f"count={bp.get('dd_count', 10)} "
                f"oflag=direct conv=notrunc 2>&1\n"
                "  sleep 1\n"
                "done\n"
            )
            return self._deploy_script(client, dd_script_path, dd_script, "dd script")
        elif tool == "dbench":
            return f"dbench {bp.get('dbench_clients', 4)} " f"-t 99999 -D {bg_dir}"
        elif tool == "fsstress":
            return (
                f"bash -c 'iter=0; while true; do "
                f"if [ $((iter % 2)) -eq 0 ] && [ $iter -gt 0 ]; then "
                f"rm -rf {bg_dir}/fsstress_bg 2>/dev/null; "
                f"mkdir -p {bg_dir}/fsstress_bg; fi; "
                f"fsstress -d {bg_dir}/fsstress_bg "
                f"-n {bp.get('fsstress_ops', 10000)} "
                f"-p {bp.get('fsstress_procs', 2)} -l 1; "
                f"iter=$((iter+1)); sleep 2; done'"
            )
        elif tool == "bonnie":
            return (
                f"bash -c 'while true; do "
                f"bonnie++ -d {bg_dir} "
                f"-s {bp.get('bonnie_size', 128)} -n 50 -u root; "
                f"rm -f {bg_dir}/Bonnie.* 2>/dev/null; "
                f"sleep 5; done'"
            )
        elif tool == "mdtest":
            return (
                f"bash -c 'iter=0; while true; do "
                f"mdtest -C -T -d {bg_dir}/mdtest_bg_$iter "
                f"-n {bp.get('mdtest_files', 5000)} -i 1 -u; "
                f"find {bg_dir}/mdtest_bg_$iter -type f 2>/dev/null "
                f"| head -2500 | xargs rm -f 2>/dev/null; "
                f"iter=$((iter+1)); sleep 2; done'"
            )
        elif tool == "vdbench":
            parm = f"{bg_dir}/vdbench_parm.txt"
            vdb_script_path = f"/tmp/ugt-bg-{service}-vdbench.sh"
            vdb_script = (
                "#!/bin/bash\n"
                f"mkdir -p {bg_dir}\n"
                f"cat > {parm} << VDB_EOF\n"
                f"fsd=fsd1,anchor={bg_dir}/vdbench_data,"
                f"depth=2,width=3,files=50,size=64k\n"
                f"fwd=fwd1,fsd=fsd1,operation=write,"
                f"xfersize=64k,"
                f"threads={bp.get('vdbench_threads', 4)}\n"
                f"rd=rd1,fwd=fwd1,"
                f"fwdrate={bp.get('vdbench_fwdrate', 100)},"
                f"format=restart,elapsed={_vdbench_elapsed(999999)},interval=5\n"
                f"VDB_EOF\n"
                f"vdbench -f {parm} -o {bg_dir}/vdbench_out 2>&1\n"
            )
            return self._deploy_script(
                client, vdb_script_path, vdb_script, "vdbench script"
            )
        elif tool == "smbclient":
            return (
                f"bash -c 'while true; do "
                f"dd if=/dev/urandom of={bg_dir}/smb_bg_file "
                f"bs=64k count={bp.get('smb_dd_count', 16)} "
                f"oflag=direct conv=notrunc 2>&1; "
                f"sleep 2; done'"
            )
        return ""

    def _build_sharded_fio_cmd(
        self,
        shard_dirs,
        rwmixread,
        fio_timeout,
        percentiles,
        service,
    ):
        """Build a multi-job fio command with one job per shard directory.

        Creates one fio job per shard so CephFS distributed ephemeral pinning
        can hash-spread directories across MDS ranks.  Uses fio --name=global
        for shared params; each shard overrides only --directory.

        rate_iops is split evenly across shards (fio applies it per-job).
        fio_size and fio_numjobs are kept at configured values per shard.
        """
        bp = self._bg_params
        rate_iops = bp.get("fio_rate_iops", 200)
        per_shard_rate = max(1, rate_iops // len(shard_dirs)) if rate_iops else 0
        rate_flag = f" --rate_iops={per_shard_rate}" if per_shard_rate else ""
        fio_bs = bp.get("fio_bs", "4k")
        bs_flag = self._fio_bs_flag(fio_bs)

        global_section = (
            f"fio --name=global"
            f" --size={bp.get('fio_size', '512M')} {bs_flag}"
            f" --rw=randrw --rwmixread={rwmixread}"
            f" --numjobs={bp.get('fio_numjobs', 1)}"
            f" --ioengine=libaio --direct=1"
            f"{rate_flag} --time_based --runtime=99999"
            f" --timeout={fio_timeout}"
            f" --percentile_list={percentiles}"
            f" --output-format=json --status-interval=5"
        )

        job_sections = " ".join(
            f"--name=ugt-{service}-shard-{i:02d} --directory={d}"
            for i, d in enumerate(shard_dirs)
        )

        return f"{global_section} {job_sections}"

    # ------------------------------------------------------------------
    #  Internal: Verification
    # ------------------------------------------------------------------

    def _verify_mount_integrity(self, client: Any, services: List[str], result: Dict):
        """Verify CRC32C on all mount-point integrity files in one SSH call.

        Replaces the previous approach of 10 individual SSH+fio commands per
        mount point with a single bash script that:

        1. Quick-checks each mount with ``timeout 15 stat`` to detect stale
           or hung mounts before attempting fio verification.
        2. Writes a fio job file per healthy mount with ``[global]`` (rw=read,
           verify=crc32c, do_verify=1) and individual ``[job]`` sections for
           each of the 10 file specs from ``_INTEGRITY_FILE_SPECS``.
        3. Runs ``fio <jobfile>`` once per mount (one fio invocation covers
           all 10 file sizes).
        4. Checks fio output for verification failures (verify+bad, non-zero
           err=, fio runtime errors).
        5. Emits structured PASS/FAIL/SKIP lines delimited by markers.

        With ~14 mounts per client, this reduces ~140 SSH round-trips to 1.

        Args:
            client: Client node object.
            services: Mount-based service names to verify.
            result: Accumulator dict with total_checked, mismatches, errors.
        """
        all_mounts: List[Tuple[str, str]] = []
        for svc in services:
            for mp in self._get_mount_points(client, svc):
                all_mounts.append((svc, mp))

        if not all_mounts:
            return

        hostname = getattr(client, "hostname", str(client))
        all_mounts = self._select_integrity_mounts(all_mounts, hostname)
        if not all_mounts:
            return

        num_specs = len(_INTEGRITY_FILE_SPECS)

        script_lines = ['echo "###VERIFY_START###"']

        for _svc, mp in all_mounts:
            integrity_dir = f"{mp}/{INTEGRITY_DIR}"
            sanitized = mp.replace("/", "_").strip("_")
            job_file = f"/tmp/verify_{sanitized}.fio"

            script_lines.append(
                f'if ! timeout 15 stat "{integrity_dir}" >/dev/null 2>&1; then'
            )
            script_lines.append(f'  echo "SKIP:{mp}:stale_or_hung"')
            script_lines.append("else")

            fio_content = self._build_fio_job_content(integrity_dir, "read", True)
            script_lines.append(f"  cat > {job_file} << 'FIOEOF'")
            script_lines.append(fio_content)
            script_lines.append("FIOEOF")

            verify_tmout = self._integrity.get("verify_timeout", 300)
            script_lines.append(
                f"  fio_out=$(timeout {verify_tmout} fio {job_file} 2>&1)"
            )
            script_lines.append("  fio_rc=$?")

            script_lines.append(
                '  if echo "$fio_out" | grep -qi "verify.*bad" || '
                'echo "$fio_out" | grep -qE "err=[[:space:]]*[1-9]" || '
                'echo "$fio_out" | grep -qiE "^fio:[[:space:]]*(error|failed|fatal)"; then'
            )
            script_lines.append(f'    echo "IO_CHECK_FAIL:{mp}:verification_failed"')
            script_lines.append("  elif [ $fio_rc -ne 0 ]; then")
            script_lines.append(f'    echo "IO_CHECK_FAIL:{mp}:fio_exit_code_$fio_rc"')
            script_lines.append("  else")
            script_lines.append(f'    echo "IO_CHECK_VERIFIED:{mp}:{num_specs}"')
            script_lines.append("  fi")
            script_lines.append(f"  rm -f {job_file}")
            script_lines.append("fi")

        script_lines.append('echo "###VERIFY_END###"')
        script = "\n".join(script_lines)

        timeout = self._batch_mount_ssh_timeout(len(all_mounts))
        log.info(
            "[verify] Verifying %d mounts on %s in single SSH call (ssh_timeout=%ds)",
            len(all_mounts),
            hostname,
            timeout,
        )

        try:
            cmd = f"bash << 'VERIFY_SCRIPT_EOF'\n{script}\nVERIFY_SCRIPT_EOF"
            out, _ = self._heavy_client_ssh(
                client,
                sudo=True,
                cmd=cmd,
                timeout=timeout,
                check_ec=False,
            )
            self._parse_verify_output(out, all_mounts, result, hostname)
        except Exception as e:
            reason = self._batched_fail_reason(e, timeout)
            log.error("[verify] Batched verify failed on %s: %s", hostname, reason)
            for svc, mp in all_mounts:
                result["errors"].append(f"{svc}:{mp}:batched_failed:{reason}")

    def _parse_verify_output(
        self,
        output: str,
        all_mounts: List[Tuple[str, str]],
        result: Dict,
        hostname: str,
    ):
        """Parse IO_CHECK_VERIFIED/IO_CHECK_FAIL/SKIP output from batched verify."""
        text = str(output)
        start = text.find("###VERIFY_START###")
        end = text.find("###VERIFY_END###")
        if start == -1 or end == -1:
            log.warning("[verify] Missing markers in verify output on %s", hostname)
            result["errors"].append(f"missing_verify_markers:{hostname}")
            return

        body = text[start + len("###VERIFY_START###") : end]
        reported_mounts: Set[str] = set()

        for line in body.strip().splitlines():
            line = line.strip()
            if line.startswith("IO_CHECK_VERIFIED:"):
                parts = line.split(":", 2)
                if len(parts) >= 3:
                    mp = parts[1]
                    count = int(parts[2]) if parts[2].isdigit() else 0
                    result["total_checked"] += count
                    reported_mounts.add(mp)
            elif line.startswith("IO_CHECK_FAIL:"):
                parts = line.split(":", 2)
                mp = parts[1] if len(parts) >= 2 else "unknown"
                detail = (parts[2] if len(parts) >= 3 else "") or "crc_mismatch"
                reported_mounts.add(mp)
                svc = next((s for s, m in all_mounts if m == mp), "unknown")
                result["total_checked"] += len(_INTEGRITY_FILE_SPECS)
                result["mismatches"].append(f"{svc}:{mp}")
                log.error(
                    "[verify] CRC MISMATCH on %s: %s (%s)",
                    hostname,
                    mp,
                    detail,
                )
            elif line.startswith("SKIP:"):
                parts = line.split(":", 2)
                mp = parts[1] if len(parts) >= 2 else "unknown"
                detail = (parts[2] if len(parts) >= 3 else "") or "stale_or_hung"
                reported_mounts.add(mp)
                svc = next((s for s, m in all_mounts if m == mp), "unknown")
                log.warning("[verify] Skipped %s on %s: %s", mp, hostname, detail)
                result["errors"].append(f"{svc}:{mp}:{detail}")

        for svc, mp in all_mounts:
            if mp not in reported_mounts:
                log.warning("[verify] No output for mount %s on %s", mp, hostname)
                result["errors"].append(f"{svc}:{mp}:no_output")

    def _verify_block_integrity(self, client: Any, service: str, result: Dict):
        """Verify CRC32C on block device baseline regions.

        Only verifies devices that had a baseline successfully written during
        _write_block_baseline. Devices used for background IO (e.g.
        background_img) are excluded to prevent false CRC mismatch reports.

        RBD targets are resolved by pool/image via live ``rbd device list``
        so device minor renumbering cannot silently verify the wrong volume.
        """
        all_baselined = self._integrity_checksums.get("block_baselined_devices", {})
        hostname = getattr(client, "hostname", str(client))
        baselined = (
            all_baselined.get(hostname, [])
            if isinstance(all_baselined, dict)
            else all_baselined
        )
        if not baselined:
            log.info(
                "[verify] No baselined block devices found for %s, skipping",
                service,
            )
            return

        live = self._get_integrity_block_devices(client, service)
        mapped = {
            (e["pool"], e["name"]): e["device"]
            for e in live
            if e.get("pool") and e.get("name")
        }
        mapped_devs = {e["device"] for e in live if e.get("device")}

        integrity_size = self._integrity.get("fio_baseline_size", "1G")
        block_verify_timeout = self._integrity.get(
            "verify_timeout", self._integrity.get("write_timeout", 600)
        )
        for key in baselined:
            if not isinstance(key, dict):
                continue
            pool, name = key.get("pool"), key.get("name")
            # Per-host list may mix RBD {pool,name} and NVMeoF {device}; only
            # process keys that belong to the service under verification.
            if service == "rbd":
                if not (pool and name):
                    continue
                label = f"{pool}/{name}"
                dev = mapped.get((pool, name))
                if not dev:
                    result["errors"].append(f"{service}:{label}:not_mapped")
                    log.warning("[verify] Baselined RBD not mapped: %s", label)
                    continue
            else:
                if pool or name:
                    continue
                dev = key.get("device")
                label = dev or "unknown"
                if not dev or dev not in mapped_devs:
                    result["errors"].append(f"{service}:{label}:not_mapped")
                    continue
            try:
                fio_cmd = (
                    f"fio --name=ugt-verify-{service}-fio --filename={dev} "
                    f"--offset=0 --size={integrity_size} "
                    f"--bs=4M --rw=read --numjobs=1 "
                    f"--verify=crc32c --do_verify=1 "
                    f"--ioengine=libaio --direct=1"
                )
                out, _ = self._heavy_client_ssh(
                    client,
                    sudo=True,
                    cmd=fio_cmd,
                    timeout=block_verify_timeout,
                    check_ec=False,
                )
                result["total_checked"] += 1
                if self._fio_verify_failed(out):
                    result["mismatches"].append(f"{service}:{label}")
                    log.error("[verify] Block CRC MISMATCH: %s (%s)", label, dev)
            except Exception as e:
                reason = remote_exec_failure_tag(e, block_verify_timeout)
                log.warning("[verify] Block verify error %s: %s", label, reason)
                result["errors"].append(f"{service}:{label}:{reason}")

    def _verify_rados_integrity(self, result: Dict):
        """Verify MD5 on RADOS integrity objects."""
        pool_name = self._integrity_checksums.get("rados_pool")
        obj_count_str = self._integrity_checksums.get("rados_count", "0")
        obj_count = int(obj_count_str)

        if not pool_name:
            log.warning("[verify] No RADOS integrity pool recorded")
            return

        installer = self.installer
        batch_size = self._integrity.get("rados_batch_size", 50)
        for batch_start in range(0, obj_count, batch_size):
            batch_end = min(batch_start + batch_size, obj_count)
            script_lines = []
            for i in range(batch_start, batch_end):
                obj = f"integrity_obj_{i:06d}"
                script_lines.append(
                    f"cur=$(rados -p {pool_name} get {obj} - | md5sum | "
                    f"awk '{{print $1}}'); "
                    f"stored=$(rados -p {pool_name} getxattr {obj} integrity_md5); "
                    f'if [ "$cur" = "$stored" ]; then echo "IO_CHECK_VERIFIED:{obj}"; '
                    f'else echo "IO_CHECK_FAIL:{obj}:got=$cur:want=$stored"; fi'
                )
            batch_script = "; ".join(script_lines)
            try:
                rados_verify_tmout = self._integrity.get("verify_timeout", 300)
                out, _ = installer.exec_command(
                    sudo=True,
                    cmd=(
                        f"cephadm shell -- bash -s <<'EOFVERIFY'\n"
                        f"{batch_script}\n"
                        f"EOFVERIFY"
                    ),
                    timeout=rados_verify_tmout,
                )
                for line in out.strip().splitlines():
                    line = line.strip()
                    if line.startswith("IO_CHECK_VERIFIED:"):
                        result["total_checked"] += 1
                    elif line.startswith("IO_CHECK_FAIL:"):
                        result["total_checked"] += 1
                        parts = line.split(":", 3)
                        obj_name = parts[1] if len(parts) > 1 else "unknown"
                        detail = parts[2] if len(parts) > 2 else ""
                        result["mismatches"].append(f"rados:{pool_name}/{obj_name}")
                        log.error(
                            "[verify] RADOS MD5 mismatch: %s (%s)",
                            obj_name,
                            detail,
                        )
            except Exception as e:
                log.warning(
                    "[verify] RADOS verify batch %d-%d: %s",
                    batch_start,
                    batch_end,
                    e,
                )
                result["errors"].append(f"rados:batch_{batch_start}-{batch_end}:{e}")

    # ------------------------------------------------------------------
    #  Internal: Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fio_verify_failed(output) -> bool:
        """Detect fio verification or runtime failures from command output.

        Checks four signals (any triggers a failure):
        1. Empty/missing output (fio not installed or file missing)
        2. "verify" + "bad" in text  (CRC/magic mismatch)
        3. Non-zero err= in the job summary line
        4. "error" or "failed" alongside "fio" (runtime crash)
        """
        text = str(output).lower().strip()
        if not text or "command not found" in text or "no such file" in text:
            return True
        if "verify" in text and "bad" in text:
            return True
        err_match = re.search(r"\berr=\s*(\d+)", text)
        if err_match and int(err_match.group(1)) != 0:
            return True
        if re.search(r"^fio:\s*(error|failed|fatal)", text, re.MULTILINE):
            return True
        return False

    @staticmethod
    def _build_fio_job_content(integrity_dir: str, rw: str, do_verify: bool) -> str:
        """Build fio INI-style job file content for all integrity file specs.

        Returns a multi-line string suitable for writing to a .fio job file.
        Each spec in ``_INTEGRITY_FILE_SPECS`` becomes a separate ``[job]``
        section, producing the same filenames as individual ``--name=`` CLI
        invocations (e.g. ``integrity_4k.0.0``).

        Args:
            integrity_dir: Directory containing integrity files.
            rw: fio read/write mode ("read" or "write").
            do_verify: Whether to enable verification during IO.

        Returns:
            Multi-line string of fio job file content (no trailing newline).
        """
        lines = [
            "[global]",
            f"rw={rw}",
            "numjobs=1",
            "verify=crc32c",
            f"do_verify={'1' if do_verify else '0'}",
            "ioengine=libaio",
            "direct=1",
        ]
        for job_name, fsize, bs in _INTEGRITY_FILE_SPECS:
            lines.extend(
                [
                    "",
                    f"[ugt-integrity-{job_name}]",
                    f"directory={integrity_dir}",
                    f"size={fsize}",
                    f"bs={bs}",
                ]
            )
        return "\n".join(lines)

    def _get_enabled_tools(self, service_type: str) -> List[str]:
        """Return list of enabled IO tools for a given service type."""
        io_tools_cfg = self.config.get("io_tools", {})
        service_tools = io_tools_cfg.get(service_type, {})
        return [tool for tool, enabled in service_tools.items() if enabled]

    def _get_mount_points(self, client: Any, service: str) -> List[str]:
        """Discover mount points for a given service type on a client."""
        try:
            if service == "cephfs":
                out, _ = client.exec_command(
                    sudo=True,
                    cmd="mount -t ceph,fuse.ceph-fuse | awk '{print $3}'",
                    timeout=15,
                )
            elif service == "nfs":
                out, _ = client.exec_command(
                    sudo=True,
                    cmd="mount -t nfs,nfs4 | awk '{print $3}'",
                    timeout=15,
                )
            elif service == "smb":
                out, _ = client.exec_command(
                    sudo=True,
                    cmd="mount -t cifs | awk '{print $3}'",
                    timeout=15,
                )
            else:
                return []
            return [mp.strip() for mp in out.strip().split("\n") if mp.strip()]
        except Exception:
            return []

    def _filter_healthy_nfs_mounts(
        self, client: Any, mount_points: List[str]
    ) -> List[str]:
        """Batch-probe NFS mounts and return only responsive ones.

        Runs a single SSH command that tests all mounts in parallel using
        background ``timeout 3 stat`` processes, avoiding the serial
        15-30s timeout per stale mount that exhausts SSH channels.
        """
        if not mount_points:
            return []
        # Build a single command: for each mount, background a quick stat,
        # then collect results. Output: one line per mount "OK <mp>" or nothing.
        probes = []
        for mp in mount_points:
            probes.append(
                f'(timeout 3 stat "{mp}/." >/dev/null 2>&1 && echo "OK {mp}") &'
            )
        probes.append("wait")
        batch_cmd = " ".join(probes)
        try:
            out, _ = client.exec_command(
                sudo=True,
                cmd=batch_cmd,
                timeout=len(mount_points) * 3 + 30,
            )
            ok_set = set()
            for line in out.strip().split("\n"):
                line = line.strip()
                if line.startswith("OK "):
                    ok_set.add(line[3:].strip())
            return [mp for mp in mount_points if mp in ok_set]
        except Exception as e:
            log.warning(
                "[bg_io] NFS mount health probe failed on %s: %s",
                getattr(client, "hostname", client),
                e,
            )
            return mount_points

    def _get_block_devices(self, client: Any, service: str) -> List[str]:
        """Discover block devices for RBD or NVMeoF on a client."""
        try:
            if service == "rbd":
                out, _ = client.exec_command(
                    sudo=True,
                    cmd="rbd device list --format json",
                    timeout=15,
                )
                devices = json.loads(out) if out.strip() else []
                return [d["device"] for d in devices]
            elif service == "nvmeof":
                out, _ = client.exec_command(
                    sudo=True,
                    cmd="nvme list -o json",
                    timeout=15,
                )
                data = json.loads(out) if out.strip() else {}
                nvme_devs = data.get("Devices", [])
                return [
                    d["DevicePath"]
                    for d in nvme_devs
                    if "ceph" in d.get("ModelNumber", "").lower()
                    or "nvmeof" in d.get("ModelNumber", "").lower()
                ]
        except Exception:
            pass
        return []

    def _get_integrity_block_devices(
        self, client: Any, service: str
    ) -> List[Dict[str, str]]:
        """Return integrity-eligible block targets as dicts.

        RBD entries: ``{pool, name, device}`` for ``rep_pool`` images that are
        not background/fill workloads. NVMeoF entries: ``{device}`` only.
        """
        try:
            if service == "rbd":
                out, _ = client.exec_command(
                    sudo=True,
                    cmd="rbd device list --format json",
                    timeout=15,
                )
                devices = json.loads(out) if out.strip() else []
                return [
                    {
                        "pool": d["pool"],
                        "name": d["name"],
                        "device": d["device"],
                    }
                    for d in devices
                    if d.get("pool", "") == "rep_pool"
                    and d.get("name")
                    and d.get("device")
                    and str(d["name"]).startswith("integrity_img")
                ]
            elif service == "nvmeof":
                return [{"device": d} for d in self._get_block_devices(client, service)]
        except Exception:
            pass
        return []

    def _get_integrity_pool(self) -> str:
        """Get or create the dedicated integrity RADOS pool name."""
        return "upgrade_integrity_pool"

    _SYSTEM_POOL_PREFIXES = (
        "cephfs.",
        ".mgr",
        ".rgw",
        "default.rgw",
        ".nfs",
        ".smb",
    )

    def _get_fill_pools(self) -> List[str]:
        """Get user-created data pools suitable for fill/bench operations.

        Excludes CephFS metadata, RGW, NFS, mgr, and integrity pools to
        prevent accidental writes into sensitive metadata pools.
        """
        try:
            pools_data = self.rados_obj.run_ceph_command("ceph osd pool ls")
            if isinstance(pools_data, list):
                return [
                    p
                    for p in pools_data
                    if "integrity" not in p
                    and not any(p.startswith(pfx) for pfx in self._SYSTEM_POOL_PREFIXES)
                    and ".meta" not in p
                ]
        except Exception:
            pass
        return ["rep_pool"]

    def _resolve_all_rgw_endpoints(self) -> List[str]:
        """Return host:port for every running RGW daemon."""
        endpoints = []
        try:
            daemons = self.rados_obj.run_ceph_command(
                cmd="ceph orch ps --daemon-type rgw"
            )
            if isinstance(daemons, list):
                for d in daemons:
                    if d.get("status_desc") == "running":
                        hostname = d.get("hostname", "")
                        ports = d.get("ports", [80])
                        port = ports[0] if ports else 80
                        if hostname and port > 0:
                            endpoints.append(f"{hostname}:{port}")
        except Exception as e:
            log.warning("RGW endpoint resolution failed: %s", e)
        return endpoints

    def _get_scaled_rgw_buckets(self) -> List[str]:
        """Return pre-created scaled RGW bucket names from config."""
        rgw_scale = self.config.get("scale", {}).get("rgw", {})
        nv = rgw_scale.get("non_versioned_buckets", 50)
        v = rgw_scale.get("versioned_buckets", 50)
        buckets = [f"bucket-nv-{i:04d}" for i in range(nv)]
        buckets += [f"bucket-ver-{i:04d}" for i in range(v)]
        return buckets

    def _get_cluster_df_stats(self) -> Tuple[Optional[float], int]:
        """Get cluster usage percent and total bytes from a single ceph df call.

        Returns:
            (usage_percent, total_bytes) -- usage_percent is None on failure;
            total_bytes falls back to 10 TiB on failure.
        """
        try:
            df = self.rados_obj.run_ceph_command("ceph df")
            stats = df.get("stats", {})
            total = stats.get("total_bytes", 0)
            used = stats.get("total_used_raw_bytes", 0)
            if total <= 0:
                log.warning("[fill] ceph df returned total_bytes=%s", total)
                return None, 10 * 1024**4
            return (used / total) * 100.0, total
        except Exception as e:
            log.warning("[fill] Failed to get cluster df stats: %s", e)
            return None, 10 * 1024**4

    def _get_cluster_usage_percent(self) -> Optional[float]:
        """Get current cluster raw usage percentage via ceph df."""
        return self._get_cluster_df_stats()[0]

    def _run_adaptive_batch(
        self,
        fill_config: Dict[str, Any],
        deployed_services: Set[str],
        size_mb: int,
        deadline: Optional[float] = None,
        effective_target: Optional[float] = None,
    ) -> None:
        """Launch one batch of fill processes with a specific size, wait for all.

        Each process writes exactly size_mb. All processes run in parallel
        and are awaited before returning.

        Args:
            deadline: Absolute time.time() value that prevents indefinite
                      hangs on stalled fill processes.
        """
        clients = self.ceph_cluster.get_nodes(role="client")
        if not clients:
            return

        pids: List[Tuple[Any, int]] = []
        tool_idx = 0

        for svc in deployed_services:
            if svc not in self._FILLABLE_SERVICES:
                continue
            tools = self._get_enabled_tools(svc)
            if not tools:
                continue

            fillable = self._get_fillable_tools(svc, tools)
            if not fillable:
                continue

            for tool in fillable:
                client = clients[tool_idx % len(clients)]
                tool_idx += 1
                try:
                    pid = self._launch_fill_process(client, svc, tool, size_mb=size_mb)
                    if pid:
                        pids.append((client, pid))
                except Exception as e:
                    log.warning("[fill][batch] Failed %s/%s: %s", svc, tool, e)

        if not pids:
            log.warning("[fill][batch] No fill processes launched, skipping wait")
            return

        target_pct = (
            effective_target
            if effective_target is not None
            else fill_config.get("target_percent", 35)
        )

        def _fill_target_reached():
            usage = self._get_cluster_usage_percent()
            return usage is not None and usage >= target_pct

        poll_interval = 30
        all_pids: List[Tuple[Any, int]] = list(pids)

        while all_pids:
            time.sleep(poll_interval)

            if time.time() >= deadline:
                log.warning("[fill][batch] Deadline reached during adaptive batch")
                break

            if self._fill_stop_event.is_set():
                break

            if _fill_target_reached():
                log.info(
                    "[fill][batch] Target %d%% reached, "
                    "stopping %d remaining processes",
                    target_pct,
                    len(all_pids),
                )
                break

            all_pids = self._poll_and_reap_pids(all_pids)
            if not all_pids:
                break

        self._kill_remaining_pids(all_pids)

    def _exec_background(
        self, client: Any, cmd: str, log_tag: str = ""
    ) -> Tuple[Optional[int], str]:
        """Execute a command in background and return (PID, log_path).

        Output is saved to /tmp/ugt_<tag>_<unique_id>.log for
        post-mortem debugging.  The unique_id is a timestamp+random
        suffix to avoid collisions across rapid launches.

        After launch, waits briefly and ``kill -0``s the PID so wrappers
        that exit immediately (e.g. vdbench without Java) are not
        registered — avoids false IO-continuity failures later.

        On SSH ``ChannelException`` (channel exhaustion), attempts one
        reconnect before giving up.
        """
        safe_tag = log_tag or "io"
        unique_id = f"{int(time.time() * 1000)}_{random.randint(10000, 99999)}"
        log_path = f"/tmp/ugt_{safe_tag}_{unique_id}.log"
        full_cmd = f"nohup {cmd} > {log_path} 2>&1 & echo $!"

        for attempt in range(2):
            try:
                out, _ = client.exec_command(sudo=True, cmd=full_cmd, timeout=30)
                pid_str = out.strip().split("\n")[-1].strip()
                if pid_str.isdigit():
                    pid = int(pid_str)
                    time.sleep(2)
                    try:
                        client.exec_command(
                            sudo=True,
                            cmd=f"kill -0 {pid}",
                            timeout=10,
                            check_ec=False,
                        )
                        if getattr(client, "exit_status", 1) != 0:
                            log.warning(
                                "[exec_bg] %s PID %d died immediately "
                                "(log=%s); not registering",
                                safe_tag,
                                pid,
                                log_path,
                            )
                            return None, log_path
                    except Exception as exc:
                        log.debug(
                            "[exec_bg] %s PID %d liveness check error: %s",
                            safe_tag,
                            pid,
                            exc,
                        )
                    return pid, log_path
                break
            except Exception as e:
                is_channel_err = "ChannelException" in type(e).__name__ or (
                    "Channel" in str(e) and "Connect failed" in str(e)
                )
                if is_channel_err and attempt == 0:
                    log.warning(
                        "[exec_bg] SSH channel exhausted on %s, reconnecting",
                        getattr(client, "hostname", client),
                    )
                    try:
                        client.reconnect()
                        time.sleep(2)
                    except Exception:
                        pass
                    continue
                log.warning("[exec_bg] Failed to launch %s: %s", log_tag, e)
                break
        return None, ""

    def _is_mount_healthy(
        self, client: Any, mount_point: str, timeout: int = 30
    ) -> bool:
        """Check if a mount point is responsive.

        Uses a configurable stat timeout (default 30s) to accommodate MDS
        session recovery windows during rolling upgrades.
        """
        try:
            client.exec_command(
                sudo=True,
                cmd=(
                    f"timeout {timeout} stat {mount_point}/. "
                    f"&& timeout {timeout} df {mount_point}"
                ),
                timeout=timeout * 2 + 15,
            )
            return True
        except Exception:
            return False

    def _attempt_lazy_umount(self, client: Any, mount_point: str):
        """Attempt lazy unmount of a stale mount point."""
        try:
            client.exec_command(
                sudo=True,
                cmd=f"umount -l {mount_point}",
                timeout=30,
                check_ec=False,
            )
        except Exception as e:
            log.debug("[cleanup] Lazy umount failed %s: %s", mount_point, e)

    _MOUNT_TYPE_FILTERS = {
        "nfs": ("nfs,nfs4", "nfs4"),
        "cephfs": ("ceph,fuse.ceph-fuse", "ceph"),
    }

    def _get_mount_info(
        self, client: Any, mount_point: str, service: str
    ) -> Dict[str, str]:
        """Extract mount details from the mount table before umount.

        Handles NFS (nfs/nfs4), CephFS kernel (ceph), and CephFS FUSE
        (fuse.ceph-fuse) mounts.

        Args:
            client: Client node handle.
            mount_point: The local mount path.
            service: ``"nfs"`` or ``"cephfs"``.

        Returns:
            Dict with 'source', 'fstype', 'options' (and 'fs_name' for
            CephFS), or empty dict if not found.
        """
        fs_filter, default_fstype = self._MOUNT_TYPE_FILTERS.get(
            service, ("nfs,nfs4", "nfs4")
        )
        try:
            out, _ = client.exec_command(
                sudo=True,
                cmd=(
                    f"mount -t {fs_filter} | grep ' {mount_point} ' "
                    f"|| mount -t {fs_filter} | grep ' {mount_point}$'"
                ),
                timeout=10,
            )
            line = out.strip().split("\n")[0] if out.strip() else ""
            if not line:
                return {}

            parts = line.split()
            info: Dict[str, str] = {"source": parts[0]}
            try:
                type_idx = parts.index("type")
                info["fstype"] = parts[type_idx + 1]
            except (ValueError, IndexError):
                info["fstype"] = default_fstype

            paren_start = line.find("(")
            paren_end = line.find(")")
            info["options"] = (
                line[paren_start + 1 : paren_end]
                if paren_start != -1 and paren_end != -1
                else ""
            )

            if service == "cephfs":
                fs_name = ""
                root_path = "/"
                for opt in info["options"].split(","):
                    opt = opt.strip()
                    if opt.startswith("fs="):
                        fs_name = opt.split("=", 1)[1]
                    elif opt.startswith("root_path="):
                        root_path = opt.split("=", 1)[1]
                if not fs_name:
                    base = mount_point.rstrip("/").rsplit("/", 1)[-1]
                    m = re.match(r"^(.+?)_sv\d+_(kernel|fuse)$", base)
                    if m:
                        fs_name = m.group(1)
                info["fs_name"] = fs_name or "cephfs_direct"
                if info["fstype"] == "ceph" and ":" in info.get("source", ""):
                    src_path = info["source"].split(":", 1)[1]
                    if src_path and src_path != "/":
                        root_path = src_path
                info["root_path"] = root_path

            return info
        except Exception as e:
            log.debug(
                "[mount_health] Could not get %s mount info for %s: %s",
                service,
                mount_point,
                e,
            )
            return {}

    @staticmethod
    def _kernel_cephfs_remount_options(mount_info: Dict[str, str]) -> str:
        """Build mount -o string for kernel CephFS without replaying masked secrets."""
        fs_name = mount_info.get("fs_name", "cephfs_direct")
        for opt in mount_info.get("options", "").split(","):
            opt = opt.strip()
            if opt.startswith("mds_namespace="):
                fs_name = opt.split("=", 1)[1]
            elif opt.startswith("fs="):
                fs_name = opt.split("=", 1)[1]
        return f"name=admin,fs={fs_name}"

    def _attempt_remount(
        self,
        client: Any,
        mount_point: str,
        mount_info: Dict[str, str],
        service: str,
    ) -> bool:
        """Attempt to remount a mount point after lazy umount.

        Handles NFS, CephFS kernel, and CephFS FUSE mounts.

        Args:
            client: Client node handle.
            mount_point: The local mount path to remount.
            mount_info: Dict from ``_get_mount_info``.
            service: ``"nfs"`` or ``"cephfs"``.

        Returns:
            True if remount succeeded, False otherwise.
        """
        source = mount_info.get("source", "")
        fstype = mount_info.get("fstype", "nfs4")
        options = mount_info.get("options", "")
        hostname = getattr(client, "hostname", str(client))

        if service == "cephfs" and fstype == "fuse.ceph-fuse":
            fs_name = mount_info.get("fs_name", "cephfs_direct")
            root_path = mount_info.get("root_path", "/")
            root_flag = f" -r {root_path}" if root_path and root_path != "/" else ""
            mount_cmd = (
                f"mkdir -p {mount_point} && "
                f"timeout 30 ceph-fuse -n client.admin "
                f"--client_fs {fs_name}{root_flag} {mount_point}"
            )
        elif service == "cephfs" and fstype == "ceph":
            if not source or source == "none":
                log.warning(
                    "[mount_health] Cannot remount %s: no source address",
                    mount_point,
                )
                return False
            remount_opts = self._kernel_cephfs_remount_options(mount_info)
            mount_cmd = (
                f"mkdir -p {mount_point} && "
                f"timeout 30 mount -t ceph {source} {mount_point} "
                f"-o {remount_opts}"
            )
        else:
            if not source or source == "none":
                log.warning(
                    "[mount_health] Cannot remount %s: no source address",
                    mount_point,
                )
                return False
            opt_flag = f" -o {options}" if options else ""
            mount_cmd = (
                f"mkdir -p {mount_point} && "
                f"timeout 30 mount -t {fstype}{opt_flag} "
                f"{source} {mount_point}"
            )

        log.info(
            "[mount_health] Attempting %s remount on %s: %s (type=%s)",
            service.upper(),
            hostname,
            mount_point,
            fstype,
        )
        try:
            client.exec_command(sudo=True, cmd=mount_cmd, timeout=45)
            if self._is_mount_healthy(client, mount_point):
                log.info(
                    "[mount_health] Remount SUCCEEDED: %s on %s",
                    mount_point,
                    hostname,
                )
                return True
            log.warning(
                "[mount_health] Remount completed but mount is not healthy: "
                "%s on %s",
                mount_point,
                hostname,
            )
            return False
        except Exception as e:
            log.error(
                "[mount_health] Remount FAILED for %s on %s: %s",
                mount_point,
                hostname,
                e,
            )
            return False
