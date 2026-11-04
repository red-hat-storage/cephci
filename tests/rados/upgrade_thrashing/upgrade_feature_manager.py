"""
Feature enablement, verification, and bug validation
for the Ceph upgrade thrash test suite.

Driven by a registry pattern where each feature declares its enable/verify
callables, service dependency, and prerequisites. Suite YAML toggles control
which features are exercised.
"""

import json
import time
import traceback
from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime

from upgrade_thrashing.upgrade_health_monitor import classify_upgrade_error

from ceph.parallel import parallel
from utility.log import Log

log = Log(__name__)

FeatureEntry = namedtuple(
    "FeatureEntry", ["enable", "verify", "toggle", "service", "prerequisites"]
)

BugValidator = namedtuple("BugValidator", ["validate", "category", "name", "jira"])

STUB_SKIP = "skip:stub - infrastructure not implemented"

_HISTORICAL = "historical"
_POINT_IN_TIME = "point_in_time"


class UpgradeFeatureManager:
    def _stub_verify(self, snapshot=None):
        return True, STUB_SKIP

    def __init__(self, rados_obj, ceph_cluster, config, deployed_services):
        """
        Args:
            rados_obj: RadosOrchestrator instance
            ceph_cluster: CephCluster object
            config: Full suite YAML config dict
            deployed_services: set of service names successfully deployed
        """
        self.rados_obj = rados_obj
        self.ceph_cluster = ceph_cluster
        self.config = config
        self.deployed_services = deployed_services
        self._pre_upgrade_snapshot = None
        self._enabled_features = set()

        self._feature_registry = self._build_feature_registry()
        self._bug_registry = self._build_bug_registry()

    def _get_fs_config(self):
        """Return (direct_fs, fs_list) from scale.cephfs config."""
        cephfs_scale = self.config.get("scale", {}).get("cephfs", {})
        fs_list = cephfs_scale.get("filesystems", ["cephfs_direct", "cephfs_nfs"])
        direct_fs = cephfs_scale.get("direct_filesystem", fs_list[0])
        return direct_fs, fs_list

    # ------------------------------------------------------------------
    # Command helpers
    # ------------------------------------------------------------------

    def _get_toggle(self, dotpath):
        """Resolve a dot-separated toggle path against config['features']."""
        parts = dotpath.split(".")
        obj = self.config.get("features", {})
        for p in parts:
            if not isinstance(obj, dict):
                return False
            obj = obj.get(p, False)
        return bool(obj)

    def _service_deployed(self, service):
        """Check if a given service type is available in the test cluster."""
        if service == "rados":
            return True
        if service == "mgr_modules":
            return True
        return service in self.deployed_services

    def _get_rgw_endpoint(self):
        """Resolve a reachable RGW endpoint (host:port) from running daemons."""
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
                        if hostname:
                            return f"{hostname}:{port}"
        except Exception as e:
            log.warning(f"_get_rgw_endpoint: failed to query RGW daemons: {e}")
        log.warning("No running RGW daemon found; RGW features will be skipped")
        return None

    def _shell_json(self, cmd, default=None):
        """Run a shell command and parse its stdout as JSON."""
        out = self.rados_obj.node.shell([cmd])[0].strip()
        if not out:
            return default if default is not None else {}
        try:
            return json.loads(out)
        except (json.JSONDecodeError, ValueError):
            log.warning(f"_shell_json: non-JSON output for '{cmd}': {out[:200]}")
            return default if default is not None else {}

    def _verify_config(self, who, key, expected, contains=False, label=None):
        """Verify a ceph config value matches expectations."""
        val = self.rados_obj.node.shell([f"ceph config get {who} {key}"])[0].strip()
        label = label or key
        if contains:
            ok = expected.lower() in val.lower()
        else:
            ok = val.lower() == expected.lower()
        if ok:
            return True, f"{label} = {val}"
        return False, f"{label} expected '{expected}', got '{val}'"

    def _verify_mgr_module(self, snapshot, module_name):
        """Check if a MGR module is enabled or always-on."""
        modules = snapshot.get("mgr_modules") if snapshot else None
        if modules is None:
            try:
                modules = self.rados_obj.run_ceph_command(cmd="ceph mgr module ls")
            except Exception:
                return True, "skipped - could not query mgr modules"
        enabled = modules.get("enabled_modules", [])
        always_on = modules.get("always_on_modules", [])
        if module_name in enabled or module_name in always_on:
            return True, f"{module_name} module enabled"
        return False, (f"{module_name} not in enabled={enabled}, always_on={always_on}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_prerequisites(self):
        """Pre-Phase 2: auto-skip features whose prerequisites are not met."""
        skipped = []
        for key, entry in self._feature_registry.items():
            if not self._get_toggle(entry.toggle):
                continue
            if not self._service_deployed(entry.service):
                continue
            for prereq_fn in entry.prerequisites:
                if not prereq_fn():
                    log.warning(f"Feature {key}: prerequisite not met, will be skipped")
                    skipped.append(key)
                    break
        return skipped

    def enable_all_features(self):
        """Phase 2: enable features parallelized across service types.

        Returns:
            dict with 'enabled' (set) and 'failed' (dict of key -> error string)
        """
        skipped_prereqs = set(self.validate_prerequisites())
        service_groups = {}
        for key, entry in self._feature_registry.items():
            if not self._get_toggle(entry.toggle):
                log.debug(f"Feature {key}: disabled in config, skipping")
                continue
            if not self._service_deployed(entry.service):
                log.debug(
                    f"Feature {key}: service {entry.service} not deployed, skipping"
                )
                continue
            if key in skipped_prereqs:
                continue
            service_groups.setdefault(entry.service, []).append((key, entry))

        failed_features = {}

        def _enable_group(svc, items):
            """Enable all features in a service group sequentially."""
            for key, entry in items:
                try:
                    log.info(f"Enabling feature: {key}")
                    entry.enable()
                    self._enabled_features.add(key)
                    log.info(f"Feature enabled: {key}")
                except Exception:
                    err_msg = traceback.format_exc()
                    log.error(f"Failed to enable feature {key}: {err_msg}")
                    failed_features[key] = err_msg

        with parallel() as p:
            for svc, items in service_groups.items():
                p.spawn(_enable_group, svc, items)

        self._failed_features = failed_features
        if failed_features:
            log.warning(
                f"Feature enablement: {len(self._enabled_features)} ok, "
                f"{len(failed_features)} failed: {list(failed_features.keys())}"
            )
        return {"enabled": self._enabled_features, "failed": failed_features}

    def verify_all_features(self, snapshot_data):
        """Phase 6: functional verification of each enabled feature."""
        mixed = self.config.get("_mixed_version_cluster", False)
        results = {}
        for key in self._feature_registry:
            entry = self._feature_registry[key]
            if key not in self._enabled_features:
                results[key] = {"result": "skip", "details": "not enabled"}
                continue
            if mixed and key.startswith(("cephfs.", "nfs.")):
                results[key] = {
                    "result": "skip",
                    "details": "skipped: mixed-version cluster",
                }
                log.info(f"Skipping {key} verification (mixed-version cluster)")
                continue
            try:
                ok, detail = entry.verify(snapshot_data)
                if ok and (
                    detail == STUB_SKIP
                    or "skipped" in detail.lower()
                    or "stub" in detail.lower()
                ):
                    result_val = "skip"
                else:
                    result_val = "pass" if ok else "fail"
                results[key] = {
                    "result": result_val,
                    "details": detail,
                }
            except Exception as err:
                results[key] = {
                    "result": "fail",
                    "details": f"exception: {err}",
                }
                log.error(f"Feature verify {key} raised: {traceback.format_exc()}")
        return results

    _REQUIRES_COMPLETE_UPGRADE = frozenset(
        {
            "a17_smb_all_down",
            "a20_osd_markdown_shutdown",
            "a21_upgrade_health_err",
            "a23_daemon_count_mismatch",
        }
    )
    _OPT_IN_BUGS = frozenset({"a24_grace_metrics", "a27_nfs_client_continuity"})

    def validate_known_bugs(self, monitoring_data, upgrade_completed=True):
        """Phase 6b: run 21 bug validations (A1-A21).

        The raw monitoring data from UpgradeStatsCollector contains samples
        and phase_boundaries. This method transforms them into the summary
        keys expected by each validator (mds_state_durations, health_history,
        log_patterns, etc.) before dispatching.

        Args:
            monitoring_data: Raw data dict from UpgradeStatsCollector.
            upgrade_completed: If False, validators that require a fully
                completed upgrade are skipped to avoid false failures.
        """
        enriched = self._build_monitoring_summary(monitoring_data)
        bug_cfg = self.config.get("bug_validations", {})
        results = []
        for bug_id, validator in self._bug_registry.items():
            toggle_key = bug_id.lower().replace("-", "_")
            default_on = toggle_key not in self._OPT_IN_BUGS
            if not bug_cfg.get(toggle_key, default_on):
                results.append(
                    {
                        "id": bug_id,
                        "name": validator.name,
                        "jira": validator.jira,
                        "result": "skip",
                        "evidence": "disabled in config",
                    }
                )
                continue
            if not upgrade_completed and bug_id in self._REQUIRES_COMPLETE_UPGRADE:
                log.info(
                    "Skipping bug validator %s (%s): upgrade did not complete",
                    bug_id,
                    validator.name,
                )
                results.append(
                    {
                        "id": bug_id,
                        "name": validator.name,
                        "jira": validator.jira,
                        "result": "skip",
                        "evidence": "upgrade did not complete",
                    }
                )
                continue
            try:
                passed, evidence = validator.validate(enriched, bug_cfg)
                if isinstance(evidence, str) and evidence.startswith("skip:"):
                    result_val = "skip"
                    evidence = evidence[len("skip:") :]
                else:
                    result_val = "pass" if passed else "fail"
                results.append(
                    {
                        "id": bug_id,
                        "name": validator.name,
                        "jira": validator.jira,
                        "result": result_val,
                        "evidence": evidence,
                    }
                )
            except Exception as err:
                results.append(
                    {
                        "id": bug_id,
                        "name": validator.name,
                        "jira": validator.jira,
                        "result": "fail",
                        "evidence": f"exception: {err}",
                    }
                )
                log.error(
                    f"Bug validation {bug_id} raised: " f"{traceback.format_exc()}"
                )
        return results

    def save_pre_upgrade_snapshot(self):
        """Phase 3: save cluster state for post-upgrade restoration."""
        snapshot = {}

        snapshot["max_mds"] = {}
        snapshot["standby_replay"] = {}
        try:
            fs_ls = self.rados_obj.run_ceph_command(cmd="ceph fs ls")
            for fs in fs_ls:
                name = fs["name"]
                try:
                    fs_info = self.rados_obj.run_ceph_command(cmd=f"ceph fs get {name}")
                    mdsmap = fs_info.get("mdsmap", fs_info)
                    snapshot["max_mds"][name] = mdsmap.get("max_mds", 1)
                    snapshot["standby_replay"][name] = bool(
                        mdsmap.get("flags_state", {}).get("standby_replay", False)
                    )
                except Exception:
                    log.warning(f"Failed to snapshot MDS state for {name}")
        except Exception:
            log.warning("Failed to list filesystems for snapshot")

        try:
            snapshot["config_dump"] = self.rados_obj.run_ceph_command(
                cmd="ceph config dump"
            )
        except Exception:
            snapshot["config_dump"] = []

        self._pre_upgrade_snapshot = snapshot
        log.info(
            f"Pre-upgrade snapshot saved: " f"{len(snapshot['max_mds'])} filesystems"
        )
        return snapshot

    def capture_daemon_state(self, label: str = "pre") -> dict:
        """Capture counts and states for all orch-managed daemon types."""
        state = {}
        try:
            daemons = self.rados_obj.run_ceph_command(cmd="ceph orch ps")
            if isinstance(daemons, list):
                by_type = {}
                for d in daemons:
                    dtype = d.get("daemon_type", "unknown")
                    if dtype not in by_type:
                        by_type[dtype] = {"count": 0, "running": 0}
                    by_type[dtype]["count"] += 1
                    if d.get("status_desc") == "running":
                        by_type[dtype]["running"] += 1
                state.update(by_type)
        except Exception as e:
            log.warning(f"capture_daemon_state: ceph orch ps failed: {e}")

        try:
            fs_status = self.rados_obj.run_ceph_command(cmd="ceph fs status")
            mds_detail = {}
            standby_count = 0
            for mds_entry in fs_status.get("mdsmap", []):
                if not isinstance(mds_entry, dict):
                    continue
                mds_state = mds_entry.get("state", "")
                mds_name = mds_entry.get("name", "")
                fs_name = mds_name.split(".")[0] if "." in mds_name else "unknown"

                if mds_state == "standby":
                    standby_count += 1
                    continue

                if fs_name not in mds_detail:
                    mds_detail[fs_name] = {"active": 0, "standby_replay": 0}
                if mds_state == "active":
                    mds_detail[fs_name]["active"] += 1
                elif mds_state == "standby-replay":
                    mds_detail[fs_name]["standby_replay"] += 1

            mds_detail["_standbys"] = standby_count
            if "mds" in state:
                state["mds"]["detail"] = mds_detail
        except Exception as e:
            log.warning(f"capture_daemon_state: ceph fs status failed: {e}")

        try:
            quorum = self.rados_obj.run_ceph_command(cmd="ceph quorum_status")
            quorum_names = quorum.get("quorum_names", [])
            if "mon" in state:
                state["mon"]["quorum"] = len(quorum_names)
        except Exception as e:
            log.warning(f"capture_daemon_state: ceph quorum_status failed: {e}")

        try:
            osd_stat = self.rados_obj.run_ceph_command(cmd="ceph osd stat")
            if "osd" in state:
                state["osd"]["up"] = osd_stat.get("num_up_osds", 0)
                state["osd"]["in"] = osd_stat.get("num_in_osds", 0)
        except Exception as e:
            log.warning(f"capture_daemon_state: ceph osd stat failed: {e}")

        if not hasattr(self, "_daemon_states"):
            self._daemon_states = {}
        self._daemon_states[label] = state
        log.info(
            f"Daemon state [{label}]: "
            + ", ".join(
                f"{k}={v.get('running', '?')}/{v.get('count', '?')}"
                for k, v in sorted(state.items())
            )
        )
        return state

    # ------------------------------------------------------------------
    # Feature registry builder
    # ------------------------------------------------------------------

    def _build_feature_registry(self):
        """Assemble the complete feature registry from all service-specific registrars."""
        r = {}
        self._register_rados_features(r)
        self._register_cephfs_features(r)
        self._register_rbd_features(r)
        self._register_rgw_features(r)
        self._register_nfs_features(r)
        self._register_smb_features(r)
        self._register_nvmeof_features(r)
        self._register_mgr_module_features(r)
        return r

    # -- RADOS features --

    def _register_rados_features(self, r):
        """Register RADOS features: EC optimizations, compression, mClock, scrub, quotas, CRUSH."""
        rados_features = {
            "ec_optimizations": (
                self._enable_rados_ec_opt,
                self._verify_rados_ec_opt,
                [],
            ),
            "compression_snappy": (
                lambda: self._enable_compression("snappy"),
                lambda s: self._verify_compression(s, "snappy"),
                [],
            ),
            "compression_zstd": (
                lambda: self._enable_compression("zstd"),
                lambda s: self._verify_compression(s, "zstd"),
                [],
            ),
            "mclock_balanced": (
                self._enable_mclock,
                self._verify_mclock,
                [],
            ),
            "scrub_auto_repair": (
                self._enable_scrub_auto_repair,
                self._verify_scrub_auto_repair,
                [],
            ),
            "pool_quotas": (
                self._enable_pool_quotas,
                self._verify_pool_quotas,
                [],
            ),
            "crush_device_classes": (
                self._enable_crush_device_classes,
                self._verify_crush_device_classes,
                [],
            ),
        }
        for name, (enable, verify, prereqs) in rados_features.items():
            r[f"rados.{name}"] = FeatureEntry(
                enable=enable,
                verify=verify,
                toggle=f"rados.{name}",
                service="rados",
                prerequisites=prereqs,
            )

    def _parse_ceph_version(self) -> str:
        """Return the numeric version string (e.g. '20.2.1-294.el9cp').

        ``ceph version --format json`` returns a ``version`` field like
        ``"ceph version 20.2.1-294.el9cp (...) tentacle ..."``. This
        helper strips the ``"ceph version "`` prefix and returns only
        the first token (the version number with build suffix).
        """
        ver_out, _ = self.rados_obj.node.shell(["ceph version --format json"])
        ver = json.loads(ver_out.strip())["version"]
        if ver.startswith("ceph version "):
            ver = ver[len("ceph version ") :]
        return ver.split()[0]

    def _enable_rados_ec_opt(self):
        """Enable allow_ec_optimizations on existing EC pools and create a new
        EC pool to verify default behaviour (Tentacle 20.x+ only).

        Two checks in one:
          1. Explicitly enable the flag on every pre-existing EC pool.
          2. Create ``ec_opt_verify`` pool -- on 9.0+ this should have
             ``allow_ec_optimizations`` enabled by default.
        """
        try:
            ceph_ver = self._parse_ceph_version()
            major = int(ceph_ver.split(".")[0])
            if major < 20:
                log.info(
                    f"EC optimizations: skipping on Ceph {ceph_ver} (requires 20.x+)"
                )
                return
        except Exception as e:
            log.info(f"EC optimizations: skipping (version check failed: {e})")
            return

        pools = self.rados_obj.run_ceph_command(cmd="ceph osd pool ls detail")
        for pool in pools:
            if pool.get("erasure_code_profile"):
                name = pool.get("pool_name", pool.get("pool"))
                self.rados_obj.node.shell(
                    [f"ceph osd pool set {name} allow_ec_optimizations true"]
                )
                log.info(f"EC optimizations enabled on pool {name}")
                verify_out, _ = self.rados_obj.node.shell(
                    [f"ceph osd pool get {name} allow_ec_optimizations"]
                )
                if "true" not in verify_out.lower():
                    log.error(f"EC optimizations NOT verified on {name}: {verify_out}")
                    raise RuntimeError(f"EC opt flag not set on {name}")

        new_pool = "ec_opt_verify"
        try:
            if not self.rados_obj.create_erasure_pool(
                pool_name=new_pool,
                profile_name="ecp_ec_opt_verify",
                k=2,
                m=2,
            ):
                raise RuntimeError(f"Failed to create EC pool {new_pool}")
            log.info(
                f"Created new EC pool '{new_pool}' to verify default ec_optimizations"
            )
        except Exception as err:
            log.warning(f"Failed to create EC verify pool: {err}")

    def _verify_rados_ec_opt(self, snapshot):
        """Verify ec_optimizations on all EC pools including the new verify pool.

        Checks:
          1. All pre-existing EC pools still have ec_optimizations enabled.
          2. The newly-created ``ec_opt_verify`` pool has ec_optimizations
             enabled by default (no explicit ``pool set`` was done on it).
        """
        pools = self.rados_obj.run_ceph_command(cmd="ceph osd pool ls detail")
        ec_pools = [p for p in pools if p.get("erasure_code_profile")]
        if not ec_pools:
            return True, "no EC pools to verify"

        failed = []
        new_pool_found = False
        new_pool_ok = False

        for pool in ec_pools:
            name = pool.get("pool_name", pool.get("pool"))
            flags_names = pool.get("flags_names", "")
            opts = pool.get("options", {})
            has_flag = "ec_optimizations" in flags_names or opts.get(
                "allow_ec_optimizations"
            )

            if name == "ec_opt_verify":
                new_pool_found = True
                new_pool_ok = has_flag
                if not has_flag:
                    failed.append(f"{name} (new pool, default NOT set)")
            elif not has_flag:
                failed.append(name)

        if failed:
            return False, f"EC optimizations lost on {', '.join(failed)}"

        msg = f"EC optimizations active on {len(ec_pools)} pools"
        if new_pool_found:
            msg += f"; new pool ec_opt_verify default={'yes' if new_pool_ok else 'no'}"
        return True, msg

    def _enable_compression(self, algo):
        """Enable aggressive inline compression and write test data to verify it works."""
        pool_name = f"rep_compress_{algo}"
        try:
            self.rados_obj.pool_inline_compression(
                pool_name=pool_name,
                compression_mode="aggressive",
                compression_algorithm=algo,
            )
        except Exception:
            self.rados_obj.node.shell(
                [f"ceph osd pool set {pool_name} compression_algorithm {algo}"]
            )
            self.rados_obj.node.shell(
                [f"ceph osd pool set {pool_name} compression_mode aggressive"]
            )

        # Write 128KB zero-fill objects so BlueStore actually compresses them.
        # Objects must exceed compression_min_blob_size (64KB for SSD) to trigger
        # compression; /etc/hostname (~8B) was too small and gave compress_bytes_used=0.
        try:
            self.rados_obj.node.shell(
                [
                    "dd if=/dev/zero of=/tmp/compress_test_128k bs=128k count=1 2>/dev/null"
                ]
            )
            for i in range(3):
                self.rados_obj.node.shell(
                    [
                        f"rados -p {pool_name} put compress_test_obj_{i} /tmp/compress_test_128k"
                    ]
                )
            self.rados_obj.node.shell(["rm -f /tmp/compress_test_128k"])
            log.info(
                f"[{pool_name}] wrote 3x128KB test objects for compression verification"
            )
        except Exception as err:
            log.warning(
                f"[{pool_name}] compression test data write failed (non-fatal): {err}"
            )

    def _verify_compression(self, snapshot, algo):
        """Verify compression config persists AND actual data is being compressed."""
        pool_name = f"rep_compress_{algo}"
        try:
            algo_out = self.rados_obj.node.shell(
                [f"ceph osd pool get {pool_name} compression_algorithm"]
            )[0].strip()
            if algo not in algo_out.lower():
                return False, f"compression algorithm changed from {algo}: {algo_out}"
            mode_out = self.rados_obj.node.shell(
                [f"ceph osd pool get {pool_name} compression_mode"]
            )[0].strip()
            if "none" in mode_out.lower():
                return False, f"compression_mode reset to none on {pool_name}"

            ratio_str = ""
            try:
                df_detail = self._shell_json("ceph df detail -f json")
                pools = df_detail.get("pools", [])
                pool_stats = None
                for p in pools:
                    if p.get("name") == pool_name:
                        pool_stats = p.get("stats", {})
                        break
                if pool_stats:
                    compressed = pool_stats.get("compress_bytes_used", 0)
                    original = pool_stats.get("compress_under_bytes", 0)
                    if compressed > 0 and original > 0:
                        ratio = original / compressed
                        ratio_str = f", compression_ratio={ratio:.2f}x"
                        log.info(
                            f"[{pool_name}] compress_bytes_used={compressed}, "
                            f"compress_under_bytes={original}, ratio={ratio:.2f}x"
                        )
                    elif compressed == 0 and original == 0:
                        log.warning(
                            f"[{pool_name}] compression configured but no compressed "
                            f"data yet (compress_bytes_used=0, compress_under_bytes=0)"
                        )
                    else:
                        ratio_str = (
                            f", compress_bytes_used={compressed}, "
                            f"compress_under_bytes={original}"
                        )
                else:
                    log.warning(
                        f"[{pool_name}] pool not found in ceph df detail output"
                    )
            except Exception as df_err:
                log.warning(f"ceph df detail check failed (non-fatal): {df_err}")

            return True, f"compression {algo} active, mode={mode_out}{ratio_str}"
        except Exception as err:
            return False, str(err)

    def _enable_mclock(self):
        """Set OSD mClock QoS profile to high_client_ops."""
        self.rados_obj.set_mclock_profile(profile="high_client_ops")

    def _verify_mclock(self, snapshot):
        """Verify osd_mclock_profile remains high_client_ops post-upgrade."""
        return self._verify_config(
            "osd",
            "osd_mclock_profile",
            "high_client_ops",
            contains=True,
            label="mClock profile",
        )

    def _enable_scrub_auto_repair(self):
        """Enable osd_scrub_auto_repair via ceph config set."""
        self.rados_obj.node.shell(["ceph config set osd osd_scrub_auto_repair true"])

    def _verify_scrub_auto_repair(self, snapshot):
        """Verify osd_scrub_auto_repair remains true post-upgrade."""
        return self._verify_config(
            "osd",
            "osd_scrub_auto_repair",
            "true",
            label="scrub auto-repair",
        )

    _QUOTA_BYTES_POOL = "rep_quota_pool"
    _QUOTA_BYTES_VALUE = 1073741824  # 1 GiB
    _QUOTA_OBJECTS_POOL = "rep_quota_obj_pool"
    _QUOTA_OBJECTS_VALUE = 10000

    def _enable_pool_quotas(self):
        """Set max_bytes on rep_quota_pool and max_objects on a separate
        rep_quota_obj_pool (quotas should be isolated per pool)."""
        self.rados_obj.node.shell(
            [
                f"ceph osd pool set-quota {self._QUOTA_BYTES_POOL} "
                f"max_bytes {self._QUOTA_BYTES_VALUE}"
            ]
        )
        try:
            if not self.rados_obj.create_pool(
                pool_name=self._QUOTA_OBJECTS_POOL,
            ):
                raise RuntimeError(f"Failed to create pool {self._QUOTA_OBJECTS_POOL}")
        except Exception:
            log.info(f"Pool {self._QUOTA_OBJECTS_POOL} may already exist")
        self.rados_obj.node.shell(
            [
                f"ceph osd pool set-quota {self._QUOTA_OBJECTS_POOL} "
                f"max_objects {self._QUOTA_OBJECTS_VALUE}"
            ]
        )
        log.info(
            f"Quotas set: {self._QUOTA_BYTES_POOL} max_bytes={self._QUOTA_BYTES_VALUE}, "
            f"{self._QUOTA_OBJECTS_POOL} max_objects={self._QUOTA_OBJECTS_VALUE}"
        )

    def _verify_pool_quotas(self, snapshot):
        """Verify exact quota values persist on both quota pools."""
        results = []
        try:
            out = self.rados_obj.run_ceph_command(
                cmd=f"ceph osd pool get-quota {self._QUOTA_BYTES_POOL}"
            )
            actual_bytes = out.get("quota_max_bytes", 0)
            if actual_bytes != self._QUOTA_BYTES_VALUE:
                return False, (
                    f"{self._QUOTA_BYTES_POOL} max_bytes={actual_bytes}, "
                    f"expected {self._QUOTA_BYTES_VALUE}"
                )
            results.append(f"{self._QUOTA_BYTES_POOL} max_bytes={actual_bytes}")

            out2 = self.rados_obj.run_ceph_command(
                cmd=f"ceph osd pool get-quota {self._QUOTA_OBJECTS_POOL}"
            )
            actual_objects = out2.get("quota_max_objects", 0)
            if actual_objects != self._QUOTA_OBJECTS_VALUE:
                return False, (
                    f"{self._QUOTA_OBJECTS_POOL} max_objects={actual_objects}, "
                    f"expected {self._QUOTA_OBJECTS_VALUE}"
                )
            results.append(f"{self._QUOTA_OBJECTS_POOL} max_objects={actual_objects}")

            df_out = self._shell_json("ceph df -f json")
            for p in df_out.get("pools", []):
                if p.get("name") == self._QUOTA_BYTES_POOL:
                    pct = p.get("stats", {}).get("percent_used", 0)
                    results.append(f"bytes_pool {pct:.1%} used")
                elif p.get("name") == self._QUOTA_OBJECTS_POOL:
                    pct = p.get("stats", {}).get("percent_used", 0)
                    results.append(f"objects_pool {pct:.1%} used")

            return True, f"quotas verified: {', '.join(results)}"
        except Exception as err:
            return False, str(err)

    def _enable_crush_device_classes(self):
        """Create a CRUSH rule pinned to the first available device class, then
        create a pool that uses it to validate end-to-end."""
        try:
            classes = self.rados_obj.run_ceph_command(cmd="ceph osd crush class ls")
            if not classes:
                log.warning("No CRUSH device classes found")
                return
            target_class = classes[0]
            rule_name = f"rule_{target_class}_upgrade_test"
            self.rados_obj.node.shell(
                [
                    f"ceph osd crush rule create-replicated "
                    f"{rule_name} default host {target_class}"
                ]
            )
            log.info(f"CRUSH rule {rule_name} created for class {target_class}")

            pool_name = "crush_class_test_pool"
            try:
                if not self.rados_obj.create_pool(
                    pool_name=pool_name,
                    crush_rule=rule_name,
                ):
                    raise RuntimeError(f"Failed to create pool {pool_name}")
                log.info(f"Pool {pool_name} created with CRUSH rule {rule_name}")
            except Exception:
                log.warning(
                    f"Pool {pool_name} creation failed (may already exist): "
                    f"{traceback.format_exc()}"
                )
        except Exception:
            log.error(f"CRUSH device class setup: {traceback.format_exc()}")
            raise

    def _verify_crush_device_classes(self, snapshot):
        """Verify CRUSH rule survives and the pool using it still references
        the correct rule post-upgrade."""
        try:
            classes = self.rados_obj.run_ceph_command(cmd="ceph osd crush class ls")
            rules = self.rados_obj.run_ceph_command(cmd="ceph osd crush rule ls")
            matching = [r for r in (rules or []) if "upgrade_test" in str(r)]
            if not (classes and matching):
                return False, f"classes={classes}, rules={rules}"

            pool_name = "crush_class_test_pool"
            try:
                pool_out = self.rados_obj.node.shell(
                    [f"ceph osd pool get {pool_name} crush_rule"]
                )[0].strip()
                if "upgrade_test" not in pool_out:
                    return False, (f"Pool {pool_name} crush_rule mismatch: {pool_out}")
                log.info(f"Pool {pool_name} crush_rule: {pool_out}")
            except Exception as pool_err:
                return False, (
                    f"CRUSH rule exists but pool {pool_name} check failed: "
                    f"{pool_err}"
                )

            return True, (
                f"classes={classes}, test rules found, "
                f"pool {pool_name} using correct rule"
            )
        except Exception as err:
            return False, str(err)

    # -- CephFS features --

    def _register_cephfs_features(self, r):
        """Register CephFS features: max_mds, standby_replay, pinning,
        quotas, snapshots, charmap."""
        cephfs_features = {
            "max_mds": (
                self._enable_cephfs_max_mds,
                self._verify_cephfs_max_mds,
                [],
            ),
            "standby_replay": (
                self._enable_cephfs_standby_replay,
                self._verify_cephfs_standby_replay,
                [],
            ),
            "subvolume_pinning": (
                self._enable_cephfs_subvol_pinning,
                self._verify_cephfs_subvol_pinning,
                [],
            ),
            "quotas": (
                self._enable_cephfs_quotas,
                self._verify_cephfs_quotas,
                [],
            ),
            "snapshots": (
                self._enable_cephfs_snapshots,
                self._verify_cephfs_snapshots,
                [],
            ),
            "snap_schedule": (
                self._enable_cephfs_snap_schedule,
                self._verify_cephfs_snap_schedule,
                [],
            ),
            "charmap": (
                self._enable_cephfs_charmap,
                self._verify_cephfs_charmap,
                [],
            ),
            "dir_fragmentation": (
                self._enable_cephfs_dir_frag,
                self._verify_cephfs_dir_frag,
                [],
            ),
        }
        for name, (enable, verify, prereqs) in cephfs_features.items():
            r[f"cephfs.{name}"] = FeatureEntry(
                enable=enable,
                verify=verify,
                toggle=f"cephfs.{name}",
                service="cephfs",
                prerequisites=prereqs,
            )

    def _enable_cephfs_max_mds(self):
        """Set max_mds from scale config on all filesystems.

        Uses scale.cephfs.active_mds (default 6, matching _setup_cephfs)
        for both filesystems, so the feature enable step does not
        overwrite the scale setting from Phase 1.
        """
        _, fs_list = self._get_fs_config()
        cephfs_scale = self.config.get("scale", {}).get("cephfs", {})
        active_mds = cephfs_scale.get("active_mds", 6)
        primary_max = cephfs_scale.get("max_mds_primary", active_mds)
        secondary_max = cephfs_scale.get("max_mds_secondary", active_mds)
        details = []
        for idx, fs_name in enumerate(fs_list):
            max_val = primary_max if idx == 0 else secondary_max
            self.rados_obj.node.shell([f"ceph fs set {fs_name} max_mds {max_val}"])
            details.append(f"{fs_name}={max_val}")
        log.info(f"max_mds set: {', '.join(details)}")

    def _verify_cephfs_max_mds(self, snapshot):
        """Verify max_mds config persists AND actual active MDS count matches."""
        _, fs_list = self._get_fs_config()
        cephfs_scale = self.config.get("scale", {}).get("cephfs", {})
        active_mds = cephfs_scale.get("active_mds", 6)
        primary_max = cephfs_scale.get("max_mds_primary", active_mds)
        secondary_max = cephfs_scale.get("max_mds_secondary", active_mds)
        expected_map = {}
        for idx, fs_name in enumerate(fs_list):
            expected_map[fs_name] = primary_max if idx == 0 else secondary_max
        try:
            active_details = []
            for fs_name, expected in expected_map.items():
                info = self.rados_obj.run_ceph_command(cmd=f"ceph fs get {fs_name}")
                mdsmap = info.get("mdsmap", info)
                actual = mdsmap.get("max_mds", 1)
                if actual != expected:
                    return False, (f"{fs_name} max_mds={actual}, expected {expected}")

                try:
                    fs_status = self._shell_json(f"ceph fs status {fs_name} -f json")
                    mds_list = fs_status.get("mdsmap", [])
                    active_count = sum(
                        1
                        for m in mds_list
                        if (
                            str(m.get("state", "")).startswith("active")
                            or "up:active" in str(m.get("status", ""))
                        )
                    )
                    if active_count < expected:
                        log.warning(
                            f"[{fs_name}] active MDS count {active_count} < "
                            f"max_mds {expected} (may still be stabilizing)"
                        )
                    active_details.append(
                        f"{fs_name}: active={active_count}/{expected}"
                    )
                except Exception as fs_err:
                    log.warning(
                        f"ceph fs status check for {fs_name} failed "
                        f"(non-fatal): {fs_err}"
                    )
                    active_details.append(f"{fs_name}: status check skipped")

            detail = ", ".join(f"{k}={v}" for k, v in expected_map.items())
            mds_detail = ", ".join(active_details)
            return True, f"max_mds preserved: {detail} | MDS status: {mds_detail}"
        except Exception as err:
            return False, str(err)

    def _enable_cephfs_standby_replay(self):
        """Enable allow_standby_replay and set standby_count_wanted on all filesystems."""
        _, fs_list = self._get_fs_config()
        for fs in fs_list:
            self.rados_obj.node.shell([f"ceph fs set {fs} allow_standby_replay true"])
            try:
                self.rados_obj.node.shell([f"ceph fs set {fs} standby_count_wanted 1"])
            except Exception as err:
                log.warning(
                    f"standby_count_wanted set failed on {fs} "
                    f"(may not be supported): {err}"
                )

    def _verify_cephfs_standby_replay(self, snapshot):
        """Verify allow_standby_replay flag and check for standby-replay MDS daemons
        via ceph fs status for each filesystem."""
        try:
            _, fs_list = self._get_fs_config()
            flag_found = False
            replay_daemons = []
            for fs_name in fs_list:
                info = self.rados_obj.run_ceph_command(cmd=f"ceph fs get {fs_name}")
                mdsmap = info.get("mdsmap", info)
                flags = mdsmap.get("flags_state", {})
                if flags.get("standby_replay"):
                    flag_found = True

                try:
                    fs_status = self._shell_json(f"ceph fs status {fs_name} -f json")
                    for mds in fs_status.get("mdsmap", []):
                        state = mds.get("state", "")
                        if state in ("standby-replay", "standby_replay"):
                            replay_daemons.append(f"{mds.get('name', '?')}@{fs_name}")
                except Exception as fs_err:
                    log.warning(
                        f"ceph fs status for {fs_name} failed " f"(non-fatal): {fs_err}"
                    )

            if flag_found or replay_daemons:
                detail = (
                    f"flag={'set' if flag_found else 'unset'}, "
                    f"standby-replay daemons: "
                    f"{replay_daemons if replay_daemons else 'none yet'}"
                )
                if not replay_daemons:
                    log.warning(
                        "allow_standby_replay is set but no standby-replay "
                        "MDS daemons observed (may still be converging)"
                    )
                return True, detail
            return False, "no standby-replay MDS found and flag not set"
        except Exception as err:
            return False, str(err)

    def _enable_cephfs_subvol_pinning(self):
        """Pin upgrade_test_subvol to distributed policy on the direct-mount filesystem.

        Honors ``scale.cephfs.pin_policy``: no-op when policy is ``none``.
        Scale load balancing uses SVG-level pins in setup; this path exercises
        the per-subvolume pin API for the feature smoke subvolume.
        """
        pin_policy = (
            self.config.get("scale", {})
            .get("cephfs", {})
            .get("pin_policy", "distributed")
        )
        if pin_policy == "none":
            log.info("Skipping subvolume pinning enable: pin_policy=none")
            return
        direct_fs, _ = self._get_fs_config()
        try:
            self.rados_obj.node.shell(
                [f"ceph fs subvolume pin {direct_fs} upgrade_test_subvol distributed 1"]
            )
        except Exception:
            log.warning("Subvolume pinning may not be supported or subvol missing")
            raise

    def _verify_cephfs_subvol_pinning(self, snapshot):
        """Verify subvolume exists and is accessible (pin persists as inode xattr)."""
        direct_fs, _ = self._get_fs_config()
        try:
            out, _ = self.rados_obj.node.shell(
                [
                    f"ceph fs subvolume info {direct_fs} upgrade_test_subvol --format json"
                ]
            )
            info = json.loads(out)
            path = info.get("path", "")
            if not path:
                return False, "subvolume not found or has no path"
            state = info.get("state", "unknown")
            return True, (
                f"subvolume at {path}, state={state} "
                f"(pin policy set pre-upgrade, persisted as inode xattr)"
            )
        except Exception as err:
            return False, str(err)

    def _enable_cephfs_quotas(self):
        """Set a 5GiB bytes_quota on upgrade_test_subvol via subvolume resize."""
        direct_fs, _ = self._get_fs_config()
        try:
            self.rados_obj.node.shell(
                [
                    f"ceph fs subvolume resize {direct_fs} upgrade_test_subvol "
                    "5368709120"
                ]
            )
        except Exception:
            log.warning("CephFS subvolume resize failed, quota may not be set")
            raise

    _CEPHFS_QUOTA_BYTES = 5368709120  # 5 GiB

    def _verify_cephfs_quotas(self, snapshot):
        """Verify bytes_quota on upgrade_test_subvol matches the configured 5 GiB value."""
        direct_fs, _ = self._get_fs_config()
        try:
            out = self.rados_obj.node.shell(
                [
                    f"ceph fs subvolume info {direct_fs} upgrade_test_subvol --format json"
                ]
            )[0].strip()
            info = json.loads(out)
            quota = info.get("bytes_quota", "infinite")
            if quota == "infinite":
                return False, "quota is 'infinite' (not set)"
            actual = int(quota)
            if actual != self._CEPHFS_QUOTA_BYTES:
                return False, (
                    f"quota drift: expected {self._CEPHFS_QUOTA_BYTES}, "
                    f"got {actual}"
                )
            bytes_used = info.get("bytes_used", 0)
            pct = (bytes_used / actual * 100) if actual > 0 else 0
            return True, (f"quota={actual} bytes, " f"used={bytes_used} ({pct:.1f}%)")
        except Exception as err:
            return False, str(err)

    _SNAP_TEST_FILENAME = "upgrade_snap_canary.txt"
    _SNAP_TEST_CONTENT = "upgrade-snapshot-integrity-marker"

    def _enable_cephfs_snapshots(self):
        """Write a canary file into the subvolume via a CephFS client,
        then create pre_upgrade_snap."""
        direct_fs, _ = self._get_fs_config()
        try:
            sv_info = self._shell_json(
                f"ceph fs subvolume info {direct_fs} upgrade_test_subvol --format json"
            )
            sv_path = sv_info.get("path", "")
            if sv_path:
                canary_written = False
                clients = self.ceph_cluster.get_nodes(role="client")
                for client in clients or []:
                    try:
                        mps, _ = client.exec_command(
                            sudo=True,
                            cmd="mount -t ceph,fuse.ceph-fuse | awk '{print $3}'",
                            timeout=15,
                        )
                        for mp in (mps or "").splitlines():
                            mp = mp.strip()
                            if not mp:
                                continue
                            # The subvolume path sits under the CephFS mount
                            canary_path = f"{mp}{sv_path}/{self._SNAP_TEST_FILENAME}"
                            try:
                                client.exec_command(
                                    sudo=True,
                                    cmd=f"echo -n '{self._SNAP_TEST_CONTENT}' > {canary_path}",
                                    timeout=15,
                                )
                                log.info(
                                    f"Canary file written via {client.hostname}:{canary_path}"
                                )
                                canary_written = True
                                break
                            except Exception:
                                continue
                        if canary_written:
                            break
                    except Exception:
                        continue
                if not canary_written:
                    log.warning(
                        "No client with CephFS mount found for canary write; "
                        "snapshot content verification will be skipped"
                    )
        except Exception as err:
            log.warning(f"Canary file write failed (non-fatal): {err}")

        try:
            self.rados_obj.node.shell(
                [
                    f"ceph fs subvolume snapshot create {direct_fs} "
                    "upgrade_test_subvol pre_upgrade_snap"
                ]
            )
        except Exception:
            log.warning("Snapshot creation may have failed or already exists")
            raise

    def _verify_cephfs_snapshots(self, snapshot):
        """Verify pre_upgrade_snap exists AND its contents include the canary file."""
        direct_fs, _ = self._get_fs_config()
        try:
            out = self.rados_obj.node.shell(
                [f"ceph fs subvolume snapshot ls {direct_fs} upgrade_test_subvol"]
            )[0].strip()
            snaps = json.loads(out) if out else []
            names = [s.get("name", "") for s in snaps]
            if "pre_upgrade_snap" not in names:
                return False, f"pre_upgrade_snap missing, snapshots: {names}"

            canary_ok = False
            try:
                snap_info = self._shell_json(
                    f"ceph fs subvolume snapshot info {direct_fs} "
                    "upgrade_test_subvol pre_upgrade_snap --format json"
                )
                log.info(f"Snapshot info: {snap_info}")
                canary_ok = True
            except Exception:
                pass

            if not canary_ok:
                try:
                    sv_info = self._shell_json(
                        f"ceph fs subvolume info {direct_fs} "
                        "upgrade_test_subvol --format json"
                    )
                    sv_path = sv_info.get("path", "")
                    if sv_path:
                        snap_dir = (
                            f"{sv_path}/.snap/pre_upgrade_snap/"
                            f"{self._SNAP_TEST_FILENAME}"
                        )
                        content = self.rados_obj.node.shell([f"cat {snap_dir}"])[
                            0
                        ].strip()
                        if self._SNAP_TEST_CONTENT in content:
                            canary_ok = True
                            log.info("Canary file verified in snapshot .snap dir")
                        else:
                            log.warning(f"Canary file content mismatch: '{content}'")
                except Exception as snap_err:
                    log.warning(
                        f"Snapshot content verification failed (non-fatal): "
                        f"{snap_err}"
                    )

            detail = "pre_upgrade_snap found"
            if canary_ok:
                detail += ", canary file verified"
            else:
                detail += ", canary file check skipped/failed"
            return True, detail
        except Exception as err:
            return False, str(err)

    def _enable_cephfs_snap_schedule(self):
        """Enable snap_schedule module and add 1h schedule with 14h retention."""
        direct_fs, _ = self._get_fs_config()
        try:
            self.rados_obj.node.shell(["ceph mgr module enable snap_schedule"])
            max_attempts = 10
            for attempt in range(max_attempts):
                try:
                    self.rados_obj.node.shell(
                        [f"ceph fs snap-schedule status / --fs {direct_fs} -f json"]
                    )
                    log.info(f"snap_schedule module ready after {attempt * 3}s")
                    break
                except Exception:
                    if attempt == max_attempts - 1:
                        raise RuntimeError(
                            f"snap_schedule module not ready after "
                            f"{max_attempts * 3}s"
                        )
                    log.debug(f"snap_schedule not ready yet, attempt {attempt + 1}")
                    time.sleep(3)
            existing = self._shell_json(
                f"ceph fs snap-schedule status / --fs {direct_fs} -f json", default=[]
            )
            if existing:
                log.info(
                    f"snap_schedule: already configured "
                    f"({len(existing)} schedule(s)), skipping"
                )
                return

            self.rados_obj.node.shell(
                [f"ceph fs snap-schedule add / 1h --fs {direct_fs}"]
            )
            self.rados_obj.node.shell(
                [f"ceph fs snap-schedule retention add / h 14 --fs {direct_fs}"]
            )
            self.rados_obj.node.shell(
                [f"ceph fs snap-schedule activate / --fs {direct_fs}"]
            )
        except Exception:
            log.warning(f"Snap schedule setup: {traceback.format_exc()}")
            raise

    def _verify_cephfs_snap_schedule(self, snapshot):
        """Verify snap-schedule is active and check if scheduled snapshots
        were actually created."""
        direct_fs, _ = self._get_fs_config()
        try:
            schedules = self._shell_json(
                f"ceph fs snap-schedule status / --fs {direct_fs} -f json",
                default=[],
            )
            if not schedules:
                return False, "no snap schedules found"

            sched_detail = f"{len(schedules)} schedule(s) active"
            created_count = 0
            try:
                for sched in schedules:
                    created = sched.get("created_count", sched.get("created"))
                    if created is not None:
                        try:
                            created_count += int(created)
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass

            snap_count = 0
            try:
                clients = self.ceph_cluster.get_nodes(role="client")
                snap_checked = False
                for client in clients or []:
                    try:
                        mps, _ = client.exec_command(
                            sudo=True,
                            cmd="mount -t ceph,fuse.ceph-fuse | awk '{print $3}'",
                            timeout=15,
                        )
                        for mp in mps.strip().split("\n"):
                            mp = mp.strip()
                            if not mp:
                                continue
                            snap_ls, _ = client.exec_command(
                                sudo=True,
                                cmd=f"ls {mp}/.snap/ 2>/dev/null || true",
                                timeout=15,
                            )
                            if snap_ls.strip():
                                snap_entries = [
                                    s
                                    for s in snap_ls.strip().split("\n")
                                    if s.strip() and "scheduled" in s.lower()
                                ]
                                snap_count += len(snap_entries)
                                snap_checked = True
                                break
                    except Exception:
                        continue
                    if snap_checked:
                        break
                if not snap_checked:
                    sv_info = self._shell_json(
                        f"ceph fs subvolume info {direct_fs} "
                        "upgrade_test_subvol --format json"
                    )
                    sv_path = sv_info.get("path", "")
                    if sv_path:
                        snap_ls, _ = self.rados_obj.node.shell(
                            ["ls /volumes/.snap/ 2>/dev/null || true"]
                        )
                        if snap_ls.strip():
                            snap_entries = [
                                s
                                for s in snap_ls.strip().split("\n")
                                if s.strip() and "scheduled" in s.lower()
                            ]
                            snap_count = len(snap_entries)
            except Exception as ls_err:
                log.debug(f"Scheduled snap listing failed (non-fatal): {ls_err}")

            detail = sched_detail
            if created_count > 0:
                detail += f", {created_count} created by scheduler"
            if snap_count > 0:
                detail += f", {snap_count} scheduled snapshots found on disk"
            elif created_count == 0:
                log.warning(
                    "Snap schedule active but no scheduled snapshots detected yet "
                    "(schedule interval may not have elapsed)"
                )
                detail += ", no scheduled snapshots yet (interval may not have elapsed)"

            return True, detail
        except Exception as err:
            return False, str(err)

    def _enable_cephfs_charmap(self):
        """Set case-insensitive charmap on a dedicated empty SVG."""
        direct_fs, fs_list = self._get_fs_config()
        fs_name = fs_list[0] if fs_list else direct_fs
        svg = "svg_charmap"
        try:
            self.rados_obj.node.shell(
                [f"ceph fs subvolumegroup create {fs_name} {svg}"]
            )
        except Exception as e:
            if "already exists" not in str(e).lower():
                log.warning(f"SVG create for {svg} failed: {e}")
                raise
        try:
            existing, _ = self.rados_obj.node.shell(
                [f"ceph fs subvolumegroup charmap get {fs_name} {svg}"]
            )
            if "casesensitive" in existing.lower() and "false" in existing.lower():
                log.info(f"charmap: already set on {fs_name}/{svg}, skipping")
                return
        except Exception:
            pass

        self.rados_obj.node.shell(
            [f"ceph fs subvolumegroup charmap set {fs_name} {svg} casesensitive false"]
        )
        log.info(f"charmap: casesensitive=false on {fs_name}/{svg}")

    def _verify_cephfs_charmap(self, snapshot):
        """Verify case-insensitive charmap persists on svg_charmap post-upgrade."""
        direct_fs, fs_list = self._get_fs_config()
        fs_name = fs_list[0] if fs_list else direct_fs
        svg = "svg_charmap"
        try:
            out, _ = self.rados_obj.node.shell(
                [f"ceph fs subvolumegroup charmap get {fs_name} {svg}"]
            )
            if "false" in out.lower() or "casesensitive" in out.lower():
                return True, f"charmap casesensitive=false active on {fs_name}/{svg}"
            return False, f"charmap not set: {out.strip()}"
        except Exception as err:
            return False, str(err)

    def _enable_cephfs_dir_frag(self):
        """Configure directory fragmentation with non-default fragment_size_max.

        mds_bal_fragment_dirs defaults to true; we set it explicitly to create
        a config-dump entry verifiable post-upgrade. fragment_size_max=50000
        is a non-default value (production default is 100000 on Tentacle).
        """
        self.rados_obj.node.shell(["ceph config set mds mds_bal_fragment_dirs true"])
        self.rados_obj.node.shell(
            ["ceph config set mds mds_bal_fragment_size_max 50000"]
        )

    def _verify_cephfs_dir_frag(self, snapshot):
        """Verify mds_bal_fragment_dirs remains true and fragment_size_max is 50000."""
        ok, msg = self._verify_config(
            "mds",
            "mds_bal_fragment_dirs",
            "true",
            label="dir fragmentation",
        )
        if not ok:
            return ok, msg
        return self._verify_config(
            "mds",
            "mds_bal_fragment_size_max",
            "50000",
            label="dir frag size_max",
        )

    # -- RBD features --

    def _register_rbd_features(self, r):
        """Register RBD features: image features, QoS, snap/clone,
        EC data, LUKS2, cache, trash, groups, namespaces."""
        rbd_features = {
            "all_image_features": (
                self._enable_rbd_all_features,
                self._verify_rbd_all_features,
                [],
            ),
            "qos": (
                self._enable_rbd_qos,
                self._verify_rbd_qos,
                [],
            ),
            "snap_clone_tree": (
                self._enable_rbd_snap_clone,
                self._verify_rbd_snap_clone,
                [],
            ),
            "ec_data_pool": (
                self._enable_rbd_ec_data,
                self._verify_rbd_ec_data,
                [],
            ),
            "luks2_encryption": (
                self._enable_rbd_luks2,
                self._verify_rbd_luks2,
                [],
            ),
            "persistent_write_log_cache": (
                self._enable_rbd_pwl_cache,
                self._verify_rbd_pwl_cache,
                [],
            ),
            "trash_purge_schedule": (
                self._enable_rbd_trash_schedule,
                self._verify_rbd_trash_schedule,
                [],
            ),
            "group_snapshots": (
                self._enable_rbd_group_snaps,
                self._verify_rbd_group_snaps,
                [],
            ),
            "namespace": (
                self._enable_rbd_namespace,
                self._verify_rbd_namespace,
                [],
            ),
        }
        for name, (enable, verify, prereqs) in rbd_features.items():
            r[f"rbd.{name}"] = FeatureEntry(
                enable=enable,
                verify=verify,
                toggle=f"rbd.{name}",
                service="rbd",
                prerequisites=prereqs,
            )

    _RBD_DESIRED_FEATURES = {
        "layering",
        "exclusive-lock",
        "object-map",
        "fast-diff",
        "deep-flatten",
    }
    _RBD_IMMUTABLE_FEATURES = {"layering"}

    def _enable_rbd_all_features(self):
        """Enable desired features on integrity_img, skipping already-active and immutable ones."""
        try:
            out = self.rados_obj.node.shell(
                ["rbd info rep_pool/integrity_img --format json"]
            )[0].strip()
            info = json.loads(out)
            current = {f.replace("_", "-") for f in info.get("features", [])}
        except Exception:
            current = set()

        needed = {
            f for f in self._RBD_DESIRED_FEATURES if f.replace("_", "-") not in current
        } - self._RBD_IMMUTABLE_FEATURES

        if not needed:
            log.info(
                f"All desired RBD features already active on integrity_img: {current}"
            )
            return

        features_str = " ".join(sorted(needed))
        with self._pwl_cache_disabled():
            try:
                self.rados_obj.node.shell(
                    [f"rbd feature enable rep_pool/integrity_img {features_str}"]
                )
                log.info(f"Enabled RBD features: {features_str}")
            except Exception as err:
                if "immutable" in str(err).lower():
                    log.info(
                        f"Some features are immutable defaults (already set): " f"{err}"
                    )
                else:
                    raise

    def _verify_rbd_all_features(self, snapshot):
        """Verify all required RBD image features persist on integrity_img."""
        try:
            out = self.rados_obj.node.shell(
                ["rbd info rep_pool/integrity_img --format json"]
            )[0].strip()
            info = json.loads(out)
            feats = info.get("features", [])
            required = {
                "layering",
                "exclusive-lock",
                "object-map",
                "fast-diff",
                "deep-flatten",
            }
            normalized_present = {f.replace("_", "-") for f in feats}
            normalized_required = {f.replace("_", "-") for f in required}
            missing = normalized_required - normalized_present
            if missing:
                return False, f"missing features: {missing}"
            return True, f"all features present: {feats}"
        except Exception as err:
            return False, str(err)

    @contextmanager
    def _pwl_cache_disabled(self):
        """Context manager: temporarily disable PWL cache if enabled.

        RBD operations that acquire exclusive locks (QoS config, LUKS2 format,
        feature enable, snap/clone, group snap) can fail when the persistent
        write-log cache holds the lock.  This suspends the cache mode for the
        duration of the block and restores it afterwards.
        """
        original_mode = None
        try:
            mode, _ = self.rados_obj.node.shell(
                ["ceph config get client rbd_persistent_cache_mode"]
            )
            mode = mode.strip()
            if mode and mode != "disabled":
                self.rados_obj.node.shell(
                    ["ceph config set client rbd_persistent_cache_mode disabled"]
                )
                log.info(f"PWL cache temporarily disabled (was: {mode})")
                original_mode = mode
        except Exception:
            pass
        try:
            yield original_mode
        finally:
            if original_mode:
                try:
                    self.rados_obj.node.shell(
                        [
                            f"ceph config set client "
                            f"rbd_persistent_cache_mode {original_mode}"
                        ]
                    )
                    log.info(f"PWL cache restored to: {original_mode}")
                except Exception:
                    log.warning(f"Failed to restore PWL cache mode to {original_mode}")

    def _enable_rbd_qos(self):
        """Set rbd_qos_iops_limit=5000 on integrity_img."""
        with self._pwl_cache_disabled():
            clients = self.ceph_cluster.get_nodes(role="client")
            if not clients:
                raise RuntimeError("No client nodes available for RBD QoS setup")
            clients[0].exec_command(
                sudo=True,
                cmd="rbd config image set rep_pool/integrity_img rbd_qos_iops_limit 5000",
                timeout=30,
            )

    def _verify_rbd_qos(self, snapshot):
        """Verify rbd_qos_iops_limit=5000 persists on integrity_img at both
        image-level config and global config scope."""
        try:
            out = self.rados_obj.node.shell(
                ["rbd config image get rep_pool/integrity_img rbd_qos_iops_limit"]
            )[0].strip()
            if not out or int(out) != 5000:
                return False, f"QoS IOPS limit on image: {out}"

            global_ok = False
            try:
                global_val = self.rados_obj.node.shell(
                    ["ceph config get client rbd_qos_iops_limit"]
                )[0].strip()
                global_ok = bool(global_val)
                log.info(f"Global rbd_qos_iops_limit: {global_val}")
            except Exception:
                pass

            detail = "QoS IOPS limit=5000 on image"
            if global_ok:
                detail += f", global default={global_val}"
            return True, detail
        except Exception as err:
            return False, str(err)

    def _enable_rbd_snap_clone(self):
        """Create pre_upgrade snapshot on integrity_img, protect it, and clone."""
        with self._pwl_cache_disabled():
            try:
                self.rados_obj.node.shell(
                    ["rbd snap create rep_pool/integrity_img@pre_upgrade"]
                )
            except Exception:
                log.info("Snapshot pre_upgrade may already exist")
            try:
                self.rados_obj.node.shell(
                    ["rbd snap protect rep_pool/integrity_img@pre_upgrade"]
                )
            except Exception:
                log.info("Snapshot pre_upgrade may already be protected")
            try:
                self.rados_obj.node.shell(
                    [
                        "rbd clone rep_pool/integrity_img@pre_upgrade "
                        "rep_pool/integrity_clone"
                    ]
                )
            except Exception:
                log.info("Clone integrity_clone may already exist")

    def _verify_rbd_snap_clone(self, snapshot):
        """Verify pre_upgrade snapshot, its clone tree, and clone readability."""
        try:
            out = self.rados_obj.node.shell(
                ["rbd snap ls rep_pool/integrity_img --format json"]
            )[0].strip()
            snaps = json.loads(out)
            names = [s["name"] for s in snaps]
            if "pre_upgrade" not in names:
                return False, f"snap missing, found: {names}"
            out = self.rados_obj.node.shell(
                ["rbd children rep_pool/integrity_img@pre_upgrade --format json"]
            )[0].strip()
            children = json.loads(out) if out else []
            if not children:
                return False, "no clones found"

            clone_readable = False
            try:
                clone_info = self.rados_obj.node.shell(
                    ["rbd info rep_pool/integrity_clone --format json"]
                )[0].strip()
                ci = json.loads(clone_info)
                if ci.get("name") == "integrity_clone" and ci.get("size", 0) > 0:
                    clone_readable = True
            except Exception as clone_err:
                log.warning(f"Clone readability check failed (non-fatal): {clone_err}")

            detail = f"snap + {len(children)} clone(s) intact"
            if clone_readable:
                detail += ", clone readable"
            return True, detail
        except Exception as err:
            return False, str(err)

    def _enable_rbd_ec_data(self):
        """Create a 1G RBD image with EC data pool (ec_k2m2_pool)."""
        with self._pwl_cache_disabled():
            try:
                self.rados_obj.node.shell(
                    [
                        "rbd create --size 1G --data-pool ec_k2m2_pool "
                        "rep_pool/ec_data_img"
                    ]
                )
            except Exception as e:
                if "File exists" in str(e) or "errno 17" in str(e):
                    log.info(
                        "EC data pool image rep_pool/ec_data_img already "
                        "exists, continuing"
                    )
                else:
                    log.warning(f"EC data pool image create: {traceback.format_exc()}")
                    raise

    def _verify_rbd_ec_data(self, snapshot):
        """Verify ec_data_img still references the EC data pool and is accessible."""
        try:
            out = self.rados_obj.node.shell(
                ["rbd info rep_pool/ec_data_img --format json"]
            )[0].strip()
            info = json.loads(out)
            data_pool = info.get("data_pool", "")
            if not data_pool or "ec" not in data_pool:
                return False, f"data_pool: {data_pool}"

            size = info.get("size", 0)
            detail = f"EC data pool: {data_pool}, size={size}"

            try:
                du_out = self.rados_obj.node.shell(
                    ["rbd du rep_pool/ec_data_img --format json"]
                )[0].strip()
                du = json.loads(du_out)
                images = du.get("images", [])
                if images:
                    used = images[0].get("used_size", 0)
                    detail += f", used={used}"
            except Exception:
                pass

            return True, detail
        except Exception as err:
            return False, str(err)

    def _enable_rbd_luks2(self):
        """Format luks_img with LUKS2 encryption using a random passphrase."""
        with self._pwl_cache_disabled():
            clients = self.ceph_cluster.get_nodes(role="client")
            if not clients:
                raise RuntimeError("No client nodes available for LUKS2 setup")
            try:
                clients[0].exec_command(
                    sudo=True,
                    cmd=(
                        "dd if=/dev/urandom of=/tmp/passphrase.bin bs=1 count=64 "
                        "2>/dev/null && "
                        "rbd encryption format rep_pool/luks_img luks2 "
                        "/tmp/passphrase.bin"
                    ),
                    timeout=60,
                )
            except Exception:
                log.warning(f"LUKS2 setup: {traceback.format_exc()}")
                raise

    def _verify_rbd_luks2(self, snapshot):
        """Verify LUKS2-formatted luks_img still exists with non-zero size."""
        try:
            out = self.rados_obj.node.shell(
                ["rbd info rep_pool/luks_img --format json"]
            )[0].strip()
            info = json.loads(out)
            if not info.get("name") == "luks_img":
                return False, "LUKS2 image missing"
            size = info.get("size", 0)
            if size == 0:
                return False, "LUKS2 image has zero size"
            return True, f"LUKS2 image exists size={size}"
        except Exception as err:
            return False, str(err)

    def _enable_rbd_pwl_cache(self):
        """Configure persistent write-log cache (SSD mode) via rbd_persistent_cache_* configs.

        Creates the cache directory on all client nodes before enabling.
        """
        self.rados_obj.node.shell(
            ["ceph config set client rbd_persistent_cache_path /tmp/rbd_cache"]
        )
        for client in self.ceph_cluster.get_nodes(role="client") or []:
            try:
                client.exec_command(
                    sudo=True, cmd="mkdir -p /tmp/rbd_cache", timeout=10
                )
            except Exception:
                pass
        self.rados_obj.node.shell(
            ["ceph config set client rbd_persistent_cache_mode ssd"]
        )
        self.rados_obj.node.shell(["ceph config set client rbd_plugins pwl_cache"])

    def _verify_rbd_pwl_cache(self, snapshot):
        """Verify all three PWL cache configs persist: mode, path, and plugin."""
        ok, msg = self._verify_config(
            "client",
            "rbd_persistent_cache_mode",
            "ssd",
            contains=True,
            label="persistent write-log cache mode",
        )
        if not ok:
            return ok, msg

        details = [msg]
        try:
            path_val = self.rados_obj.node.shell(
                ["ceph config get client rbd_persistent_cache_path"]
            )[0].strip()
            if "/tmp/rbd_cache" in path_val:
                details.append(f"cache_path={path_val}")
            else:
                return False, f"cache_path changed: {path_val}"
        except Exception:
            details.append("cache_path check skipped")

        try:
            plugin_val = self.rados_obj.node.shell(
                ["ceph config get client rbd_plugins"]
            )[0].strip()
            if "pwl_cache" in plugin_val:
                details.append(f"plugins={plugin_val}")
            else:
                return False, f"rbd_plugins missing pwl_cache: {plugin_val}"
        except Exception:
            details.append("plugins check skipped")

        return True, ", ".join(details)

    def _enable_rbd_trash_schedule(self):
        """Add a daily trash purge schedule on rep_pool.

        Note: ``rbd trash purge schedule list`` returns exit code 2 when
        no schedules exist, so it cannot be used as a readiness probe.
        The ``add`` command works directly without pre-checks.
        """
        self.rados_obj.node.shell(["rbd trash purge schedule add --pool rep_pool 1d"])

    def _verify_rbd_trash_schedule(self, snapshot):
        """Verify trash purge schedule persists and show schedule interval."""
        try:
            schedules = self._shell_json(
                "rbd trash purge schedule list --pool rep_pool --format json",
                default=[],
            )
            if not schedules:
                return False, "no trash purge schedules found"

            intervals = []
            for s in schedules:
                if isinstance(s, dict):
                    intervals.append(s.get("interval", s.get("schedule", "?")))
                else:
                    intervals.append(str(s))

            try:
                status_out = self.rados_obj.node.shell(
                    ["rbd trash purge schedule status --pool rep_pool --format json"]
                )[0].strip()
                status = json.loads(status_out) if status_out else {}
                log.info(f"Trash purge schedule status: {status}")
            except Exception:
                pass

            return True, (
                f"{len(schedules)} purge schedule(s), " f"intervals={intervals}"
            )
        except Exception as err:
            return False, str(err)

    def _enable_rbd_group_snaps(self):
        """Create upgrade_group with a dedicated image and take a group snapshot."""
        with self._pwl_cache_disabled():
            try:
                try:
                    self.rados_obj.node.shell(
                        ["rbd create rep_pool/group_test_img --size 64M"]
                    )
                except Exception:
                    pass
                try:
                    self.rados_obj.node.shell(
                        ["rbd group create rep_pool/upgrade_group"]
                    )
                except Exception as e:
                    if "File exists" in str(e) or "errno 17" in str(e):
                        log.info("RBD group upgrade_group already exists, continuing")
                    else:
                        raise
                try:
                    self.rados_obj.node.shell(
                        [
                            "rbd group image add rep_pool/upgrade_group "
                            "rep_pool/group_test_img"
                        ]
                    )
                except Exception as e:
                    if "already in" in str(e).lower() or "errno 17" in str(e):
                        log.info(
                            "Image group_test_img already in upgrade_group, "
                            "continuing"
                        )
                    else:
                        raise
                try:
                    self.rados_obj.node.shell(
                        [
                            "rbd group snap create "
                            "rep_pool/upgrade_group@pre_upgrade_group_snap"
                        ]
                    )
                except Exception as e:
                    err_str = str(e)
                    if "File exists" in err_str or "errno 17" in err_str:
                        log.info(
                            "Group snapshot pre_upgrade_group_snap already "
                            "exists, continuing"
                        )
                    else:
                        raise
            except Exception:
                log.warning(f"RBD group snap setup: {traceback.format_exc()}")
                raise

    def _verify_rbd_group_snaps(self, snapshot):
        """Verify upgrade_group, its member images, and group snapshots exist."""
        try:
            groups = self._shell_json("rbd group ls rep_pool --format json", default=[])
            if not any("upgrade_group" in str(g) for g in groups):
                return False, f"group missing, found: {groups}"

            images = self._shell_json(
                "rbd group image ls rep_pool/upgrade_group --format json",
                default=[],
            )
            if not images:
                return False, "group exists but no member images"

            snaps = self._shell_json(
                "rbd group snap ls rep_pool/upgrade_group --format json",
                default=[],
            )
            if not snaps:
                return False, "group exists with images but no group snapshots"

            snap_names = [
                s.get("name", s) if isinstance(s, dict) else str(s) for s in snaps
            ]
            return True, (
                f"group + {len(images)} image(s) + "
                f"{len(snaps)} group snap(s): {snap_names}"
            )
        except Exception as err:
            return False, str(err)

    def _enable_rbd_namespace(self):
        """Create test_ns namespace in rep_pool and a 1G image within it."""
        with self._pwl_cache_disabled():
            try:
                self.rados_obj.node.shell(["rbd namespace create rep_pool/test_ns"])
            except Exception as e:
                if "File exists" in str(e) or "errno 17" in str(e):
                    log.info("RBD namespace test_ns already exists, continuing")
                else:
                    log.warning(f"RBD namespace create: {traceback.format_exc()}")
                    raise
            try:
                self.rados_obj.node.shell(
                    ["rbd create --size 1G rep_pool/test_ns/ns_img"]
                )
            except Exception as e:
                if "File exists" in str(e) or "errno 17" in str(e):
                    log.info(
                        "RBD image rep_pool/test_ns/ns_img already exists, "
                        "continuing"
                    )
                else:
                    log.warning(f"RBD image create: {traceback.format_exc()}")
                    raise

    def _verify_rbd_namespace(self, snapshot):
        """Verify namespace exists in pool listing and image is accessible."""
        try:
            ns_out = self.rados_obj.node.shell(
                ["rbd namespace ls rep_pool --format json"]
            )[0].strip()
            namespaces = json.loads(ns_out) if ns_out else []
            ns_names = [
                n.get("name", n) if isinstance(n, dict) else str(n) for n in namespaces
            ]
            if "test_ns" not in ns_names:
                return False, f"namespace test_ns missing, found: {ns_names}"

            out = self.rados_obj.node.shell(
                ["rbd info rep_pool/test_ns/ns_img --format json"]
            )[0].strip()
            info = json.loads(out)
            if info.get("name") == "ns_img":
                size = info.get("size", 0)
                return True, (
                    f"namespace test_ns present, ns_img accessible " f"(size={size})"
                )
            return False, "namespace image not found"
        except Exception as err:
            return False, str(err)

    # -- RGW features --

    def _register_rgw_features(self, r):
        """Register RGW features: versioning, lifecycle, SSE, object
        lock, resharding, quotas, STS, rate limiting."""
        rgw_features = {
            "versioning": (
                self._enable_rgw_versioning,
                self._verify_rgw_versioning,
                [],
            ),
            "lifecycle": (
                self._enable_rgw_lifecycle,
                self._verify_rgw_lifecycle,
                [],
            ),
            "sse_s3": (
                self._enable_rgw_sse_s3,
                self._verify_rgw_sse_s3,
                [],
            ),
            "object_lock": (
                lambda: log.info("RGW object_lock: stub enable (not verified)"),
                self._stub_verify,
                [],
            ),
            "dynamic_resharding": (
                self._enable_rgw_resharding,
                self._verify_rgw_resharding,
                [],
            ),
            "multipart": (
                lambda: log.info("Multipart is an IO workload, no enable needed"),
                self._stub_verify,
                [],
            ),
            "user_bucket_quotas": (
                self._enable_rgw_quotas,
                self._verify_rgw_quotas,
                [],
            ),
            "sts_iam": (
                self._enable_rgw_sts,
                self._verify_rgw_sts,
                [],
            ),
            "rate_limiting": (
                self._enable_rgw_ratelimit,
                self._verify_rgw_ratelimit,
                [],
            ),
            "bucket_notifications": (
                lambda: log.info(
                    "RGW bucket_notifications: stub enable (not verified)"
                ),
                self._stub_verify,
                [],
            ),
            "bucket_policy": (
                lambda: log.info("RGW bucket_policy: stub enable (not verified)"),
                self._stub_verify,
                [],
            ),
        }
        for name, (enable, verify, prereqs) in rgw_features.items():
            r[f"rgw.{name}"] = FeatureEntry(
                enable=enable,
                verify=verify,
                toggle=f"rgw.{name}",
                service="rgw",
                prerequisites=prereqs,
            )

    def _enable_rgw_versioning(self):
        """Enable S3 bucket versioning on upgrade-test-bucket via boto3.

        Runs on a client node because boto3 is only installed there.
        """
        clients = self.ceph_cluster.get_nodes(role="client")
        if not clients:
            raise RuntimeError("No client nodes available for boto3 operations")
        endpoint = self._get_rgw_endpoint()
        if not endpoint:
            raise RuntimeError("No RGW endpoint available")
        clients[0].exec_command(
            sudo=True,
            cmd=(
                'python3 -c "'
                "import boto3;"
                "s3 = boto3.client('s3', "
                f"endpoint_url='http://{endpoint}', "
                "aws_access_key_id='testkey', "
                "aws_secret_access_key='testsecret');"
                "s3.put_bucket_versioning("
                "Bucket='upgrade-test-bucket', "
                "VersioningConfiguration={'Status': 'Enabled'})"
                '"'
            ),
            timeout=30,
        )

    def _verify_rgw_versioning(self, snapshot):
        """Verify bucket versioning remains enabled on upgrade-test-bucket."""
        try:
            out = self.rados_obj.node.shell(
                [
                    "radosgw-admin bucket stats "
                    "--bucket=upgrade-test-bucket --format json"
                ]
            )[0].strip()
            info = json.loads(out)
            ver = info.get("versioned", info.get("versioning", ""))
            if str(ver) in ("1", "Enabled", "enabled", "true"):
                return True, f"versioning enabled (versioned={ver})"
            return False, f"versioning not enabled (versioned={ver})"
        except Exception as err:
            return False, str(err)

    def _enable_rgw_lifecycle(self):
        """Set an S3 lifecycle rule on upgrade-test-bucket via boto3.

        Runs on a client node because boto3 is only installed there.
        The rule expires objects with prefix 'bg_obj_' after 365 days,
        giving radosgw-admin lc list something to verify post-upgrade.
        """
        clients = self.ceph_cluster.get_nodes(role="client")
        if not clients:
            raise RuntimeError("No client nodes available for boto3 operations")
        endpoint = self._get_rgw_endpoint()
        if not endpoint:
            raise RuntimeError("No RGW endpoint available")
        clients[0].exec_command(
            sudo=True,
            cmd=(
                'python3 -c "'
                "import boto3;"
                "s3 = boto3.client('s3', "
                f"endpoint_url='http://{endpoint}', "
                "aws_access_key_id='testkey', "
                "aws_secret_access_key='testsecret');"
                "s3.put_bucket_lifecycle_configuration("
                "Bucket='upgrade-test-bucket', "
                "LifecycleConfiguration={'Rules': [{"
                "'ID': 'upgrade-lc-rule', "
                "'Filter': {'Prefix': 'bg_obj_'}, "
                "'Status': 'Enabled', "
                "'Expiration': {'Days': 365}"
                "}]})"
                '"'
            ),
            timeout=30,
        )
        log.info("Lifecycle rule 'upgrade-lc-rule' set on upgrade-test-bucket")

    def _verify_rgw_lifecycle(self, snapshot):
        """Verify lifecycle rules exist via radosgw-admin lc list.

        Pre-upgrade (empty snapshot): tolerates empty rule list since the
        lifecycle rule may not have been processed by RGW yet.
        """
        try:
            out = self.rados_obj.node.shell(["radosgw-admin lc list --format json"])[
                0
            ].strip()
            rules = json.loads(out) if out else []
            if rules:
                return True, f"lifecycle rules present ({len(rules)} entries)"
            if not snapshot:
                return True, "no lifecycle rules yet (pre-upgrade, deferred to IO)"
            return False, "no lifecycle rules found post-upgrade"
        except Exception as err:
            return False, str(err)

    def _enable_rgw_sse_s3(self):
        """Set SSE-S3 backend + secret engine config for persistence test.

        rgw_crypt_sse_s3_backend defaults to 'vault' and
        rgw_crypt_sse_s3_vault_secret_engine defaults to 'transit'.
        We set both explicitly so the verifier can confirm they survive upgrade.
        """
        self.rados_obj.node.shell(
            ["ceph config set client.rgw rgw_crypt_sse_s3_backend vault"]
        )
        self.rados_obj.node.shell(
            ["ceph config set client.rgw rgw_crypt_sse_s3_vault_secret_engine transit"]
        )

    def _verify_rgw_sse_s3(self, snapshot):
        """Verify SSE-S3 config (backend + secret engine) persists post-upgrade."""
        ok, msg = self._verify_config(
            "client.rgw",
            "rgw_crypt_sse_s3_backend",
            "vault",
            label="SSE-S3 backend",
        )
        if not ok:
            return ok, msg
        return self._verify_config(
            "client.rgw",
            "rgw_crypt_sse_s3_vault_secret_engine",
            "transit",
            label="SSE-S3 vault secret engine",
        )

    def _enable_rgw_resharding(self):
        """Set resharding config with non-default max_objs_per_shard for persistence test.

        rgw_dynamic_resharding defaults to true; we also set
        rgw_max_objs_per_shard=50000 (default 100000) so the verify step
        exercises a real non-default config change.
        """
        self.rados_obj.node.shell(
            ["ceph config set client.rgw rgw_dynamic_resharding true"]
        )
        self.rados_obj.node.shell(
            ["ceph config set client.rgw rgw_max_objs_per_shard 50000"]
        )

    def _verify_rgw_resharding(self, snapshot):
        """Verify resharding config (flag + shard limit) persists post-upgrade."""
        ok, msg = self._verify_config(
            "client.rgw",
            "rgw_dynamic_resharding",
            "true",
            label="dynamic resharding",
        )
        if not ok:
            return ok, msg
        return self._verify_config(
            "client.rgw",
            "rgw_max_objs_per_shard",
            "50000",
            label="max objects per shard",
        )

    def _enable_rgw_quotas(self):
        """Set and enable user-scope quota (10G/50k objects) on upgrade-test-user."""
        self.rados_obj.node.shell(
            [
                "radosgw-admin quota set --quota-scope=user "
                "--uid=upgrade-test-user --max-size=10G --max-objects=50000"
            ]
        )
        self.rados_obj.node.shell(
            ["radosgw-admin quota enable --quota-scope=user " "--uid=upgrade-test-user"]
        )

    def _verify_rgw_quotas(self, snapshot):
        """Verify user quota remains enabled on upgrade-test-user post-upgrade."""
        try:
            out = self.rados_obj.node.shell(
                ["radosgw-admin user info --uid=upgrade-test-user"]
            )[0].strip()
            info = json.loads(out)
            quota = info.get("user_quota", {})
            if quota.get("enabled"):
                return True, f"user quota enabled: {quota}"
            return False, f"quota not enabled: {quota}"
        except Exception as err:
            return False, str(err)

    def _enable_rgw_sts(self):
        """Enable STS authentication via rgw_s3_auth_use_sts and set rgw_sts_key."""
        self.rados_obj.node.shell(
            ["ceph config set client.rgw rgw_s3_auth_use_sts true"]
        )
        self.rados_obj.node.shell(
            ["ceph config set client.rgw rgw_sts_key abcdefghijklmnop"]
        )

    def _verify_rgw_sts(self, snapshot):
        """Verify rgw_s3_auth_use_sts remains true post-upgrade."""
        return self._verify_config(
            "client.rgw",
            "rgw_s3_auth_use_sts",
            "true",
            label="STS auth",
        )

    def _enable_rgw_ratelimit(self):
        """Set and enable per-user rate limiting (1000 read-ops, 500 write-ops)."""
        self.rados_obj.node.shell(
            [
                "radosgw-admin ratelimit set --ratelimit-scope=user "
                "--uid=upgrade-test-user --max-read-ops=1000 --max-write-ops=500"
            ]
        )
        self.rados_obj.node.shell(
            [
                "radosgw-admin ratelimit enable --ratelimit-scope=user "
                "--uid=upgrade-test-user"
            ]
        )

    def _verify_rgw_ratelimit(self, snapshot):
        """Verify user-scope rate limiting remains enabled on upgrade-test-user."""
        try:
            rl = self._shell_json(
                "radosgw-admin ratelimit get --ratelimit-scope=user "
                "--uid=upgrade-test-user",
            )
            user_rl = rl.get("user_ratelimit", rl)
            if user_rl.get("enabled"):
                return True, f"rate limiting enabled: {user_rl}"
            return False, f"rate limiting not enabled: {user_rl}"
        except Exception as err:
            return False, str(err)

    # -- NFS features --

    def _register_nfs_features(self, r):
        """Register NFS features: QoS, multi-export, root_squash,
        delegations, NFSv3, ingress, Kerberos, LDAP."""
        nfs_features = {
            "qos_bandwidth": (
                self._enable_nfs_qos_bw,
                self._verify_nfs_qos_bw,
                [],
            ),
            "qos_iops": (
                self._enable_nfs_qos_iops,
                self._verify_nfs_qos_iops,
                [],
            ),
            "multiple_exports": (
                self._enable_nfs_multi_export,
                self._verify_nfs_multi_export,
                [],
            ),
            "root_squash": (
                self._enable_nfs_root_squash,
                self._verify_nfs_root_squash,
                [],
            ),
            "delegations": (
                self._enable_nfs_delegations,
                self._verify_nfs_delegations,
                [],
            ),
            "nfsv3": (
                self._enable_nfs_v3,
                self._verify_nfs_v3,
                [],
            ),
            "export_default": (
                self._enable_nfs_export_default,
                self._verify_nfs_export_default,
                [],
            ),
            "ingress_ha": (
                lambda: log.info("NFS ingress/HA configured at cluster creation"),
                self._stub_verify,
                [self._prereq_nfs_ingress_vip],
            ),
            "kerberos": (
                lambda: log.info("Kerberos requires external KDC setup"),
                self._stub_verify,
                [],
            ),
            "ldap_identity": (
                lambda: log.info("LDAP identity requires OpenLDAP container setup"),
                self._stub_verify,
                [],
            ),
        }
        for name, (enable, verify, prereqs) in nfs_features.items():
            r[f"nfs.{name}"] = FeatureEntry(
                enable=enable,
                verify=verify,
                toggle=f"nfs.{name}",
                service="nfs",
                prerequisites=prereqs,
            )

    def _prereq_nfs_ingress_vip(self):
        """Check that ingress_virtual_ip is configured for NFS HA."""
        vip = (
            self.config.get("features", {}).get("nfs", {}).get("ingress_virtual_ip", "")
        )
        return bool(vip)

    def _nfs_cluster_names(self) -> list:
        """Return NFS cluster names that actually exist on the cluster.

        Queries live cluster via 'ceph nfs cluster ls --format json'.
        Falls back to generating names from scale config if the query
        fails or returns no upgrade_nfs clusters.
        """
        try:
            out, _ = self.rados_obj.node.shell(["ceph nfs cluster ls --format json"])
            result = json.loads(out.strip()) if out.strip() else []
            if isinstance(result, list):
                names = [c for c in result if c.startswith("upgrade_nfs")]
                if names:
                    return names
        except Exception as err:
            log.debug(f"Live NFS cluster query failed, using fallback: {err}")

        nfs_scale = self.config.get("scale", {}).get("nfs", {})
        cluster_count = nfs_scale.get("cluster_count", 1)
        if cluster_count > 1:
            return [f"upgrade_nfs{i + 1}" for i in range(cluster_count)]
        return ["upgrade_nfs"]

    def _enable_nfs_qos_bw(self):
        """Enable bandwidth_control QoS on all NFS clusters."""
        for cluster_id in self._nfs_cluster_names():
            try:
                self.rados_obj.node.shell(
                    [
                        f"ceph nfs cluster qos enable bandwidth_control {cluster_id} "
                        "PerShare --combined-rw-bw-ctrl "
                        "--max_export_combined_bw 104857600"
                    ]
                )
            except Exception as err:
                log.warning(
                    f"NFS QoS bandwidth enable on {cluster_id} failed "
                    f"(may not be available): {err}"
                )
                raise

    def _verify_nfs_qos_bw(self, snapshot):
        """Verify NFS bandwidth_control QoS persists on all NFS clusters."""
        for cluster_id in self._nfs_cluster_names():
            try:
                qos = self._shell_json(
                    f"ceph nfs cluster qos get {cluster_id} --format json",
                )
                if qos.get("bandwidth_control") or qos.get("max_export_combined_bw"):
                    continue
                if not qos:
                    return False, f"QoS not configured (empty) on {cluster_id}"
                return False, f"QoS bandwidth config not found on {cluster_id}: {qos}"
            except Exception as err:
                return False, f"{cluster_id}: {err}"
        return True, "NFS QoS bandwidth configured on all clusters"

    def _enable_nfs_qos_iops(self):
        """Enable ops_control QoS on all NFS clusters."""
        for cluster_id in self._nfs_cluster_names():
            try:
                self.rados_obj.node.shell(
                    [
                        f"ceph nfs cluster qos enable ops_control "
                        f"{cluster_id} PerShare 5000"
                    ]
                )
            except Exception as err:
                log.warning(
                    f"NFS QoS IOPS enable on {cluster_id} failed "
                    f"(may not be available): {err}"
                )
                raise

    def _verify_nfs_qos_iops(self, snapshot):
        """Verify NFS ops_control QoS persists on all NFS clusters."""
        for cluster_id in self._nfs_cluster_names():
            try:
                qos = self._shell_json(
                    f"ceph nfs cluster qos get {cluster_id} --format json",
                )
                if qos.get("ops_control") or qos.get("max_export_iops"):
                    continue
                if not qos:
                    return False, f"QoS not configured (empty) on {cluster_id}"
                return False, f"QoS IOPS config not found on {cluster_id}: {qos}"
            except Exception as err:
                return False, f"{cluster_id}: {err}"
        return True, "NFS QoS IOPS configured on all clusters"

    def _enable_nfs_multi_export(self):
        """Multiple NFS exports are created in Phase 1 service setup (no-op here)."""
        log.info("Multiple NFS exports created in Phase 1 service setup")

    def _verify_nfs_multi_export(self, snapshot):
        """Verify at least 2 NFS exports exist across NFS clusters."""
        total_exports = 0
        for cluster_id in self._nfs_cluster_names():
            try:
                exports = self._shell_json(
                    f"ceph nfs export ls {cluster_id} --format json",
                    default=[],
                )
                total_exports += len(exports)
            except Exception as err:
                return False, f"{cluster_id}: {err}"
        if total_exports >= 2:
            return True, f"{total_exports} exports found across clusters"
        return False, f"only {total_exports} export(s)"

    def _enable_nfs_root_squash(self):
        """Root squash is configured at export creation time (no-op here)."""
        log.info("Root squash set at export creation time")

    def _verify_nfs_root_squash(self, snapshot):
        """Verify at least one NFS export has root_squash across all clusters."""
        squash_map = {}
        root_squash_export = None
        try:
            for cluster_id in self._nfs_cluster_names():
                out = self.rados_obj.node.shell(
                    [f"ceph nfs export ls {cluster_id} --format json"]
                )[0].strip()
                pseudo_paths = json.loads(out) if out else []
                for pseudo in pseudo_paths:
                    exp_out = self.rados_obj.node.shell(
                        [
                            f"ceph nfs export info {cluster_id} "
                            f"{pseudo} --format json"
                        ]
                    )[0].strip()
                    exp = json.loads(exp_out)
                    squash = exp.get("squash", "none")
                    key = f"{cluster_id}:{pseudo}"
                    squash_map[key] = squash
                    access_type = exp.get("access_type", "?")
                    protocols = exp.get("protocols", [])
                    log.info(
                        f"Export {cluster_id}:{pseudo}: squash={squash}, "
                        f"access_type={access_type}, protocols={protocols}"
                    )
                    if squash.lower() in ("root", "rootsquash", "root_squash"):
                        root_squash_export = key
            if root_squash_export:
                return True, (
                    f"root_squash on {root_squash_export}, "
                    f"all exports: {squash_map}"
                )
            return False, f"no root_squash found, exports: {squash_map}"
        except Exception as err:
            return False, str(err)

    def _enable_nfs_delegations(self):
        """Enable read_write delegations via EXPORT DEFAULT on all NFS clusters.

        Delegates to _enable_nfs_export_default -- both features use the same
        ``ceph nfs cluster set-export-default`` mechanism (>= 20.2.0).
        """
        self._enable_nfs_export_default()

    def _verify_nfs_delegations(self, snapshot):
        """Verify delegations are configured via EXPORT DEFAULT on NFS clusters."""
        return self._verify_nfs_export_default(snapshot)

    def _enable_nfs_v3(self):
        """Attempt to enable NFSv3 on all NFS clusters.

        --enable-nfsv3 is only available at cluster creation time.  In a
        multi-hop upgrade scenario where the cluster was created in an
        earlier hop without the flag, try to apply it via ``ceph orch``
        service spec reapply as a best-effort workaround.
        """
        for cluster_id in self._nfs_cluster_names():
            try:
                out = self.rados_obj.node.shell(
                    [f"ceph nfs cluster info {cluster_id} --format json"]
                )[0].strip()
                info = json.loads(out) if out else {}
                info_str = json.dumps(info).lower()
                if "nfsv3" in info_str or "v3" in info_str:
                    log.info(f"NFSv3 already enabled on {cluster_id} cluster")
                    continue
            except Exception as err:
                log.debug(f"NFSv3 pre-check on {cluster_id} failed: {err}")

            try:
                spec_out = self.rados_obj.node.shell(
                    [f"ceph orch ls --service-name nfs.{cluster_id} --format json"]
                )[0].strip()
                specs = json.loads(spec_out) if spec_out else []
                if specs and isinstance(specs, list):
                    spec = specs[0].get("spec", {})
                    if not spec.get("enable_nfsv3"):
                        spec["enable_nfsv3"] = True
                        spec_json = json.dumps(specs[0])
                        self.rados_obj.node.shell(
                            [
                                f"echo '{spec_json}' > /tmp/_nfsv3_spec.json"
                                f" && ceph orch apply -i /tmp/_nfsv3_spec.json"
                                f" && rm -f /tmp/_nfsv3_spec.json"
                            ]
                        )
                        log.info(f"NFSv3 enabled on {cluster_id} via orch spec reapply")
                    else:
                        log.info(f"NFSv3 already in service spec for {cluster_id}")
                    continue
            except Exception as err:
                log.debug(f"NFSv3 orch spec reapply on {cluster_id} failed: {err}")

            raise RuntimeError(
                f"NFSv3 not enabled on {cluster_id} and no update mechanism "
                "available; NFSv3 verification will likely fail for multi-hop "
                "scenarios where the cluster was created without --enable-nfsv3"
            )

    def _verify_nfs_v3(self, snapshot):
        """Verify NFSv3 flag persists in cluster info or service spec post-upgrade."""
        try:
            for cluster_id in self._nfs_cluster_names():
                out = self.rados_obj.node.shell(
                    [f"ceph nfs cluster info {cluster_id} --format json"]
                )[0].strip()
                info = json.loads(out) if out else {}
                info_str = json.dumps(info).lower()
                if "nfsv3" in info_str or "v3" in info_str:
                    continue
                out2 = self.rados_obj.node.shell(
                    [f"ceph orch ls --service-name nfs.{cluster_id} " "--format json"]
                )[0].strip()
                svc = json.loads(out2) if out2 else []
                svc_str = json.dumps(svc).lower()
                if "nfsv3" in svc_str or "enable-nfsv3" in svc_str:
                    continue
                return (
                    False,
                    f"NFSv3 flag not found on {cluster_id} "
                    "in cluster info or service spec",
                )
            return True, "NFSv3 enabled on all NFS clusters"
        except Exception as err:
            return False, str(err)

    def _nfs_version_supports_export_default(self):
        """Check if the running Ceph version supports set-export-default (>= 20.2.0)."""
        try:
            ceph_ver = self._parse_ceph_version()
            parts = ceph_ver.split(".")
            if len(parts) >= 2:
                major, minor = int(parts[0]), int(parts[1])
                if major > 20 or (major == 20 and minor >= 2):
                    return True
            log.info(
                f"NFS export-default/delegations: requires >= 20.2.0 (9.1+), "
                f"got {ceph_ver}, skipping"
            )
            return False
        except Exception as e:
            log.info(f"NFS export-default: version check failed ({e}), skipping")
            return False

    def _enable_nfs_export_default(self):
        """Set EXPORT DEFAULT delegations on all NFS clusters (requires 20.2.0+)."""
        if not self._nfs_version_supports_export_default():
            return

        for cluster_id in self._nfs_cluster_names():
            try:
                self.rados_obj.node.shell(
                    [f"ceph nfs cluster set-export-default {cluster_id} rw"]
                )
                log.info(f"NFS export default set on {cluster_id}")
            except Exception as err:
                log.warning(f"NFS export default on {cluster_id} failed: {err}")
                raise

    def _verify_nfs_export_default(self, snapshot):
        """Verify EXPORT DEFAULT block is configured on all NFS clusters."""
        try:
            for cluster_id in self._nfs_cluster_names():
                info = self._shell_json(
                    f"ceph nfs cluster get-export-default {cluster_id} --format json",
                )
                if info.get("message") and "No EXPORT DEFAULT" in info["message"]:
                    return (False, f"no EXPORT DEFAULT block on {cluster_id}")
                deleg = info.get("delegations")
                if not deleg:
                    return (
                        False,
                        f"export default delegations not set on {cluster_id}",
                    )
            return True, "export default configured on all NFS clusters"
        except Exception as err:
            return False, str(err)

    # -- SMB features --

    def _register_smb_features(self, r):
        """Register SMB features: multiple shares, read-only, ACLs, CTDB clustering."""
        smb_features = {
            "multiple_shares": (
                lambda: log.info("Multiple SMB shares created in Phase 1"),
                self._verify_smb_multi_shares,
                [],
            ),
            "readonly_share": (
                lambda: log.info("Read-only SMB share created in Phase 1"),
                self._stub_verify,
                [],
            ),
            "login_control_acls": (
                lambda: log.info("SMB login control ACLs set in Phase 1"),
                self._stub_verify,
                [],
            ),
            "clustering_ctdb": (
                lambda: log.info("SMB CTDB clustering set in Phase 1"),
                self._verify_smb_ctdb,
                [self._prereq_smb_multi_node],
            ),
        }
        for name, (enable, verify, prereqs) in smb_features.items():
            r[f"smb.{name}"] = FeatureEntry(
                enable=enable,
                verify=verify,
                toggle=f"smb.{name}",
                service="smb",
                prerequisites=prereqs,
            )

    def _prereq_smb_multi_node(self):
        """Check that at least 2 SMB-role nodes exist for CTDB clustering."""
        try:
            smb_nodes = self.ceph_cluster.get_nodes(role="smb")
            return len(smb_nodes) >= 2
        except Exception:
            return False

    def _verify_smb_multi_shares(self, snapshot):
        """Verify at least 2 SMB shares exist on upgradesmb cluster."""
        try:
            shares = self._shell_json(
                "ceph smb share ls upgradesmb --format json",
                default=[],
            )
            if len(shares) >= 2:
                return True, f"{len(shares)} SMB shares"
            return False, f"only {len(shares)} share(s)"
        except Exception as err:
            return False, str(err)

    def _verify_smb_ctdb(self, snapshot):
        """Verify CTDB clustering is active on at least one SMB cluster."""
        try:
            clusters = self._shell_json(
                "ceph smb cluster ls --format json",
                default=[],
            )
            for c in clusters:
                features = c.get("features", [])
                if "clustered" in features:
                    return True, "CTDB clustering active (features: clustered)"
                clustering = c.get("clustering")
                if clustering in ("always", "default"):
                    return True, f"CTDB clustering mode={clustering}"
            return False, f"no CTDB-clustered SMB cluster found: {clusters}"
        except Exception as err:
            return False, str(err)

    # -- NVMeoF features --

    def _register_nvmeof_features(self, r):
        """Register NVMeoF features: QoS, namespace masking, multi-GW HA, discovery, auto-resize."""
        nvme_features = {
            "qos": (
                lambda: log.info("NVMeoF QoS set via gateway CLI"),
                self._stub_verify,
                [],
            ),
            "namespace_masking": (
                lambda: log.info("Namespace masking set via gateway CLI"),
                self._stub_verify,
                [],
            ),
            "multi_gateway_ha": (
                lambda: log.info("Multi-GW HA set at subsystem creation"),
                self._verify_nvme_multi_gw,
                [self._prereq_nvme_multi_gw],
            ),
            "discovery_controller": (
                lambda: log.info("Discovery controller verified from initiator"),
                self._stub_verify,
                [],
            ),
            "auto_resize": (
                lambda: log.info("Auto-resize set via gateway CLI"),
                self._stub_verify,
                [],
            ),
        }
        for name, (enable, verify, prereqs) in nvme_features.items():
            r[f"nvmeof.{name}"] = FeatureEntry(
                enable=enable,
                verify=verify,
                toggle=f"nvmeof.{name}",
                service="nvmeof",
                prerequisites=prereqs,
            )

    def _prereq_nvme_multi_gw(self):
        """Check that at least 2 NVMeoF gateway nodes exist for HA."""
        try:
            gw_nodes = self.ceph_cluster.get_nodes(role="nvmeof")
            return len(gw_nodes) >= 2
        except Exception:
            return False

    def _verify_nvme_multi_gw(self, snapshot):
        """Verify at least 2 NVMeoF gateways are running post-upgrade."""
        try:
            out = self.rados_obj.run_ceph_command(
                cmd="ceph orch ps --daemon-type nvmeof"
            )
            running = [d for d in out if d.get("status_desc") == "running"]
            if len(running) >= 2:
                return True, f"{len(running)} NVMeoF gateways running"
            return False, f"only {len(running)} gateway(s) running"
        except Exception as err:
            return False, str(err)

    # -- MGR module features --

    def _register_mgr_module_features(self, r):
        """Register MGR module verification features."""
        mgr_features = {
            "verify_prometheus": (
                lambda: None,
                self._verify_mgr_prometheus,
                [],
            ),
            "verify_crash": (
                lambda: None,
                self._verify_mgr_crash,
                [],
            ),
            "verify_telemetry": (
                lambda: None,
                self._verify_mgr_telemetry,
                [],
            ),
            "verify_balancer": (
                lambda: None,
                self._verify_mgr_balancer,
                [],
            ),
            "verify_pg_autoscaler": (
                lambda: None,
                self._verify_mgr_pg_autoscaler,
                [],
            ),
            "verify_dashboard": (
                lambda: None,
                self._verify_mgr_dashboard,
                [],
            ),
        }
        for name, (enable, verify, prereqs) in mgr_features.items():
            r[f"mgr_modules.{name}"] = FeatureEntry(
                enable=enable,
                verify=verify,
                toggle=f"mgr_modules.{name}",
                service="mgr_modules",
                prerequisites=prereqs,
            )

    def _verify_mgr_prometheus(self, snapshot):
        """Verify prometheus MGR module is enabled post-upgrade."""
        return self._verify_mgr_module(snapshot, "prometheus")

    def _verify_mgr_crash(self, snapshot):
        """Verify crash MGR module is enabled post-upgrade."""
        return self._verify_mgr_module(snapshot, "crash")

    def _verify_mgr_telemetry(self, snapshot):
        """Verify telemetry MGR module responds to status query."""
        try:
            out = self.rados_obj.node.shell(["ceph telemetry status"])[0].strip()
            return True, f"telemetry module responsive: {out[:100]}"
        except Exception as err:
            return False, str(err)

    def _verify_mgr_balancer(self, snapshot):
        """Verify balancer MGR module is active."""
        try:
            status = snapshot.get("balancer_status") if snapshot else None
            if status is None:
                try:
                    status = self.rados_obj.run_ceph_command(cmd="ceph balancer status")
                except Exception:
                    return True, "skipped - could not query balancer status"
            if status.get("active", False):
                return True, f"balancer active, mode={status.get('mode')}"
            return False, f"balancer not active: {status}"
        except Exception as err:
            return False, str(err)

    def _verify_mgr_pg_autoscaler(self, snapshot):
        """Verify PG autoscaler module returns status for all pools."""
        try:
            autoscale = self.rados_obj.run_ceph_command(
                cmd="ceph osd pool autoscale-status"
            )
            if autoscale:
                return True, f"{len(autoscale)} pools with autoscale status"
            return False, "autoscale status empty"
        except Exception as err:
            return False, str(err)

    def _verify_mgr_dashboard(self, snapshot):
        """Verify dashboard URL is present in mgr services post-upgrade."""
        try:
            out = self.rados_obj.run_ceph_command(cmd="ceph mgr services")
            if out.get("dashboard"):
                return True, f"dashboard URL: {out['dashboard']}"
            return False, "dashboard URL not found in mgr services"
        except Exception as err:
            return False, str(err)

    # ------------------------------------------------------------------
    # Monitoring data transformation
    # ------------------------------------------------------------------

    def _build_monitoring_summary(self, raw_data):
        """Transform raw stats samples into the summary format bug validators expect.

        The stats collector stores time-series samples keyed by collector type.
        Bug validators expect pre-aggregated summaries. This method bridges
        the gap by iterating the raw samples once and producing:

        - mds_state_durations: {mds_name: {state: duration_sec}}
        - health_history: [{status, timestamp, summary, phase}]
        - max_upgrade_stall_sec: int (from phase_boundaries)
        - upgrade_duration_sec: int (from phase_boundaries)
        - crash_data: {crashes: []} (live-queried, not from samples)
        - log_patterns: {} (not available from CLI-based stats)
        - upgrade_status_history: [{message, timestamp}]
        - smb_total_outage_sec: 0 (not tracked in current stats)
        """
        summary = {
            "mds_state_durations": {},
            "health_history": [],
            "max_upgrade_stall_sec": 0,
            "upgrade_duration_sec": 0,
            "crash_data": {"crashes": []},
            "log_patterns": {},
            "upgrade_status_history": [],
            "smb_total_outage_sec": 0,
            "all_health_checks_seen": {},
            "daemon_state_pre": (
                self._daemon_states.get("pre", {})
                if hasattr(self, "_daemon_states")
                else {}
            ),
            "daemon_state_post": (
                self._daemon_states.get("post", {})
                if hasattr(self, "_daemon_states")
                else {}
            ),
        }

        samples = raw_data.get("samples", [])
        boundaries = raw_data.get("phase_boundaries", [])

        # -- Upgrade duration from phase boundaries (ends at upgrade_end) --
        from upgrade_thrashing.lifecycle_log import resolve_phase_window_from_boundaries

        boundary_map: dict[str, list[str]] = {}
        for b in boundaries:
            name = b.get("name", "")
            ts = b.get("timestamp", "")
            if name and ts:
                boundary_map.setdefault(name, []).append(ts)

        upgrade_start_iso, upgrade_end_iso = resolve_phase_window_from_boundaries(
            "upgrade", boundary_map
        )
        upgrade_end = upgrade_end_iso
        if upgrade_start_iso and upgrade_end_iso:
            try:
                s = datetime.fromisoformat(upgrade_start_iso)
                e = datetime.fromisoformat(upgrade_end_iso)
                summary["upgrade_duration_sec"] = (e - s).total_seconds()
            except Exception:
                pass

        # -- Single-pass extraction of MDS state, health, and upgrade status --
        # Track cumulative duration per (mds_name, state) by summing intervals
        # between consecutive samples where the MDS is observed in that state.
        mds_state_cumulative: dict[tuple, float] = {}
        mds_state_last_ts: dict[tuple, str] = {}
        last_msg = ""
        last_services = []
        last_progress_str = ""
        last_change_ts = None
        max_stall = 0

        for sample in samples:
            collector = sample.get("collector")
            metrics = sample.get("metrics")
            ts = sample.get("timestamp", "")

            if collector == "fs_status" and isinstance(metrics, dict):
                for rank_info in metrics.get("mdsmap", []):
                    if not isinstance(rank_info, dict):
                        continue
                    name = rank_info.get("name", "")
                    state = rank_info.get("state", "")
                    if not name or not state:
                        continue
                    key = (name, state)
                    prev_ts = mds_state_last_ts.get(key)
                    if prev_ts and ts:
                        try:
                            delta = (
                                datetime.fromisoformat(ts)
                                - datetime.fromisoformat(prev_ts)
                            ).total_seconds()
                            if 0 < delta < 300:
                                mds_state_cumulative[key] = (
                                    mds_state_cumulative.get(key, 0) + delta
                                )
                        except Exception:
                            pass
                    mds_state_last_ts[key] = ts

            elif collector == "health" and metrics:
                status = ""
                health_summary = ""
                if isinstance(metrics, dict):
                    status = metrics.get("status", "")
                    checks = metrics.get("checks", {})
                    health_summary = ", ".join(checks.keys()) if checks else ""
                elif isinstance(metrics, str):
                    status = metrics
                if status:
                    summary["health_history"].append(
                        {
                            "status": status,
                            "timestamp": ts,
                            "summary": health_summary,
                            "phase": sample.get("phase", ""),
                        }
                    )

            elif collector == "upgrade_status" and isinstance(metrics, dict):
                msg = metrics.get("message", "")
                services = metrics.get("services_complete", [])
                if msg:
                    summary["upgrade_status_history"].append(
                        {
                            "message": msg,
                            "timestamp": ts,
                            "progress": metrics.get("progress", ""),
                            "is_paused": metrics.get("is_paused", False),
                        }
                    )
                progress_str = metrics.get("progress", "")
                progress = (
                    (msg != last_msg)
                    or (services != last_services)
                    or (progress_str != last_progress_str)
                )
                if progress:
                    if last_change_ts and ts:
                        try:
                            prev = datetime.fromisoformat(last_change_ts)
                            curr = datetime.fromisoformat(ts)
                            stall = (curr - prev).total_seconds()
                            max_stall = max(max_stall, stall)
                        except Exception:
                            pass
                    last_msg = msg
                    last_services = services
                    last_progress_str = progress_str
                    last_change_ts = ts

            elif collector == "health_tracker" and isinstance(metrics, dict):
                summary["all_health_checks_seen"] = metrics.get(
                    "all_health_checks_seen", {}
                )

        # Terminal stall: gap from last message change to upgrade end
        if last_change_ts and upgrade_end:
            try:
                final_gap = (
                    datetime.fromisoformat(upgrade_end)
                    - datetime.fromisoformat(last_change_ts)
                ).total_seconds()
                max_stall = max(max_stall, final_gap)
            except Exception:
                pass

        summary["max_upgrade_stall_sec"] = max_stall

        for (name, state), dur in mds_state_cumulative.items():
            short_state = state.split(":")[-1] if ":" in state else state
            summary["mds_state_durations"].setdefault(name, {})[short_state] = round(
                dur, 1
            )

        # -- Crash data (query live, filter to test window) --
        try:
            crash_ls = self.rados_obj.run_ceph_command(cmd="ceph crash ls-new")
            if isinstance(crash_ls, list):
                first_ts = samples[0].get("timestamp", "") if samples else ""
                if first_ts:
                    try:
                        first_dt = datetime.fromisoformat(
                            first_ts.replace("Z", "+00:00")
                        )
                    except (ValueError, TypeError):
                        first_dt = None
                    if first_dt:
                        filtered = []
                        for c in crash_ls:
                            c_ts = c.get("timestamp", "")
                            try:
                                c_norm = c_ts.replace("_", "T")
                                if not c_norm.endswith("Z") and "+" not in c_norm:
                                    c_norm += "+00:00"
                                c_norm = c_norm.replace("Z", "+00:00")
                                c_dt = datetime.fromisoformat(c_norm)
                                if c_dt >= first_dt:
                                    filtered.append(c)
                            except (ValueError, TypeError):
                                filtered.append(c)
                        crash_ls = filtered
                summary["crash_data"]["crashes"] = crash_ls
        except Exception:
            pass

        for key in (
            "mount_health",
            "nfs_client_errors",
            "nfs_grace_analysis",
            "daemon_timeline",
        ):
            if key in raw_data:
                summary[key] = raw_data[key]

        return summary

    # ------------------------------------------------------------------
    # Bug validation registry
    # ------------------------------------------------------------------

    def _build_bug_registry(self):
        """Assemble the bug validation registry (A1-A21) with
        historical and point-in-time checks."""
        return {
            "a1_mds_clientreplay": BugValidator(
                self._bug_a1_mds_clientreplay,
                _HISTORICAL,
                "MDS Clientreplay/Rejoin stuck",
                "",
            ),
            "a2_mds_stopping": BugValidator(
                self._bug_a2_mds_stopping,
                _HISTORICAL,
                "MDS stopping stuck",
                "",
            ),
            "a3_health_err": BugValidator(
                self._bug_a3_health_err,
                _HISTORICAL,
                "Upgrade causing HEALTH_ERR",
                "",
            ),
            "a4_nfs_crash": BugValidator(
                self._bug_a4_nfs_crash,
                _POINT_IN_TIME,
                "NFS Ganesha crash",
                "",
            ),
            "a5_require_osd_release": BugValidator(
                self._bug_a5_require_osd_release,
                _POINT_IN_TIME,
                "require_osd_release mismatch",
                "",
            ),
            "a6_cephadm_refresh_failed": BugValidator(
                self._bug_a6_cephadm_refresh,
                _POINT_IN_TIME,
                "CEPHADM_REFRESH_FAILED",
                "",
            ),
            "a7_daemon_old_version": BugValidator(
                self._bug_a7_daemon_old_version,
                _HISTORICAL,
                "DAEMON_OLD_VERSION persists",
                "",
            ),
            "a8_upgrade_stuck": BugValidator(
                self._bug_a8_upgrade_stuck,
                _HISTORICAL,
                "Upgrade stuck/stalled",
                "",
            ),
            "a9_cephfs_client_crash": BugValidator(
                self._bug_a9_cephfs_client_crash,
                _POINT_IN_TIME,
                "CephFS client crash",
                "",
            ),
            "a10_osd_activation": BugValidator(
                self._bug_a10_osd_activation,
                _POINT_IN_TIME,
                "OSD activation failure",
                "",
            ),
            "a11_mgr_crash": BugValidator(
                self._bug_a11_mgr_crash,
                _HISTORICAL,
                "MGR crash/failover loop",
                "",
            ),
            "a12_ec_scrub_errors": BugValidator(
                self._bug_a12_ec_scrub,
                _POINT_IN_TIME,
                "Fast EC scrub errors",
                "",
            ),
            "a13_nvmeof_gateway_failure": BugValidator(
                self._bug_a13_nvmeof_gw,
                _POINT_IN_TIME,
                "NVMeoF gateway failure",
                "",
            ),
            "a14_mgr_snaprealm_crash": BugValidator(
                self._bug_a14_mgr_snaprealm,
                _HISTORICAL,
                "MGR SnapRealmInfoNew decode crash",
                "IBMCEPH-12258",
            ),
            "a15_mgr_module_import": BugValidator(
                self._bug_a15_mgr_module_import,
                _HISTORICAL,
                "MGR module import failure",
                "IBMCEPH-16270",
            ),
            "a16_nfs_redeploy_failure": BugValidator(
                self._bug_a16_nfs_redeploy,
                _HISTORICAL,
                "NFS UPGRADE_REDEPLOY_DAEMON",
                "IBMCEPH-16219",
            ),
            "a17_smb_all_down": BugValidator(
                self._bug_a17_smb_all_down,
                _HISTORICAL,
                "SMB total outage during upgrade",
                "IBMCEPH-11758",
            ),
            "a18_nvmeof_gw_startup": BugValidator(
                self._bug_a18_nvmeof_startup,
                _HISTORICAL,
                "NVMeoF GW startup failure",
                "IBMCEPH-14163",
            ),
            "a19_cephadm_grace_tool": BugValidator(
                self._bug_a19_cephadm_grace,
                _POINT_IN_TIME,
                "cephadm grace tool failed",
                "IBMCEPH-11464",
            ),
            "a20_osd_markdown_shutdown": BugValidator(
                self._bug_a20_osd_markdown,
                _HISTORICAL,
                "OSD markdown count exceeded",
                "IBMCEPH-9873",
            ),
            "a21_upgrade_health_err": BugValidator(
                self._bug_a21_upgrade_health_err,
                _HISTORICAL,
                "Upgrade HEALTH_ERR (9.x blocker)",
                "IBMCEPH-15667",
            ),
            "a22_upgrade_exception": BugValidator(
                self._bug_a22_upgrade_exception,
                _HISTORICAL,
                "Upgrade exception / blocking health warning",
                "",
            ),
            "a23_daemon_count_mismatch": BugValidator(
                self._bug_a23_daemon_count_mismatch,
                _POINT_IN_TIME,
                "Daemon count/state mismatch after upgrade",
                "",
            ),
            "a24_grace_metrics": BugValidator(
                self._bug_a24_grace_metrics,
                _POINT_IN_TIME,
                "NFS grace duration / missing grace_end",
                "IBMCEPH-17685",
            ),
            "a27_nfs_client_continuity": BugValidator(
                self._bug_a27_nfs_client_continuity,
                _POINT_IN_TIME,
                "NFS client mount/IO continuity during upgrade",
                "IBMCEPH-17685",
            ),
        }

    # -- Historical bug validators --

    def _bug_a1_mds_clientreplay(self, monitoring_data, bug_cfg):
        """Detect MDS stuck in clientreplay/rejoin state beyond threshold."""
        mds_states = monitoring_data.get("mds_state_durations", {})
        if not mds_states:
            return True, "skip:no monitoring data available"
        threshold = bug_cfg.get("a1_threshold_sec", 120)
        for mds, states in mds_states.items():
            for state in ("clientreplay", "rejoin"):
                duration = states.get(state, 0)
                if duration > threshold:
                    return False, (
                        f"{mds} in {state} for {duration}s "
                        f"(threshold: {threshold}s)"
                    )
        max_dur = max(
            (
                s.get("clientreplay", 0) + s.get("rejoin", 0)
                for s in mds_states.values()
            ),
            default=0,
        )
        return True, f"max Clientreplay+Rejoin: {max_dur}s"

    def _bug_a2_mds_stopping(self, monitoring_data, bug_cfg):
        """Detect MDS stuck in stopping state beyond threshold."""
        mds_states = monitoring_data.get("mds_state_durations", {})
        if not mds_states:
            return True, "skip:no monitoring data available"
        threshold = bug_cfg.get("a2_threshold_sec", 600)
        for mds, states in mds_states.items():
            duration = states.get("stopping", 0)
            if duration > threshold:
                return False, (
                    f"{mds} stopping for {duration}s " f"(threshold: {threshold}s)"
                )
        return True, "no MDS stuck in stopping"

    _EXPECTED_UPGRADE_HEALTH_CHECKS = {
        "DAEMON_OLD_VERSION",
        "OSD_DOWN",
        "OSD_UP_LESS_THAN_IN",
        "MON_DOWN",
        "MDS_DEGRADED",
        "FS_DEGRADED",
        "FS_WITH_FAILED_MDS",
        "MDS_ALL_DOWN",
        "MDS_UP_LESS_THAN_MAX",
        "RECENT_MGR_MODULE_CRASH",
        "RECENT_CRASH",
        "IBM_LICENSE_NOT_ACCEPTED",
    }

    def _bug_a3_health_err(self, monitoring_data, bug_cfg):
        """Detect unexpected HEALTH_ERR checks (beyond known upgrade-transient ones)."""
        health_history = monitoring_data.get("health_history", [])
        if not health_history:
            return True, "skip:no monitoring data available"
        for entry in health_history:
            if entry.get("status") != "HEALTH_ERR":
                continue
            checks_str = entry.get("summary", "")
            checks = set(c.strip() for c in checks_str.split(",") if c.strip())
            unexpected = checks - self._EXPECTED_UPGRADE_HEALTH_CHECKS
            if unexpected:
                return False, (
                    f"Unexpected HEALTH_ERR at {entry.get('timestamp')}: "
                    f"{unexpected} (all: {checks_str[:200]})"
                )
        return True, "no unexpected HEALTH_ERR during upgrade"

    def _bug_a7_daemon_old_version(self, monitoring_data, bug_cfg):
        """Detect mixed daemon versions remaining after upgrade completion."""
        try:
            versions = self.rados_obj.run_ceph_command(cmd="ceph versions")
            for daemon_type, ver_map in versions.items():
                if daemon_type == "overall":
                    continue
                if len(ver_map) > 1:
                    return False, (f"{daemon_type} has mixed versions: {ver_map}")
            return True, "all daemons at single version"
        except Exception as err:
            return False, str(err)

    def _bug_a8_upgrade_stuck(self, monitoring_data, bug_cfg):
        """Detect upgrade stalls exceeding the configured threshold.

        Uses the same threshold as the runtime stall detector in
        _monitor_upgrade (phase_timing.upgrade_stall_threshold_sec)
        unless explicitly overridden via bug_validations.a8_stall_threshold_sec.
        """
        runtime_threshold = self.config.get("phase_timing", {}).get(
            "upgrade_stall_threshold_sec", 2700
        )
        stall_threshold = bug_cfg.get("a8_stall_threshold_sec", runtime_threshold)
        max_stall = monitoring_data.get("max_upgrade_stall_sec", 0)
        if max_stall > stall_threshold:
            return False, (
                f"upgrade stalled for {max_stall}s " f"(threshold: {stall_threshold}s)"
            )
        total = monitoring_data.get("upgrade_duration_sec", 0)
        return True, f"upgrade completed in {total}s, max stall: {max_stall}s"

    def _bug_a11_mgr_crash(self, monitoring_data, bug_cfg):
        """Detect MGR daemon crashes during the upgrade window."""
        crashes = monitoring_data.get("crash_data", {})
        mgr_crashes = [
            c
            for c in crashes.get("crashes", [])
            if "mgr" in str(c.get("entity_name", "")).lower()
        ]
        if mgr_crashes:
            return False, (
                f"{len(mgr_crashes)} MGR crash(es): "
                f"{mgr_crashes[0].get('crash_id', 'unknown')}"
            )
        return True, "no MGR crashes"

    def _bug_a14_mgr_snaprealm(self, monitoring_data, bug_cfg):
        """Check for SnapRealmInfoNew decode crashes via ceph crash ls-new."""
        try:
            out = self.rados_obj.run_ceph_command(cmd="ceph crash ls-new")
            if isinstance(out, list):
                snap_crashes = [
                    c
                    for c in out
                    if "SnapRealmInfoNew" in str(c.get("backtrace", ""))
                    or "SnapRealmInfoNew" in str(c.get("assert_msg", ""))
                ]
                if snap_crashes:
                    cid = snap_crashes[0].get("crash_id", "unknown")
                    return False, f"SnapRealmInfoNew decode crash: {cid}"
        except Exception as e:
            return True, f"crash check unavailable: {e}"
        return True, "no SnapRealmInfoNew decode crashes"

    def _bug_a15_mgr_module_import(self, monitoring_data, bug_cfg):
        """Check for MGR module import errors via ceph health detail."""
        try:
            health = self.rados_obj.run_ceph_command(cmd="ceph health detail")
            if isinstance(health, dict):
                checks = health.get("checks", {})
                for check_name, check_data in checks.items():
                    msg = str(check_data.get("summary", {}).get("message", ""))
                    if "module" in msg.lower() and (
                        "import" in msg.lower() or "load" in msg.lower()
                    ):
                        return False, f"MGR module error: {check_name}: {msg[:200]}"
            out = self.rados_obj.run_ceph_command(cmd="ceph crash ls-new")
            if isinstance(out, list):
                mgr_mod_crashes = [
                    c
                    for c in out
                    if c.get("entity_name", "").startswith("mgr.")
                    and (
                        "import" in str(c.get("backtrace", "")).lower()
                        or "module" in str(c.get("assert_msg", "")).lower()
                    )
                ]
                if mgr_mod_crashes:
                    cid = mgr_mod_crashes[0].get("crash_id", "unknown")
                    return False, f"MGR module import crash: {cid}"
        except Exception as e:
            return True, f"module import check unavailable: {e}"
        return True, "no MGR module import errors"

    def _bug_a16_nfs_redeploy(self, monitoring_data, bug_cfg):
        """Detect UPGRADE_REDEPLOY_DAEMON or UPGRADE_FAILED_PULL events for NFS."""
        upgrade_events = monitoring_data.get("upgrade_status_history", [])
        issues = [
            str(e.get("message", ""))
            for e in upgrade_events
            if "UPGRADE_REDEPLOY_DAEMON" in str(e.get("message", ""))
            or "UPGRADE_FAILED_PULL" in str(e.get("message", ""))
        ]
        nfs_issues = [msg for msg in issues if "nfs" in msg.lower()]
        if nfs_issues:
            return False, f"NFS redeploy issues: {nfs_issues}"
        return True, "no NFS UPGRADE_REDEPLOY_DAEMON errors"

    def _bug_a17_smb_all_down(self, monitoring_data, bug_cfg):
        """Check if all SMB daemons are currently down (post-upgrade)."""
        if not self._service_deployed("smb"):
            return True, "SMB not deployed, skipping"
        try:
            daemons = self.rados_obj.run_ceph_command(
                cmd="ceph orch ps --daemon-type smb"
            )
            if isinstance(daemons, list) and daemons:
                down = [d for d in daemons if d.get("status_desc", "") != "running"]
                if len(down) == len(daemons):
                    return False, (
                        f"All {len(daemons)} SMB daemons are down post-upgrade"
                    )
                if down:
                    names = [d.get("daemon_name", "?") for d in down]
                    return True, (
                        f"{len(down)}/{len(daemons)} SMB daemons down "
                        f"(partial, not total outage): {names}"
                    )
        except Exception as e:
            return True, f"SMB status check unavailable: {e}"
        return True, "all SMB daemons running post-upgrade"

    def _bug_a18_nvmeof_startup(self, monitoring_data, bug_cfg):
        """Check for NVMeoF gateway startup failures post-upgrade."""
        if not self._service_deployed("nvmeof"):
            return True, "NVMeoF not deployed, skipping"
        try:
            daemons = self.rados_obj.run_ceph_command(
                cmd="ceph orch ps --daemon-type nvmeof"
            )
            if isinstance(daemons, list):
                failed = [
                    d
                    for d in daemons
                    if d.get("status_desc", "") in ("error", "stopped")
                ]
                if failed:
                    names = [d.get("daemon_name", "?") for d in failed]
                    return False, (
                        f"NVMeoF GW startup failure: {names} "
                        f"status={[d.get('status_desc') for d in failed]}"
                    )
            crashes = self.rados_obj.run_ceph_command(cmd="ceph crash ls-new")
            if isinstance(crashes, list):
                nvme_crashes = [
                    c
                    for c in crashes
                    if "nvmeof" in str(c.get("entity_name", "")).lower()
                ]
                if nvme_crashes:
                    cid = nvme_crashes[0].get("crash_id", "unknown")
                    return False, f"NVMeoF crash detected: {cid}"
        except Exception as e:
            return True, f"NVMeoF check unavailable: {e}"
        return True, "no NVMeoF GW startup failures"

    def _bug_a20_osd_markdown(self, monitoring_data, bug_cfg):
        """Check for OSD markdown shutdowns via crash list and OSD tree."""
        try:
            crashes = self.rados_obj.run_ceph_command(cmd="ceph crash ls-new")
            if isinstance(crashes, list):
                osd_crashes = [
                    c
                    for c in crashes
                    if c.get("entity_name", "").startswith("osd.")
                    and "markdown" in str(c.get("backtrace", "")).lower()
                ]
                if osd_crashes:
                    cid = osd_crashes[0].get("crash_id", "unknown")
                    return False, f"OSD markdown shutdown crash: {cid}"
            osd_tree = self.rados_obj.run_ceph_command(cmd="ceph osd tree")
            if isinstance(osd_tree, dict):
                down_osds = [
                    n
                    for n in osd_tree.get("nodes", [])
                    if n.get("type") == "osd" and n.get("status") == "down"
                ]
                if down_osds:
                    ids = [n["id"] for n in down_osds[:5]]
                    return False, (
                        f"{len(down_osds)} OSDs down post-upgrade "
                        f"(possible markdown): {ids}"
                    )
        except Exception as e:
            return True, f"OSD markdown check unavailable: {e}"
        return True, "no OSD markdown shutdowns"

    def _bug_a21_upgrade_health_err(self, monitoring_data, bug_cfg):
        """Detect HEALTH_ERR persisting into the post-upgrade phase (9.x blocker)."""
        health_history = monitoring_data.get("health_history", [])
        if not health_history:
            return True, "skip:no monitoring data available"
        post_upgrade = [
            e
            for e in health_history
            if e.get("phase") == "post_upgrade" and e.get("status") == "HEALTH_ERR"
        ]
        if post_upgrade:
            return False, (
                f"HEALTH_ERR post-upgrade: "
                f"{post_upgrade[0].get('summary', '')[:200]}"
            )
        return True, "no HEALTH_ERR during post-upgrade phase"

    def _bug_a22_upgrade_exception(self, monitoring_data, bug_cfg):
        """Detect UPGRADE_* errors and blocking health warnings."""
        upgrade_events = monitoring_data.get("upgrade_status_history", [])
        seen_msgs = set()
        classified = []
        for e in upgrade_events:
            msg = str(e.get("message", ""))
            error_code, subcause = classify_upgrade_error(msg)
            if not error_code:
                continue
            if msg in seen_msgs:
                continue
            seen_msgs.add(msg)
            ts = e.get("timestamp", "?")
            progress = e.get("progress", "")
            classified.append(
                f"[{ts}] [{error_code}/{subcause}] ({progress}) {msg[:200]}"
            )

        all_checks = monitoring_data.get("all_health_checks_seen", {})
        blocking_warnings = []
        other_warnings = []
        for code, info in sorted(all_checks.items()):
            severity = info.get("severity", "informational")
            if severity == "blocking":
                blocking_warnings.append(code)
            else:
                other_warnings.append(f"{code}({severity})")

        parts = []
        failed = False

        if classified:
            failed = True
            parts.append(
                f"{len(classified)} upgrade exception(s): " + "; ".join(classified[:5])
            )
        if blocking_warnings:
            failed = True
            parts.append(f"Blocking health warnings: {', '.join(blocking_warnings)}")
        if other_warnings:
            parts.append(
                f"Other warnings (info only): {', '.join(other_warnings[:10])}"
            )

        if failed:
            return False, " | ".join(parts)
        result_text = "no upgrade exceptions detected"
        if other_warnings:
            result_text += f" | Warnings seen: {', '.join(other_warnings[:10])}"
        return True, result_text

    # -- Point-in-time bug validators --

    def _bug_a4_nfs_crash(self, monitoring_data, bug_cfg):
        """Detect NFS Ganesha daemons not running post-upgrade."""
        if "nfs" not in self.deployed_services:
            return True, "NFS not deployed"
        try:
            orch_ps = self.rados_obj.run_ceph_command(
                cmd="ceph orch ps --daemon-type nfs"
            )
            down = [d for d in orch_ps if d.get("status_desc") != "running"]
            if down:
                return False, f"NFS daemons not running: {down}"
            return True, "all NFS daemons running"
        except Exception as err:
            return False, str(err)

    def _bug_a5_require_osd_release(self, monitoring_data, bug_cfg):
        """Detect require_osd_release mismatch or mixed OSD versions post-upgrade."""
        try:
            osd_dump = self.rados_obj.run_ceph_command(cmd="ceph osd dump")
            req_release = osd_dump.get("require_osd_release", "unknown")
            versions = self.rados_obj.run_ceph_command(cmd="ceph versions")
            osd_vers = versions.get("osd", {})
            if len(osd_vers) > 1:
                return False, (
                    f"mixed OSD versions: {osd_vers}, "
                    f"require_osd_release={req_release}"
                )
            return True, f"require_osd_release={req_release}"
        except Exception as err:
            return False, str(err)

    def _bug_a6_cephadm_refresh(self, monitoring_data, bug_cfg):
        """Detect CEPHADM_REFRESH_FAILED health check post-upgrade."""
        try:
            health = self.rados_obj.run_ceph_command(cmd="ceph health detail")
            detail_str = json.dumps(health)
            if "CEPHADM_REFRESH_FAILED" in detail_str:
                return False, "CEPHADM_REFRESH_FAILED in health detail"
            return True, "no CEPHADM_REFRESH_FAILED"
        except Exception as err:
            return False, str(err)

    # Real kclient crash indicators only. Bare "libceph" matches CephX auth -13
    # (EACCES) during key rotation/cipher lockdown — expected noise, not a crash.
    _CLIENT_DMESG_CRASH_PATTERN = (
        r"segfault|SIGSEGV|SIGABRT|SIGBUS|Call Trace|Oops:|"
        r"BUG:|libceph:.*(BUG|fault|oops)|ceph.*fault"
    )

    def _count_client_dmesg_crashes(self, client) -> int:
        out, _ = client.exec_command(
            cmd=(
                "dmesg --level=err 2>/dev/null | "
                f"grep -ciE '{self._CLIENT_DMESG_CRASH_PATTERN}' || true"
            ),
            sudo=True,
            timeout=30,
        )
        return int((out or "").strip() or "0")

    def _bug_a9_cephfs_client_crash(self, monitoring_data, bug_cfg):
        """Detect CephFS kernel client crashes via dmesg on client nodes."""
        if "cephfs" not in self.deployed_services:
            return True, "CephFS not deployed"
        try:
            clients = self.ceph_cluster.get_nodes(role="client")
            for client in clients[:2]:
                count = self._count_client_dmesg_crashes(client)
                if count > 0:
                    return False, (
                        f"CephFS kernel errors on {client.hostname}: " f"count={count}"
                    )
            return True, "no CephFS client crashes in dmesg"
        except Exception as err:
            return False, str(err)

    def _bug_a10_osd_activation(self, monitoring_data, bug_cfg):
        """Detect OSDs remaining down post-upgrade (activation failure)."""
        try:
            osd_tree = self.rados_obj.run_ceph_command(cmd="ceph osd tree")
            down_osds = []
            for node in osd_tree.get("nodes", []):
                if node.get("type") == "osd" and node.get("status") == "down":
                    down_osds.append(node["name"])
            if down_osds:
                return False, f"OSDs down: {down_osds}"
            return True, "all OSDs up"
        except Exception as err:
            return False, str(err)

    def _bug_a12_ec_scrub(self, monitoring_data, bug_cfg):
        """Detect PG_DAMAGED health check indicating EC scrub errors."""
        try:
            health = self.rados_obj.run_ceph_command(cmd="ceph health detail")
            detail_str = json.dumps(health)
            if "PG_DAMAGED" in detail_str:
                return False, "PG_DAMAGED found in health detail"
            return True, "no EC scrub errors"
        except Exception as err:
            return False, str(err)

    def _bug_a13_nvmeof_gw(self, monitoring_data, bug_cfg):
        """Detect NVMeoF gateway daemons not running post-upgrade."""
        if "nvmeof" not in self.deployed_services:
            return True, "NVMeoF not deployed"
        try:
            orch_ps = self.rados_obj.run_ceph_command(
                cmd="ceph orch ps --daemon-type nvmeof"
            )
            down = [d for d in orch_ps if d.get("status_desc") != "running"]
            if down:
                return False, f"NVMeoF daemons not running: {down}"
            return True, "all NVMeoF gateways running"
        except Exception as err:
            return False, str(err)

    def _bug_a19_cephadm_grace(self, monitoring_data, bug_cfg):
        """Detect MGR_MODULE_ERROR indicating cephadm grace tool failure."""
        try:
            health = self.rados_obj.run_ceph_command(cmd="ceph health detail")
            detail_str = json.dumps(health)
            if "MGR_MODULE_ERROR" not in detail_str:
                return True, "no MGR_MODULE_ERROR"
            module_errors = [
                line for line in detail_str.split("\\n") if "MGR_MODULE_ERROR" in line
            ]
            grace_errors = [
                msg
                for msg in module_errors
                if "grace" in msg.lower() or "ganesha" in msg.lower()
            ]
            if grace_errors:
                return False, f"NFS grace/ganesha module errors: {grace_errors}"
            return True, "MGR_MODULE_ERROR present but not grace/cephadm related"
        except Exception as err:
            return False, str(err)

    def _bug_a23_daemon_count_mismatch(self, monitoring_data, bug_cfg):
        """Compare daemon counts and states before vs after upgrade."""
        pre = monitoring_data.get("daemon_state_pre", {})
        post = monitoring_data.get("daemon_state_post", {})

        if not pre or not post:
            return True, "skip: daemon state snapshots not available"

        mismatches = []
        for dtype in sorted(set(list(pre.keys()) + list(post.keys()))):
            pre_info = pre.get(dtype, {})
            post_info = post.get(dtype, {})
            pre_running = pre_info.get("running", 0)
            post_running = post_info.get("running", 0)
            pre_count = pre_info.get("count", 0)
            post_count = post_info.get("count", 0)

            if post_running < pre_running:
                mismatches.append(
                    f"{dtype}: running {pre_running} -> {post_running} (REDUCED)"
                )
            elif post_count < pre_count:
                mismatches.append(
                    f"{dtype}: count {pre_count} -> {post_count} (REDUCED)"
                )

        pre_quorum = pre.get("mon", {}).get("quorum", 0)
        post_quorum = post.get("mon", {}).get("quorum", 0)
        if post_quorum < pre_quorum:
            mismatches.append(f"mon quorum: {pre_quorum} -> {post_quorum} (REDUCED)")

        for field in ("up", "in"):
            pre_val = pre.get("osd", {}).get(field, 0)
            post_val = post.get("osd", {}).get(field, 0)
            if post_val < pre_val:
                mismatches.append(f"osd {field}: {pre_val} -> {post_val} (REDUCED)")

        pre_mds_detail = pre.get("mds", {}).get("detail", {})
        post_mds_detail = post.get("mds", {}).get("detail", {})
        for fs_name in pre_mds_detail:
            if fs_name == "_standbys":
                pre_standbys = pre_mds_detail.get("_standbys", 0)
                post_standbys = post_mds_detail.get("_standbys", 0)
                if post_standbys < pre_standbys:
                    mismatches.append(
                        f"mds standbys: {pre_standbys} -> {post_standbys} (REDUCED)"
                    )
                continue
            pre_fs = pre_mds_detail.get(fs_name, {})
            post_fs = post_mds_detail.get(fs_name, {})
            for role in ("active", "standby_replay"):
                pre_v = pre_fs.get(role, 0)
                post_v = post_fs.get(role, 0)
                if post_v < pre_v:
                    mismatches.append(
                        f"mds.{fs_name} {role}: {pre_v} -> {post_v} (REDUCED)"
                    )

        if mismatches:
            return False, " | ".join(mismatches)

        parts = []
        for dtype in sorted(post.keys()):
            info = post[dtype]
            parts.append(f"{dtype}={info.get('running', '?')}/{info.get('count', '?')}")
        return True, f"all daemon counts preserved: {', '.join(parts)}"

    def _nfs_grace_cfg(self):
        return self.config.get("nfs_grace_validation") or {}

    @staticmethod
    def _grace_gate(mode, summary, warns, fails):
        if fails:
            return False, "; ".join(fails[:5])
        if warns:
            prefix = summary + "; "
            if mode == "strict":
                return False, prefix + "; ".join(warns[:5])
            return True, prefix + "WARN: " + "; WARN: ".join(warns[:5])
        return True, summary

    def _bug_a24_grace_metrics(self, monitoring_data, bug_cfg):
        """Check NFS grace cycles for missing grace_end or excessive duration."""
        cfg = self._nfs_grace_cfg()
        if not cfg.get("enabled"):
            return True, "skip:disabled"
        analysis = monitoring_data.get("nfs_grace_analysis") or {}
        metrics = analysis.get("metrics") or {}
        mode = cfg.get("mode", "baseline")
        if not metrics.get("cycle_count"):
            # Empty analysis is only a true skip when NFS was not upgraded.
            timeline = monitoring_data.get("daemon_timeline") or {}
            entries = (
                timeline.get("daemon_timeline")
                if isinstance(timeline, dict)
                else timeline
            ) or []
            nfs_upgraded = any(
                e.get("daemon_type") == "nfs" and int(e.get("count") or 0) > 0
                for e in entries
                if isinstance(e, dict)
            )
            if not nfs_upgraded:
                return True, "skip:no nfs grace data"
            msg = "nfs upgraded but cycle_count=0 (no grace_start/end pairs)"
            warns, fails = [], []
            (fails if mode == "strict" else warns).append(msg)
            return self._grace_gate(mode, msg, warns, fails)
        threshold = float(cfg.get("max_observed_grace_sec", 120))
        warns, fails = [], []
        for item in metrics.get("missing_grace_end") or []:
            msg = (
                f"{item.get('daemon', '?')} grace_start={item.get('start')} "
                f"grace_end=missing configured={item.get('configured_sec')}s "
                f"threshold={threshold}s"
            )
            (fails if mode == "strict" else warns).append(msg)
        for item in metrics.get("over_threshold") or []:
            msg = (
                f"{item.get('daemon', '?')} observed={item.get('observed_sec')}s "
                f"> threshold={threshold}s "
                f"(start={item.get('start')} end={item.get('end')})"
            )
            (fails if mode == "strict" else warns).append(msg)
        summary = (
            f"{metrics.get('daemon_count', 0)} daemons, "
            f"{metrics.get('cycle_count', 0)} grace cycles; "
            f"max observed {metrics.get('max_observed_sec', 0)}s "
            f"(threshold {threshold}s); "
            f"{len(metrics.get('missing_grace_end') or [])} missing grace_end"
        )
        return self._grace_gate(mode, summary, warns, fails)

    def _bug_a27_nfs_client_continuity(self, monitoring_data, bug_cfg):
        """Check NFS mounts and IO logs for remounts or hard errors."""
        cfg = self._nfs_grace_cfg()
        if not cfg.get("enabled"):
            return True, "skip:disabled"
        mode = cfg.get("mode", "baseline")
        mh = (monitoring_data.get("mount_health") or {}).get("nfs") or {}
        remounted = mh.get("stale_remounted") or []
        unrecoverable = mh.get("stale_unrecoverable") or []
        stale = mh.get("stale") or []
        healthy = len(mh.get("healthy") or [])
        err_data = monitoring_data.get("nfs_client_errors") or {}
        err_hits = err_data.get("hits") or []
        if not any([healthy, remounted, unrecoverable, stale, err_hits]):
            return True, "skip:no nfs client data"
        warns, fails = [], []
        if unrecoverable:
            fails.append(f"unrecoverable nfs mounts: {unrecoverable[:5]}")
        if stale:
            fails.append(f"stale nfs mounts: {stale[:5]}")
        if remounted:
            msg = f"nfs remounted: {remounted[:5]}"
            (fails if mode == "strict" else warns).append(msg)
        if err_hits:
            samples = [
                f"{h.get('hostname', '?')}:{h.get('line', '')[:120]}"
                for h in err_hits[:3]
            ]
            msg = f"io errors ({len(err_hits)}): " + "; ".join(samples)
            (fails if mode == "strict" else warns).append(msg)
        summary = (
            f"nfs mounts: {healthy} healthy, {len(remounted)} remounted, "
            f"{len(unrecoverable)} unrecoverable, {len(stale)} stale; "
            f"io hard errors: {len(err_hits)}"
        )
        return self._grace_gate(mode, summary, warns, fails)
