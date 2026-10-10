"""
Post-upgrade CephX key rotation remediation.

When AUTH_INSECURE_* health checks are present after upgrade, rotate insecure
keys, push keyrings, lock down ciphers, verify health JSON, and confirm IO
continuity until the cluster returns to HEALTH_OK.

Reusable from upgrade thrashing and other suites via remediate_cephx_auth_warnings().
"""

import re
import time
from datetime import datetime, timezone

from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from cli.cephadm.cephadm import CephAdm
from utility.log import Log

log = Log(__name__)

AUTH_TARGET_CHECKS = (
    "AUTH_INSECURE_CLIENT_KEY_TYPE",
    "AUTH_INSECURE_KEYS_ALLOWED",
    "AUTH_INSECURE_KEYS_CREATABLE",
    "AUTH_INSECURE_ROTATING_SERVICE_KEY_TYPE",
)

DEFAULT_CEPHX_CONFIG = {
    "enabled": True,
    "key_type": "aes256k",
    "rotate_admin": True,
    "lock_down_ciphers": True,
    "wipe_rotating_service_keys": True,
    "io_recovery_sec": 300,
    "io_recovery_poll_sec": 15,
    "lockdown_verify_sec": 60,
    "lockdown_verify_poll_sec": 15,
    "timeout_sec": 7200,
    "wait_upgrade_complete_sec": 1800,
    # When False, IO continuity failures are logged/reported but do not fail remediation.
    "fail_on_io_continuity": True,
}


def _merge_cephx_config(config):
    cfg = dict(DEFAULT_CEPHX_CONFIG)
    if config:
        cfg.update(config.get("cephx_key_rotation") or {})
    return cfg


def cephx_remediation_enabled(config):
    """Return True if remediation is enabled in suite config."""
    return bool(_merge_cephx_config(config).get("enabled", True))


def cephx_failure_reasons(cephx_result):
    """Build failure_reason strings for emergency report when remediation fails."""
    if not cephx_result or cephx_result.get("status") != "failed":
        return []
    errors = cephx_result.get("errors") or ["unknown error"]
    return [f"CephX remediation failed: {err}" for err in errors]


def remediate_cephx_auth_warnings(
    cephadm_obj,
    rados_obj,
    ceph_cluster,
    io_mgr=None,
    clients=None,
    deployed_services=None,
    config=None,
    stats=None,
):
    """Run full CephX remediation; return result dict."""
    rem = CephXRotationRemediator(
        cephadm_obj, ceph_cluster, config, rados_obj=rados_obj
    )
    return rem.run(
        io_mgr=io_mgr,
        clients=clients,
        deployed_services=deployed_services or set(),
        stats=stats,
    )


class CephXRotationRemediator:
    def __init__(self, cephadm_obj, ceph_cluster, config=None, rados_obj=None):
        self.cephadm_obj = cephadm_obj
        self.ceph_cluster = ceph_cluster
        self.rados_obj = rados_obj
        self.config = config or {}
        self.cfg = _merge_cephx_config(self.config)
        self.installer = getattr(cephadm_obj, "installer", None)
        if self.installer is None:
            installers = ceph_cluster.get_nodes(role="installer")
            self.installer = installers[0] if installers else None
        self._adm = CephAdm(self.installer) if self.installer else None
        self._orch = Orch(ceph_cluster, **self.config)
        self.errors = []
        self.entities_rotated = []
        self._deadline = None

    def _json_cmd(self, cmd, timeout=300):
        return self.rados_obj.run_ceph_command(cmd=cmd, timeout=timeout)

    def _shell(self, args, check=True):
        out, err = self.cephadm_obj.shell(args=args)
        text = (out or err or "").strip()
        if check and err and "Error" in err and not out:
            raise RuntimeError(err.strip())
        return text

    def run(
        self,
        io_mgr=None,
        clients=None,
        deployed_services=None,
        stats=None,
    ):
        started = time.time()
        self._deadline = started + int(self.cfg["timeout_sec"])
        result = {
            "status": "failed",
            "warnings_before": [],
            "warnings_after": [],
            "entities_rotated": [],
            "duration_sec": 0,
            "errors": [],
            "io_continuity": {},
        }
        clients = clients or self.ceph_cluster.get_nodes(role="client")
        deployed_services = deployed_services or set()
        if not self.rados_obj:
            raise RuntimeError("rados_obj is required for CephX remediation")
        if not self.cfg.get("enabled", True):
            result["status"] = "skipped"
            return self._finish(result, started)

        if stats:
            stats.tag_phase_boundary(
                "cephx_remediation_start", datetime.now(timezone.utc).isoformat()
            )

        io_continuity = bool(io_mgr)

        try:
            self._wait_for_upgrade_complete()

            checks = self._health_checks()
            result["warnings_before"] = sorted(
                k for k in AUTH_TARGET_CHECKS if k in checks
            )
            if not result["warnings_before"]:
                result["status"] = "skipped"
                return self._finish(result, started)

            if not self._adm:
                raise RuntimeError(
                    "CephX remediation requires an installer node with CephAdm"
                )

            bg_snapshot = (
                io_mgr.capture_background_pid_snapshot() if io_continuity else None
            )

            if self._needs_entity_rotation(checks):
                self._step_preferred_cipher()
                self._rotate_entities(self._parse_insecure_client_entities(checks))
                self._step_rotating_service_keys(checks)
                self._step_admin(checks, clients)

            self._rotate_stragglers()
            if not self._step_pre_lockdown_verify():
                return self._finish_failed(result, started)

            if self.cfg.get("lock_down_ciphers"):
                self._step_lockdown()

            self._rotate_stragglers()
            if not self._step_final_auth_verify():
                return self._finish_failed(result, started)

            if io_continuity:
                io_result = io_mgr.wait_for_background_io_continuity(
                    clients,
                    deployed_services,
                    deadline_sec=int(self.cfg["io_recovery_sec"]),
                    interval=int(self.cfg["io_recovery_poll_sec"]),
                    bg_snapshot=bg_snapshot,
                )
                result["io_continuity"] = io_result
                if not io_result.get("ok"):
                    dead = io_result.get("dead_pids") or []
                    msg = f"IO continuity check failed: dead_pids={dead}"
                    if self.cfg.get("fail_on_io_continuity", True):
                        self.errors.append("IO continuity check failed")
                        return self._finish_failed(result, started)
                    log.warning("%s (fail_on_io_continuity=false; continuing)", msg)

            result["status"] = "success"
            if stats:
                stats.tag_phase_boundary(
                    "cephx_remediation_end", datetime.now(timezone.utc).isoformat()
                )
        except Exception as exc:
            log.error("CephX remediation failed: %s", exc)
            self.errors.append(str(exc))
            result["errors"] = list(self.errors)

        # After a failed admin rotate the host keyring may still be stale —
        # never let the post-mortem health probe escalate into a hard crash.
        try:
            result["warnings_after"] = sorted(
                k for k in AUTH_TARGET_CHECKS if k in self._health_checks()
            )
        except Exception as exc:
            log.warning("CephX remediation: could not collect warnings_after: %s", exc)
            result["warnings_after"] = []
        return self._finish(result, started)

    def _finish_failed(self, result, started):
        result["errors"] = list(self.errors)
        return self._finish(result, started)

    def _finish(self, result, started):
        result["duration_sec"] = round(time.time() - started, 1)
        result["entities_rotated"] = list(self.entities_rotated)
        self.config["cephx_remediation_result"] = result
        log.info(
            "CephX remediation %s in %.1fs (rotated=%d, errors=%d)",
            result["status"],
            result["duration_sec"],
            len(result["entities_rotated"]),
            len(result.get("errors") or []),
        )
        return result

    def _check_timeout(self):
        if self._deadline and time.time() > self._deadline:
            raise TimeoutError(
                f"CephX remediation exceeded timeout_sec={self.cfg['timeout_sec']}"
            )

    def _health_checks(self):
        data = self._json_cmd("ceph health detail", timeout=60)
        return data.get("checks", {})

    def _parse_insecure_client_entities(self, checks):
        detail = checks.get("AUTH_INSECURE_CLIENT_KEY_TYPE", {}).get("detail", [])
        entities = []
        for item in detail:
            msg = item.get("message", "") if isinstance(item, dict) else str(item)
            match = re.search(r"entity (client\.\S+)", msg)
            if match:
                entities.append(match.group(1))
        return entities

    def _insecure_non_admin_clients(self, checks):
        return [
            e
            for e in self._parse_insecure_client_entities(checks)
            if e != "client.admin"
        ]

    def _auth_check_violations(self, checks):
        """AUTH checks that still block success for the YAML-enabled scope."""
        remaining = []
        for key in AUTH_TARGET_CHECKS:
            if key not in checks:
                continue
            if key == "AUTH_INSECURE_CLIENT_KEY_TYPE":
                if not self.cfg.get("rotate_admin", True):
                    non_admin = self._insecure_non_admin_clients(checks)
                    if not non_admin:
                        continue
            elif key in (
                "AUTH_INSECURE_KEYS_ALLOWED",
                "AUTH_INSECURE_KEYS_CREATABLE",
            ):
                if not self.cfg.get("lock_down_ciphers", True):
                    continue
            elif key == "AUTH_INSECURE_ROTATING_SERVICE_KEY_TYPE":
                if not self.cfg.get("wipe_rotating_service_keys", True):
                    continue
            remaining.append(key)
        return remaining

    def _needs_entity_rotation(self, checks):
        return "AUTH_INSECURE_CLIENT_KEY_TYPE" in checks or (
            "AUTH_INSECURE_ROTATING_SERVICE_KEY_TYPE" in checks
        )

    def _wait_for_upgrade_complete(self):
        wait_sec = int(self.cfg.get("wait_upgrade_complete_sec", 0))
        if wait_sec <= 0:
            return
        deadline = time.time() + wait_sec
        while time.time() < deadline:
            self._check_timeout()
            status = self._orch.upgrade_status(timeout=60)
            if not status.get("in_progress"):
                return
            log.info(
                "CephX remediation: waiting for upgrade complete (%s) ...",
                status.get("progress"),
            )
            time.sleep(30)
        log.warning(
            "CephX remediation: upgrade still in_progress after %ss; "
            "continuing with straggler re-scan",
            wait_sec,
        )

    @staticmethod
    def _cipher_name(val):
        """Normalize mon dump cipher fields to a comparable name string."""
        if val is None:
            return None
        if isinstance(val, str):
            return val
        if isinstance(val, dict):
            return val.get("name")
        if isinstance(val, list):
            names = []
            for item in val:
                if isinstance(item, dict) and item.get("name"):
                    names.append(item["name"])
                elif isinstance(item, str):
                    names.append(item)
            if len(names) == 1:
                return names[0]
            return ",".join(names) if names else None
        return str(val)

    def _mon_auth_settings(self):
        data = self._json_cmd("ceph mon dump", timeout=60) or {}
        return {
            "auth_preferred_cipher": self._cipher_name(
                data.get("auth_preferred_cipher")
            ),
            "auth_service_cipher": self._cipher_name(data.get("auth_service_cipher")),
            "auth_allowed_ciphers": self._cipher_name(data.get("auth_allowed_ciphers")),
        }

    def _ensure_mon_cipher(self, field):
        key_type = self.cfg["key_type"]
        if self._mon_auth_settings().get(field) == key_type:
            log.info("CephX remediation: %s already %s", field, key_type)
            return
        log.info("CephX remediation: set %s %s", field, key_type)
        self._shell(["ceph", "mon", "set", field, key_type])

    def _step_preferred_cipher(self):
        self._ensure_mon_cipher("auth_preferred_cipher")

    def _nfs_cluster_id(self, entity):
        match = re.match(r"client\.nfs\.([^.\s]+)", entity)
        return match.group(1) if match else None

    def _rotate_nfs_clusters(self, entities):
        by_cluster = {}
        for entity in entities:
            cluster_id = self._nfs_cluster_id(entity)
            if cluster_id:
                by_cluster.setdefault(cluster_id, set()).add(entity)
        if not by_cluster:
            return set()
        handled = set()
        clusters = sorted(by_cluster)
        total = len(clusters)
        for idx, cluster_id in enumerate(clusters, 1):
            self._check_timeout()
            log.info(
                "CephX remediation: rotating NFS cluster %s (%d/%d)",
                cluster_id,
                idx,
                total,
            )
            resp = self._adm.ceph.nfs.cluster.rotate_key(
                cluster_id, key_type=self.cfg["key_type"]
            )
            if not resp.get("rotated"):
                raise RuntimeError(
                    f"NFS rotate-key {cluster_id}: empty rotated list: {resp}"
                )
            if not resp.get("service_redeployed"):
                raise RuntimeError(
                    f"NFS rotate-key {cluster_id}: service_redeployed is false: {resp}"
                )
            self.entities_rotated.append(f"nfs:{cluster_id}")
            handled.update(by_cluster[cluster_id])
        return handled

    @staticmethod
    def _as_keyring(text, entity):
        """Keep from first section header so cephadm stderr noise cannot corrupt the file.

        Always ends with a newline: ceph keyring parse rejects files without a
        trailing \\n (Malformed input [buffer:3]).
        """
        text = (text or "").strip()
        marker = f"[{entity}]"
        idx = text.find(marker)
        if idx < 0:
            idx = text.find("[")
        if idx < 0:
            raise RuntimeError(f"empty keyring content for {entity}")
        return text[idx:] + "\n"

    def _push_keyring(self, entity, path, nodes, content=None):
        # After client.admin rotate, auth get with the stale host keyring fails —
        # pass rotate stdout as content.
        if content is None:
            content = self._shell(["ceph", "auth", "get", entity])
        content = self._as_keyring(content, entity)
        if not nodes:
            raise RuntimeError(f"no nodes to push keyring for {entity}")

        def _write(node):
            def _put(dest):
                fh = node.remote_file(file_name=dest, file_mode="w", sudo=True)
                fh.write(content)
                fh.flush()
                fh.close()

            node.exec_command(sudo=True, cmd="mkdir -p /etc/ceph")
            _put(path)
            # cephadm shell maps FSID config/ceph.client.admin.keyring → ceph.keyring
            if entity != "client.admin":
                return
            cfg_out, _ = node.exec_command(
                sudo=True, cmd="ls -d /var/lib/ceph/*/config 2>/dev/null | head -1"
            )
            cfg_dir = (cfg_out or "").strip()
            if cfg_dir:
                _put(f"{cfg_dir}/ceph.client.admin.keyring")

        with parallel() as p:
            for node in nodes:
                p.spawn(_write, node)

    def _keyring_target_nodes(self):
        """Installer, clients, conf _admin roles, plus live orch _admin labels."""
        seen = set()
        nodes = []

        def _add(node):
            key = getattr(node, "hostname", id(node))
            if key in seen:
                return
            seen.add(key)
            nodes.append(node)

        for role in ("installer", "client", "_admin"):
            for node in self.ceph_cluster.get_nodes(role=role):
                _add(node)
        try:
            hosts = self._json_cmd("ceph orch host ls", timeout=60) or []
            admin_names = {
                h.get("hostname")
                for h in hosts
                if isinstance(h, dict) and "_admin" in (h.get("labels") or [])
            }
            for node in self.ceph_cluster.get_nodes():
                hn = getattr(node, "hostname", None)
                if not hn:
                    continue
                if hn in admin_names or hn.split(".")[0] in admin_names:
                    _add(node)
        except Exception as exc:
            log.warning(
                "CephX remediation: could not resolve orch _admin hosts: %s", exc
            )
        return nodes

    def _classify_entity(self, entity):
        if entity == "client.admin":
            return "admin"
        if entity.startswith("client.nfs."):
            return "nfs"
        if entity.startswith("client.rgw."):
            return "orch"
        if entity.startswith("client.crash.") or entity.startswith(
            "client.ceph-exporter."
        ):
            return "orch"
        if entity.startswith("client.bootstrap-"):
            return "auth"
        if re.match(r"client\.\d+$", entity):
            return "client_keyring"
        return "auth"

    def _rotate_entities(self, entities):
        entities = [e for e in entities if e != "client.admin"]
        if not entities:
            return

        nfs_handled = self._rotate_nfs_clusters(entities)
        client_nodes = self.ceph_cluster.get_nodes(role="client")
        key_type = self.cfg["key_type"]

        for entity in entities:
            self._check_timeout()
            if entity in nfs_handled:
                continue
            kind = self._classify_entity(entity)
            if kind == "nfs":
                continue
            rotated = self._shell(
                ["ceph", "auth", "rotate", entity, "--key-type", key_type]
            )
            self.entities_rotated.append(entity)
            if kind == "orch":
                self._shell(
                    [
                        "ceph",
                        "orch",
                        "daemon",
                        "reconfig",
                        entity.replace("client.", "", 1),
                    ]
                )
            elif kind == "client_keyring":
                self._push_keyring(
                    entity,
                    f"/etc/ceph/ceph.{entity}.keyring",
                    client_nodes,
                    content=rotated,
                )

    def _step_rotating_service_keys(self, checks):
        if "AUTH_INSECURE_ROTATING_SERVICE_KEY_TYPE" not in checks:
            return
        self._ensure_mon_cipher("auth_service_cipher")

        checks = self._health_checks()
        if "AUTH_INSECURE_ROTATING_SERVICE_KEY_TYPE" not in checks:
            return
        if not self.cfg.get("wipe_rotating_service_keys", True):
            log.info(
                "CephX remediation: wipe_rotating_service_keys=false; "
                "skipping rotating service key wipe"
            )
            return
        if "AUTH_INSECURE_SERVICE_TICKETS" in checks:
            raise RuntimeError(
                "Refusing wipe-rotating-service-keys while "
                "AUTH_INSECURE_SERVICE_TICKETS is present"
            )
        log.info("CephX remediation: ceph auth wipe-rotating-service-keys")
        self._shell(["ceph", "auth", "wipe-rotating-service-keys"])
        self.entities_rotated.append("wipe-rotating-service-keys")

    def _step_admin(self, checks, clients):
        if not self.cfg.get("rotate_admin", True):
            return
        entities = self._parse_insecure_client_entities(checks)
        if "client.admin" not in entities:
            log.info(
                "CephX remediation: client.admin not insecure; skipping admin rotate"
            )
            return

        backup_entity = "client.admin-backup"
        backup_path = f"/etc/ceph/ceph.{backup_entity}.keyring"
        key_type = self.cfg["key_type"]
        log.info("CephX remediation: backup %s before admin rotate", backup_entity)
        # rm + get-or-create + rotate: get-or-create alone can leave aes.
        try:
            self._shell(["ceph", "auth", "rm", backup_entity])
        except Exception:
            pass
        # shell() joins args with spaces; quote caps so * is not glob-expanded.
        self._shell(
            [
                "ceph",
                "auth",
                "get-or-create",
                backup_entity,
                "mon",
                "'allow *'",
                "osd",
                "'allow *'",
                "mds",
                "'allow *'",
                "mgr",
                "'allow *'",
            ]
        )
        backup = self._shell(
            [
                "ceph",
                "auth",
                "rotate",
                backup_entity,
                "--key-type",
                key_type,
            ]
        )
        self._push_keyring(backup_entity, backup_path, [self.installer], content=backup)
        # Host /etc/ceph is not mounted into the shell; pass --keyring in.
        try:
            self.cephadm_obj.shell(
                args=["ceph", "-s", "-n", backup_entity],
                base_cmd_args={"keyring": backup_path},
            )
        except Exception as exc:
            raise RuntimeError(
                f"{backup_entity} probe failed before admin rotate: {exc}"
            ) from exc

        log.info("CephX remediation: rotate client.admin")
        # Resolve targets before rotate: any CLI after rotate fails until keyrings
        # are on disk (stale host/FSID admin key).
        targets = self._keyring_target_nodes()
        rotated = self._shell(
            [
                "ceph",
                "auth",
                "rotate",
                "client.admin",
                "--key-type",
                key_type,
            ]
        )
        self.entities_rotated.append("client.admin")
        self._push_keyring(
            "client.admin",
            "/etc/ceph/ceph.client.admin.keyring",
            targets,
            content=rotated,
        )
        cfg_out, _ = self.installer.exec_command(
            sudo=True,
            cmd=(
                "ls /var/lib/ceph/*/config/ceph.client.admin.keyring "
                "2>/dev/null | head -1"
            ),
        )
        if not (cfg_out or "").strip():
            raise RuntimeError(
                "client.admin rotated but FSID config keyring missing on installer"
            )

        if not self._probe_admin_auth(clients[:1]):
            raise RuntimeError("client.admin auth probe failed after keyring push")

        try:
            self._adm.ceph.auth.rm(backup_entity)
            self.installer.exec_command(
                sudo=True, cmd=f"rm -f {backup_path}", timeout=30
            )
        except Exception as exc:
            log.warning("Failed to remove %s: %s", backup_entity, exc)

    def _probe_admin_auth(self, clients=None):
        try:
            self._adm.ceph.status()
        except Exception as exc:
            log.error("Admin auth probe failed on installer: %s", exc)
            return False
        for client in clients or []:
            try:
                client.exec_command(sudo=True, cmd="ceph -s", timeout=30)
            except Exception as exc:
                log.error(
                    "Admin auth probe failed on %s: %s",
                    getattr(client, "hostname", client),
                    exc,
                )
                return False
        return True

    def _rotate_stragglers(self):
        checks = self._health_checks()
        entities = self._parse_insecure_client_entities(checks)
        non_admin = [e for e in entities if e != "client.admin"]
        if not non_admin:
            return
        log.info(
            "CephX remediation: rotating %d straggler entity/entities",
            len(non_admin),
        )
        self._rotate_entities(entities)

    def _step_pre_lockdown_verify(self):
        checks = self._health_checks()
        pre_keys = {
            "AUTH_INSECURE_CLIENT_KEY_TYPE",
            "AUTH_INSECURE_ROTATING_SERVICE_KEY_TYPE",
        }
        remaining = [k for k in self._auth_check_violations(checks) if k in pre_keys]
        if remaining:
            for key in remaining:
                self.errors.append(f"pre-lockdown check still present: {key}")
            return False
        return True

    def _step_lockdown(self):
        self._ensure_mon_cipher("auth_allowed_ciphers")

    def _step_final_auth_verify(self):
        deadline = time.time() + int(self.cfg["lockdown_verify_sec"])
        poll = int(self.cfg["lockdown_verify_poll_sec"])
        while True:
            checks = self._health_checks()
            remaining = self._auth_check_violations(checks)
            if not remaining:
                return True
            if time.time() >= deadline:
                self.errors.append(f"AUTH warnings remain after lockdown: {remaining}")
                return False
            log.info(
                "CephX remediation: waiting for AUTH checks to clear: %s",
                remaining,
            )
            time.sleep(poll)
