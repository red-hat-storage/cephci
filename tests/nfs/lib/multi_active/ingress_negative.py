"""Helpers for NFS multi-active ingress negative orchestrator apply tests."""

import json
import uuid
from time import monotonic, sleep

import yaml

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from tests.nfs.lib.multi_active.constants import HEALTH_CHECK_INTERVAL, log_section
from utility.log import Log

log = Log(__name__)

_INFRA_FAILURE_MARKERS = (
    "can't open input file",
    "no such file or directory",
    "errno 2",
)


class IngressApplyResult:
    def __init__(self, exit_code, stdout, stderr):
        self.exit_code = exit_code
        self.stdout = stdout or ""
        self.stderr = stderr or ""

    @property
    def combined_output(self):
        return f"{self.stdout}\n{self.stderr}".strip()


class NfsMultiActiveIngressNegative:
    @staticmethod
    def build_base_ingress_spec(
        nfs_name,
        vip,
        ingress_hosts,
        frontend_port,
        monitor_port,
        backend_service=None,
    ):
        return {
            "service_type": "ingress",
            "service_id": f"nfs.{nfs_name}",
            "placement": {"hosts": list(ingress_hosts)},
            "spec": {
                "backend_service": backend_service or f"nfs.{nfs_name}",
                "frontend_port": frontend_port,
                "monitor_port": monitor_port,
                "virtual_ip": vip,
                "health_check_interval": HEALTH_CHECK_INTERVAL,
                "enable_haproxy_protocol": True,
            },
        }

    @staticmethod
    def _reject_infra_failure(result, case_id):
        output = result.combined_output.lower()
        for marker in _INFRA_FAILURE_MARKERS:
            if marker in output:
                raise OperationFailedError(
                    f"{case_id}: orch apply failed for infrastructure/setup reason, "
                    f"not ingress spec validation:\n{result.combined_output}"
                )

    @staticmethod
    def _log_apply_rejection(result, case_id, expected_errors):
        log.info(
            "%s: orch apply rejected (exit=%s)\n%s",
            case_id,
            result.exit_code,
            result.combined_output,
        )
        NfsMultiActiveIngressNegative._assert_expected_error_messages(
            result.combined_output,
            case_id,
            expected_errors,
        )

    @staticmethod
    def apply_specs(installer, specs, mount="/tmp"):
        """Apply orchestrator specs and return exit code plus captured output."""
        mount_dir = mount if mount.endswith("/") else f"{mount}/"
        remote_spec = f"{mount_dir}cephci_neg_{uuid.uuid4().hex}.yaml"
        spec_fp = None
        try:
            spec_fp = installer.remote_file(
                sudo=True, file_name=remote_spec, file_mode="wb"
            )
            spec_fp.write(
                yaml.dump_all(
                    specs,
                    sort_keys=False,
                    indent=2,
                    default_flow_style=False,
                ).encode("utf-8")
            )
            spec_fp.flush()
        finally:
            if spec_fp is not None:
                try:
                    spec_fp.close()
                except OSError:
                    pass

        log_section(log, "NEGATIVE ORCHESTRATOR SPEC TO APPLY")
        log.info("%s", yaml.dump_all(specs, sort_keys=False, default_flow_style=False))

        cephadm = CephAdm(installer, mount=mount_dir)
        cmd = f"{cephadm.base_shell_cmd} ceph orch apply -i {remote_spec}"
        try:
            result = installer.exec_command(
                sudo=True,
                cmd=cmd,
                check_ec=False,
                verbose=True,
            )
            if isinstance(result, tuple) and len(result) >= 3:
                stdout, stderr, exit_code = result[0], result[1], result[2]
            else:
                stdout, stderr, exit_code = result or "", "", 0
            return IngressApplyResult(exit_code, stdout, stderr)
        finally:
            installer.exec_command(
                sudo=True, cmd=f"rm -f {remote_spec}", check_ec=False
            )

    @staticmethod
    def _assert_expected_error_messages(output, case_id, patterns, source="orch apply"):
        """Require at least one expected error fragment in output."""
        if not patterns:
            return
        text = output or ""
        lowered = text.lower()
        if any(pattern.lower() in lowered for pattern in patterns):
            log.info(
                "%s: matched expected error message from %s",
                case_id,
                source,
            )
            return
        raise OperationFailedError(
            f"{case_id}: expected error message from {source}. "
            f"Expected one of {patterns!r}. Got:\n{text}"
        )

    @staticmethod
    def assert_apply_validation_failure(result, case_id, expected_errors=()):
        """Require orch apply to fail immediately with a validation-style error."""
        NfsMultiActiveIngressNegative._reject_infra_failure(result, case_id)
        if result.exit_code != 0:
            NfsMultiActiveIngressNegative._log_apply_rejection(
                result, case_id, expected_errors
            )
            return

        raise OperationFailedError(
            f"{case_id}: expected orch apply validation failure, got exit 0. "
            f"Output:\n{result.combined_output}"
        )

    @staticmethod
    def wait_ingress_unhealthy(installer, service_name, timeout=120, interval=10):
        """Return once ingress is absent or not fully healthy."""
        last_seen = None
        for _ in WaitUntil(timeout=timeout, interval=interval):
            raw = CephAdm(installer).ceph.orch.ls(format="json", service_type="ingress")
            services = json.loads(raw.strip()) if raw else []
            match = [svc for svc in services if svc.get("service_name") == service_name]
            if not match:
                log.info("%s not present in orch ls", service_name)
                return "absent"

            status = match[0].get("status") or {}
            last_seen = status
            running = status.get("running")
            size = status.get("size")
            if running != size or not running:
                log.info(
                    "%s unhealthy in orch ls (running=%s size=%s)",
                    service_name,
                    running,
                    size,
                )
                return "unhealthy"

        raise OperationFailedError(
            f"Timed out after {timeout}s waiting for {service_name} to be "
            f"absent or unhealthy (last status={last_seen!r})"
        )

    @staticmethod
    def assert_apply_failed_or_ingress_unhealthy(
        installer,
        result,
        service_name,
        case_id,
        timeout=120,
        expected_errors=(),
    ):
        """NEG-2: apply may fail or ingress may remain unhealthy."""
        NfsMultiActiveIngressNegative._reject_infra_failure(result, case_id)
        if result.exit_code != 0:
            NfsMultiActiveIngressNegative._log_apply_rejection(
                result, case_id, expected_errors
            )
            return

        state = NfsMultiActiveIngressNegative.wait_ingress_unhealthy(
            installer, service_name, timeout=timeout
        )
        log.info("%s: orch apply accepted; ingress state=%s", case_id, state)

    @staticmethod
    def remove_ingress_service(installer, service_name):
        log.info("Cleaning up ingress service %s", service_name)
        installer.exec_command(
            sudo=True,
            cmd=f"ceph orch rm {service_name} --force",
            check_ec=False,
        )
        wait_start = monotonic()
        while monotonic() - wait_start < 120:
            raw = CephAdm(installer).ceph.orch.ls(
                format="json", service_name=service_name
            )
            if not raw or "No services reported" in raw:
                return
            try:
                services = json.loads(raw.strip())
            except json.JSONDecodeError:
                sleep(5)
                continue
            if not services:
                return
            sleep(5)
