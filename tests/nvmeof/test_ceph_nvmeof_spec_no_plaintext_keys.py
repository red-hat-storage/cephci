"""
IBMCEPH-16447 — NVMeoF service specs must not embed plaintext encryption/mTLS keys.

Exports applied nvmeof orch specs and fails if PEM private-key material or
inline `encryption_key` / client|server_key blobs are present.
"""

import re

from ceph.ceph import Ceph
from utility.log import Log

LOG = Log(__name__)

# Private-key / PEM markers that must not appear in exported nvmeof specs
_PEM_PRIVATE = re.compile(
    r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
    re.IGNORECASE,
)
_SENSITIVE_INLINE = re.compile(
    r"(?im)^\s*(encryption_key|server_key|client_key)\s*:\s*\|?\s*$"
)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    config = kwargs["config"]
    installer = ceph_cluster.get_nodes(role="installer")[0]
    service = config.get("nvmeof_service")  # optional: nvmeof.rbd.group1
    cmd = "ceph orch ls nvmeof --export"
    if service:
        cmd = f"ceph orch ls {service} --export"

    try:
        out, err = installer.exec_command(cmd=cmd, sudo=True)
        blob = f"{out or ''}\n{err or ''}"
        LOG.info("Exported nvmeof orch spec(s):\n%s", blob)

        if "service_type: nvmeof" not in blob and "service_type:nvmeof" not in blob:
            # No nvmeof service exported — treat as skip/soft pass unless forced
            if config.get("require_nvmeof_spec", True):
                raise RuntimeError(f"No nvmeof service found via: {cmd}")
            LOG.warning("No nvmeof export content; skipping plaintext-key assert")
            return 0

        violations = []
        if _PEM_PRIVATE.search(blob):
            violations.append("PEM private key material found in exported nvmeof spec")
        if _SENSITIVE_INLINE.search(blob) and "BEGIN" in blob.upper():
            violations.append(
                "Inline encryption_key/server_key/client_key block present in export"
            )
        # Single-line inline PEM / long base64 under sensitive keys (not a file path ref)
        if re.search(
            r"(?i)(encryption_key|server_key|client_key)\s*:\s*.*(BEGIN|MII[A-Za-z0-9+/=]{40,})",
            blob,
        ):
            violations.append(
                "encryption_key/server_key/client_key appears to contain inline key material"
            )
        # Reject obvious absolute key-file contents pasted as scalars (not path-only refs)
        if re.search(
            r"(?im)^\s*(encryption_key|server_key|client_key)\s*:\s*[\"']?[A-Za-z0-9+/=]{64,}[\"']?\s*$",
            blob,
        ):
            violations.append(
                "Sensitive key field looks like inline base64 material (not a path reference)"
            )

        if violations:
            raise RuntimeError(
                "IBMCEPH-16447 plaintext key check failed: " + "; ".join(violations)
            )
        LOG.info("No plaintext encryption/mTLS private keys in exported nvmeof specs")
        return 0
    except Exception as err:
        LOG.error(err)
        return 1
