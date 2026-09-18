"""
CephX auth_allowed_ciphers consistency across upgrade (9x → 9x).

Rule (fail anything else):
  - If aes256k is present before upgrade → it must still be present after.
  - If aes256k is absent before upgrade → it must still be absent after.
  - The full auth_allowed_ciphers set must be unchanged across upgrade.

Suite config:
  operation: before_upgrade | after_upgrade
  state_file: path on installer (default /tmp/cephci_auth_allowed_ciphers.json)
  cipher: name to track for presence (default aes256k)
"""

from __future__ import annotations

import base64
import json

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)

DEFAULT_STATE_FILE = "/tmp/cephci_auth_allowed_ciphers.json"
DEFAULT_CIPHER = "aes256k"


def _run_mon_dump(installer):
    out, err = installer.exec_command(
        sudo=True,
        cmd="cephadm shell -- ceph mon dump --format json",
        check_ec=False,
    )
    raw = (out or "").strip() or (err or "").strip()
    if installer.exit_status != 0:
        raise OperationFailedError(
            f"ceph mon dump --format json failed (rc={installer.exit_status}): "
            f"{raw or '<empty>'}"
        )
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise OperationFailedError(
            f"Failed to parse mon dump JSON: {exc}; raw={raw[:500]!r}"
        ) from exc


def _cipher_names(mon_dump):
    """Return frozenset of cipher names, or None if field is absent."""
    raw = mon_dump.get("auth_allowed_ciphers")
    if raw is None:
        return None
    names = set()
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                name = item.get("name") or item.get("type")
                if name:
                    names.add(str(name).strip().lower())
            else:
                names.add(str(item).strip().lower())
    elif isinstance(raw, str):
        names = {p.strip().lower() for p in raw.split(",") if p.strip()}
    else:
        raise OperationFailedError(
            f"Unexpected auth_allowed_ciphers type {type(raw).__name__}: {raw!r}"
        )
    return frozenset(names)


def _snapshot(installer, cipher):
    names = _cipher_names(_run_mon_dump(installer))
    return {
        "ciphers": None if names is None else sorted(names),
        "field_present": names is not None,
        "cipher": cipher,
        "cipher_present": None if names is None else (cipher in names),
    }


def _write_state(installer, path, state):
    payload = json.dumps(state, sort_keys=True)
    b64 = base64.b64encode(payload.encode()).decode()
    installer.exec_command(
        sudo=True,
        cmd=f"echo {b64} | base64 -d > {path}",
    )
    log.info("Saved pre-upgrade auth_allowed_ciphers state to %s: %s", path, state)


def _read_state(installer, path):
    out, _ = installer.exec_command(sudo=True, cmd=f"cat {path}", check_ec=False)
    if installer.exit_status != 0 or not (out or "").strip():
        raise OperationFailedError(
            f"Missing pre-upgrade cipher state at {path}. "
            "Run operation: before_upgrade before after_upgrade."
        )
    return json.loads(out.strip())


def _before_upgrade(installer, state_file, cipher):
    state = _snapshot(installer, cipher)
    _write_state(installer, state_file, state)
    log.info(
        "before_upgrade: field_present=%s ciphers=%s %s_present=%s",
        state["field_present"],
        state["ciphers"],
        cipher,
        state["cipher_present"],
    )
    return 0


def _after_upgrade(installer, state_file, cipher):
    before = _read_state(installer, state_file)
    tracked = (before.get("cipher") or cipher).lower()
    after = _snapshot(installer, tracked)

    log.info("before_upgrade state: %s", before)
    log.info("after_upgrade state:  %s", after)

    errors = []

    if before["field_present"] != after["field_present"]:
        errors.append(
            "auth_allowed_ciphers field presence changed: "
            f"before={before['field_present']} after={after['field_present']}"
        )

    before_present = before.get("cipher_present")
    after_present = after.get("cipher_present")
    if before_present is True and after_present is not True:
        errors.append(
            f"{tracked} was present before upgrade but missing after upgrade"
        )
    if before_present is False and after_present is not False:
        errors.append(
            f"{tracked} was absent before upgrade but present after upgrade"
        )

    if before.get("ciphers") != after.get("ciphers"):
        errors.append(
            "auth_allowed_ciphers set changed across upgrade: "
            f"before={before.get('ciphers')} after={after.get('ciphers')}"
        )

    if errors:
        raise OperationFailedError(
            "CephX auth_allowed_ciphers inconsistent across upgrade:\n- "
            + "\n- ".join(errors)
        )

    log.info(
        "TEST PASSED - auth_allowed_ciphers unchanged across upgrade "
        "(field_present=%s ciphers=%s %s_present=%s)",
        after["field_present"],
        after["ciphers"],
        tracked,
        after["cipher_present"],
    )
    return 0


def run(ceph_cluster, **kw):
    config = kw.get("config", {}) or {}
    operation = (config.get("operation") or "").strip().lower()
    state_file = config.get("state_file", DEFAULT_STATE_FILE)
    cipher = str(config.get("cipher", DEFAULT_CIPHER)).strip().lower() or DEFAULT_CIPHER

    if operation not in ("before_upgrade", "after_upgrade"):
        raise ConfigError(
            "config.operation must be before_upgrade or after_upgrade, "
            f"got {operation!r}"
        )

    installer = ceph_cluster.get_nodes("installer")[0]
    try:
        if operation == "before_upgrade":
            return _before_upgrade(installer, state_file, cipher)
        return _after_upgrade(installer, state_file, cipher)
    except Exception as exc:
        log.error("TEST FAILED - %s", exc, exc_info=True)
        return 1
