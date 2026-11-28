"""
Enable cephadm agent via run.py ``--custom-config use_agent=true``.

Opt-in for all bootstrap entry points:

* ``CephAdmin.bootstrap`` (``test_bootstrap.py``) — merge into
  ``cephadm bootstrap --config`` so the mgr store is seeded before host add.
* ``cli.ops.cephadm.bootstrap`` (``test_cephadm_bootstrap.py``) — same via the
  CLI bootstrap ``config`` / ``file_type: ini`` shape.
* ``test_cephadm_ansible_bootstrap.py`` — ansible wrapper playbooks do not pass
  ``--config``; enable with ``ceph config set`` immediately after bootstrap.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from utility.log import Log
from utility.odf_defaults import overrides_enabled

LOG = Log(__name__)

# CLI override key from --custom-config use_agent=true
USE_AGENT_KEY = "use_agent"
# Ceph mgr option written into bootstrap --config / config set
USE_AGENT_CONF_KEY = "mgr/cephadm/use_agent"


def _parse_custom_config_list(custom_config: Optional[List[str]]) -> Dict[str, Any]:
    """Parse ``--custom-config key=value`` list into a dict (avoid utils import weight)."""
    if not custom_config:
        return {}
    return dict(item.split("=", 1) for item in custom_config if "=" in item)


def resolve_custom_overrides(
    custom_config: Optional[Union[List[str], Dict[str, Any]]] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Normalize overrides from either ``custom_config_dict`` or ``custom-config`` list.

    Precedence: explicit *overrides* dict wins over *custom_config*.
    """
    if overrides:
        return overrides
    if isinstance(custom_config, dict):
        return custom_config
    return _parse_custom_config_list(custom_config)


def use_agent_requested(
    custom_config: Optional[Union[List[str], Dict[str, Any]]] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> bool:
    """Return True when ``--custom-config use_agent=true`` (or equivalent) is set."""
    return overrides_enabled(
        resolve_custom_overrides(custom_config, overrides), USE_AGENT_KEY
    )


def apply_use_agent_to_bootstrap_config(
    args: Dict[str, Any],
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Merge ``mgr/cephadm/use_agent=true`` into CephAdmin bootstrap ``args["config"]``.

    Shape matches ``create_ceph_config_file`` / ``config.jinja``::

        config:
          mgr:
            mgr/cephadm/use_agent: true

    Suite keys under ``mgr`` win if ``use_agent`` is already present.
    Mutates and returns *args*. No-op when the custom-config flag is unset.
    """
    if not overrides_enabled(overrides, USE_AGENT_KEY):
        return args

    existing = args.get("config")
    if existing is not None and not isinstance(existing, dict):
        LOG.warning(
            "bootstrap args.config is not a dict (%s); skipping use_agent merge",
            type(existing),
        )
        return args

    existing = dict(existing) if existing else {}
    mgr = existing.get("mgr")
    if mgr is None:
        existing["mgr"] = {USE_AGENT_CONF_KEY: "true"}
    elif isinstance(mgr, dict):
        mgr = dict(mgr)
        mgr.setdefault(USE_AGENT_CONF_KEY, "true")
        existing["mgr"] = mgr
    else:
        LOG.warning(
            "bootstrap args.config.mgr is not a dict (%s); skipping use_agent merge",
            type(mgr),
        )
        return args

    args["config"] = existing
    LOG.info(
        "Merged %s=true into bootstrap --config "
        "(--custom-config %s=true)",
        USE_AGENT_CONF_KEY,
        USE_AGENT_KEY,
    )
    return args


def apply_use_agent_to_cli_bootstrap_config(
    bootstrap_config: Dict[str, Any],
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Merge use_agent into ``test_cephadm_bootstrap`` bootstrap ``config`` block.

    CLI path uses ``file_type: ini`` with a space-separated mgr string of
    ``key=value`` tokens (see suite mgr image config examples).
    Mutates and returns *bootstrap_config*.
    """
    if not overrides_enabled(overrides, USE_AGENT_KEY):
        return bootstrap_config

    token = f"{USE_AGENT_CONF_KEY}=true"
    cfg = bootstrap_config.get("config")
    if cfg is None:
        bootstrap_config["config"] = {"file_type": "ini", "mgr": token}
        LOG.info(
            "Added bootstrap config with %s (--custom-config %s=true)",
            token,
            USE_AGENT_KEY,
        )
        return bootstrap_config

    if not isinstance(cfg, dict):
        LOG.warning(
            "bootstrap config is not a dict (%s); skipping use_agent merge",
            type(cfg),
        )
        return bootstrap_config

    cfg.setdefault("file_type", "ini")
    mgr = cfg.get("mgr", "")
    if isinstance(mgr, dict):
        # Uncommon mapping form: flatten to CLI ini ``key=value`` tokens.
        parts = []
        for key, value in mgr.items():
            key_s = str(key)
            if value is None or "=" in key_s:
                parts.append(key_s)
            else:
                parts.append(f"{key_s}={value}")
        mgr = " ".join(parts)

    if not isinstance(mgr, str):
        LOG.warning(
            "bootstrap config.mgr is not a string (%s); skipping use_agent merge",
            type(mgr),
        )
        return bootstrap_config

    if "mgr/cephadm/use_agent=" not in mgr:
        cfg["mgr"] = f"{mgr} {token}".strip()
        LOG.info(
            "Appended %s to bootstrap config.mgr (--custom-config %s=true)",
            token,
            USE_AGENT_KEY,
        )
    return bootstrap_config


def enable_use_agent_post_bootstrap(
    installer,
    custom_config: Optional[Union[List[str], Dict[str, Any]]] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Set ``mgr/cephadm/use_agent true`` after bootstrap (ansible / fallback path).

    Returns True if the config set was issued, False if the flag was not requested.
    """
    if not use_agent_requested(custom_config, overrides):
        return False

    # Local import avoids circular import at module load (cli -> utility).
    from cli.cephadm.cephadm import CephAdm
    from cli.exceptions import OperationFailedError

    LOG.info(
        "Enabling %s after bootstrap (--custom-config %s=true)",
        USE_AGENT_CONF_KEY,
        USE_AGENT_KEY,
    )
    out, _ = CephAdm(installer).ceph.config.set(
        daemon="mgr", key=USE_AGENT_CONF_KEY, value="true"
    )
    if isinstance(out, str) and "Error" in out:
        raise OperationFailedError(f"Failed to enable {USE_AGENT_CONF_KEY}: {out}")
    return True
