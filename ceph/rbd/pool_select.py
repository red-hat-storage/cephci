"""Helpers to choose replicated vs EC pool type for RBD/mirror tests."""

import random

from utility.log import Log

log = Log(__name__)

VALID_POOL_TYPES = ("rep_pool_config", "ec_pool_config")


def select_pool_type(config=None, available=None, *, apply_pool_only=False):
    """Select replicated or EC pool type for an RBD/mirror test.

    Suite config contract:
      - Default: randomly choose from ``available`` (or both valid types).
      - ``no_random: true`` requires ``pool_type: rep_pool_config|ec_pool_config``
        and runs only that pool type.

    Args:
        config: Test config dict (mutated when ``apply_pool_only`` is set).
        available: Iterable of pool type keys to choose from.
        apply_pool_only: If True, set ``rep-pool-only`` / ``ec-pool-only`` on
            config so only the selected pool is created during setup.

    Returns:
        str: Selected pool type key.
    """
    config = config if config is not None else {}
    available = list(available) if available is not None else list(VALID_POOL_TYPES)
    if not available:
        raise ValueError("No pool types available for selection")

    no_random = bool(config.get("no_random"))
    requested = config.get("pool_type")

    if no_random:
        if requested not in VALID_POOL_TYPES:
            raise ValueError(
                "no_random=true requires config.pool_type to be "
                "'rep_pool_config' or 'ec_pool_config'"
            )
        if requested not in available:
            raise ValueError(
                f"Requested pool_type '{requested}' is not available; "
                f"available={available}"
            )
        pool_type = requested
    else:
        pool_type = random.choice(available)

    kind = "replicated" if pool_type == "rep_pool_config" else "EC"
    log.info(f"Executing test on {kind} pool ({pool_type})")

    if apply_pool_only:
        if pool_type == "rep_pool_config":
            config["rep-pool-only"] = True
            config.pop("ec-pool-only", None)
        else:
            config["ec-pool-only"] = True
            config.pop("rep-pool-only", None)

    return pool_type


def select_pool_entry(
    config, pool_entries, *, pool_type_index=1, apply_pool_only=False
):
    """Select a pool entry tuple using :func:`select_pool_type`.

    Args:
        config: Test config dict.
        pool_entries: Sequence of tuples that include a pool type key.
        pool_type_index: Index of the pool type string inside each entry.
        apply_pool_only: Forwarded to :func:`select_pool_type`.

    Returns:
        The matching entry tuple for the selected pool type.
    """
    if not pool_entries:
        raise ValueError("pool_entries must not be empty")
    available = [entry[pool_type_index] for entry in pool_entries]
    pool_type = select_pool_type(
        config, available=available, apply_pool_only=apply_pool_only
    )
    for entry in pool_entries:
        if entry[pool_type_index] == pool_type:
            return entry
    raise ValueError(f"No pool entry found for pool_type={pool_type}")
