"""
Suite step: ensure CephX kernel-mount clients run a current RHEL kernel.

Runs after cluster upgrade on selected clients only (default C2/C3). Bootstrap
keeps the normal MTE path (``SKIP_SUBSCRIPTION=true``). At this step, if a
client is not registered, register it (cdn, then stage — same as install_prereq),
then ``enable_rhel_rpms`` for that ``VERSION_ID`` (CDN/EUS/AUS map), list/install
``latest`` or a pinned kernel NVR, and reboot if needed.

Intentionally does **not** ``subscription-manager unregister`` afterward: MTE
tears down / deletes the nodes when the TC finishes, so leftover registration
on these ephemeral clients is not cleaned up here.

Config::

    clients: [client.2, client.3]          # default
    kernel: latest                         # default; or e.g. 5.14.0-427.156.1.el9_4
    reboot_timeout: 900                    # seconds
    update_kernel: true                    # false → no-op (return 0)

With ``update_kernel: true`` (default), install/reboot only on RHEL 9.4 / 9.6
when the running kernel is below the CephX floor for that release; other
``VERSION_ID`` values skip. A pin that already matches ``uname -r`` skips
subscription, repos, install, and reboot. For ``latest`` (or a pin checked
after listing repos), install/reboot is skipped when current already equals
the resolved target.

Pinned NVRs that are not present in enabled repos fail this step (no skip).
"""

import shlex
import traceback

from cli.exceptions import OperationFailedError
from tests.cephfs.cephfs_upgrade.cephfs_cephx_key_helpers import (
    _remote_exit_status,
    client_by_entity,
)
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.misc_env.install_prereq import (
    ConfigNotFoundError,
    enable_rhel_rpms,
    setup_subscription_manager,
)
from utility.log import Log

log = Log(__name__)

_DEFAULT_CLIENTS = ["client.2", "client.3"]

# CephX client kernel floors (reviewer): upgrade only on these RHEL minors,
# and only when uname -r is older than the floor NVR.
_KERNEL_FLOORS = {
    "9.4": "5.14.0-427.131.1.el9_4",
    "9.6": "5.14.0-570.121.1.el9_6",
}


def _normalize_nvr(value: str) -> str:
    """Strip optional ``kernel-`` prefix and ``.x86_64`` suffix."""
    nvr = (value or "").strip()
    if nvr.startswith("kernel-"):
        nvr = nvr[len("kernel-") :]
    if nvr.endswith(".x86_64"):
        nvr = nvr[: -len(".x86_64")]
    return nvr


def _nvr_ge(client, left: str, right: str) -> bool:
    """True if ``left >= right`` using the same ``sort -V`` ordering as dnf NVRs."""
    a = _normalize_nvr(left)
    b = _normalize_nvr(right)
    if a == b:
        return True
    out, _ = client.exec_command(
        sudo=True,
        cmd=(
            f"printf '%s\\n%s\\n' {shlex.quote(a)} {shlex.quote(b)} "
            "| sort -V | tail -1"
        ),
        check_ec=False,
    )
    return (out or "").strip() == a


def _rhel_version_id(client) -> str:
    """Prefer node.distro_info; else parse /etc/os-release VERSION_ID."""
    info = getattr(getattr(client, "node", client), "distro_info", None) or {}
    ver = (info.get("VERSION_ID") or "").strip()
    if not ver:
        out, _ = client.exec_command(
            sudo=True,
            cmd="bash -lc '. /etc/os-release && printf %s \"$VERSION_ID\"'",
        )
        ver = (out or "").strip()
    if not ver or "." not in ver:
        raise OperationFailedError(
            f"Could not parse RHEL VERSION_ID on {client.node.hostname}: {ver!r}"
        )
    return ver


def _subscription_registered(client) -> bool:
    """True if subscription-manager sees a registered system (not MTE skip path)."""
    out, _ = client.exec_command(
        sudo=True,
        cmd="subscription-manager identity",
        check_ec=False,
        timeout=120,
    )
    if _remote_exit_status(client) != 0:
        return False
    # Unregistered often still prints help text; identity success is enough.
    text = (out or "").lower()
    if "not yet registered" in text or "this system is not registered" in text:
        return False
    return True


def _ensure_subscribed(client) -> None:
    """
    Register this client for CDN content if needed (post-upgrade kernel gate).

    Same cdn→stage fallback as install_prereq; scoped to this node only so the
    rest of the suite can keep SKIP_SUBSCRIPTION=true.

    No unregister: nodes are deleted after the TC, so registration is left as-is.
    """
    node = getattr(client, "node", client)
    if _subscription_registered(client):
        log.info("%s already subscription-manager registered", node.hostname)
        return
    log.info(
        "%s not registered; registering for kernel-gate CDN/EUS/AUS repos",
        node.hostname,
    )
    try:
        if setup_subscription_manager(node, "cdn"):
            return
        log.info("CDN register failed on %s; trying stage", node.hostname)
        if setup_subscription_manager(node, "stage"):
            return
    except ConfigNotFoundError as exc:
        raise OperationFailedError(
            f"Cannot register {node.hostname} for kernel repos: {exc}"
        ) from exc
    raise OperationFailedError(
        f"Failed to register {node.hostname} with CDN and stage "
        "(needed for enable_rhel_rpms / latest kernel)"
    )


def _enable_kernel_repos(client, version_id: str) -> None:
    """
    Register if needed, then enable CDN/EUS/AUS via ``enable_rhel_rpms`` map.

    Post-upgrade only; only clients passed into this step. Refresh dnf metadata
    afterward for kernel listing.
    """
    node = getattr(client, "node", client)
    _ensure_subscribed(client)
    log.info(
        "Enabling RHEL repos for %s via enable_rhel_rpms(%s)",
        node.hostname,
        version_id,
    )
    enable_rhel_rpms(node, version_id)
    out, _ = client.exec_command(
        sudo=True,
        cmd="subscription-manager repos --list-enabled | grep -c baseos",
        check_ec=False,
        timeout=300,
    )
    try:
        baseos_count = int((out or "0").strip() or 0)
    except ValueError:
        baseos_count = 0
    if baseos_count < 1:
        raise OperationFailedError(
            f"No BaseOS repo enabled on {node.hostname} for RHEL {version_id}"
        )

    client.exec_command(sudo=True, cmd="dnf clean all", check_ec=False, timeout=300)
    out, err = client.exec_command(
        sudo=True, cmd="dnf makecache", check_ec=False, timeout=900
    )
    log.info(
        "dnf makecache on %s: %s",
        node.hostname,
        ((out or err or "").strip() or "(no output)")[:500],
    )


def _list_kernel_nvrs(client) -> list:
    """Return available kernel NVRs (no arch), ascending via sort -V."""
    cmd = (
        "dnf --showduplicates list kernel 2>/dev/null | "
        "awk '/^kernel\\.x86_64/ { print $2 }' | sort -V | uniq"
    )
    out, _ = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
    nvrs = []
    for line in (out or "").splitlines():
        nvr = line.strip()
        if nvr and nvr[:1].isdigit():
            nvrs.append(nvr)
    return nvrs


def _resolve_target_nvr(client, kernel_cfg: str, available: list) -> str:
    """
    Resolve install target from an already-fetched available NVR list.

    ``latest`` → newest available NVR.
    Otherwise treat as an exact NVR (must be in the available list).
    """
    kernel_cfg = (kernel_cfg or "latest").strip()
    if not available:
        raise OperationFailedError(
            f"No kernel packages listed on {client.node.hostname} "
            "(enable_rhel_rpms channels + dnf metadata refreshed?)"
        )
    if kernel_cfg.lower() == "latest":
        return available[-1]

    want = _normalize_nvr(kernel_cfg)
    if want not in available:
        raise OperationFailedError(
            f"Requested kernel NVR {want!r} not available on "
            f"{client.node.hostname}; newest is {available[-1]!r}"
        )
    return want


def _current_nvr(client) -> str:
    host = client.node.hostname
    try:
        out, err = client.exec_command(sudo=True, cmd="uname -r")
    except Exception as exc:
        log.error("%s uname -r failed: %s", host, exc)
        raise OperationFailedError(f"uname -r failed on {host}: {exc}") from exc
    raw = (out or "").strip()
    if not raw:
        raise OperationFailedError(
            f"uname -r returned empty on {host}; stderr={(err or '').strip()!r}"
        )
    log.info("%s uname -r => %s", host, raw)
    return _normalize_nvr(raw)


def _install_and_boot(client, nvr: str, fs_util: FsUtils, reboot_timeout: int) -> bool:
    """Install kernel NVR, set grubby default, reboot, verify.

    Returns True if install/reboot ran, False if already on target (skipped).

    On failure after ``grubby --set-default``, restore the previous default
    (best-effort) then re-raise so the suite step still fails.
    """
    host = client.node.hostname
    current = _current_nvr(client)
    if current == nvr:
        log.info("%s already running target kernel %s; skip install/reboot", host, nvr)
        return False

    prev, _ = client.exec_command(sudo=True, cmd="grubby --default-kernel")
    prev = (prev or "").strip()
    log.info("Installing kernel-%s on %s (current %s)", nvr, host, current)
    try:
        client.exec_command(
            sudo=True,
            cmd=f"dnf install -y {shlex.quote(f'kernel-{nvr}')}",
            long_running=True,
        )
        vmlinuz = f"/boot/vmlinuz-{nvr}.x86_64"
        qvmlinuz = shlex.quote(vmlinuz)
        out, _ = client.exec_command(sudo=True, cmd=f"ls {qvmlinuz}", check_ec=False)
        if vmlinuz not in (out or "") and nvr not in (out or ""):
            # ls prints path on success; tolerate quiet success
            client.exec_command(sudo=True, cmd=f"test -f {qvmlinuz}")

        client.exec_command(sudo=True, cmd=f"grubby --set-default {qvmlinuz}")
        default, _ = client.exec_command(sudo=True, cmd="grubby --default-kernel")
        default = (default or "").strip()
        if nvr not in default:
            raise OperationFailedError(
                f"grubby default on {host} is {default!r}, expected NVR {nvr}"
            )
        log.info(
            "%s grubby default=%s; rebooting (timeout %ss)",
            host,
            default,
            reboot_timeout,
        )
        fs_util.reboot_node(client, timeout=reboot_timeout)
        after = _current_nvr(client)
        if after != nvr:
            raise OperationFailedError(
                f"After reboot {host} is on {after!r}, expected {nvr!r}"
            )
        log.info("%s kernel gate OK: %s", host, after)
        return True
    except Exception:
        if prev:
            log.warning(
                "%s kernel gate failed; restoring grubby default %s", host, prev
            )
            client.exec_command(
                sudo=True,
                cmd=f"grubby --set-default {shlex.quote(prev)}",
                check_ec=False,
            )
        raise


def run(ceph_cluster, **kw):
    """Config keys: clients, kernel, reboot_timeout, update_kernel."""
    try:
        config = kw.get("config") or {}
        entities = config.get("clients") or list(_DEFAULT_CLIENTS)
        kernel_cfg = str(config.get("kernel", "latest")).strip() or "latest"
        reboot_timeout = int(config.get("reboot_timeout", 900))
        update_kernel = bool(config.get("update_kernel", True))
        if not update_kernel:
            log.info(
                "update_kernel=false; skipping CephX client kernel gate for %s",
                entities,
            )
            log.info(
                "CephX client kernel gate PASSED for %s (installed=%s skipped=%s)",
                entities,
                [],
                list(entities),
            )
            return 0

        fs_util = FsUtils(ceph_cluster, test_data=kw.get("test_data"))
        clients = ceph_cluster.get_ceph_objects("client")
        installed = []
        skipped = []

        for entity in entities:
            client = client_by_entity(clients, entity)
            host = client.node.hostname
            version_id = _rhel_version_id(client)
            current = _current_nvr(client)
            log.info(
                "=== kernel gate %s (%s) RHEL %s config.kernel=%s ===",
                entity,
                host,
                version_id,
                kernel_cfg,
            )
            log.info("%s RHEL VERSION_ID: %s", host, version_id)
            log.info("%s current kernel (uname -r): %s", host, current)

            floor = _KERNEL_FLOORS.get(version_id)
            if not floor:
                log.info(
                    "%s RHEL %s not in kernel-gate floors %s; skip SM/repos/install/reboot",
                    host,
                    version_id,
                    sorted(_KERNEL_FLOORS),
                )
                skipped.append(entity)
                continue
            if _nvr_ge(client, current, floor):
                log.info(
                    "%s current kernel %s >= floor %s; skip SM/repos/install/reboot",
                    host,
                    current,
                    floor,
                )
                skipped.append(entity)
                continue

            # Pinned NVR already running: skip SM/repos/install/reboot.
            if kernel_cfg.lower() != "latest":
                want = _normalize_nvr(kernel_cfg)
                if current == want:
                    log.info(
                        "%s already on pinned kernel %s; skip SM/repos/install/reboot",
                        host,
                        want,
                    )
                    skipped.append(entity)
                    continue

            _enable_kernel_repos(client, version_id)
            available = _list_kernel_nvrs(client)
            log.info(
                "%s available kernel NVRs after enable_rhel_rpms (%d):",
                host,
                len(available),
            )
            for i, nvr in enumerate(available, 1):
                log.info("  [%d/%d] %s", i, len(available), nvr)
            target = _resolve_target_nvr(client, kernel_cfg, available)
            log.info(
                "%s selected target kernel NVR: %s (from config.kernel=%s)",
                host,
                target,
                kernel_cfg,
            )
            if _install_and_boot(client, target, fs_util, reboot_timeout):
                installed.append(entity)
            else:
                skipped.append(entity)
            log.info(
                "%s kernel gate done: RHEL=%s was=%s now=%s target=%s",
                host,
                version_id,
                current,
                _current_nvr(client),
                target,
            )

        log.info(
            "CephX client kernel gate PASSED for %s (installed=%s skipped=%s)",
            entities,
            installed,
            skipped,
        )
        return 0
    except Exception as exc:
        log.error(exc)
        log.error(traceback.format_exc())
        return 1
