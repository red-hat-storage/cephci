"""
Initiator reboot + persistent nvmf-autoconnect reconnect.

Customer-like Linux initiator reboot path:
  1. ``nvme connect`` / ``connect-all --persistent``
  2. ``/etc/modules-load.d/nvme-fabrics.conf``
  3. ``/etc/nvme/discovery.conf``
  4. ``nvmf-autoconnect.service`` enabled

After initiator reboot, namespace UUIDs must reappear via autoconnect
WITHOUT a manual discover / connect-all.

Contrasts with CEPH-83576087 (``test_ceph_83576087``), which explicitly
reconnects after reboot.
"""

import time

from ceph.ceph import Ceph
from ceph.waiter import WaitUntil
from cli.utilities.utils import reboot_node
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.initiator import prepare_io_execution
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def _read_cmd(node, cmd):
    out, err = node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    return (out or "").strip(), (err or "").strip()


def _assert_autoconnect_prereqs(initiator):
    """Fail early if boot-reconnect plumbing is missing."""
    node = initiator.node

    conf, _ = _read_cmd(node, "cat /etc/nvme/discovery.conf 2>/dev/null || true")
    if not conf.strip():
        raise RuntimeError(
            f"{node.hostname}: /etc/nvme/discovery.conf empty or missing "
            "(required for nvmf-autoconnect)"
        )
    LOG.info("[%s] discovery.conf:\n%s", node.hostname, conf)

    mods, _ = _read_cmd(
        node, "cat /etc/modules-load.d/nvme-fabrics.conf 2>/dev/null || true"
    )
    if "nvme-fabrics" not in mods:
        raise RuntimeError(
            f"{node.hostname}: nvme-fabrics not in modules-load.d " f"(got: {mods!r})"
        )

    enabled, _ = _read_cmd(
        node, "systemctl is-enabled nvmf-autoconnect.service 2>/dev/null || true"
    )
    if enabled not in ("enabled", "static", "indirect", "enabled-runtime"):
        # Best-effort enable, then re-check
        initiator.enable_nvmf_autoconnect()
        enabled, _ = _read_cmd(
            node, "systemctl is-enabled nvmf-autoconnect.service 2>/dev/null || true"
        )
        if enabled not in ("enabled", "static", "indirect", "enabled-runtime"):
            raise RuntimeError(
                f"{node.hostname}: nvmf-autoconnect.service not enabled "
                f"(is-enabled={enabled!r})"
            )
    LOG.info("[%s] nvmf-autoconnect is-enabled=%s", node.hostname, enabled)


def _capture_inventory(initiator):
    """Return (uuids, device_paths) from lsblk WWN + nvme list."""
    uuids = initiator.fetch_lsblk_nvme_devices() or []
    try:
        devices = initiator.list_spdk_drives() or []
    except Exception as err:
        LOG.warning("list_spdk_drives before reboot failed: %s", err)
        devices = []
    LOG.info(
        "[%s] pre-reboot inventory: %d UUID(s), %d device(s)",
        initiator.node.hostname,
        len(uuids),
        len(devices),
    )
    LOG.debug("UUIDs=%s devices=%s", uuids, devices)
    return sorted(uuids), sorted(devices)


def _wait_namespaces_after_reboot(initiator, expected_uuids, timeout, interval):
    """Poll until expected namespace UUIDs reappear — no discover/connect."""
    last_uuids = []
    last_devices = []
    for w in WaitUntil(timeout=timeout, interval=interval):
        try:
            last_uuids = initiator.fetch_lsblk_nvme_devices() or []
        except Exception as err:
            LOG.warning("lsblk after reboot not ready: %s", err)
            last_uuids = []
        try:
            last_devices = initiator.list_spdk_drives() or []
        except Exception as err:
            LOG.warning("nvme list after reboot not ready: %s", err)
            last_devices = []

        if expected_uuids:
            missing = set(expected_uuids) - set(last_uuids)
            if not missing:
                LOG.info(
                    "[%s] All %d pre-reboot UUIDs present after reboot",
                    initiator.node.hostname,
                    len(expected_uuids),
                )
                return sorted(last_uuids), sorted(last_devices)
            LOG.warning(
                "[%s] Waiting for autoconnect; missing UUIDs: %s "
                "(have %d/%d, devices=%d)",
                initiator.node.hostname,
                sorted(missing),
                len(expected_uuids) - len(missing),
                len(expected_uuids),
                len(last_devices),
            )
        elif last_devices:
            LOG.info(
                "[%s] Namespaces reappeared via devices (no UUID baseline): %s",
                initiator.node.hostname,
                last_devices,
            )
            return sorted(last_uuids), sorted(last_devices)

    raise TimeoutError(
        f"{initiator.node.hostname}: namespaces did not return within {timeout}s "
        f"via nvmf-autoconnect (expected UUIDs={expected_uuids}, "
        f"last_uuids={last_uuids}, last_devices={last_devices}). "
        "Did NOT call discover/connect after reboot."
    )


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Reboot Linux initiator and assert persistent autoconnect restores NS."""
    config = kwargs["config"]
    rbd_pool = initial_rbd_config(**kwargs)["rbd_reppool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    nvme_service = None
    try:
        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            nvme_service.deploy()
        nvme_service.init_gateways()

        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd_pool, cluster=ceph_cluster)

        if not config.get("initiators"):
            raise RuntimeError("config.initiators is required")

        clients = prepare_io_execution(
            config["initiators"],
            gateways=nvme_service.gateways,
            cluster=ceph_cluster,
            return_clients=True,
        )
        initiator = clients[0]

        # Reinforce boot-reconnect plumbing (configure already ran in prepare)
        initiator.configure()
        # Ensure discovery.conf points at the live gateway discovery port
        gw = nvme_service.gateways[0]
        initiator.configure_discovery_conf(
            traddr=gw.node.ip_address,
            trsvcid=initiator.discovery_port,
            transport="tcp",
        )
        initiator.enable_nvmf_autoconnect()
        _assert_autoconnect_prereqs(initiator)

        before_uuids, before_devices = _capture_inventory(initiator)
        if not before_uuids and not before_devices:
            raise RuntimeError(
                f"No NVMe namespaces on {initiator.node.hostname} before reboot"
            )

        min_ns = int(config.get("min_namespaces", 1))
        count = len(before_uuids) or len(before_devices)
        if count < min_ns:
            raise RuntimeError(
                f"Expected at least {min_ns} namespace(s) before reboot, found {count}"
            )

        LOG.info(
            "Rebooting initiator %s — namespaces must return WITHOUT "
            "manual discover/connect",
            initiator.node.hostname,
        )
        if not reboot_node(initiator.node):
            raise RuntimeError(
                f"Initiator {initiator.node.hostname} did not come back after reboot"
            )

        # Brief settle for systemd / fabrics after SSH is up
        time.sleep(int(config.get("post_reboot_settle", 15)))

        # CRITICAL: do not call discover / connect / connect_all here
        after_uuids, after_devices = _wait_namespaces_after_reboot(
            initiator,
            expected_uuids=before_uuids,
            timeout=int(config.get("autoconnect_timeout", 300)),
            interval=int(config.get("autoconnect_interval", 10)),
        )

        if before_uuids:
            missing = set(before_uuids) - set(after_uuids)
            if missing:
                raise RuntimeError(
                    f"UUIDs missing after reboot autoconnect: {sorted(missing)}"
                )
        elif before_devices and not after_devices:
            raise RuntimeError("No NVMe devices after reboot autoconnect")

        LOG.info(
            "[%s] Autoconnect OK: %d UUID(s), %d device(s) after reboot",
            initiator.node.hostname,
            len(after_uuids),
            len(after_devices),
        )

        if config.get("fio_smoke", True):
            paths = after_devices or initiator.list_spdk_drives() or []
            if not paths:
                raise RuntimeError("No devices for post-reboot FIO smoke")
            runtime = str(config.get("fio_runtime", 30))
            LOG.info("Post-reboot FIO smoke on %s (runtime=%ss)", paths[:2], runtime)
            initiator.start_fio(
                io_size=config.get("fio_size", "256M"),
                runtime=runtime,
                paths=paths[:2],
                io_type=config.get("io_type", "randrw"),
                iodepth=8,
                time_based=True,
                execute_blkdiscard=False,
                test_name=config.get("fio_test_name", "reboot-autoconnect-smoke"),
            )

        LOG.info("Initiator reboot + nvmf-autoconnect validation succeeded")
        return 0
    except Exception as err:
        LOG.error(err)
        return 1
    finally:
        if config.get("cleanup") and nvme_service is not None:
            teardown(nvme_service, rbd_pool)
