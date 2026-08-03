"""
IBMCEPH-17197 / IBMCEPH-17223

Remove a connected host from a subsystem while IO is running.
Optional keep-connections path for PSK-rotation style removals.

Expect:
- No GW crash/restart storm on peer (or any) gateways
- keep_connections=true: IO continues on the still-connected session
- keep_connections=false: host ACL removed; FIO may fail (expected)
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph import Ceph
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.initiator import prepare_io_execution
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def _snapshot_nvmeof_daemons(installer):
    out, _ = installer.exec_command(
        cmd="ceph orch ps --daemon_type nvmeof --format json", sudo=True
    )
    rows = json.loads(out or "[]")
    snap = {}
    for r in rows:
        name = r.get("daemon_name") or ""
        if "nvmeof" not in name:
            continue
        snap[name] = {
            "container_id": r.get("container_id") or r.get("container_id_short"),
            "started": r.get("started") or r.get("start_time"),
            "status": r.get("status_desc") or r.get("status"),
            "host": r.get("hostname"),
        }
    return snap


def _assert_daemons_stable(before, after, allow_missing=False):
    """Fail on unhealthy status or container_id/started changes (crash/restart)."""
    bad = []
    for name, prev in before.items():
        cur = after.get(name)
        if not cur:
            if allow_missing:
                continue
            bad.append({"daemon": name, "reason": "missing after host remove"})
            continue
        status = str(cur.get("status", "")).lower()
        if (
            status
            and status not in ("running", "1", "active")
            and "running" not in status
        ):
            bad.append(
                {"daemon": name, "reason": f"unhealthy status={cur.get('status')}"}
            )
            continue
        if prev.get("container_id") and cur.get("container_id"):
            if prev["container_id"] != cur["container_id"]:
                bad.append(
                    {
                        "daemon": name,
                        "reason": "container_id changed (restart)",
                        "before": prev["container_id"],
                        "after": cur["container_id"],
                    }
                )
                continue
        if (
            prev.get("started")
            and cur.get("started")
            and prev["started"] != cur["started"]
        ):
            bad.append(
                {
                    "daemon": name,
                    "reason": "started timestamp changed (restart)",
                    "before": prev["started"],
                    "after": cur["started"],
                }
            )
    if bad:
        raise RuntimeError(f"NVMeoF daemon instability after host remove: {bad}")


def _host_still_listed(gateway, subsystem, host_nqn):
    out, _ = gateway.host.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"subsystem": subsystem},
        }
    )
    try:
        data = json.loads(out or "{}")
    except Exception:
        return host_nqn in (out or "")
    hosts = data.get("hosts") or data.get("Hosts") or []
    if isinstance(hosts, list):
        for h in hosts:
            if isinstance(h, str) and h == host_nqn:
                return True
            if isinstance(h, dict) and host_nqn in (
                h.get("nqn"),
                h.get("host_nqn"),
                h.get("hostnqn"),
            ):
                return True
    return host_nqn in json.dumps(data)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    nvme_service = None
    executor = None
    try:
        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            nvme_service.deploy()
        nvme_service.init_gateways()
        configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)

        clients = prepare_io_execution(
            config["initiators"],
            gateways=nvme_service.gateways,
            cluster=ceph_cluster,
            return_clients=True,
        )
        client = clients[0]
        paths = client.list_spdk_drives() or client.list_devices()
        if not paths:
            raise RuntimeError("No NVMe devices for host-remove stability test")

        installer = ceph_cluster.get_nodes(role="installer")[0]
        before = _snapshot_nvmeof_daemons(installer)
        LOG.info("GW daemons before host remove: %s", before)

        runtime = str(config.get("fio_runtime", 180))
        executor = ThreadPoolExecutor(max_workers=1)
        fut = executor.submit(
            client.start_fio,
            io_size="1G",
            runtime=runtime,
            paths=paths[:1],
            io_type="randwrite",
            iodepth=8,
            time_based=True,
            execute_blkdiscard=False,
            test_name="bm-host-remove-under-io",
        )
        time.sleep(int(config.get("fio_settle_seconds", 15)))

        subsystem = config["subsystem"]
        host_nqn = client.initiator_nqn()
        gateway = nvme_service.gateways[0]
        keep = config.get("keep_connections", False)
        args = {"subsystem": subsystem, "host": host_nqn, "force": ""}
        if keep:
            args["keep-connections"] = ""

        LOG.info(
            "Removing host %s from %s (keep_connections=%s)",
            host_nqn,
            subsystem,
            keep,
        )
        try:
            gateway.host.delete(**{"args": args})
        except Exception as err:
            # Bug notes gRPC may fail even when remove succeeds — continue to validate GW health
            LOG.warning("host.delete raised (may still have removed): %s", err)

        time.sleep(int(config.get("post_remove_settle", 45)))
        after = _snapshot_nvmeof_daemons(installer)
        LOG.info("GW daemons after host remove: %s", after)
        _assert_daemons_stable(before, after)

        if not keep:
            # ACL path: host should no longer be listed (best-effort; warn if CLI shape differs)
            try:
                if _host_still_listed(gateway, subsystem, host_nqn):
                    raise RuntimeError(
                        f"Host {host_nqn} still listed on {subsystem} after delete"
                    )
            except RuntimeError:
                raise
            except Exception as err:
                LOG.warning("Could not verify host ACL removal via host.list: %s", err)

            try:
                fut.result(timeout=30)
            except Exception as err:
                LOG.info("FIO ended after ACL host remove (expected): %s", err)
        else:
            # keep-connections: session should remain usable — FIO must complete cleanly
            try:
                fut.result(timeout=int(runtime) + 60)
            except Exception as err:
                raise RuntimeError(
                    f"FIO failed after host remove with keep-connections "
                    f"(IBMCEPH-17223 path): {err}"
                ) from err

        LOG.info("Host-remove stability check passed (keep_connections=%s)", keep)
        return 0
    except Exception as err:
        LOG.error(err)
        return 1
    finally:
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        if config.get("cleanup") and nvme_service is not None:
            teardown(nvme_service, rbd_obj)
