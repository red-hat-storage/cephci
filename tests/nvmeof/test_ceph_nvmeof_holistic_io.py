"""
Customer-centric NVMeoF holistic IO + day-2 ops under load.

Modes:
  - default / under_load: background multi-tenant FIO + day2_ops
  - auth_fio: configure DHCHAP tenants and run authenticated FIO in the
    same phase (keys never leave this process)

Does not tear down gateway/pool unless cleanup is requested.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from ceph.utils import get_node_by_id
from tests.nvmeof.test_nvmeof_gwgroup_inbandauth import (
    configure_gw_entities_with_encryption,
)
from tests.nvmeof.workflows.gateway_entities import (
    _hostnames_match,
    disconnect_initiators,
    fetch_namespaces,
    teardown,
)
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.initiator import prepare_io_execution
from tests.nvmeof.workflows.load_balancing import scale_down, scale_up, validate_scaleup
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    check_gateway,
    validate_qos,
    verify_qos,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def _run_client_fio(client, io_args):
    """Run time-based FIO on all NVMe paths visible to one initiator."""
    paths = client.list_spdk_drives() or client.list_devices()
    if not paths:
        raise RuntimeError(f"No NVMe paths on {client.node.hostname}")

    LOG.info(
        "Starting FIO on %s with %s device(s): %s",
        client.node.hostname,
        len(paths),
        io_args,
    )
    runtime = str(io_args.get("run_time", 600))
    fio_kwargs = {
        "io_size": io_args.get("size", "1G"),
        "runtime": runtime,
        "paths": paths,
        "io_type": io_args.get("io_type", "randrw"),
        "iodepth": io_args.get("iodepth", 16),
        "rwmixread": io_args.get("rwmixread"),
        "test_name": io_args.get("test_name"),
        "time_based": True,
        "execute_blkdiscard": io_args.get("execute_blkdiscard", False),
    }
    if io_args.get("verify"):
        fio_kwargs["verify"] = io_args["verify"]
    if io_args.get("verify_fatal") is not None:
        fio_kwargs["verify_fatal"] = io_args["verify_fatal"]
    if io_args.get("bs"):
        fio_kwargs["bs"] = io_args["bs"]
    if io_args.get("num_jobs") is not None:
        fio_kwargs["num_jobs"] = io_args["num_jobs"]
    if io_args.get("fsync") is not None:
        fio_kwargs["fsync"] = io_args["fsync"]
    if io_args.get("direct") is not None:
        fio_kwargs["direct"] = io_args["direct"]
    return client.start_fio(**fio_kwargs)


def _op_set_qos(gateway, args):
    qos_args = deepcopy(args)
    subsystem = qos_args["subsystem"]
    nsid = qos_args["nsid"]
    gateway.namespace.set_qos(**{"args": qos_args})
    verify_qos(deepcopy(qos_args), gateway)
    LOG.info("QoS applied under load: %s nsid=%s", subsystem, nsid)


def _op_qos_validate_io(gateway, clients, ceph_cluster, args):
    """Set QoS, briefly drive IO, validate observed bandwidth via iostat."""
    _op_set_qos(gateway, args)
    node_id = args.get("initiator_node")
    if not node_id:
        raise ValueError("qos_validate_io requires initiator_node")

    client = next((c for c in clients if c.node.id == node_id), None)
    if client is None:
        node = get_node_by_id(ceph_cluster, node_id)
        # Reconnect path if client object not in under-load set
        from tests.nvmeof.workflows.initiator import NVMeInitiator

        client = NVMeInitiator(node)

    paths = client.list_spdk_drives() or client.list_devices()
    if not paths:
        raise RuntimeError(f"No devices on {node_id} for QoS IO validation")

    device = paths[0].replace("/dev/", "")
    io_mode = {
        "r-megabytes-per-second": "read",
        "w-megabytes-per-second": "write",
        "rw-megabytes-per-second": "randrw",
    }
    # Prefer write validation when present
    key = next(
        (
            k
            for k in (
                "w-megabytes-per-second",
                "r-megabytes-per-second",
                "rw-megabytes-per-second",
            )
            if k in args
        ),
        "w-megabytes-per-second",
    )
    runtime = str(args.get("io_runtime", 40))
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        fut = executor.submit(
            client.start_fio,
            io_size="1G",
            runtime=runtime,
            paths=[paths[0]],
            io_type=io_mode.get(key, "write"),
            iodepth=4,
            time_based=True,
            execute_blkdiscard=False,
            test_name=f"qos-validate-{device}",
        )
        time.sleep(15)
        limits = {k: args[k] for k in args if k.endswith("megabytes-per-second")}
        validate_qos(client.node, device, **limits)
        fut.result()
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    # Also pull gateway-side IO stats when available
    try:
        out, _ = gateway.namespace.get_io_stats(
            **{
                "base_cmd_args": {"format": "json"},
                "args": {"subsystem": args["subsystem"], "nsid": args["nsid"]},
            }
        )
        LOG.info("Namespace IO stats after QoS validate: %s", out)
    except Exception as err:
        LOG.warning("get_io_stats not available/failed: %s", err)


def _op_fio_burst(clients, ceph_cluster, args):
    """
    Short foreground FIO burst on one client (e.g. backup restore / media read)
    while other persona FIO continues in the background.
    """
    node_id = args.get("initiator_node")
    if not node_id:
        raise ValueError("fio_burst requires initiator_node")

    client = next((c for c in clients if c.node.id == node_id), None)
    if client is None:
        from tests.nvmeof.workflows.initiator import NVMeInitiator

        client = NVMeInitiator(get_node_by_id(ceph_cluster, node_id))

    paths = client.list_spdk_drives() or client.list_devices()
    if not paths:
        raise RuntimeError(f"No devices on {node_id} for fio_burst")

    runtime = str(args.get("io_runtime", 60))
    fio_kwargs = {
        "io_size": args.get("size", "2G"),
        "runtime": runtime,
        "paths": [paths[0]],
        "io_type": args.get("io_type", "read"),
        "iodepth": args.get("iodepth", 16),
        "bs": args.get("bs", "1M"),
        "time_based": True,
        "execute_blkdiscard": False,
        "test_name": args.get("test_name", f"fio-burst-{node_id}"),
        "direct": args.get("direct", 1),
    }
    if args.get("num_jobs") is not None:
        fio_kwargs["num_jobs"] = args["num_jobs"]
    LOG.info(
        "fio_burst on %s path=%s io_type=%s bs=%s runtime=%s",
        node_id,
        paths[0],
        fio_kwargs["io_type"],
        fio_kwargs["bs"],
        runtime,
    )
    client.start_fio(**fio_kwargs)


def _op_change_visibility(gateway, args):
    vis_args = {
        "subsystem": args["subsystem"],
        "nsid": args["nsid"],
        "auto-visible": args.get("auto-visible", "yes"),
    }
    if args.get("force", True):
        vis_args["force"] = ""
    gateway.namespace.change_visibility(**{"args": vis_args})
    LOG.info(
        "change_visibility under load: %s nsid=%s auto-visible=%s",
        args["subsystem"],
        args["nsid"],
        args.get("auto-visible"),
    )


def _op_resize(gateway, args):
    resize_args = {
        "subsystem": args["subsystem"],
        "nsid": args["nsid"],
        "size": args["size"],
    }
    gateway.namespace.resize(**{"args": resize_args})
    LOG.info(
        "resize under load: %s nsid=%s -> %s",
        args["subsystem"],
        args["nsid"],
        args["size"],
    )


def _op_scale_down(nvme_service, orch, args):
    nodes = args["nodes"]
    if not isinstance(nodes, list):
        nodes = [nodes]
    LOG.info("scale_down under load: %s", nodes)
    scale_down(nvme_service, orch, nodes)
    remaining = [
        n for n in nvme_service.config.get("gw_nodes", []) if n not in set(nodes)
    ]
    nvme_service.config["gw_nodes"] = remaining
    LOG.info("GW membership after scale_down: %s", remaining)


def _assert_listeners(nvme_service, args):
    """
    ISCE-2203-oriented check: every live GW must have a listener for each NQN.
    """
    nqns = args.get("subsystems") or args.get("nqns") or []
    if not nqns:
        raise ValueError("assert_listeners requires subsystems/nqns")
    port = str(args.get("listener_port", 4420))
    gateway = nvme_service.gateways[0]

    for nqn in nqns:
        out, _ = gateway.listener.list(
            **{
                "base_cmd_args": {"format": "json"},
                "args": {"subsystem": nqn},
            }
        )
        listeners = json.loads(out).get("listeners", []) if out else []
        missing = []
        for gw in nvme_service.gateways:
            matched = any(
                _hostnames_match(lst.get("host_name"), gw.hostname)
                and str(lst.get("trsvcid")) == port
                for lst in listeners
            )
            if not matched:
                missing.append(gw.hostname)
        if missing:
            raise RuntimeError(
                f"Listener assert failed for {nqn} port={port}: "
                f"missing on GWs {missing}; have={listeners}"
            )
        LOG.info(
            "Listener assert OK: %s port=%s on %s GW(s)",
            nqn,
            port,
            len(nvme_service.gateways),
        )


def _op_scale_up(nvme_service, orch, args):
    nodes = args["nodes"]
    if not isinstance(nodes, list):
        nodes = [nodes]
    LOG.info("scale_up under load: %s", nodes)
    namespaces = fetch_namespaces(nvme_service.gateways[0])
    prev_gws = list(nvme_service.gateways)
    scale_up(nvme_service, orch, nodes, prev_gws, namespaces)
    validate_scaleup(nvme_service, orch, nodes, namespaces)
    merged = list(dict.fromkeys(list(nvme_service.config.get("gw_nodes", [])) + nodes))
    nvme_service.config["gw_nodes"] = merged
    LOG.info("GW membership after scale_up: %s", merged)
    # Optional post-LB auto-listener validation (ISCE-2203)
    if args.get("assert_listeners"):
        _assert_listeners(nvme_service, args["assert_listeners"])


def _op_ha_failover(ceph_cluster, nvme_service, config, args):
    tool = args.get("tool", "systemctl")
    nodes = args["nodes"]
    if not isinstance(nodes, list):
        nodes = [nodes]

    ha_cfg = deepcopy(config)
    ha_cfg["nvme_service"] = nvme_service
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **ha_cfg)
    ha.gateways = nvme_service.gateways
    ha.nvme_service = nvme_service

    for node_id in nodes:
        gw = check_gateway(nvme_service.gateways, node_id)
        LOG.info("HA failover under load on %s via %s", node_id, tool)
        ha.failover(gw, tool)
        LOG.info("HA failback under load on %s via %s", node_id, tool)
        ha.failback(gw, tool)


def _run_day2_ops(ceph_cluster, nvme_service, orch, config, ops, clients):
    gateway = nvme_service.gateways[0]

    for idx, step in enumerate(ops, start=1):
        op = step.get("op")
        args = deepcopy(step.get("args", {}))
        LOG.info("=== Day-2 op %s/%s: %s ===", idx, len(ops), op)

        if op == "sleep":
            time.sleep(int(args.get("seconds", 30)))
        elif op == "set_qos":
            _op_set_qos(gateway, args)
        elif op == "qos_validate_io":
            _op_qos_validate_io(gateway, clients, ceph_cluster, args)
        elif op == "fio_burst":
            _op_fio_burst(clients, ceph_cluster, args)
        elif op == "change_visibility":
            _op_change_visibility(gateway, args)
        elif op == "resize":
            _op_resize(gateway, args)
        elif op == "scale_down":
            _op_scale_down(nvme_service, orch, args)
            gateway = nvme_service.gateways[0]
        elif op == "scale_up":
            _op_scale_up(nvme_service, orch, args)
            gateway = nvme_service.gateways[0]
        elif op == "assert_listeners":
            _assert_listeners(nvme_service, args)
        elif op == "ha_failover":
            ha_config = deepcopy(config)
            ha_config["gw_nodes"] = list(nvme_service.config.get("gw_nodes", []))
            _op_ha_failover(ceph_cluster, nvme_service, ha_config, args)
            nvme_service.gateways = []
            nvme_service.init_gateways()
            gateway = nvme_service.gateways[0]
        else:
            raise ValueError(f"Unsupported day2 op: {op}")

        settle = int(step.get("settle_seconds", config.get("op_settle_seconds", 15)))
        if settle:
            time.sleep(settle)


def _inventory_snapshot(gateway):
    """Best-effort subsystem/namespace inventory for post-run logging."""
    try:
        out, _ = gateway.subsystem.list(
            **{"base_cmd_args": {"format": "json"}, "args": {}}
        )
        subs = json.loads(out).get("subsystems", [])
        LOG.info("Inventory: %s subsystems present after under-load ops", len(subs))
        for sub in subs:
            nqn = sub.get("nqn") or sub.get("subnqn")
            ns_out, _ = gateway.namespace.list(
                **{
                    "base_cmd_args": {"format": "json"},
                    "args": {"subsystem": nqn},
                }
            )
            ns_count = len(json.loads(ns_out).get("namespaces", []))
            LOG.info("  %s -> %s namespaces", nqn, ns_count)
    except Exception as err:
        LOG.warning("Inventory snapshot failed: %s", err)


def _run_auth_fio(ceph_cluster, nvme_service, config, rbd_obj):
    """
    ISCE-3542 style: create DHCHAP keys and run authenticated FIO in one phase.
    Uses unique NQNs so standing open tenants are untouched.
    """
    auth_cfg = config.get("auth_fio", {})
    if not auth_cfg.get("subsystems") or not auth_cfg.get("initiators"):
        raise ValueError("auth_fio requires subsystems and initiators")

    gwgroup_config = {
        "gw_group": config.get("gw_group"),
        "gw_nodes": config.get("gw_nodes"),
        "rbd_pool": config.get("rbd_pool", "rbd"),
        "inband_auth_mode": auth_cfg.get("inband_auth_mode", "bidirectional"),
        "subsystems": auth_cfg["subsystems"],
        "hosts": [],
    }
    for sub in gwgroup_config["subsystems"]:
        sub.setdefault("auth_mode", gwgroup_config["inband_auth_mode"])
        sub.setdefault("subnqn", sub.get("nqn") or sub.get("subnqn"))

    LOG.info("Configuring DHCHAP tenants and running authenticated FIO (same phase)")
    keyed_initiators = configure_gw_entities_with_encryption(
        gwgroup_config, ceph_cluster, nvme_service
    )

    clients = prepare_io_execution(
        auth_cfg["initiators"],
        gateways=nvme_service.gateways,
        cluster=ceph_cluster,
        return_clients=True,
        pre_configured_initiators=keyed_initiators,
    )
    if not clients:
        raise RuntimeError("Failed to prepare authenticated initiators")

    errors = []
    with ThreadPoolExecutor(max_workers=len(clients)) as executor:
        futures = {}
        for idx, client in enumerate(clients):
            io_args = dict(auth_cfg["initiators"][idx].get("io_args", {}))
            io_args.setdefault("run_time", auth_cfg.get("fio_runtime", 90))
            io_args.setdefault("size", "1G")
            io_args.setdefault("execute_blkdiscard", False)
            futures[executor.submit(_run_client_fio, client, io_args)] = client
        for future in as_completed(futures):
            client = futures[future]
            try:
                future.result()
                LOG.info("Authenticated FIO OK on %s", client.node.hostname)
            except Exception as err:
                LOG.error(
                    "Authenticated FIO failed on %s: %s", client.node.hostname, err
                )
                errors.append(err)

    for initiator_cfg in auth_cfg["initiators"]:
        node = get_node_by_id(ceph_cluster, initiator_cfg["node"])
        disconnect_initiators(nvme_service, node=node)

    if errors:
        raise RuntimeError(f"Authenticated FIO failures: {errors}")
    return 0


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Entry: auth_fio mode or under-load day-2 orchestration."""
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    nvme_service = None
    executor = None
    futures = {}
    try:
        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            nvme_service.deploy()
        nvme_service.init_gateways()
        orch = Orch(ceph_cluster, **{})

        # --- Mode: DHCHAP configure + FIO same phase ---
        if config.get("mode") == "auth_fio" or config.get("auth_fio"):
            if config.get("mode") == "auth_fio" or not config.get("day2_ops"):
                return _run_auth_fio(ceph_cluster, nvme_service, config, rbd_obj)
            # If both present, run auth first then continue under-load
            _run_auth_fio(ceph_cluster, nvme_service, config, rbd_obj)

        initiators = config.get("initiators")
        if not initiators:
            raise ValueError("initiators config is required for under-load mode")

        clients = prepare_io_execution(
            initiators,
            gateways=nvme_service.gateways,
            cluster=ceph_cluster,
            return_clients=True,
        )
        if not clients:
            raise RuntimeError("Failed to prepare NVMe initiators for FIO")

        work = []
        for idx, client in enumerate(clients):
            io_args = dict(initiators[idx].get("io_args", {}))
            if "run_time" not in io_args:
                io_args["run_time"] = config.get("fio_runtime", 600)
            work.append((client, io_args))

        LOG.info("Starting background customer FIO on %s clients", len(work))
        executor = ThreadPoolExecutor(max_workers=len(work))
        futures = {
            executor.submit(_run_client_fio, client, io_args): client
            for client, io_args in work
        }

        settle = int(config.get("fio_settle_seconds", 30))
        LOG.info("Waiting %ss for FIO to settle before day-2 ops", settle)
        time.sleep(settle)

        day2_ops = config.get("day2_ops", [])
        day2_error = None
        if day2_ops:
            try:
                _run_day2_ops(
                    ceph_cluster, nvme_service, orch, config, day2_ops, clients
                )
            except Exception as err:
                day2_error = err
                LOG.error("Day-2 op failed; cancelling background FIO: %s", err)
                if executor is not None:
                    executor.shutdown(wait=False, cancel_futures=True)
                    executor = None

        LOG.info("Waiting for background FIO to complete")
        errors = []
        for future in as_completed(list(futures.keys())):
            client = futures[future]
            try:
                future.result()
                LOG.info("FIO completed on %s", client.node.hostname)
            except Exception as err:
                LOG.error("FIO failed on %s: %s", client.node.hostname, err)
                errors.append(f"{client.node.hostname}: {err}")

        _inventory_snapshot(nvme_service.gateways[0])

        if day2_error:
            raise RuntimeError(f"Holistic day-2 failure: {day2_error}") from day2_error
        if errors:
            raise RuntimeError(f"Holistic under-load FIO failures: {errors}")
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        if config.get("cleanup") and nvme_service is not None:
            teardown(nvme_service, rbd_obj)
    return 1
