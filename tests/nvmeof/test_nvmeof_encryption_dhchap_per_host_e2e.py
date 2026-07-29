"""
End-to-end NVMe-oF test: encryption-key service, DHCHAP host/controller keys,
controller-key rotation, mutual exclusion with subsystem keys, and FIO.

Manual test mapping (suites/tentacle/nvmeof/tier-2_nvmeof_9-1_feature.yaml):
  1. Deploy NVMe-oF service (2 gateways, single GW group) with encryption_key PEM
  2. Create two subsystems without subsystem DHCHAP key; add listeners and namespaces
  3. Generate host and controller DHCHAP keys; host add with both keys per subsystem
  4. Discover, connect, FIO (100% fill)
  5. Rotate controller key; reconnect; FIO
  6. Delete controller key from a subsystem host
  7. Create a new subsystem with subsystem DHCHAP key
  8. Host add with both keys must fail; host add with host-nqn and dhchap-key only should succeed
  9. Add listeners/namespaces; discover, connect, FIO
"""

from looseversion import LooseVersion

from ceph.ceph import Ceph
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import (
    configure_listeners,
    configure_namespaces,
    configure_subsystems,
    teardown,
    validate_hosts,
    validate_subsystems,
)
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import get_ceph_version_from_cluster

LOG = Log(__name__)
_MAX_KEY_GEN_ATTEMPTS = 10


def gen_dhchap_key(initiator: NVMeInitiator, nqn: str) -> str:
    """Generate a DHCHAP key for the given NQN using the initiator host."""
    out, _ = initiator.gen_dhchap_key(n=nqn)
    key = out.strip() if out else ""
    if not key:
        raise RuntimeError(f"Failed to generate DHCHAP key for {nqn}")
    return key


def gen_unique_dhchap_key(
    initiator: NVMeInitiator,
    nqn: str,
    exclude=None,
    max_attempts: int = _MAX_KEY_GEN_ATTEMPTS,
) -> str:
    """Generate a DHCHAP key for *nqn* that is not already in *exclude*.

    Retries up to *max_attempts* times before raising ``RuntimeError``.
    """
    excluded = set(exclude or [])
    for attempt in range(1, max_attempts + 1):
        key = gen_dhchap_key(initiator, nqn)
        if key not in excluded:
            return key
        LOG.warning(
            "Generated duplicate DHCHAP key on attempt %d/%d for %s",
            attempt,
            max_attempts,
            nqn,
        )
    raise RuntimeError(
        f"Could not generate a unique DHCHAP key for {nqn} "
        f"after {max_attempts} attempts"
    )


def apply_subsystem_defaults(sub_cfg, config, listener_port):
    """Apply default bdev, listener, and port settings to a subsystem config dict."""
    sub_cfg.setdefault("listener_port", listener_port)
    sub_cfg.setdefault("no-group-append", True)
    sub_cfg.setdefault(
        "bdevs",
        [
            {
                "count": config.get("bdev_count", 2),
                "size": config.get("bdev_size", "2G"),
                "ns_create_image": True,
            }
        ],
    )
    sub_cfg.setdefault("listeners", config.get("listeners", []))


def add_host_with_dhchap_keys(
    gateway, nqn, host_nqn, host_key=None, controller_key=None
):
    """Add a host to *nqn* on *gateway*, optionally setting DHCHAP keys.

    Returns the raw (out, err) tuple from the gateway ``host.add`` call.
    """
    args = {"subsystem": nqn, "host": host_nqn}
    if host_key:
        args["dhchap-key"] = host_key
    if controller_key:
        args["dhchap-controller-key"] = controller_key
    return gateway.host.add(**{"args": args})


def expect_host_add_failure(gateway, nqn, host_nqn, host_key=None, controller_key=None):
    """Assert that adding a host with the given keys raises an error or returns a failure.

    Validates the mutual-exclusion rule: a subsystem that already carries a
    subsystem-level DHCHAP key must reject ``dhchap-controller-key`` on host add.
    Raises ``AssertionError`` if the operation unexpectedly succeeds.
    """
    try:
        out, err = add_host_with_dhchap_keys(
            gateway, nqn, host_nqn, host_key, controller_key
        )
        combined = f"{out} {err}".lower()
        if (
            "mutually exclusive" in combined
            or "error" in combined
            or "fail" in combined
        ):
            LOG.info("Expected host add failure: %s %s", out, err)
            return
    except Exception as exc:
        message = str(exc).lower()
        if "mutually exclusive" in message or "subsystem" in message:
            LOG.info("Expected failure: %s", exc)
            return
        raise
    raise AssertionError("Expected host add to fail but it succeeded")


def discover_connect_fio(
    initiator,
    gateway,
    listener_port,
    dhchap_secret,
    controller_secret=None,
    subnqns=None,
):
    """Discover/connect to *subnqns* on *gateway*, run FIO (100 % fill), then restore state.

    Sets unidirectional auth when *controller_secret* is ``None``, bidirectional
    otherwise.  Initiator auth attributes are always restored in the ``finally``
    block, even on failure.
    """
    old_host_key = initiator.host_key
    old_subsys_key = initiator.subsys_key
    old_auth_mode = initiator.auth_mode
    try:
        initiator.host_key = dhchap_secret
        initiator.subsys_key = controller_secret
        initiator.auth_mode = "bidirectional" if controller_secret else "unidirectional"
        initiator.connect_targets(
            gateway,
            {
                "nqn": "discover-all" if subnqns else "connect-all",
                "listener_port": listener_port,
                "subsystems": subnqns,
            },
        )
        initiator.start_fio(io_size="100%")
        LOG.info("FIO (100%% fill) completed on %s", initiator.node.hostname)
    finally:
        initiator.host_key = old_host_key
        initiator.subsys_key = old_subsys_key
        initiator.auth_mode = old_auth_mode


def run_encryption_dhchap_e2e(ceph_cluster, config, nvme_service, rbd_obj):
    """Execute the full encryption + DHCHAP controller-key E2E workflow."""
    gw = nvme_service.gateways[0]
    listener_port = config.get("listener_port", 4420)

    initiator = NVMeInitiator(get_node_by_id(ceph_cluster, config["initiator_node"]))
    host_nqn = initiator.initiator_nqn()

    subsystems_cfg = config["subsystems"]
    for sub_cfg in subsystems_cfg:
        apply_subsystem_defaults(sub_cfg, config, listener_port)

    # Steps 2-3: subsystems, listeners, and namespaces (no subsystem DHCHAP key)
    LOG.info("Step 2-3: Create subsystems without subsystem DHCHAP key")
    configure_subsystems(
        nvme_service, ceph_cluster=ceph_cluster, subsystem_config=subsystems_cfg
    )
    validate_subsystems(nvme_service, subsystems_cfg)

    ceph_version = get_ceph_version_from_cluster(
        ceph_cluster.get_nodes(role="client")[0]
    )

    stack_config = {**config, "subsystems": subsystems_cfg}
    if LooseVersion(ceph_version) <= LooseVersion("20.2.1"):
        configure_listeners(nvme_service.gateways, stack_config)

    opt_args = {"ceph_cluster": ceph_cluster}
    configure_namespaces(gw, stack_config, opt_args=opt_args, rbd_obj=rbd_obj)

    # Steps 4-5: generate keys and host add with host + controller keys
    LOG.info("Step 4-5: Host add with DHCHAP host key and controller key")
    host_key = gen_dhchap_key(initiator, host_nqn)
    LOG.info("Host key: %s for host %s", host_key, host_nqn)
    controller_keys = {}
    for sub_cfg in subsystems_cfg:
        controller_keys[sub_cfg["nqn"]] = gen_dhchap_key(initiator, sub_cfg["nqn"])
        LOG.info(
            "Controller key: %s for subsystem %s",
            controller_keys[sub_cfg["nqn"]],
            sub_cfg["nqn"],
        )
        add_host_with_dhchap_keys(
            gw,
            sub_cfg["nqn"],
            host_nqn,
            host_key=host_key,
            controller_key=controller_keys[sub_cfg["nqn"]],
        )

    # Step 6: discover, connect to all subsystems, and FIO
    LOG.info("Step 6: Discover, connect, and FIO for all subsystems")
    for sub_cfg in subsystems_cfg:
        sub_nqn = sub_cfg["nqn"]
        discover_connect_fio(
            initiator,
            gw,
            listener_port,
            host_key,
            controller_secret=controller_keys[sub_nqn],
            subnqns=[sub_nqn],
        )
        initiator.disconnect_all()

    # nvme connect to a subsystem-A should not work with the controlley key of subsystem-B
    first_nqn = subsystems_cfg[0]["nqn"]
    if len(subsystems_cfg) > 1:
        for index, sub_cfg in enumerate(subsystems_cfg):
            sub_nqn = sub_cfg["nqn"]
            wrong_controller_nqn = subsystems_cfg[(index + 1) % len(subsystems_cfg)][
                "nqn"
            ]
            try:
                discover_connect_fio(
                    initiator,
                    gw,
                    listener_port,
                    host_key,
                    controller_secret=controller_keys[wrong_controller_nqn],
                    subnqns=[sub_nqn],
                )
            except Exception as err:
                LOG.info(
                    "Connection with controller key from %s to %s failed as expected: %s",
                    wrong_controller_nqn,
                    sub_nqn,
                    err,
                )
            else:
                raise AssertionError(
                    f"Connection to {sub_nqn} succeeded with controller key from "
                    f"{wrong_controller_nqn}"
                )
            finally:
                initiator.disconnect_all()

    # Steps 7-9: rotate controller key on first subsystem
    LOG.info("Step 7: Change controller DHCHAP key on %s", first_nqn)
    new_ctrl_key = gen_unique_dhchap_key(
        initiator, host_nqn, exclude={controller_keys[first_nqn]}
    )
    gw.host.change_controller_key(
        **{
            "args": {
                "subsystem": first_nqn,
                "host": host_nqn,
                "dhchap-controller-key": new_ctrl_key,
            }
        }
    )

    LOG.info("Step 8: Discover, connect, and FIO after controller key change")
    discover_connect_fio(
        initiator,
        gw,
        listener_port,
        host_key,
        controller_secret=new_ctrl_key,
        subnqns=[first_nqn],
    )
    initiator.disconnect_all()

    if len(subsystems_cfg) > 1:
        for index, sub_cfg in enumerate(subsystems_cfg):
            sub_nqn = sub_cfg["nqn"]
            wrong_controller_nqn = subsystems_cfg[(index + 1) % len(subsystems_cfg)][
                "nqn"
            ]
            expected_controller_key = (
                new_ctrl_key if sub_nqn == first_nqn else controller_keys[sub_nqn]
            )
            if controller_keys[wrong_controller_nqn] == expected_controller_key:
                continue
            try:
                discover_connect_fio(
                    initiator,
                    gw,
                    listener_port,
                    host_key,
                    controller_secret=controller_keys[wrong_controller_nqn],
                    subnqns=[sub_nqn],
                )
            except Exception as err:
                LOG.info(
                    "Post-rotation connection with controller key from %s to %s failed as expected: %s",
                    wrong_controller_nqn,
                    sub_nqn,
                    err,
                )
            else:
                raise AssertionError(
                    f"Connection to {sub_nqn} succeeded with stale controller key from "
                    f"{wrong_controller_nqn} after rotation"
                )
            finally:
                initiator.disconnect_all()

    # Step 9: delete controller key (default: second subsystem)
    del_nqn = config.get(
        "del_controller_key_on",
        subsystems_cfg[1]["nqn"] if len(subsystems_cfg) > 1 else first_nqn,
    )
    LOG.info("Step 9: Delete controller key on %s", del_nqn)
    gw.host.del_controller_key(**{"args": {"subsystem": del_nqn, "host": host_nqn}})

    # Steps 10-16: subsystem with subsystem-level DHCHAP key
    dhchap_sub = config["dhchap_subsystem"]
    apply_subsystem_defaults(dhchap_sub, config, listener_port)
    dhchap_nqn = dhchap_sub["nqn"]
    subsys_dhchap_key = gen_dhchap_key(initiator, dhchap_nqn)
    LOG.info("Subsystem key: %s for subsystem nqn %s", subsys_dhchap_key, dhchap_nqn)
    dhchap_sub["dhchap-key"] = subsys_dhchap_key

    LOG.info("Step 10: Create subsystem %s with subsystem DHCHAP key", dhchap_nqn)

    configure_subsystems(
        nvme_service, ceph_cluster=ceph_cluster, subsystem_config=[dhchap_sub]
    )
    validate_subsystems(nvme_service, [dhchap_sub])

    LOG.info("Step 11: Host add with both keys on subsystem-keyed subsystem must fail")
    expect_host_add_failure(
        gw,
        dhchap_nqn,
        host_nqn,
        host_key=gen_dhchap_key(initiator, host_nqn),
        controller_key=subsys_dhchap_key,
    )

    LOG.info("Step 12: Host add with host-nqn only")
    add_host_with_dhchap_keys(gw, dhchap_nqn, host_nqn, host_key=subsys_dhchap_key)
    validate_hosts(gw, [host_nqn], dhchap_nqn)

    LOG.info("Step 13: Add listeners and namespaces for subsystem-keyed subsystem")
    stack_config = {**config, "subsystems": [dhchap_sub]}
    if LooseVersion(ceph_version) <= LooseVersion("20.2.1"):
        configure_listeners(nvme_service.gateways, stack_config)
    opt_args = {"ceph_cluster": ceph_cluster}

    configure_namespaces(gw, stack_config, opt_args=opt_args, rbd_obj=rbd_obj)

    LOG.info("Step 14: Discover, connect, and FIO on subsystem with subsystem key")
    discover_connect_fio(
        initiator,
        gw,
        listener_port,
        subsys_dhchap_key,
        subnqns=[dhchap_nqn],
    )
    initiator.disconnect_all()

    LOG.info(
        "CEPH-83632329 - Allow setting controller DHCHAP key per host, E2E test completed successfully"
    )
    return 0


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Entry point for cephci suite execution."""
    config = kwargs["config"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    kwargs["config"].update(
        {
            "do_not_create_image": True,
            "rep-pool-only": True,
            "rep_pool_config": {"pool": config["rbd_pool"]},
        }
    )

    rbd_obj = None
    nvme_service = None
    try:
        rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
        check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)
        LOG.info(
            "STEP 1: CEPH-83632329 - Allow setting controller DHCHAP key per host, E2E test started"
        )
        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            config.setdefault("spec_deployment", True)
            nvme_service.deploy()
            if config.get("redeploy_service", True):
                nvme_service.redeploy()
        nvme_service.init_gateways()
        return run_encryption_dhchap_e2e(ceph_cluster, config, nvme_service, rbd_obj)
    except Exception as err:
        LOG.exception(
            "CEPH-83632329 - Allow setting controller DHCHAP key per host failed: %s",
            err,
        )
        return 1
    finally:
        if config.get("cleanup") and nvme_service is not None and rbd_obj is not None:
            LOG.info(
                "CEPH-83632329 - Allow setting controller DHCHAP key per host, E2E test cleanup started"
            )
            teardown(nvme_service, rbd_obj)
