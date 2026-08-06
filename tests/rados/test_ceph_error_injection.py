"""
test_ceph_error_injection.py - Standalone Error Injection Test Module

Reusable test module that injects or cleans up Ceph error injection
variables as a standalone suite step. Place this before other tests
to enable fault injection, and after them to clean up. No code changes
are needed in the tests that run between inject and cleanup.

The injector state is stored in test_data["error_injector"], a shared
dict that run.py passes to every test's run() function, so the same
CephErrorInjector instance persists across tests in a suite run.

Supported actions (set via config.action):
    inject  - Apply error injection config and store injector in test_data
    cleanup - Retrieve stored injector and remove all injections

==============================================================================
SUITE YAML USAGE
==============================================================================

Example 1 -- Inject before tests, cleanup after:

    - test:
        name: Inject MON instability errors
        module: test_ceph_error_injection.py
        abort-on-fail: true
        config:
          action: inject
          error_injection:
            profile: mon_instability
            profile_overrides:
              mon_inject_transaction_delay_probability: 0.1

    - test:
        name: MON thrashing under fault injection
        module: test_osd_thrashing.py
        config:
          enable_mon_thrashing: true
          duration: 1200

    - test:
        name: Cleanup error injections
        module: test_ceph_error_injection.py
        config:
          action: cleanup

Example 2 -- Layer multiple profiles before tests:

    - test:
        name: Inject network chaos
        module: test_ceph_error_injection.py
        config:
          action: inject
          error_injection:
            profile: network_chaos

    - test:
        name: Layer MON instability on top
        module: test_ceph_error_injection.py
        config:
          action: inject
          error_injection:
            profile: mon_instability

    - test:
        name: Run tests under combined faults
        module: test_osd_thrashing.py
        config:
          enable_mon_thrashing: true
          duration: 1200

    - test:
        name: Cleanup all error injections
        module: test_ceph_error_injection.py
        config:
          action: cleanup

Example 3 -- Individual config injection (no profile):

    - test:
        name: Inject OSD dispatch delays
        module: test_ceph_error_injection.py
        abort-on-fail: true
        config:
          action: inject
          error_injection:
            configs:
              osd_debug_inject_dispatch_delay_probability: 0.1
              osd_debug_inject_dispatch_delay_duration: 3
              bluestore_debug_inject_read_err: true

    - test:
        name: Run CephFS tests under OSD faults
        module: cephfs_basic_tests.py
        config:
          ...

    - test:
        name: Cleanup error injections
        module: test_ceph_error_injection.py
        config:
          action: cleanup

Example 4 -- Full config with EC errors and admin socket commands:

    - test:
        name: Inject storage corruption with EC errors
        module: test_ceph_error_injection.py
        abort-on-fail: true
        config:
          action: inject
          error_injection:
            profile: storage_corruption
            configs:
              osd_debug_inject_dispatch_delay_probability: 0.05
            ec_write_errors:
              - osd_id: 0
                pool: ec-pool-1
                shard: 2
                duration: 500
            admin_socket:
              - command: injectfull
                osd_id: 3
                full_type: nearfull
                count: 1

    - test:
        name: OSD thrashing under storage faults
        module: test_osd_thrashing.py
        config:
          enable_osd_thrashing: true
          duration: 1800

    - test:
        name: Cleanup error injections
        module: test_ceph_error_injection.py
        config:
          action: cleanup

==============================================================================
CONFIG REFERENCE
==============================================================================

    config:
      action: inject | cleanup       (required, default: inject)
      error_injection:                (required for inject, ignored for cleanup)
        profile: <profile_name>       (optional)
        profile_overrides:            (optional)
          <config_key>: <value>
        configs:                      (optional)
          <config_key>: <value>
        ec_write_errors:              (optional)
          - osd_id: <int>
            pool: <str>
            ...
        ec_read_errors:               (optional)
          - osd_id: <int>
            pool: <str>
            ...
        admin_socket:                 (optional)
          - command: <str>
            ...

See ceph/rados/ceph_error_injector.py for the full error_injection schema,
available profiles, and the complete catalog of 50 inject variables.
"""

from ceph.ceph_admin import CephAdmin
from ceph.rados.ceph_error_injector import CephErrorInjector
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Standalone error injection test entry point.

    Args:
        ceph_cluster: Ceph cluster object
        **kw: Test kwargs including:
            config (dict): Must contain 'action' and optionally 'error_injection'
            test_data (dict): Shared state dict for passing injector between tests

    Config keys:
        action (str): "inject" to apply errors, "cleanup" to remove them
        error_injection (dict): Error injection config (same schema as
            CephErrorInjector.apply_from_config)

    Returns:
        0: Success
        1: Failure
    """
    config = kw["config"]
    test_data = kw.get("test_data", {})
    action = config.get("action", "inject")

    if action == "inject":
        return _do_inject(ceph_cluster, config, test_data)
    elif action == "cleanup":
        return _do_cleanup(test_data)
    else:
        log.error(f"Unknown action: '{action}'. Use 'inject' or 'cleanup'.")
        return 1


def _do_inject(ceph_cluster, config, test_data):
    """Apply error injection config and store injector in test_data."""
    error_config = config.get("error_injection", {})
    if not error_config:
        log.error("No error_injection config provided for inject action")
        return 1

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    injector = test_data.get("error_injector")
    if injector:
        log.info(
            "Reusing existing CephErrorInjector for layered injection "
            f"({injector.get_active_injections()['total_active']} "
            f"injections already active)"
        )
    else:
        injector = CephErrorInjector(rados_obj=rados_obj)
        log.info("Created new CephErrorInjector instance")

    validation = injector.validate_config(error_config)
    if not validation["valid"]:
        log.error(
            "Error injection config validation failed: " f"{validation['errors']}"
        )
        return 1

    summary = injector.apply_from_config(error_config)
    log.info(
        f"Error injection applied -- "
        f"profile={summary['profile']}, "
        f"configs={summary['configs_applied']}, "
        f"ec_errors={summary['ec_errors_injected']}, "
        f"admin_cmds={summary['admin_commands_run']}"
    )

    active = injector.get_active_injections()
    log.info(f"Total active injections: {active['total_active']}")

    test_data["error_injector"] = injector
    return 0


def _do_cleanup(test_data):
    """Retrieve stored injector and remove all injections."""
    injector = test_data.get("error_injector")
    if not injector:
        log.warning("No active error injector found in test_data, nothing to clean up")
        return 0

    active = injector.get_active_injections()
    log.info(
        f"Cleaning up {active['total_active']} active injections "
        f"({len(active['config_injections'])} config, "
        f"{len(active['daemon_injections'])} daemon)"
    )

    summary = injector.cleanup_all()
    log.info(
        f"Cleanup complete -- "
        f"configs removed={summary['configs_removed']}, "
        f"configs failed={summary['configs_failed']}, "
        f"daemon cleared={summary['daemon_cmds_cleared']}, "
        f"daemon failed={summary['daemon_cmds_failed']}"
    )

    test_data.pop("error_injector", None)
    return 0
