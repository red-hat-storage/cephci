"""
EC pool inconsistent-object scrub/deep-scrub auto-repair verification.

Validates how Ceph handles inconsistent objects in an erasure-coded pool when
``osd_scrub_auto_repair`` and ``osd_scrub_auto_repair_num_errors`` are configured
and scheduled scrub or deep-scrub runs on the affected placement group (PG).

Ceph configuration
------------------
osd_scrub_auto_repair (bool, default ``false``)
    When ``true``, scrub/deep-scrub may automatically repair PG inconsistencies.

osd_scrub_auto_repair_num_errors (int, default ``5``)
    Auto-repair is skipped when more than this many scrub errors are found.

Test workflow
-------------
1. Disable PG autoscaler; reset scrub intervals; create an EC pool.
2. Inject inconsistent objects and wait for the setup deep-scrub to finish.
3. Run cases from ``config['case_to_run']`` (all six when omitted).
4. For each case: set auto-repair parameters, adjust OSD scrub flags as needed,
   run scheduled scrub/deep-scrub via ``get_inconsistent_count()``
   (``user_initiated=False``), then assert inconsistent object count.
5. Teardown restores cluster defaults, re-enables autoscaler, optionally deletes
   the pool, and checks for OSD crashes.

Scrub execution
---------------
Cases call ``get_inconsistent_count()`` with ``user_initiated=False`` (default).
Short scrub intervals (10--60 s) and a 2-hour scrub window trigger scheduled scrub
or deep-scrub. Scrub wait exceptions are logged and the case continues with a
fresh inconsistent-count read (same pattern as the replicated pool test).
Cases 5 and 6 call ``_inject_and_verify_auto_repair()`` so acting OSDs hold the
expected in-memory auto-repair settings before deep-scrub.

Cases (n = inconsistent object count before the case)
-----------------------------------------------------
case1 - Shallow scrub; ``num_errors = n-1``, ``auto_repair = true``
        Expectation: no repairs (count unchanged).

case2 - Shallow scrub; ``num_errors = n+1``, ``auto_repair = false``
        Expectation: no repairs (count unchanged).

case3 - Shallow scrub; ``num_errors = n+1``, ``auto_repair = true``
        Expectation: all objects repaired (count ``0``) via follow-up deep-scrub.

case4 - Deep-scrub; ``num_errors = n-1``, ``auto_repair = true``
        Expectation: no repairs (count unchanged).

case5 - Deep-scrub; ``num_errors = n+1``, ``auto_repair = false``
        Expectation: no repairs (count unchanged).

case6 - Deep-scrub; ``num_errors = n+1``, ``auto_repair = true``
        Expectation: all objects repaired and PG ``repair`` state cleared.

Suite configuration (``config`` dict passed to ``run()``)
---------------------------------------------------------
ec_pool (dict, required)
    EC pool definition; must include ``pool_name``.
inconsistent_obj_count (int, required)
    Positive number of inconsistent objects to create before cases run.
case_to_run (list[str], optional)
    Subset of ``case1``..``case6``; default runs all six sequentially.
delete_pool (bool, optional)
    When set, the EC pool is deleted during teardown.
debug_enable (bool, optional)
    When set, enables ``debug_osd`` and ``debug_mgr`` for the test duration.

Returns
-------
``run()`` returns ``0`` on success and ``1`` on failure.
"""

import json
import time
import traceback
from datetime import datetime, timedelta

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.rados_scrub import RadosScrubber
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)

CASE_LOG_PREFIX = {
    "case1": "CASE1",
    "case2": "CASE2",
    "case3": "CASE3",
    "case4": "CASE4",
    "case5": "CASE5",
    "case6": "CASE6",
}


def run(ceph_cluster, **kw):
    """
    Execute EC-pool inconsistent-object auto-repair verification cases.

    Creates an EC pool, injects inconsistent objects, runs selected scrub/deep-scrub
    scenarios with scheduled scrub (``user_initiated=False``), and validates
    inconsistent object counts and PG state.

    Args:
        ceph_cluster: Ceph cluster fixture provided by cephci.
        **kw: Keyword arguments; must include ``config`` with suite parameters:
            ec_pool (dict): EC pool configuration including ``pool_name``.
            inconsistent_obj_count (int): Number of inconsistent objects to create.
            case_to_run (list[str], optional): Cases to execute; default all six.
            delete_pool (bool, optional): Delete the pool in teardown.
            debug_enable (bool, optional): Enable OSD/mgr debug logging.

    Returns:
        int: ``0`` if all selected cases pass, ``1`` on setup/case/teardown failure.
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    scrub_object = RadosScrubber(node=cephadm)
    objectstore_obj = objectstoreToolWorkflows(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    wait_time = 45
    start_time = get_cluster_timestamp(rados_obj.node)
    log.info(f"[SETUP] Test workflow started at cluster time: {start_time}")
    try:
        log.info("[SETUP] Disabling PG autoscaler for the test duration")
        rados_obj.configure_pg_autoscaler(**{"default_mode": "off"})
        ec_config = config.get("ec_pool")
        pool_name = ec_config["pool_name"]
        req_no_of_objects = config.get("inconsistent_obj_count")
        if (
            not req_no_of_objects
            or not isinstance(req_no_of_objects, int)
            or req_no_of_objects <= 0
        ):
            log.error("[SETUP] inconsistent_obj_count must be a positive integer")
            return 1

        log.info(
            f"[SETUP] EC pool configuration - pool: {pool_name}, "
            f"requested inconsistent objects: {req_no_of_objects}"
        )

        log.info(
            "[SETUP] Resetting scrub interval settings to defaults to prevent "
            "immediate scrub from prior suite tests"
        )
        mon_obj.remove_config(section="osd", name="osd_scrub_min_interval")
        mon_obj.remove_config(section="osd", name="osd_scrub_max_interval")
        mon_obj.remove_config(section="osd", name="osd_deep_scrub_interval")

        log.info("[SETUP] Setting noscrub and nodeep-scrub OSD flags")
        scrub_object.set_osd_flags("set", "nodeep-scrub")
        scrub_object.set_osd_flags("set", "noscrub")

        # Ensure auto-repair is off while creating inconsistents so setup deep-scrub
        # cannot repair them before the cases run.
        if not mon_obj.set_config(
            section="osd", name="osd_scrub_auto_repair", value="false"
        ):
            log.error("[SETUP] Failed to disable osd_scrub_auto_repair")
            return 1
        log.info(
            "[SETUP] Set osd_scrub_auto_repair to false for inconsistent object creation"
        )

        if not rados_obj.create_erasure_pool(name=pool_name, **ec_config):
            log.error(f"[SETUP] Failed to create EC pool '{pool_name}'")
            return 1
        log.info(f"[SETUP] EC pool '{pool_name}' created successfully")

        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
        log.info(f"[SETUP] Acting PG set for pool '{pool_name}': {acting_pg_set}")

        if config.get("debug_enable"):
            log.info("[SETUP] Enabling debug logging for osd and mgr")
            mon_obj.set_config(section="osd", name="debug_osd", value="20/20")
            mon_obj.set_config(section="mgr", name="debug_mgr", value="20/20")

        log.info("[SETUP] Waiting for all PGs to reach clean state")
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=600, sleep_interval=30
        )
        log.info("[SETUP] All PGs are in clean state")

        try:
            log.info(
                f"[SETUP] Creating {req_no_of_objects} inconsistent objects in "
                f"pool '{pool_name}'"
            )
            pg_info = rados_obj.create_ecpool_inconsistent_obj(
                objectstore_obj, client_node, pool_name, req_no_of_objects
            )
            pg_id, no_of_inconsistent_objects = pg_info
            log.info(
                f"[SETUP] Created inconsistent objects in PG {pg_id}. "
                f"Inconsistent object count: {no_of_inconsistent_objects}"
            )
        except Exception as e:
            log.error(f"[SETUP] Failed to create inconsistent objects: {e}")
            log.error(
                "[SETUP] inconsistent_obj_count must be greater than 0. "
                "Cannot proceed when auto_repair_param_value would be invalid."
            )
            return 1

        # Wait for setup deep-scrub to finish so a later case enabling auto_repair
        # cannot race with that scrub and repair objects unexpectedly.
        log.info(
            f"[SETUP] Waiting for setup deep-scrub to finish on PG {pg_id} "
            "before starting cases"
        )
        try:
            rados_obj.start_check_deep_scrub_complete(
                pg_id=pg_id, user_initiated=False, wait_time=180
            )
            log.info(f"[SETUP] Setup deep-scrub completed on PG {pg_id}")
        except Exception as err:
            log.warning(
                f"[SETUP] Setup deep-scrub wait ended without stamp update on PG {pg_id}: "
                f"{err}. Continuing with current inconsistent count."
            )
        no_of_inconsistent_objects = get_pg_inconsistent_object_count(rados_obj, pg_id)
        log.info(
            f"[SETUP] Inconsistent object count before cases: {no_of_inconsistent_objects}"
        )
        if no_of_inconsistent_objects <= 0:
            log.error(
                "[SETUP] No inconsistent objects present before cases; cannot proceed"
            )
            return 1

        case_to_run = config.get("case_to_run")
        if not case_to_run:
            case_to_run = ["case1", "case2", "case3", "case4", "case5", "case6"]
            log.info(
                "[SETUP] case_to_run not specified; executing all cases sequentially: "
                f"{case_to_run}"
            )
        else:
            log.info(f"[SETUP] Cases selected for execution: {case_to_run}")

        if "case1" in case_to_run:
            log_case_start(
                "case1",
                "Inconsistent objects > osd_scrub_auto_repair_num_errors, auto_repair enabled",
                "scrub",
                "No repairs to be made",
            )
            log.info(
                "[CASE1] Verifying default values of osd_scrub_auto_repair_num_errors "
                "and osd_scrub_auto_repair"
            )
            auto_repair_num_value = mon_obj.get_config(
                section="osd", param="osd_scrub_auto_repair_num_errors"
            )
            log.info(
                f"[CASE1] Default osd_scrub_auto_repair_num_errors: {auto_repair_num_value}"
            )
            if int(auto_repair_num_value) != 5:
                log_case_failure(
                    "case1",
                    "Default osd_scrub_auto_repair_num_errors is not equal to 5",
                )
                return 1

            auto_repair_value = mon_obj.get_config(
                section="osd", param="osd_scrub_auto_repair"
            )
            log.info(f"[CASE1] Default osd_scrub_auto_repair: {auto_repair_value}")
            if str(auto_repair_value).lower() == "true":
                # if auto_repair_value == "true":
                log_case_failure(
                    "case1", "Default osd_scrub_auto_repair should be false"
                )
                return 1

            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log_case_failure(
                    "case1",
                    "Scrub operation still in progress; cannot proceed with test",
                )
                return 1
            no_of_inconsistent_objects = get_pg_inconsistent_object_count(
                rados_obj, pg_id
            )
            auto_repair_param_value = no_of_inconsistent_objects - 1

            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"[CASE1] Set osd_scrub_auto_repair_num_errors to {auto_repair_param_value}"
            )
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="true"
            )
            log.info("[CASE1] Set osd_scrub_auto_repair to true")
            scrub_object.set_osd_flags("unset", "noscrub")
            log.info("[CASE1] Unset noscrub OSD flag to allow scheduled scrub")
            log_case_parameters(
                "case1",
                no_of_inconsistent_objects,
                auto_repair_param_value,
                "True",
                "scrub",
            )

            try:
                get_inconsistent_count(
                    scrub_object,
                    mon_obj,
                    pg_id,
                    rados_obj,
                    "scrub",
                    acting_pg_set,
                )
            except Exception as e:
                log.info(e)

            # Get the inconsistent object count after scrub
            obj_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            # Same validation logic as Version 1
            if no_of_inconsistent_objects != obj_count:
                log_case_failure(
                    "case1",
                    f"Scrub repaired "
                    f"{no_of_inconsistent_objects - obj_count} "
                    f"inconsistent objects unexpectedly",
                )
                rados_obj.log_cluster_health()
                return 1
            log_case_complete(
                "case1",
                "scrub",
                "No repairs to be made",
                no_of_inconsistent_objects,
                obj_count,
            )

        if "case2" in case_to_run:
            log_case_start(
                "case2",
                "Inconsistent objects < osd_scrub_auto_repair_num_errors, auto_repair disabled",
                "scrub",
                "No repairs to be made",
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log_case_failure(
                    "case2",
                    "Scrub operation still in progress; cannot proceed with test",
                )
                return 1

            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="false"
            )
            log.info("[CASE2] Set osd_scrub_auto_repair to false")
            no_of_inconsistent_objects = get_pg_inconsistent_object_count(
                rados_obj, pg_id
            )
            auto_repair_param_value = no_of_inconsistent_objects + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"[CASE2] Set osd_scrub_auto_repair_num_errors to {auto_repair_param_value}"
            )
            scrub_object.set_osd_flags("unset", "noscrub")
            log.info("[CASE2] Unset noscrub OSD flag to allow scheduled scrub")
            log_case_parameters(
                "case2",
                no_of_inconsistent_objects,
                auto_repair_param_value,
                "False",
                "scrub",
            )
            try:
                get_inconsistent_count(
                    scrub_object,
                    mon_obj,
                    pg_id,
                    rados_obj,
                    "scrub",
                    acting_pg_set,
                )
            except Exception as e:
                log.info(e)
            obj_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            if obj_count != no_of_inconsistent_objects:
                log_case_failure(
                    "case2",
                    f"Scrub repaired {no_of_inconsistent_objects - obj_count} "
                    f"inconsistent objects unexpectedly",
                )
                rados_obj.log_cluster_health()
                return 1
            log_case_complete(
                "case2",
                "scrub",
                "No repairs to be made",
                no_of_inconsistent_objects,
                obj_count,
            )

        if "case3" in case_to_run:
            log_case_start(
                "case3",
                "Inconsistent objects < osd_scrub_auto_repair_num_errors, auto_repair enabled",
                "scrub",
                "All inconsistent objects are auto-repaired",
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log_case_failure(
                    "case3",
                    "Scrub operation still in progress; cannot proceed with test",
                )
                return 1
            no_of_inconsistent_objects = get_pg_inconsistent_object_count(
                rados_obj, pg_id
            )
            auto_repair_param_value = no_of_inconsistent_objects + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"[CASE3] Set osd_scrub_auto_repair_num_errors to {auto_repair_param_value}"
            )
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="true"
            )
            log.info("[CASE3] Set osd_scrub_auto_repair to true")
            # Shallow scrub with auto_repair schedules a follow-up deep-scrub to repair.
            # nodeep-scrub must be clear for that auto deep-scrub-on-error to run.
            scrub_object.set_osd_flags("unset", "noscrub")
            scrub_object.set_osd_flags("unset", "nodeep-scrub")
            log.info(
                "[CASE3] Unset noscrub and nodeep-scrub so scrub and auto-repair "
                "deep-scrub-on-error can run"
            )
            log_case_parameters(
                "case3",
                no_of_inconsistent_objects,
                auto_repair_param_value,
                "True",
                "scrub",
            )
            try:
                get_inconsistent_count(
                    scrub_object,
                    mon_obj,
                    pg_id,
                    rados_obj,
                    "scrub",
                    acting_pg_set,
                )
            except Exception as e:
                log.info(e)
            obj_count = get_pg_inconsistent_object_count(rados_obj, pg_id)

            # Auto-repair after shallow scrub is performed by a follow-up deep-scrub.
            if obj_count != 0:
                log.info(
                    f"[CASE3] Scrub finished with {obj_count} inconsistents still present; "
                    "running deep-scrub with auto_repair to complete repairs"
                )
                try:
                    get_inconsistent_count(
                        scrub_object,
                        mon_obj,
                        pg_id,
                        rados_obj,
                        "deep-scrub",
                        acting_pg_set,
                    )
                except Exception as e:
                    log.info(e)
                obj_count = get_pg_inconsistent_object_count(rados_obj, pg_id)

            if obj_count != 0:
                log_case_failure(
                    "case3",
                    f"expected all objects auto-repaired (count 0), still have {obj_count} "
                    f"(started with {no_of_inconsistent_objects})",
                )
                rados_obj.log_cluster_health()
                return 1
            log_case_complete(
                "case3",
                "scrub",
                "All inconsistent objects are auto-repaired",
                no_of_inconsistent_objects,
                obj_count,
            )
            if any(case in case_to_run for case in ("case4", "case5", "case6")):
                scrub_object.set_osd_flags("set", "noscrub")
                scrub_object.set_osd_flags("set", "nodeep-scrub")
                log.info(
                    "[CASE3] Set noscrub and nodeep-scrub before upcoming deep-scrub cases"
                )

        if "case4" in case_to_run:
            log_case_start(
                "case4",
                "Inconsistent objects > osd_scrub_auto_repair_num_errors, auto_repair enabled",
                "deep-scrub",
                "No repairs to be made",
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log_case_failure(
                    "case4",
                    "Scrub operation still in progress; cannot proceed with test",
                )
                return 1
            no_of_inconsistent_objects = get_pg_inconsistent_object_count(
                rados_obj, pg_id
            )
            log.info(
                f"[CASE4] Inconsistent object count before deep-scrub: "
                f"{no_of_inconsistent_objects}"
            )

            auto_repair_param_value = no_of_inconsistent_objects - 1
            log.info(
                f"[CASE4] Setting osd_scrub_auto_repair_num_errors to "
                f"{auto_repair_param_value} (inconsistent_count - 1) so errors exceed "
                "the auto-repair threshold"
            )

            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"[CASE4] Set osd_scrub_auto_repair_num_errors to {auto_repair_param_value}"
            )
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="true"
            )
            log.info("[CASE4] Set osd_scrub_auto_repair to true")
            scrub_object.set_osd_flags("unset", "noscrub")
            scrub_object.set_osd_flags("unset", "nodeep-scrub")
            log.info(
                "[CASE4] Unset noscrub and nodeep-scrub OSD flags to allow scheduled "
                "deep-scrub"
            )
            log_case_parameters(
                "case4",
                no_of_inconsistent_objects,
                auto_repair_param_value,
                "True",
                "deep-scrub",
            )
            log.info(
                f"[CASE4] Starting deep-scrub on PG {pg_id}; expectation: no auto-repair "
                f"(count {no_of_inconsistent_objects} > num_errors {auto_repair_param_value})"
            )
            try:
                get_inconsistent_count(
                    scrub_object,
                    mon_obj,
                    pg_id,
                    rados_obj,
                    "deep-scrub",
                    acting_pg_set,
                )
            except Exception as e:
                log.info(e)
            obj_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            log.info(
                f"[CASE4] Inconsistent object count after deep-scrub: {obj_count} "
                f"(before={no_of_inconsistent_objects}, "
                f"osd_scrub_auto_repair_num_errors={auto_repair_param_value})"
            )
            if obj_count != no_of_inconsistent_objects:
                log_case_failure(
                    "case4",
                    f"Deep-scrub repaired {no_of_inconsistent_objects - obj_count} "
                    f"inconsistent objects unexpectedly "
                    f"(before={no_of_inconsistent_objects}, after={obj_count}, "
                    f"osd_scrub_auto_repair_num_errors={auto_repair_param_value})",
                )
                rados_obj.log_cluster_health()
                return 1
            log_case_complete(
                "case4",
                "deep-scrub",
                "No repairs to be made",
                no_of_inconsistent_objects,
                obj_count,
            )

        if "case5" in case_to_run:
            log_case_start(
                "case5",
                "Inconsistent objects < osd_scrub_auto_repair_num_errors, auto_repair disabled",
                "deep-scrub",
                "No repairs to be made",
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log_case_failure(
                    "case5",
                    "Scrub operation still in progress; cannot proceed with test",
                )
                return 1
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="false"
            )
            log.info("[CASE5] Set osd_scrub_auto_repair to false")
            no_of_inconsistent_objects = get_pg_inconsistent_object_count(
                rados_obj, pg_id
            )
            auto_repair_param_value = no_of_inconsistent_objects + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"[CASE5] Set osd_scrub_auto_repair_num_errors to {auto_repair_param_value}"
            )
            if not _inject_and_verify_auto_repair(
                rados_obj,
                acting_pg_set,
                auto_repair="false",
                num_errors=auto_repair_param_value,
                case_id="case5",
            ):
                return 1
            scrub_object.set_osd_flags("unset", "noscrub")
            scrub_object.set_osd_flags("unset", "nodeep-scrub")
            log.info(
                "[CASE5] Unset noscrub and nodeep-scrub OSD flags to allow scheduled "
                "deep-scrub"
            )

            log_case_parameters(
                "case5",
                no_of_inconsistent_objects,
                auto_repair_param_value,
                "False",
                "deep-scrub",
            )
            try:
                get_inconsistent_count(
                    scrub_object,
                    mon_obj,
                    pg_id,
                    rados_obj,
                    "deep-scrub",
                    acting_pg_set,
                )
            except Exception as e:
                log.info(e)
            obj_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            if obj_count != no_of_inconsistent_objects:
                log_case_failure(
                    "case5",
                    f"Deep-scrub repaired {no_of_inconsistent_objects - obj_count} "
                    f"inconsistent objects unexpectedly",
                )
                rados_obj.log_cluster_health()
                return 1
            log_case_complete(
                "case5",
                "deep-scrub",
                "No repairs to be made",
                no_of_inconsistent_objects,
                obj_count,
            )

        if "case6" in case_to_run:
            log_case_start(
                "case6",
                "Inconsistent objects < osd_scrub_auto_repair_num_errors, auto_repair enabled",
                "deep-scrub",
                "All inconsistent objects are auto-repaired and PG repair state is cleared",
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log_case_failure(
                    "case6",
                    "Scrub operation still in progress; cannot proceed with test",
                )
                return 1
            no_of_inconsistent_objects = get_pg_inconsistent_object_count(
                rados_obj, pg_id
            )
            auto_repair_param_value = no_of_inconsistent_objects + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"[CASE6] Set osd_scrub_auto_repair_num_errors to {auto_repair_param_value}"
            )
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="true"
            )
            log.info("[CASE6] Set osd_scrub_auto_repair to true")
            if not _inject_and_verify_auto_repair(
                rados_obj,
                acting_pg_set,
                auto_repair="true",
                num_errors=auto_repair_param_value,
                case_id="case6",
            ):
                return 1
            scrub_object.set_osd_flags("unset", "noscrub")
            scrub_object.set_osd_flags("unset", "nodeep-scrub")
            log.info(
                "[CASE6] Unset noscrub and nodeep-scrub OSD flags to allow scheduled "
                "deep-scrub"
            )

            log_case_parameters(
                "case6",
                no_of_inconsistent_objects,
                auto_repair_param_value,
                "True",
                "deep-scrub",
            )
            try:
                get_inconsistent_count(
                    scrub_object,
                    mon_obj,
                    pg_id,
                    rados_obj,
                    "deep-scrub",
                    acting_pg_set,
                )
            except Exception as e:
                log.info(e)
            obj_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            if obj_count != 0:
                log_case_failure(
                    "case6",
                    f"Deep-scrub repaired only {no_of_inconsistent_objects - obj_count} "
                    f"of {no_of_inconsistent_objects} inconsistent objects; "
                    f"expected all objects to be repaired",
                )
                rados_obj.log_cluster_health()
                return 1
            log.info(
                f"[CASE6] All {no_of_inconsistent_objects} inconsistent objects "
                "were repaired as expected"
            )
            result = verify_pg_state(rados_obj, pg_id)
            if not result:
                log_case_failure(
                    "case6", "PG state still contains repair state after deep-scrub"
                )
                rados_obj.log_cluster_health()
                return 1
            log.info("[CASE6] PG state verified: repair state cleared after deep-scrub")
            log_case_complete(
                "case6",
                "deep-scrub",
                "All inconsistent objects are auto-repaired and PG repair state is cleared",
                no_of_inconsistent_objects,
                obj_count,
            )

        log.info(f"[SETUP] All selected cases completed successfully: {case_to_run}")
    except Exception as e:
        log.error(f"[SETUP] Test failed with unexpected exception: {e}")
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("\n\n ********* Executing finally block ******** \n\n")
        log.info("\n[TEARDOWN] Starting test cleanup\n")
        scrub_object.set_osd_flags("unset", "nodeep-scrub")
        scrub_object.set_osd_flags("unset", "noscrub")
        log.info("[TEARDOWN] Unset noscrub and nodeep-scrub OSD flags")
        rados_obj.configure_pg_autoscaler(**{"default_mode": "on"})
        log.info("[TEARDOWN] Re-enabled PG autoscaler")
        if config.get("delete_pool"):
            method_should_succeed(rados_obj.delete_pool, pool_name)
            log.info(f"[TEARDOWN] Deleted EC pool '{pool_name}' successfully")
        set_ecpool_inconsistent_default_param_value(mon_obj, scrub_object)
        time.sleep(30)

        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.info(
            f"[TEARDOWN] Test workflow completed. Start time: {start_time}, "
            f"End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("[TEARDOWN] Test failed due to OSD crash during test execution")
            return 1
        rados_obj.log_cluster_health()
        log.info("[TEARDOWN] Cleanup completed successfully")
    return 0


def get_pg_inconsistent_object_count(rados_obj, pg_id):
    """
    Return the inconsistent object count for a PG with stability polling.

    Repeatedly queries ``get_inconsistent_object_details`` until the count is
    unchanged for two consecutive reads or ``max_attempts`` is reached.

    Args:
        rados_obj: ``RadosOrchestrator`` instance.
        pg_id: Placement group identifier (e.g. ``"4.0s0"``).

    Returns:
        int: Stabilized count of inconsistent objects on the PG.
    """
    max_attempts = 8
    stable_threshold = 2

    obj_count = 0
    prev_count = -1
    stable_count = 0

    log.info(f"Fetching inconsistent object count for PG {pg_id}")

    for attempt in range(max_attempts + 1):
        inconsistent_details = rados_obj.get_inconsistent_object_details(pg_id)
        log.debug(f"Inconsistent object details for PG {pg_id}: {inconsistent_details}")
        obj_count = len(inconsistent_details["inconsistents"])
        log.info(
            f"PG {pg_id} inconsistent object count (attempt {attempt + 1}/{max_attempts + 1}): "
            f"{obj_count}"
        )

        # Check if count has stabilized
        if obj_count == prev_count:
            stable_count += 1
            if stable_count >= stable_threshold:
                log.info(
                    f"Count stabilized at {obj_count} after {attempt + 1} attempts"
                )
                break
        else:
            stable_count = 0

        prev_count = obj_count

        # Don't sleep on last iteration
        if attempt < max_attempts:
            time.sleep(5)

    return obj_count


def get_inconsistent_count(
    scrub_object,
    mon_object,
    pg_id,
    rados_obj,
    operation,
    acting_pg_set,
    user_initiated=False,
):
    """
    Trigger scrub or deep-scrub on a PG and return the inconsistent object count.

    When ``user_initiated`` is ``False`` (default), applies short scrub intervals
    and a scrub time window on the ``osd`` section, then waits up to three minutes
    for the scheduled scrub stamp to advance. When ``user_initiated`` is ``True``,
    triggers an immediate user-initiated scrub or deep-scrub instead.

    Args:
        scrub_object: ``RadosScrubber`` instance.
        mon_object: ``MonConfigMethods`` instance for config cleanup.
        pg_id: Placement group identifier.
        rados_obj: ``RadosOrchestrator`` instance.
        operation: ``"scrub"`` or ``"deep-scrub"``.
        acting_pg_set: List of acting OSD ids for the PG.
        user_initiated: Use scheduled scrub when ``False``; direct trigger when ``True``.

    Returns:
        int: Inconsistent object count after scrub completes, or ``-1`` on timeout.
    """
    if user_initiated:
        return _run_user_initiated_scrub_and_count(
            rados_obj, pg_id, operation, wait_time=180
        )

    operation_chk_flag = False
    osd_scrub_min_interval = 10
    osd_scrub_max_interval = 60
    osd_deep_scrub_interval = 60
    (
        scrub_begin_hour,
        scrub_begin_weekday,
        scrub_end_hour,
        scrub_end_weekday,
    ) = scrub_object.add_begin_end_hours(0, 2)
    log.info(
        f"Configuring scheduled {operation} on PG {pg_id} for acting OSDs {acting_pg_set}. "
        f"Intervals - min: {osd_scrub_min_interval}s, max: {osd_scrub_max_interval}s, "
        f"deep: {osd_deep_scrub_interval}s; scrub window hours "
        f"{scrub_begin_hour}-{scrub_end_hour}, weekdays "
        f"{scrub_begin_weekday}-{scrub_end_weekday}"
    )
    # Set once on osd section instead of per-OSD to avoid many slow config round-trips
    scrub_object.set_osd_configuration("osd_scrub_begin_hour", scrub_begin_hour)
    scrub_object.set_osd_configuration("osd_scrub_begin_week_day", scrub_begin_weekday)
    scrub_object.set_osd_configuration("osd_scrub_end_hour", scrub_end_hour)
    scrub_object.set_osd_configuration("osd_scrub_end_week_day", scrub_end_weekday)
    scrub_object.set_osd_configuration("osd_scrub_min_interval", osd_scrub_min_interval)
    scrub_object.set_osd_configuration("osd_scrub_max_interval", osd_scrub_max_interval)
    scrub_object.set_osd_configuration(
        "osd_deep_scrub_interval", osd_deep_scrub_interval
    )
    endtime = datetime.now() + timedelta(minutes=3)
    log.info(
        f"Waiting up to 3 minutes for scheduled {operation} to complete on PG {pg_id}"
    )

    while datetime.now() <= endtime:
        try:
            if operation == "scrub":
                log.info(f"Checking scheduled scrub completion on PG {pg_id}")
                status = rados_obj.start_check_scrub_complete(
                    pg_id=pg_id, user_initiated=False, wait_time=120
                )
            else:
                log.info(f"Checking scheduled deep-scrub completion on PG {pg_id}")
                status = rados_obj.start_check_deep_scrub_complete(
                    pg_id=pg_id, user_initiated=False, wait_time=120
                )
            if status:
                log.info(f"Scheduled {operation} completed on PG {pg_id}")
                if operation != "scrub":
                    time.sleep(10)
                operation_chk_flag = True
                break
        except Exception as err:
            # start_check_*_complete raises on timeout instead of returning False
            log.error(f"Scheduled {operation} wait failed on PG {pg_id}: {err}")
        log.info(
            f"Scheduled {operation} not yet complete on PG {pg_id}; retrying in 5s"
        )
        time.sleep(5)
    if not operation_chk_flag:
        log.error(
            f"Scheduled {operation} was not completed on PG {pg_id} within the wait window"
        )
        _clear_scheduled_scrub_intervals(mon_object, acting_pg_set)
        return -1
    log.info("Clearing temporary scheduled scrub interval overrides")
    _clear_scheduled_scrub_intervals(mon_object, acting_pg_set)

    obj_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
    log.info(f"Inconsistent object count on PG {pg_id} after {operation}: {obj_count}")

    return obj_count


def _run_user_initiated_scrub_and_count(rados_obj, pg_id, operation, wait_time=180):
    """
    Run a user-initiated scrub or deep-scrub and return the inconsistent object count.

    Args:
        rados_obj: ``RadosOrchestrator`` instance.
        pg_id: Placement group identifier.
        operation: ``"scrub"`` or ``"deep-scrub"``.
        wait_time: Seconds to wait for scrub completion.

    Returns:
        int: Inconsistent object count after scrub, or ``-1`` if scrub did not complete.
    """
    try:
        if operation == "scrub":
            log.info(f"Starting user-initiated scrub on PG {pg_id}")
            status = rados_obj.start_check_scrub_complete(
                pg_id=pg_id, user_initiated=True, wait_time=wait_time
            )
        else:
            log.info(f"Starting user-initiated deep-scrub on PG {pg_id}")
            status = rados_obj.start_check_deep_scrub_complete(
                pg_id=pg_id, user_initiated=True, wait_time=wait_time
            )
            time.sleep(10)
        if not status:
            log.error(f"User-initiated {operation} did not complete on PG {pg_id}")
            return -1
    except Exception as err:
        log.error(f"User-initiated {operation} wait failed on PG {pg_id}: {err}")
        return -1

    obj_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
    log.info(
        f"Inconsistent object count on PG {pg_id} after user-initiated {operation}: "
        f"{obj_count}"
    )
    return obj_count


def _clear_scheduled_scrub_intervals(mon_object, acting_pg_set):
    """
    Remove temporary scrub schedule overrides from mon config.

    Clears interval and scrub-window settings from the ``osd`` section and from
    each acting OSD section that may have inherited overrides.

    Args:
        mon_object: ``MonConfigMethods`` instance.
        acting_pg_set: List of acting OSD ids for the PG under test.
    """
    log.info(
        f"Removing scrub schedule overrides (osd section and OSDs: {acting_pg_set})"
    )
    for section in ["osd"] + [f"osd.{osd_id}" for osd_id in acting_pg_set]:
        mon_object.remove_config(section=section, name="osd_scrub_min_interval")
        mon_object.remove_config(section=section, name="osd_scrub_max_interval")
        mon_object.remove_config(section=section, name="osd_deep_scrub_interval")
        mon_object.remove_config(section=section, name="osd_scrub_begin_hour")
        mon_object.remove_config(section=section, name="osd_scrub_begin_week_day")
        mon_object.remove_config(section=section, name="osd_scrub_end_hour")
        mon_object.remove_config(section=section, name="osd_scrub_end_week_day")


def verify_pg_state(rados_obj, pg_id):
    """
    Check whether a PG has cleared the ``repair`` state after auto-repair.

    Args:
        rados_obj: ``RadosOrchestrator`` instance.
        pg_id: Placement group identifier.

    Returns:
        bool: ``True`` if ``repair`` is not present in PG state, else ``False``.
    """
    pool_pg_dump = rados_obj.get_ceph_pg_dump(pg_id=pg_id)
    pg_state = pool_pg_dump["state"]
    log.info(f"The pg status is -{pg_state}")
    if "repair" not in pg_state:
        return True
    return False


def set_ecpool_inconsistent_default_param_value(mon_obj, scrub_obj):
    """
    Restore scrub and auto-repair settings changed during the test.

    Removes monitor overrides for auto-repair, scrub intervals, scrub windows,
    and debug logging; unsets ``noscrub`` and ``nodeep-scrub`` OSD flags.

    Args:
        mon_obj: ``MonConfigMethods`` instance.
        scrub_obj: ``RadosScrubber`` instance.
    """
    mon_obj.remove_config(section="osd", name="osd_scrub_auto_repair_num_errors")
    mon_obj.remove_config(section="osd", name="osd_scrub_auto_repair")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_min_interval")
    mon_obj.remove_config(section="osd", name="osd_scrub_max_interval")
    mon_obj.remove_config(section="osd", name="osd_deep_scrub_interval")
    mon_obj.remove_config(section="osd", name="debug_osd")
    scrub_obj.set_osd_flags("unset", "noscrub")
    scrub_obj.set_osd_flags("unset", "nodeep-scrub")
    mon_obj.remove_config(section="global", name="osd_pool_default_pg_autoscale_mode")
    mon_obj.remove_config(section="mgr", name="debug_mgr")
    time.sleep(10)


def check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
    """
    Wait until no scrub is in progress on the given PG.

    Args:
        rados_obj: ``RadosOrchestrator`` instance.
        pg_id: Placement group identifier.
        wait_time: Maximum wait in minutes before giving up.

    Returns:
        bool: ``True`` when scrubbing is not in PG state; ``False`` on timeout or error.
    """
    end_time = datetime.now() + timedelta(minutes=wait_time)
    while end_time > datetime.now():
        try:
            pg_state = rados_obj.get_pg_state(pg_id=pg_id)
            if "scrubbing" in pg_state:
                log.info("Scrubbing in progress, waiting 5 seconds...")
                time.sleep(5)
            else:
                log.info("No scrub operations running.")
                return True
        except Exception as err:
            log.error(f"PGID : {pg_id} was not found, err: {err}")
            return False
    log.info("Timeout reached, scrubbing still in progress.")
    return False


def _parse_osd_config_get(raw_output, key):
    """
    Parse ``ceph tell osd.<id> config get <key>`` command output.

    Newer Ceph releases return JSON (e.g. ``{"osd_scrub_auto_repair_num_errors": "3"}``);
    older builds may return a bare scalar string.

    Args:
        raw_output: Shell command stdout.
        key: Config option name to extract from JSON output.

    Returns:
        str: Parsed config value, or stripped raw output when JSON parsing fails.
    """
    raw = str(raw_output).strip()
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and key in data:
            return str(data[key]).strip()
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return raw


def _inject_and_verify_auto_repair(
    rados_obj, acting_pg_set, auto_repair, num_errors, case_id
):
    """
    Push auto-repair settings into acting OSD memory and verify them.

    Monitor ``config set`` alone can leave OSD daemons on default in-memory values
    briefly. Cases 5 and 6 use this helper so deep-scrub runs with the expected
    ``osd_scrub_auto_repair`` and ``osd_scrub_auto_repair_num_errors`` values.

    Args:
        rados_obj: ``RadosOrchestrator`` instance.
        acting_pg_set: List of acting OSD ids for the PG.
        auto_repair: Expected ``osd_scrub_auto_repair`` value (``"true"`` or ``"false"``).
        num_errors: Expected ``osd_scrub_auto_repair_num_errors`` value.
        case_id: Case identifier for log prefixes (e.g. ``"case5"``).

    Returns:
        bool: ``True`` when all acting OSDs report matching in-memory config.
    """
    prefix = CASE_LOG_PREFIX[case_id]
    for osd_id in acting_pg_set:
        inject_cmd = (
            f"ceph tell osd.{osd_id} injectargs "
            f"--osd_scrub_auto_repair_num_errors={num_errors} "
            f"--osd_scrub_auto_repair={auto_repair}"
        )
        log.info(f"[{prefix}] Applying auto-repair config on osd.{osd_id}")
        rados_obj.node.shell([inject_cmd])

    time.sleep(2)

    for osd_id in acting_pg_set:
        num_out, _ = rados_obj.node.shell(
            [f"ceph tell osd.{osd_id} config get osd_scrub_auto_repair_num_errors"]
        )
        repair_out, _ = rados_obj.node.shell(
            [f"ceph tell osd.{osd_id} config get osd_scrub_auto_repair"]
        )
        runtime_num = _parse_osd_config_get(num_out, "osd_scrub_auto_repair_num_errors")
        runtime_repair = _parse_osd_config_get(
            repair_out, "osd_scrub_auto_repair"
        ).lower()
        log.info(
            f"[{prefix}] In-memory config on osd.{osd_id} - "
            f"osd_scrub_auto_repair_num_errors={runtime_num}, "
            f"osd_scrub_auto_repair={runtime_repair}"
        )
        if runtime_num in ("null", "") or int(runtime_num) != int(num_errors):
            log_case_failure(
                case_id,
                f"osd.{osd_id} in-memory osd_scrub_auto_repair_num_errors mismatch: "
                f"expected {num_errors}, got {runtime_num}",
            )
            return False
        expected = str(auto_repair).strip().lower()
        if not (
            (expected == "true" and runtime_repair in ("true", "1"))
            or (expected == "false" and runtime_repair in ("false", "0"))
        ):
            log_case_failure(
                case_id,
                f"osd.{osd_id} in-memory osd_scrub_auto_repair mismatch: "
                f"expected {auto_repair}, got {runtime_repair}",
            )
            return False

    log.info(f"[{prefix}] Auto-repair config verified on acting OSDs {acting_pg_set}")
    return True


def _get_inconsistent_count_once(rados_obj, pg_id):
    """
    Read the current inconsistent object count for a PG (single query).

    Args:
        rados_obj: ``RadosOrchestrator`` instance.
        pg_id: Placement group identifier.

    Returns:
        int: Number of inconsistent objects reported for the PG.
    """
    inconsistent_details = rados_obj.get_inconsistent_object_details(pg_id)
    return len(inconsistent_details["inconsistents"])


def log_case_start(case_id, description, operation, expectation):
    """
    Log the start banner for a test case.

    Args:
        case_id: Case identifier (``"case1"``..``"case6"``).
        description: Short summary of the scenario under test.
        operation: Scrub type (``"scrub"`` or ``"deep-scrub"``).
        expectation: Expected outcome text for the log banner.
    """
    prefix = CASE_LOG_PREFIX[case_id]
    log.info(
        f"\n{'=' * 70}\n"
        f"[{prefix}] Starting: {description}\n"
        f"[{prefix}] Operation: {operation}\n"
        f"[{prefix}] Expectation: {expectation}\n"
        f"{'=' * 70}"
    )


def log_case_parameters(
    case_id, inconsistent_count, auto_repair_num_errors, auto_repair, operation
):
    """
    Log auto-repair parameters configured immediately before scrub/deep-scrub.

    Args:
        case_id: Case identifier (``"case1"``..``"case6"``).
        inconsistent_count: Inconsistent object count before the operation.
        auto_repair_num_errors: Value set for ``osd_scrub_auto_repair_num_errors``.
        auto_repair: Value set for ``osd_scrub_auto_repair`` (``"True"``/``"False"``).
        operation: Scrub type (``"scrub"`` or ``"deep-scrub"``).
    """
    prefix = CASE_LOG_PREFIX[case_id]
    log.info(
        f"[{prefix}] Parameters - inconsistent object count: {inconsistent_count}, "
        f"osd_scrub_auto_repair_num_errors: {auto_repair_num_errors}, "
        f"osd_scrub_auto_repair: {auto_repair}, operation: {operation}"
    )


def log_case_complete(
    case_id, operation, expectation, inconsistent_count_before, inconsistent_count_after
):
    """
    Log successful completion of a test case with before/after counts.

    Args:
        case_id: Case identifier (``"case1"``..``"case6"``).
        operation: Scrub type that was executed.
        expectation: Expected outcome that was validated.
        inconsistent_count_before: Inconsistent count before scrub/deep-scrub.
        inconsistent_count_after: Inconsistent count after scrub/deep-scrub.
    """
    prefix = CASE_LOG_PREFIX[case_id]
    log.info(
        f"[{prefix}] Completed successfully - operation: {operation}, "
        f"expectation: {expectation}, inconsistent count before: "
        f"{inconsistent_count_before}, after: {inconsistent_count_after}"
    )


def log_case_failure(case_id, message):
    """
    Log a test case failure with the case-specific prefix.

    Args:
        case_id: Case identifier (``"case1"``..``"case6"``).
        message: Failure reason shown in the log and used for triage.
    """
    log.error(f"[{CASE_LOG_PREFIX[case_id]}] Failed - {message}")
