"""
RADOS OMAP Performance and Functionality Testing

This module tests OMAP operations on objects created by fill_omap_entries().

Objects are created using fill_omap_entries() method which:
- Creates objects named: omap_obj_{PID}_{index} (e.g., omap_obj_12345_0)
- Adds OMAP keys named: key_0, key_1, key_2, ... key_N
- Adds OMAP values named: value_0, value_1, value_2, ... value_N

Default Scale:
- 500 objects with 10,000 OMAP keys each = 5,000,000 total OMAP entries
- Uses efficient librados Python API for bulk creation

Tests include:
- Direct OMAP read operations (listomapkeys, listomapvals, getomapval)
- OMAP iteration and filtering
- Performance timing measurements
"""

import time
from typing import Dict, List

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from utility.log import Log

log = Log(__name__)

DEFAULT_NUM_OBJECTS = 500
DEFAULT_OMAP_KEYS_PER_OBJECT = 10000


def get_first_omap_object(rados_obj, pool: str, prefix: str = "omap_obj_") -> str:
    """
    Get first OMAP object from the pool

    Args:
        rados_obj: RadosOrchestrator object
        pool: Pool name
        prefix: Object name prefix to filter

    Returns:
        First object name or empty string
    """
    cmd = "rados -p %s ls" % pool
    objects = rados_obj.run_ceph_command(cmd=cmd)

    if objects:
        for obj in objects:
            obj_name = obj.get("name", "")
            if obj_name.startswith(prefix):
                log.info("Using test object: %s", obj_name)
                return obj_name

    return ""


def test_listomapkeys(node, pool: str, obj: str, max_return: int = None) -> Dict:
    """
    Test rados listomapkeys command

    Args:
        node: Client node
        pool: Pool name
        obj: Object name
        max_return: Maximum number of keys to return

    Returns:
        Dict with execution time and key count
    """
    cmd = "rados -p %s listomapkeys %s" % (pool, obj)
    if max_return:
        cmd += " --max-return %s" % max_return

    start_time = time.time()
    out, _ = node.exec_command(cmd=cmd, sudo=True, timeout=300)
    elapsed = time.time() - start_time

    key_count = len([line for line in out.split("\n") if line.strip()])

    return {"elapsed": elapsed, "key_count": key_count}


def test_listomapvals(
    node,
    pool: str,
    obj: str,
    max_return: int = None,
    start_after: str = None,
    filter_prefix: str = None,
) -> Dict:
    """
    Test rados listomapvals command with various parameters

    Args:
        node: Client node
        pool: Pool name
        obj: Object name
        max_return: Maximum number of entries to return
        start_after: Start listing after this key
        filter_prefix: Filter keys by prefix

    Returns:
        Dict with execution time and entry count
    """
    cmd = "rados -p %s listomapvals %s" % (pool, obj)
    if max_return:
        cmd += " --max-return %s" % max_return
    if start_after:
        cmd += " --start-after %s" % start_after
    if filter_prefix:
        cmd += " --filter-prefix %s" % filter_prefix

    start_time = time.time()
    out, _ = node.exec_command(cmd=cmd, sudo=True, timeout=300)
    elapsed = time.time() - start_time

    # Count key-value pairs in output
    entry_count = out.count("key_")

    return {"elapsed": elapsed, "entry_count": entry_count}


def test_getomapval(node, pool: str, obj: str, keys: List[str]) -> Dict:
    """
    Test rados getomapval command for multiple keys

    Args:
        node: Client node
        pool: Pool name
        obj: Object name
        keys: List of keys to retrieve

    Returns:
        Dict with total elapsed time and success count
    """
    start_time = time.time()
    success_count = 0

    for key in keys:
        cmd = "rados -p %s getomapval %s %s" % (pool, obj, key)
        _, err = node.exec_command(cmd=cmd, sudo=True, check_ec=False, timeout=30)
        if not err:
            success_count += 1

    elapsed = time.time() - start_time

    return {
        "elapsed": elapsed,
        "total_keys": len(keys),
        "success_count": success_count,
        "avg_per_key": elapsed / len(keys) if keys else 0,
    }


# Test configuration
TEST_CONFIG = {
    "list_keys_basic": {
        "desc": "List OMAP keys with varying max-return values",
        "operation": "listomapkeys",
        "tests": [
            {"max_return": None},  # List all keys
            {"max_return": 5},
            {"max_return": 10},
            {"max_return": 50},
            {"max_return": 100},
            {"max_return": 500},
            {"max_return": 1000},
            {"max_return": 5000},
        ],
    },
    "list_vals_basic": {
        "desc": "List OMAP key-value pairs with varying max-return",
        "operation": "listomapvals",
        "tests": [
            {"max_return": 5},
            {"max_return": 10},
            {"max_return": 50},
            {"max_return": 100},
            {"max_return": 500},
            {"max_return": 1000},
            {"max_return": 5000},
        ],
    },
    "list_vals_start_after": {
        "desc": "List OMAP vals with start-after parameter (keys: key_0 to key_9999)",
        "operation": "listomapvals",
        "tests": [
            {"max_return": 100, "start_after": "key_0"},
            {"max_return": 100, "start_after": "key_500"},
            {"max_return": 100, "start_after": "key_1000"},
            {"max_return": 100, "start_after": "key_2500"},
            {"max_return": 100, "start_after": "key_5000"},
            {"max_return": 100, "start_after": "key_7500"},
            {"max_return": 100, "start_after": "key_9000"},
            {"max_return": 500, "start_after": "key_0"},
            {"max_return": 500, "start_after": "key_5000"},
            {"max_return": 1000, "start_after": "key_0"},
            {"max_return": 1000, "start_after": "key_5000"},
        ],
    },
    "list_vals_filter": {
        "desc": "List OMAP vals with filter-prefix parameter",
        "operation": "listomapvals",
        "tests": [
            {"max_return": 100, "filter_prefix": "key"},
            {"max_return": 100, "filter_prefix": "key_"},
            {"max_return": 100, "filter_prefix": "key_0"},
            {"max_return": 100, "filter_prefix": "key_1"},
            {"max_return": 100, "filter_prefix": "key_2"},
            {"max_return": 100, "filter_prefix": "key_5"},
            {"max_return": 100, "filter_prefix": "key_9"},
            {"max_return": 500, "filter_prefix": "key_"},
            {"max_return": 500, "filter_prefix": "key_1"},
            {"max_return": 1000, "filter_prefix": "key_"},
        ],
    },
    "list_vals_combined": {
        "desc": "List OMAP vals with combined parameters (max-return + start-after + filter)",
        "operation": "listomapvals",
        "tests": [
            {"max_return": 50, "start_after": "key_0", "filter_prefix": "key_"},
            {"max_return": 50, "start_after": "key_1000", "filter_prefix": "key_"},
            {"max_return": 50, "start_after": "key_5000", "filter_prefix": "key_"},
            {"max_return": 100, "start_after": "key_0", "filter_prefix": "key_1"},
            {"max_return": 100, "start_after": "key_1000", "filter_prefix": "key_1"},
            {"max_return": 100, "start_after": "key_5000", "filter_prefix": "key_5"},
            {"max_return": 200, "start_after": "key_0", "filter_prefix": "key_"},
            {"max_return": 200, "start_after": "key_2500", "filter_prefix": "key_"},
            {"max_return": 500, "start_after": "key_0", "filter_prefix": "key_"},
            {"max_return": 500, "start_after": "key_5000", "filter_prefix": "key_"},
        ],
    },
    "list_vals_edge_cases": {
        "desc": "Edge case tests for OMAP iteration",
        "operation": "listomapvals",
        "tests": [
            {"max_return": 1},  # Minimum return
            {"max_return": 10000},  # All keys in one call
            {"max_return": 50, "start_after": "key_9900"},  # Near end
            {"max_return": 100, "filter_prefix": "key_99"},  # Last hundred keys
            {"max_return": 1000, "start_after": "key_9000", "filter_prefix": "key_9"},
        ],
    },
    "get_values_single": {
        "desc": "Retrieve individual OMAP values (single key tests)",
        "operation": "getomapval",
        "tests": [
            {"keys": ["key_0"]},
            {"keys": ["key_100"]},
            {"keys": ["key_1000"]},
            {"keys": ["key_5000"]},
            {"keys": ["key_9999"]},
        ],
    },
    "get_values_batch": {
        "desc": "Retrieve multiple OMAP values (batch tests)",
        "operation": "getomapval",
        "tests": [
            {"keys": ["key_0", "key_100", "key_500", "key_1000", "key_5000"]},
            {"keys": ["key_%s" % i for i in range(0, 100, 10)]},  # 10 keys
            {"keys": ["key_%s" % i for i in range(0, 500, 50)]},  # 10 keys
            {"keys": ["key_%s" % i for i in range(0, 1000, 100)]},  # 10 keys
            {"keys": ["key_%s" % i for i in range(0, 10000, 1000)]},  # 10 keys
            {
                "keys": [
                    "key_%s" % i
                    for i in [0, 1, 2, 5, 10, 50, 100, 500, 1000, 5000, 9999]
                ]
            },  # 11 keys
        ],
    },
    "get_values_sequential": {
        "desc": "Retrieve sequential OMAP values",
        "operation": "getomapval",
        "tests": [
            {"keys": ["key_%s" % i for i in range(0, 20)]},  # First 20 keys
            {"keys": ["key_%s" % i for i in range(100, 120)]},  # 20 keys from middle
            {"keys": ["key_%s" % i for i in range(9980, 10000)]},  # Last 20 keys
        ],
    },
    "list_vals_stress": {
        "desc": "Stress test with large max-return values across different positions",
        "operation": "listomapvals",
        "tests": [
            {"max_return": 2000},
            {"max_return": 3000},
            {"max_return": 5000},
            {"max_return": 2000, "start_after": "key_2000"},
            {"max_return": 2000, "start_after": "key_5000"},
            {"max_return": 3000, "start_after": "key_3000"},
            {"max_return": 5000, "start_after": "key_5000"},
        ],
    },
    "list_vals_precision_filter": {
        "desc": "Precise filter-prefix tests for different digit patterns",
        "operation": "listomapvals",
        "tests": [
            {"max_return": 200, "filter_prefix": "key_0"},
            {"max_return": 200, "filter_prefix": "key_1"},
            {"max_return": 200, "filter_prefix": "key_2"},
            {"max_return": 200, "filter_prefix": "key_3"},
            {"max_return": 200, "filter_prefix": "key_4"},
            {"max_return": 200, "filter_prefix": "key_5"},
            {"max_return": 200, "filter_prefix": "key_6"},
            {"max_return": 200, "filter_prefix": "key_7"},
            {"max_return": 200, "filter_prefix": "key_8"},
            {"max_return": 200, "filter_prefix": "key_9"},
            {"max_return": 100, "filter_prefix": "key_10"},
            {"max_return": 100, "filter_prefix": "key_20"},
            {"max_return": 100, "filter_prefix": "key_50"},
        ],
    },
    "list_vals_boundary": {
        "desc": "Boundary tests at different keyspace positions",
        "operation": "listomapvals",
        "tests": [
            {"max_return": 10, "start_after": "key_0"},
            {"max_return": 10, "start_after": "key_99"},
            {"max_return": 10, "start_after": "key_999"},
            {"max_return": 10, "start_after": "key_9999"},
            {"max_return": 100, "start_after": "key_0", "filter_prefix": "key_0"},
            {"max_return": 100, "start_after": "key_1000", "filter_prefix": "key_1"},
            {"max_return": 100, "start_after": "key_5000", "filter_prefix": "key_5"},
            {"max_return": 100, "start_after": "key_9000", "filter_prefix": "key_9"},
        ],
    },
    "list_vals_small_chunks": {
        "desc": "Small chunk iteration patterns",
        "operation": "listomapvals",
        "tests": [
            {"max_return": 10, "start_after": "key_0"},
            {"max_return": 10, "start_after": "key_100"},
            {"max_return": 10, "start_after": "key_500"},
            {"max_return": 10, "start_after": "key_1000"},
            {"max_return": 10, "start_after": "key_5000"},
            {"max_return": 25, "start_after": "key_0"},
            {"max_return": 25, "start_after": "key_1000"},
            {"max_return": 25, "start_after": "key_5000"},
        ],
    },
    "list_vals_medium_chunks": {
        "desc": "Medium chunk iteration patterns (100-500 entries)",
        "operation": "listomapvals",
        "tests": [
            {"max_return": 150, "start_after": "key_0"},
            {"max_return": 150, "start_after": "key_1000"},
            {"max_return": 150, "start_after": "key_5000"},
            {"max_return": 250, "start_after": "key_0"},
            {"max_return": 250, "start_after": "key_2500"},
            {"max_return": 250, "start_after": "key_5000"},
            {"max_return": 350, "start_after": "key_0"},
            {"max_return": 350, "start_after": "key_3000"},
            {"max_return": 450, "start_after": "key_0"},
            {"max_return": 450, "start_after": "key_5000"},
        ],
    },
    "list_vals_complex_filter": {
        "desc": "Complex filter combinations with various prefixes",
        "operation": "listomapvals",
        "tests": [
            {"max_return": 50, "start_after": "key_100", "filter_prefix": "key_1"},
            {"max_return": 50, "start_after": "key_200", "filter_prefix": "key_2"},
            {"max_return": 50, "start_after": "key_500", "filter_prefix": "key_5"},
            {"max_return": 100, "start_after": "key_1500", "filter_prefix": "key_1"},
            {"max_return": 100, "start_after": "key_2500", "filter_prefix": "key_2"},
            {"max_return": 100, "start_after": "key_7500", "filter_prefix": "key_7"},
            {"max_return": 200, "start_after": "key_1000", "filter_prefix": "key_1"},
            {"max_return": 200, "start_after": "key_5000", "filter_prefix": "key_5"},
            {"max_return": 200, "start_after": "key_8000", "filter_prefix": "key_8"},
        ],
    },
    "get_values_distributed": {
        "desc": "Distributed key retrieval patterns",
        "operation": "getomapval",
        "tests": [
            {"keys": ["key_%s" % i for i in range(0, 1000, 100)]},  # 10 keys, every 100
            {"keys": ["key_%s" % i for i in range(0, 2000, 200)]},  # 10 keys, every 200
            {"keys": ["key_%s" % i for i in range(0, 5000, 500)]},  # 10 keys, every 500
            {
                "keys": ["key_%s" % i for i in range(1000, 6000, 500)]
            },  # 10 keys, middle range
            {
                "keys": ["key_%s" % i for i in range(5000, 10000, 500)]
            },  # 10 keys, upper range
        ],
    },
    "get_values_sparse": {
        "desc": "Sparse key retrieval across entire keyspace",
        "operation": "getomapval",
        "tests": [
            {
                "keys": [
                    "key_0",
                    "key_1111",
                    "key_2222",
                    "key_3333",
                    "key_4444",
                    "key_5555",
                    "key_6666",
                    "key_7777",
                    "key_8888",
                    "key_9999",
                ]
            },
            {
                "keys": [
                    "key_%s" % i
                    for i in [10, 210, 510, 1010, 2010, 4010, 6010, 8010, 9510]
                ]
            },
            {
                "keys": [
                    "key_%s" % i for i in [50, 550, 1050, 2050, 3050, 5050, 7050, 9050]
                ]
            },
        ],
    },
    "get_values_dense": {
        "desc": "Dense consecutive key retrieval",
        "operation": "getomapval",
        "tests": [
            {"keys": ["key_%s" % i for i in range(0, 50)]},  # First 50 keys
            {"keys": ["key_%s" % i for i in range(500, 550)]},  # 50 keys from middle
            {"keys": ["key_%s" % i for i in range(1000, 1050)]},  # 50 keys
            {
                "keys": ["key_%s" % i for i in range(5000, 5050)]
            },  # 50 keys from upper middle
            {"keys": ["key_%s" % i for i in range(9950, 10000)]},  # Last 50 keys
        ],
    },
    "list_keys_with_filters": {
        "desc": "List keys with different max-return and implicit filtering",
        "operation": "listomapkeys",
        "tests": [
            {"max_return": 100},
            {"max_return": 250},
            {"max_return": 500},
            {"max_return": 750},
            {"max_return": 1500},
            {"max_return": 2500},
            {"max_return": 7500},
        ],
    },
}


def run(ceph_cluster, **kw):
    """
    Test OMAP operations on objects created by fill_omap_entries()
    """
    log.info("Starting RADOS OMAP Performance and Functionality Testing")

    config = kw.get("config", {})
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client = ceph_cluster.get_nodes(role="client")[0]

    pool_name = config.get("pool_name", "omap_test_pool")
    pg_num = config.get("pg_num", 32)

    # OMAP configuration
    num_objects = config.get("num_objects", DEFAULT_NUM_OBJECTS)
    num_keys = config.get("num_keys_per_object", DEFAULT_OMAP_KEYS_PER_OBJECT)

    results = []

    try:
        # Create test pool
        log.info("Creating pool: %s", pool_name)
        rados_obj.create_pool(pool_name=pool_name, pg_num=pg_num)
        time.sleep(5)

        # Populate pool with OMAP objects using fill_omap_entries
        log.info("\n%s", "=" * 80)
        log.info("SETUP: Populating pool with OMAP objects")
        log.info("%s", "=" * 80)
        log.info(
            "Creating %s objects with %s OMAP keys each (Total: %s OMAP entries)",
            num_objects,
            num_keys,
            num_objects * num_keys,
        )

        if not pool_obj.fill_omap_entries(
            pool_name=pool_name,
            obj_start=0,
            obj_end=num_objects,
            num_keys_obj=num_keys,
            retain_script=False,
        ):
            log.error("Failed to populate pool with OMAP entries")
            return 1

        log.info("OMAP objects created successfully")
        time.sleep(5)

        # Get first object for testing
        test_obj = get_first_omap_object(rados_obj, pool_name, prefix="omap_obj_")
        if not test_obj:
            log.error("No OMAP objects found in pool")
            return 1

        # Run all test scenarios
        for scenario_name, scenario in TEST_CONFIG.items():
            log.info("\n%s", "=" * 80)
            log.info("SCENARIO: %s - %s", scenario_name.upper(), scenario["desc"])
            log.info("%s", "=" * 80)

            operation = scenario["operation"]

            for idx, test in enumerate(scenario["tests"], 1):
                log.info(
                    "Test %s/%s: %s %s", idx, len(scenario["tests"]), operation, test
                )

                try:
                    if operation == "listomapkeys":
                        result = test_listomapkeys(
                            client,
                            pool_name,
                            test_obj,
                            max_return=test.get("max_return"),
                        )
                        if result.get("key_count", 0) <= 0:
                            raise RuntimeError(
                                "Empty output: listomapkeys returned 0 keys"
                            )
                        log.info(
                            "Result: %s keys in %.2fs (%.2f keys/sec)",
                            result["key_count"],
                            result["elapsed"],
                            (
                                result["key_count"] / result["elapsed"]
                                if result["elapsed"] > 0
                                else 0
                            ),
                        )

                    elif operation == "listomapvals":
                        result = test_listomapvals(
                            client,
                            pool_name,
                            test_obj,
                            max_return=test.get("max_return"),
                            start_after=test.get("start_after"),
                            filter_prefix=test.get("filter_prefix"),
                        )
                        if result.get("entry_count", 0) <= 0:
                            raise RuntimeError(
                                "Empty output: listomapvals returned 0 entries"
                            )
                        log.info(
                            "Result: %s entries in %.2fs (%.2f entries/sec)",
                            result["entry_count"],
                            result["elapsed"],
                            (
                                result["entry_count"] / result["elapsed"]
                                if result["elapsed"] > 0
                                else 0
                            ),
                        )

                    elif operation == "getomapval":
                        result = test_getomapval(
                            client,
                            pool_name,
                            test_obj,
                            keys=test.get("keys", []),
                        )
                        if result.get("success_count", 0) <= 0:
                            raise RuntimeError(
                                "Empty output: getomapval returned 0 successful keys"
                            )
                        log.info(
                            "Result: %s/%s keys retrieved in %.2fs (avg: %.4fs per key)",
                            result["success_count"],
                            result["total_keys"],
                            result["elapsed"],
                            result["avg_per_key"],
                        )

                    results.append((scenario_name, test, "PASSED"))

                except Exception as e:
                    log.error("Failed: %s", str(e))
                    results.append((scenario_name, test, "FAILED: %s" % str(e)))

        # Summary
        log.info("\n%s", "=" * 80)
        log.info("TEST SUMMARY")
        log.info("%s", "=" * 80)

        passed_count = sum(1 for _, _, status in results if "PASSED" in status)
        total_count = len(results)

        log.info(
            "Total: %s | Passed: %s | Failed: %s",
            total_count,
            passed_count,
            total_count - passed_count,
        )

        if passed_count == total_count:
            log.info("ALL TESTS PASSED")
            return 0
        else:
            log.error("%s tests failed", total_count - passed_count)
            return 1

    except Exception as e:
        log.error("Test execution failed: %s", str(e), exc_info=True)
        return 1

    finally:
        # Cleanup
        log.info("\nCleaning up pool: %s", pool_name)
        rados_obj.delete_pool(pool=pool_name)
        log.info("Cleanup completed")
        # Log cluster health
        rados_obj.log_cluster_health()

        # Check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
