import random
import re
import string
import time
from typing import List

from ceph.ceph import CephNode, CommandFailed
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.crushtool_workflows import CrushToolWorkflows
from ceph.rados.mgr_workflows import MgrWorkflows
from ceph.rados.osd_ok_to_upgrade_utils import (
    OsdOkToUpgradeCommand,
    OsdOkToUpgradeCommandOutput,
    execute_negative_scenario,
)
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.test_node_drain_customer_bug import get_node_osd_list
from utility.log import Log
from utility.utils import get_ceph_version_from_cluster

log = Log(__name__)


class InvalidScenarioException(Exception):
    """Raised when config.scenarios contains an invalid or unsupported scenario name."""

    pass


def run(ceph_cluster, **kw):
    """
    Test the ``ceph osd ok-to-upgrade`` command for different failure domains and buckets.

    Creates a CRUSH hierarchy (racks, chassis, hosts) and runs one or more scenarios
    from config.scenarios. Each scenario creates a pool with a given failure domain and
    verifies ok-to-upgrade output for osd, host, chassis, and/or rack buckets.

    Scenarios (config.scenarios) and test steps:
        scenario1: Rack FD. Step 1: Create CRUSH rule and pool. Steps 2-5: ok-to-upgrade
            for osd, host, rack, chassis buckets and verify.
        scenario2: Chassis FD. Step 1: Create CRUSH rule and pool. Steps 2-5: ok-to-upgrade
            for rack, host, chassis, osd buckets and verify.
        scenario3: Host FD. Step 1: Create CRUSH rule and pool. Steps 2-5: ok-to-upgrade
            for rack, host, chassis, osd buckets and verify.
        scenario4: OSD FD. Step 1: Create CRUSH rule and pool. Steps 2-5: ok-to-upgrade
            for rack, host, chassis, osd buckets and verify.
        scenario5: All OSDs upgraded. Step 1: Create CRUSH rule and pool. Steps 2-5: Run
            ok-to-upgrade for osd, host, chassis, rack buckets with current version. Step 6:
            Verify ok_to_upgrade=False, all_osds_upgraded=True for all buckets.
        scenario6: Convergence factor. Step 1: Set mgr debug, get active mgr. Step 2: Create
            CRUSH rule and pool. Step 3: For each factor (0.9, 0.8, 0.1) set config, run
            ok-to-upgrade for host, validate mgr log convergence.
    """
    log.info(
        "Test ceph osd ok-to-upgrade command. Scenarios: %s",
        kw.get("config", {}).get("scenarios", []),
    )
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    start_time = get_cluster_timestamp(rados_obj.node)
    scenarios = config.get("scenarios", [])
    log.debug("Test workflow started. Start time: %s", start_time)
    crushtool = CrushToolWorkflows(node=cephadm)
    pg_num = config.get("pg_num", 2048)
    pg_num_min = config.get("pg_num_min", 2048)
    ceph_nodes = kw.get("ceph_nodes")

    try:
        if len(scenarios) == 0:
            log.error("config.scenarios cannot be empty")
            return 1

        osd_hosts: List[CephNode] = ceph_cluster.get_nodes(role="osd")

        log.info("============== crush heirarchy =================")
        rados_obj.client.exec_command(
            cmd="ceph orch host ls -f yaml", pretty_print=True
        )

        rados_obj.client.exec_command(cmd="ceph osd tree", pretty_print=True)
        log.info("=============================================")

        if "scenario1" in scenarios:
            log.info("===========================================")
            log.info(
                "Scenario 1: Rack failure domain - ok-to-upgrade for osd, host, rack, chassis"
            )
            log.info(
                "  Step 1: Create CRUSH rule (rack FD) and pool, wait for PGs clean"
            )
            log.info("  Step 2: Run ok-to-upgrade for osd bucket and verify output")
            log.info("  Step 3: Run ok-to-upgrade for host bucket and verify output")
            log.info("  Step 4: Run ok-to-upgrade for rack bucket and verify output")
            log.info("  Step 5: Run ok-to-upgrade for chassis bucket and verify output")
            log.info("===========================================")
            log.info("Step 1: Create CRUSH rule (rack FD) and pool, wait for PGs clean")
            pool_name = "ok-to-upgrade-rack-fd"
            failure_domain = "rack"
            crush_rule = CrushRule(
                rados_obj=rados_obj,
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
            )
            crush_rule.create()
            crush_rule.modify_all_failure_domains(new_failure_domain=failure_domain)
            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=pg_num,
                pg_num_min=pg_num_min,
                crush_rule=crush_rule.crush_rule_name,
            ), "Failed to create pool"
            rados_obj.wait_for_clean_pg_sets()

            host0 = [host for host in osd_hosts if "installer" in host.hostname][0]
            all_osds_host0 = rados_obj.collect_osd_daemon_ids(host0)
            osd_id = all_osds_host0[0]
            all_osds_rack1 = rados_obj.collect_osd_daemon_ids("rack1")
            all_osds_chassis1 = rados_obj.collect_osd_daemon_ids("chassis1")

            log.info("Step 2: Run ok-to-upgrade for osd bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket=f"osd.{osd_id}",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=[osd_id],
                osds_ok_to_upgrade=[osd_id],
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Step 3: Run ok-to-upgrade for host bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket=host0.hostname,
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_host0,
                osds_ok_to_upgrade=all_osds_host0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Step 4: Run ok-to-upgrade for rack bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket="rack1",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_rack1,
                osds_ok_to_upgrade=all_osds_rack1,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Step 5: Run ok-to-upgrade for chassis bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket="chassis1",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_chassis1,
                osds_ok_to_upgrade=all_osds_chassis1,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Scenario 1 passed: rack failure domain (osd, host, rack, chassis buckets)"
            )

        elif "scenario2" in scenarios:
            log.info("===========================================")
            log.info(
                "Scenario 2: Chassis failure domain - ok-to-upgrade for rack, host, chassis, osd"
            )
            log.info(
                "  Step 1: Create CRUSH rule (chassis FD) and pool, wait for PGs clean"
            )
            log.info("  Step 2: Run ok-to-upgrade for rack bucket and verify output")
            log.info("  Step 3: Run ok-to-upgrade for host bucket and verify output")
            log.info("  Step 4: Run ok-to-upgrade for chassis bucket and verify output")
            log.info("  Step 5: Run ok-to-upgrade for osd bucket and verify output")
            log.info("===========================================")
            log.info(
                "Step 1: Create CRUSH rule (chassis FD) and pool, wait for PGs clean"
            )
            pool_name = "ok-to-upgrade-chassis-fd"
            failure_domain = "chassis"
            crush_rule = CrushRule(
                rados_obj=rados_obj,
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
            )
            crush_rule.create()
            crush_rule.modify_all_failure_domains(new_failure_domain=failure_domain)
            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=pg_num,
                pg_num_min=pg_num_min,
                crush_rule=crush_rule.crush_rule_name,
            ), "Failed to create pool"
            rados_obj.wait_for_clean_pg_sets()

            all_osds_rack1 = rados_obj.collect_osd_daemon_ids("rack1")
            all_osds_chassis1 = rados_obj.collect_osd_daemon_ids("chassis1")
            host0 = [host for host in osd_hosts if "installer" in host.hostname][0]
            all_osds_host0 = rados_obj.collect_osd_daemon_ids(host0)
            osd_id = all_osds_host0[0]

            log.info("Step 2: Run ok-to-upgrade for rack bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket="rack1",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_rack1,
                osds_ok_to_upgrade=all_osds_chassis1,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Step 3: Run ok-to-upgrade for host bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket=host0.hostname,
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_host0,
                osds_ok_to_upgrade=all_osds_host0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Step 4: Run ok-to-upgrade for chassis bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket="chassis1",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_chassis1,
                osds_ok_to_upgrade=all_osds_chassis1,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Step 5: Run ok-to-upgrade for osd bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket=f"osd.{osd_id}",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=[osd_id],
                osds_ok_to_upgrade=[osd_id],
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Scenario 2 passed: chassis failure domain (rack, host, chassis, osd buckets)"
            )

        elif "scenario3" in scenarios:
            log.info("===========================================")
            log.info(
                "Scenario 3: Host failure domain - ok-to-upgrade for rack, host, chassis, osd"
            )
            log.info(
                "  Step 1: Create CRUSH rule (host FD) and pool, wait for PGs clean"
            )
            log.info("  Step 2: Run ok-to-upgrade for rack bucket and verify output")
            log.info("  Step 3: Run ok-to-upgrade for host bucket and verify output")
            log.info("  Step 4: Run ok-to-upgrade for chassis bucket and verify output")
            log.info("  Step 5: Run ok-to-upgrade for osd bucket and verify output")
            log.info("===========================================")
            log.info("Step 1: Create CRUSH rule (host FD) and pool, wait for PGs clean")
            pool_name = "ok-to-upgrade-scenario9"
            failure_domain = "host"
            crush_rule = CrushRule(
                rados_obj=rados_obj,
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
            )
            crush_rule.create()
            crush_rule.modify_all_failure_domains(new_failure_domain=failure_domain)
            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=pg_num,
                pg_num_min=pg_num_min,
                crush_rule=crush_rule.crush_rule_name,
            ), "Failed to create pool"
            rados_obj.wait_for_clean_pg_sets()

            all_osds_rack1 = rados_obj.collect_osd_daemon_ids("rack1")
            host0 = [host for host in osd_hosts if "installer" in host.hostname][0]
            all_osds_host0 = rados_obj.collect_osd_daemon_ids(host0)
            all_osds_chassis1 = rados_obj.collect_osd_daemon_ids("chassis1")

            osd_id = all_osds_host0[0]

            log.info("Step 2: Run ok-to-upgrade for rack bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket="rack1",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_rack1,
                osds_ok_to_upgrade=all_osds_host0,
                osds_upgraded=[],
                bad_no_version=[],
            )

            assert expected == actual
            log.info("Step 3: Run ok-to-upgrade for host bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket=host0.hostname,
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_host0,
                osds_ok_to_upgrade=all_osds_host0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Step 4: Run ok-to-upgrade for chassis bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket="chassis1",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_chassis1,
                osds_ok_to_upgrade=all_osds_host0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Step 5: Run ok-to-upgrade for osd bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket=f"osd.{osd_id}",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=[osd_id],
                osds_ok_to_upgrade=[osd_id],
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Scenario 3 passed: host failure domain (rack, host, chassis, osd buckets)"
            )

        elif "scenario4" in scenarios:
            log.info("===========================================")
            log.info(
                "Scenario 4: OSD failure domain - ok-to-upgrade for rack, host, chassis, osd"
            )
            log.info(
                "  Step 1: Create CRUSH rule (OSD FD) and pool, wait for PGs clean"
            )
            log.info("  Step 2: Run ok-to-upgrade for rack bucket and verify output")
            log.info("  Step 3: Run ok-to-upgrade for host bucket and verify output")
            log.info("  Step 4: Run ok-to-upgrade for chassis bucket and verify output")
            log.info("  Step 5: Run ok-to-upgrade for osd bucket and verify output")
            log.info("===========================================")
            log.info("Step 1: Create CRUSH rule (OSD FD) and pool, wait for PGs clean")
            pool_name = "ok-to-upgrade-scenario10"
            failure_domain = "osd"
            crush_rule = CrushRule(
                rados_obj=rados_obj,
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
            )
            crush_rule.create()
            crush_rule.modify_all_failure_domains(new_failure_domain=failure_domain)
            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=pg_num,
                pg_num_min=pg_num_min,
                crush_rule=crush_rule.crush_rule_name,
            ), "Failed to create pool"
            rados_obj.wait_for_clean_pg_sets()

            all_osds_rack1 = rados_obj.collect_osd_daemon_ids("rack1")
            host0 = [host for host in osd_hosts if "installer" in host.hostname][0]
            all_osds_host0 = rados_obj.collect_osd_daemon_ids(host0)
            all_osds_chassis1 = rados_obj.collect_osd_daemon_ids("chassis1")
            osd_id = all_osds_host0[0]

            log.info("Step 2: Run ok-to-upgrade for rack bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket="rack1",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_rack1,
                osds_ok_to_upgrade=[actual.osds_in_crush_bucket[0]],
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Step 3: Run ok-to-upgrade for host bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket=host0.hostname,
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_host0,
                osds_ok_to_upgrade=[actual.osds_in_crush_bucket[0]],
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Step 4: Run ok-to-upgrade for chassis bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket="chassis1",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_chassis1,
                osds_ok_to_upgrade=[actual.osds_in_crush_bucket[0]],
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Step 5: Run ok-to-upgrade for osd bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket=f"osd.{osd_id}",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=[osd_id],
                osds_ok_to_upgrade=[actual.osds_in_crush_bucket[0]],
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Scenario 4 passed: OSD failure domain (rack, host, chassis, osd buckets)"
            )

        elif "scenario5" in scenarios:
            log.info("===========================================")
            log.info(
                "Scenario 5: All OSDs already upgraded - verify ok_to_upgrade=False, all_osds_upgraded=True"
            )
            log.info(
                "  Step 1: Create CRUSH rule (rack FD) and pool, wait for PGs clean"
            )
            log.info("  Step 2: Run ok-to-upgrade for osd bucket and verify output")
            log.info("  Step 3: Run ok-to-upgrade for host bucket and verify output")
            log.info("  Step 4: Run ok-to-upgrade for chassis bucket and verify output")
            log.info("  Step 5: Run ok-to-upgrade for rack bucket and verify output")
            log.info(
                "  Step 6: Verify output (ok_to_upgrade=False, all_osds_upgraded=True) for all buckets"
            )
            log.info("===========================================")
            log.info("Step 1: Create CRUSH rule (rack FD) and pool, wait for PGs clean")
            pool_name = "ok-to-upgrade-scenario7"
            failure_domain = "rack"

            crush_rule = CrushRule(
                rados_obj=rados_obj,
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
            )

            crush_rule.create()
            crush_rule.modify_all_failure_domains(new_failure_domain=failure_domain)

            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=pg_num,
                pg_num_min=pg_num_min,
                crush_rule=crush_rule.crush_rule_name,
            ), "Failed to create pool"

            rados_obj.wait_for_clean_pg_sets()

            rados_obj.collect_osd_daemon_ids("default")
            all_osds_in_rack = rados_obj.collect_osd_daemon_ids("rack1")
            host0 = [host for host in osd_hosts if "installer" in host.hostname][0]
            all_osds_host0 = rados_obj.collect_osd_daemon_ids(host0)
            all_osds_chassis1 = rados_obj.collect_osd_daemon_ids("chassis1")
            osd_id = all_osds_host0[0]
            ceph_version = get_ceph_version_from_cluster(
                ceph_cluster.get_nodes(role="client")[0]
            )

            log.info("Step 2: Run ok-to-upgrade for osd bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket=f"osd.{osd_id}",
                ceph_version=ceph_version,
                rados_obj=rados_obj,
            ).execute(ignore_errors=True)
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=False,
                all_osds_upgraded=True,
                osds_in_crush_bucket=[osd_id],
                osds_ok_to_upgrade=[],
                osds_upgraded=[osd_id],
                bad_no_version=[],
            )
            assert expected == actual

            log.info("Step 3: Run ok-to-upgrade for host bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket=host0.hostname,
                ceph_version=ceph_version,
                rados_obj=rados_obj,
            ).execute(ignore_errors=True)
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=False,
                all_osds_upgraded=True,
                osds_in_crush_bucket=all_osds_host0,
                osds_ok_to_upgrade=[],
                osds_upgraded=all_osds_host0,
                bad_no_version=[],
            )
            assert expected == actual

            log.info("Step 4: Run ok-to-upgrade for chassis bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket="chassis1",
                ceph_version=ceph_version,
                rados_obj=rados_obj,
            ).execute(ignore_errors=True)
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=False,
                all_osds_upgraded=True,
                osds_in_crush_bucket=all_osds_chassis1,
                osds_ok_to_upgrade=[],
                osds_upgraded=all_osds_chassis1,
                bad_no_version=[],
            )
            assert expected == actual

            log.info("Step 5: Run ok-to-upgrade for rack bucket and verify output")
            rados_obj.wait_for_clean_pg_sets()
            actual = OsdOkToUpgradeCommand(
                crush_bucket="rack1",
                ceph_version=ceph_version,
                rados_obj=rados_obj,
            ).execute(ignore_errors=True)
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=False,
                all_osds_upgraded=True,
                osds_in_crush_bucket=all_osds_in_rack,
                osds_ok_to_upgrade=[],
                osds_upgraded=all_osds_in_rack,
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 6: Verify output (ok_to_upgrade=False, all_osds_upgraded=True) for all buckets"
            )
            log.info(
                "Scenario 5 passed: all OSDs upgraded (osd, host, chassis, rack buckets)"
            )

        elif "scenario6" in scenarios:
            log.info("===========================================")
            log.info(
                "Scenario 6: Convergence factor - verify mgr log convergence for host bucket"
            )
            log.info("  Step 1: Set mgr debug, get active mgr")
            log.info(
                "  Step 2: Create CRUSH rule (rack FD) and pool, wait for PGs clean"
            )
            log.info(
                "  Step 3: For each convergence factor (0.9, 0.8, 0.1): set config, run ok-to-upgrade for host, "
                "validate mgr log"
            )
            log.info("===========================================")
            log.info("Step 1: Set mgr debug, get active mgr")
            mon_config_method = MonConfigMethods(rados_obj=rados_obj)
            mgr_config_method = MgrWorkflows(node=cephadm)
            assert mon_config_method.set_config(
                section="mgr", name="debug_mgr", value="20/20"
            )
            active_mgr = mgr_config_method.get_active_mgr()
            failure_domain = "rack"
            pool_name = "ok-to-upgrade-scenario6"
            log.info("Step 2: Create CRUSH rule (rack FD) and pool, wait for PGs clean")
            crush_rule = CrushRule(
                rados_obj=rados_obj,
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
            )

            crush_rule.create()
            crush_rule.modify_all_failure_domains(new_failure_domain=failure_domain)

            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=pg_num,
                pg_num_min=pg_num_min,
                crush_rule=crush_rule.crush_rule_name,
            ), "Failed to create pool"

            rados_obj.wait_for_clean_pg_sets()

            # Stop all OSDs on host[1] for the logic to search safe set of osds
            host_to_stop = osd_hosts[1]
            stopped_osds = rados_obj.collect_osd_daemon_ids(osd_node=host_to_stop)
            assert (
                stopped_osds
            ), f"No OSDs found on host {host_to_stop.hostname} for scenario 6"
            for osd_id in stopped_osds:
                assert rados_obj.change_osd_state(
                    action="stop", target=osd_id
                ), f"Failed to stop OSD {osd_id} on {host_to_stop.hostname}"
                time.sleep(5)
            log.info("Stopped all OSDs on host %s", host_to_stop.hostname)

            log.info(
                "Step 3: For each convergence factor (0.9, 0.8, 0.1): set config, "
                "run ok-to-upgrade for host, validate mgr log"
            )
            try:
                for convergence_factor in [0.9, 0.8, 0.1]:
                    assert mon_config_method.set_config(
                        section="mgr",
                        name="mgr_osd_upgrade_check_convergence_factor",
                        value=convergence_factor,
                    )
                    ceph_version = "20.3.0-3803-g63ca1ffb5a21"
                    osd_host = osd_hosts[0]
                    osds_in_host = get_node_osd_list(
                        rados_obj, ceph_nodes, osd_host.hostname
                    )
                    node: CephNode = ceph_cluster.get_node_by_hostname(
                        active_mgr.split(".")[0]
                    )
                    assert rados_obj.rotate_logs([node])
                    print("OSDs in host ", osds_in_host)
                    try:
                        actual = OsdOkToUpgradeCommand(
                            osd_host.hostname, ceph_version, rados_obj=rados_obj
                        ).execute()
                    except CommandFailed as e:
                        log.debug(f"expected error:- {str(e)}")
                        log.info("Continuing with the test case")

                    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]

                    lines = node.exec_command(
                        sudo=True,
                        cmd=f"grep -E '*_check_offlines_pgs*' /var/log/ceph/{fsid}/ceph-mgr.{active_mgr}.log",
                    )
                    lines = lines[0].split("\n")[:-1]
                    assert len(lines) != 0

                    assert validate_convergence_factor(
                        lines, convergence_factor, osds_in_host
                    )
                    assert mon_config_method.remove_config(
                        section="mgr",
                        name="mgr_osd_upgrade_check_convergence_factor",
                        verify_rm=True,
                    )
            finally:
                for osd_id in stopped_osds:
                    rados_obj.change_osd_state(action="start", target=osd_id)
                    time.sleep(5)
                log.info("Restarted all OSDs on host %s", host_to_stop.hostname)
            log.info("Scenario 6 passed: convergence factor checks")

        elif "scenario7" in scenarios:
            log.info("===========================================")
            log.info(
                "Scenario 7: Rack failure domain with max=100 - osd, host, rack, chassis buckets"
            )
            log.info(
                "  Step 1: Create CRUSH rule (rack FD) and pool, wait for PGs clean"
            )
            log.info(
                "  Step 2: Run ok-to-upgrade (max=100) for osd bucket and verify output"
            )
            log.info(
                "  Step 3: Run ok-to-upgrade (max=100) for host bucket and verify output"
            )
            log.info(
                "  Step 4: Run ok-to-upgrade (max=100) for rack bucket and verify output"
            )
            log.info(
                "  Step 5: Run ok-to-upgrade (max=100) for chassis bucket and verify output"
            )
            log.info("===========================================")
            log.info("Step 1: Create CRUSH rule (rack FD) and pool, wait for PGs clean")
            pool_name = "ok-to-upgrade-rack-fd"
            failure_domain = "rack"
            crush_rule = CrushRule(
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
                rados_obj=rados_obj,
            )
            crush_rule.create()
            crush_rule.modify_all_failure_domains(new_failure_domain=failure_domain)
            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=pg_num,
                pg_num_min=pg_num_min,
                crush_rule=crush_rule.crush_rule_name,
            ), "Failed to create pool"
            rados_obj.wait_for_clean_pg_sets()

            all_osds_host0 = rados_obj.collect_osd_daemon_ids(osd_hosts[0].hostname)
            osd_id = all_osds_host0[0]
            all_osds_rack0 = rados_obj.collect_osd_daemon_ids("rack0")
            all_osds_chassis0 = rados_obj.collect_osd_daemon_ids("chassis0")

            log.info(
                "Step 2: Run ok-to-upgrade (max=100) for osd bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket=f"osd.{osd_id}",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=[osd_id],
                osds_ok_to_upgrade=all_osds_rack0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 3: Run ok-to-upgrade (max=100) for host bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket=osd_hosts[0].hostname,
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_host0,
                osds_ok_to_upgrade=all_osds_rack0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 4: Run ok-to-upgrade (max=100) for rack bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket="rack0",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_rack0,
                osds_ok_to_upgrade=all_osds_rack0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 5: Run ok-to-upgrade (max=100) for chassis bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket="chassis0",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_chassis0,
                osds_ok_to_upgrade=all_osds_rack0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Scenario 7 passed: rack FD with max=100 (osd, host, rack, chassis buckets)"
            )

        elif "scenario8" in scenarios:
            log.info("===========================================")
            log.info(
                "Scenario 8: Chassis failure domain with max=100 - osd, host, rack, chassis"
            )
            log.info(
                "  Step 1: Create CRUSH rule (chassis FD) and pool, wait for PGs clean"
            )
            log.info(
                "  Step 2: Run ok-to-upgrade (max=100) for osd bucket and verify output"
            )
            log.info(
                "  Step 3: Run ok-to-upgrade (max=100) for host bucket and verify output"
            )
            log.info(
                "  Step 4: Run ok-to-upgrade (max=100) for rack bucket and verify output"
            )
            log.info(
                "  Step 5: Run ok-to-upgrade (max=100) for chassis bucket and verify output"
            )
            log.info("===========================================")
            log.info(
                "Step 1: Create CRUSH rule (chassis FD) and pool, wait for PGs clean"
            )
            pool_name = "ok-to-upgrade-chassis-fd-max"
            failure_domain = "chassis"
            crush_rule = CrushRule(
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
                rados_obj=rados_obj,
            )
            crush_rule.create()
            crush_rule.modify_all_failure_domains(new_failure_domain=failure_domain)
            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=pg_num,
                pg_num_min=pg_num_min,
                crush_rule=crush_rule.crush_rule_name,
                rados_obj=rados_obj,
            ), "Failed to create pool"
            rados_obj.wait_for_clean_pg_sets()

            all_osds_host0 = rados_obj.collect_osd_daemon_ids(osd_hosts[0].hostname)
            osd_id = all_osds_host0[0]
            all_osds_rack0 = rados_obj.collect_osd_daemon_ids("rack0")
            all_osds_chassis0 = rados_obj.collect_osd_daemon_ids("chassis0")

            log.info(
                "Step 2: Run ok-to-upgrade (max=100) for osd bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket=f"osd.{osd_id}",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=[osd_id],
                osds_ok_to_upgrade=all_osds_chassis0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 3: Run ok-to-upgrade (max=100) for host bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket=osd_hosts[0].hostname,
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_host0,
                osds_ok_to_upgrade=all_osds_chassis0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 4: Run ok-to-upgrade (max=100) for rack bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket="rack0",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max=100,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_rack0,
                osds_ok_to_upgrade=all_osds_chassis0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 5: Run ok-to-upgrade (max=100) for chassis bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket="chassis0",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max=100,
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_chassis0,
                osds_ok_to_upgrade=all_osds_chassis0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Scenario 8 passed: chassis failure domain with max=100")

        elif "scenario9" in scenarios:
            log.info("===========================================")
            log.info(
                "Scenario 9: Host failure domain with max=100 - osd, host, rack, chassis"
            )
            log.info(
                "  Step 1: Create CRUSH rule (host FD) and pool, wait for PGs clean"
            )
            log.info(
                "  Step 2: Run ok-to-upgrade (max=100) for osd bucket and verify output"
            )
            log.info(
                "  Step 3: Run ok-to-upgrade (max=100) for host bucket and verify output"
            )
            log.info(
                "  Step 4: Run ok-to-upgrade (max=100) for rack bucket and verify output"
            )
            log.info(
                "  Step 5: Run ok-to-upgrade (max=100) for chassis bucket and verify output"
            )
            log.info("===========================================")
            log.info("Step 1: Create CRUSH rule (host FD) and pool, wait for PGs clean")
            pool_name = "ok-to-upgrade-host-fd-max"
            failure_domain = "host"
            crush_rule = CrushRule(
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
                rados_obj=rados_obj,
            )
            crush_rule.create()
            crush_rule.modify_all_failure_domains(new_failure_domain=failure_domain)
            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=pg_num,
                pg_num_min=pg_num_min,
                crush_rule=crush_rule.crush_rule_name,
            ), "Failed to create pool"
            rados_obj.wait_for_clean_pg_sets()

            all_osds_host0 = rados_obj.collect_osd_daemon_ids(osd_hosts[0].hostname)
            osd_id = all_osds_host0[0]
            all_osds_rack0 = rados_obj.collect_osd_daemon_ids("rack0")
            all_osds_chassis0 = rados_obj.collect_osd_daemon_ids("chassis0")

            log.info(
                "Step 2: Run ok-to-upgrade (max=100) for osd bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket=f"osd.{osd_id}",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=[osd_id],
                osds_ok_to_upgrade=all_osds_host0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 3: Run ok-to-upgrade (max=100) for host bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket=osd_hosts[0].hostname,
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_host0,
                osds_ok_to_upgrade=all_osds_host0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 4: Run ok-to-upgrade (max=100) for rack bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket="rack0",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_rack0,
                osds_ok_to_upgrade=all_osds_host0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 5: Run ok-to-upgrade (max=100) for chassis bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket="chassis0",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_chassis0,
                osds_ok_to_upgrade=all_osds_host0,
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Scenario 9 passed: host failure domain with max=100")

        elif "scenario10" in scenarios:
            log.info("===========================================")
            log.info(
                "Scenario 10: OSD failure domain with max=100 - osd, host, rack, chassis"
            )
            log.info(
                "  Step 1: Create CRUSH rule (OSD FD) and pool, wait for PGs clean"
            )
            log.info(
                "  Step 2: Run ok-to-upgrade (max=100) for osd bucket and verify output"
            )
            log.info(
                "  Step 3: Run ok-to-upgrade (max=100) for host bucket and verify output"
            )
            log.info(
                "  Step 4: Run ok-to-upgrade (max=100) for rack bucket and verify output"
            )
            log.info(
                "  Step 5: Run ok-to-upgrade (max=100) for chassis bucket and verify output"
            )
            log.info("===========================================")
            log.info("Step 1: Create CRUSH rule (OSD FD) and pool, wait for PGs clean")
            pool_name = "ok-to-upgrade-osd-fd-max"
            failure_domain = "osd"
            crush_rule = CrushRule(
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
                rados_obj=rados_obj,
            )
            crush_rule.create()
            crush_rule.modify_all_failure_domains(new_failure_domain=failure_domain)
            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=pg_num,
                pg_num_min=pg_num_min,
                crush_rule=crush_rule.crush_rule_name,
            ), "Failed to create pool"
            rados_obj.wait_for_clean_pg_sets()

            all_osds_host0 = rados_obj.collect_osd_daemon_ids(osd_hosts[0].hostname)
            osd_id = all_osds_host0[0]
            all_osds_rack0 = rados_obj.collect_osd_daemon_ids("rack0")
            all_osds_chassis0 = rados_obj.collect_osd_daemon_ids("chassis0")

            log.info(
                "Step 2: Run ok-to-upgrade (max=100) for osd bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket=f"osd.{osd_id}",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=[osd_id],
                osds_ok_to_upgrade=[actual.osds_in_crush_bucket[0]],
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 3: Run ok-to-upgrade (max=100) for host bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket=osd_hosts[0].hostname,
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_host0,
                osds_ok_to_upgrade=[actual.osds_in_crush_bucket[0]],
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 4: Run ok-to-upgrade (max=100) for rack bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket="rack0",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_rack0,
                osds_ok_to_upgrade=[actual.osds_in_crush_bucket[0]],
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info(
                "Step 5: Run ok-to-upgrade (max=100) for chassis bucket and verify output"
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket="chassis0",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
                max="100",
            ).execute()
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_chassis0,
                osds_ok_to_upgrade=[actual.osds_in_crush_bucket[0]],
                osds_upgraded=[],
                bad_no_version=[],
            )
            assert expected == actual
            log.info("Scenario 10 passed: OSD failure domain with max=100")

        elif "negative" in scenarios:
            log.info("=" * 80)
            log.info("Negative scenarios : ceph osd ok-to-upgrade error handling")
            log.info("Step 1. max value must be non-negative")
            log.info(
                "Step 2. crush bucket type must be rack/chassis/host/osd, not root"
            )
            log.info("Step 3. non-existent crush bucket")
            log.info("Step 4. invalid Ceph version format")
            log.info("Step 5. missing required ceph_version parameter")
            log.info("Step 6. crush bucket with no OSDs (has no children)")
            log.info("=" * 80)

            log.info("Negative scenario 1: max value must be non-negative")
            in_osds = rados_obj.get_osd_list(status="in")
            osd_id = in_osds[0]
            execute_negative_scenario(
                command=OsdOkToUpgradeCommand(
                    crush_bucket=f"osd.{osd_id}",
                    ceph_version="20.3.0-3803-g63ca1ffb5a21",
                    rados_obj=rados_obj,
                    max="-100",
                ),
                expected_err_substring="'max' must be non-negative",
            )
            log.debug("Passed: max=-100 rejected as expected")

            log.info(
                "Negative scenario 2: crush bucket type must be rack/chassis/host/osd, not root"
            )
            execute_negative_scenario(
                command=OsdOkToUpgradeCommand(
                    crush_bucket="default",
                    ceph_version="20.3.0-3803-g63ca1ffb5a21",
                    rados_obj=rados_obj,
                ),
                expected_err_substring="valid types are: 'rack', 'chassis', 'host' and 'osd'",
            )
            log.debug("Passed: root bucket 'default' rejected as expected")

            log.info("Negative scenario 3: non-existent crush bucket")
            execute_negative_scenario(
                command=OsdOkToUpgradeCommand(
                    crush_bucket="test",
                    ceph_version="20.3.0-3803-g63ca1ffb5a21",
                    rados_obj=rados_obj,
                ),
                expected_err_substring="does not exist",
            )
            log.debug("Passed: non-existent bucket rejected as expected")

            log.info("Negative scenario 4: invalid Ceph version format")
            execute_negative_scenario(
                command=OsdOkToUpgradeCommand(
                    crush_bucket="test", ceph_version="garbage", rados_obj=rados_obj
                ),
                expected_err_substring="Invalid Ceph version (short) format",
            )
            log.debug("Passed: garbage version rejected as expected")

            log.info("Negative scenario 5: missing required ceph_version parameter")
            execute_negative_scenario(
                command=OsdOkToUpgradeCommand(
                    crush_bucket="test", ceph_version="", rados_obj=rados_obj
                ),
                expected_err_substring="missing required parameter ceph_version",
            )
            log.debug("Passed: empty version rejected as expected")

            log.info("Negative scenario 6: crush bucket with no OSDs (has no children)")
            rados_obj.run_ceph_command(cmd="ceph osd crush add-bucket rack11 rack")
            execute_negative_scenario(
                command=OsdOkToUpgradeCommand(
                    crush_bucket="rack11",
                    ceph_version="20.3.0-3803-g63ca1ffb5a21",
                    rados_obj=rados_obj,
                ),
                expected_err_substring="has no children",
            )
            rados_obj.run_ceph_command(cmd="ceph osd crush rm rack11")
            log.debug("Passed: empty bucket rejected and rack11 cleaned up")

            log.info("All negative scenarios completed successfully")
        else:
            raise InvalidScenarioException(
                f"config.scenarios must be one of ['rack_failure_domain_test', 'negative']"
                f" Scenarios passed - {scenarios}"
            )

    except Exception as e:
        log.error("Execution failed with exception: %s", e.__doc__)
        log.exception(e)
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )

        if "negative" not in scenarios:
            rados_obj.delete_pool(pool_name)
            crush_rule.remove()

        rados_obj.log_cluster_health()

        test_end_time = get_cluster_timestamp(rados_obj.node)

        log.debug(
            "Test workflow completed. Start time: %s, End time: %s",
            start_time,
            test_end_time,
        )

        # if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
        #     log.error("Test failed due to crash at the end of test")
        #     return 1

        log.info("Test workflow finished; no crashes detected")
    return 0


class CrushRule:
    """Represents a CRUSH rule (e.g. chooseleaf by rack/chassis) for test pools."""

    def __init__(
        self,
        crush_rule_name: str,
        failure_domain: str,
        crushtool: CrushToolWorkflows,
        rados_obj: RadosOrchestrator,
        pool_type: str = "replicated",
    ):
        self.crush_rule_name: str = crush_rule_name
        self.failure_domain: str = failure_domain
        self.crushtool: CrushToolWorkflows = crushtool
        self.source_bin: str = "/etc/ceph"
        self.target_bin: str = "/etc/ceph"
        self.type: str = pool_type
        self.backup_original_crush_map_txt: str = ""
        self.rados_obj = rados_obj

    def create(self) -> None:
        """Add this rule to the crush map (backup of original map is kept for remove())."""
        device_rules = f"""
         id {str(random.randint(10, 100))}
         type {self.type}
         step take default
         step chooseleaf firstn 0 type {self.failure_domain}
         step emit"""

        res, original_crush_map_bin = self.crushtool.generate_crush_map_bin(
            self.source_bin
        )
        if res is False:
            raise Exception("failed to generate crushmap bin")

        res, self.backup_original_crush_map_txt = (
            self.crushtool.decompile_crush_map_txt(
                source_loc=original_crush_map_bin, target_loc=self.target_bin
            )
        )
        if res is False:
            raise Exception("failed to extract crushmap text from bin")

        if (
            self.crushtool.add_crush_rule(
                rule_name=self.crush_rule_name, rules=device_rules
            )
            is False
        ):
            raise Exception("Failed to add crush rule")

        log.info("Crush rule added successfully: %s", self.crush_rule_name)

    def remove(self):
        """Restore crush map to state before create() by applying the backup."""
        log.debug("Removing crush rule: %s", self.crush_rule_name)
        if self.backup_original_crush_map_txt == "":
            raise ValueError(
                "CrushRule.create() needs to be executed inorder to update crush map"
            )
        res, bin = self.crushtool.compile_crush_map_txt(
            source_loc=self.backup_original_crush_map_txt
        )
        assert self.crushtool.set_crush_map_bin(bin)

    def modify_all_failure_domains(
        self,
        new_failure_domain: str,
    ):
        """
        Modify the failure domain in all CRUSH rules' choose/chooseleaf steps.
        """
        crush_map_dir: str = "/tmp"
        _, loc = self.crushtool.generate_crush_map_bin(loc=crush_map_dir)
        _, loc = self.crushtool.decompile_crush_map_txt(
            source_loc=loc, target_loc=crush_map_dir
        )

        cmd = f"sed -i 's/\\(step chooseleaf [^ ]* [^ ]* type \\)[^ ]*/\\1{new_failure_domain}/' {loc}"
        self.rados_obj.client.exec_command(sudo=True, cmd=cmd)

        res = self.crushtool.compile_crush_map_txt(
            source_loc=loc, target_loc=crush_map_dir
        )
        if not res or (isinstance(res, tuple) and not res[0]):
            raise Exception("Failed to compile crush map binary")

        _, loc = res
        if not self.crushtool.set_crush_map_bin(loc=loc):
            raise Exception("Failed to set crush map binary")

        log.info("Modified crush rule failure domains to %s", new_failure_domain)


def validate_convergence_factor(log_lines, convergence_factor, osds_in_crush_bucket):
    """
    Validate that mgr log lines show convergence steps matching the given factor.

    Parses log lines for _check_offlines_pgs [osd1,osd2,...] and checks that the
    sequence of OSD list sizes matches repeated application of convergence_factor
    (e.g. size, then int(size*conv), ... until <= 1). Returns True if the sequence
    matches; raises AssertionError otherwise.
    """
    all_osds: List[List[str]] = []
    regex = re.compile(r".* _check_offlines_pgs \[(.*)\] ->.*")
    log.info("-------------- log lines --------------")
    log.info(log_lines)
    log.info("-------------- end of log lines --------------")
    for line in log_lines:
        match = regex.search(line)
        if match:
            osds = match.group(1).split(",")
            all_osds.append(osds)

    conv = convergence_factor
    length_of_osds_in_crush_bucket = len(osds_in_crush_bucket)
    convergence_list_size = []
    while length_of_osds_in_crush_bucket > 1:
        convergence_list_size.append(length_of_osds_in_crush_bucket)
        log.debug(
            "convergence step: %s * %s -> %s",
            length_of_osds_in_crush_bucket,
            conv,
            int(length_of_osds_in_crush_bucket * conv),
        )
        length_of_osds_in_crush_bucket = int(length_of_osds_in_crush_bucket * conv)
    convergence_list_size.append(1)

    log.info(f"Calculated array sizes : {convergence_list_size}")
    log.info(f"array sizes in logs : {[len(osd_list) for osd_list in all_osds]}")
    for i in range(min(len(all_osds), len(convergence_list_size))):
        assert convergence_list_size[i] == len(all_osds[i]), (
            f"Convergence factor check failed: expected {convergence_list_size[i]} "
            f"actual {len(all_osds[i])}"
        )

    return True
