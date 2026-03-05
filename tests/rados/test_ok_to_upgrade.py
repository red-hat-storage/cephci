import random
import string
from typing import List

from ceph.ceph import CephNode
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.crushtool_workflows import CrushToolWorkflows
from ceph.rados.osd_ok_to_upgrade_utils import (
    OsdOkToUpgradeCommand,
    OsdOkToUpgradeCommandOutput,
    execute_negative_scenario,
)
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.test_deploy_stretch_cluster_baremetal import move_crush_item
from utility.log import Log

log = Log(__name__)


class InvalidScenarioException(Exception):
    """Raised when config.scenarios contains an invalid or unsupported scenario name."""

    pass


def run(ceph_cluster, **kw):
    """
    Test ceph osd ok-to-upgrade command.

    Scenarios:
    - scenario1: ok-to-upgrade for single OSD (osd.<id>)
    - scenario2: ok-to-upgrade for host bucket
    - scenario3: ok-to-upgrade for rack bucket
    - negative: error handling; sub-cases:
      - max value < 0
      - crush bucket type must be rack/chassis/host/osd, not root
      - non-existent crush bucket
      - invalid Ceph version format
      - missing required ceph_version parameter
      - crush bucket with no OSDs (has no children)

    Polarion: CEPH-83632279
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    start_time = get_cluster_timestamp(rados_obj.node)
    scenarios = config.get("scenarios", [])
    log.debug("Test workflow started. Start time: %s", start_time)
    crushtool = CrushToolWorkflows(node=cephadm)
    number_of_racks = 4
    number_of_chassis_per_rack = 2
    crush_hierarchy_setup = config.get("crush_hierarchy_setup", True)
    crush_hierarchy_cleanup = config.get("crush_hierarchy_cleanup", False)

    try:
        if len(scenarios) == 0:
            log.error("config.scenarios cannot be empty")
            return 1

        osd_hosts: List[CephNode] = ceph_cluster.get_nodes(role="osd")

        if crush_hierarchy_setup:
            log.info("=" * 80)
            log.info("Crush hierarchy setup")
            log.info("=" * 80)
            for index in range(0, number_of_racks):
                rack = f"rack{index}"
                move_crush_item(
                    node=cephadm, crush_obj=rack, name="root", value="default"
                )

            for index in range(0, number_of_chassis_per_rack * number_of_racks):
                chassis = f"chassis{index}"
                rack = f"rack{index % number_of_racks}"
                move_crush_item(
                    node=cephadm, crush_obj=chassis, name="rack", value=rack
                )

            osd_hostnames = list()
            for index, host in enumerate(osd_hosts):
                osd_hostnames.append(host.hostname)
                chassis = (
                    f"chassis{index % (number_of_chassis_per_rack * number_of_racks)}"
                )
                move_crush_item(
                    node=cephadm, crush_obj=host.hostname, name="chassis", value=chassis
                )

        if "scenario1" in scenarios:
            log.info("=" * 80)
            log.info("Scenario 1 : ok-to-upgrade for single OSD")
            log.info("Step 1. Create CRUSH rule and pool, wait for PGs clean")
            log.info("Step 2. Run ceph osd ok-to-upgrade for single OSD (osd.<id>)")
            log.info(
                "Step 3. Verify output matches expected (ok_to_upgrade=True, etc.)"
            )
            log.info("=" * 80)
            pool_name = "ok-to-upgrade-scenario1"
            failure_domain = "rack"
            crush_rule = CrushRule(
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
            )

            crush_rule.create()

            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=128,
                pg_num_min=128,
                crush_rule=crush_rule.crush_rule_name,
            ), "Failed to create pool"

            # self.config.rados_obj.bench_write(self.pool_name), "Failed to fill the"
            rados_obj.wait_for_clean_pg_sets()

            all_osds_in_host = rados_obj.collect_osd_daemon_ids(osd_hosts[0])
            osd_id = all_osds_in_host[0]

            # Execute the command :-
            # ceph osd ok-to-upgrade osd<N> 20.3.0-3803-g63ca1ffb5a21
            log.info("Step 2: Run ceph osd ok-to-upgrade for osd.%s", osd_id)
            actual = OsdOkToUpgradeCommand(
                crush_bucket=f"osd.{osd_id}",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()

            log.info(
                "Step 3: Verify output matches expected (ok_to_upgrade=True, etc.)"
            )
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=[osd_id],
                osds_ok_to_upgrade=[osd_id],
                osds_upgraded=[],
                bad_no_version=[],
            )

            assert expected == actual
            log.debug("Scenario1 passed: output matches expected for osd.%s", osd_id)

        elif "scenario2" in scenarios:
            log.info("=" * 80)
            log.info("Scenario 2 : ok-to-upgrade for host bucket")
            log.info("Step 1. Create CRUSH rule and pool, wait for PGs clean")
            log.info("Step 2. Run ceph osd ok-to-upgrade for host bucket")
            log.info(
                "Step 3. Verify output matches expected (ok_to_upgrade=True, etc.)"
            )
            log.info("=" * 80)
            pool_name = "ok-to-upgrade-scenario2"
            failure_domain = "rack"
            crush_rule = CrushRule(
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
            )
            crush_rule.create()
            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=128,
                pg_num_min=128,
                crush_rule=crush_rule.crush_rule_name,
            ), "Failed to create pool"

            rados_obj.wait_for_clean_pg_sets()

            all_osds_in_host = rados_obj.collect_osd_daemon_ids(osd_hosts[0])

            # Execute the command :-
            # ceph osd ok-to-upgrade <hostname> 20.3.0-3803-g63ca1ffb5a21
            log.info(
                "Step 2: Run ceph osd ok-to-upgrade for host %s", osd_hosts[0].hostname
            )
            actual = OsdOkToUpgradeCommand(
                crush_bucket=osd_hosts[0].hostname,
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()

            log.info(
                "Step 3: Verify output matches expected (ok_to_upgrade=True, etc.)"
            )
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_in_host,
                osds_ok_to_upgrade=all_osds_in_host,
                osds_upgraded=[],
                bad_no_version=[],
            )

            assert expected == actual
            log.debug(
                "Scenario2 passed: output matches expected for host %s",
                osd_hosts[0].hostname,
            )

        # https://tracker.ceph.com/issues/74924
        elif "scenario3" in scenarios:
            log.info("=" * 80)
            log.info("Scenario 3 : ok-to-upgrade for rack bucket")
            log.info("Step 1. Create CRUSH rule and pool, wait for PGs clean")
            log.info("Step 2. Run ceph osd ok-to-upgrade for rack bucket")
            log.info(
                "Step 3. Verify output matches expected (ok_to_upgrade=True, etc.)"
            )
            log.info("=" * 80)
            pool_name = "ok-to-upgrade-scenario3"
            failure_domain = "rack"

            crush_rule = CrushRule(
                crush_rule_name=f"{failure_domain}_failure_domain"
                + "".join(random.choices(string.ascii_letters, k=6)),
                failure_domain=failure_domain,
                crushtool=crushtool,
            )

            crush_rule.create()

            rados_obj.create_pool(
                pool_name=pool_name,
                pg_num=128,
                pg_num_min=128,
                crush_rule=crush_rule.crush_rule_name,
            ), "Failed to create pool"

            rados_obj.wait_for_clean_pg_sets()

            all_osds_of_rack: List[int] = rados_obj.collect_osd_daemon_ids("rack1")

            # Execute the command :-
            # ceph osd ok-to-upgrade rack1 20.3.0-3803-g63ca1ffb5a21
            log.info("Step 2: Run ceph osd ok-to-upgrade for rack1")
            actual = OsdOkToUpgradeCommand(
                crush_bucket="rack1",
                ceph_version="20.3.0-3803-g63ca1ffb5a21",
                rados_obj=rados_obj,
            ).execute()

            log.info(
                "Step 3: Verify output matches expected (ok_to_upgrade=True, etc.)"
            )
            expected = OsdOkToUpgradeCommandOutput(
                ok_to_upgrade=True,
                all_osds_upgraded=False,
                osds_in_crush_bucket=all_osds_of_rack,
                osds_ok_to_upgrade=all_osds_of_rack,
                osds_upgraded=[],
                bad_no_version=[],
            )

            assert expected == actual
            log.debug("Scenario3 passed: output matches expected for rack1")

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

        if crush_hierarchy_cleanup:
            log.info("=" * 80)
            log.info("Crush hierarchy cleanup")
            log.info("=" * 80)
            for osd_hostname in osd_hostnames:
                move_crush_item(
                    node=cephadm, crush_obj=osd_hostname, name="root", value="default"
                )

            for index in range(number_of_chassis_per_rack):
                chassis = f"chassis{index}"
                rados_obj.run_ceph_command(cmd=f"ceph osd crush rm {chassis}")

            for index in range(number_of_racks):
                rack = f"rack{index}"
                rados_obj.run_ceph_command(cmd=f"ceph osd crush rm {rack}")

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

        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1

        log.info("Test workflow finished; no crashes detected")
    return 0


class CrushRule:
    """Represents a CRUSH rule (e.g. chooseleaf by rack/chassis) for test pools."""

    def __init__(
        self,
        crush_rule_name: str,
        failure_domain: str,
        crushtool: CrushToolWorkflows,
        pool_type: str = "replicated",
    ):
        self.crush_rule_name: str = crush_rule_name
        self.failure_domain: str = failure_domain
        self.crushtool: CrushToolWorkflows = crushtool
        self.source_bin: str = "/etc/ceph"
        self.target_bin: str = "/etc/ceph"
        self.type: str = pool_type
        self.backup_original_crush_map_txt: str = ""

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
