import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from tests.rados.monitor_configurations import MonElectionStrategies
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Changes b/w various election strategies and observes mon quorum behaviour
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonElectionStrategies(rados_obj=rados_obj)
    installer_node = ceph_cluster.get_nodes(role="installer")[0]
    mon_workflow_obj = MonitorWorkflows(node=cephadm)
    rados_obj = RadosOrchestrator(node=cephadm)

    # Collecting the number of mons in the quorum before the test
    mon_init_count = len(mon_obj.get_mon_quorum().keys())

    if config.get("stretch_mode"):
        try:
            log.info(
                "Verifying ceph mon election strategies in Stretch mode enabled stretch cluster"
            )

            # Verifying the default mode of election strategy is connectivity.
            default_strategy = mon_obj.get_election_strategy()
            if default_strategy != 3:
                log.error(
                    f"Stretch cluster election strategy was not 'connectivity', i.e {default_strategy}"
                )
                return 1
            log.info(
                f"Default election strategy for Mon verified successfully: {default_strategy}"
            )

            # Try changing the election strategy mode to classic or disallow, should fail
            # RFE raised to restrict user from changing Mon election strategy in Stretch mode
            # https://bugzilla.redhat.com/show_bug.cgi?id=2264124
            # Changing strategy to 1. i.e. classic mode
            """
            log.info("Trying to change mon election strategy to 1. i.e. classic mode")
            if mon_obj.set_election_strategy(mode="classic"):
                log.error(
                    "Able to set election strategy to classic mode in stretch mode, should not have worked"
                )
                return 1

            # Changing strategy to 2. i.e. disallowed mode
            log.info("Trying to change mon election strategy to 2. i.e. disallow mode")
            if mon_obj.set_election_strategy(mode="disallow"):
                log.error(
                    "Able to set election strategy to disallow mode in stretch mode, should not have worked"
                )
                return 1
            """

            # Checking connectivity scores of all the mons
            cmd = f"ceph daemon mon.{installer_node.hostname} connection scores dump"
            mon_conn_score = rados_obj.run_ceph_command(cmd=cmd)
            log.info(
                f"Connectivity score of all daemons in the cluster: \n {mon_conn_score}"
            )

            # reset the connectivity scores of all the mons
            log.info("Reset the connectivity scores of all the mons")
            cmd = f"ceph daemon mon.{installer_node.hostname} connection scores reset"
            cephadm.shell(args=[cmd])

            # Print the connectivity scores of all the mons post reset
            cmd = f"ceph daemon mon.{installer_node.hostname} connection scores dump"
            mon_conn_score = rados_obj.run_ceph_command(cmd=cmd)
            log.info(
                f"Connectivity score of all daemons post reset: \n {mon_conn_score}"
            )

            if not check_mon_status(rados_obj, mon_obj):
                log.error("All Mon daemons are not in running state")
                return 1
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("Completed all mon election test scenarios valid for stretch mode")
        return 0

    else:
        try:
            # By default, the election strategy is classic. Verifying that
            default_strategy = mon_obj.get_election_strategy()
            if default_strategy != 1:
                log.error(
                    f"cluster created election strategy other than classic, i.e {default_strategy}"
                )
                return 1

            log.info(
                "\n -------------------------------------------"
                "\nValidating error generated when Mon is added to disallow list in classic mode"
                "\n -------------------------------------------"
            )
            leader_mon = mon_obj.get_mon_quorum_leader()
            try:
                if mon_obj.set_disallow_mon(mon=leader_mon):
                    log.error(
                        f"Mon {leader_mon} added to disallow list in classic Mode"
                    )
                    return 1
            except Exception as e:
                expected_err_msg = "Error EINVAL: You cannot disallow monitors in your current election mode"
                generated_err_msg = e.args[0]
                log.info(
                    f"\nExpected error: {expected_err_msg}"
                    f"\nGenerated error: {generated_err_msg}"
                )
                if expected_err_msg not in generated_err_msg:
                    log.error(
                        "Expected error not in generated error when Mon added to disallow list in classic mode\n"
                    )
                    return 1
            log.info(
                "\n -------------------------------------------"
                "\nSuccessfully validated expected error in generated error "
                "when Mon is added to disallow list in classic mode"
                "\n -------------------------------------------"
            )

            check_rank_after_mon_removal(
                mon_workflow_obj=mon_workflow_obj, rados_obj=rados_obj
            )

            # # Changing strategy to 2. i.e. disallowed mode
            if not mon_obj.set_election_strategy(mode="disallow"):
                log.error("could not set election strategy to disallow mode")
                return 1

            # sleeping for 2 seconds for new elections to be triggered and new leader to be elected
            time.sleep(2)

            log.info(
                "Election strategy set to disallow mode. adding leader mon to disallowed list"
            )
            # Checking if new leader will be chosen if leader is added to disallowed list
            old_leader = mon_obj.get_mon_quorum_leader()
            if not mon_obj.set_disallow_mon(mon=old_leader):
                log.error(f"could not add Mon {old_leader} to the disallowed list")
                return 1

            # sleeping for 2 seconds for new elections to be triggered and new leader to be elected
            time.sleep(2)

            current_leader = mon_obj.get_mon_quorum_leader()
            if re.search(current_leader, old_leader):
                log.error(
                    f"The mon: {old_leader} added to disallow list is still leader"
                )
                return 1

            # removing the mon from the disallowed list
            if not mon_obj.remove_disallow_mon(mon=old_leader):
                log.error(f"could not remove mon: {old_leader} from disallowed list")
                return 1

            # sleeping for 2 seconds for new elections to be triggered and new leader to be elected
            time.sleep(2)

            add_all_mons_to_disallow_list(
                mon_obj=mon_obj, mon_workflow_obj=mon_workflow_obj
            )

            disallowed_leader_checks(
                mon_obj=mon_obj,
                mon_workflow_obj=mon_workflow_obj,
                election_strategy="classic",
            )

            check_rank_after_mon_removal(
                mon_workflow_obj=mon_workflow_obj, rados_obj=rados_obj
            )

            if not mon_obj.set_election_strategy(mode="connectivity"):
                log.error("could not set election strategy to connectivity mode")
                return 1

            # Changing strategy to 3. i.e Connectivity mode.
            if not mon_obj.set_election_strategy(mode="connectivity"):
                log.error("could not set election strategy to connectivity mode")
                return 1

            # Checking connectivity scores of all the mons
            cmd = f"ceph daemon mon.{installer_node.hostname} connection scores dump"
            mon_conn_score = rados_obj.run_ceph_command(cmd=cmd)
            log.info(
                f"Connectivity score of all daemons in the cluster: \n {mon_conn_score}"
            )

            add_all_mons_to_disallow_list(
                mon_obj=mon_obj, mon_workflow_obj=mon_workflow_obj
            )

            disallowed_leader_checks(
                mon_obj=mon_obj,
                mon_workflow_obj=mon_workflow_obj,
                election_strategy="classic",
            )

            disallowed_leader_checks(
                mon_obj=mon_obj,
                mon_workflow_obj=mon_workflow_obj,
                election_strategy="disallow",
            )

            check_rank_after_mon_removal(
                mon_workflow_obj=mon_workflow_obj, rados_obj=rados_obj
            )

            # Changing strategy to default
            if not mon_obj.set_election_strategy(mode="classic"):
                log.error("could not set election strategy to classic mode")
                return 1

            # sleeping for 5 seconds for new elections to be triggered and new leader to be elected
            time.sleep(5)

            # todo: add other tests to ascertain the health of mon daemons in quorum
            # [12-Feb-2024] added ^
            if not check_mon_status(rados_obj, mon_obj):
                log.error("All Mon daemons are not in running state")
                return 1

            # Collecting the number of mons in the quorum after the test
            mon_final_count = len(mon_obj.get_mon_quorum().keys())
            if mon_init_count < mon_final_count:
                log.error(
                    "There are less mons in the quorum at the end than there before"
                )
                return 1

        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # Changing strategy to default
            if not mon_obj.set_election_strategy(mode="classic"):
                log.error("could not set election strategy to classic mode")
                return 1
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("Completed all mon election test cases")
        return 0


def check_mon_status(rados_obj, mon_obj):
    """Method to verify all Mon daemons are in running state
    Args:
        rados_obj: CephAdmin object
        mon_obj: MonElectionStrategies class object
    Returns:
        True -> pass | False -> fail
    """
    # check running status of each mon daemon
    mon_list = mon_obj.get_mon_quorum().keys()
    log.debug(f"List of Monitors in the cluster: {mon_list}")
    for mon in mon_list:
        status, desc = rados_obj.get_daemon_status(daemon_type="mon", daemon_id=mon)
        if status != 1 or desc != "running":
            log.error(f"{mon} is not in running state: {status} | {desc}")
            return False
        log.info(f"Mon {mon} is in healthy state: {status} | {desc}")
    return True


def add_all_mons_to_disallow_list(mon_obj, mon_workflow_obj) -> None:
    """Method to validate last Mon cannot be added to disallowed leaders list

    Steps:
    - Get list of all Mons from the cluster
    - Add all Mons to disallowed leaders list
    - Validate last allowed Mon cannot be added to disallow list
    - Revert cluster back to original state by removing all Mons from disallow list

    Args:
        mon_obj: object MonElectionStrategies
        mon_workflow_obj: Object of MonitorWorkflows
    Returns:
        Raises Exception when test fails
    Example:

    """
    log.info(
        "\n -------------------------------------------"
        "\n Starting validation:"
        "\n `last allowed Mon` cannot be added to disallow list"
        "\n -------------------------------------------"
    )

    mons = list(mon_workflow_obj.get_mon_quorum().keys())
    last_allowed_mon = mons.pop()

    for mon in mons:
        log.info(f"Adding mon {mon} to disallow list")
        if not mon_obj.set_disallow_mon(mon=mon):
            raise AssertionError(f"could not add Mon {mon} to disallowed list")
        log.info(f"Successfully added mon {mon} to disallow list")

    log.info(
        f"Adding last remaining allowed mon {last_allowed_mon} to disallow list\n"
        "Proceeding to validate error generation when last allowed mon is added to disallow list"
    )
    try:
        if mon_obj.set_disallow_mon(mon=last_allowed_mon):
            raise AssertionError(
                f"Last allowed mon {last_allowed_mon} added to disallow list"
            )

    except AssertionError as ae:
        raise AssertionError(ae.args[0])

    except Exception as e:
        expected_err_msg = f"Error EINVAL: mon.{last_allowed_mon} is the only remaining allowed leader!"
        generated_err_msg = e.args[0]
        log.info(
            f"\nExpected error: {expected_err_msg}"
            f"\nGenerated error: {generated_err_msg}"
        )
        if expected_err_msg not in generated_err_msg:
            raise AssertionError(
                "Expected error not in generated error when adding all mons to disallow list\n"
            )
        log.info(
            "Successfully validated all Mons cannot not be added to disallow list\n"
        )

    log.info(
        "Successfully validated Last remaining Mon cannot be added to disallow list\n"
        "Reverting cluster back to original state\n"
        f"Proceeding to remove mons {mons} from disallow list"
    )

    for mon in mons:
        log.info(f"Removing mon {mon} from disallow list")
        if not mon_obj.remove_disallow_mon(mon=mon):
            raise AssertionError(f"could not remove Mon {mon} from disallowed list")
        log.info(f"Successfully removed Mon {mon} from disallow list")

    log.info(
        "\n -------------------------------------------"
        "\n Completed validation:"
        "\n `last allowed Mon` cannot be added to disallow list"
        "\n -------------------------------------------"
    )


def check_rank_after_mon_removal(mon_workflow_obj, rados_obj):
    """Method to remove and add Mon back to the cluster and perform checks on Mon rank

    Steps:
    - Select Monitor daemon
    - Set Monitor service to unmanaged
    - Remove Monitor daemon
    - Add back removed Monitor daemon
    - Set Monitor service to managed
    - perform validations for rank change described in check_rank_change()
    - Repeat steps for Monitor with least rank, middle rank and highest rank

    Args:
        mon_workflow_obj: Object of MonitorWorkflows
        rados_obj: Object of RadosOrchestrator
    Returns:
        Raises exception if any of the checks fail
    Example:
        check_rank_after_mon_removal(mon_workflow_obj=mon_workflow_obj, rados_obj=rados_obj)
    """

    def check_rank_change(
        quorum_before_mon_removal: dict,
        quorum_after_mon_removal: dict,
        removed_mon_rank: str,
    ) -> bool:
        """Method to validate Mon ranks are updated after Mon removal and addition.

        Example:
            # ceph mon dump
            0: [v2:10.0.66.26:3300/0,v1:10.0.66.26:6789/0] mon.node0
            1: [v2:10.0.67.217:3300/0,v1:10.0.67.217:6789/0] mon.node1
            2: [v2:10.0.67.245:3300/0,v1:10.0.67.245:6789/0] mon.node2
            3: [v2:10.0.67.243:3300/0,v1:10.0.67.243:6789/0] mon.node3
            4: [v2:10.0.67.220:3300/0,v1:10.0.67.220:6789/0] mon.node4

            Method validates below scenarios based on removed_mon_rank:

            If mon.node0 is removed and added back
                - all Mons with rank {1,2,3,4} should move up by 1 rank
                - Mon node0 is added back with rank 4
            If Mon node2 is removed and added back
                - all Mons with rank {3,4} should move up by 1 rank
                - Mon node2 is added back with rank 4
                - Mons with rank {0,1} should remain as it is
            If Mon node4 is removed and added back
                - Ranks of Mons remain unchanged

        Args:
            quorum_before_mon_removal: Mon details before removal and Addition of Mon
            quorum_after_mon_removal:  Mon details after removal and Addition of Mon
            removed_mon_rank: Rank of Mon removed from cluster
        Returns:
            True: Validation completed
            False: Validation failed
        Usage:
            check_rank_change(mons={"node1":0, "node2":1, "node3":2}, removed_mon_rank="0")
        """

        log.info(
            "Starting to validate Mon rank changes"
            f"Rank of Mon removed and added back: {removed_mon_rank}"
            f"\nQuorum before Monitor removal: {quorum_before_mon_removal}"
            f"\nQuorum after Monitor removal: {quorum_after_mon_removal}"
        )

        expected_quorum_after_mon_removal = dict()
        # generate expected_quorum_after_mon_removal dict
        for mon_name, mon_rank in quorum_before_mon_removal.items():
            if mon_rank == removed_mon_rank:
                expected_quorum_after_mon_removal[mon_name] = (
                    len(quorum_before_mon_removal) - 1
                )
            elif mon_rank > removed_mon_rank:
                expected_quorum_after_mon_removal[mon_name] = (
                    quorum_before_mon_removal[mon_name] - 1
                )
            else:
                expected_quorum_after_mon_removal[mon_name] = quorum_before_mon_removal[
                    mon_name
                ]

        log.info(
            f"\nExpected quorum after Monitor removal: {expected_quorum_after_mon_removal}"
        )

        for mon_name, mon_rank in quorum_after_mon_removal.items():
            if (
                quorum_after_mon_removal[mon_name]
                != expected_quorum_after_mon_removal[mon_name]
            ):
                log.error(
                    "Ranks of Mons has not updated after Mon removal and addition"
                    f"Expected rank of removed Mon after adding it back {mon_name} "
                    f"=> {expected_quorum_after_mon_removal[mon_name]}"
                    "Current rank of removed Mon after adding it back "
                    f"{mon_name} => {quorum_after_mon_removal[mon_name]}"
                )
                return False

        log.info(
            "\nCompleted validations for Mon rank change"
            f"\nQuorum before Monitor removal: {quorum_before_mon_removal}"
            f"\nQuorum after Monitor removal: {quorum_after_mon_removal}"
            f"\nExpected quorum after Monitor removal: {expected_quorum_after_mon_removal}"
        )
        return True

    log.info(
        "\n -------------------------------------------"
        "\n Starting validation:"
        "\n Rank update on removing and adding Mon"
        "\n -------------------------------------------"
    )

    n = len(mon_workflow_obj.get_mon_quorum())
    # Selects lowest ranked Mon, middle ranked Mon and highest ranked Mon from `ceph mon dump` command
    for index in [0, n // 2, -1]:

        log.info(f"Collecting details of Mon for Index {index}")
        quorum_before_mon_removal = mon_workflow_obj.get_mon_quorum()
        mon_names = list(quorum_before_mon_removal.keys())
        mon_name = mon_names[index]
        mon_rank = quorum_before_mon_removal[mon_name]
        mon_host = rados_obj.get_host_object(hostname=mon_name)

        log.info(f"Mon ranks before Mon removal: {quorum_before_mon_removal}")
        log.info(
            "\nValidating rank update when Mon is removed and added back"
            f"\nMon name: {mon_name}"
            f"\nMon rank: {mon_rank}"
        )

        if not mon_workflow_obj.set_mon_service_managed_type(unmanaged=True):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not unmanaged error")

        log.info(f"Removing and adding Mon {mon_rank} back to cluster")
        if not mon_workflow_obj.remove_mon_service(host=mon_name):
            log.error("Could not remove the Data site mon daemon")
            raise Exception("mon service not removed error")

        if not mon_workflow_obj.add_mon_service(host=mon_host):
            log.error("Could not add mon service")
            raise Exception("mon service not added error")

        if not mon_workflow_obj.set_mon_service_managed_type(unmanaged=False):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not unmanaged error")

        quorum_after_mon_removal = mon_workflow_obj.get_mon_quorum()

        log.info(f"Mon ranks before Mon removal: {quorum_before_mon_removal}")
        log.info("Proceeding to validate rank changes after Mon removal and addition")
        if not check_rank_change(
            quorum_before_mon_removal, quorum_after_mon_removal, mon_rank
        ):
            raise AssertionError("Mon ranks are not updated")
        log.info(
            f"Completed validation of rank changes when Mon with rank {mon_rank} is removed and added back"
        )

    log.info(
        "\n -------------------------------------------"
        "\n Completed validation:"
        "\n Rank update on removing and adding Mon"
        "\n -------------------------------------------"
    )


def disallowed_leader_checks(mon_obj, mon_workflow_obj, election_strategy: str) -> bool:
    """Module to perform validations of election strategy changes when Mon in disallow list.

     **Note Test module not supported with classic mode, since adding Mons to disallow list is not supported
     in classic mode

     Detailed test steps:
        - Add lowest ranked Mon to disallowed leaders list
        - Check Mon is not leader, Mon is in quorum and Mon is in disallowed leaders list
        - Change election strategy
        - Scenario 1: disallow -> classic
            > Mon becomes leader
            > Mon is part of Quorum
        - Scenario 2: connectivity -> classic
            > Mon becomes leader
            > Mon is part of Quorum
        - Scenario 3: connectivity -> disallow
            > Mon does not become leader
            > Mon is part of Quorum
            > Mon is still in disallowed leaders list
        - Switch back to previous election strategy
        - Check Mon is not leader, Mon is in quorum and Mon is in disallowed leaders list
        - Remove lowest ranked Mon from disallowed leaders list
        - If original strategy was disallow, Check lowest ranked Mon becomes leader.
          Check Mon is in Quorum and Mon is removed from disallowed leaders list

    Args:
        mon_obj: Object of MonElectionStrategies
        election_strategy: disallow or connectivity
        mon_workflow_obj: Object of MonitorWorkflows
    Example:
        Scenario 1: disallow -> classic
            check_disallowed_leaders_election_strategy(mon_obj, election_strategy="classic")
        Scenario 2: connectivity -> classic
            check_disallowed_leaders_election_strategy(mon_obj, election_strategy="classic")
        Scenario 3: connectivity -> disallow
            check_disallowed_leaders_election_strategy(mon_obj, election_strategy="disallow")
    Returns:
        None
        Raises exception when tests fail
    """

    def _validations(mon, mon_election_obj, mon_workflow_obj) -> None:
        """
        validation module to check below scenarios:
        - Mon should not be leader
        - Mon is in disallowed leaders list
        - Mon is part of Quorum
        Args:
            mon: Monitor to perform validations
            mon_election_obj: Object of MonElectionStrategies
            mon_workflow_obj: Object of MonitorWorkflows
        Returns:
            None
            Raises exception when validations fail
        Usage:
            _validations(mon=lowest_ranked_mon, mon_election_obj=mon_obj, mon_workflow_obj=mon_workflow_obj)
        """
        log.info(
            f"Validating Mon {mon} is not elected leader after adding Mon to disallow list"
        )

        leader = mon_election_obj.get_mon_quorum_leader()
        if mon == leader:
            raise AssertionError(
                f"Mon with rank 0 [{mon}] was added to disallow list,"
                "but was elected as leader in disallow mode"
            )

        log.info(
            f"Successfully validated Mon {mon} is not leader. Current leader is {leader}"
            f"Validating Mon {mon} is part of disallowed list"
        )

        disallowed_list = mon_election_obj.get_disallowed_mon_list()
        if mon not in disallowed_list:
            raise AssertionError(
                f"Mon with rank 0 [{mon}] was added to disallow list,"
                "but not present in disallow list"
            )

        log.info(
            f"Successfully validated Mon {mon} is part of disallow list"
            f"Validating Mon {mon} is part of Monitor quorum"
        )

        quorum = list(mon_workflow_obj.get_mon_quorum().keys())
        if mon not in quorum:
            raise AssertionError(
                f"Mon with rank 0 [{mon}] was added to disallow list,"
                "but removed from quorum"
            )

        log.info(
            f"Successfully validated Mon {mon} is still part of quorum"
            f"Changing election strategy to Connectivity mode"
        )

        log.info(
            f"\nVerified Mon {mon} is not elected leader"
            f"\nVerified Mon {mon} is part of disallowed leaders list"
            f"\nVerified Mon {mon} is part of Monitor quorum"
        )

    strategies = {1: "classic", 2: "disallow", 3: "connectivity"}
    original_election_strategy = strategies[mon_obj.get_election_strategy()]

    if (
        original_election_strategy == 1
        or original_election_strategy == election_strategy
    ):
        log.error(
            "Cannot perform test in classic election strategy and"
            " election strategy to switch cannot be same as current strategy"
        )
        return False

    log.info(
        "\n -------------------------------------------"
        "\n Starting validation:"
        f"\n Changing election strategy from {original_election_strategy} mode "
        f"to {election_strategy} mode when lowest ranked Mon in disallowed leader list"
        "\n -------------------------------------------"
    )

    lowest_ranked_mon = list(mon_workflow_obj.get_mon_quorum())[0]

    log.info(f"Adding Mon {lowest_ranked_mon} to disallowed leaders list")
    if not mon_obj.set_disallow_mon(mon=lowest_ranked_mon):
        log.error(f"Could not add Mon {lowest_ranked_mon} to disallow list")
        return False

    log.info(f"Proceeding to perform checks on disallowed Mon {lowest_ranked_mon}")
    # Check Mon is not leader, Mon is in quorum and Mon is in disallow leaders list
    _validations(
        mon=lowest_ranked_mon,
        mon_election_obj=mon_obj,
        mon_workflow_obj=mon_workflow_obj,
    )

    log.info(f"Proceeding to set election strategy to {election_strategy}")
    if not mon_obj.set_election_strategy(mode=election_strategy):
        log.error(f"Could not set election strategy to {election_strategy} mode")
        return False

    log.info("Proceeding to checks on lowest ranked Mon as disallowed leader")
    if election_strategy == "classic":
        # When switching election mode to classic from disallow/connectivity,
        # If lowest ranked Mon is in disallowed leaders list
        #   - Mon should become leader
        #   - Mon should be in Quorum
        current_leader = mon_workflow_obj.get_mon_quorum_leader()
        if lowest_ranked_mon != current_leader:
            log.error("Mon with lowest rank is not leader in classic mode")
            return False

        if lowest_ranked_mon not in mon_workflow_obj.get_mon_quorum().keys():
            log.error("Mon with lowest rank is not in Quorum in classic mode")
            return False

    elif election_strategy == "disallow":
        # When switching election mode to disallow from connectivity,
        # If lowest ranked Mon is in disallowed leaders list,
        #  - Mon should not become leader
        #  - Mon should still be in Quorum
        #  - Mon should still be in disallowed leaders list
        _validations(
            mon=lowest_ranked_mon,
            mon_election_obj=mon_obj,
            mon_workflow_obj=mon_workflow_obj,
        )

    log.info(
        "Successfully completed validation of lowest ranked Mon as disallowed leader"
    )
    log.info("Proceeding to revert back cluster to previous state")
    log.info(
        f"Proceeding to revert election strategy to original state {original_election_strategy}"
    )
    if not mon_obj.set_election_strategy(mode=original_election_strategy):
        raise AssertionError(
            f"could not set election strategy to {original_election_strategy} mode"
        )

    log.info(
        "Validate Mon is not leader, Mon is in quorum and Mon is in disallow leaders list "
        "after reverting election strategy to original state"
    )
    _validations(
        mon=lowest_ranked_mon,
        mon_election_obj=mon_obj,
        mon_workflow_obj=mon_workflow_obj,
    )

    log.info(
        f"Proceeding to revert disallowed leaders, removing Mon {lowest_ranked_mon} from list"
    )
    if not mon_obj.remove_disallow_mon(mon=lowest_ranked_mon):
        raise AssertionError(
            f"could not remove Mon {lowest_ranked_mon} from disallowed list"
        )

    log.info(
        "Proceeding to perform post test scenario validations\n"
        "- Mon becomes leader if original election strategy is disallow\n"
        "- Mon is part of Quorum\n"
        "- Mon is not in disallowed leaders list"
    )
    current_leader = mon_workflow_obj.get_mon_quorum_leader()
    if original_election_strategy == "disallow" and lowest_ranked_mon != current_leader:
        log.error(
            "Mon with lowest rank is not leader after removal from disallowed leaders list"
        )
        return False

    if lowest_ranked_mon not in mon_workflow_obj.get_mon_quorum().keys():
        log.error(
            "Mon with lowest rank is not in Quorum after removal from disallowed leaders list"
        )
        return False

    disallowed_list = mon_obj.get_disallowed_mon_list()
    if lowest_ranked_mon in disallowed_list:
        log.error(
            "Mon with lowest rank is still in disallowed leaders list post removal"
        )
        return False

    log.info(
        "\n -------------------------------------------"
        "\n Completed validation:"
        f"\n Changing election strategy from {original_election_strategy} mode "
        f"to {election_strategy} mode when lowest ranked Mon in disallowed leader list"
        "\n -------------------------------------------"
    )
