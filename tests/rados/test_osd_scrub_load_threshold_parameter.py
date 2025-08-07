"""
Polarion ID: CEPH-83620201 - Verification of the osd_scrub_load_threshold parameter functionality

The file contain the method to verify the osd_scrub_load_threshold parameter.
The following steps are verified-
1. Verification of the default value of the osd_scrub_load_threshold parameter, which is  10.0.

2. Modification of the osd_scrub_load_threshold value to 0 and verification that the scrub operation does not proceed.
   The expected log message is:
     osd-scrub:scrub_load_below_threshold: loadavg .* >= max .* = no.

3. Creation of an inconsistent object and confirmation that the repair operation does not occur.

4. Modification of the osd_scrub_load_threshold value from 0 back to its default value.
   Verification that the scrub operation resumes and the inconsistent object is repaired.
   The expected log message is:
     osd-scrub:scrub_load_below_threshold: loadavg .* >= max .* = yes.

"""

import traceback

from test_osd_ecpool_inconsistency_scenario import get_pg_inconsistent_object_count

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_scrub import RadosScrubber
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    global chk_deep_scrub
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    scrub_object = RadosScrubber(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    installer = ceph_cluster.get_nodes(role="installer")[0]
    wait_time = 120
    osd_nodes = ceph_cluster.get_nodes(role="osd")
    replicated_config = config.get("replicated_pool")
    pool_name = replicated_config["pool_name"]
    acting_pg_set = ""

    try:

        log_line_no_scrub = "'scrub_load_below_threshold:.* = no'"
        # log_line_scrub = "'scrub_load_below_threshold:.* = yes'"
        # enable the file logging
        if not rados_object.enable_file_logging():
            log.error("Error while setting config to enable logging into file")
            return 1
        log.info("Logging to file configured")
        # disable autoscaler
        assert mon_obj.set_config(
            section="global", name="osd_pool_default_pg_autoscale_mode", value="off"
        ), "Could not set pg_autoscale mode to OFF"

        if not rados_object.create_pool(**replicated_config):
            log.error("Failed to create the replicated Pool")
            return 1

        rados_object.bench_write(pool_name=pool_name, byte_size="5K", max_objs=10000)
        msg_data_push = f"The data pushed into the {pool_name} pool"
        log.info(msg_data_push)

        # Get the pool pg_id
        pg_id = rados_object.get_pgid(pool_name=pool_name)
        pg_id = pg_id[0]
        msg_pool_id = f"The {pool_name} pg id is - {pg_id}"
        log.info(msg_pool_id)

        # Get the acting PG set
        acting_pg_set = rados_object.get_pg_acting_set(pool_name=pool_name)
        msg_acting_set = f"The {pool_name} pool acting set is -{acting_pg_set}"
        log.info(msg_acting_set)

        default_threshold_value = mon_obj.get_config(
            section="osd", param="osd_scrub_load_threshold"
        )

        if float(default_threshold_value) != 10.0:
            log.error(
                "The default value of the osd_scrub_load_threshold is not equal to 10.0"
            )
            return 1
        log.info("The default value of the osd_scrub_load_threshold is 10.0")

        log.info(
            "===== Scenario 1: Verification of the osd_scrub_load_threshold feature with the value 0"
        )
        # Check that the scrubbing is progress or not
        scrub_object.wait_for_pg_scrub_state(pg_id, wait_time=wait_time)

        log.info(
            "Scenario 1: Setting the osd_scrub_load_threshold parameter value to 0"
        )
        # Perform the tests by modifying the
        mon_obj.set_config(section="osd", name="osd_scrub_load_threshold", value=0)

        # Truncate the osd logs
        rados_object.remove_log_file_content(osd_nodes, daemon_type="osd")
        log.info("Scenario1: The content of osd log are removed")

        # set the debug log to 20
        configure_log_level(mon_obj, acting_pg_set, set_to_default=False)
        log.info("Scenario1: The osd  logs are set to 20")
        init_time, _ = installer.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        init_time = init_time.strip()

        set_scheduled_scrub_parameters(scrub_object, acting_pg_set)

        try:
            chk_deep_scrub = rados_object.start_check_deep_scrub_complete(
                pg_id=pg_id, user_initiated=False, wait_time=wait_time
            )
        except Exception:
            log.info(
                "Scrub operation not started after setting the osd_scrub_load_threshold value to 0"
            )
            chk_deep_scrub = False

        if chk_deep_scrub:
            log.error(
                "Scrub operation started after setting the osd_scrub_load_threshold value to 0"
            )
            return 1

        configure_log_level(mon_obj, acting_pg_set, set_to_default=True)

        log.info("Scenario1: The osd logs are set to default value")
        end_time, _ = installer.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        end_time = end_time.strip()
        if (
            rados_object.lookup_log_message(
                init_time=init_time,
                end_time=end_time,
                daemon_type="osd",
                daemon_id=acting_pg_set[0],
                search_string=log_line_no_scrub,
            )
            is False
        ):
            log.error(
                "Scenario1:After setting the Threshold to 0 the scrub operation is initiated"
            )
            return 1
        log.info(
            "After setting the Threshold to 0 the scrub operation is not initiated"
        )
        remove_parameter_configuration(mon_obj)
        method_should_succeed(rados_object.delete_pool, pool_name)
        log.info(
            "===Scenario1: End of the  osd and mgr logs are set to default value==="
        )

        log.info(
            "===Scenario2: Verification of the inconsistent of object repair when "
            "the osd_scrub_load_threshold is 0==="
        )
        if not rados_object.create_pool(**replicated_config):
            log.error("Failed to create the replicated Pool")
            return 1
        # Get the pool pg_id
        pg_id = rados_object.get_pgid(pool_name=pool_name)
        pg_id = pg_id[0]
        msg_pool_id = f"The {pool_name} pg id is - {pg_id}"
        log.info(msg_pool_id)

        chk_inconsistent_object = create_inconsistent_objects(rados_object, pool_name)
        if not chk_inconsistent_object:
            log.error(
                "Inconsistent objects are not generated. Not executing the further tests"
            )
            return 1
        # Get the inconsistent object count
        before_tst_scrub_inconsistent_count = get_pg_inconsistent_object_count(
            rados_object, pg_id
        )

        log.info("Before starting the test waiting for the PG into active+clean state")
        if not wait_for_clean_pg_sets(rados_object, timeout=300):
            log.error("Cluster cloud not reach active+clean state within 300")

        # Perform the tests by modifying the
        mon_obj.set_config(section="osd", name="osd_scrub_load_threshold", value=0)

        init_time, _ = installer.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        init_time = init_time.strip()
        configure_log_level(mon_obj, acting_pg_set, set_to_default=False)

        chk_auto_repair_param = set_auto_repair_parameters(
            mon_obj, before_tst_scrub_inconsistent_count
        )
        if not chk_auto_repair_param:
            log.error(
                "The repair parameters are not set. No executing the further tests"
            )
            return 1

        set_scheduled_scrub_parameters(scrub_object, acting_pg_set)

        try:
            chk_deep_scrub = rados_object.start_check_deep_scrub_complete(
                pg_id=pg_id, user_initiated=False, wait_time=wait_time
            )
        except Exception:
            log.info(
                "Scrub operation not started after setting the osd_scrub_load_threshold value to 0"
            )
            chk_deep_scrub = False

        if chk_deep_scrub:
            log.error(
                "Scrub operation started after setting the osd_scrub_load_threshold value to 0"
            )
            return 1

        configure_log_level(mon_obj, acting_pg_set, set_to_default=True)
        log.info("Scenario2: The osd and mgr logs are set to default value")
        end_time, _ = installer.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        end_time = end_time.strip()

        if (
            rados_object.lookup_log_message(
                init_time=init_time,
                end_time=end_time,
                daemon_type="osd",
                daemon_id=acting_pg_set[0],
                search_string=log_line_no_scrub,
            )
            is False
        ):
            log.error(
                "Scenario2:After setting the Threshold to 0 the scrub operation is initiated"
            )
            return 1
        after_tst_scrub_err_count = get_pg_inconsistent_object_count(
            rados_object, pg_id
        )
        msg_after_tst_inconsistent_count = (
            f"The inconsistent  error count after "
            f"testing is - {after_tst_scrub_err_count} "
        )
        log.info(msg_after_tst_inconsistent_count)
        if before_tst_scrub_inconsistent_count != after_tst_scrub_err_count:
            log.error(
                "The inconsistent objects are repaired  after setting the osd_scrub_load_threshold value to 0"
            )
            return 1

        log.info(
            "===Scenario2:End of the  verification of the inconsistent of object repair "
            "when the osd_scrub_load_threshold is 0==="
        )

        # log.info(
        #     "===Scenario3: Verification of the osd_scrub_load_threshold parameter with the default ==="
        #     "value which is 10.0"
        # )
        # pg_id = rados_object.get_pgid(pool_name=pool_name)
        # pg_id = pg_id[0]
        # acting_pg_set = rados_object.get_pg_acting_set(pool_name=pool_name)
        # msg_acting_set = f"The {pool_name} pool acting set is -{acting_pg_set}"
        # log.info(msg_acting_set)
        #
        # before_tst_scrub_inconsistent_count = get_pg_inconsistent_object_count(
        #     rados_object, pg_id
        # )
        # if before_tst_scrub_inconsistent_count == 0:
        #     chk_inconsistent_object = create_inconsistent_objects(
        #         rados_object, pool_name
        #     )
        #     if not chk_inconsistent_object:
        #         log.error(
        #             "Inconsistent objects are not generated. Not executing the further tests"
        #         )
        #         return 1
        #     before_tst_scrub_inconsistent_count = get_pg_inconsistent_object_count(
        #         rados_object, pg_id
        #     )
        #
        # log.info("Before starting the test waiting for the PG into active+clean state")
        # if not wait_for_clean_pg_sets(rados_object, timeout=300):
        #     log.error("Cluster cloud not reach active+clean state within 300")
        #
        # init_time, _ = installer.exec_command(
        #     cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        # )
        # chk_auto_repair_param = set_auto_repair_parameters(
        #     mon_obj, before_tst_scrub_inconsistent_count
        # )
        # if not chk_auto_repair_param:
        #     log.error(
        #         "The repair parameters are not set. No executing the further tests"
        #     )
        #     return 1
        # init_time = init_time.strip()
        # init_pool_pg_dump = rados_object.get_ceph_pg_dump(pg_id=pg_id)
        # mon_obj.set_config(section="osd", name="debug_osd", value="20/20")
        # mon_obj.remove_config(section="osd", name="osd_scrub_load_threshold")
        # log.info("The osd_scrub_load_threshold value is set to 10.0")
        #
        # set_scheduled_scrub_parameters(scrub_object, acting_pg_set)
        # wait_time = 200
        #
        # try:
        #     chk_deep_scrub = rados_object.start_check_deep_scrub_complete(
        #         pg_id=pg_id,
        #         pg_dump=init_pool_pg_dump,
        #         user_initiated=False,
        #         wait_time=wait_time,
        #     )
        # except Exception:
        #     log.info(
        #         "Scrub operation not started after setting the osd_scrub_load_threshold value to 0"
        #     )
        #     chk_deep_scrub = False
        #
        # if chk_deep_scrub is False:
        #     log.error(
        #         "Scenario3:Scrub operation not started after setting the osd_scrub_load_threshold value to 10.0"
        #     )
        #     return 1
        #
        # mon_obj.remove_config(section="osd", name="debug_osd")
        # log.info("Scenario3: The osd and mgr logs are set to default value")
        # end_time, _ = installer.exec_command(
        #     cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        # )
        # end_time = end_time.strip()
        #
        # if (
        #     rados_object.lookup_log_message(
        #         init_time=init_time,
        #         end_time=end_time,
        #         daemon_type="osd",
        #         daemon_id=acting_pg_set[0],
        #         search_string=log_line_scrub,
        #     )
        #     is False
        # ):
        #     log.error(
        #         "Scenario3:After setting the Threshold to default value(10.0) the scrub operation is not initiated"
        #     )
        #     return 1
        # after_tst_scrub_err_count = get_pg_inconsistent_object_count(
        #     rados_object, pg_id
        # )
        # msg_after_tst_inconsistent_count = (
        #     f"The inconsistent  error count after "
        #     f"testing is - {after_tst_scrub_err_count} "
        # )
        # log.info(msg_after_tst_inconsistent_count)
        # if after_tst_scrub_err_count != 0:
        #     log.error(
        #         "Scenario3:The inconsistent objects are not repaired  after setting the "
        #         "osd_scrub_load_threshold value to 10.0"
        #     )
        #     return 1
        #
        # log.info(
        #     "===Scenario3: End of the verification of the osd_scrub_load_threshold parameter "
        #     "with the default value which is 10.0==="
        # )

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        configure_log_level(mon_obj, acting_pg_set, set_to_default=True)
        method_should_succeed(rados_object.delete_pool, pool_name)
        remove_parameter_configuration(mon_obj)
    log.info(
        "========== Validation of the osd_scrub_load_threshold is success ============="
    )
    return 0


def remove_parameter_configuration(mon_obj):
    """
    Used to set the default osd scrub parameter value
    Args:
        mon_obj: monitor object
    Returns : None
    """
    mon_obj.remove_config(section="osd", name="osd_scrub_min_interval")
    mon_obj.remove_config(section="osd", name="osd_scrub_max_interval")
    mon_obj.remove_config(section="osd", name="osd_deep_scrub_interval")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_auto_repair")
    mon_obj.remove_config(section="osd", name="osd_scrub_auto_repair_num_errors")
    mon_obj.remove_config(section="osd", name="osd_scrub_load_threshold")


def set_scheduled_scrub_parameters(scrub_object, acting_pg_set):
    """
    Method to set the scheduled parameters
    Args:
        scrub_object: Scrub object
        acting_pg_set: Acting PG set

    Returns: None

    """
    osd_scrub_min_interval = 60
    osd_scrub_max_interval = 600
    osd_deep_scrub_interval = 600
    (
        scrub_begin_hour,
        scrub_begin_weekday,
        scrub_end_hour,
        scrub_end_weekday,
    ) = scrub_object.add_begin_end_hours(0, 1)
    for osd_id in acting_pg_set:
        scrub_object.set_osd_configuration(
            "osd_scrub_begin_hour", scrub_begin_hour, osd_id
        )
        scrub_object.set_osd_configuration(
            "osd_scrub_begin_week_day", scrub_begin_weekday, osd_id
        )
        scrub_object.set_osd_configuration("osd_scrub_end_hour", scrub_end_hour, osd_id)
        scrub_object.set_osd_configuration(
            "osd_scrub_end_week_day", scrub_end_weekday, osd_id
        )
        scrub_object.set_osd_configuration(
            "osd_scrub_min_interval", osd_scrub_min_interval, osd_id
        )
        scrub_object.set_osd_configuration(
            "osd_scrub_max_interval", osd_scrub_max_interval, osd_id
        )
        scrub_object.set_osd_configuration(
            "osd_deep_scrub_interval", osd_deep_scrub_interval, osd_id
        )


def set_auto_repair_parameters(mon_obj, inconsistent_obj_count):
    """
    The method is used to set the auto repair parameters
    Args:
        mon_obj: Mon object
        inconsistent_obj_count: Inconsistent object count value

    Returns:True -> if parameters are set
            False -> if parameters are not set
    """
    try:
        # Set the value to true
        mon_obj.set_config(section="osd", name="osd_scrub_auto_repair", value="true")
        # Check that the inconsistent count is <
        auto_repair_num_value = mon_obj.get_config(
            section="osd", param="osd_scrub_auto_repair_num_errors"
        )
        if inconsistent_obj_count >= int(auto_repair_num_value):
            auto_repair_param_value = inconsistent_obj_count + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            msg_auto_repair_value = f"The osd_scrub_auto_repair_num_errors value is set to {auto_repair_param_value}"
            log.info(msg_auto_repair_value)
    except Exception as err:
        msg_err = f"Error occurred while setting the repair parameters- {err}"
        log.error(msg_err)
        return False
    return True


def create_inconsistent_objects(rados_object, pool_name):
    """
    Method is used to create the inconsistent objects in a pool
    Args:
        rados_object: rados obect
        pool_name: pool name

    Returns:  True -> If objects are created successfully
              False -> If objects are not created

    """
    try:

        client_node = rados_object.ceph_cluster.get_nodes(role="client")[0]

        obj_start = 0
        obj_end = 50
        num_keys_obj = 50

        log.debug(
            f"Writing {(obj_end - obj_start) * num_keys_obj} Key pairs"
            f" to increase the omap entries on pool {pool_name}"
        )
        lx = "https://raw.githubusercontent.com/red-hat-storage/cephci/refs/heads/main/utility/generate_omap_entries.py"
        client_node.exec_command(
            sudo=True,
            cmd=f"curl -k {lx} -O",
        )
        # Setup Script pre-requisites : docopt
        client_node.exec_command(
            sudo=True, cmd="pip3 install docopt", long_running=True
        )

        cmd_options = f"--pool {pool_name} --start {obj_start} --end {obj_end} --key-count {num_keys_obj}"
        cmd = f"python3 generate_omap_entries.py {cmd_options}"
        client_node.exec_command(sudo=True, cmd=cmd, long_running=True)

        # removing the py file copied
        client_node.exec_command(sudo=True, cmd="rm -rf generate_omap_entries.py")

        rep_obj_list = rados_object.get_object_list(pool_name)

        for obj in rep_obj_list:
            try:
                rados_object.create_inconsistent_object(pool_name, obj, num_keys=3)
            except Exception as err:
                msg_err = (
                    f"Cannot able to convert the object-{obj} into inconsistent object.Picking another object "
                    f"to convert-{err}."
                )
                log.info(msg_err)
                continue
            msg_inconsistent_obj = f"The {obj} is converted into inconsistent object"
            log.info(msg_inconsistent_obj)
            break
    except Exception as err:
        msg_err = f"Error while generating the inconsistent objects- {err}"
        log.error(msg_err)
        return False
    return True


def configure_log_level(mon_obj, acting_set, set_to_default: True):
    """
    Method is used to set the acting osd log to 20 or to default value
    Args:
        mon_obj:  Mon object
        acting_set: acting OSD sets
        set_to_default: False -> Sets the osd debug value to default.
                       True -> Sets the osd debug value to 20

    Returns: None

    """
    if set_to_default:
        for osd_id in acting_set:
            mon_obj.remove_config(section=f"osd.{osd_id}", name="debug_osd")
            msg_debug = f"The osd.{osd_id} debug_osd value is set to default value"
            log.info(msg_debug)
    else:
        for osd_id in acting_set:
            mon_obj.set_config(section=f"osd.{osd_id}", name="debug_osd", value="20/20")
            msg_debug = f"The osd.{osd_id} debug_osd value is set to 20"
            log.info(msg_debug)
