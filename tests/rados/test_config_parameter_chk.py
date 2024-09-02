"""
This method contains the scenarios to check the-
1. MSGR V2 parameters in 5.x build
2. MSGR V2 parameters in 6.x build
3. Mclock *sleep* parameters are not modifiable
4. Mclock default parameter values
5. Mclock reservation,weight and limit parameters are not modifiable
"""

import random
import re
import time
from configparser import ConfigParser

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    osd_list = []
    mclock_profile = ["balanced", "high_recovery_ops", "high_client_ops"]
    rados_object = RadosOrchestrator(node=cephadm)
    mon_object = MonConfigMethods(rados_obj=rados_object)
    config_info = ConfigParser()

    # Get the RHCS build
    regex = r"\s*(\d.\d)-rhel-\d"
    build = (re.search(regex, config.get("build", config.get("rhbuild")))).groups()[0]
    log.info(f"The rhcs build version is-{build}")

    # To get the OSD list
    ceph_nodes = kw.get("ceph_nodes")
    for node in ceph_nodes:
        if node.role == "osd":
            node_osds = rados_object.collect_osd_daemon_ids(node)
            # Pick a single OSD from the host OSDs list
            node_osds = random.sample(node_osds, 1)
            osd_list = osd_list + node_osds
    log.info(f"The number of OSDs in the cluster are-{len(osd_list)}")
    # If OSD list is more than 10 then randomly picking the 10 OSDs to check the parameters.
    if len(osd_list) > 10:
        osd_list = random.sample(osd_list, 10)
    log.info(f"The parameters are checking on {osd_list} osds")
    if config.get("scenario") == "msgrv2_5x":
        ini_file = config.get("ini-file")
        config_info.read(ini_file)
        msgr_parameter = config_info.items("MSGRV2")
        result = config_param_checker(mon_object, osd_list, msgr_parameter)
        if result:
            log.error("The MSGRV2 parameters are exist in the 5.x build")
            return 1
        return 0
    if config.get("scenario") == "msgrv2_6x":
        ini_file = config.get("ini-file")
        config_info.read(ini_file)
        msgr_parameter = config_info.items("MSGRV2")
        result = config_param_checker(mon_object, osd_list, msgr_parameter)
        if not result:
            log.error("The MSGRV2 parameter value is not correct in the 6.x build")
            return 1
        return 0

    # Checking all sleep parameters in mclock profiles
    if config.get("scenario") == "mclock_sleep":
        ini_file = config.get("ini-file")
        config_info.read(ini_file)
        mclock_sleep_parameters = config_info.items("Mclock_sleep")
        for profile in mclock_profile:
            try:
                mon_object.set_config(
                    section="osd", name="osd_mclock_profile", value=profile
                )
                # Setting the osd_mclock_override_recovery_settings parameters
                for option in True, False:
                    mon_object.set_config(
                        section="osd",
                        name="osd_mclock_override_recovery_settings",
                        value=option,
                    )
                    # setting the  sleep parameters from 0 to 0.1
                    for param in mclock_sleep_parameters:
                        mon_object.set_config(
                            section="osd", name=param[0], value="0.100000"
                        )
                    # Check that sleep parameters are not modifiable and the value is 0
                    result = config_param_checker(
                        mon_object, osd_list, mclock_sleep_parameters
                    )
                    if not result:
                        log.error(
                            "The Mclock sleep parameter value is not correct in the 6.x build"
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
                for param in mclock_sleep_parameters:
                    mon_object.remove_config(section="osd", name=param[0])
                mon_object.remove_config(section="osd", name="osd_mclock_profile")
                mon_object.remove_config(
                    section="osd", name="osd_mclock_override_recovery_settings"
                )
                # log cluster health
                rados_object.log_cluster_health()
                # check for crashes after test execution
                if rados_object.check_crash_status():
                    log.error("Test failed due to crash at the end of test")
                    return 1
        return 0

    if config.get("scenario") == "mclock_chg_chk":
        change_backfill_values = [0, 1, 2, 3]
        chk_flag = False
        ini_file = config.get("ini-file")
        config_info.read(ini_file)
        mclock_chg_parameters = config_info.items("Mclock_paramert_set")
        mclock_default_parmeters = config_info.items("Mclock_default")
        for profile in mclock_profile:
            mon_object.set_config(
                section="osd", name="osd_mclock_profile", value=profile
            )
            for option in (True, False):
                try:
                    mon_object.set_config(
                        section="osd",
                        name="osd_mclock_override_recovery_settings",
                        value=option,
                    )

                    # Checking the Recovery/Backfill options in all mclock profiles
                    result = config_param_checker(
                        mon_object, osd_list, mclock_default_parmeters
                    )
                    if not result:
                        log.error(
                            f"The Mclock default parameter value is not correct for the {profile} profile"
                        )
                        return 1
                    log.info(
                        f"The default parameter values are correct for the {profile} profile"
                    )
                    # Changing the Recovery/Backfill options
                    for parameter, old_value in zip(
                        mclock_default_parmeters, change_backfill_values
                    ):
                        mon_object.set_config(
                            section="osd", name=parameter[0], value=old_value
                        )
                    # Implicit Wait after set configuration
                    time.sleep(5)
                    for parameter in mclock_default_parmeters:
                        for id in osd_list:
                            param_value = mon_object.show_config(
                                "osd", id, parameter[0]
                            )
                            param_value = str(param_value).strip()

                            if (param_value != parameter[-1]) and not option:
                                log.error(
                                    f"The mclock {parameter[0]} parameter changed after setting the "
                                    f"osd_mclock_override_recovery_settings to False"
                                )
                                return 1
                            elif (param_value == parameter[-1]) and option:
                                log.error(
                                    f"The mclock {parameter[0]} parameter not changed after setting the "
                                    f"osd_mclock_override_recovery_settings to True"
                                )
                                return 1
                    chk_flag = True
                except Exception as er:
                    log.error(
                        f"Error occured while testing the default backfill/recovery paramters:{er}"
                    )
                    return 1
                finally:
                    log.info(
                        "\n \n ************** Execution of finally block begins here *************** \n \n"
                    )
                    # Setting to default values
                    for parameter in mclock_default_parmeters:
                        mon_object.remove_config(section="osd", name=parameter[0])
                    mon_object.remove_config(
                        section="osd",
                        name="osd_mclock_override_recovery_settings",
                    )
                    if not chk_flag:
                        return 1
                    # log cluster health
                    rados_object.log_cluster_health()
                    # check for crashes after test execution
                    if rados_object.check_crash_status():
                        log.error("Test failed due to crash at the end of test")
                        return 1

                # The logic implemented is -
                # 1.Getting the expected value of the parameter using the config show
                # 2.Modifying the expected value and set the OSDs with this new value.
                # 3.Get the actual value from the config show
                # 4.Compare the expected value and actual value.
                for param in mclock_chg_parameters:
                    for id in osd_list:
                        try:
                            # Get the Value of the parameter using config show
                            old_value = mon_object.show_config("osd", id, param[0])
                            old_value = str(old_value).strip()
                            old_value = (
                                int(old_value) if "wgt" in param[0] else old_value
                            )
                            log.debug(f"The {param[0]}  parameter value is{old_value} ")
                            chg_value = float(old_value) + float(param[-1])
                            chg_value = (
                                int(chg_value) if "wgt" in param[0] else chg_value
                            )
                            # set the new value to OSDs
                            mon_object.set_config(
                                section="osd", name=param[0], value=chg_value
                            )
                            # Implicit Wait after set configuration
                            time.sleep(5)
                            # Get the new value of the parameter using config show
                            actual_value = mon_object.show_config("osd", id, param[0])
                            actual_value = str(actual_value).strip()
                            actual_value = (
                                int(actual_value) if "wgt" in param[0] else actual_value
                            )
                            log.debug(
                                f"The {param[0]}  parameter value after setting is {actual_value}"
                            )
                            # Comparing the actual value with the expected value.
                            if actual_value != old_value:
                                log.error(
                                    f"In the mclock {profile}  {param[0]} parameter value is changeable"
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
                            # remove value set for OSDs
                            mon_object.remove_config(section="osd", name=param[0])
                            # log cluster health
                            rados_object.log_cluster_health()
                            # check for crashes after test execution
                            if rados_object.check_crash_status():
                                log.error("Test failed due to crash at the end of test")
                                return 1

    # RocksDB compression parameter check
    if config.get("scenario") == "rocksdb_compression":
        # Checking the RHCS version.
        if float(build) < 7.1:
            log.info(
                "Test running on version less than 7.1, skipping verifying rocksdb compression parameter check"
            )
            return 0

        rocksdb_chk = mon_object.get_config(
            section="osd", param="bluestore_rocksdb_options"
        )
        if "kLZ4Compression" not in rocksdb_chk:
            log.error(
                f"The default compression value is not  kLZ4Compression- {rocksdb_chk}"
            )
            return 1
        log.info(
            f"The RocksDB compression value is found to be  kLZ4Compression-{rocksdb_chk}"
        )
    return 0


def config_param_checker(obj, id_list, parameters):
    """
    Checks the paramter value for all osd's
    Args:
        1. obj: Monitor object
        2. ID_list: OSD id list
        3. parameters : paramter name and value to verify
    Returns: True -> if all paramter values are same as input
             False -> if all paramter values are not same as input

    """
    flag = True
    for param in parameters:
        log.info(f"Checking the {param[0]} value")
        for id in id_list:
            param_value = obj.show_config("osd", id, param[0])
            param_value = str(param_value).strip()
            if param_value != param[-1]:
                flag = False
    return flag
