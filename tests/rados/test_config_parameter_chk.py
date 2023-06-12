"""
This method contains the scenarios to check the-
1. MSGR V2 parameters in 5.x build
2. MSGR V2 parameters in 6.x build
3. Mclock *sleep* parameters are not modifiable
4. Mclock default parameter values
5. Mclock reservation,weight and limit parameters are not modifiable
"""

from configparser import ConfigParser

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)
ini_file = "conf/quincy/rados/test-confs/rados_config_parameters.ini"


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    osd_list = []
    mclock_profile = ["balanced", "high_recovery_ops", "high_client_ops"]
    # To get the parameters from the ini
    rados_object = RadosOrchestrator(node=cephadm)
    mon_object = MonConfigMethods(rados_obj=rados_object)

    config_info = ConfigParser()
    config_info.read(ini_file)

    # To get the OSD list
    ceph_nodes = kw.get("ceph_nodes")
    for node in ceph_nodes:
        if node.role == "osd":
            node_osds = rados_object.collect_osd_daemon_ids(node)
            osd_list = osd_list + node_osds
    if config.get("scenario") == "msgrv2_5x":
        msgr_parameter = config_info.items("MSGRV2")
        result = config_param_checker(mon_object, osd_list, msgr_parameter)
        if result:
            log.error("The MSGRV2 parameters are exist in the 5.x build")
            return 1
        return 0
    if config.get("scenario") == "msgrv2_6x":
        msgr_parameter = config_info.items("MSGRV2")
        result = config_param_checker(mon_object, osd_list, msgr_parameter)
        if not result:
            log.error("The MSGRV2 parameter value is not correct in the 6.x build")
            return 1
        return 0

    # Checking all sleep parameters in mclock profiles
    if config.get("scenario") == "mclock_sleep":
        mclock_sleep_parameters = config_info.items("Mclock_sleep")
        for profile in mclock_profile:
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
        return 0

    # Verifying the reservation, weight and limit parameters are not modifiable
    # Currently a BZ raised on this feature. The number is BZ#2124137.
    change_backfill_values = [0, 1, 2, 3]
    chk_flag = False
    if config.get("scenario") == "mclock_chg_chk":
        mclock_chg_parameters = config_info.items("Mclock_paramert_set")
        mclock_default_parmeters = config_info.items("Mclock_default")
        for profile in mclock_profile:
            mon_object.set_config(
                section="osd", name="osd_mclock_profile", value=profile
            )
            for option in (True, False):
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
                # This scenario  is fail due to the BZ#2215272
                try:
                    for parameter, old_value in zip(
                        mclock_default_parmeters, change_backfill_values
                    ):
                        mon_object.set_config(
                            section="osd", name=parameter[0], value=old_value
                        )

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
                finally:
                    # Setting to default values
                    for parameter in mclock_default_parmeters:
                        mon_object.set_config(
                            section="osd", name=parameter[0], value=parameter[-1]
                        )
                    mon_object.set_config(
                        section="osd",
                        name="osd_mclock_override_recovery_settings",
                        value="false",
                    )
                    if not chk_flag:
                        return 1

                # The logic implemented is -
                # 1.Getting the expected value of the parameter using the config show
                # 2.Modifying the expected value and set the OSDs with this new value.
                # 3.Get the actual value from the config show
                # 4.Compare the expected value and actual value.
                for param in mclock_chg_parameters:
                    for id in osd_list:
                        # Get the Value of the parameter using config show
                        old_value = mon_object.show_config("osd", id, param[0])
                        old_value = str(old_value).strip()
                        log.debug(f"The {param[0]}  parameter value is{old_value} ")
                        chg_value = float(old_value) + float(param[-1])
                        # set the new value to OSDs
                        mon_object.set_config(
                            section="osd", name=param[0], value=chg_value
                        )
                        # Get the new value of the parameter using config show
                        actual_value = mon_object.show_config("osd", id, param[0])
                        actual_value = str(actual_value).strip()
                        log.debug(
                            f"The {param[0]}  parameter value after setting is {actual_value}"
                        )
                        # Comparing the actual value with the expected value.
                        if actual_value != old_value:
                            log.error(
                                f"In the mclock {profile}  {param[0]} parameter value is changeable"
                            )
                            return 1
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
