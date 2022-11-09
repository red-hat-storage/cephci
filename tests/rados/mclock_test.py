"""
This method contains the scenarios to the the Mclock
"""


import time
from configparser import ConfigParser

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)
get_config_cmd = "ceph config get osd  "
set_config_cmd = "ceph config set osd  "
ini_file = "tests/rados/rados_config_parameters.ini"


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    cmd = "ceph config get osd osd_mclock_profile"
    mclock_profiles = ["high_client_ops", "balanced", "high_recovery_ops", "custom"]

    # check for the default mclock profile
    status = cephadm.shell([cmd])
    if status[0].strip() != "high_client_ops":
        log.error("The default Mclock profile is not high_client_ops")
        return 1
    try:
        for profile in mclock_profiles:
            set_cmd = f"{set_config_cmd} {'osd_mclock_profile'}  {profile}"
            log.info(f" Changing the Mclock profile to  {profile} profile")
            rados_obj.run_ceph_command(cmd=set_cmd)
            time.sleep(3)
            test_status = config_change_check(rados_obj, profile)
            if test_status == 1:
                log.error(
                    f" Failed to set or get configuration parameters of the cluster for the  {profile} profile"
                )
                return 1
        return 0
    except Exception as err:
        log.error(
            f" Failed to set or get configuration parameters of the cluster for the  {profile} profile"
        )
        log.error(err)
        return 1
    finally:
        set_default_cmd = f"{set_config_cmd} {'osd_mclock_profile high_client_ops'}"
        rados_obj.run_ceph_command(cmd=set_default_cmd)
        time.sleep(3)


def config_change_check(rados_obj: RadosOrchestrator, profile):

    # Read Config paramters
    config_info = ConfigParser()
    config_info.read(ini_file)
    try:
        # Read values from mclock section
        mclock_parameters = config_info.items("Mclock")
        for param in mclock_parameters:
            cmd = f"{get_config_cmd}{param[0]}"
            status = rados_obj.run_ceph_command(cmd=cmd)
            if type(status) == int:
                new_status = status + 1
            set_cmd = f"{set_config_cmd}{param[0]}  {new_status}"
            rados_obj.run_ceph_command(cmd=set_cmd)
            time.sleep(2)
            get_cmd = f"{get_config_cmd}{param[0]}"
            status = rados_obj.run_ceph_command(cmd=get_cmd)
            if profile != "custom":
                if str(status).strip() != str(param[-1]).strip():
                    log.error(
                        f" Able to modify the configuration parameters for the {profile} profile"
                    )
                    return 1
            else:
                if str(status).strip() == str(param[-1]).strip():
                    log.error(
                        f" Not able to modify the configuration parameters for the {profile} profile"
                    )
                    return 1
        return 0
    except Exception as err:
        log.error("Failed to set or get configuration parameteers from the cluster")
        log.error(err)
        return 1
    finally:
        set_default_config(rados_obj)


def set_default_config(rados_obj: RadosOrchestrator):
    log.info(" Setting the all Mclock configration parameters to the default values")
    # Read Config paramters
    config_info = ConfigParser()
    # set_config_cmd = "ceph config set osd  "
    config_info.read(ini_file)
    # Read values from mclock section
    mclock_parameters = config_info.items("Mclock")
    for param in mclock_parameters:
        set_cmd = f"{set_config_cmd}{param[0]}  { param[-1]}"
        rados_obj.run_ceph_command(cmd=set_cmd)
        time.sleep(2)
