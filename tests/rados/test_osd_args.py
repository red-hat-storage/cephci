"""
Tier-4 test module to verify negative scenarios for 'ceph osd reweight' commands
Currently executes the following cmds:
    - ceph osd reweight-by-pg     [<oload:int>] [<max_change:float>] [<max_osds:int>]
    - ceph osd reweight-by-utilization    [<oload:int>] [<max_change:float>] [<max_osds:int>]
    - ceph osd test-reweight-by-pg    [<oload:int>] [<max_change:float>] [<max_osds:int>]
    - ceph osd test-reweight-by-utilization    [<oload:int>] [<max_change:float>] [<max_osds:int>]
Input Args checked:
    oload | max_change | max_osds
"""

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    #CEPH-10417
    #BZ-1331770
    This test is to verify negative scenarios and inputs for
    'ceph osd' commands as listed below:
    - ceph osd reweight-by-pg
    - ceph osd reweight-by-utilization
    - ceph osd test-reweight-by-pg
    - ceph osd test-reweight-by-utilization
    1. Create cluster with default configuration
    2. Execute the commands with invalid inputs
    3. Ensure that execution with invalid input fails
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    cmd_list = config["cmd_list"]
    base_cmd = "ceph osd"
    oload_invalid = [100, -10, 0, "abcd"]
    max_invalid = [-10, 0, "abcd"]

    def execute_cmd_verify() -> bool:
        """
        Executes the input command on installer node,
        inside cephadm shell and verifies negative scenarios.
        :param cmd: (string) command to run
        :param value: invalid value for the cmd
        :return: True -> pass | False -> fail
        """
        log.info(
            f"Executing '{exec_cmd}' on {cephadm.installer.node.hostname} | Expected to fail"
        )
        out, err = cephadm.shell(args=[exec_cmd], check_status=False)
        log.debug(f"stdout: {out}")
        log.debug(f"stderr: {err}")
        if isinstance(value, str) and (
            f"Invalid command: {value} doesn't represent a float" in err
            or f"Invalid command: {value} doesn't represent an int"
        ):
            return True
        else:
            if "oload" in exec_cmd and (
                "Error EINVAL: You must give a percentage higher than 100." in err
                or "FAILED reweight-by-" in err
            ):
                return True
            if f"{value} must be positive" in err:
                return True
        return False

    try:
        for _cmd in cmd_list:
            for value in oload_invalid:
                exec_cmd = f"{base_cmd} {_cmd} --oload {value}"
                assert execute_cmd_verify()
                log.info("Failed as expected")
            for value in max_invalid:
                exec_cmd = f"{base_cmd} {_cmd} --max_change {value}"
                assert execute_cmd_verify()
                log.info("Failed as expected")
            for value in max_invalid:
                exec_cmd = f"{base_cmd} {_cmd} --max_osds {value}"
                assert execute_cmd_verify()
                log.info("Failed as expected")
            log.info(f"Verification for {base_cmd} {_cmd} completed")
    except AssertionError as AE:
        log.error(f"CMD: {exec_cmd} did not fail as expected")
        log.error(f"Execution failed with exception: {AE.__doc__}")
        log.exception(AE)
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
    log.info("All verifications completed")
    return 0
