"""
Module to verify health warnings in ceph cluster
1. BLUESTORE_SLOW_OP_ALERT
2. BLOCK_DEVICE_STALLED_READ_ALERT
"""

import random

from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Module to verify specific Health warnings being generated correctly
    1. BLUESTORE_SLOW_OP_ALERT
    2. BLOCK_DEVICE_STALLED_READ_ALERT
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)

    if config.get("param-check"):
        doc = (
            "\n# CEPH-83620333"
            "\n# CEPH-83620336"
            "\n Verify ceph config parameters 'bluestore_slow_ops_warn_lifetime',"
            " 'bluestore_slow_ops_warn_threshold', 'bdev_stalled_read_warn_lifetime' and"
            " 'bdev_stalled_read_warn_threshold' with various positive and negative values"
            "\n\t 1. Deploy a cluster with at least 1 mon, mgr, OSD, and RGW each"
            "\n\t 2. Retrieve the default value of above mentioned ceph config parameters"
            "\n\t 3. Check default value of parameters"
            "\n\t 4. Ensure setting of positive values if accepted"
            "\n\t 5. Ensure setting of negative values is rejected"
        )

        log.info(doc)
        log.info(
            "Running test to verify config parameters around: 'Bluestore Slow Ops' & 'Bluestore Stalled Reads'"
        )

        def common_workflow(param_dict: dict):
            """
            Method to execute common workflow for verification of parameters
            Args:
                param_dict: dictionary to control parameters for verification
            Returns:
                None
            """
            # verify default values
            log.info(
                "Verifying default values of newly introduced parameters - %s"
                % param_dict.keys()
            )

            for key, val in param_dict.items():
                out = mon_obj.get_config(section="osd", param=key)
                if str(val) != out:
                    log.error("Default value of %s is not as expected" % key)
                    log.error("Expected %d | Actual %s" % (val, out))
                    raise AssertionError("Default value of %s is not as expected" % key)

            log.info("Completed verification of newly added parameters")

            # verify possible modifications
            pos_values = [0, 100, 65356, 1048576]
            neg_values = [-1, "fdn56", 9.6, "gfdgdf", "1000s", 1 / 3]
            for key in param_dict.keys():
                for val in pos_values:
                    assert mon_obj.set_config(
                        section="osd", name=key, value=val
                    ), "Could not set value %s for parameter %s" % (val, key)

                    osd_id = random.choice(rados_obj.get_osd_list(status="up"))
                    assert str(val) == mon_obj.show_config(
                        daemon="osd", id=osd_id, param=key
                    ), ("Config show o/p for %s not as expected" % key)

                for val in neg_values:
                    try:
                        if mon_obj.set_config(section="osd", name=key, value=val):
                            log.error(
                                "Could set negative value %s for parameter %s"
                                % (val, key)
                            )
                            raise Exception("Comfig change should have failed")
                    except CommandFailed as err:
                        log.warning(
                            "Expected Failure - Command execution failed with error: \n%s"
                            % err
                        )

                assert mon_obj.remove_config(section="osd", name=key)

        try:
            if config.get("param-check").get("slow-ops"):
                _dict = {
                    "bluestore_slow_ops_warn_lifetime": 86400,
                    "bluestore_slow_ops_warn_threshold": 1,
                }

            elif config.get("param-check").get("stalled-reads"):
                _dict = {
                    "bdev_stalled_read_warn_lifetime": 86400,
                    "bdev_stalled_read_warn_threshold": 1,
                }

            else:
                log.error("Incorrect input, check test case configs")
                raise Exception("Incorrect input, check test case configs")

            common_workflow(param_dict=_dict)

        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            # log cluster health
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )

            _dict = {
                "bluestore_slow_ops_warn_lifetime": 86400,
                "bluestore_slow_ops_warn_threshold": 1,
                "bdev_stalled_read_warn_lifetime": 86400,
                "bdev_stalled_read_warn_threshold": 1,
            }

            [mon_obj.remove_config(section="osd", name=key) for key in _dict.keys()]

            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info(
            "Verification of config parameters for 'Bluestore Slow Ops' & 'Bluestore Stalled Reads' has been completed"
        )
        return 0
