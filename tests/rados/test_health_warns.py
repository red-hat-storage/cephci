"""
Module to verify health warnings in ceph cluster
1. BLUESTORE_SLOW_OP_ALERT
2. BLOCK_DEVICE_STALLED_READ_ALERT
"""

import random

from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
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
                    assert val == int(
                        mon_obj.show_config(daemon="osd", id=osd_id, param=key)
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

                assert mon_obj.remove_config(section="osd", name=key, verify_rm=False)

            # begin verification of precedence for the input parameters across config levels and MON and MGR daemons
            log.debug(
                "Begin verification of precedence for the input"
                " parameters across config levels and MON, MGR daemons"
            )
            value_map = {}
            for key in param_dict.keys():
                for level in ["global", "osd", "host", "daemon", "mon", "mgr"]:
                    log.info(
                        "Setting the value of parameter %s at %s level and verifying precedence/priority"
                        % (key, level)
                    )
                    rand_val = random.randint(10, 9999)
                    # retain value against each level
                    value_map[level] = rand_val
                    osd_id = random.choice(rados_obj.get_osd_list(status="up"))
                    if level == "host":
                        host_obj = rados_obj.fetch_host_node(
                            daemon_type="osd", daemon_id=osd_id
                        )
                        host_name = host_obj.hostname
                        assert mon_obj.set_config(
                            section="osd",
                            location_type=level,
                            location_value=host_name,
                            name=key,
                            value=rand_val,
                        )
                    elif level == "daemon":
                        assert mon_obj.set_config(
                            section=f"osd.{osd_id}", name=key, value=rand_val
                        )
                    else:
                        assert mon_obj.set_config(
                            section=level, name=key, value=rand_val
                        )

                    # validation checks depending on config level
                    if level == "host":
                        # get osds from host
                        changed_osds = rados_obj.collect_osd_daemon_ids(
                            osd_node=host_name
                        )
                        up_osds = rados_obj.get_osd_list(status="up")
                        diff_osds = list(set(up_osds) - set(changed_osds))
                        _osd_id = random.choice(diff_osds)
                        # OSDs from other hosts should still retain the value set at OSD level
                        assert value_map["osd"] == int(
                            mon_obj.show_config(daemon="osd", id=_osd_id, param=key)
                        ), "Config show o/p for %s for OSD %s is not as expected" % (
                            key,
                            _osd_id,
                        )
                    elif level == "daemon":
                        diff_list = rados_obj.get_osd_list(status="up")
                        diff_list.remove(osd_id)
                        _osd_id = random.choice(diff_list)
                        # Other OSDs should not get affected
                        assert value_map["daemon"] != int(
                            mon_obj.show_config(daemon="osd", id=_osd_id, param=key)
                        ), "Config show o/p for %s for OSD %s is not as expected" % (
                            key,
                            _osd_id,
                        )
                    if level in ["mon", "mgr"]:
                        assert rand_val != int(
                            mon_obj.show_config(daemon="osd", id=osd_id, param=key)
                        ), ("Config show o/p for %s not as expected" % key)
                    else:
                        assert rand_val == int(
                            mon_obj.show_config(daemon="osd", id=osd_id, param=key)
                        ), ("Config show o/p for %s not as expected" % key)

                # config clean-up
                log.info(
                    "Clean up all entries of parameter %s from ceph cluster config"
                    % key
                )
                # fetch ceph config dump in json format
                config_dump_json = rados_obj.run_ceph_command(
                    cmd="ceph config dump", client_exec=True
                )
                for config_section in config_dump_json:
                    if config_section["name"] == key:
                        if config_section.get("location_type"):
                            mon_obj.remove_config(
                                section=config_section["section"],
                                name=key,
                                location_type=config_section["location_type"],
                                location_value=config_section["location_value"],
                                verify_rm=False,
                            )
                        else:
                            mon_obj.remove_config(
                                section=config_section["section"],
                                name=key,
                                verify_rm=False,
                            )

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
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

            # removal of any residual entries in ceph config dump
            _params = [
                "bluestore_slow_ops_warn_lifetime",
                "bluestore_slow_ops_warn_threshold",
                "bdev_stalled_read_warn_lifetime",
                "bdev_stalled_read_warn_threshold",
            ]

            # fetch ceph config dump in json format
            config_dump_json = rados_obj.run_ceph_command(
                cmd="ceph config dump", client_exec=True
            )
            for config_section in config_dump_json:
                if config_section["name"] in _params:
                    if config_section.get("location_type"):
                        mon_obj.remove_config(
                            section=config_section["section"],
                            name=config_section["name"],
                            location_type=config_section["location_type"],
                            location_value=config_section["location_value"],
                            verify_rm=False,
                        )
                    else:
                        mon_obj.remove_config(
                            section=config_section["section"],
                            name=config_section["name"],
                            verify_rm=False,
                        )

            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.debug(
                f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
            )
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info(
            "Verification of config parameters for 'Bluestore Slow Ops' & 'Bluestore Stalled Reads' has been completed"
        )
        return 0
