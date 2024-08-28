"""
Module to Verify if the config changes made in the ceph.conf file can be moved to central monitor database.

This is achieved by ceph config assimilate-conf -i <input file> -o <output file>.
This will ingest a configuration file from input file and move any valid options into the monitors’
configuration database.

Any settings that are unrecognized, invalid, or cannot be controlled by the monitor will be returned in an
abbreviated config file stored in output file.

This command is useful for transitioning from legacy configuration files to centralized monitor-based configuration.
"""

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw) -> int:
    """
    Module to Verify if the config changes made in the ceph.conf file can be moved to central monitor database.
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    cluster_conf_path = config["cluster_conf_path"]
    client_node = ceph_cluster.get_nodes(role="client")[0]

    log.info("Testing configuration assimilation from ceph.conf files")

    try:
        with open(cluster_conf_path, "r") as fd:
            cluster_conf_file = fd.read()

        log.debug(
            f"Content to be added in /etc/ceph/ceph.conf file : \n {cluster_conf_file}"
        )

        # Adding the configurations into the conf file.
        cmd = f"echo '{cluster_conf_file}' >> /etc/ceph/ceph.conf"
        client_node.exec_command(cmd=cmd, sudo=True)
        log.debug("Completed adding the configurations into the ceph.conf file ")

        log.debug(
            f"printing file contents of ceph.conf post modification "
            f"\n{client_node.exec_command(cmd='cat /etc/ceph/ceph.conf', sudo=True)}"
        )

        log.info("Assimilating the contents of ceph.conf into mon config")
        cmd = "ceph config assimilate-conf -i /etc/ceph/ceph.conf -o /etc/ceph/assimilate-op.txt"
        try:
            client_node.exec_command(cmd=cmd, sudo=True)
        except Exception as err:
            log.error(f"Failed to run the assimilate command. error : {err}")
            return 1

        # Verifying the configs set via the CLI in the mon config database.
        test_config = config.get("Verify_config_parameters")
        for conf in test_config["configurations"]:
            for entry in conf.values():
                if not mon_obj.verify_set_config(**entry):
                    log.error(f"Error setting config {entry}")
                    return 1

        log.info(
            "Verified the configurations set."
            " The configs are successfully moved from ceph.conf to mon database "
        )
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        # removing the additional configurations set in the mon database
        for conf in test_config["configurations"]:
            for entry in conf.values():
                if not mon_obj.remove_config(**entry):
                    log.error(
                        f"Failed to remove config : {conf} from the mon config database"
                    )
                    return 1
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    log.info("Configurations added from file and then removed. Workflow complete")
    return 0
