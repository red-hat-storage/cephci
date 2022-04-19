from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Prepares the cluster to run rados tests.
    Actions Performed:
    1. Create a Replicated and Erasure coded pools and write Objects into pools
    2. Setup email alerts for sending errors/warnings on the cluster.
        Verifies Bugs:
        https://bugzilla.redhat.com/show_bug.cgi?id=1849894
        https://bugzilla.redhat.com/show_bug.cgi?id=1878145
    3. Enable logging into file and check file permissions
        Verifies Bug : https://bugzilla.redhat.com/show_bug.cgi?id=1884469
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
        kw: Args that need to be passed to the test for initialization

    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    out, err = cephadm.shell(["uuidgen"])
    uuid = out.split("-")[0]

    if config.get("ec_pool"):
        ec_config = config.get("ec_pool")
        ec_config.setdefault("pool_name", f"ecpool_{uuid}")
        if not rados_obj.create_erasure_pool(name=uuid, **ec_config):
            log.error("Failed to create the EC Pool")
            return 1

        if ec_config.get("test_overwrites_pool"):
            if not rados_obj.verify_ec_overwrites(**ec_config):
                log.error("Failed to create the EC Pool")
                return 1
        else:
            if not rados_obj.bench_write(**ec_config):
                log.error("Failed to write objects into the EC Pool")
                return 1
            rados_obj.bench_read(**ec_config)
            log.info("Created the EC Pool, Finished writing data into the pool")

        if ec_config.get("delete_pool"):
            if not rados_obj.detete_pool(pool=ec_config["pool_name"]):
                log.error("Failed to delete EC Pool")
                return 1

    if config.get("replicated_pool"):
        rep_config = config.get("replicated_pool")
        rep_config.setdefault("pool_name", f"repool_{uuid}")
        if not rados_obj.create_pool(
            **rep_config,
        ):
            log.error("Failed to create the replicated Pool")
            return 1
        if not rados_obj.bench_write(**rep_config):
            log.error("Failed to write objects into the EC Pool")
            return 1
        rados_obj.bench_read(**rep_config)
        log.info("Created the replicated Pool, Finished writing data into the pool")
        if rep_config.get("delete_pool"):
            if not rados_obj.detete_pool(pool=rep_config["pool_name"]):
                log.error("Failed to delete replicated Pool")
                return 1

    if config.get("set_pool_configs"):
        changes = config["set_pool_configs"]
        pool_name = changes["pool_name"]
        configurations = changes["configurations"]
        for conf in configurations.keys():
            if not rados_obj.set_pool_property(
                pool=pool_name, props=conf, value=configurations[conf]
            ):
                log.error(f"failed to set property {conf} on the cluster")
                return 1
        log.info(f"made the config changes on the pool {pool_name}")

    if config.get("email_alerts"):
        alert_config = config.get("email_alerts")
        if not rados_obj.enable_email_alerts(**alert_config):
            log.error("Error while configuring email alerts")
            return 1
        log.info("email alerts configured")

    if config.get("Verify_config_parameters"):
        test_config = config.get("Verify_config_parameters")
        test_node = ceph_cluster.get_nodes(role="osd")[0]
        for conf in test_config["configurations"]:
            for entry in conf.values():
                if entry.get("location_type") == "host":
                    entry["location_value"] = test_node.hostname
                if not mon_obj.set_config(**entry):
                    log.error(f"Error setting config {conf}")
                    return 1
        log.info("done")
        pool_name = "test_pool_1"
        if not rados_obj.create_pool(pool_name=pool_name, pg_num=16):
            log.error("Failed to create the replicated Pool")
            return 1

        rados_obj.bench_write(pool_name=pool_name, rados_write_duration=50)

        # Removing test configurations
        for conf in test_config["configurations"]:
            for entry in conf.values():
                if entry.get("location_type") == "host":
                    entry["location_value"] = test_node.hostname
                if not mon_obj.remove_config(**entry):
                    log.error(f"Error setting config {conf}")
                    return 1
        log.info("finished removing values, passed")

    if config.get("log_to_file"):
        if not rados_obj.enable_file_logging():
            log.error("Error while setting config to enable logging into file")
            return 1
        log.info("Logging to file configured")

    if config.get("cluster_configuration_checks"):
        cls_config = config.get("cluster_configuration_checks")
        if not rados_obj.set_cluster_configuration_checks(**cls_config):
            log.error("Error while setting Cluster config checks")
            return 1
        log.info("Set up cluster configuration checks")

    if config.get("configure_balancer"):
        balancer_config = config.get("configure_balancer")
        if not rados_obj.enable_balancer(**balancer_config):
            log.error("Error while setting up balancer on the Cluster")
            return 1
        log.info("Set up Balancer on the cluster")

    if config.get("configure_pg_autoscaler"):
        autoscaler_config = config.get("configure_pg_autoscaler")
        if not rados_obj.configure_pg_autoscaler(**autoscaler_config):
            log.error("Error while setting up pg_autoscaler on the Cluster")
            return 1
        log.info("Set up pg_autoscaler on the cluster")

    if config.get("enable_compression"):
        compression_conf = config["enable_compression"]
        pool_name = compression_conf["pool_name"]
        for conf in compression_conf["configurations"]:
            for entry in conf.values():
                if not rados_obj.pool_inline_compression(pool_name=pool_name, **entry):
                    log.error(
                        f"Error setting compression on pool : {pool_name} for config {conf}"
                    )
                    return 1
                if not rados_obj.bench_write(**compression_conf):
                    log.error("Failed to write objects into Pool")
                    return 1
                rados_obj.bench_read(**compression_conf)
                log.info(
                    "Created the replicated Pool, Finished writing data into the pool"
                )
        log.info("Completed compression tests")

    if config.get("delete_pools"):
        for name in config["delete_pools"]:
            if not rados_obj.detete_pool(name):
                log.error(f"the pool {name} could not be deleted")
                return 1
        log.info("deleted all the given pools successfully")

    log.info("All Pre-requisites completed to run Rados suite")
    return 0
