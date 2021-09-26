import json
import logging
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator

log = logging.getLogger(__name__)


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
    ceph_nodes = kw.get("ceph_nodes")
    out, err = ceph_nodes[0].exec_command(cmd="uuidgen")
    uuid = out.read().strip().decode()[0:5]

    if config.get("ec_pool"):
        ec_config = config.get("ec_pool")
        ec_config.setdefault("pool_name", f"ecpool_{uuid}")
        if not create_erasure_pool(node=cephadm, name=uuid, **ec_config):
            log.error("Failed to create the EC Pool")
            return 1
        if not rados_obj.bench_write(**ec_config):
            log.error("Failed to write objects into the EC Pool")
            return 1
        rados_obj.bench_read(**ec_config)
        log.info("Created the EC Pool, Finished writing data into the pool")
        if ec_config.get("delete_pool"):
            if not detete_pool(node=cephadm, pool=ec_config["pool_name"]):
                log.error("Failed to delete EC Pool")
                return 1

    if config.get("replicated_pool"):
        rep_config = config.get("replicated_pool")
        rep_config.setdefault("pool_name", f"repool_{uuid}")
        if not create_pool(
            node=cephadm,
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
            if not detete_pool(node=cephadm, pool=rep_config["pool_name"]):
                log.error("Failed to delete replicated Pool")
                return 1

    if config.get("email_alerts"):
        alert_config = config.get("email_alerts")
        if not rados_obj.enable_email_alerts(**alert_config):
            log.error("Error while configuring email alerts")
            return 1
        log.info("email alerts configured")

    if config.get("log_to_file"):
        if not set_logging_to_file(node=cephadm):
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
                    log.error("Failed to write objects into the EC Pool")
                    return 1
                rados_obj.bench_read(**compression_conf)
                log.info(
                    "Created the replicated Pool, Finished writing data into the pool"
                )

    log.info("All Pre-requisites completed to run Rados suite")
    return 0


def set_logging_to_file(node: CephAdmin) -> bool:
    """
    Enables the cluster logging into files at var/log/ceph and checks file permissions
    Args:
        node: Cephadm node where the commands need to be executed

    Returns: True -> pass, False -> fail
    """
    try:
        cmd = "ceph config set global log_to_file true"
        node.shell([cmd])
        cmd = "ceph config set global mon_cluster_log_to_file true"
        node.shell([cmd])
    except Exception:
        log.error("Error while enabling config to log into file")
        return False

    # Sleeping for 10 seconds for files to be generated
    time.sleep(10)

    cmd = "ls -ll /var/log/ceph"
    out, err = node.shell([cmd])
    log.info(out)
    regex = r"\s*([-rwx]*)\.\s+\d\s+([\w]*)\s+([\w]*)\s+[\w\s:]*(ceph[\w.]*log)"
    perm = "-rw-------"
    user = "ceph"
    files = ["ceph.log", "ceph.audit.log"]
    if re.search(regex, out):
        match = re.findall(regex, out)
        for val in match:
            if not (val[0] == perm and val[1] == user and val[2] == user):
                log.error(f"file permissions are not correct for file : {val[3]}")
                return False
            if val[3] in files:
                files.remove(val[3])
    if files:
        log.error(f"Did not find the log files : {files}")
        return False
    return True


def create_erasure_pool(node: CephAdmin, name: str, **kwargs) -> bool:
    """
    Creates a erasure code profile and then creates a pool with the same
    Args:
        node: Cephadm node where the commands need to be executed
        name: Name of the profile to create
        **kwargs: Any other param that needs to be set in the EC profile
    Returns: True -> pass, False -> fail

    """
    failure_domain = kwargs.get("crush-failure-domain", "osd")
    k = kwargs.get("k", 3)
    m = kwargs.get("m", 2)
    plugin = kwargs.get("plugin", "jerasure")
    pool_name = kwargs.get("pool_name")
    profile_name = f"ecprofile_{name}"

    # Creating a erasure coded profile with the options provided
    cmd = (
        f"ceph osd erasure-code-profile set {profile_name}"
        f" crush-failure-domain={failure_domain} k={k} m={m} plugin={plugin}"
    )
    try:
        node.shell([cmd])
    except Exception as err:
        log.error(f"Failed to create ec profile : {profile_name}")
        log.error(err)
        return False

    cmd = f"ceph osd erasure-code-profile get {profile_name}"
    log.info(node.shell([cmd]))
    # Creating the pool with the profile created
    if not create_pool(
        node=node,
        ec_profile_name=profile_name,
        **kwargs,
    ):
        log.error(f"Failed to create Pool {pool_name}")
        return False
    log.info(f"Created the ec profile : {profile_name} and pool : {pool_name}")
    return True


def detete_pool(node: CephAdmin, pool: str) -> bool:
    """
    Deletes the given pool from the cluster
    Args:
        node: Cephadm node where the commands need to be executed
        pool: name of the pool to be deleted

    Returns: True -> pass, False -> fail
    """
    # Checking if config is set to allow pool deletion
    config_dump = run_ceph_command(node, cmd="ceph config dump")
    if "mon_allow_pool_delete" not in [conf["name"] for conf in config_dump]:
        cmd = "ceph config set mon mon_allow_pool_delete true"
        node.shell([cmd])

    existing_pools = run_ceph_command(node, cmd="ceph df")
    if pool not in [ele["name"] for ele in existing_pools["pools"]]:
        log.error(f"Pool:{pool} does not exist on cluster, cannot delete")
        return True

    cmd = f"ceph osd pool delete {pool} {pool} --yes-i-really-really-mean-it"
    node.shell([cmd])

    existing_pools = run_ceph_command(node, cmd="ceph df")
    if pool not in [ele["name"] for ele in existing_pools["pools"]]:
        log.info(f"Pool:{pool} deleted Successfully")
        return True
    log.error(f"Pool:{pool} could not be deleted on cluster")
    return False


def create_pool(node: CephAdmin, pool_name: str, pg_num: int = 64, **kwargs) -> bool:
    """
    Create a pool named from the pool_name parameter.
     Args:
        node: Cephadm node where the commands need to be executed
        pool_name: name of the pool being created.
        pg_num: initial number of pgs.
        kwargs: Any other args that need to be passed

     Returns: True -> pass, False -> fail
    """

    log.info(f"creating pool_name {pool_name}")
    cmd = f"ceph osd pool create {pool_name} {pg_num} {pg_num}"
    if kwargs.get("ec_profile_name"):
        cmd = f"{cmd} erasure {kwargs['ec_profile_name']}"
    try:
        node.shell([cmd])
    except Exception as err:
        log.error(f"Error creating pool : {pool_name}")
        log.error(err)
        return False

    # Enabling rados application on the pool
    enable_app_cmd = f"sudo ceph osd pool application enable {pool_name} {kwargs.get('app_name','rados')}"
    node.shell([enable_app_cmd])

    cmd_map = {
        "min_size": f" ceph osd pool set {pool_name} min_size {kwargs.get('min_size')}",
        "size": f" ceph osd pool set {pool_name} size {kwargs.get('min_size')}",
        "erasure_code_use_overwrites": f"ceph osd pool set {pool_name} "
        f"allow_ec_overwrites {kwargs.get('erasure_code_use_overwrites')}",
        "disable_pg_autoscale": f"ceph osd pool set {pool_name} pg_autoscale_mode off",
        "crush_rule": f"sudo ceph osd pool set {pool_name} crush_rule {kwargs.get('crush_rule')}",
        "pool_quota": f"ceph osd pool set-quota {pool_name} {kwargs.get('pool_quota')}",
    }
    for key in kwargs:
        if cmd_map.get(key):
            try:
                node.shell([cmd_map[key]])
            except Exception as err:
                log.error(f"Error setting the property : {key} for pool : {pool_name}")
                log.error(err)
                return False

    log.info(f"Created pool {pool_name} successfully")
    return True


def run_ceph_command(node: CephAdmin, cmd: str) -> dict:
    """
    Runs ceph commands with json tag for the action specified otherwise treats action as command
    and returns formatted output
    Args:
        node: Cephadm node where the commands need to be executed
        cmd: Command that needs to be run

    Returns: dictionary of the output
    """

    cmd = f"{cmd} -f json"
    out, err = node.shell([cmd])
    status = json.loads(out)
    return status
