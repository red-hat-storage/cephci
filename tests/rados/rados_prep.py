import logging
import re
import time

from ceph.ceph_admin import CephAdmin

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
    ceph_nodes = kw.get("ceph_nodes")
    out, err = ceph_nodes[0].exec_command(cmd="uuidgen")
    uuid = out.read().strip().decode()[0:5]

    if config.get("ec_pool"):
        ec_config = config.get("ec_pool")
        pool_name = ec_config.setdefault("pool_name", f"ecpool_{uuid}")
        if not create_erasure_pool(node=cephadm, name=uuid, **ec_config):
            log.error("Failed to create the EC Pool")
            return 1
        if not run_rados_bench_write(node=cephadm, pool_name=pool_name):
            log.error("Failed to write objects into the EC Pool")
            return 1
        run_rados_bench_read(node=cephadm, pool_name=pool_name)
        log.info("Created the EC Pool, Finished writing data into the pool")

    if config.get("replicated_pool"):
        rep_config = config.get("replicated_pool")
        pool_name = rep_config.setdefault("pool_name", f"repool_{uuid}")
        if not create_pool(
            node=cephadm,
            disable_pg_autoscale=True,
            **rep_config,
        ):
            log.error("Failed to create the replicated Pool")
            return 1
        if not run_rados_bench_write(node=cephadm, pool_name=pool_name):
            log.error("Failed to write objects into the EC Pool")
            return 1
        run_rados_bench_read(node=cephadm, pool_name=pool_name)
        log.info("Created the replicated Pool, Finished writing data into the pool")

    if config.get("email_alerts"):
        alert_config = config.get("email_alerts")
        if not enable_email_alerts(node=cephadm, **alert_config):
            log.error("Error while configuring email alerts")
            return 1
        log.info("email alerts configured")

    if config.get("log_to_file"):
        if not set_logging_to_file(node=cephadm):
            log.error("Error while setting config to enable logging into file")
            return 1
        log.info("Logging to file configured")

    log.info("All Pre-requisites completed to run Rados suite")
    return 0


def run_rados_bench_write(node: CephAdmin, pool_name: str, **kwargs) -> bool:
    """
    Method to trigger Write operations via the Rados Bench tool
    Args:
        node: Cephadm node where the commands need to be executed
        pool_name: pool on which the operation will be performed
        kwargs: Any other param that needs to passed

    Returns: True -> pass, False -> fail

    """
    duration = kwargs.get("duration", 200)
    byte_size = kwargs.get("byte_size", 4096)
    cmd = f"sudo rados --no-log-to-stderr -b {byte_size} -p {pool_name} bench {duration} write --no-cleanup"
    try:
        node.shell([cmd])
        return True
    except Exception as err:
        log.error(f"Error running rados bench write on pool : {pool_name}")
        log.error(err)
        return False


def run_rados_bench_read(node: CephAdmin, pool_name: str, **kwargs) -> bool:
    """
    Method to trigger Read operations via the Rados Bench tool
    Args:
        node: Cephadm node where the commands need to be executed
        pool_name: pool on which the operation will be performed
        kwargs: Any other param that needs to passed

    Returns: True -> pass, False -> fail

    """
    duration = kwargs.get("duration", 200)
    try:
        cmd = f"rados --no-log-to-stderr -p {pool_name} bench {duration} seq"
        node.shell([cmd])
        cmd = f"rados --no-log-to-stderr -p {pool_name} bench {duration} rand"
        node.shell([cmd])
        return True
    except Exception as err:
        log.error(f"Error running rados bench write on pool : {pool_name}")
        log.error(err)
        return False


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
                print(f"file permissions are not correct for file : {val[3]}")
                return False
            if val[3] in files:
                files.remove(val[3])
    if files:
        log.error(f"Did not find the log files : {files}")
        return False
    return True


def enable_email_alerts(node: CephAdmin, **kwargs) -> bool:
    """
    Enables the email alerts module and configures alerts to be sent
    Args:
        node: Cephadm node where the commands need to be executed
        **kwargs: Any other param that needs to be set

    Returns: True -> pass, False -> fail
    """
    alert_cmds = {
        "smtp_host": f"ceph config set mgr mgr/alerts/smtp_host "
        f"{kwargs.get('smtp_host', 'smtp.corp.redhat.com')}",
        "smtp_sender": f"ceph config set mgr mgr/alerts/smtp_sender "
        f"{kwargs.get('smtp_sender', 'ceph-iad2-c01-lab.mgr@redhat.com')}",
        "smtp_ssl": f"ceph config set mgr mgr/alerts/smtp_ssl {kwargs.get('smtp_ssl', 'false')}",
        "smtp_port": f"ceph config set mgr mgr/alerts/smtp_port {kwargs.get('smtp_port', '25')}",
        "interval": f"ceph config set mgr mgr/alerts/interval {kwargs.get('interval', '5')}",
        "smtp_from_name": f"ceph config set mgr mgr/alerts/smtp_from_name "
        f"'{kwargs.get('smtp_from_name', 'Rados 5.0 sanity Cluster')}'",
    }
    try:
        cmd = "ceph mgr module enable alerts"
        node.shell([cmd])

        for cmd in alert_cmds.values():
            node.shell([cmd])

        if kwargs.get("smtp_destination"):
            for email in kwargs.get("smtp_destination"):
                cmd = f"ceph config set mgr mgr/alerts/smtp_destination {email}"
                node.shell([cmd])
        else:
            log.error("email addresses not provided")
            return False

    except Exception as err:
        log.error("Error while configuring the cluster for email alerts")
        log.error(err)
        return False

    # Printing all the configuration set for email alerts
    cmd = "ceph config dump | grep 'mgr/alerts'"
    log.info(node.shell([cmd]))

    # Disabling and enabling the email alert module after setting all the config
    try:
        states = ["disable", "enable"]
        for state in states:
            cmd = f"ceph mgr module {state} alerts"
            node.shell([cmd])
            time.sleep(2)
    except Exception as err:
        log.error("Error while enabling/disabling alerts module after configuration")
        log.error(err)
        return False

    # Triggering email alert
    try:
        cmd = "ceph alerts send"
        node.shell([cmd])
    except Exception as err:
        log.error("Error while Sending email alerts")
        log.error(err)
        return False

    log.info("Email alerts configured on the cluster")
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
        disable_pg_autoscale=True,
        **kwargs,
    ):
        log.error(f"Failed to create Pool {pool_name}")
        return False
    log.info(f"Created the ec profile : {profile_name} and pool : {pool_name}")
    return True


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
