import json
import os
import re
from datetime import datetime, timedelta
from time import sleep

import requests

from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import (
    file_or_path_exists,
    get_cluster_state,
    get_hosts_deployed,
)
from utility.log import Log

log = Log(__name__)

__DEFAULT_CEPH_DIR = "/etc/ceph"
__DEFAULT_CONF_PATH = "/etc/ceph/ceph.conf"
__DEFAULT_KEYRING_PATH = "/etc/ceph/ceph.client.admin.keyring"
__DEFAULT_SSH_PATH = "/etc/ceph/ceph.pub"


class BootStrapValidationFailure(Exception):
    pass


def verify_dashboard_login(url, data):
    """Verify Ceph dashboard login using API call.

    Args:
        url: server url path
        data: credentials data in dict
    Returns:
        boolean
    """
    try:
        session = requests.Session()
        session.headers = {
            "accept": "application/vnd.ceph.api.v1.0+json",
            "content-type": "application/json",
        }

        resp = session.post(url, data=data, verify=False)
        if not resp.ok:
            raise BootStrapValidationFailure(
                f"Status code: {resp.status_code}\nResponse: {resp.text}"
            )
        log.info(
            "Dashboard API login is successful, status code : %s" % resp.status_code
        )
        return True
    except requests.exceptions.ConnectionError:
        log.info("connection timeout.. try again")
    return False


def validate_fsid(cls, fsid: str):
    """
    Method to validate fsid
    Args:
        cls: class object
        fsid: fsid

    """
    out, _ = cls.shell(args=["ceph", "fsid"])
    log.info("custom fsid provided for bootstrap: %s" % fsid)
    log.info("cluster fsid : %s" % out)
    if out.strip() != fsid:
        raise BootStrapValidationFailure("FSID verification failed")


def validate_skip_monitoring_stack(cls, flag):
    """
    Method to validate monitoring service(s) has been skipped during bootstrap
        monitoring services - grafana, prometheus, alertmanager, node-exporter

    if skip-monitoring-stack is,
    used - monitoring services should not get deployed
    not used - monitoring services should get deployed

    Args:
        cls: class object
        flag: skip-monitoring-stack enabled/disabled status
    """
    out, _ = cls.shell(args=["ceph", "orch", "ls", "-f json"])
    monitoring = ["prometheus", "grafana", "alertmanager", "node-exporter"]

    svcs = [
        svc["service_type"]
        for svc in json.loads(out)
        if svc["service_type"] in monitoring
    ]

    log.info("skip-monitoring-stack: %s\nMonitoring services: %s" % (flag, svcs))
    if flag:
        if svcs:
            raise BootStrapValidationFailure(f"Monitoring service found : {svcs}")
    else:
        if sorted(svcs) != sorted(monitoring):
            raise BootStrapValidationFailure(f"Monitoring service not found : {svcs}")

    log.info("skip-monitoring-stack validation is successful")


def validate_dashboard(cls, out, user=None, password=None, port=None):
    """
    Method to validate dashboard login using provided user/password
    and Also validates user/passwd in bootstrap console output

    Args:
        cls: class object
        out: bootstrap response log
        user: dashboard user
        password: dashboard password
        port: SSL dashboard port number
    """
    _, _, db_log_part = out.partition("Ceph Dashboard is now available at:")

    if not db_log_part:
        raise BootStrapValidationFailure("Dashboard log part not found")

    def parse(string):
        str_ = r"%s:\s(.+)" % string
        return re.search(str_, db_log_part).group(1)

    host = parse("URL")
    user_ = parse("User")
    passwd_ = parse("Password")

    if user:
        log.info("custom username: %s, configured username: %s" % (user, user_))
        if user != user_:
            raise BootStrapValidationFailure(f"user {user} did not match")
    if password:
        log.info("custom password: %s, configured password: %s" % (password, passwd_))
        if password != passwd_:
            raise BootStrapValidationFailure(f"password {password} did not match")

    groups = re.search(r"https?:[/]{2}(.*):(\d+)", host)
    host_ = groups.group(1)
    port_ = groups.group(2)

    if port:
        log.info("expected port : %s, configured port: %s" % (port, port_))
        if port_ != port:
            raise BootStrapValidationFailure("SSL dashboard port did not match")

    for node in cls.cluster.get_nodes():
        if host_ in node.hostname:
            host = host.replace(host_, node.ip_address)
            break
    else:
        raise BootStrapValidationFailure("Dashboard node not found")

    data = json.dumps({"password": passwd_, "username": user_})
    url_ = f"{host}/api/auth" if not host.endswith("/") else f"{host}api/auth"

    timeout = 600
    interval = 5
    end_time = datetime.now() + timedelta(seconds=timeout)

    while end_time > datetime.now():
        if verify_dashboard_login(url=url_, data=data):
            break
        sleep(interval)
    else:
        raise BootStrapValidationFailure("Ceph dashboard API Login call failed....")


def validate_dashboard_user(cls, user: str, out: str):
    """
    Method is used to validate fsid
    Args:
        cls: class object
        user: dashboard user
        out: bootstrap console response

    """
    validate_dashboard(cls, out, user=user)


def validate_dashboard_passwd(cls, password: str, out: str):
    """
    Method is used to validate fsid
    Args:
        cls: class object
        password: dashboard password
        out: bootstrap console response

    """
    validate_dashboard(cls, out, password=password)


def validate_ssl_dashboard_port(cls, out: str, port: str = None):
    """
    Method is used to validate port number
    Args:
        cls: class object
        port: dashboard port number
        out: bootstrap console response

    """
    port = str(port) if port else "8443"
    validate_dashboard(cls, out, port=port)


def validate_orphan_intial_daemons(cls, flag):
    """
    Method to validate orphan-initial-daemons

    [–orphan-initial-daemons]
     Do not create initial mon, mgr, and crash service specs

    if used
        MON, MGR service specs should not be created
        and with unmanaged=true
    else
        MON, MGR, Crash specs would be deployed with unmanaged=false

    Args:
        cls: cephadm instance
        flag: orphan-initial-daemons usage flag

    """
    out, _ = cls.shell(args=["ceph", "orch", "ls", "-f json"])
    svcs = ["mon", "mgr", "crash"]

    svcs_ = dict(
        (svc["service_type"], svc)
        for svc in json.loads(out)
        if svc["service_type"] in svcs
    )

    log.info("orphan-intial-daemons: %s\n Services: %s" % (flag, svcs_))
    if flag:
        # verify crash service should not get deployed
        if "crash" in svcs_:
            raise BootStrapValidationFailure(
                "crash service should not have deployed "
                "with orphan-initial-daemons option"
            )

        if sorted(svcs[0:-1]) != sorted(list(svcs_.keys())):
            raise BootStrapValidationFailure(f"MON/MGR service(s) not found: {svcs_}")

        # verify unmanaged flag which should be set to true
        for svc_name, config_ in svcs_.items():
            if not config_.get("unmanaged"):
                raise BootStrapValidationFailure(
                    f"{svc_name} spec is created with unmanaged=false"
                )
    else:
        if sorted(list(svcs_.keys())) != sorted(svcs):
            raise BootStrapValidationFailure(
                f"MGR/MON/Crash service(s) not found : {svcs_}"
            )

        # verify unmanaged flag which should be set to false
        for svc_name, config_ in svcs_.items():
            if config_.get("unmanaged"):
                raise BootStrapValidationFailure(
                    f"{svc_name} spec is created with unmanaged=true"
                )

    log.info("orphan-initial-daemons validation is successful")


def fetch_file_content(node, file):
    """
    Method to fetch file content
    Args:
        node: node object where file exists
        file: ceph file path

    Returns:
        string
    """
    try:
        out, _ = node.exec_command(cmd=f"cat {file}", sudo=True)
        log.info("Output : %s" % out)
        return out
    except CommandFailed as err:
        log.error("Error: %s" % err)
        return None


def verify_ceph_config_location(cls, ceph_conf_directory):
    """
    Verify ceph configuration output directory
        - check ceph config directory exists
    Args:
        cls: cephadm instance object
        ceph_conf_directory: ceph configuration directory
    """
    _dir = ceph_conf_directory if ceph_conf_directory else __DEFAULT_CEPH_DIR

    log.info(
        "Ceph configuration directory - default: %s , configured: %s"
        % (__DEFAULT_CEPH_DIR, ceph_conf_directory)
    )

    if not file_or_path_exists(cls.installer, _dir):
        raise BootStrapValidationFailure("ceph configuration directory not found")
    log.info("Ceph Configuration Directory found")


def verify_ceph_admin_keyring_file(cls, keyring_path):
    """
    Verify ceph configuration output directory
        - check ceph.keyring file exists
    Args:
        cls: cephadm instance object
        keyring_path: ceph configuration directory
    """
    keyring = keyring_path if keyring_path else __DEFAULT_KEYRING_PATH

    log.info(
        "Ceph keyring - default: %s , configured: %s"
        % (__DEFAULT_KEYRING_PATH, keyring_path)
    )

    if not file_or_path_exists(cls.installer, keyring):
        raise BootStrapValidationFailure("Ceph keyring not found")

    log.info("Ceph Keyring found")


def verify_ceph_configuration_file(cls, config_path):
    """
    Verify ceph configuration file
        - check ceph.conf file exists
    Args:
        cls: cephadm instance object
        config_path: ceph configuration directory
    """
    ceph_conf = config_path if config_path else __DEFAULT_CONF_PATH

    log.info(
        "Ceph configuration file - default: %s , configured: %s"
        % (__DEFAULT_CONF_PATH, config_path)
    )

    if not file_or_path_exists(cls.installer, ceph_conf):
        raise BootStrapValidationFailure("Ceph configuration file not found")

    log.info("Ceph configuration file found")


def verify_ceph_public_ssh_key(cls, ssh_key_path):
    """
    Verify ceph configuration file
        - check ssh ceph.pub file exists
    Args:
        cls: cephadm instance object
        ssh_key_path: ceph configuration directory
    """
    ssh_key = ssh_key_path if ssh_key_path else __DEFAULT_SSH_PATH

    log.info(
        "Ceph SSH public key file - default: %s , configured: %s"
        % (__DEFAULT_SSH_PATH, ssh_key_path)
    )

    if not file_or_path_exists(cls.installer, ssh_key):
        raise BootStrapValidationFailure("Ceph SSH public key file not found")

    log.info("Ceph SSH public key file found")


def validate_skip_dashboard(cls, flag, response):
    """
    Method to validate skip dashboard
    flag:
        False - mgr dashboard plugin should be enabled
        True  - mgr dashboard plugin be disabled
    Args:
        cls: cephadm instance
        flag: skip-dashboard bootstrap option
        response: bootstrap command response
    """
    out, _ = cls.shell(args=["ceph", "mgr", "module", "ls", "-f", "json"])
    dashboard = "dashboard" in json.loads(out)["enabled_modules"]
    log.info(
        "skip-dashboard Enabled : %s, Dashboard configured: %s" % (flag, dashboard)
    )

    # skip-dashboard = True , dashboard = False(should be), flag == dashboard >> False
    # skip-dashboard = False, dashboard = True(should be) , flag == dashboard >> False
    # If (flag == dashboard) >> True, then skip dashboard failed.
    if flag == dashboard:
        raise BootStrapValidationFailure("skip dashboard validation failed")

    if not flag:
        validate_dashboard(cls, response)
    log.info("skip-dashboard validation is successful")


def validate_ssh_public_key(cls, ssh_public_key):
    """
    Method to validate ssh public key
    ssh_public_key:
        if specified, then the specified public key should be configured for the cluster
    Args:
        cls: cephadm instance
        ssh_public_key: ssh-public-key bootstrap option
        response: bootstrap command response
    """
    out, _ = cls.shell(args=["ceph", "cephadm", "get-pub-key"])

    log.info(f"Ceph SSH public key file - configured: {ssh_public_key}")

    if not file_or_path_exists(cls.installer, ssh_public_key):
        raise BootStrapValidationFailure("Ceph SSH public key file not found")

    pub_key = fetch_file_content(cls.installer, ssh_public_key)

    log.info(
        f"ssh-public-key configured: {out}, ssh-public-key used in cluster: {pub_key}"
    )

    # if pub_key found and out == pub_key, configured and used keys are same. Valid
    # if pub_key found and out != pub_key, configured and used keys are different, not Valid
    if pub_key and out != pub_key:
        raise BootStrapValidationFailure(
            "ssh public key provided in the configuration is not used in the cluster"
        )

    log.info("ssh-public-key validation is successful")


def validate_ssh_private_key(cls, ssh_private_key):
    """
    Method to validate ssh public key
    ssh_public_key:
        if specified, then the specified public key should be configured for the cluster
    Args:
        cls: cephadm instance
        ssh_private_key: ssh-private-key bootstrap option
        response: bootstrap command response
    """
    out, _ = cls.shell(
        args=["ceph", "config-key", "get", "mgr/cephadm/ssh_identity_key"]
    )

    log.info(f"Ceph SSH private key file - configured: {ssh_private_key}")

    if not file_or_path_exists(cls.installer, ssh_private_key):
        raise BootStrapValidationFailure("Ceph SSH private key file not found")

    pri_key = fetch_file_content(cls.installer, ssh_private_key)

    log.info(
        f"ssh-private-key configured: {out}, ssh-private-key used in cluster: {pri_key}"
    )

    # if pri_key found and out == pri_key, configured and used keys are same. Valid
    # if pri_key found and out != pri_key, configured and used keys are different, not Valid
    if pri_key and out != pri_key:
        raise BootStrapValidationFailure(
            "ssh private key provided in the configuration is not used in the cluster"
        )

    log.info("ssh-private-key validation is successful")


def validate_log_file_generation(cls):
    """
    Method to verify generation of log files in default log directory
    Args:
        cls: cephadm instance object

    Returns:
        string
    """
    out, _ = cls.shell(args=["ceph", "fsid"])
    fsid = out.strip()
    log_file_path = "/var/log/ceph"
    hosts = get_hosts_deployed(cls)
    retry_count = 3
    for node in cls.cluster.get_nodes():
        try:
            if node.hostname not in hosts:
                continue
            file = os.path.join(log_file_path, "cephadm.log")
            fileExists = file_or_path_exists(node, file)
            if not fileExists:
                raise BootStrapValidationFailure(
                    f"cephadm.log is not present in the node {node.ip_address}"
                )
            count = 0
            fileExists = False
            while count < retry_count:
                count += 1
                sleep(60)
                file = os.path.join(log_file_path, fsid, "ceph-volume.log")
                fileExists = file_or_path_exists(node, file)
                if fileExists:
                    break
            if not fileExists:
                raise BootStrapValidationFailure(
                    f"ceph-volume.log is not present in the node {node.ip_address}"
                )
            log.info(f"Log verification on node {node.ip_address} successful")
        except CommandFailed as err:
            log.error("Error: %s" % err)
            raise BootStrapValidationFailure(
                f"ceph-volume.log is not present in the node {node.ip_address}"
            )
    log.info("Log file validation successful")


def verify_bootstrap(cls, args, response):
    """
    Verify bootstrap based on the parameter(s) provided.
    Args:
        cls: cephadm instance
        args: bootstrap args options
        response: console output of bootstrap
    """
    verify_ceph_config_location(cls, args.get("output-dir"))
    verify_ceph_admin_keyring_file(cls, args.get("output-keyring"))
    verify_ceph_configuration_file(cls, args.get("output-config"))
    verify_ceph_public_ssh_key(cls, args.get("output-pub-ssh-key"))

    # spec service could be deployed using apply-spec
    if not args.get("apply-spec"):
        validate_skip_monitoring_stack(cls, args.get("skip-monitoring-stack"))
        validate_orphan_intial_daemons(cls, args.get("orphan-initial-daemons"))
    if args.get("fsid"):
        validate_fsid(cls, args.get("fsid"))

    # Dashboard validations
    validate_skip_dashboard(cls, args.get("skip-dashboard", False), response)
    if not args.get("skip-dashboard"):
        validate_ssl_dashboard_port(cls, response, args.get("ssl-dashboard-port"))
        validate_dashboard_user(cls, args.get("initial-dashboard-user"), response)
        validate_dashboard_passwd(cls, args.get("initial-dashboard-password"), response)

    # Public Key validations
    if args.get("ssh-public-key"):
        validate_ssh_public_key(cls, args.get("ssh-public-key"))

    # Private Key validations
    if args.get("ssh-private-key"):
        validate_ssh_private_key(cls, args.get("ssh-private-key"))

    # Log file validations
    validate_log_file_generation(cls)


def run(ceph_cluster, **kw):
    """
    Cephadm Bootstrap

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    - Bootstrap cluster with default or custom image and
      returns after cephadm.bootstrap. To use default image, set 'registry'.

        Example:
            config:
                command: bootstrap
                base_cmd_args:
                    verbose: true
                args:
                    custom_image: true | false
                    mon-ip: <node_name>
                    mgr-id: <mgr_id>
                    fsid: <id>
    """
    config = kw.get("config")
    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build
    config["overrides"] = kw.get("test_data", {}).get("custom-config")

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    service = config.pop("service", "")
    log.info("Executing %s %s" % (service, command))

    instance = CephAdmin(cluster=ceph_cluster, **config)
    if "shell" in command:
        instance.shell(args=config["args"])
        return 0
    try:
        method = fetch_method(instance, command)
        out, err = method(config)

        # Verification of arguments
        # bootstrap response through stdout & stderr are combined here
        # currently console response coming through stderr.
        args = config.get("args", {})
        verify_bootstrap(instance, args, out + err)
    finally:
        log.debug("Gathering cluster state after running test_bootstrap")
        get_cluster_state(instance)
    return 0
