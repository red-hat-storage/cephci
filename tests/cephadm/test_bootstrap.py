import json
import logging
import re

import requests

from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import get_cluster_state

log = logging.getLogger(__name__)

__DEFAULT_CEPH_DIR = "/etc/ceph"
__DEFAULT_CONF_PATH = "/etc/ceph/ceph.conf"
__DEFAULT_KEYRING_PATH = "/etc/ceph/ceph.client.admin.keyring"
__DEFAULT_SSH_PATH = "/etc/ceph/ceph.pub"


class BootStrapValidationFailure(Exception):
    pass


def validate_fsid(cls, fsid: str):
    """
    Method to validate fsid
    Args:
        cls: class object
        fsid: fsid

    """
    out, err = cls.shell(args=["ceph", "fsid"])
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

    session = requests.Session()
    session.headers = {
        "accept": "application/vnd.ceph.api.v1.0+json",
        "content-type": "application/json",
    }

    resp = session.post(url_, data=data, verify=False)
    if not resp.ok:
        raise BootStrapValidationFailure(
            f"Status code: {resp.status_code}\nResponse: {resp.text}"
        )
    log.info("Dashboard API login is successful, status code : %s" % resp.status_code)


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


def file_or_path_exists(node, file_or_path):
    """
    Method to check abs path exists
    Args:
        node: node object where file should be exists
        file_or_path: ceph file or directory path

    Returns:
        boolean
    """
    try:
        out, _ = node.exec_command(cmd=f"ls -l {file_or_path}", sudo=True)
        log.info("Output : %s" % out.read().decode())
    except CommandFailed as err:
        log.error("Error: %s" % err)
        return False
    return True


def copy_ceph_configuration_files(cls, ceph_conf_args):
    """
    Copy ceph configuration files to ceph default "/etc/ceph" path.

     we can eliminate this definition when we have support to access
     ceph cli via custom ceph config files.

    Args:
        ceph_conf_args: bootstrap arguments
        cls: cephadm instance

    ceph_conf_args:
          output-dir: "/root/ceph"
          output-keyring : "/root/ceph/ceph.client.admin.keyring"
          output-config : "/root/ceph/ceph.conf"
          output-pub-ssh-key : "/root/ceph/ceph.pub"
    """
    ceph_dir = ceph_conf_args.get("output-dir")
    if ceph_dir:
        cls.installer.exec_command(cmd=f"mkdir -p {__DEFAULT_CEPH_DIR}", sudo=True)

    def copy_file(node, src, destination):
        node.exec_command(cmd=f"cp {src} {destination}", sudo=True)

    ceph_files = {
        "output-keyring": __DEFAULT_KEYRING_PATH,
        "output-config": __DEFAULT_CONF_PATH,
        "output-pub-ssh-key": __DEFAULT_SSH_PATH,
    }

    for arg, default_path in ceph_files.items():
        if ceph_conf_args.get(arg):
            copy_file(cls.installer, ceph_conf_args.get(arg), default_path)


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
    out, _ = cls.shell(args=["ceph", "mgr", "module", "ls"])
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
        copy_ceph_configuration_files(instance, args)
        verify_bootstrap(instance, args, out + err)
    finally:
        # Get cluster state
        get_cluster_state(instance)
    return 0
