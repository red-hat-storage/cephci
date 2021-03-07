import json
import logging
import re

import requests

from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.common import fetch_method

log = logging.getLogger(__name__)


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
    if out != fsid:
        raise BootStrapValidationFailure("FSID verification failed")


def validate_skip_monitoring_stack(cls):
    """
    Method to validate monitoring service(s) has been skipped during bootstrap
        monitoring services - grafana, prometheus, alertmanager, node-exporter
    Args:
        cls: class object
    """
    out, _ = cls.shell(args=["ceph", "orch", "ls", "-f json"])
    monitoring = ["prometheus", "grafana", "alertmanager", "node-exporter"]
    for svc in json.loads(out):
        if svc["service_type"] in monitoring:
            raise BootStrapValidationFailure(
                "Monitoring service found : %s" % svc["service_type"]
            )


def validate_dashboard(cls, out, user=None, password=None):
    """
    Method to validate dashboard login using provided user/password
    and Also validates user/passwd in bootstrap console output

    Args:
        cls: class object
        out: bootstrap response log
        user: dashboard user
        password: dashbard password
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
        log.info("custom password: %s, configured password: %s" % (user, user_))
        if password != passwd_:
            raise BootStrapValidationFailure(f"password {password} did not match")

    host_ = re.search(r"https?:[/]{2}(.*):", host).group(1)
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


def verify_bootstrap(cls, args, response):
    """
    Verify bootstrap based on the parameter(s) provided.
    Args:
        cls: cephadm instance
        args: bootstrap args options
        response: console output of bootstrap
    """
    if args.get("fsid"):
        validate_fsid(cls, args.get("fsid"))
    if args.get("initial-dashboard-user"):
        validate_dashboard_user(cls, args.get("initial-dashboard-user"), response)
    if args.get("initial-dashboard-password"):
        validate_dashboard_passwd(cls, args.get("initial-dashboard-password"), response)
    if args.get("skip-monitoring-stack"):
        validate_skip_monitoring_stack(cls)


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

    method = fetch_method(instance, command)
    out, err = method(config)

    if "get_cluster_details" in config:
        instance.get_cluster_state(config["get_cluster_details"])

    # Verification of arguments
    # bootstrap response through stdout & stderr are combined here
    # currently console response coming through stderr.
    args = config.get("args", {})
    verify_bootstrap(instance, args, out + err)

    return 0
