import base64
import json
import re
import time

from ceph.parallel import parallel
from ceph.utils import config_ntp, update_ca_cert
from ceph.waiter import WaitUntil
from cli.utilities.packages import Package
from cli.utilities.packages import SubscriptionManager as sm
from cli.utilities.packages import SubscriptionManagerError
from cli.utilities.utils import (
    enable_fips_mode,
    is_fips_mode_enabled,
    os_major_version,
    reboot_node,
)
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


class ConfigNotFoundError(Exception):
    pass


class RepoConfigError(Exception):
    pass


class FIPSConfigError(Exception):
    pass


rpm_packages = {
    "7": [
        "wget",
        "git-core",
        "python-virtualenv",
        "python-nose",
        "ntp",
        "python2-pip",
        "chrony",
    ],
    "all": [
        "wget",
        "git-core",
        "python3-devel",
        "chrony",
        "yum-utils",
        "net-tools",
        "lvm2",
        "podman",
        "net-snmp-utils",
        "net-snmp",
        "kernel-modules-extra",
        "iproute-tc",
    ],
}
deb_packages = ["wget", "git-core", "python-virtualenv", "lsb-release", "ntp"]
deb_all_packages = " ".join(deb_packages)


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get("ceph_nodes")

    # skip subscription manager if testing beta RHEL
    config = kw.get("config")
    skip_subscription = config.get("skip_subscription", False)
    enable_eus = config.get("enable_eus", False)
    repo = config.get("add-repo", False)
    skip_enabling_rhel_rpms = config.get("skip_enabling_rhel_rpms", False)
    fips_mode = config.get("enable_fips_mode", False)

    cloud_type = config.get("cloud-type", "openstack")
    with parallel() as p:
        for ceph in ceph_nodes:
            p.spawn(
                install_prereq,
                ceph,
                skip_subscription,
                repo,
                enable_eus,
                skip_enabling_rhel_rpms,
                cloud_type,
                fips_mode,
            )
            time.sleep(20)

    return 0


def install_prereq(
    ceph,
    skip_subscription=False,
    repo=False,
    enable_eus=False,
    skip_enabling_rhel_rpms=False,
    cloud_type="openstack",
    fips_mode=False,
):
    log.info("Waiting for cloud config to complete on " + ceph.hostname)
    ceph.exec_command(cmd="while [ ! -f /ceph-qa-ready ]; do sleep 15; done")
    log.info("cloud config to completed on " + ceph.hostname)
    _is_client = len(ceph.role.role_list) == 1 and "client" in ceph.role.role_list

    if cloud_type.lower() == "baremetal":
        # Update certs
        update_ca_cert(
            node=ceph,
            cert_url="https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem",
            out_file="RH-IT-Root-CA.crt",
            check_ec=False,
        )

        # Update CephCI Cert to all nodes. Useful when creating self-signed certificates.
        update_ca_cert(
            node=ceph,
            cert_url="http://magna002.ceph.redhat.com/cephci-jenkins/.cephqe-ca.pem",
            out_file="cephqe-ca.pem",
            check_ec=False,
        )
    distro_info = ceph.distro_info
    distro_ver = distro_info["VERSION_ID"]
    log.info("distro name: {name}".format(name=distro_info["NAME"]))
    log.info("distro id: {id}".format(id=distro_info["ID"]))
    log.info(
        "distro version_id: {version_id}".format(version_id=distro_info["VERSION_ID"])
    )

    # Remove apache-arrow.repo for baremetal
    cmd_remove_apache_arrow = "sudo rm -f /etc/yum.repos.d/apache-arrow.repo"
    ceph.exec_command(cmd=cmd_remove_apache_arrow)

    # Max SSH Sessions
    sshd_configs = [
        "sed -i '/MaxSessions*/d' /etc/ssh/sshd_config",
        "echo 'MaxSessions 150' | tee -a /etc/ssh/sshd_config",
        "systemctl restart sshd",
    ]
    for sshd_cfg in sshd_configs:
        ceph.exec_command(cmd=sshd_cfg, sudo=True)

    if ceph.pkg_type == "deb":
        ceph.exec_command(
            cmd="sudo apt-get install -y " + deb_all_packages, long_running=True
        )
    else:
        if distro_ver.startswith("7"):
            ceph.exec_command(cmd="sudo systemctl restart NetworkManager.service")

        if not skip_subscription:
            if not setup_subscription_manager(ceph, "cdn"):
                log.info("Trying to subscribe to stage server")
                setup_subscription_manager(ceph, "stage")

            status = subscription_manager_status(ceph)
            if status == "Unknown" or skip_enabling_rhel_rpms:
                log.info("Enabling local RHEL repositories")
                if not setup_local_repos(ceph):
                    raise RepoConfigError("Failed to enable local RHEL repositories")

            elif enable_eus:
                enable_rhel_eus_rpms(ceph, distro_ver)

            else:
                enable_rhel_rpms(ceph, distro_ver)

        if repo:
            setup_addition_repo(ceph, repo)

        ceph.exec_command(cmd="sudo yum -y upgrade", timeout=600, check_ec=False)

        rpm_all_packages = " ".join(rpm_packages.get("all"))
        if distro_ver.startswith("7"):
            rpm_all_packages = " ".join(rpm_packages.get("7"))

        ceph.exec_command(
            cmd=f"sudo yum install -y {rpm_all_packages}", long_running=True
        )

        # Restarting the node for qdisc filter to be loaded. This is required for
        # RHEL-8
        if not distro_ver.startswith("7"):
            # Avoiding early channel close and ignoring channel exception thrown during
            # reboot
            time.sleep(10)
            ceph.exec_command(sudo=True, cmd="reboot", check_ec=False)

            # Sleep before and after reconnect
            time.sleep(60)
            ceph.reconnect()
            time.sleep(10)

        if skip_enabling_rhel_rpms and skip_subscription:
            # Ansible is required for RHCS 4.x
            if distro_ver.startswith("8") and not _is_client:
                # TODO(vamahaja): Temporary changes. Revert ansible package with latest epel repo.
                ansible_pkg = (
                    "http://download-node-02.eng.bos.redhat.com/nightly/rhel-8/ANSIBLE/latest-ANSIBLE-2-RHEL-8/"
                    "compose/Base/x86_64/os/Packages/ansible-2.9.27-1.el8ae.noarch.rpm"
                )
                ceph.exec_command(
                    sudo=True,
                    cmd=f"yum install -y {ansible_pkg}",
                    check_ec=False,
                )

            if distro_ver.startswith("7") and not _is_client:
                ceph.exec_command(
                    sudo=True,
                    cmd="yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm",
                    check_ec=False,
                )
                ceph.exec_command(sudo=True, cmd="yum install -y ansible-2.9.27-1.el7")

        if _is_client:
            ceph.exec_command(cmd="sudo yum install -y attr gcc", long_running=True)
            ceph.exec_command(cmd="sudo pip install crefi", long_running=True)

        ceph.exec_command(cmd="sudo yum clean all")
        config_ntp(ceph, cloud_type)

    registry_login(ceph, distro_ver)
    update_iptables(ceph)

    if not fips_mode:
        return

    # Enable FIPS mode
    if not enable_fips_mode(ceph):
        raise FIPSConfigError("Failed to enable FIPS mode")
    log.info("Enable FIPS mode config set successfully")

    # Restart node and wait
    reboot_node(ceph)

    # Check for FIPS mode setting
    if not is_fips_mode_enabled(ceph):
        raise FIPSConfigError("FIPS mode not enabled after reboot")
    log.info("FIPS mode is enabled")


def setup_addition_repo(ceph, repo):
    log.info("Adding addition repo {repo} to {sn}".format(repo=repo, sn=ceph.shortname))
    ceph.exec_command(
        sudo=True,
        cmd="curl -o /etc/yum.repos.d/rh_add_repo.repo {repo}".format(repo=repo),
    )
    ceph.exec_command(sudo=True, cmd="yum update metadata", check_ec=False)


def setup_subscription_manager(ceph, server, timeout=300, interval=60):
    # Get configuration details from `~/.cephci.yaml`
    configs = get_cephci_config()

    # Get credentials and validate
    creds = configs.get(f"{server}_credentials")
    if not creds:
        raise ConfigNotFoundError(f"{server} credentials are not provided")

    # Subscribe to server
    for w in WaitUntil(timeout=timeout, interval=interval):
        try:
            sm(ceph).register(
                username=creds.get("username"),
                password=creds.get("password"),
                serverurl=creds.get("serverurl"),
                baseurl=creds.get("baseurl"),
                force=True,
            )
            log.info(f"Subscribed to {server} server successfully")
            return True
        except SubscriptionManagerError:
            log.info(f"Failed to subscribe to {server} server. Retrying")

    if w.expired:
        log.info(f"Failed to subscribe to {server} server.")

    return False


def subscription_manager_status(ceph):
    expr = ".*Overall Status:(.*).*"
    status = sm(ceph).status()

    match = re.search(expr, status)
    if not match:
        raise SubscriptionManagerError("Unexpected subscription manager status")

    return match.group(0)


def setup_local_repos(ceph):
    # Get configuration details from `~/.cephci.yaml`
    configs = get_cephci_config()

    # Get distro version
    os_version = os_major_version(ceph)

    # Get local repositories
    repos = configs.get("repo")
    if not repos:
        raise ConfigNotFoundError("Repos are not provided")

    # Get local repositories
    local_repos = repos.get("local", {}).get(f"rhel-{os_version}")
    if not local_repos:
        raise ConfigNotFoundError("local repositories are not provided")

    # Add local repositories
    for repo in local_repos:
        Package(ceph).add_repo(repo=repo)

    log.info("Added local RHEL repos successfully")
    return True


def enable_rhel_rpms(ceph, distro_ver):
    """
    Setup cdn repositories for rhel systems
    Args:
        ceph:       cluster instance
        distro_ver: distro version details
    """

    repos = {
        "7": ["rhel-7-server-rpms", "rhel-7-server-extras-rpms"],
        "8": ["rhel-8-for-x86_64-appstream-rpms", "rhel-8-for-x86_64-baseos-rpms"],
        "9": ["rhel-9-for-x86_64-appstream-rpms", "rhel-9-for-x86_64-baseos-rpms"],
    }

    ceph.exec_command(sudo=True, cmd=f"subscription-manager release --set {distro_ver}")

    for repo in repos.get(distro_ver[0]):
        ceph.exec_command(
            sudo=True,
            cmd="subscription-manager repos --enable={r}".format(r=repo),
            long_running=True,
        )


def enable_rhel_eus_rpms(ceph, distro_ver):
    """
    Setup cdn repositories for rhel systems
    reference: http://wiki.test.redhat.com/CEPH/SubscriptionManager
    Args:
        distro_ver:     distro version - example: 7.7
        ceph:           ceph object
    """

    eus_repos = {"7": ["rhel-7-server-eus-rpms", "rhel-7-server-extras-rpms"]}

    for repo in eus_repos.get(distro_ver[0]):
        ceph.exec_command(
            sudo=True,
            cmd="subscription-manager repos --enable={r}".format(r=repo),
            long_running=True,
        )

    rhel_major_version = distro_ver[0]

    if rhel_major_version == "7":
        # We only support one EUS release for RHEL 7:
        release = "7.7"
    else:
        raise NotImplementedError("cannot set EUS repos for %s", rhel_major_version)

    cmd = f"subscription-manager release --set={release}"

    ceph.exec_command(
        sudo=True,
        cmd=cmd,
        long_running=True,
    )

    ceph.exec_command(sudo=True, cmd="yum clean all", long_running=True)


def registry_login(ceph, distro_ver):
    """
    Login to the given Container registries provided in the configuration.

    In this method, docker or podman is installed based on OS.
    """
    container = "podman"
    if distro_ver.startswith("7"):
        container = "docker"

    ceph.exec_command(
        cmd="sudo yum install -y {c}".format(c=container), long_running=True
    )

    if container == "docker":
        ceph.exec_command(cmd="sudo systemctl restart docker", long_running=True)

    config = get_cephci_config()
    registries = [
        {
            "registry": "registry.redhat.io",
            "user": config["cdn_credentials"]["username"],
            "passwd": config["cdn_credentials"]["password"],
        }
    ]

    if (
        config.get("registry_credentials")
        and config["registry_credentials"]["registry"] != "registry.redhat.io"
    ):
        registries.append(
            {
                "registry": config["registry_credentials"]["registry"],
                "user": config["registry_credentials"]["username"],
                "passwd": config["registry_credentials"]["password"],
            }
        )
    auths = {}
    for r in registries:
        b64_auth = base64.b64encode(f"{r['user']}:{r['passwd']}".encode("ascii"))
        auths[r["registry"]] = {"auth": b64_auth.decode("utf-8")}
    auths_dict = {"auths": auths}
    ceph.exec_command(sudo=True, cmd="mkdir -p ~/.docker")
    ceph.exec_command(cmd="mkdir -p ~/.docker")
    auths_file_sudo = ceph.remote_file(
        sudo=True, file_name="/root/.docker/config.json", file_mode="w"
    )
    auths_file = ceph.remote_file(
        file_name="/home/cephuser/.docker/config.json", file_mode="w"
    )
    files = [auths_file_sudo, auths_file]
    for file in files:
        file.write(json.dumps(auths_dict, indent=4))
        file.flush()
        file.close()


def update_iptables(node):
    """update ip-tables rules.

    Drop ip-table rule which matches the list of reject entries,
     which ensures no side-effects at Ceph configuration.

     Reference:
     https://docs.ceph.com/en/latest/rados/configuration/network-config-ref/#ip-tables

    Args:
        node: CephNode object
    """
    drop_rules = ["INPUT -j REJECT --reject-with icmp-host-prohibited"]
    try:
        out, _ = node.exec_command(cmd="$(which iptables) --list-rules", sudo=True)
        for rule in drop_rules:
            if rule in out:
                node.exec_command(cmd=f"$(which iptables) -D {rule}", sudo=True)
    except Exception as err:
        log.error(f"iptables rpm do not exist... error : {err}")
