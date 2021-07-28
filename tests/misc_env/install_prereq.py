import datetime
import itertools
import logging
import time
import traceback

from ceph.parallel import parallel
from ceph.utils import config_ntp, update_ca_cert
from utility.utils import get_cephci_config

log = logging.getLogger(__name__)

rpm_packages = {
    "py2": [
        "wget",
        "git",
        "python-virtualenv",
        "python-nose",
        "ntp",
        "python2-pip",
    ],
    "py3": [
        "wget",
        "git",
        "python3-virtualenv",
        "python3-nose",
        "python3-pip",
    ],
}
deb_packages = ["wget", "git", "python-virtualenv", "lsb-release", "ntp"]
deb_all_packages = " ".join(deb_packages)


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get("ceph_nodes")
    # skip subscription manager if testing beta RHEL
    config = kw.get("config")
    skip_subscription = config.get("skip_subscription", False)
    enable_eus = config.get("enable_eus", False)
    repo = config.get("add-repo", False)
    rhbuild = config.get("rhbuild")
    skip_enabling_rhel_rpms = config.get("skip_enabling_rhel_rpms", False)
    is_production = config.get("is_production", False)
    with parallel() as p:
        for ceph in ceph_nodes:
            p.spawn(
                install_prereq,
                ceph,
                1800,
                skip_subscription,
                repo,
                rhbuild,
                enable_eus,
                skip_enabling_rhel_rpms,
                is_production,
            )
            time.sleep(20)
    return 0


def install_prereq(
    ceph,
    timeout=1800,
    skip_subscription=False,
    repo=False,
    rhbuild=None,
    enable_eus=False,
    skip_enabling_rhel_rpms=False,
    is_production=False,
):
    log.info("Waiting for cloud config to complete on " + ceph.hostname)
    ceph.exec_command(cmd="while [ ! -f /ceph-qa-ready ]; do sleep 15; done")
    log.info("cloud config to completed on " + ceph.hostname)

    # Update certs
    update_ca_cert(
        node=ceph,
        cert_url="https://password.corp.redhat.com/RH-IT-Root-CA.crt",
        out_file="RH-IT-Root-CA.crt",
    )
    distro_info = ceph.distro_info
    distro_ver = distro_info["VERSION_ID"]
    log.info("distro name: {name}".format(name=distro_info["NAME"]))
    log.info("distro id: {id}".format(id=distro_info["ID"]))
    log.info(
        "distro version_id: {version_id}".format(version_id=distro_info["VERSION_ID"])
    )
    if ceph.pkg_type == "deb":
        ceph.exec_command(
            cmd="sudo apt-get install -y " + deb_all_packages, long_running=True
        )
    else:
        # workaround: rhel 7.7's cloud-init has a bug:
        # https://bugzilla.redhat.com/show_bug.cgi?id=1748015
        ceph.exec_command(cmd="sudo systemctl restart NetworkManager.service")
        if not skip_subscription:
            setup_subscription_manager(ceph, is_production)
            if not skip_enabling_rhel_rpms:
                if enable_eus:
                    enable_rhel_eus_rpms(ceph, distro_ver)
                else:
                    enable_rhel_rpms(ceph, distro_ver)
            else:
                log.info("Skipped enabling the RHEL RPM's provided by Subscription")
        if repo:
            setup_addition_repo(ceph, repo)
        # TODO enable only python3 rpms on both rhel7 &rhel8 once all component suites(rhcs3,4) are comptatible
        if distro_ver.startswith("8"):
            rpm_all_packages = rpm_packages.get("py3") + ["net-tools"]
            if str(rhbuild).startswith("5"):
                rpm_all_packages = rpm_packages.get("py3") + ["lvm2", "podman"]
            rpm_all_packages = " ".join(rpm_all_packages)
        else:
            rpm_all_packages = " ".join(rpm_packages.get("py2"))
        ceph.exec_command(
            cmd="sudo yum install -y " + rpm_all_packages, long_running=True
        )
        if ceph.role == "client":
            ceph.exec_command(cmd="sudo yum install -y attr", long_running=True)
            ceph.exec_command(cmd="sudo pip install crefi", long_running=True)
        ceph.exec_command(cmd="sudo yum clean metadata")
        config_ntp(ceph)
    registry_login(ceph, distro_ver)


def setup_addition_repo(ceph, repo):
    log.info("Adding addition repo {repo} to {sn}".format(repo=repo, sn=ceph.shortname))
    ceph.exec_command(
        sudo=True,
        cmd="curl -o /etc/yum.repos.d/rh_add_repo.repo {repo}".format(repo=repo),
    )
    ceph.exec_command(sudo=True, cmd="yum update metadata", check_ec=False)


def setup_subscription_manager(ceph, is_production=False, timeout=1800):
    timeout = datetime.timedelta(seconds=timeout)
    starttime = datetime.datetime.now()
    log.info(
        "Subscribing {ip} host with {timeout} timeout".format(
            ip=ceph.ip_address, timeout=timeout
        )
    )
    while True:
        try:
            # subscription-manager tips:
            #
            # "--serverurl" (optional) is the entitlement service. The default
            # server (production) has customer-facing entitlement and SKU
            # information. The "stage" server has QE-only entitlement data.
            # We use Red Hat's internal "Ethel" tool to add SKUs to the
            # "rhcsuser" account that only exists in stage.
            #
            # "--baseurl" (optional) is the RPM content host. The default
            # value is the production CDN (cdn.redhat.com), and this hosts the
            # RPM contents to which all customers have access. Alternatively
            # you can push content to the staging CDN through the Errata Tool,
            # and then test it with --baseurl=cdn.stage.redhat.com.
            config_ = get_cephci_config()
            command = "sudo subscription-manager --force register "
            if is_production:
                command += "--serverurl=subscription.rhsm.redhat.com:443/subscription "
                username_ = config_["cdn_credentials"]["username"]
                password_ = config_["cdn_credentials"]["password"]
                pool_id = "8a85f98960dbf6510160df23eb447470"

            else:
                command += (
                    "--serverurl=subscription.rhsm.stage.redhat.com:443/subscription "
                )
                username_ = config_["stage_credentials"]["username"]
                password_ = config_["stage_credentials"]["password"]
                pool_id = "8a99f9ad7ac6855d017aebb094cc11b7"

            command += f"--baseurl=https://cdn.redhat.com --username={username_} --password={password_}"

            ceph.exec_command(
                cmd=command,
                timeout=720,
            )

            ceph.exec_command(
                cmd=f"sudo subscription-manager attach --pool {pool_id}",
                timeout=720,
            )
            break
        except (KeyError, AttributeError):
            raise RuntimeError(
                "Require the {} to be set in ~/.cephci.yaml, Please refer cephci.yaml.template".format(
                    "cdn_credentials" if is_production else "stage_credentails"
                )
            )

        except BaseException:
            if datetime.datetime.now() - starttime > timeout:
                try:
                    out, err = ceph.exec_command(
                        cmd="cat /var/log/rhsm/rhsm.log", timeout=120
                    )
                    rhsm_log = out.read().decode()
                except BaseException:
                    rhsm_log = "No Log Available"
                raise RuntimeError(
                    "Failed to subscribe {ip} with {timeout} timeout:\n {stack_trace}\n\n rhsm.log:\n{log}".format(
                        ip=ceph.ip_address,
                        timeout=timeout,
                        stack_trace=traceback.format_exc(),
                        log=rhsm_log,
                    )
                )
            else:
                wait = iter(x for x in itertools.count(1, 10))
                time.sleep(next(wait))
    ceph.exec_command(
        cmd="sudo subscription-manager repos --disable=*", long_running=True
    )


def enable_rhel_rpms(ceph, distro_ver):
    """
    Setup cdn repositories for rhel systems
    Args:
        distro_ver: distro version details
    """

    repos = {
        "7": ["rhel-7-server-rpms", "rhel-7-server-extras-rpms"],
        "8": ["rhel-8-for-x86_64-appstream-rpms", "rhel-8-for-x86_64-baseos-rpms"],
    }

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
        distro_ver: distro version - example: 7.7
        ceph: ceph object
    """

    eus_repos = {"7": ["rhel-7-server-eus-rpms", "rhel-7-server-extras-rpms"]}

    ceph.exec_command(
        sudo=True,
        cmd="subscription-manager attach --pool 8a99f9ad77a7d7290177ce3852fc0c44",
        timeout=720,
    )

    ceph.exec_command(
        sudo=True, cmd="subscription-manager repos --disable=*", long_running=True
    )

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
    login to this registry 'registry.redhat.io' on all nodes
        docker for RHEL 7.x and podman for RHEL 8.x
    """
    cdn_cred = get_cephci_config().get("cdn_credentials")
    if not cdn_cred:
        log.warning(
            "no cdn_credentials in ~/.cephci.yaml."
            " Not logging into registry.redhat.io."
        )
        return
    user = cdn_cred.get("username")
    pwd = cdn_cred.get("password")
    if not (user and pwd):
        log.warning("username and password not found for cdn_credentials")
        return

    container = "docker"
    if distro_ver.startswith("8"):
        container = "podman"

    ceph.exec_command(
        cmd="sudo yum install -y {c}".format(c=container), long_running=True
    )

    if container == "docker":
        ceph.exec_command(cmd="sudo systemctl restart docker", long_running=True)

    ceph.exec_command(
        cmd="sudo {c} login -u {u} -p {p} registry.redhat.io".format(
            c=container, u=user, p=pwd
        ),
        check_ec=True,
    )
