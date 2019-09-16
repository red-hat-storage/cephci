import datetime
import itertools
import logging
import time
import traceback

from ceph.parallel import parallel
from ceph.utils import config_ntp
from ceph.utils import update_ca_cert
from utility.retry import retry

log = logging.getLogger(__name__)

rpm_packages = {'py2': ['wget', 'git', 'python-virtualenv', 'redhat-lsb', 'python-nose', 'ntp'],
                'py3': ['wget', 'git', 'python3-virtualenv', 'redhat-lsb', 'python3-nose', 'python3-pip']}
deb_packages = ['wget', 'git', 'python-virtualenv', 'lsb-release', 'ntp']
epel_rpm = 'https://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-9.noarch.rpm'
epel_packages = ['python-pip']
deb_all_packages = " ".join(deb_packages)


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    # skip subscription manager if testing beta RHEL
    config = kw.get('config')
    skip_subscription = config.get('skip_subscription', False)
    repo = config.get('add-repo', False)
    rhbuild = config.get('rhbuild')

    with parallel() as p:
        for ceph in ceph_nodes:
            p.spawn(install_prereq, ceph, 1800, skip_subscription, repo, rhbuild)
            time.sleep(20)
    return 0


def install_prereq(ceph, timeout=1800, skip_subscription=False, repo=False, rhbuild=None):
    log.info("Waiting for cloud config to complete on " + ceph.hostname)
    ceph.exec_command(cmd='while [ ! -f /ceph-qa-ready ]; do sleep 15; done')
    log.info("cloud config to completed on " + ceph.hostname)
    update_ca_cert(ceph, 'https://password.corp.redhat.com/RH-IT-Root-CA.crt')
    update_ca_cert(ceph, 'https://password.corp.redhat.com/legacy.crt')
    distro_info = ceph.distro_info
    log.info('distro name: {name}'.format(name=distro_info['NAME']))
    log.info('distro id: {id}'.format(id=distro_info['ID']))
    log.info('distro version_id: {version_id}'.format(version_id=distro_info['VERSION_ID']))
    if ceph.pkg_type == 'deb':
        ceph.exec_command(cmd='sudo apt-get install -y ' + deb_all_packages, long_running=True)
    else:
        if not skip_subscription:
            setup_subscription_manager(ceph)
            enable_rhel_rpms(ceph, distro_info)
        if repo:
            setup_addition_repo(ceph, repo)
        if rhbuild.startswith('4'):
            rpm_all_packages = ' '.join(rpm_packages.get('py3'))
        else:
            rpm_all_packages = ' '.join(rpm_packages.get('py2'))
        ceph.exec_command(cmd='sudo yum install -y ' + rpm_all_packages, long_running=True)
        if ceph.role == 'client':
            ceph.exec_command(cmd='sudo yum install -y attr', long_running=True)
            ceph.exec_command(cmd='sudo pip install crefi', long_running=True)
        # install epel package
        ceph.exec_command(cmd='sudo yum clean metadata')
        # finally install python2-pip directly using rpm since its available only in epel
        install_pip(ceph)
        config_ntp(ceph)


@retry(Exception, tries=5, delay=10)
def install_pip(ceph):
    log.info("Installing pip on {host}".format(host=ceph.hostname))
    base_dir_path = "http://dl.fedoraproject.org/pub/fedora-secondary/releases/28/Everything/i386/os/Packages/p"
    pip_package_name = "python2-pip-9.0.3-1.fc28.noarch.rpm"
    ceph.exec_command(
        cmd='sudo yum install -y {base}/{package}'.format(base=base_dir_path, package=pip_package_name))


def setup_addition_repo(ceph, repo):
    log.info("Adding addition repo {repo} to {sn}".format(
             repo=repo, sn=ceph.shortname))
    ceph.exec_command(sudo=True,
                      cmd='curl -o /etc/yum.repos.d/rh_add_repo.repo {repo}'.format(repo=repo))
    ceph.exec_command(sudo=True, cmd='yum update metadata', check_ec=False)


def setup_subscription_manager(ceph, timeout=1800):
    timeout = datetime.timedelta(seconds=timeout)
    starttime = datetime.datetime.now()
    log.info(
        "Subscribing {ip} host with {timeout} timeout".format(ip=ceph.ip_address, timeout=timeout))
    while True:
        try:
            ceph.exec_command(
                cmd='sudo subscription-manager --force register  '
                    '--serverurl=subscription.rhsm.stage.redhat.com:443/subscription  '
                    '--baseurl=https://cdn.redhat.com --username=cephuser --password=cephuser '
                    '--auto-attach',
                timeout=720)

            break
        except BaseException:
            if datetime.datetime.now() - starttime > timeout:
                try:
                    out, err = ceph.exec_command(
                        cmd='cat /var/log/rhsm/rhsm.log', timeout=120)
                    rhsm_log = out.read().decode()
                except BaseException:
                    rhsm_log = 'No Log Available'
                raise RuntimeError(
                    "Failed to subscribe {ip} with {timeout} timeout:\n {stack_trace}\n\n rhsm.log:\n{log}".format(
                        ip=ceph.ip_address,
                        timeout=timeout, stack_trace=traceback.format_exc(), log=rhsm_log))
            else:
                wait = iter(x for x in itertools.count(1, 10))
                time.sleep(next(wait))
    ceph.exec_command(cmd='sudo subscription-manager repos --disable=*', long_running=True)


def enable_rhel_rpms(ceph, distro_info):
    """
        Setup cdn repositories for rhel systems
        Args:
            distro_info(dict): distro details
    """
    repos_7x = ['rhel-7-server-rpms',
                'rhel-7-server-optional-rpms',
                'rhel-7-server-extras-rpms']

    repos_8x = ['rhel-8-for-x86_64-appstream-rpms',
                'rhel-8-for-x86_64-baseos-rpms']

    repos = None
    distro_ver = distro_info['VERSION_ID']
    if not distro_ver.startswith('8'):
        repos = repos_7x
    else:
        repos = repos_8x
    for repo in repos:
        ceph.exec_command(
            sudo=True, cmd='subscription-manager repos --enable={r}'.format(r=repo), long_running=True)
