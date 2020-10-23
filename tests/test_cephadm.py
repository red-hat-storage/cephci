import logging
from utility.utils import get_latest_container

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Runs cephadm deployment
    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
    """
    log.info("Running test")
    log.info("Running cephadm test")
    config = kw.get('config')
    hotfix_repo = config.get('hotfix_repo')
    test_data = kw.get('test_data')
    base_url = config.get('base_url', None)
    ceph_cluster.custom_config = test_data.get('custom-config')
    ceph_cluster.custom_config_file = test_data.get('custom-config-file')

    ceph_cluster.use_cdn = config.get('use_cdn')
    build = config.get('build', config.get('rhbuild'))
    ceph_cluster.rhcs_version = build

    if config.get('skip_setup') is True:
        log.info("Skipping setup of ceph cluster")
        return 0

    ceph_installer = ceph_cluster.get_ceph_object('installer')
    # enable internal repo, cdn rpm and hotfix repo yet to be implemented
    ceph_installer.exec_command(
        cmd='sudo yum-config-manager --add {}compose/Tools/x86_64/os/'.format(base_url))
    ceph_cluster.setup_ssh_keys()

    installerNode = ceph_cluster.get_nodes(role="installer")
    ceph_installer.install_cephadm()
    image = get_latest_container(build)
    ceph_installer.exec_command(sudo=True, cmd='mkdir -p /etc/ceph')
    out, rc = ceph_installer.exec_command(sudo=True,
        cmd='cephadm --image {}/{}:{} bootstrap --mon-ip {}'.\
            format(image['docker_registry'], image['docker_image'], image['docker_tag'],
            installerNode[0].ip_address),
            long_running=True)

    if rc != 0:
        log.error("Failed during deployment")

    return rc
