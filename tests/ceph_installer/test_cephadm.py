import logging
from ceph.ceph_admin import CephAdmin

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
    test_data = kw.get('test_data')
    # base_url = config.get('base_url', None)
    ceph_cluster.custom_config = test_data.get('custom-config')
    ceph_cluster.custom_config_file = test_data.get('custom-config-file')

    build = config.get('build', config.get('rhbuild'))
    name = config.get('cluster_name', 'ceph')
    # deploy = config.get('deploy', False)
    exec_shell = config.get('exec_shell', False)
    ceph_cluster.rhcs_version = build

    if config.get('skip_setup') is True:
        log.info("Skipping setup of ceph cluster")
        return 0

    # get installer node
    ceph_installer = ceph_cluster.get_ceph_object('installer')
    cephadm = CephAdmin(name=name,
                        ceph_cluster=ceph_cluster,
                        ceph_installer=ceph_installer,
                        **config)

    # Deployment-only
    # if config.get('deployment'):
    #     cephadm.deploy()
    #     return 0

    if exec_shell:
        for cmd in exec_shell:
            cmd = cmd if isinstance(cmd, list) else [cmd]
            cephadm.shell(
                remote=ceph_installer,
                args=cmd,
            )
        return 0

    # copy ssh keys to other hosts
    ceph_cluster.setup_ssh_keys()

    # set tool download repository
    cephadm.set_tool_repo()

    # install/download cephadm package on installer
    cephadm.install_cephadm()

    # bootstrap cluster
    cephadm.bootstrap()

    # add all hosts
    cephadm.manage_hosts()

    # Add lables to the host
    cephadm.label_host()

    # add all daemons
    cephadm.add_daemons()

    return 0
