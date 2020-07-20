import logging
from ceph.ceph import CommandFailed

log = logging.getLogger(__name__)

_VERSION = {
    "7": ("pip2", "python2"),
    "8": ("pip3", "python3")
}

_CHECK_EXE_INSTALLED = "type {}"


def run(**kw):
    log.info("Running rbd tests")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
    script_name = config.get('test_name')
    timeout = config.get('timeout', 1800)
    python3 = config.get('python3', True)

    rgw_client_nodes = []
    for node in ceph_nodes:
        if node.role == 'client':
            rgw_client_nodes.append(node)
    git_url = 'https://github.com/red-hat-storage/ceph-qe-scripts.git'
    git_clone = 'git clone {}'.format(git_url)
    client_node = rgw_client_nodes[0]

    # Cleanup any existing stale test dir
    test_folder = 'rbd-tests'
    client_node.exec_command(cmd='sudo rm -rf {}'.format(test_folder))
    client_node.exec_command(cmd='mkdir {}'.format(test_folder))
    client_node.exec_command(cmd='cd {}; {} '.format(test_folder, git_clone))

    # Get python, pip
    distro_info = client_node.distro_info
    distro_ver = distro_info['VERSION_ID'].split(".")[0]
    pip, python = _VERSION[distro_ver] if not python3 else _VERSION['8']

    # Check python, pip existence
    _INSTALLED = True
    for exe in (pip, python):
        try:
            client_node.exec_command(cmd=_CHECK_EXE_INSTALLED.format(exe))
        except CommandFailed as err:
            _INSTALLED = False
            log.error(err)

    # Install pip on rhel-7.x systems
    if distro_ver.startswith('7') and not _INSTALLED:
        try:
            client_node.exec_command(
                cmd='sudo yum install python3 -y',
                long_running=True, check_ec=False
            )
            client_node.exec_command(
                cmd='sudo curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py --insecure',
                long_running=True, check_ec=False
            )
            client_node.exec_command(
                cmd='sudo {} get-pip.py --trusted-host files.pythonhosted.org '
                    '--trusted-host pypi.org --trusted-host pypi.python.org'.format(python),
                long_running=True, check_ec=False
            )
        except CommandFailed as err:
            raise CommandFailed(err)

    # Install pip utils
    client_node.exec_command(
        cmd='{} install boto names PyYaml ConfigParser'.format(pip)
    )

    command = 'sudo {} ~/{}/ceph-qe-scripts/rbd/system/{}'.format(python,
                                                                  test_folder,
                                                                  script_name)

    if config.get('ec-pool-k-m', None):
        command += ' --ec-pool-k-m {}'.format(config.get('ec-pool-k-m'))

    stdout, stderr = client_node.exec_command(cmd=command,
                                              timeout=timeout,
                                              check_ec=False)
    error, output = stderr.read().decode(), stdout.read().decode()
    log.info(output)
    log.error(error)

    ec = client_node.exit_status
    if ec == 0:
        log.info("{command} completed successfully".format(command=command))
    else:
        log.error("{command} has failed".format(command=command))
    return ec
