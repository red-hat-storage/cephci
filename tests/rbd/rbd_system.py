import logging as log


def run(**kw):
    log.info("Running rbd tests")
    ceph_nodes = kw.get('ceph_nodes')
    rgw_client_nodes = []
    for node in ceph_nodes:
        if node.role == 'client':
            rgw_client_nodes.append(node)
    git_url = 'https://github.com/red-hat-storage/ceph-qe-scripts.git'
    git_clone = 'git clone ' + git_url
    client_node = rgw_client_nodes[0]
    # cleanup any existing stale test dir
    test_folder = 'rbd-tests'
    client_node.exec_command(cmd='sudo yum -y install python3')
    create_virtual_env = 'python3 -m venv env ; source env/bin/activate'
    del_add_folder = 'sudo rm -rf ' + test_folder + ' ; ' + 'mkdir ' + test_folder
    clone_folder = 'cd ' + test_folder + ' ; ' + git_clone
    config = kw.get('config')
    script_name = config.get('test_name')
    timeout = config.get('timeout', 1800)
    if config.get('ec-pool-k-m', None):
        ec_pool_arg = ' --ec-pool-k-m ' + config.get('ec-pool-k-m')
    else:
        ec_pool_arg = ''
    command = 'sudo python3 ~/' + test_folder + '/ceph-qe-scripts/rbd/system/' + script_name + ec_pool_arg
    commands = (create_virtual_env + ';' + del_add_folder + ';' + clone_folder + ';' + command + '; deactivate')
    stdout, stderr = client_node.exec_command(cmd=commands, timeout=timeout, check_ec=False)
    output = stdout.read().decode()
    if output:
        log.info(output)
    output = stderr.read().decode()
    if output:
        log.error(output)
    ec = client_node.exit_status
    if ec == 0:
        log.info("{command} completed successfully".format(command=command))
    else:
        log.error("{command} has failed".format(command=command))
    return ec
