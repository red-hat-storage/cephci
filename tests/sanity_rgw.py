import logging

log = logging.getLogger(__name__)

DIR = {"v1": {"script": "/ceph-qe-scripts/rgw/v1/tests/s3/",
              "config": "/ceph-qe-scripts/rgw/v1/tests/s3/yamls/"},
       "v2": {"script": "/ceph-qe-scripts/rgw/v2/tests/s3_swift/",
              "config": "/ceph-qe-scripts/rgw/v2/tests/s3_swift/configs/"}}


def run(ceph_cluster, **kw):
    """

    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """
    log.info("Running test")
    config = kw.get('config')
    log.info("Running rgw tests %s" % config.get('test-version', 'v2'))
    rgw_ceph_object = ceph_cluster.get_ceph_object('rgw')
    git_url = 'https://github.com/red-hat-storage/ceph-qe-scripts.git'
    branch = ' -b master'
    git_clone = 'sudo git clone ' + git_url + branch
    rgw_node = rgw_ceph_object.node
    # cleanup any existing stale test dir
    log.info('flushing iptables')
    rgw_node.exec_command(cmd='sudo iptables -F', check_ec=False)
    test_folder = 'rgw-tests'
    test_folder_path = '~/{test_folder}'.format(test_folder=test_folder)
    rgw_node.exec_command(cmd='sudo yum install python3 -y', check_ec=False)
    rgw_node.exec_command(cmd='sudo rm -rf ' + test_folder)
    rgw_node.exec_command(cmd='sudo mkdir ' + test_folder)
    rgw_node.exec_command(cmd='cd ' + test_folder + ' ; ' + git_clone)
    if ceph_cluster.containerized:
        test_folder_path = '/{test_folder}'.format(test_folder=test_folder)
        rgw_ceph_object.exec_command(cmd='sudo rm -rf ' + test_folder)
        rgw_ceph_object.exec_command(cmd='sudo mkdir ' + test_folder)
        rgw_node.exec_command(
            cmd='sudo docker cp {test_folder}/* {container}:/{test_folder}/'.format(
                container=rgw_ceph_object.container_name,
                test_folder=test_folder))
        rgw_node.exec_command(cmd='curl https://bootstrap.pypa.io/get-pip.py -o ~/get-pip.py')
        rgw_node.exec_command(
            cmd='sudo docker cp ~/get-pip.py {container}:/get-pip.py'.format(container=rgw_ceph_object.container_name))
        rgw_ceph_object.exec_command('python3 /get-pip.py')
    rgw_ceph_object.exec_command(
        cmd='sudo pip3 install -r ' + test_folder + '/ceph-qe-scripts/rgw/requirements.txt')
    script_name = config.get('script-name')
    config_file_name = config.get('config-file-name')
    test_version = config.get('test-version', 'v2')
    script_dir = DIR[test_version]['script']
    config_dir = DIR[test_version]['config']
    timeout = config.get('timeout', 300)
    out, err = rgw_ceph_object.exec_command(
        cmd='sudo python3 ' + test_folder_path + script_dir + script_name + ' -c '
            + test_folder + config_dir + config_file_name,
        timeout=timeout)
    log.info(out.read().decode())
    log.error(err.read().decode())
    return 0
