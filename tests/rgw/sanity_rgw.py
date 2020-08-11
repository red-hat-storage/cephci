import logging
# from ceph.ceph import Ceph

log = logging.getLogger(__name__)

DIR = {"v1": {"script": "/ceph-qe-scripts/rgw/v1/tests/s3/",
              "lib": "/ceph-qe-scripts/rgw/v1/lib/",
              "config": "/ceph-qe-scripts/rgw/v1/tests/s3/yamls/"},

       "v2": {"script": "/ceph-qe-scripts/rgw/v2/tests/s3_swift/",
              "lib": "/ceph-qe-scripts/rgw/v2/lib/",
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
    run_io_verify = config.get('run_io_verify', False)
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
        rgw_node.exec_command(cmd='sudo yum install ceph-radosgw -y')
    rgw_node.exec_command(
        cmd='sudo pip3 install -r {test_folder}/ceph-qe-scripts/rgw/requirements.txt'.format(test_folder=test_folder))
    script_name = config.get('script-name')
    config_file_name = config.get('config-file-name')
    test_version = config.get('test-version', 'v2')
    script_dir = DIR[test_version]['script']
    config_dir = DIR[test_version]['config']
    lib_dir = DIR[test_version]['lib']
    timeout = config.get('timeout', 300)
    out, err = rgw_node.exec_command(
        cmd='sudo python3 ' + test_folder_path + script_dir + script_name + ' -c '
            + test_folder + config_dir + config_file_name, timeout=timeout)
    log.info(out.read().decode())
    log.error(err.read().decode())

    if run_io_verify:
        log.info('running io verify script')
        verify_out, err = rgw_node.exec_command(cmd='sudo python3 ' + test_folder_path + lib_dir + 'read_io_info.py',
                                                timeout=timeout)
        log.info(verify_out.read().decode())
        log.error(verify_out.read().decode())

    return 0
