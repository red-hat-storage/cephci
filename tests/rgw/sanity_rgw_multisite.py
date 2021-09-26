"""
This module will allow us to run tests from ceph-qe-scritps repo.
We tests multisite scenarios here
Repo: https://github.com/red-hat-storage/ceph-qe-scripts
Folder: rgw

Below configs are needed in order to run the tests

    clusters: Primary node or Secondary node
        config:
            script-name:
                        The script to run
            config-file-name:
                        Config file for the above script,
                        use this file if custom test-config is not given
            test-config (optional):
                        Custom test config supported for the above script,
                        refer structure in ceph-qe-scripts/rgw
                        example:
                            test-config:
                                user_count: 1
                                bucket_count: 10
                                objects_count: 5
                                objects_size_range:
                                min: 5M
                                max: 15M
            verify-io-on-site (optional):
                        Primary node> or Secondary node
            extra-pkgs (optional):
                        Packages to install
                        example:
                            a. distro specific packages
                                extra-pkgs:
                                    7:
                                        - pkg1
                                        - pkg2
                                    8:
                                        - pkg1
                                        - pkg2
                            b. list of packages without distro version
                                extra-pkgs:
                                    - pkg1
                                    - pkg2
"""

import logging
import time

import yaml

from utility.utils import setup_cluster_access, sync_status_on_primary

log = logging.getLogger(__name__)

TEST_DIR = {
    "v2": {
        "script": "/ceph-qe-scripts/rgw/v2/tests/s3_swift/",
        "lib": "/ceph-qe-scripts/rgw/v2/lib/",
        "config": "/ceph-qe-scripts/rgw/v2/tests/s3_swift/multisite_configs/",
    }
}


def run(**kw):
    log.info("Running test")
    clusters = kw.get("ceph_cluster_dict")
    config = kw.get("config")
    test_site = kw.get("ceph_cluster")
    log.info(f"test site: {test_site.name}")
    test_site_node = test_site.get_ceph_object("rgw").node

    # adding sleep for 60 seconds before another test starts, sync needs to complete
    time.sleep(60)
    set_env = config.get("set-env", False)
    primary_cluster = clusters.get("ceph-rgw1", clusters[list(clusters.keys())[0]])
    secondary_cluster = clusters.get("ceph-rgw2", clusters[list(clusters.keys())[1]])
    primary_rgw_node = primary_cluster.get_ceph_object("rgw").node
    secondary_rgw_node = secondary_cluster.get_ceph_object("rgw").node

    test_folder = "rgw-ms-tests"
    test_folder_path = f"/home/cephuser/{test_folder}"
    home_dir_path = "/home/cephuser/"
    config["test_folder"] = test_folder
    config["test_folder_path"] = test_folder_path

    if set_env:
        set_test_env(config, primary_rgw_node)
        set_test_env(config, secondary_rgw_node)

        if primary_cluster.rhcs_version == "5.0":
            setup_cluster_access(primary_cluster, primary_rgw_node)
            setup_cluster_access(secondary_cluster, secondary_rgw_node)

    # run the test
    script_name = config.get("script-name")
    config_file_name = config.get("config-file-name")
    test_config = {"config": config.get("test-config", {})}
    test_version = config.get("test-version", "v2")
    script_dir = TEST_DIR[test_version]["script"]
    config_dir = TEST_DIR[test_version]["config"]
    lib_dir = TEST_DIR[test_version]["lib"]
    timeout = config.get("timeout", 300)

    log.info("flushing iptables")
    test_site_node.exec_command(cmd="sudo iptables -F", check_ec=False)

    if test_config["config"]:
        log.info("creating custom config")
        f_name = test_folder + config_dir + config_file_name
        remote_fp = test_site_node.remote_file(file_name=f_name, file_mode="w")
        remote_fp.write(yaml.dump(test_config, default_flow_style=False))

    out, err = test_site_node.exec_command(
        cmd="sudo python3 "
        + test_folder_path
        + script_dir
        + script_name
        + " -c "
        + test_folder
        + config_dir
        + config_file_name,
        timeout=timeout,
    )

    log.info(out.read().decode())
    log.error(err.read().decode())

    copy_user_to_site = clusters.get(config.get("copy-user-info-to-site"))
    if copy_user_to_site:
        log.info(f'copy_user_to_site: {config.get("copy-user-info-to-site")}')
        copy_user_to_site_node = copy_user_to_site.get_ceph_object("rgw").node
        user_details_file = test_folder_path + lib_dir + "user_details.json"
        copy_file_from_node_to_node(
            user_details_file, test_site_node, copy_user_to_site_node, user_details_file
        )

    verify_io_on_sites = config.get("verify-io-on-site", [])
    if verify_io_on_sites:
        io_info = home_dir_path + "io_info.yaml"
        for site in verify_io_on_sites:
            verify_io_on_site_node = clusters.get(site).get_ceph_object("rgw").node
            log.info(f"Check sync status on {site}")
            sync_status_on_primary(verify_io_on_site_node)
            # adding sleep for 60 seconds before verification of data starts
            time.sleep(60)
            log.info(f"verification IO on {site}")
            if test_site != site:
                copy_file_from_node_to_node(
                    io_info, test_site_node, verify_io_on_site_node, io_info
                )

            verify_out, err = verify_io_on_site_node.exec_command(
                cmd="sudo python3 " + test_folder_path + lib_dir + "read_io_info.py",
                timeout=timeout,
            )
            log.info(verify_out.read().decode())
            log.error(verify_out.read().decode())

    return 0


def set_test_env(config, rgw_node):
    """
    Sets up the test environment

    :param config: test config
    :param rgw_node: rgw node object

    """

    log.info("setting up the test env")

    test_folder = config["test_folder"]
    test_folder_path = config["test_folder_path"]

    log.info("flushing iptables")
    rgw_node.exec_command(cmd="sudo iptables -F", check_ec=False)
    rgw_node.exec_command(cmd="sudo yum install python3 -y", check_ec=False)
    rgw_node.exec_command(cmd="sudo rm -rf " + test_folder)
    rgw_node.exec_command(cmd="sudo mkdir " + test_folder)
    clone_the_repo(config, rgw_node, test_folder_path)

    rgw_node.exec_command(
        cmd=f"sudo pip3 install -r {test_folder}/ceph-qe-scripts/rgw/requirements.txt"
    )


def copy_file_from_node_to_node(src_file, src_node, dest_node, dest_file):
    """
    Copies file from one node to another node

    :param src_file: filename to be copied
    :param src_node: node to be copied from
    :param dest_node: node to copied to
    :param dest_file: destination filename

    """
    log.info(f"copying {src_file} from {src_node.ip_address} to {dest_node.ip_address}")
    src_file_obj = read_file_from_node(src_file, src_node)
    write_file_to_node(src_file_obj, dest_file, dest_node)


def read_file_from_node(file_name, node):
    """
    read file_name from node and returns
    remote_file object

    :param file_name: file_name to read
    :param node: ceph node
    :return: remote file object
    """

    log.info(f"reading {file_name} from {node.ip_address}")
    try:
        file_obj = node.remote_file(
            sudo=True, file_name=file_name, file_mode="r"
        ).read()

        return file_obj

    except FileNotFoundError:
        raise FileNotFoundError(f"file to read is missing here: {file_name}")


def write_file_to_node(file_obj, file_name, node):
    """
    write to file from ceph node using remote_file obj
    :param file_obj: remote_file object
    :param file_name: destination file name
    :param node: ceph node to write

    """

    log.info(f"write to {file_name} in node: {node.ip_address}")
    dest_file_obj = node.remote_file(sudo=True, file_name=file_name, file_mode="w")
    dest_file_obj.write(file_obj)
    dest_file_obj.flush()


def clone_the_repo(config, node, path_to_clone):
    """
    clone the repo on to test node
    :param config: test config
    :param node: ceph rgw node
    :param path_to_clone: the path to clone the repo

    """
    log.info("cloning the repo")
    branch = config.get("branch", "master")
    log.info(f"branch: {branch}")
    repo_url = config.get(
        "git-url", "https://github.com/red-hat-storage/ceph-qe-scripts.git"
    )
    log.info(f"repo_url: {repo_url}")
    git_clone_cmd = f"sudo git clone {repo_url} -b {branch}"
    node.exec_command(cmd=f"cd {path_to_clone} ; {git_clone_cmd}")
