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
            env-vars (optional):
                        - cleanup=False
                        - objects_count: 500
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

import json
import os
import time

import yaml

from utility import utils
from utility.log import Log
from utility.utils import (
    configure_kafka_security,
    install_start_kafka,
    retain_bucket_pol_at_archive,
    set_config_param,
    setup_cluster_access,
    test_bucket_stats_with_archive,
    test_sync_via_bucket_stats,
    test_user_stats_consistency,
    verify_sync_status,
)

log = Log(__name__)

TEST_DIR = {
    "v1": {
        "script": "/ceph-qe-scripts/rgw/v1/tests/multisite/",
        "lib": "/ceph-qe-scripts/rgw/v1/lib/",
        "config": "/ceph-qe-scripts/rgw/v1/tests/multisite/yamls/",
    },
    "v2": {
        "script": "/ceph-qe-scripts/rgw/v2/tests/s3_swift/",
        "lib": "/ceph-qe-scripts/rgw/v2/lib/",
        "config": "/ceph-qe-scripts/rgw/v2/tests/s3_swift/multisite_configs/",
    },
}


def run(**kw):
    log.info("Running test")
    clusters = kw.get("ceph_cluster_dict")
    config = kw.get("config")
    test_site = kw.get("ceph_cluster")
    log.info(f"test site: {test_site.name}")
    test_site_node = test_site.get_ceph_object("rgw").node
    test_client_node = test_site.get_ceph_object("client").node
    config["git-url"] = config.get(
        "git-url", "https://github.com/red-hat-storage/ceph-qe-scripts.git"
    )

    set_env = config.get("set-env", False)
    extra_pkgs = config.get("extra-pkgs")
    install_start_kafka_broker = config.get("install_start_kafka")
    configure_kafka_broker_security = config.get("configure_kafka_security")
    cloud_type = config.get("cloud-type")
    primary_cluster = clusters.get("ceph-rgw1", clusters[list(clusters.keys())[0]])
    secondary_cluster = clusters.get("ceph-rgw2", clusters[list(clusters.keys())[1]])
    primary_rgw_nodes = primary_cluster.get_ceph_objects("rgw")
    primary_rgw_node = primary_rgw_nodes[0].node
    primary_client_node = primary_cluster.get_ceph_object("client").node
    secondary_rgw_nodes = secondary_cluster.get_ceph_objects("rgw")
    secondary_rgw_node = secondary_rgw_nodes[0].node
    secondary_client_node = secondary_cluster.get_ceph_object("client").node
    run_on_rgw = (
        True
        if primary_cluster.rhcs_version.version[0] == 4
        else config.get("run-on-rgw", False)
    )
    run_on_haproxy = (
        True
        if primary_cluster.rhcs_version.version[0] == 4
        else config.get("run-on-haproxy", False)
    )

    if run_on_rgw:
        exec_from = test_site_node
        append_param = ""

    elif run_on_haproxy:
        exec_from = test_client_node
        append_param = ""

    else:
        exec_from = test_client_node
        append_param = " --rgw-node " + str(test_site_node.ip_address)
    archive_cluster_exists = False
    if "ceph-arc" in clusters.keys():
        if "ceph-sec" not in clusters.keys():
            archive_cluster = clusters.get(
                "ceph-arc", clusters[list(clusters.keys())[1]]
            )
        else:
            archive_cluster = clusters.get(
                "ceph-arc", clusters[list(clusters.keys())[2]]
            )
        archive_rgw_node = archive_cluster.get_ceph_object("rgw").node
        archive_client_node = archive_cluster.get_ceph_object("client").node
        archive_cluster_exists = True

    tertiary_cluster_exists = False
    if "ceph-ter" in clusters.keys():
        if "ceph-arc" not in clusters.keys():
            tertiary_cluster = clusters.get(
                "ceph-ter", clusters[list(clusters.keys())[2]]
            )
        else:
            tertiary_cluster = clusters.get(
                "ceph-ter", clusters[list(clusters.keys())[3]]
            )
        tertiary_rgw_node = tertiary_cluster.get_ceph_object("rgw").node
        tertiary_client_node = tertiary_cluster.get_ceph_object("client").node
        tertiary_cluster_exists = True

    test_folder = "rgw-ms-tests"
    test_folder_path = f"/home/cephuser/{test_folder}"
    home_dir_path = "/home/cephuser/"
    config["test_folder"] = test_folder
    config["test_folder_path"] = test_folder_path

    if set_env:
        set_test_env(config, primary_client_node)
        set_test_env(config, secondary_client_node)
        set_test_env(config, primary_rgw_node)
        set_test_env(config, secondary_rgw_node)
        if len(primary_rgw_nodes) > 1:
            for i in range(1, len(primary_rgw_nodes)):
                set_test_env(config, primary_rgw_nodes[i].node)
        if len(secondary_rgw_nodes) > 1:
            for i in range(1, len(secondary_rgw_nodes)):
                set_test_env(config, secondary_rgw_nodes[i].node)
        if archive_cluster_exists:
            set_test_env(config, archive_rgw_node)
            set_test_env(config, archive_client_node)
        if tertiary_cluster_exists:
            set_test_env(config, tertiary_rgw_node)
            set_test_env(config, tertiary_client_node)
        if primary_cluster.rhcs_version.version[0] >= 5:
            setup_cluster_access(primary_cluster, primary_client_node)
            setup_cluster_access(secondary_cluster, secondary_client_node)
            setup_cluster_access(primary_cluster, primary_rgw_node)
            setup_cluster_access(secondary_cluster, secondary_rgw_node)
            if archive_cluster_exists:
                setup_cluster_access(archive_cluster, archive_rgw_node)
                setup_cluster_access(archive_cluster, archive_client_node)
                set_config_param(archive_client_node)
            if tertiary_cluster_exists:
                setup_cluster_access(tertiary_cluster, tertiary_rgw_node)
                setup_cluster_access(tertiary_cluster, tertiary_client_node)
                set_config_param(tertiary_client_node)
            ms_clusters = [primary_client_node, secondary_client_node]
            for cluster_node in ms_clusters:
                set_config_param(cluster_node)
    # run the test
    script_name = config.get("script-name")
    config_file_name = config.get("config-file-name")
    test_config = {"config": config.get("test-config", {})}
    test_version = config.get("test-version", "v2")
    script_dir = TEST_DIR[test_version]["script"]
    config_dir = TEST_DIR[test_version]["config"]
    lib_dir = TEST_DIR[test_version]["lib"]
    timeout = config.get("timeout", 3600)
    # install extra package which are test specific
    distro_version_id = primary_rgw_node.distro_info["VERSION_ID"]
    if extra_pkgs:
        log.info(f"got extra pkgs: {extra_pkgs}")
        if isinstance(extra_pkgs, dict):
            _pkgs = extra_pkgs.get(int(distro_version_id[0]))
            pkgs = " ".join(_pkgs)
        else:
            pkgs = " ".join(extra_pkgs)

        exec_from.exec_command(
            sudo=True, cmd=f"yum install -y {pkgs}", long_running=True
        )

    log.info("flushing iptables")
    exec_from.exec_command(cmd="sudo iptables -F", check_ec=False)

    if install_start_kafka_broker:
        install_start_kafka(primary_rgw_node, cloud_type)
        install_start_kafka(secondary_rgw_node, cloud_type)
    install_start_kafka_broker_archive = config.get("install_start_kafka_archive")
    if install_start_kafka_broker_archive:
        install_start_kafka(archive_rgw_node, cloud_type)
    if configure_kafka_broker_security:
        configure_kafka_security(primary_rgw_node, cloud_type)
        configure_kafka_security(secondary_rgw_node, cloud_type)

    if test_config["config"]:
        log.info("creating custom config")
        f_name = test_folder_path + config_dir + config_file_name
        remote_fp = exec_from.remote_file(file_name=f_name, file_mode="w", sudo=True)
        remote_fp.write(yaml.dump(test_config, default_flow_style=False))

    cmd_env = " ".join(config.get("env-vars", []))
    test_status = exec_from.exec_command(
        cmd=cmd_env
        + "sudo venv/bin/python "
        + test_folder_path
        + script_dir
        + script_name
        + " -c "
        + test_folder
        + config_dir
        + config_file_name
        + append_param,
        long_running=True,
        timeout=timeout,
    )
    if test_status == 0:
        copy_user_to_site = clusters.get(config.get("copy-user-info-to-site"))
        if copy_user_to_site:
            log.info(f'copy_user_to_site: {config.get("copy-user-info-to-site")}')
            copy_list = []
            copy_list.append(copy_user_to_site.get_ceph_object("rgw").node)
            copy_list.append(copy_user_to_site.get_ceph_object("client").node)
            user_details_file = test_folder_path + lib_dir + "user_details.json"
            for lis in copy_list:
                copy_file_from_node_to_node(
                    user_details_file,
                    exec_from,
                    lis,
                    user_details_file,
                )
            verify_sync_status(copy_user_to_site.get_ceph_object("rgw").node)

        monitor_user_stats = config.get("monitor-user-stats")
        if monitor_user_stats:
            log.info("Test user stats consistency on multisite.")
            test_user_stats_consistency(primary_rgw_node, secondary_rgw_node)

        monitor_consistency_via_bucket_stats = config.get(
            "monitor-consistency-bucket-stats"
        )
        if monitor_consistency_via_bucket_stats:
            log.info("Monitor sync consistency via bucket stats across sites.")
            time.sleep(120)
            test_sync_via_bucket_stats(primary_rgw_node, secondary_rgw_node)

        test_bucket_stats_at_archive = config.get("test-archive-bucket-stats")
        if test_bucket_stats_at_archive:
            log.info(
                "Test no duplicate objects created at archive site via bucket stats"
            )
            time.sleep(1200)
            test_bucket_stats_with_archive(
                primary_client_node, secondary_client_node, archive_client_node
            )

        test_retain_bucket_pol_at_archive = config.get(
            "test-bucket-pol-retained-archive"
        )

        if test_retain_bucket_pol_at_archive:
            log.info("test bucket policies are retained at archive site")
            time.sleep(120)
            retain_bucket_pol_at_archive(
                primary_client_node, secondary_client_node, archive_client_node
            )
        stat_all_archive_site_buckets = config.get("stat-all-buckets-at-archive")
        if stat_all_archive_site_buckets:
            log.info("Bucket stats should not fail for any bucket at the archive site")
            bucket_list_archive = json.loads(
                archive_client_node.exec_command(cmd="radosgw-admin bucket list")[0]
            )
            try:
                for bucket in bucket_list_archive:
                    if "deleted" in bucket:
                        log.info(f"Perform bucket stats on {bucket} at archive site")
                        output, _ = archive_client_node.exec_command(
                            cmd=f"sudo radosgw-admin bucket stats --bucket {bucket}"
                        )
                        log.info(f"Output : {output}")
            except BaseException as err:
                log.error("Error: %s" % err)
                raise Exception(
                    "bucket stats should not fail for any deleted bucket at archive."
                )

        verify_io_on_sites = config.get("verify-io-on-site", [])
        if verify_io_on_sites:
            io_info = home_dir_path + f"io_info_{os.path.basename(config_file_name)}"
            for site in verify_io_on_sites:
                verify_io_on_site_node = clusters.get(site).get_ceph_object("rgw").node
                if config.get("multisite-replication-disabled", False):
                    log.info(
                        f"Multisite replication disabled. skipping sync status check on {site}"
                    )
                else:
                    log.info(f"Check sync status on {site}")
                    verify_sync_status(verify_io_on_site_node)
                # adding sleep for 80 seconds before verification of data starts
                log.info("sleeping for 80 seconds before verification of data starts")
                time.sleep(80)
                log.info(f"verification IO on {site}")
                if test_site != site:
                    copy_file_from_node_to_node(
                        io_info, exec_from, verify_io_on_site_node, io_info
                    )

                verify_status = verify_io_on_site_node.exec_command(
                    cmd="sudo venv/bin/python "
                    + test_folder_path
                    + lib_dir
                    + f"read_io_info.py -c {config_file_name}",
                    long_running=True,
                )
                log.info(f"verify io status code is : {verify_status}")
                if verify_status != 0:  # Verify io failure on other site
                    if config.get("multisite-replication-disabled", False):
                        log.info(
                            f"Multisite replication disabled. objects not synced to {site}"
                            + " and verify io failed as expected."
                        )
                    else:
                        raise Exception(f"verify io failed on {site}")
                else:  # Verify io is successful on other site
                    if config.get("multisite-replication-disabled", False):
                        raise Exception(
                            f"objects synced to {site} even after disabling multisite replication."
                        )
                    else:
                        log.info(f"verify io is successful on {site}")

    return test_status


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
    install_common = config.get("install_common", True)
    if install_common:
        rgw_node.exec_command(
            cmd="yum install -y ceph-common", check_ec=False, sudo=True
        )
    out, err = rgw_node.exec_command(cmd=f"ls -l {test_folder}", check_ec=False)
    if not out:
        rgw_node.exec_command(cmd="sudo mkdir " + test_folder)
        utils.clone_the_repo(config, rgw_node, test_folder_path)
        out, err = rgw_node.exec_command(cmd="ls -l venv", check_ec=False)

        if not out:
            rgw_node.exec_command(
                cmd="yum install python3 -y --nogpgcheck", check_ec=False, sudo=True
            )
            rgw_node.exec_command(cmd="python3 -m venv venv")
            rgw_node.exec_command(cmd="venv/bin/pip install --upgrade pip")
            rgw_node.exec_command(
                cmd=f"venv/bin/pip install -r {test_folder}/ceph-qe-scripts/rgw/requirements.txt"
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
