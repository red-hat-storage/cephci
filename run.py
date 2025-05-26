#!/usr/bin/env python3

import datetime
import importlib
import json
import os
import pickle
import re
import sys
import time
import traceback
from copy import deepcopy
from getpass import getuser

import requests
import yaml
from docopt import docopt
from libcloud.common.types import LibcloudError

import init_suite
from ceph.ceph import Ceph, CephNode
from ceph.clients import WinNode
from ceph.utils import (
    cleanup_ceph_nodes,
    cleanup_ibmc_ceph_nodes,
    create_baremetal_ceph_nodes,
    create_ceph_nodes,
    create_ibmc_ceph_nodes,
)
from cephci.cluster_info import get_ceph_var_logs
from cli.performance.memory_and_cpu_utils import (
    start_logging_processes,
    stop_logging_process,
    upload_mem_and_cpu_logger_script,
)
from utility import sosreport
from utility.log import Log
from utility.polarion import post_to_polarion
from utility.retry import retry
from utility.utils import (  # ReportPortal,
    check_build_overrides,
    create_run_dir,
    create_unique_test_name,
    email_results,
    fetch_build_artifacts,
    generate_unique_id,
    magna_url,
    setup_cluster_access,
    validate_conf,
    validate_image,
)
from utility.xunit import create_xunit_results

doc = """
A simple test suite wrapper that executes tests based on yaml test configuration

 Usage:
  run.py --rhbuild BUILD
        (--platform <name>)
        (--suite <FILE>)...
        (--global-conf FILE | --cluster-conf FILE)
        [--cloud <openstack> | <ibmc> | <baremetal>]
        [--build <name>]
        [--inventory FILE]
        [--osp-cred <file>]
        [--rhs-ceph-repo <repo>]
        [--ubuntu-repo <repo>]
        [--add-repo <repo>]
        [--kernel-repo <repo>]
        [--store | --reuse <file>]
        [--skip-cluster]
        [--skip-subscription]
        [--docker-registry <registry>]
        [--docker-image <image>]
        [--docker-tag <tag>]
        [--insecure-registry]
        [--post-results]
        [--upstream-build <upstream-build>]
        [--log-level <LEVEL>]
        [--log-dir  <directory-name>]
        [--instances-name <name>]
        [--osp-image <image>]
        [--filestore]
        [--use-ec-pool <k,m>]
        [--hotfix-repo <repo>]
        [--skip-version-compare]
        [--custom-config <key>=<value>]...
        [--custom-config-file <file>]
        [--xunit-results]
        [--enable-eus]
        [--skip-enabling-rhel-rpms]
        [--skip-sos-report]
        [--skip-tc <items>]
        [--monitor-performance]
        [--disable-console-log]
  run.py --cleanup=name --osp-cred <file> [--cloud <str>]
        [--log-level <LEVEL>]

Options:
  -h --help                         show this screen
  -v --version                      run version
  -s <smoke> --suite <smoke>        test suite to run
                                    eg: -s smoke or -s rbd
  -f <tests> --filter <tests>       filter tests based on the patter
                                    eg: -f 'rbd' will run tests that have 'rbd'
  --global-conf <file>              global cloud configuration file
  --cluster-conf <file>             cluster configuration file
  --inventory <file>                hosts inventory file
  --cloud <cloud_type>              cloud type [default: openstack]
  --osp-cred <file>                 openstack credentials as separate file
  --rhbuild <1.3.0>                 ceph downstream version
                                    eg: 1.3.0, 2.0, 2.1 etc
  --build <latest>                  Type of build to be use for testing
                                    eg: latest|tier-0|tier-1|tier-2|released|upstream
                                    [default: released]
  --upstream-build <upstream-build> eg: quincy|pacific
  --platform <rhel-8>               select platform version eg., rhel-8, rhel-7
  --rhs-ceph-repo <repo>            location of rhs-ceph repo
                                    Top level location of compose
  --add-repo <repo>                 Any additional repo's need to be enabled
  --ubuntu-repo <repo>              http location of downstream ubuntu repo
  --kernel-repo <repo>              Zstream Kernel Repo location
  --store                           store the current vm state for reuse
  --reuse <file>                    use the stored vm state for rerun
  --skip-cluster                    skip cluster creation from ansible/ceph-deploy
  --skip-subscription               skip subscription manager if using beta rhel images
  --docker-registry <registry>      Docker registry, default value is taken from ansible
                                    config
  --docker-image <image>            Docker image, default value is taken from ansible
                                    config
  --docker-tag <tag>                Docker tag, default value is 'latest'
  --insecure-registry               Disable security check for docker registry
  --post-results                    Post results to polarion, needs Polarion IDs
                                    in test suite yamls. Requires config file, see
                                    README.
  --log-level <LEVEL>               Set logging level
  --log-dir <LEVEL>                 Set log directory
  --instances-name <name>           Name that will be used for instances creation
  --osp-image <image>               Image for osp instances, default value is taken from
                                    conf file
  --filestore                       To specify filestore as osd object store
  --use-ec-pool <k,m>               To use ec pools instead of replicated pools
  --hotfix-repo <repo>              To run sanity on hotfix build
  --skip-version-compare            Skip verification that ceph versions change post
                                    upgrade
  -c --custom-config <name>=<value> Add a custom config key/value to ceph_conf_overrides
  --custom-config-file <file>       Add custom config yaml to ceph_conf_overrides
  --xunit-results                   Create xUnit result file for test suite run
                                    [default: false]
  --enable-eus                      Enables EUS rpms on EUS suppored distro
                                    [default: false]
  --skip-enabling-rhel-rpms         skip adding rpms from subscription if using beta
                                    rhel images for Interop runs
  --skip-sos-report                 Enables to collect sos-report on test suite failures
                                    [default: false]
  --skip-tc <items>                 skip test case provided in comma seperated fashion
  --monitor-performance             Monitor performance and CPU usage on all/required nodes
                                    for every test and collects data to specified dir
  --disable-console-log             To stopping logging to console
                                    [default: false]
"""
log = Log()
test_names = []
run_summary = {}


@retry(LibcloudError, tries=5, delay=15)
def create_nodes(
    conf,
    inventory,
    osp_cred,
    run_id,
    cloud_type="openstack",
    instances_name=None,
    enable_eus=False,
):
    """Creates the system under test environment."""

    validate_conf(conf)
    validate_image(conf, cloud_type)
    if cloud_type == "openstack":
        cleanup_ceph_nodes(osp_cred, instances_name)
    elif cloud_type == "ibmc":
        cleanup_ibmc_ceph_nodes(osp_cred, instances_name)

    ceph_cluster_dict = {}
    clients = []
    for cluster in conf.get("globals"):
        if cloud_type == "openstack":
            ceph_vmnodes = create_ceph_nodes(
                cluster,
                inventory,
                osp_cred,
                run_id,
                instances_name,
                enable_eus=enable_eus,
            )
        elif cloud_type == "ibmc":
            ceph_vmnodes = create_ibmc_ceph_nodes(
                cluster, inventory, osp_cred, run_id, instances_name
            )
        elif "baremetal" in cloud_type:
            ceph_vmnodes = create_baremetal_ceph_nodes(cluster)
        else:
            log.error(f"Unknown cloud type: {cloud_type}")
            raise AssertionError("Unsupported test environment.")

        ceph_nodes = []
        root_password = None
        for node in ceph_vmnodes.values():
            look_for_key = False
            private_key_path = ""

            if cloud_type == "openstack":
                private_ip = node.get_private_ip()
                ceph_nodename = node.node.name
            elif "baremetal" in cloud_type:
                private_key_path = node.private_key if node.private_key else ""
                private_ip = node.ip_address
                look_for_key = True if node.private_key else False
                root_password = node.root_password
                ceph_nodename = node.hostname
            elif cloud_type == "ibmc":
                glbs = osp_cred.get("globals")
                ibmc = glbs.get("ibm-credentials")
                private_key_path = ibmc.get("private_key_path")
                private_ip = node.ip_address
                look_for_key = True
                ceph_nodename = node.hostname

            if node.role == "win-iscsi-clients":
                clients.append(
                    WinNode(ip_address=node.ip_address, private_ip=private_ip)
                )
            else:
                ceph = CephNode(
                    username="cephuser",
                    password="cephuser",
                    root_password="passwd" if not root_password else root_password,
                    look_for_key=look_for_key,
                    private_key_path=private_key_path,
                    root_login=node.root_login,
                    role=node.role,
                    no_of_volumes=node.no_of_volumes,
                    ip_address=node.ip_address,
                    subnet=node.subnet,
                    private_ip=private_ip,
                    hostname=node.hostname,
                    ceph_vmnode=node,
                    ceph_nodename=ceph_nodename,
                    id=node.id,
                )
                ceph_nodes.append(ceph)

        cluster_name = cluster.get("ceph-cluster").get("name", "ceph")
        ceph_cluster_dict[cluster_name] = Ceph(cluster_name, ceph_nodes)

        # Set the network attributes of the cluster
        # ToDo: Support other providers like openstack and IBM-C
        if "baremetal" in cloud_type:
            ceph_cluster_dict[cluster_name].networks = deepcopy(
                cluster.get("ceph-cluster", {}).get("networks", {})
            )

    # TODO: refactor cluster dict to cluster list
    log.info("Done creating osp instances")
    log.info("Waiting for Floating IPs to be available")
    log.info("Sleeping 15 Seconds")
    time.sleep(15)

    for cluster_name, cluster in ceph_cluster_dict.items():
        for instance in cluster:
            try:
                instance.connect()
            except BaseException:
                raise

    return ceph_cluster_dict, clients


def print_results(tc):
    header = "\n{name:<30s}   {desc:<60s}   {duration:<30s}   {status:<15s}    {comments:>15s}".format(
        name="TEST NAME",
        desc="TEST DESCRIPTION",
        duration="DURATION",
        status="STATUS",
        comments="COMMENTS",
    )
    print(header)
    for test in tc:
        if test.get("duration"):
            dur = str(test["duration"])
        else:
            dur = "0s"

        name = test["name"]
        desc = test["desc"] or "None"
        status = test["status"]
        comments = test["comments"]
        line = f"{name:<30.30s}   {desc:<60.60s}   {dur:<30s}   {status:<15s}   {comments:>15s}"

        print(line)


def load_file(file_name):
    """Retrieve yaml data content from file."""
    file_path = os.path.abspath(file_name)
    with open(file_path, "r") as conf_:
        content = yaml.safe_load(conf_)

    return content


def get_tier_level(files: str) -> str:
    """
    Retrieves the RHCS QE tier level based on the suite path or filename.

    The argument contains "::" as a separate for multiple test suite execution. The
    understanding in both the cases is that, the qe stage is provided at the starting
    of the filename or directory.

    Supported formats are

        tier_0/
        tier-1/
        tier_0_rgw.yhaml
        tier-1_cephadm.yaml

    Args:
        files (str):    The suite files passed for execution.

    Returns:
        (str) Tier level
    """
    file = files.split("::")[0]
    file_name = file.split("/")[-1]

    if file_name.startswith("tier_"):
        return "-".join(file_name.split("_")[0:2])

    if file_name.startswith("tier-"):
        return file_name.split("_")[0]

    return "Unknown"


def get_html_page(url: str) -> str:
    """Returns the content of the provided link."""
    resp = requests.get(url, verify=False)

    try:
        if resp.ok:
            return resp.text
    except BaseException as be:  # noqa
        log.debug(f"Failed to retrieve contents at {url}")

    return ""


def run(args):
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Mandatory arguments
    rhbuild = args["--rhbuild"]
    suite_files = args["--suite"]

    glb_file = args.get("--global-conf")
    if args.get("--cluster-conf"):
        glb_file = args["--cluster-conf"]

    # Deciders
    reuse = args.get("--reuse")
    cloud_type = args.get("--cloud")

    # These are not mandatory options
    inventory_file = args.get("--inventory")
    osp_cred_file = args.get("--osp-cred")

    osp_cred = load_file(osp_cred_file) if osp_cred_file else dict()
    cleanup_name = args.get("--cleanup")

    # Set log directory and get absolute path
    console_log_level = args.get("--log-level")
    log_directory = args.get("--log-dir")
    disable_console_log = args.get("--disable-console-log", False)
    post_to_report_portal = args.get("--report-portal")

    # Get Perf and CPU mon param
    enable_perf_mon = args.get("--monitor-performance", False)

    # jenkin job url
    jenkin_job_url = os.environ.get("BUILD_URL")

    run_id = generate_unique_id(length=6)

    run_dir = create_run_dir(run_id, log_directory)

    log.configure_logger("startup", run_dir, disable_console_log)

    if console_log_level:
        log.logger.setLevel(console_log_level.upper())

    run_start_time = datetime.datetime.now()
    trigger_user = getuser()

    build = None
    base_url = None
    ubuntu_repo = None
    docker_registry = None
    docker_image = None
    docker_tag = None

    ceph_name = None
    compose_id = None

    if cleanup_name and not osp_cred:
        raise Exception("Need cloud credentials to perform cleanup.")

    if cleanup_name:
        if cloud_type == "openstack":
            cleanup_ceph_nodes(osp_cred, cleanup_name)
        elif cloud_type == "ibmc":
            cleanup_ibmc_ceph_nodes(osp_cred, cleanup_name)
        else:
            log.warning("Unknown cloud type.")

        return 0

    if glb_file is None and not reuse:
        raise Exception("Unable to gather information about cluster layout.")

    if osp_cred_file is None and not reuse and cloud_type in ["openstack", "ibmc"]:
        raise Exception("Require cloud credentials to create cluster.")

    if inventory_file is None and not reuse and cloud_type in ["openstack", "ibmc"]:
        raise Exception("Require system configuration information to provision.")

    platform = args["--platform"]
    build = args.get("--build")
    upstream_build = args.get("--upstream-build", None)

    base_url = args.get("--rhs-ceph-repo")
    ubuntu_repo = args.get("--ubuntu-repo")
    docker_registry = args.get("--docker-registry")
    docker_image = args.get("--docker-image")
    docker_tag = args.get("--docker-tag")
    kernel_repo = args.get("--kernel-repo")

    if not check_build_overrides(base_url, docker_registry, docker_image, docker_tag):
        if build and build not in ["released"]:
            base_url, docker_registry, docker_image, docker_tag = fetch_build_artifacts(
                build, rhbuild, platform, upstream_build
            )
    else:
        build = None

    store = args.get("--store") or False

    docker_insecure_registry = args.get("--insecure-registry")

    post_results = args.get("--post-results")
    skip_setup = args.get("--skip-cluster")
    skip_tc = args.get("--skip-tc")
    if skip_tc:
        skip_tc_list = (
            [item for item in skip_tc.split(",")] if "," in skip_tc else [skip_tc]
        )
    else:
        skip_tc_list = []
    skip_subscription = args.get("--skip-subscription")

    instances_name = args.get("--instances-name")
    if instances_name:
        instances_name = instances_name.replace(".", "-")

    osp_image = args.get("--osp-image")
    filestore = args.get("--filestore")
    ec_pool_vals = args.get("--use-ec-pool")
    skip_version_compare = args.get("--skip-version-compare")
    custom_config = args.get("--custom-config")
    custom_config_file = args.get("--custom-config-file")
    xunit_results = args.get("--xunit-results")

    enable_eus = args.get("--enable-eus")
    skip_enabling_rhel_rpms = args.get("--skip-enabling-rhel-rpms")
    skip_sos_report = args.get("--skip-sos-report")

    # load config, suite and inventory yaml files
    conf = load_file(glb_file)
    suite = init_suite.load_suites(suite_files)
    log.debug(f"Found the following valid test suites: {suite['tests']}")
    if suite["nan"] and not suite["tests"]:
        raise Exception("Please provide valid test suite name")

    cli_arguments = f"{sys.executable} {' '.join(sys.argv)}"
    log.info(f"The CLI for the current run :\n{cli_arguments}\n")
    log.info(f"RPM Compose source - {base_url}")
    log.info(f"Red Hat Ceph Image used - {docker_registry}/{docker_image}:{docker_tag}")

    ceph_version = []
    ceph_ansible_version = []
    distro = []
    clients = []

    inventory = None
    image_name = None
    if inventory_file:
        inventory = load_file(inventory_file)

        if osp_image and inventory.get("instance", {}).get("create"):
            inventory.get("instance").get("create").update({"image-name": osp_image})

        image_name = inventory.get("instance", {}).get("create", {}).get("image-name")

        if inventory.get("instance", {}).get("create"):
            distro.append(inventory.get("instance").get("create").get("image-name"))

    for cluster in conf.get("globals"):
        if cluster.get("ceph-cluster").get("inventory"):
            cluster_inventory_path = os.path.abspath(
                cluster.get("ceph-cluster").get("inventory")
            )
            with open(cluster_inventory_path, "r") as inventory_stream:
                cluster_inventory = yaml.safe_load(inventory_stream)
            image_name = (
                cluster_inventory.get("instance").get("create").get("image-name")
            )
            distro.append(image_name.replace(".iso", ""))

        # get COMPOSE ID and ceph version
        if build not in ["released", "cvp", "upstream", None]:
            compose_id = get_html_page(url=f"{base_url}/COMPOSE_ID")

            ver_url = f"{base_url}/compose/Tools/x86_64/os/Packages/"
            if cloud_type.startswith("ibmc"):
                ver_url = f"{base_url}/Tools/Packages/"

            if platform.startswith("ubuntu"):
                os_version = platform.split("-")[1]
                ver_url = (
                    f"{base_url}/Tools/dists/{os_version}/main/binary-amd64/Packages"
                )

            # Ceph Version
            ver_text = get_html_page(url=ver_url)
            search_results = re.search(r"ceph-common-(.*?).x86", ver_text)
            if search_results:
                ceph_version.append(search_results.group(1))

            # Ceph Ansible Version
            search_results = re.search(r"ceph-ansible-(.*?).rpm", ver_text)
            if search_results:
                ceph_ansible_version.append(search_results.group(1))

    distro = ", ".join(list(set(distro)))
    if not ceph_version and build == "upstream":
        ceph_version.append(upstream_build)
    ceph_version = ", ".join(list(set(ceph_version)))
    ceph_ansible_version = ", ".join(list(set(ceph_ansible_version)))

    log.info(f"Compose id is: {compose_id}")
    log.info(f"Testing Ceph Version: {ceph_version}")
    log.info(f"Testing Ceph Ansible Version: {ceph_ansible_version}")

    service = None
    suite_name = "::".join(suite_files)

    def fetch_test_details(var) -> dict:
        """
        Accepts the test and then provides the parameters of that test as a list.

        :param var: the test collected from the suite file
        :return: Returns a dictionary of the various test params
        """
        details = dict()
        details["docker-containers-list"] = []
        details["name"] = var.get("name")
        details["desc"] = var.get("desc")
        details["file"] = var.get("module")
        details["cli_arguments"] = cli_arguments
        details["polarion-id"] = var.get("polarion-id")
        polarion_default_url = "https://polarion.engineering.redhat.com/polarion/#/project/CEPH/workitem?id="
        details["polarion-id-link"] = "{}{}".format(
            polarion_default_url, details["polarion-id"]
        )
        details["rhbuild"] = rhbuild
        details["cloud-type"] = cloud_type
        details["ceph-version"] = ceph_version
        details["ceph-ansible-version"] = ceph_ansible_version
        details["compose-id"] = compose_id
        details["distro"] = distro
        details["suite-name"] = suite_name
        details["suite-file"] = suite_files
        details["conf-file"] = glb_file
        details["ceph-version-name"] = ceph_name
        details["duration"] = "0s"
        details["status"] = "Not Executed"
        details["comments"] = var.get("comments", str())
        details["jenkin-url"] = jenkin_job_url
        details["cloud-type"] = cloud_type
        details["instance-name"] = instances_name
        details["invoked-by"] = trigger_user
        return details

    if reuse is None:
        try:
            ceph_cluster_dict, clients = create_nodes(
                conf,
                inventory,
                osp_cred,
                run_id,
                cloud_type,
                instances_name,
                enable_eus=enable_eus,
            )
        except Exception as err:
            log.error(err)
            tests = suite.get("tests")
            res = []
            for test in tests:
                test = test.get("test")
                tmp = fetch_test_details(test)
                res.append(tmp)
            run_end_time = datetime.datetime.now()
            duration = divmod((run_end_time - run_start_time).total_seconds(), 60)
            total_time = {
                "start": run_start_time.strftime("%d %B %Y , %I:%M:%S %p"),
                "end": run_end_time.strftime("%d %B %Y , %I:%M:%S %p"),
                "total": f"{int(duration[0])} mins, {int(duration[1])} secs",
            }
            send_to_cephci = post_results or post_to_report_portal
            info = {
                "status": "Fail",
                "trace": (traceback.format_exc(limit=2)).split("\n"),
            }
            test_res = {
                "result": res,
                "run_id": run_id,
                "trigger_user": trigger_user,
                "run_directory": run_dir,
                "total_time": total_time,
                "info": info,
                "send_to_cephci": send_to_cephci,
                "prefix": instances_name,
            }
            email_results(test_result=test_res)
            return 1
    else:
        ceph_store_nodes = open(reuse, "rb")
        ceph_cluster_dict = pickle.load(ceph_store_nodes)
        ceph_store_nodes.close()
        for cluster_name, cluster in ceph_cluster_dict.items():
            for node in cluster:
                node.reconnect()
    if store:
        ceph_clusters_file = f"rerun/{instances_name}-{run_id}"
        if not os.path.exists(os.path.dirname(ceph_clusters_file)):
            os.makedirs(os.path.dirname(ceph_clusters_file))
        store_cluster_state(ceph_cluster_dict, ceph_clusters_file)

    sys.path.append(os.path.abspath("tests"))
    sys.path.append(os.path.abspath("tests/rados"))
    sys.path.append(os.path.abspath("tests/cephadm"))
    sys.path.append(os.path.abspath("tests/rbd"))
    sys.path.append(os.path.abspath("tests/rbd/rest"))
    sys.path.append(os.path.abspath("tests/rbd_mirror"))
    sys.path.append(os.path.abspath("tests/cephfs"))
    sys.path.append(os.path.abspath("tests/iscsi"))
    sys.path.append(os.path.abspath("tests/rgw"))
    sys.path.append(os.path.abspath("tests/ceph_ansible"))
    sys.path.append(os.path.abspath("tests/ceph_installer"))
    sys.path.append(os.path.abspath("tests/mgr"))
    sys.path.append(os.path.abspath("tests/dashboard"))
    sys.path.append(os.path.abspath("tests/misc_env"))
    sys.path.append(os.path.abspath("tests/parallel"))
    sys.path.append(os.path.abspath("tests/upgrades"))
    sys.path.append(os.path.abspath("tests/ceph_volume"))
    sys.path.append(os.path.abspath("tests/nvmeof"))
    sys.path.append(os.path.abspath("tests/nvmeof/rest"))
    sys.path.append(os.path.abspath("tests/nfs"))
    sys.path.append(os.path.abspath("tests/smb"))
    sys.path.append(os.path.abspath("tests/upstream/nfs"))

    tests = suite.get("tests")
    tcs = []
    jenkins_rc = 0
    _rhcs_version = rhbuild[:3]
    # use ceph_test_data to pass around dynamic data between tests
    ceph_test_data = dict()
    ceph_test_data["custom-config"] = custom_config
    ceph_test_data["custom-config-file"] = custom_config_file

    # Initialize test return code
    rc = 0
    run_config = {
        "log_dir": run_dir,
        "run_id": run_id,
    }
    download_path = run_dir if not log_directory else log_directory
    cluster_info = []

    for test in tests:
        test = test.get("test")
        tc = fetch_test_details(test)
        do_not_skip_test = test.get("do-not-skip-tc", False)
        test_file = tc["file"]
        unique_test_name = create_unique_test_name(tc["name"], test_names)
        test_names.append(unique_test_name)

        tc["log-link"] = log.configure_logger(
            unique_test_name, run_dir, disable_console_log
        )
        run_config.update({"test_name": unique_test_name, "log_link": tc["log-link"]})
        mod_file_name = os.path.splitext(test_file)[0]
        test_mod = importlib.import_module(mod_file_name)
        print("\nRunning test: {test_name}".format(test_name=tc["name"]))

        if tc.get("log-link"):
            print("Test logfile location: {log_url}".format(log_url=tc["log-link"]))

        log.info(f"Running test {test_file}")
        start = datetime.datetime.now()

        for cluster_name in test.get("clusters", ceph_cluster_dict):
            # Add cluster names
            if cluster_name not in cluster_info:
                cluster_info.append(cluster_name)

            # If Performance and CPU usage monitoring is enabled, perform pre-reqs
            if enable_perf_mon:
                if not upload_mem_and_cpu_logger_script(
                    ceph_cluster_dict[cluster_name]
                ):
                    log.error(
                        "Failed to upload Memory and CPU monitoring scripts to nodes. "
                        "The tests will proceed without monitoring"
                    )
                    enable_perf_mon = False

            if test.get("clusters"):
                config = test.get("clusters").get(cluster_name).get("config", {})
            else:
                config = test.get("config", {})
            parallel = test.get("parallel", [])

            if not config.get("base_url"):
                config["base_url"] = base_url

            config["rhbuild"] = f"{rhbuild}-{platform}"
            config["cloud-type"] = cloud_type
            if "ubuntu_repo" in locals():
                config["ubuntu_repo"] = ubuntu_repo

            if skip_setup is True:
                config["skip_setup"] = True

            if skip_subscription is True:
                config["skip_subscription"] = True

            if config.get("skip_version_compare"):
                skip_version_compare = config.get("skip_version_compare")

            if args.get("--add-repo"):
                repo = args.get("--add-repo")
                if repo.startswith("http"):
                    config["add-repo"] = repo

            config["build_type"] = build
            config["enable_eus"] = enable_eus
            config["skip_enabling_rhel_rpms"] = skip_enabling_rhel_rpms
            config["docker-insecure-registry"] = docker_insecure_registry
            config["skip_version_compare"] = skip_version_compare
            config["container_image"] = "%s/%s:%s" % (
                docker_registry,
                docker_image,
                docker_tag,
            )

            if custom_config:
                for _config in custom_config:
                    if "ibm-build=" in _config:
                        config["ibm_build"] = bool(_config.split("=")[1])

                    if "enable-fips-mode=" in _config:
                        config["enable_fips_mode"] = bool(_config.split("=")[1])

            config["ceph_docker_registry"] = docker_registry
            config["ceph_docker_image"] = docker_image
            config["ceph_docker_image_tag"] = docker_tag

            if filestore:
                config["filestore"] = filestore

            if ec_pool_vals:
                config["ec-pool-k-m"] = ec_pool_vals

            if args.get("--hotfix-repo"):
                hotfix_repo = args.get("--hotfix-repo")
                if hotfix_repo.startswith("http"):
                    config["hotfix_repo"] = hotfix_repo

            if kernel_repo is not None:
                config["kernel-repo"] = kernel_repo

            if osp_cred:
                config["osp_cred"] = osp_cred

            # if Kernel Repo is defined in ENV then set the value in config
            if os.environ.get("KERNEL-REPO-URL") is not None:
                config["kernel-repo"] = os.environ.get("KERNEL-REPO-URL")

            # Start performance and Cpu usage monitoring
            if enable_perf_mon:
                logging_process, tracker = start_logging_processes(
                    ceph_cluster_dict[cluster_name], unique_test_name
                )
            try:
                if "build" in config.keys():
                    _rhcs_version = config["build"]

                # Initialize the cluster with the expected rhcs_version
                ceph_cluster_dict[cluster_name].rhcs_version = _rhcs_version
                if mod_file_name not in skip_tc_list or do_not_skip_test:
                    if parallel:
                        parallel_tcs, rc = test_mod.run(
                            ceph_cluster=ceph_cluster_dict[cluster_name],
                            ceph_nodes=ceph_cluster_dict[cluster_name],
                            config=config,
                            parallel=parallel,
                            test_data=ceph_test_data,
                            ceph_cluster_dict=ceph_cluster_dict,
                            clients=clients,
                            run_config=run_config,
                            tc=tc,
                        )
                        tcs.extend(parallel_tcs)
                    else:
                        rc = test_mod.run(
                            ceph_cluster=ceph_cluster_dict[cluster_name],
                            ceph_nodes=ceph_cluster_dict[cluster_name],
                            config=config,
                            parallel=parallel,
                            test_data=ceph_test_data,
                            ceph_cluster_dict=ceph_cluster_dict,
                            clients=clients,
                            run_config=run_config,
                        )
                else:
                    rc = -1

            except BaseException as be:  # noqa
                # Log exception to stdout
                log.exception(be)

                # Set failure details
                tc["err_type"] = "exception"
                tc["err_msg"] = str(be)
                tc["err_text"] = traceback.format_exc()

                # Set return code to 1
                rc = 1

            finally:
                # Stop performance and Cpu usage monitoring
                if enable_perf_mon:
                    stop_logging_process(
                        ceph_cluster_dict[cluster_name],
                        logging_process,
                        download_path,
                        tracker,
                    )
                collect_recipe(ceph_cluster_dict[cluster_name])
                if store:
                    store_cluster_state(ceph_cluster_dict, ceph_clusters_file)

                # Artifacts from test appended to comments
                if config.get("artifacts"):
                    tc["comments"] += f"\n{config['artifacts']}"

            # Check for Log object
            _objects, _object = vars(test_mod), None
            for k in _objects.keys():
                if type(_objects.get(k)) is Log:
                    _object = _objects.get(k)
                    break

            if rc != 0:
                # Check if err_type is set for exception
                if _object and not (tc.get("err_type") == "exception"):
                    tc["err_type"], tc["err_msg"] = "error", ""

                    # Get error messages
                    tc["err_msg"] = "\n".join(map(str, _object._log_errors))

                break

        # Calculate test execution time
        elapsed = datetime.datetime.now() - start
        tc["duration"] = elapsed

        # Reset errors list
        if _object:
            _object._log_errors = []

        if rc == 0:
            tc["status"] = "Pass"
            msg = "Test {} passed".format(test_mod)
            log.info(msg)
            print(msg)

            if post_results:
                post_to_polarion(tc=tc)

        elif rc == -1:
            tc["status"] = "Skipped"
            msg = "Test {} Skipped".format(test_mod)
            log.info(msg)
            print(msg)

            if post_results:
                post_to_polarion(tc=tc)

        else:
            tc["status"] = "Failed"
            msg = "Test {} failed".format(test_mod)
            log.info(msg)
            print(msg)
            jenkins_rc = 1

            if post_results:
                post_to_polarion(tc=tc)

            if test.get("abort-on-fail", False):
                log.info("Aborting on test failure")
                tcs.append(tc)
                break

        if test.get("destroy-cluster") is True:
            if cloud_type == "openstack":
                cleanup_ceph_nodes(osp_cred, instances_name)
            elif cloud_type == "ibmc":
                cleanup_ibmc_ceph_nodes(osp_cred, instances_name)

        if test.get("recreate-cluster") is True:
            ceph_cluster_dict, clients = create_nodes(
                conf,
                inventory,
                osp_cred,
                run_id,
                cloud_type,
                service,
                instances_name,
                enable_eus=enable_eus,
            )

        tcs.append(tc)

    url_base = (
        magna_url + run_dir.split("/")[-1]
        if "/ceph/cephci-jenkins" in run_dir
        else run_dir
    )
    log.info("\nAll test logs located here: {base}".format(base=url_base))

    log.close_and_remove_filehandlers()

    test_run_metadata = {
        "jenkin-url": jenkin_job_url,
        "build": rhbuild,
        "ceph-version": ceph_version,
        "ceph-ansible-version": ceph_ansible_version,
        "base_url": base_url,
        "suite-name": suite_name,
        "conf-file": glb_file,
        "polarion-project-id": "CEPH",
        "distro": distro,
        "container-registry": docker_registry,
        "container-image": docker_image,
        "container-tag": docker_tag,
        "compose-id": compose_id,
        "log-dir": run_dir,
        "run-id": run_id,
        "cloud-type": cloud_type,
        "instance-name": instances_name,
        "invoked-by": trigger_user,
    }

    if xunit_results:
        create_xunit_results(suite_name, tcs, test_run_metadata)

    print("\nAll test logs located here: {base}".format(base=url_base))
    print_results(tcs)
    send_to_cephci = post_results  # or post_to_report_portal
    run_end_time = datetime.datetime.now()
    duration = divmod((run_end_time - run_start_time).total_seconds(), 60)
    total_time = {
        "start": run_start_time.strftime("%d %B %Y , %I:%M:%S %p"),
        "end": run_end_time.strftime("%d %B %Y , %I:%M:%S %p"),
        "total": f"{int(duration[0])} mins, {int(duration[1])} secs",
    }
    info = {"status": "Pass"}
    with open(f"{run_dir}/run_summary.json", "w", encoding="utf-8") as f:
        json.dump(run_summary, f, ensure_ascii=False, indent=4)
    test_res = {
        "result": tcs,
        "run_id": run_id,
        "trigger_user": trigger_user,
        "run_directory": run_dir,
        "total_time": total_time,
        "info": info,
        "send_to_cephci": send_to_cephci,
        "cluster_info": cluster_info,
        "prefix": instances_name,
    }

    email_results(test_result=test_res)

    if jenkins_rc and not skip_sos_report:
        log.info(
            "\n\nGenerating sosreports for all the nodes due to failures in testcase"
        )
        for cluster in ceph_cluster_dict.keys():
            log.info(f"Installing Ceph-common on {cluster} nodes to gather Sos report")
            for node in ceph_cluster_dict[cluster].get_nodes():
                setup_cluster_access(ceph_cluster_dict[cluster], node)
            installer = ceph_cluster_dict[cluster].get_nodes(role="installer")[0]
            sosreport.run(installer.ip_address, "cephuser", "cephuser", run_dir)
            # This can be Removed as sos report will have this details as well
            get_ceph_var_logs(ceph_cluster_dict[cluster], run_dir)
        log.info(f"Generated sosreports location : {url_base}/sosreports\n")

    return jenkins_rc


def store_cluster_state(ceph_cluster_object, ceph_clusters_file_name):
    cn = open(ceph_clusters_file_name, "w+b")
    pickle.dump(ceph_cluster_object, cn)
    cn.close()
    log.info("ceph_clusters_file %s", ceph_clusters_file_name)


def collect_recipe(ceph_cluster):
    """
    Gather the system under test details.

    At present, the following information are gathered
        container (podman/docker)   version
        ceph                        Deployed Ceph version

    Args:
        ceph_cluster:   Cluster participating in the test.

    Returns:
        None
    """
    version_datails = {}
    installer_node = ceph_cluster.get_ceph_objects("installer")
    client_node = ceph_cluster.get_ceph_objects("client")
    out, rc = installer_node[0].exec_command(
        sudo=True, cmd="podman --version | awk {'print $3'}", check_ec=False
    )
    output = out.rstrip()
    if output:
        log.info(f"Podman Version {output}")
        version_datails["PODMAN"] = output

    out, rc = installer_node[0].exec_command(
        sudo=True, cmd="docker --version | awk {'print $3'}", check_ec=False
    )
    output = out.rstrip()
    if output:
        log.info(f"Docker Version {output}")
        version_datails["DOCKER"] = output

    if client_node:
        out, rc = client_node[0].exec_command(
            sudo=True, cmd="ceph --version | awk '{print $3}'", check_ec=False
        )
        output = out.rstrip()
        log.info(f"ceph Version {output}")
        version_datails["CEPH"] = output

    version_detail = open("version_info.json", "w+")
    json.dump(version_datails, version_detail)
    version_detail.close()


if __name__ == "__main__":
    args = docopt(doc)
    rc = run(args)
    log.info("final rc of test run %d" % rc)
    sys.exit(rc)
