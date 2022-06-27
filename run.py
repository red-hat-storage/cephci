#!/usr/bin/env python

# Allow parallelized behavior of gevent. It has to be the first line.
from gevent import monkey

monkey.patch_all()

import datetime
import importlib
import json
import logging
import os
import pickle
import re
import sys
import textwrap
import time
import traceback
from copy import deepcopy
from getpass import getuser
from typing import Optional

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
from utility import sosreport
from utility.config import TestMetaData
from utility.log import Log
from utility.polarion import post_to_polarion
from utility.retry import retry
from utility.utils import (
    ReportPortal,
    close_and_remove_filehandlers,
    configure_logger,
    create_run_dir,
    create_unique_test_name,
    email_results,
    fetch_build_artifacts,
    generate_unique_id,
    magna_url,
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
        [--report-portal]
        [--upstream-build <upstream-build>]
        [--log-level <LEVEL>]
        [--log-dir  <directory-name>]
        [--instances-name <name>]
        [--osp-image <image>]
        [--filestore]
        [--use-ec-pool <k,m>]
        [--hotfix-repo <repo>]
        [--ignore-latest-container]
        [--skip-version-compare]
        [--custom-config <key>=<value>]...
        [--custom-config-file <file>]
        [--xunit-results]
        [--enable-eus]
        [--skip-enabling-rhel-rpms]
        [--skip-sos-report]
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
  --build <latest>                  eg: latest|tier-0|tier-1|tier-2|cvp|released|upstream
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
  --report-portal                   Post results to report portal. Requires config file,
                                    see README.
  --log-level <LEVEL>               Set logging level
  --log-dir <LEVEL>                 Set log directory
  --instances-name <name>           Name that will be used for instances creation
  --osp-image <image>               Image for osp instances, default value is taken from
                                    conf file
  --filestore                       To specify filestore as osd object store
  --use-ec-pool <k,m>               To use ec pools instead of replicated pools
  --hotfix-repo <repo>              To run sanity on hotfix build
  --ignore-latest-container         Skip getting latest nightly container
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
"""
log = Log()
test_names = []


@retry(LibcloudError, tries=5, delay=15)
def create_nodes(
    conf,
    inventory,
    osp_cred,
    run_id,
    cloud_type="openstack",
    report_portal_session=None,
    instances_name=None,
    enable_eus=False,
    rp_logger: Optional[ReportPortal] = None,
):
    """Creates the system under test environment."""
    if rp_logger:
        name = create_unique_test_name("ceph node creation", test_names)
        test_names.append(name)
        desc = "Ceph cluster preparation"
        rp_logger.start_test_item(name=name, description=desc, item_type="STEP")

    log.info("Destroying existing osp instances..")
    if cloud_type == "openstack":
        cleanup_ceph_nodes(osp_cred, instances_name)
    elif cloud_type == "ibmc":
        cleanup_ibmc_ceph_nodes(osp_cred, instances_name)

    ceph_cluster_dict = {}

    log.info("Creating osp instances")
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
            elif "baremetal" in cloud_type:
                private_key_path = node.private_key if node.private_key else ""
                private_ip = node.ip_address
                look_for_key = True if node.private_key else False
                root_password = node.root_password
            elif cloud_type == "ibmc":
                glbs = osp_cred.get("globals")
                ibmc = glbs.get("ibm-credentials")
                private_key_path = ibmc.get("private_key_path")
                private_ip = node.ip_address
                look_for_key = True

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
                if rp_logger:
                    rp_logger.finish_test_item(status="FAILED")
                raise

    if rp_logger:
        rp_logger.finish_test_item(status="PASSED")

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
    reuse = args.get("--reuse", None)
    cloud_type = args.get("--cloud", "openstack")

    # These are not mandatory options
    inventory_file = args.get("--inventory")
    osp_cred_file = args.get("--osp-cred")

    osp_cred = load_file(osp_cred_file) if osp_cred_file else dict()
    cleanup_name = args.get("--cleanup", None)

    ignore_latest_nightly_container = args.get("--ignore-latest-container", False)

    # Set log directory and get absolute path
    console_log_level = args.get("--log-level")
    log_directory = args.get("--log-dir")
    post_to_report_portal = args.get("--report-portal", False)

    run_id = generate_unique_id(length=6)
    rp_logger = None
    if post_to_report_portal:
        rp_logger = ReportPortal()
    TestMetaData(run_id=run_id, rhbuild=rhbuild, rhcs="rhcs", rp_logger=rp_logger)
    run_dir = create_run_dir(run_id, log_directory)
    startup_log = os.path.join(run_dir, "startup.log")

    handler = logging.FileHandler(startup_log)
    log.logger.addHandler(handler)

    if console_log_level:
        log.logger.setLevel(console_log_level.upper())

    log.info(f"Startup log location: {startup_log}")
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
    build = args.get("--build", None)

    if build and build not in ["released"]:
        base_url, docker_registry, docker_image, docker_tag = fetch_build_artifacts(
            build, rhbuild, platform, args.get("--upstream-build", None)
        )

    store = args.get("--store", False)

    base_url = args.get("--rhs-ceph-repo") or base_url
    ubuntu_repo = args.get("--ubuntu-repo") or ubuntu_repo
    docker_registry = args.get("--docker-registry") or docker_registry
    docker_image = args.get("--docker-image") or docker_image
    docker_tag = args.get("--docker-tag") or docker_tag
    kernel_repo = args.get("--kernel-repo", None)

    docker_insecure_registry = args.get("--insecure-registry", False)

    post_results = args.get("--post-results")
    skip_setup = args.get("--skip-cluster", False)
    skip_subscription = args.get("--skip-subscription", False)

    instances_name = args.get("--instances-name")
    if instances_name:
        instances_name = instances_name.replace(".", "-")

    osp_image = args.get("--osp-image")
    filestore = args.get("--filestore", False)
    ec_pool_vals = args.get("--use-ec-pool", None)
    skip_version_compare = args.get("--skip-version-compare", False)
    custom_config = args.get("--custom-config")
    custom_config_file = args.get("--custom-config-file")
    xunit_results = args.get("--xunit-results", False)

    enable_eus = args.get("--enable-eus", False)
    skip_enabling_rhel_rpms = args.get("--skip-enabling-rhel-rpms", False)
    skip_sos_report = args.get("--skip-sos-report", False)

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
    ceph_version = ", ".join(list(set(ceph_version)))
    ceph_ansible_version = ", ".join(list(set(ceph_ansible_version)))

    log.info(f"Compose id is: {compose_id}")
    log.info(f"Testing Ceph Version: {ceph_version}")
    log.info(f"Testing Ceph Ansible Version: {ceph_ansible_version}")

    service = None
    suite_name = "::".join(suite_files)
    if post_to_report_portal:
        log.info("Creating report portal session")

        # Only the first file is considered for launch description.
        suite_file_name = suite_name.split("::")[0].split("/")[-1]
        suite_file_name = suite_file_name.strip(".yaml")
        suite_file_name = " ".join(suite_file_name.split("_"))
        _log = run_dir.replace("/ceph/", "http://magna002.ceph.redhat.com/")

        launch_name = f"RHCS {rhbuild} - {suite_file_name}"
        launch_desc = textwrap.dedent(
            """
            ceph version: {ceph_version}
            ceph-ansible version: {ceph_ansible_version}
            compose-id: {compose_id}
            invoked-by: {user}
            log-location: {_log}
            """.format(
                ceph_version=ceph_version,
                ceph_ansible_version=ceph_ansible_version,
                user=getuser(),
                compose_id=compose_id,
                _log=_log,
            )
        )
        if docker_image and docker_registry and docker_tag:
            launch_desc = launch_desc + textwrap.dedent(
                """
                docker registry: {docker_registry}
                docker image: {docker_image}
                docker tag: {docker_tag}
                invoked-by: {user}
                """.format(
                    docker_registry=docker_registry,
                    docker_image=docker_image,
                    user=getuser(),
                    docker_tag=docker_tag,
                )
            )

        qe_tier = get_tier_level(suite_name)
        attributes = dict(
            {
                "rhcs": rhbuild,
                "tier": qe_tier,
                "ceph_version": ceph_version,
                "os": platform if platform else "-".join(rhbuild.split("-")[1:]),
            }
        )

        rp_logger.start_launch(
            name=launch_name, description=launch_desc, attributes=attributes
        )

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
        return details

    if reuse is None:
        try:
            ceph_cluster_dict, clients = create_nodes(
                conf,
                inventory,
                osp_cred,
                run_id,
                cloud_type,
                service,
                instances_name,
                enable_eus=enable_eus,
                rp_logger=rp_logger,
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

    tests = suite.get("tests")
    tcs = []
    jenkins_rc = 0
    # use ceph_test_data to pass around dynamic data between tests
    ceph_test_data = dict()
    ceph_test_data["custom-config"] = custom_config
    ceph_test_data["custom-config-file"] = custom_config_file

    # Initialize test return code
    rc = 0

    for test in tests:
        test = test.get("test")
        parallel = test.get("parallel")
        tc = fetch_test_details(test)
        test_file = tc["file"]
        report_portal_description = tc["desc"] or ""
        unique_test_name = create_unique_test_name(tc["name"], test_names)
        test_names.append(unique_test_name)

        tc["log-link"] = configure_logger(unique_test_name, run_dir)

        mod_file_name = os.path.splitext(test_file)[0]
        test_mod = importlib.import_module(mod_file_name)
        print("\nRunning test: {test_name}".format(test_name=tc["name"]))

        if tc.get("log-link"):
            print("Test logfile location: {log_url}".format(log_url=tc["log-link"]))

        log.info(f"Running test {test_file}")
        start = datetime.datetime.now()

        for cluster_name in test.get("clusters", ceph_cluster_dict):
            if test.get("clusters"):
                config = test.get("clusters").get(cluster_name).get("config", {})
            else:
                config = test.get("config", {})

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

            config["ceph_docker_registry"] = docker_registry
            report_portal_description += f"docker registry: {docker_registry}"
            config["ceph_docker_image"] = docker_image
            report_portal_description += f"docker image: {docker_image}"
            config["ceph_docker_image_tag"] = docker_tag
            report_portal_description += f"docker registry: {docker_registry}"

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

            try:
                if post_to_report_portal:
                    rp_logger.start_test_item(
                        name=unique_test_name,
                        description=report_portal_description,
                        item_type="STEP",
                    )
                    rp_logger.log(message=f"Logfile location - {tc['log-link']}")
                    rp_logger.log(message=f"Polarion ID: {tc['polarion-id']}")

                # Initialize the cluster with the expected rhcs_version hence the
                # precedence would be from test suite.
                # rhbuild would start with the version for example 5.0 or 4.2-rhel-7
                _rhcs_version = test.get("ceph_rhcs_version", rhbuild[:3])
                ceph_cluster_dict[cluster_name].rhcs_version = _rhcs_version

                rc = test_mod.run(
                    ceph_cluster=ceph_cluster_dict[cluster_name],
                    ceph_nodes=ceph_cluster_dict[cluster_name],
                    config=config,
                    parallel=parallel,
                    test_data=ceph_test_data,
                    ceph_cluster_dict=ceph_cluster_dict,
                    clients=clients,
                )
            except BaseException as be:  # noqa
                log.exception(be)
                rc = 1
            finally:
                collect_recipe(ceph_cluster_dict[cluster_name])
                if store:
                    store_cluster_state(ceph_cluster_dict, ceph_clusters_file)

            if rc != 0:
                break

        elapsed = datetime.datetime.now() - start
        tc["duration"] = elapsed

        # Write to report portal
        if post_to_report_portal:
            rp_logger.finish_test_item(status="PASSED" if rc == 0 else "FAILED")

        if rc == 0:
            tc["status"] = "Pass"
            msg = "Test {} passed".format(test_mod)
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

    close_and_remove_filehandlers()

    test_run_metadata = {
        "build": rhbuild,
        "polarion-project-id": "CEPH",
        "suite-name": suite_name,
        "distro": distro,
        "ceph-version": ceph_version,
        "ceph-ansible-version": ceph_ansible_version,
        "base_url": base_url,
        "container-registry": docker_registry,
        "container-image": docker_image,
        "container-tag": docker_tag,
        "compose-id": compose_id,
        "log-dir": run_dir,
        "run-id": run_id,
    }

    if post_to_report_portal:
        rp_logger.finish_launch()

    if xunit_results:
        create_xunit_results(suite_name, tcs, test_run_metadata)

    print("\nAll test logs located here: {base}".format(base=url_base))
    print_results(tcs)
    send_to_cephci = post_results or post_to_report_portal
    run_end_time = datetime.datetime.now()
    duration = divmod((run_end_time - run_start_time).total_seconds(), 60)
    total_time = {
        "start": run_start_time.strftime("%d %B %Y , %I:%M:%S %p"),
        "end": run_end_time.strftime("%d %B %Y , %I:%M:%S %p"),
        "total": f"{int(duration[0])} mins, {int(duration[1])} secs",
    }
    info = {"status": "Pass"}
    test_res = {
        "result": tcs,
        "run_id": run_id,
        "trigger_user": trigger_user,
        "run_directory": run_dir,
        "total_time": total_time,
        "info": info,
        "send_to_cephci": send_to_cephci,
    }

    email_results(test_result=test_res)

    if jenkins_rc and not skip_sos_report:
        log.info(
            "\n\nGenerating sosreports for all the nodes due to failures in testcase"
        )
        for cluster in ceph_cluster_dict.keys():
            installer = ceph_cluster_dict[cluster].get_nodes(role="installer")[0]
            sosreport.run(installer.ip_address, "cephuser", "cephuser", run_dir)
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
