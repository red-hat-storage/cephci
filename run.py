#!/usr/bin/env python
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
from getpass import getuser

import requests
import yaml
from docopt import docopt
from libcloud.common.types import LibcloudError

from ceph.ceph import Ceph, CephNode
from ceph.clients import WinNode
from ceph.utils import cleanup_ceph_nodes, create_ceph_nodes
from utility.polarion import post_to_polarion
from utility.retry import retry
from utility.utils import (
    close_and_remove_filehandlers,
    configure_logger,
    create_report_portal_session,
    create_run_dir,
    create_unique_test_name,
    email_results,
    get_latest_container,
    timestamp,
)
from utility.xunit import create_xunit_results

doc = """
A simple test suite wrapper that executes tests based on yaml test configuration

 Usage:
  run.py --rhbuild BUILD --global-conf FILE --inventory FILE --suite FILE
        [--osp-cred <file>]
        [--rhs-con-repo <repo> --rhs-ceph-repo <repo>]
        [ --ubuntu-repo <repo>]
        [--add-repo <repo>]
        [--kernel-repo <repo>]
        [--store]
        [--reuse <file>]
        [--skip-cluster]
        [--skip-subscription]
        [--docker-registry <registry>]
        [--docker-image <image>]
        [--docker-tag <tag>]
        [--insecure-registry]
        [--post-results]
        [--report-portal]
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
  run.py --cleanup=name [--osp-cred <file>]
        [--log-level <LEVEL>]

Options:
  -h --help                         show this screen
  -v --version                      run version
  -s <smoke> --suite <smoke>        test suite to run
                                    eg: -s smoke or -s rbd
  -f <tests> --filter <tests>       filter tests based on the patter
                                    eg: -f 'rbd' will run tests that have 'rbd'
  --global-conf <file>              global cloud configuration file
  --inventory <file>                hosts inventory file
  --osp-cred <file>                 openstack credentials as separate file
  --rhbuild <1.3.0>                 ceph downstream version
                                    eg: 1.3.0, 2.0, 2.1 etc
  --rhs-con-repo <repo>             location of rhs console repo
                                    Top level location of console compose
  --rhs-ceph-repo <repo>            location of rhs-ceph repo
                                    Top level location of compose
  --add-repo <repo>                 Any additional repo's need to be enabled
  --ubuntu-repo <repo>              http location of downstream ubuntu repo
  --kernel-repo <repo>              Zstream Kernel Repo location
  --store                           store the current vm state for reuse
  --reuse <file>                    use the stored vm state for rerun
  --skip-cluster                    skip cluster creation from ansible/ceph-deploy
  --skip-subscription               skip subscription manager if using beta rhel images
  --docker-registry <registry>      Docker registry, default value is taken from ansible config
  --docker-image <image>            Docker image, default value is taken from ansible config
  --docker-tag <tag>                Docker tag, default value is 'latest'
  --insecure-registry               Disable security check for docker registry
  --post-results                    Post results to polarion, needs Polarion IDs
                                    in test suite yamls. Requires config file, see README.
  --report-portal                   Post results to report portal. Requires config file, see README.
  --log-level <LEVEL>               Set logging level
  --log-dir <LEVEL>                 Set log directory [default: /tmp]
  --instances-name <name>           Name that will be used for instances creation
  --osp-image <image>               Image for osp instances, default value is taken from conf file
  --filestore                       To specify filestore as osd object store
  --use-ec-pool <k,m>               To use ec pools instead of replicated pools
  --hotfix-repo <repo>              To run sanity on hotfix build
  --ignore-latest-container         Skip getting latest nightly container
  --skip-version-compare            Skip verification that ceph versions change post upgrade
  -c --custom-config <name>=<value> Add a custom config key/value to ceph_conf_overrides
  --custom-config-file <file>       Add custom config yaml to ceph_conf_overrides
  --xunit-results                   Create xUnit result file for test suite run [default: false]
"""
log = logging.getLogger(__name__)
root = logging.getLogger()
root.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)
root.addHandler(ch)

test_names = []


@retry(LibcloudError, tries=5, delay=15)
def create_nodes(
    conf, inventory, osp_cred, run_id, report_portal_session=None, instances_name=None
):
    if report_portal_session:
        name = create_unique_test_name("ceph node creation", test_names)
        test_names.append(name)
        desc = "Ceph cluster preparation"
        report_portal_session.start_test_item(
            name=name, description=desc, start_time=timestamp(), item_type="STEP"
        )
    log.info("Destroying existing osp instances..")
    cleanup_ceph_nodes(osp_cred, instances_name)
    ceph_cluster_dict = {}
    log.info("Creating osp instances")
    for cluster in conf.get("globals"):
        ceph_vmnodes = create_ceph_nodes(
            cluster, inventory, osp_cred, run_id, instances_name
        )
        ceph_nodes = []
        clients = []
        for node in ceph_vmnodes.values():
            if node.role == "win-iscsi-clients":
                clients.append(
                    WinNode(
                        ip_address=node.ip_address, private_ip=node.get_private_ip()
                    )
                )
            else:
                ceph = CephNode(
                    username="cephuser",
                    password="cephuser",
                    root_password="passwd",
                    root_login=node.root_login,
                    role=node.role,
                    no_of_volumes=node.no_of_volumes,
                    ip_address=node.ip_address,
                    subnet=node.subnet,
                    private_ip=node.get_private_ip(),
                    hostname=node.hostname,
                    ceph_vmnode=node,
                )
                ceph_nodes.append(ceph)
        cluster_name = cluster.get("ceph-cluster").get("name", "ceph")
        ceph_cluster_dict[cluster_name] = Ceph(cluster_name, ceph_nodes)
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
                if report_portal_session:
                    report_portal_session.finish_test_item(
                        end_time=timestamp(), status="FAILED"
                    )
                raise
    if report_portal_session:
        report_portal_session.finish_test_item(end_time=timestamp(), status="PASSED")
    return ceph_cluster_dict, clients


def print_results(tc):
    header = "\n{name:<30s}   {desc:<60s}   {duration:<30s}   {status:>15s}".format(
        name="TEST NAME", desc="TEST DESCRIPTION", duration="DURATION", status="STATUS"
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
        line = f"{name:<30.30s}   {desc:<60.60s}   {dur:<30s}   {status:>15s}"
        print(line)


def run(args):
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    glb_file = args["--global-conf"]
    inventory_file = args["--inventory"]
    osp_cred_file = args["--osp-cred"]
    suite_file = args["--suite"]
    store = args.get("--store", False)
    reuse = args.get("--reuse", None)
    base_url = args.get("--rhs-ceph-repo", None)
    ubuntu_repo = args.get("--ubuntu-repo", None)
    kernel_repo = args.get("--kernel-repo", None)
    rhbuild = args.get("--rhbuild")
    docker_registry = args.get("--docker-registry", None)
    docker_image = args.get("--docker-image", None)
    docker_tag = args.get("--docker-tag", None)
    docker_insecure_registry = args.get("--insecure-registry", False)
    post_results = args.get("--post-results")
    skip_setup = args.get("--skip-cluster", False)
    skip_subscription = args.get("--skip-subscription", False)
    cleanup_name = args.get("--cleanup", None)
    post_to_report_portal = args.get("--report-portal", False)
    console_log_level = args.get("--log-level")
    log_directory = args.get("--log-dir", "/tmp")
    instances_name = args.get("--instances-name")
    if instances_name:
        instances_name = instances_name.replace(".", "-")
    osp_image = args.get("--osp-image")
    filestore = args.get("--filestore", False)
    ec_pool_vals = args.get("--use-ec-pool", None)
    ignore_latest_nightly_container = args.get("--ignore-latest-container", False)
    skip_version_compare = args.get("--skip-version-compare", False)
    custom_config = args.get("--custom-config")
    custom_config_file = args.get("--custom-config-file")
    xunit_results = args.get("--xunit-results", False)

    # Set log directory and get absolute path
    run_id = timestamp()
    run_dir = create_run_dir(run_id, log_directory)
    startup_log = os.path.join(run_dir, "startup.log")
    print("Startup log location: {}".format(startup_log))
    run_start_time = datetime.datetime.now()
    trigger_user = getuser()
    handler = logging.FileHandler(startup_log)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    root.addHandler(handler)

    if console_log_level:
        ch.setLevel(logging.getLevelName(console_log_level.upper()))

    if osp_cred_file:
        with open(osp_cred_file, "r") as osp_cred_stream:
            osp_cred = yaml.safe_load(osp_cred_stream)

    if cleanup_name is not None:
        cleanup_ceph_nodes(osp_cred, cleanup_name)
        return 0

    # Get ceph cluster version name
    with open("rhbuild.yaml") as fd:
        rhbuild_file = yaml.safe_load(fd)
    ceph = rhbuild_file["ceph"]
    ceph_name = None
    rhbuild_ = None
    try:
        ceph_name, rhbuild_ = next(
            filter(
                lambda x: x,
                [(ceph[x]["name"], x) for x in ceph if x == rhbuild.split(".")[0]],
            )
        )
    except StopIteration:
        print("\nERROR: Please provide correct RH build version, run exited.")
        sys.exit(1)

    # Get base-url
    composes = ceph[rhbuild_]["composes"]
    if not base_url:
        if rhbuild in composes:
            base_url = composes[rhbuild]["base_url"]
        else:
            base_url = composes["latest"]["base_url"]

    # Get ubuntu-repo
    if not ubuntu_repo and rhbuild.startswith("3"):
        if rhbuild in composes:
            ubuntu_repo = composes[rhbuild]["ubuntu_repo"]
        else:
            ubuntu_repo = composes["latest"]["ubuntu_repo"]

    if glb_file:
        conf_path = os.path.abspath(glb_file)
        with open(conf_path, "r") as conf_stream:
            conf = yaml.safe_load(conf_stream)

    if inventory_file:
        inventory_path = os.path.abspath(inventory_file)
        with open(inventory_path, "r") as inventory_stream:
            inventory = yaml.safe_load(inventory_stream)

    if suite_file:
        suites_path = os.path.abspath(suite_file)
        with open(suites_path, "r") as suite_stream:
            suite = yaml.safe_load(suite_stream)

    if osp_image and inventory.get("instance").get("create"):
        inventory.get("instance").get("create").update({"image-name": osp_image})

    compose_id = None

    if os.environ.get("TOOL") is not None:
        ci_message = json.loads(os.environ["CI_MESSAGE"])
        compose_id = ci_message["compose_id"]
        compose_url = ci_message["compose_url"] + "/"
        product_name = ci_message.get("product_name", None)
        product_version = ci_message.get("product_version", None)
        log.info("COMPOSE_URL = %s ", compose_url)
        if os.environ["TOOL"] == "pungi":
            # is a rhel compose
            log.info("trigger on CI RHEL Compose")
        elif os.environ["TOOL"] == "rhcephcompose":
            # is a ubuntu compose
            log.info("trigger on CI Ubuntu Compose")
            ubuntu_repo = compose_url
            log.info("using ubuntu repo" + ubuntu_repo)
        elif os.environ["TOOL"] == "bucko":
            # is a docker compose
            log.info("Trigger on CI Docker Compose")
            docker_registry, docker_tag = ci_message["repository"].split(
                "/rh-osbs/rhceph:"
            )
            docker_image = "rh-osbs/rhceph"
            log.info(
                "\nUsing docker registry from ci message: {registry} \nDocker image: {image}\nDocker tag:{tag}".format(
                    registry=docker_registry, image=docker_image, tag=docker_tag
                )
            )
            log.warning("Using Docker insecure registry setting")
            docker_insecure_registry = True
        if product_name == "ceph":
            # is a rhceph compose
            base_url = compose_url
            log.info("using base url" + base_url)

    image_name = inventory.get("instance").get("create").get("image-name")
    ceph_version = []
    ceph_ansible_version = []
    distro = []
    clients = []
    if inventory.get("instance").get("create"):
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
        id = requests.get(base_url + "/COMPOSE_ID")
        compose_id = id.text
        if "rhel" in image_name.lower():
            ceph_pkgs = requests.get(base_url + "/compose/Tools/x86_64/os/Packages/")
            m = re.search(r"ceph-common-(.*?).x86", ceph_pkgs.text)
            ceph_version.append(m.group(1))
            m = re.search(r"ceph-ansible-(.*?).rpm", ceph_pkgs.text)
            ceph_ansible_version.append(m.group(1))
            log.info("Compose id is: " + compose_id)
        else:
            ubuntu_pkgs = requests.get(
                ubuntu_repo + "/Tools/dists/xenial/main/binary-amd64/Packages"
            )
            m = re.search(r"ceph\nVersion: (.*)", ubuntu_pkgs.text)
            ceph_version.append(m.group(1))
            m = re.search(r"ceph-ansible\nVersion: (.*)", ubuntu_pkgs.text)
            ceph_ansible_version.append(m.group(1))

    distro = ",".join(list(set(distro)))
    ceph_version = ", ".join(list(set(ceph_version)))
    ceph_ansible_version = ", ".join(list(set(ceph_ansible_version)))
    log.info("Testing Ceph Version: " + ceph_version)
    log.info("Testing Ceph Ansible Version: " + ceph_ansible_version)

    if not os.environ.get("TOOL") and not ignore_latest_nightly_container:
        try:
            latest_container = get_latest_container(rhbuild)
        except ValueError:
            print(
                "\nERROR:No latest nightly container UMB msg at /ceph/cephci-jenkins/latest-rhceph-container-info/,"
                "specify using the cli args or use --ignore-latest-container"
            )
            sys.exit(1)
        docker_registry = (
            latest_container.get("docker_registry")
            if not docker_registry
            else docker_registry
        )
        docker_image = (
            latest_container.get("docker_image") if not docker_image else docker_image
        )
        docker_tag = (
            latest_container.get("docker_tag") if not docker_tag else docker_tag
        )
        log.info(
            "Using latest nightly docker image \nRegistry: {registry} \nDocker image: {image}\nDocker tag:{tag}".format(
                registry=docker_registry, image=docker_image, tag=docker_tag
            )
        )
        docker_insecure_registry = True
        log.warning("Using Docker insecure registry setting")

    service = None
    suite_name = os.path.basename(suite_file).split(".")[0]
    if post_to_report_portal:
        log.info("Creating report portal session")
        service = create_report_portal_session()
        launch_name = "{suite_name} ({distro})".format(
            suite_name=suite_name, distro=distro
        )
        launch_desc = textwrap.dedent(
            """
            ceph version: {ceph_version}
            ceph-ansible version: {ceph_ansible_version}
            compose-id: {compose_id}
            invoked-by: {user}
            """.format(
                ceph_version=ceph_version,
                ceph_ansible_version=ceph_ansible_version,
                user=getuser(),
                compose_id=compose_id,
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
        service.start_launch(
            name=launch_name, start_time=timestamp(), description=launch_desc
        )

    def fetch_test_details(var) -> dict:
        """Accepts the test and then provides the parameters of that test as a list
        :param var: the test collected from the suite file
        :return: Returns a dictionary of the various test params
        """
        details = dict()
        details["docker-containers-list"] = []
        details["name"] = var.get("name")
        details["desc"] = var.get("desc")
        details["file"] = var.get("module")
        details["polarion-id"] = var.get("polarion-id")
        polarion_default_url = "https://polarion.engineering.redhat.com/polarion/#/project/CEPH/workitem?id="
        details["polarion-id-link"] = "{}{}".format(
            polarion_default_url, details["polarion-id"]
        )
        details["rhbuild"] = rhbuild
        details["ceph-version"] = ceph_version
        details["ceph-ansible-version"] = ceph_ansible_version
        details["compose-id"] = compose_id
        details["distro"] = distro
        details["suite-name"] = suite_name
        details["suite-file"] = suite_file
        details["conf-file"] = glb_file
        details["ceph-version-name"] = ceph_name
        details["duration"] = "0s"
        details["status"] = "Not Executed"
        return details

    if reuse is None:
        try:
            ceph_cluster_dict, clients = create_nodes(
                conf, inventory, osp_cred, run_id, service, instances_name
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
        ceph_clusters_file = "rerun/ceph-snapshot-" + timestamp()
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

    tests = suite.get("tests")
    tcs = []
    jenkins_rc = 0
    # use ceph_test_data to pass around dynamic data between tests
    ceph_test_data = dict()
    ceph_test_data["custom-config"] = custom_config
    ceph_test_data["custom-config-file"] = custom_config_file

    for test in tests:
        test = test.get("test")
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
        log.info("Running test %s", test_file)
        start = datetime.datetime.now()
        for cluster_name in test.get("clusters", ceph_cluster_dict):
            if test.get("clusters"):
                config = test.get("clusters").get(cluster_name).get("config", {})
            else:
                config = test.get("config", {})
            if not config.get("base_url"):
                config["base_url"] = base_url
            config["rhbuild"] = rhbuild
            if "ubuntu_repo" in locals():
                config["ubuntu_repo"] = ubuntu_repo
            if skip_setup is True:
                config["skip_setup"] = True
            if skip_subscription is True:
                config["skip_subscription"] = True
            if args.get("--add-repo"):
                repo = args.get("--add-repo")
                if repo.startswith("http"):
                    config["add-repo"] = repo
            config["docker-insecure-registry"] = docker_insecure_registry
            config["skip_version_compare"] = skip_version_compare
            config["container_image"] = None
            if ignore_latest_nightly_container:
                config["container_image"] = "%s/%s:%s" % (
                    docker_registry,
                    docker_image,
                    docker_tag,
                )
            # For cdn container installation provide GAed container parameters
            # in test suite file as below, In case cdn is not enabled the latest
            # container details will be considered.
            #
            # config:
            #     use_cdn: True
            #     ansi_config:
            #       ceph_repository_type: cdn
            #       ceph_docker_image: "rhceph/rhceph-4-rhel8"
            #       ceph_docker_image_tag: "latest"
            #       ceph_docker_registry: "registry.redhat.io"

            if (
                config
                and config.get("ansi_config")
                and config.get("ansi_config").get("ceph_repository_type") != "cdn"
            ):
                if docker_registry:
                    config.get("ansi_config")["ceph_docker_registry"] = str(
                        docker_registry
                    )
                if docker_image:
                    config.get("ansi_config")["ceph_docker_image"] = str(docker_image)
                if docker_tag:
                    config.get("ansi_config")["ceph_docker_image_tag"] = str(docker_tag)
                cluster_docker_registry = config.get("ansi_config").get(
                    "ceph_docker_registry"
                )
                cluster_docker_image = config.get("ansi_config").get(
                    "ceph_docker_image"
                )
                cluster_docker_tag = config.get("ansi_config").get(
                    "ceph_docker_image_tag"
                )
                if cluster_docker_registry:
                    cluster_docker_registry = config.get("ansi_config").get(
                        "ceph_docker_registry"
                    )
                    report_portal_description = (
                        report_portal_description
                        + "\ndocker registry: {docker_registry}".format(
                            docker_registry=cluster_docker_registry
                        )
                    )
                if cluster_docker_image:
                    cluster_docker_image = config.get("ansi_config").get(
                        "ceph_docker_image"
                    )
                    report_portal_description = (
                        report_portal_description
                        + "\ndocker image: {docker_image}".format(
                            docker_image=cluster_docker_image
                        )
                    )
                if cluster_docker_tag:
                    cluster_docker_tag = config.get("ansi_config").get(
                        "ceph_docker_image_tag"
                    )
                    report_portal_description = (
                        report_portal_description
                        + "\ndocker tag: {docker_tag}".format(
                            docker_tag=cluster_docker_tag
                        )
                    )
                if cluster_docker_image and cluster_docker_registry:
                    tc["docker-containers-list"].append(
                        "{docker_registry}/{docker_image}:{docker_tag}".format(
                            docker_registry=cluster_docker_registry,
                            docker_image=cluster_docker_image,
                            docker_tag=cluster_docker_tag,
                        )
                    )
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
                    service.start_test_item(
                        name=unique_test_name,
                        description=report_portal_description,
                        start_time=timestamp(),
                        item_type="STEP",
                    )
                    service.log(
                        time=timestamp(),
                        message="Logfile location: {}".format(tc["log-link"]),
                        level="INFO",
                    )
                    service.log(
                        time=timestamp(),
                        message="Polarion ID: {}".format(tc["polarion-id"]),
                        level="INFO",
                    )
                rc = test_mod.run(
                    ceph_cluster=ceph_cluster_dict[cluster_name],
                    ceph_nodes=ceph_cluster_dict[cluster_name],
                    config=config,
                    test_data=ceph_test_data,
                    ceph_cluster_dict=ceph_cluster_dict,
                    clients=clients,
                )
            except BaseException:
                if post_to_report_portal:
                    service.log(
                        time=timestamp(), message=traceback.format_exc(), level="ERROR"
                    )
                log.error(traceback.format_exc())
                rc = 1
            finally:
                if store:
                    store_cluster_state(ceph_cluster_dict, ceph_clusters_file)
            if rc != 0:
                break
        elapsed = datetime.datetime.now() - start
        tc["duration"] = elapsed
        if rc == 0:
            tc["status"] = "Pass"
            msg = "Test {} passed".format(test_mod)
            log.info(msg)
            print(msg)
            if post_to_report_portal:
                service.finish_test_item(end_time=timestamp(), status="PASSED")
            if post_results:
                post_to_polarion(tc=tc)
        else:
            tc["status"] = "Failed"
            msg = "Test {} failed".format(test_mod)
            log.info(msg)
            print(msg)
            jenkins_rc = 1
            if post_to_report_portal:
                service.finish_test_item(end_time=timestamp(), status="FAILED")
            if post_results:
                post_to_polarion(tc=tc)
            if test.get("abort-on-fail", False):
                log.info("Aborting on test failure")
                tcs.append(tc)
                break
        if test.get("destroy-cluster") is True:
            cleanup_ceph_nodes(osp_cred, instances_name)
        if test.get("recreate-cluster") is True:
            ceph_cluster_dict, clients = create_nodes(
                conf, inventory, osp_cred, run_id, service, instances_name
            )
        tcs.append(tc)
    url_base = "http://magna002.ceph.redhat.com/cephci-jenkins"
    run_dir_name = run_dir.split("/")[-1]
    log.info(
        "\nAll test logs located here: {base}/{dir}".format(
            base=url_base, dir=run_dir_name
        )
    )
    close_and_remove_filehandlers()
    if post_to_report_portal:
        service.finish_launch(end_time=timestamp())
        service.terminate()
    if xunit_results:
        create_xunit_results(suite_name, tcs, run_dir)
    print(
        "\nAll test logs located here: {base}/{dir}".format(
            base=url_base, dir=run_dir_name
        )
    )
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
    return jenkins_rc


def store_cluster_state(ceph_cluster_object, ceph_clusters_file_name):
    cn = open(ceph_clusters_file_name, "w+b")
    pickle.dump(ceph_cluster_object, cn)
    cn.close()
    log.info("ceph_clusters_file %s", ceph_clusters_file_name)


if __name__ == "__main__":
    args = docopt(doc)
    rc = run(args)
    log.info("final rc of test run %d" % rc)
    sys.exit(rc)
