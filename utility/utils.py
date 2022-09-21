import datetime
import getpass
import logging
import os
import random
import re
import smtplib
import subprocess
import time
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from ipaddress import ip_address
from string import ascii_uppercase, digits
from typing import Dict, Optional, Tuple
from urllib import request

import requests
import yaml
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from jinja2 import Environment, FileSystemLoader, select_autoescape
from jinja_markdown import MarkdownExtension
from reportportal_client import ReportPortalService

from utility.log import Log

log = Log(__name__)

# variables
mounting_dir = "/mnt/cephfs/"
clients = []
md5sum_list1 = []
md5sum_list2 = []
fuse_clients = []
kernel_clients = []

mon_node = ""
mon_node_ip = ""
mds_nodes = []

md5sum_file_lock = []
active_mdss = []
RC = []
failure = {}
output = []

magna_server = "http://magna002.ceph.redhat.com"
magna_url = f"{magna_server}/cephci-jenkins/"
magna_rhcs_artifacts = f"{magna_server}/cephci-jenkins/latest-rhceph-container-info/"


class TestSetupFailure(Exception):
    pass


# function for getting the clients
def get_client_info(ceph_nodes, clients):
    log.info("Getting Clients")

    for node in ceph_nodes:
        if node.role == "client":
            clients.append(node)

    # Identifying MON node
    for node in ceph_nodes:
        if node.role == "mon":
            mon_node = node
            out, err = mon_node.exec_command(cmd="sudo hostname -I")
            mon_node_ip = out.rstrip("\n")
            break

    for node in ceph_nodes:
        if node.role == "mds":
            mds_nodes.append(node)

    for node in clients:
        node.exec_command(cmd="sudo yum install -y attr")

    fuse_clients = clients[0:2]  # separating clients for fuse and kernel
    kernel_clients = clients[2:4]

    return (
        fuse_clients,
        kernel_clients,
        mon_node,
        mounting_dir,
        mds_nodes,
        md5sum_file_lock,
        mon_node_ip,
    )


# function for providing authorization to the clients from MON node
def auth_list(clients, mon_node):
    for node in clients:
        log.info("Giving required permissions for clients from MON node:")
        mon_node.exec_command(
            cmd="sudo ceph auth get-or-create client.%s mon 'allow *' mds 'allow *, allow rw path=/' "
            "osd 'allow rw pool=cephfs_data' -o /etc/ceph/ceph.client.%s.keyring"
            % (node.hostname, node.hostname)
        )
        keyring, err = mon_node.exec_command(
            sudo=True, cmd="cat /etc/ceph/ceph.client.%s.keyring" % (node.hostname)
        )
        key_file = node.remote_file(
            sudo=True,
            file_name="/etc/ceph/ceph.client.%s.keyring" % (node.hostname),
            file_mode="w",
        )
        key_file.write(keyring)

        key_file.flush()

        node.exec_command(
            cmd="sudo chmod 644 /etc/ceph/ceph.client.%s.keyring" % (node.hostname)
        )

        # creating mounting directory
        node.exec_command(cmd="sudo mkdir %s" % (mounting_dir))


# Mounting single FS with ceph-fuse
def fuse_mount(fuse_clients, mounting_dir):
    try:
        for client in fuse_clients:
            log.info("Creating mounting dir:")
            log.info("Mounting fs with ceph-fuse on client %s:" % (client.hostname))
            client.exec_command(
                cmd="sudo ceph-fuse -n client.%s %s" % (client.hostname, mounting_dir)
            )
            out, err = client.exec_command(cmd="mount")
            mount_output = out.split()

            log.info("Checking if fuse mount is is passed of failed:")
            if "fuse" in mount_output:
                log.info("ceph-fuse mounting passed")
            else:
                log.error("ceph-fuse mounting failed")

        return md5sum_list1
    except Exception as e:
        log.error(e)


def verify_sync_status(verify_io_on_site_node, retry=10, delay=60):
    """
    verify multisite sync status on primary
    """
    check_sync_status, err = verify_io_on_site_node.exec_command(
        cmd="sudo radosgw-admin sync status"
    )

    # check for 'failed' or 'ERROR' in sync status.
    if "failed|ERROR" in check_sync_status:
        log.info("checking for any sync error")
        sync_error_list, err = verify_io_on_site_node.exec_command(
            cmd="sudo radosgw-admin sync error list"
        )
        log.error(err)
        raise Exception(sync_error_list)
    else:
        log.info("No errors or failures in sync status")

    log.info(
        f"check if sync is in progress, if sync is in progress retry {retry} times with {delay}secs of sleep"
    )
    if "behind" in check_sync_status or "recovering" in check_sync_status:
        log.info("sync is in progress")
        log.info(f"sleep of {delay}secs for sync to complete")
        for retry_count in range(retry):
            time.sleep(delay)
            check_sync_status, err = verify_io_on_site_node.exec_command(
                cmd="sudo radosgw-admin sync status"
            )
            if "behind" in check_sync_status or "recovering" in check_sync_status:
                log.info(f"sync is still in progress. sleep for {delay}secs and retry")
            else:
                log.info("sync completed")
                break

        if (retry_count > retry) and (
            "behind" in check_sync_status or "recovering" in check_sync_status
        ):
            raise Exception(
                f"sync is still in progress. with {retry} retries and sleep of {delay}secs between each retry"
            )

    # check metadata sync status
    if "metadata is behind" in check_sync_status:
        raise Exception("metadata sync is either in progress or stuck")

    # check status for complete sync
    if "data is caught up with source" in check_sync_status:
        log.info("sync status complete")
    elif (
        "archive" in check_sync_status and "not syncing from zone" in check_sync_status
    ):
        log.info("data from archive zone does not sync to source zone as per design")
    else:
        raise Exception("sync is either in progress or stuck")


def kernel_mount(mounting_dir, mon_node_ip, kernel_clients):
    try:
        for client in kernel_clients:
            out, err = client.exec_command(
                cmd="sudo ceph auth get-key client.%s" % (client.hostname)
            )
            secret_key = out.rstrip("\n")
            mon_node_ip = mon_node_ip.replace(" ", "")
            client.exec_command(
                cmd="sudo mount -t ceph %s:6789:/ %s -o name=%s,secret=%s"
                % (mon_node_ip, mounting_dir, client.hostname, secret_key)
            )
            out, err = client.exec_command(cmd="mount")
            mount_output = out.split()

            log.info("Checking if kernel mount is is passed of failed:")
            if "%s:6789:/" % (mon_node_ip) in mount_output:
                log.info("kernel mount passed")
            else:
                log.error("kernel mount failed")

        return md5sum_list2
    except Exception as e:
        log.error(e)


def fuse_client_io(client, mounting_dir):
    try:
        rand_count = random.randint(1, 5)
        rand_bs = random.randint(100, 300)
        log.info("Performing IOs on fuse-clients")
        client.exec_command(
            cmd="sudo dd if=/dev/zero of=%snewfile_%s bs=%dM count=%d"
            % (mounting_dir, client.hostname, rand_bs, rand_count),
            long_running=True,
        )
    except Exception as e:
        log.error(e)


def kernel_client_io(client, mounting_dir):
    try:
        rand_count = random.randint(1, 6)
        rand_bs = random.randint(100, 500)
        log.info("Performing IOs on kernel-clients")
        client.exec_command(
            cmd="sudo dd if=/dev/zero of=%snewfile_%s bs=%dM count=%d"
            % (mounting_dir, client.hostname, rand_bs, rand_count),
            long_running=True,
        )
    except Exception as e:
        log.error(e)


def fuse_client_md5(fuse_clients, md5sum_list1):
    try:
        log.info("Calculating MD5 sums of files in fuse-clients:")
        for client in fuse_clients:
            md5sum_list1.append(
                client.exec_command(
                    cmd="sudo md5sum %s* | awk '{print $1}' " % (mounting_dir),
                    long_running=True,
                )
            )

    except Exception as e:
        log.error(e)


def kernel_client_md5(kernel_clients, md5sum_list2):
    try:
        log.info("Calculating MD5 sums of files in kernel-clients:")
        for client in kernel_clients:
            md5sum_list2.append(
                client.exec_command(
                    cmd="sudo md5sum %s* | awk '{print $1}' " % (mounting_dir),
                    long_running=True,
                )
            )
    except Exception as e:
        log.error(e)


# checking file locking mechanism
def file_locking(client):
    try:
        to_lock_file = """
import fcntl
import subprocess
import time
try:
    f = open('/mnt/cephfs/to_test_file_lock', 'w+')
    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    print "locking file:--------------------------------"
    subprocess.check_output(["sudo","dd","if=/dev/zero","of=/mnt/cephfs/to_test_file_lock","bs=1M","count=2"])
except IOError as e:
    print e
finally:
    print "Unlocking file:------------------------------"
    fcntl.lockf(f,fcntl.LOCK_UN)
            """
        to_lock_code = client.remote_file(
            sudo=True, file_name="/home/cephuser/file_lock.py", file_mode="w"
        )
        to_lock_code.write(to_lock_file)
        to_lock_code.flush()
        output, err = client.exec_command(cmd="sudo python /home/cephuser/file_lock.py")
        output = output.split()
        if "Errno 11" in output:
            log.info("File locking achieved, data is not corrupted")
        elif "locking" in output:
            log.info("File locking achieved, data is not corrupted")
        else:
            log.error("Data is corrupted")

        out, err = client.exec_command(
            cmd="sudo md5sum %sto_test_file_lock | awk '{print $1}'" % (mounting_dir)
        )

        md5sum_file_lock.append(out)

    except Exception as e:
        log.error(e)


def activate_multiple_mdss(mds_nodes):
    try:
        log.info("Activating Multiple MDSs")
        for node in mds_nodes:
            out1, err = node.exec_command(
                cmd="sudo ceph fs set cephfs allow_multimds true --yes-i-really-mean-it"
            )
            out2, err = node.exec_command(cmd="sudo ceph fs set cephfs max_mds 2")
            break

    except Exception as e:
        log.error(e)


def mkdir_pinning(clients, range1, range2, dir_name, pin_val):
    try:
        log.info("Creating Directories and Pinning to MDS %s" % (pin_val))
        for client in clients:
            for num in range(range1, range2):
                out, err = client.exec_command(
                    cmd="sudo mkdir %s%s_%d" % (mounting_dir, dir_name, num)
                )
                if pin_val != "":
                    client.exec_command(
                        cmd="sudo setfattr -n ceph.dir.pin -v %s %s%s_%d"
                        % (pin_val, mounting_dir, dir_name, num)
                    )
                else:
                    print("Pin val not given")
                print(out)
                print(time.time())
            break
    except Exception as e:
        log.error(e)


def allow_dir_fragmentation(mds_nodes):
    try:
        log.info("Allowing directorty fragmenation for splitting")
        for node in mds_nodes:
            node.exec_command(cmd="sudo ceph fs set cephfs allow_dirfrags 1")
            break
    except Exception as e:
        log.error(e)


def mds_fail_over(mds_nodes):
    try:
        rand = random.randint(0, 1)
        for node in mds_nodes:
            log.info("Failing MDS %d" % (rand))
            node.exec_command(cmd="sudo ceph mds fail %d" % (rand))
            break

    except Exception as e:
        log.error(e)


def pinned_dir_io(clients, mds_fail_over, num_of_files, range1, range2):
    try:
        log.info("Performing IOs and MDSfailovers on clients")
        for client in clients:
            client.exec_command(cmd="sudo pip install crefi")
            for num in range(range1, range2):
                if mds_fail_over != "":
                    mds_fail_over(mds_nodes)
                rc = client.exec_command(
                    cmd="sudo crefi -n %d %sdir_%d" % (num_of_files, mounting_dir, num),
                    long_running=True,
                )
                RC.append(rc)
                print(time.time())
                if rc == 0:
                    log.info("Client IO is going on,success")
                else:
                    log.error("Client IO got interrupted")
                    break
            break

    except Exception as e:
        log.error(e)


def rc_verify(tc, RC):
    return_codes_set = set(RC)

    if len(return_codes_set) == 1:

        out = "Test case %s Passed" % (tc)

        return out
    else:
        out = "Test case %s Failed" % (tc)

        return out


# colors for pass and fail status
# class Bcolors:
#     HEADER = '\033[95m'
#     OKGREEN = '\033[92m'
#     FAIL = '\033[91m'
#     ENDC = '\033[0m'
#     BOLD = '\033[1m'


def configure_logger(test_name, run_dir):
    """
    Configures a new FileHandler for the root logger.

    Args:
        test_name: name of the test being executed. used for naming the logfile
        run_dir: directory where logs are being placed

    Returns:
        URL where the log file can be viewed or None if the run_dir does not exist
    """
    if not os.path.isdir(run_dir):
        log.error(
            f"Run directory '{run_dir}' does not exist, logs will not output to file."
        )
        return None

    close_and_remove_filehandlers()
    log_format = logging.Formatter(log.log_format)

    full_log_name = f"{test_name}.log"
    test_logfile = os.path.join(run_dir, full_log_name)
    log.info(f"Test logfile: {test_logfile}")

    _handler = logging.FileHandler(test_logfile)
    _handler.setFormatter(log_format)
    log.logger.addHandler(_handler)

    # error file handler
    err_logfile = os.path.join(run_dir, f"{test_name}.err")
    _err_handler = logging.FileHandler(err_logfile)
    _err_handler.setFormatter(log_format)
    _err_handler.setLevel(logging.ERROR)
    log.logger.addHandler(_err_handler)

    url_base = (
        magna_url + run_dir.split("/")[-1]
        if "/ceph/cephci-jenkins" in run_dir
        else run_dir
    )
    log_url = "{url_base}/{log_name}".format(url_base=url_base, log_name=full_log_name)
    log.debug("Completed log configuration")

    return log_url


def create_run_dir(run_id, log_dir=""):
    """
    Create the directory where test logs will be placed.

    Args:
        run_id: id of the test run. used to name the directory
        log_dir: log directory name.
    Returns:
        Full path of the created directory
    """
    msg = """\nNote :
    1. Custom log directory will be disabled if '/ceph/cephci-jenkins' exists.
    2. If custom log directory not specified, then '/tmp' directory is considered .
    """
    print(msg)
    dir_name = "cephci-run-{run_id}".format(run_id=run_id)
    base_dir = "/ceph/cephci-jenkins"

    if log_dir:
        base_dir = log_dir
        if not os.path.isabs(log_dir):
            base_dir = os.path.join(os.getcwd(), log_dir)
    elif not os.path.isdir(base_dir):
        base_dir = f"/tmp/{dir_name}"
    else:
        base_dir = os.path.join(base_dir, dir_name)

    print(f"log directory - {base_dir}")
    try:
        os.makedirs(base_dir)
    except OSError:
        if "jenkins" in getpass.getuser():
            raise

    return base_dir


def close_and_remove_filehandlers(logger=logging.getLogger("cephci")):
    """
    Close FileHandlers and then remove them from the loggers handlers list.

    Args:
        logger: the logger in which to remove the handlers from, defaults to root logger

    Returns:
        None
    """
    handlers = logger.handlers[:]
    for h in handlers:
        if isinstance(h, logging.FileHandler):
            h.close()
            logger.removeHandler(h)


def create_report_portal_session():
    """
    Configures and creates a session to the Report Portal instance.

    Returns:
        The session object
    """
    cfg = get_cephci_config()["report-portal"]

    try:
        return ReportPortalService(
            endpoint=cfg["endpoint"],
            project=cfg["project"],
            token=cfg["token"],
            verify_ssl=False,
        )
    except BaseException:  # noqa
        print("Encountered an issue in connecting to report portal.")


def timestamp():
    """
    The current epoch timestamp in milliseconds as a string.

    Returns:
        The timestamp
    """
    return str(int(time.time() * 1000))


def error_handler(exc_info):
    """
    Error handler for the Report Portal session.

    Returns:
        None
    """
    print("Error occurred: {}".format(exc_info[1]))
    traceback.print_exception(*exc_info)


def create_unique_test_name(test_name, name_list):
    """
    Creates a unique test name using the actual test name and an increasing integer for
    each duplicate test name.

    Args:
        test_name: name of the test
        name_list: list of names to compare test name against

    Returns:
        unique name for the test case
    """
    base = "_".join(str(test_name).split())
    num = 0
    while "{base}_{num}".format(base=base, num=num) in name_list:
        num += 1
    return "{base}_{num}".format(base=base, num=num)


def get_latest_container_image_tag(version):
    """
    Retrieves the container image tag of the latest compose for the given version

    Args:
        version: version to get the latest image tag for (2.x, 3.0, or 3.x)

    Returns:
        str: Image tag of the latest compose for the given version

    """
    image_tag = get_latest_container(version).get("docker_tag")
    log.info("Found image tag: {image_tag}".format(image_tag=image_tag))
    return str(image_tag)


def get_latest_container(version):
    """
    Retrieves latest nightly-build container details from magna002.ceph.redhat.com

    Args:
        version:    version to get the latest image tag, should match
                    latest-RHCEPH-{version} filename at magna002 storage

    Returns:
        Container details dictionary with given format:
        {
            'docker_registry': docker_registry,
            'docker_image': docker_image,
            'docker_tag': docker_tag
        }
    """
    url = f"{magna_rhcs_artifacts}latest-RHCEPH-{version}.json"
    data = requests.get(url, verify=False)
    docker_registry, docker_tag = data.json()["repository"].split("/rh-osbs/rhceph:")
    docker_image = "rh-osbs/rhceph"

    return {
        "docker_registry": docker_registry,
        "docker_image": docker_image,
        "docker_tag": docker_tag,
    }


def get_release_repo(version):
    """
    Retrieves the repo and image information for the RC build of the version specified
    from magna002.ceph.redhat.com

    Args:
        version:    version to get the latest image tag, should match version in
                    release.yaml at magna002 storage

    Returns:
        Repo and Container details dictionary with given format:
        {
            'composes': <RC release composes>,
            'image': <CEPH and monitoring images related to the RC build>
        }
    """
    recipe_url = get_cephci_config().get("build-url", magna_rhcs_artifacts)
    url = f"{recipe_url}release.yaml"
    data = requests.get(url, verify=False)
    repo_details = yaml.safe_load(data.text)[version]

    return repo_details


def yaml_to_dict(file_name):
    """Retrieve yaml data content from file."""
    file_path = os.path.abspath(file_name)
    with open(file_path, "r") as conf_:
        content = yaml.safe_load(conf_)

    return content


def custom_ceph_config(suite_config, custom_config, custom_config_file):
    """
    Combines and returns custom configuration overrides for ceph.
    Hierarchy is as follows:
        custom_config > custom_config_file > suite_config

    Args:
        suite_config: ceph_conf_overrides that currently exist in the test suite
        custom_config: custom config args provided by the cli (these all go to the
                       global scope)
        custom_config_file: path to custom config yaml file provided by the cli

    Returns
        New value to be used for ceph_conf_overrides in test config
    """
    log.debug("Suite config: {}".format(suite_config))
    log.debug("Custom config: {}".format(custom_config))
    log.debug("Custom config file: {}".format(custom_config_file))

    full_custom_config = suite_config or {}
    cli_config_dict = {}
    custom_config_dict = {}

    # retrieve custom config from file
    if custom_config_file:
        with open(custom_config_file) as f:
            custom_config_dict = yaml.safe_load(f)
            log.info("File contents: {}".format(custom_config_dict))

    # format cli configs into dict
    if custom_config:
        cli_config_dict = dict(item.split("=") for item in custom_config)

    # combine file and cli configs
    if cli_config_dict:
        if not custom_config_dict.get("global"):
            custom_config_dict["global"] = {}
        for key, value in cli_config_dict.items():
            custom_config_dict["global"][key] = value

    # combine file and suite configs
    for key, value in custom_config_dict.items():
        subsection = {}
        if full_custom_config.get(key):
            subsection.update(full_custom_config[key])
        subsection.update(value)
        full_custom_config[key] = subsection

    log.info("Full custom config: {}".format(full_custom_config))
    return full_custom_config


def email_results(test_result):
    """
    Email results of test run to QE

    If the user specifies no "email" settings in ~/.cephci.yaml, this method
    is a no-op.

    Args:
        test_result (dict): contains all the various keyword arguments containing
                            execution details.
    Supported Keywords
        results_list (list): test case results info
        run_id (str): id of the test run
        trigger_user (str): user of the node where the run is triggered from
        run_dir (str): log directory path
        suite_run_time (dict): suite total duration info
        info (dict): General information about the test run.
        send_to_cephci (optional [bool]): send to cephci@redhat.com as well as user
                                          email

    Returns: None
    """

    try:
        run_id = test_result["run_id"]
        results_list = test_result["result"]
    except KeyError as kerr:
        log.error(f"Key not found : {kerr}")
        exit(1)

    run_name = "cephci-run-{id}".format(id=run_id)
    msg = MIMEMultipart("alternative")
    run_status = get_run_status(results_list)
    msg["Subject"] = "[{run_status}]  Suite:{suite}  Build:{compose}  ID:{id}".format(
        suite=results_list[0]["suite-name"],
        compose=results_list[0]["compose-id"],
        run_status=run_status,
        id=run_id,
    )

    test_result["run_name"] = run_name
    html = create_html_file(test_result=test_result)
    part1 = MIMEText(html, "html")
    msg.attach(part1)

    props_content = f"""
    run_status=\"{run_status}\"
    compose=\"{results_list[0]['compose-id']}\"
    suite=\"{results_list[0]['suite-name']}\"
    """

    # result properties file and summary html log for injecting vars in jenkins jobs,
    # gitlab JJB to parse
    abs_path = os.path.join(os.getcwd(), "result.props")
    write_to_file(data=props_content.strip(), abs_path=abs_path)
    cfg = get_cephci_config().get("email")
    if not cfg:
        return

    sender = "cephci@redhat.com"
    recipients = []
    address = cfg.get("address")
    send_to_cephci = test_result.get("send_to_cephci", False)
    if cfg and address:
        recipients = re.split(r",\s*", cfg["address"])

    if address.count("@") != len(recipients):
        log.warning(
            "No email address configured in ~/.cephci.yaml."
            "Or please specify in this format eg., address: email1, email2.......emailn"
            "Please configure if you would like to receive run result emails."
        )

    if send_to_cephci:
        recipients.append(sender)
        recipients = list(set(recipients))

    if recipients:
        msg["From"] = sender
        msg["To"] = ", ".join(recipients)
        try:
            s = smtplib.SMTP("localhost")
            s.sendmail(sender, recipients, msg.as_string())
            s.quit()
            log.info(
                "Results have been emailed to {recipients}".format(
                    recipients=recipients
                )
            )
        except Exception as e:
            print("\n")
            log.exception(e)


def create_html_file(test_result) -> str:
    """Creates the HTML file from the template to be sent via mail

    Args:
        test_result (dict): contains all the various keyword arguments containing
                            execution details.
    Supported Args
        results_list (list): test case results info
        run_name (str): name of the test run
        user (str): user of the node where the run is triggered from
        run_dir (str): log directory path
        run_time (dict): suite total duration info
        info (dict): General information about the test run

    Returns: HTML file
    """
    try:
        run_name = test_result["run_name"]
        trigger_user = test_result["trigger_user"]
        run_dir = test_result["run_directory"]
        suite_run_time = test_result["total_time"]
        info = test_result["info"]
        test_results = test_result["result"]
    except KeyError as kerr:
        log.error(f"Key not found : {kerr}")
        exit(1)

    # we are checking for /ceph/cephci-jenkins to see if the magna is already mounted
    # on system we are executing
    log_link = (
        f"{magna_url}{run_name}" if "/ceph/cephci-jenkins" in run_dir else run_dir
    )
    info["link"] = f"{log_link}/startup.log"
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_dir = os.path.join(project_dir, "templates")

    jinja_env = Environment(
        extensions=[MarkdownExtension],
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(["html", "xml"]),
    )

    template = jinja_env.get_template("result-email-template.html")

    html = template.render(
        run_name=run_name,
        log_link=log_link,
        test_results=test_results,
        suite_run_time=suite_run_time,
        trigger_user=trigger_user,
        info=info,
        use_abs_log_link=True,
    )

    # Result.html file is stored in the folder containing the log files.
    # Moving to relative path facilitate the copying of the
    # files to a different location without breaking the hyperlinks in result.html.
    result_html = template.render(
        run_name=run_name,
        log_link=log_link,
        test_results=test_results,
        suite_run_time=suite_run_time,
        trigger_user=trigger_user,
        info=info,
        use_abs_log_link=False,
    )

    abs_path = os.path.join(run_dir, "index.html")
    write_to_file(data=result_html, abs_path=abs_path)

    return html


def write_to_file(data, abs_path):
    """
    Writes the given data into the file at the path provided

    Args:
        data: Contents of the file
        abs_path: Path where the file needs to be created

    Returns: None

    """
    try:
        with open(abs_path, "w+") as fp:
            fp.write(data)
    except IOError as err:
        log.error(f"IO error hit during opening the file. Error : {err}")


def get_cephci_config():
    """
    Receives the data from ~/.cephci.yaml.

    Returns:
        (dict) configuration from ~/.cephci.yaml

    """
    home_dir = os.path.expanduser("~")
    cfg_file = os.path.join(home_dir, ".cephci.yaml")
    try:
        with open(cfg_file, "r") as yml:
            cfg = yaml.safe_load(yml)
    except IOError:
        log.error(
            "Please create ~/.cephci.yaml from the cephci.yaml.template. "
            "See README for more information."
        )
        raise

    return cfg


def get_run_status(results_list):
    """
    Returns overall run status either Pass or Fail.
    """
    for tc in results_list:
        if tc["status"] == "Failed":
            return "FAILED"
        if tc["status"] == "Not Executed":
            return "SETUP-FAILURE"

    return "PASSED"


def setup_cluster_access(cluster, target) -> None:
    """
    Configures the target to communicate with the Ceph cluster.

    The admin keyring and minimal cluster configuration is copied to the target node
    to enable it to communicate with the provided cluster. This enables testers to use
    a target for all communications.

    Note: This is not a replacement to the client role

    Args:
        cluster:    The cluster participating in the test
        target:     The node that needs to interact with the cluster.

    Returns:
        None

    Raises:
        CommandException    when a remote command fails to execute.
    """
    node_commands = ["yum install -y ceph-common --nogpgcheck", "mkdir -p /etc/ceph"]
    for command in node_commands:
        target.exec_command(sudo=True, cmd=command)

    installer_node = cluster.get_nodes(role="installer")[0]
    commands = [
        ("cephadm shell -- ceph auth get client.admin", "/etc/ceph/ceph.keyring"),
        ("cephadm shell -- ceph config generate-minimal-conf", "/etc/ceph/ceph.conf"),
    ]

    for command, out_file in commands:
        file_, err = installer_node.exec_command(sudo=True, cmd=command)
        fh = target.remote_file(file_name=out_file, file_mode="w", sudo=True)
        fh.write(file_)
        fh.flush()


def generate_unique_id(length):
    """
    Return unique string of N(length) characters
    Args:
        length: positive integer

    Note:
        make sure length > 0 for a proper value
        length = 0, returns empty string ''
    """
    return "".join(random.choices(ascii_uppercase + digits, k=length))


def generate_node_name(cluster_name, instance_name, run_id, node, role):
    """
    Return node name using provided parameters

    Args:
        cluster_name: cluster name from config
        instance_name: preferred instance name
        run_id: unique run Id
        node: node name
        role: all node roles

    Only Installer node will get prefixed with "Installer" name,
    which helps in identification of admin node.
    """
    _role = ""
    if "installer" in role:
        _role = "installer"
    elif "pool" in role:
        _role = "pool"

    node_name = [
        cluster_name,
        instance_name if instance_name else "",
        run_id,
        node,
        _role,
    ]
    node_name = "-".join([i for i in node_name if i])
    if len(node_name) > 48:
        log.warning(f"[{node_name}] WARNING!!!!, Node name too long(>48 chars)")

    return node_name


def get_cephqe_ca() -> Optional[Tuple]:
    """Retrieve CephCI QE CA certificate and key."""
    base_uri = (
        get_cephci_config()
        .get("root-ca-location", "http://magna002.ceph.redhat.com/cephci-jenkins")
        .rstrip("/")
    )
    ca_cert = None
    ca_key = None

    try:
        with request.urlopen(url=f"{base_uri}/.cephqe-ca.pem") as fd:
            ca_cert = x509.load_pem_x509_certificate(fd.read())

        with request.urlopen(url=f"{base_uri}/.cephqe-ca.key") as fd:
            ca_key = serialization.load_pem_private_key(fd.read(), None)
    except BaseException as be:
        log.debug(be)

    return ca_key, ca_cert


def generate_self_signed_certificate(subject: Dict) -> Tuple:
    """
    Create and return a self-signed certificate using the provided subject information.

    Args:
        subject     Dictionary holding the certificate subject key/value pair

    Returns:
        device_key, device_cert, ca_cert   Tuple as strings
    """
    ca_key, ca_cert = get_cephqe_ca()

    # Generate the private key
    cert_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )

    cert_subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "IN"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Karnataka"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "Bengaluru"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Red Hat Inc"),
            x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Storage"),
            x509.NameAttribute(NameOID.COMMON_NAME, subject["common_name"]),
        ]
    )

    cert = (
        x509.CertificateBuilder()
        .subject_name(cert_subject)
        .issuer_name(ca_cert.issuer if ca_cert else cert_subject)
        .public_key(cert_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=30))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName(f"*.{subject['common_name']}"),
                    x509.DNSName(subject["common_name"]),
                    x509.IPAddress(ip_address(subject["ip_address"])),
                ]
            ),
            critical=False,
        )
        .sign(ca_key if ca_key else cert_key, hashes.SHA256(), default_backend())
    )

    return (
        cert_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode("utf-8"),
        cert.public_bytes(serialization.Encoding.PEM).decode("utf-8"),
        ca_cert.public_bytes(serialization.Encoding.PEM).decode("utf-8")
        if ca_cert
        else None,
    )


def fetch_build_artifacts(build, ceph_version, platform, upstream_build=None):
    """Retrieves build details from magna002.ceph.redhat.com.

    if "{build}" is "upstream"  "{build}.yaml" would be file name
    else its "RHCEPH-{ceph_version}.yaml" which is
    searched in magna002 Ceph artifacts location.

    Args:
        ceph_version: RHCS version
        build: build section to be fetched
        platform: OS distribution name with major Version(ex., rhel-8)
        upstream_build: upstream build(ex., pacific/quincy)
    Returns:
        base_url, container_registry, image-name, image-tag
    """
    try:
        recipe_url = get_cephci_config().get("build-url", magna_rhcs_artifacts)
        filename = (
            f"{build}.yaml" if build == "upstream" else f"RHCEPH-{ceph_version}.yaml"
        )
        url = f"{recipe_url}{filename}"
        data = requests.get(url, verify=False)
        yml_data = yaml.safe_load(data.text)

        build_info = (
            yml_data[upstream_build] if build == "upstream" else yml_data[build]
        )

        container_image = (
            build_info["image"] if build == "upstream" else build_info["repository"]
        )
        registry, image_name = container_image.split(":")[0].split("/", 1)
        image_tag = container_image.split(":")[-1]
        base_url = (
            build_info["composes"]
            if build == "upstream"
            else build_info["composes"][platform]
        )
        return base_url, registry, image_name, image_tag
    except Exception as e:
        raise TestSetupFailure(f"Could not fetch build details of : {e}")


def rp_deco(func):
    def inner_method(cls, *args, **kwargs):
        if not cls.client:
            return

        try:
            func(cls, *args, **kwargs)
        except BaseException as be:  # noqa
            log.debug(be, exc_info=True)
            log.warning("Encountered an error during report portal operation.")

    return inner_method


class ReportPortal:
    """Handles logging to report portal."""

    def __init__(self):
        """Initializes the instance."""
        cfg = get_cephci_config()
        access = cfg.get("report-portal")

        self.client = None
        self._test_id = None

        if access:
            try:
                self.client = ReportPortalService(
                    endpoint=access["endpoint"],
                    project=access["project"],
                    token=access["token"],
                    verify_ssl=False,
                )
            except BaseException:  # noqa
                log.warning("Unable to connect to Report Portal.")

    @rp_deco
    def start_launch(self, name: str, description: str, attributes: dict) -> None:
        """
        Initiates a test execution with the provided details

        Args:
            name (str):         Name of test execution.
            description (str):  Meta data information to be added to the launch.
            attributes (dict):  Meta data information as dict

        Returns:
             None
        """
        self.client.start_launch(
            name, start_time=timestamp(), description=description, attributes=attributes
        )

    @rp_deco
    def start_test_item(self, name: str, description: str, item_type: str) -> None:
        """
        Records an entry within the initiated launch.

        Args:
            name (str):         Name to be set for the test step
            description (str):  Meta information to be used.
            item_type (str):    Type of entry to be created.

        Returns:
            None
        """
        self._test_id = self.client.start_test_item(
            name, start_time=timestamp(), item_type=item_type, description=description
        )

    @rp_deco
    def finish_test_item(self, status: Optional[str] = "PASSED") -> None:
        """
        Ends a test entry with the given status.

        Args:
            status (str):
        """
        if not self._test_id:
            return

        self.client.finish_test_item(
            item_id=self._test_id, end_time=timestamp(), status=status
        )

    @rp_deco
    def finish_launch(self) -> None:
        """Closes the Report Portal execution run."""
        self.client.finish_launch(end_time=timestamp())
        self.client.terminate()
        tfacon(self.client.get_launch_ui_id())

    @rp_deco
    def log(self, message: str, level="INFO") -> None:
        """
        Adds log records to the event.

        Args:
            message (str):  Message to be logged.
            level (str):    The level at which the record has to be logged.

        Returns:
            None
        """
        self.client.log(
            time=timestamp(),
            message=message.__str__(),
            level=level,
            item_id=self._test_id,
        )


def tfacon(launch_id):
    """
    Connects the launch with TFA and gives the predictions for the launch
    It will fail silently

    Args:
         launch_id : launch_id that has been created
    """
    cfg = get_cephci_config()
    tfacon_cfg = cfg.get("tfacon")
    if not tfacon_cfg:
        return
    project_name = tfacon_cfg.get("project_name")
    auth_token = tfacon_cfg.get("auth_token")
    platform_url = tfacon_cfg.get("platform_url")
    tfa_url = tfacon_cfg.get("tfa_url")
    re_url = tfacon_cfg.get("re_url")
    connector_type = tfacon_cfg.get("connector_type")
    cmd = (
        f"~/.local/bin/tfacon run --auth-token {auth_token} "
        f"--connector-type {connector_type} "
        f"--platform-url {platform_url} "
        f"--project-name {project_name} "
        f"--tfa-url {tfa_url} "
        f"--re-url {re_url} -r "
        f"--launch-id {launch_id}"
    )
    log.info(cmd)
    p1 = subprocess.Popen(
        cmd,
        shell=True,
        stdin=None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    result, result_err = p1.communicate()

    log.info(result.decode("utf-8"))
    if p1.returncode != 0:
        log.warning("Unable to get the TFA anaylsis for the results")
        log.warning(result_err.decode("utf-8"))
        log.warning(result.decode("utf-8"))


def install_start_kafka(rgw_node, cloud_type):
    """Install kafka package and start zookeeper and kafka services."""
    log.info("install kafka broker for bucket notification tests")
    if cloud_type == "ibmc":
        wget_cmd = "curl -o /tmp/kafka.tgz https://10.245.4.89/kafka_2.13-2.8.0.tgz"
    else:
        wget_cmd = "curl -o /tmp/kafka.tgz http://magna002.ceph.redhat.com/cephci-jenkins/kafka_2.13-2.8.0.tgz"

    tar_cmd = "tar -zxvf /tmp/kafka.tgz -C /usr/local/"
    rename_cmd = "mv /usr/local/kafka_2.13-2.8.0 /usr/local/kafka"
    chown_cmd = "chown cephuser:cephuser /usr/local/kafka"
    rgw_node.exec_command(
        cmd=f"{wget_cmd} && {tar_cmd} && {rename_cmd} && {chown_cmd}", sudo=True
    )

    KAFKA_HOME = "/usr/local/kafka"

    # start zookeeper service
    rgw_node.exec_command(
        check_ec=False,
        sudo=True,
        cmd=f"{KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon {KAFKA_HOME}/config/zookeeper.properties",
    )

    # wait for zookeepeer service to start
    time.sleep(30)

    # start kafka servicee
    rgw_node.exec_command(
        check_ec=False,
        sudo=True,
        cmd=f"{KAFKA_HOME}/bin/kafka-server-start.sh -daemon {KAFKA_HOME}/config/server.properties",
    )

    # wait for kafka service to start
    time.sleep(30)


def method_should_succeed(function, *args, **kwargs):
    """
    Wrapper function to verify the return value of executed method.

    This function will raise Assertion if return value is false, empty, or 0.

    Args:
        function: name of the function
        args: arg list
        kwargs: arg dict

    Usage:
        Basic usage is pass a function and it's list and/or dict arguments
        ex:     method_should_succeed(set_osd_out, ceph_cluster, osd_id)
                method_should_succeed(bench_write, **pool)
    """
    rc = function(*args, **kwargs)

    log.debug(f"The {function} return status is {rc}")
    if not rc:
        raise AssertionError(f"Execution failed at function {function}")


def should_not_be_empty(variable, msg="Variable is empty"):
    """
    Function to raise assertion if variable is empty.
    Works with strings, lists, dicts etc.
    Args:
        variable: variable that should be verified
        msg: [optional] custom message
    """
    if not variable:
        raise AssertionError(msg)


def generate_self_signed_cert_on_rgw(rgw_node):
    """Generate self-signed certificate on given rgw node."""
    ssl_cert_path = "/etc/ceph/"
    pem_file_name = "server.pem"

    subject = {
        "common_name": rgw_node.hostname,
        "ip_address": rgw_node.ip_address,
    }
    key, cert, ca = generate_self_signed_certificate(subject=subject)
    pem = key + cert + ca
    rgw_node.exec_command(
        sudo=True,
        cmd="mkdir /etc/ceph; chown 755 /etc/ceph; touch /etc/ceph/server.pem",
    )
    pem_file_path = os.path.join(ssl_cert_path, pem_file_name)
    server_pem_file = rgw_node.remote_file(
        sudo=True, file_name=pem_file_path, file_mode="w+"
    )
    server_pem_file.write(pem)
    server_pem_file.flush()
    log.debug(pem)


def clone_the_repo(config, node, path_to_clone):
    """clone the repo on to test node.

    Args:
        config: test config
        node: ceph node
        path_to_clone: the path to clone the repo

    TODO: if path_to_clone is not given, make temporary dir on test
          node and clone the repo in it.
    """
    log.info("cloning the repo")
    branch = config.get("branch", "master")
    log.info(f"branch: {branch}")
    repo_url = config.get("git-url")
    log.info(f"repo_url: {repo_url}")
    git_clone_cmd = f"sudo git clone {repo_url} -b {branch}"
    node.exec_command(cmd=f"cd {path_to_clone} ; {git_clone_cmd}")


def run_fio(**kw):
    """Run IO using fio tool on given target.

    Args:
        filename: Target device or file.
        rbdname: rbd image name
        pool: name of rbd image pool
        runtime: fio runtime
        long_running(bool): True for long running required

    Prerequisite: fio package must have been installed on the client node.
    """
    sudo = False
    if kw.get("filename"):
        opt_args = f"--filename={kw['filename']}"
        sudo = True
    else:
        opt_args = (
            f"--ioengine=rbd --rbdname={kw['image_name']} --pool={kw['pool_name']}"
        )

    opt_args += f" --runtime={kw.get('runtime', 120)}"

    long_running = kw.get("long_running", False)
    cmd = (
        "fio --name=test-1  --numjobs=1 --rw=write"
        " --bs=1M --iodepth=8 --fsync=32  --time_based"
        f" --group_reporting {opt_args}"
    )

    return kw["client_node"].exec_command(cmd=cmd, long_running=long_running, sudo=sudo)


def fetch_image_tag(rhbuild):
    """Retrieves image tag from magna002.ceph.redhat.com.

    Args:
        rhbuild: build section to be fetched)
    Returns:
        image-tag
    """
    try:
        recipe_url = get_cephci_config().get("build-url", magna_rhcs_artifacts)
        filename = f"RHCEPH-{rhbuild}.yaml"
        url = f"{recipe_url}{filename}"
        data = requests.get(url, verify=False)
        yml_data = yaml.safe_load(data.text)
        if "rc" in yml_data.keys():
            container_image = yml_data["latest"]["repository"]
            image_tag = container_image.split(":")[-1]
            return image_tag
        raise TestSetupFailure("Not a live testing")
    except Exception as e:
        raise TestSetupFailure(f"Could not fetch image tag : {e}")
