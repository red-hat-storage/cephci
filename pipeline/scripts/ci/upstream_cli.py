"""
This script provides the upstream latest rpm repo path and container image.
Please note that this script should be run under root privilages
inorder to pull the image and podman should be installed on that host.
"""
import logging
import os
import re
import sys
import time

import requests
import yaml
from docopt import docopt
from packaging import version
from yaml.loader import SafeLoader

LOG = logging.getLogger(__name__)


def set_lock(file_name):
    """Method to lock the file to avoid race condition"""
    lock_file = file_name + ".lock"
    exit_status = os.system(f"ls -l {lock_file}")
    if exit_status:
        print("File lock does not exist, Creating it")
        os.system(f"touch {lock_file}")
        return
    # If any process locks the file it will wait till it unlock
    timeout = 600
    timeout_start = time.time()
    while time.time() < timeout_start + timeout:
        # wait for 2 minutes before retrying
        time.sleep(120)
        exit_status = os.system(f"ls -l {lock_file}")
        if exit_status:
            print("File lock does not exist, Creating it")
            os.system(f"touch {lock_file}")
            return

    raise Exception(f"Lock file: {lock_file} already exist, can not create lock file")


def unset_lock(file_name):
    """Method to unlock the above set lock"""
    lock_file = file_name + ".lock"
    exit_status = os.system(f"rm -f {lock_file}")
    if exit_status == 0:
        print("File un-locked successfully")
        return
    raise Exception("Unable to unlock the file")


def update_upstream(file_name, content, repo, image, upstream_version):
    """Method to update for the provided file
    along with file set lock to avoid race condition.
    Args:
        file_name: Name of the file to update
        content: things to update in file
    """
    with open(file_name, "w") as yaml_file:
        try:
            # To lock the file to avoid race condition
            set_lock(file_name)
            # update the file with write operation
            yaml_file.write(yaml.dump(content, default_flow_style=False))
        finally:
            # unlock it so other processed can use it
            unset_lock(file_name)
            LOG.info(
                f"Updated build info: \n repo:{repo} \n image:{image} \n ceph version:{upstream_version} in {file_name}"
            )


def store_in_upstream(branch_name, repo, image, upstream_version):
    """Method to update it in upstream.yaml.
    Args:
        branch_name: Name of upstream branch
        repo: upstream branch compose URL
        image: upstream branch image
        version: upstream branch ceph version.
    """
    file_name = "/ceph/cephci-jenkins/latest-rhceph-container-info/upstream.yaml"
    repo_details = {}
    if os.path.exists(file_name):
        stream = open(file_name, "r")
        repo_details = yaml.load(stream, Loader=SafeLoader) or {}
    if repo_details and repo_details.get(branch_name):
        existing_version = repo_details[branch_name]["ceph-version"]
        # version.parse helps to compare version as easy as integers.
        old_version = version.parse(existing_version)
        new_version = version.parse(upstream_version)
        # update only if current version is greater than existing version.
        if new_version <= old_version:
            print(
                f"Current version:{upstream_version} not greater than existing version:{existing_version}"
            )
            return
        repo_details[branch_name]["composes"] = repo
        repo_details[branch_name]["image"] = image
        repo_details[branch_name]["ceph-version"] = upstream_version
    else:
        # updating new branch details in upstream.yaml
        repo_details[branch_name] = {
            "ceph-version": upstream_version,
            "composes": repo,
            "image": image,
        }
    update_upstream(file_name, repo_details, repo, image, upstream_version)


usage = """
This script helps to provide needed info for upstream specific.

Usage:
  upstream_cli.py build <branch_name>


Example:
    python upstream_cli.py build quincy

Options:
    -h --help                       Show this screen
"""


def fetch_upstream_build(branch: str):
    """
    Method to get rpm repo path,container image and ceph version of given branch.

    Args:
        branch: str    Upstream branch name
    """
    branch = branch.lower()
    branch_repo = branch + "/latest/centos/8/repo/"
    url = "https://shaman.ceph.com/api/repos/ceph/" + branch_repo

    # if any connection issue to URL it will wait for 30 seconds to establish connection.
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        response.raise_for_status()
    repo = response.url
    LOG.info(f"Upstream repo for the given branch is {repo}")

    # To get image for that rpm repo
    id = re.search("[0-9a-f]{5,40}", repo).group()
    image = "quay.ceph.io/ceph-ci/ceph:" + id
    LOG.info(f"Upstream image for the given branch is {image}")

    cmd = "podman -v"
    exit_status = os.system(cmd)
    if exit_status:
        raise Exception("Please install podman on host")

    cmd = f"sudo podman pull {image}"
    exit_status = os.system(cmd)
    if exit_status:
        raise Exception(f"{image} is unable to pull")

    # Regex to get sprm url to find ceph version
    content = response.text
    regex = "(?i)(https?://|www.|\\w+\\.)[^\\s]+"
    urls = [match.group() for match in re.finditer(regex, content)]
    srpm = urls[2]

    response = requests.get(srpm, timeout=30)
    content = response.text

    # Regex to match ceph version
    pattern = re.search(r"ceph.*rpm", content).group(0)
    version = re.search(r"\d(?:.*?(-).[0-9]*){1}", pattern).group(0)
    LOG.info(f"Upstream version for the given branch is {version}")
    store_in_upstream(branch_name, repo, image, version)


OPS = {
    "build": fetch_upstream_build,
}

if __name__ == "__main__":

    _args = docopt(usage, help=True, version=None, options_first=False)
    logging.basicConfig(
        handlers=[logging.StreamHandler(sys.stdout)],
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )

    try:
        method = None
        for key, value in OPS.items():
            if _args[key]:
                method = value
                break
        else:
            raise Exception("please provide right action")

        branch_name = _args["<branch_name>"]
        args = [branch_name]

        method(*args)
    except BaseException as be:
        raise Exception(be)
