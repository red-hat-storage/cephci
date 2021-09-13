"""
This script executes the development build upload workflow to IBM COS.

The steps followed are
- Create the necessary repo files
- Perform a repo sync on all required repositories
- Upload all the packages to IBM Cloud Object Storage

Note:
    The script is required to be executed with sudo privileges. This is required as
    we would create .repo files in /etc/yum.conf.d/ for sync operations. Also, the
    script works only on CentOS-7 due to options provided for reposync.
"""
import logging
import subprocess
import sys
import tempfile

from docopt import docopt
from jinja2 import Template

from storage.ibm_cos import CloudObjectStorage

LOG = logging.getLogger(__name__)
REPO_TEMPLATE = """
{%- for repo in ["OSD", "MON", "Tools"] -%}
[{{ data.nvr }}-{{ repo }}]
name = {{ data.nvr }}-{{ repo}}
baseurl = {{ data.base_url }}/compose/{{ repo }}/$basearch/os
enabled = 0
gpgcheck = 0

{% endfor %}
"""

usage = """Upload Compose.

This script pulls the development RPMs available in the given base_url and uploads an
archive of it using the provided object_name to the mentioned bucket.

In the context of CephCI,
    bucket_name     is the name of the RHCS build
    ceph_version    is the ceph version which is the prefixed to the uploaded items.
    base_url        base uri from which the OSD, MON, Tools repo can be constructed.

  Example:
    python upload_compose.py \
           rhceph-4.2-rhel-7 \
           14.6.2-113 \
           http://download.eng.bos.redhat.com/rhel-8/composes/auto/ceph-5.0-rhel-8/RHCEPH-5.0-RHEL-8-20210913.ci.1

Usage:
  upload_compose.py <bucket_name> <ceph_version> <base_url>
  upload_compose.py -h | --help

Options:
  -h --help     Show this screen

"""


def create_repo(build: str, url: str) -> None:
    """
    Creates all the required repo files based on the given base_url.

    Args:
        build: str          Bucket name or RH Ceph build
        url: str       URL holding the RPMs to be uploaded
    Returns:
        None
    """
    _tmpl = Template(REPO_TEMPLATE)
    data = dict({"nvr": build, "base_url": url})
    repo_file = _tmpl.render(data=data)
    LOG.debug(f"The repo file is \n {repo_file}")

    with open(f"/etc/yum.repos.d/{build}.repo", "w") as fh:
        fh.write(repo_file)

    LOG.info("Successfully created repo file.")


def compress_build(build: str) -> str:
    """
    Returns the compressed file name containing the development RPMs.

    Args:
        build: str  The base repo id to be used to pull the packages.
    Returns:
        str: The complete file path to the archive file created.
    """
    repos = tempfile.mkdtemp()

    for repo in ["OSD", "MON", "Tools"]:
        subprocess.run(
            [
                "reposync",
                "-l",
                "-m",
                "--delete",
                "--newest-only",
                "--download-metadata",
                "--repoid",
                f"{build}-{repo}",
                "--download_path",
                repos,
            ],
            check=True,
        )

    return repos


def upload_directory(local_dir: str, bucket: str, item: str) -> None:
    """
    Uploads the given file to the provided bucket with the mentioned name.

    Args:
        local_dir (str):    Complete path to the file that needs to be uploaded
        bucket (str):       The name of container to which file has to be uploaded
        item (str):         The name to be used for the uploaded object
    """
    cos = CloudObjectStorage()

    try:
        cos.create_bucket(bucket)
        cos.set_lifecycle_rules(bucket)
    except BaseException as be:  # no-qa
        # Ignoring the exception with assumption that the bucket is created.
        LOG.debug(be)

    cos.upload_directory(local_dir, item, bucket)
    LOG.info("Successfully upload the object to IBM COS !!!")


if __name__ == "__main__":
    logging.basicConfig(
        handlers=[logging.StreamHandler(sys.stdout)],
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )

    _args = docopt(usage)
    bucket_name = _args["<bucket_name>"]
    ceph_version = _args["<ceph_version>"]
    base_url = _args["<base_url>"]

    create_repo(build=bucket_name, url=base_url)
    temp_dir = compress_build(bucket_name)
    upload_directory(temp_dir, bucket_name, ceph_version)
