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
[{{ repo }}]
name = {{ repo }}
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
           ceph-4.2-rhel-7 \
           14.2.11-196 \
           http://download.eng.bos.redhat.com/rhel-7/composes/auto/ceph-4.2-rhel-7/RHCEPH-4.2-RHEL-7-20210909.ci.0

Usage:
  upload_compose.py <bucket_name> <ceph_version> <base_url>
  upload_compose.py -h | --help

Options:
  -h --help     Show this screen

"""


def create_repo(url: str) -> None:
    """
    Creates all the required repo files based on the given base_url.

    Args:
        url: str    URL holding the RPMs to be uploaded
    Returns:
        None
    """
    _tmpl = Template(REPO_TEMPLATE)
    data = dict({"base_url": url})
    repo_file = _tmpl.render(data=data)
    LOG.debug(f"The repo file is \n {repo_file}")

    with open("/etc/yum.repos.d/ceph.repo", "w") as fh:
        fh.write(repo_file)

    LOG.info("Successfully created repo file.")


def compress_build() -> str:
    """
    Returns the compressed file name containing the development RPMs.

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
                f"{repo}",
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

    create_repo(url=base_url)
    temp_dir = compress_build()
    upload_directory(temp_dir, bucket_name, ceph_version)
