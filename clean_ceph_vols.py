#!/usr/bin/env python
"""
Utility to cleanup orphaned volumes.

"""
from gevent import monkey, sleep

monkey.patch_all()
import sys
from datetime import datetime

import yaml
from docopt import docopt
from libcloud.common.exceptions import BaseHTTPError
from libcloud.common.types import LibcloudError
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

from ceph.parallel import parallel
from utility.retry import retry

doc = """
Utility to cleanup orphaned volumes.

 Usage:
  clean_ceph_vols.py [--osp-cred <file>]

Options:
  --osp-cred <file>                 openstack credentials as separate file [default: osp/osp-cred-ci-2.yaml]
"""


def get_openstack_driver(yaml):
    """
    Create an openstack handle from config to manage RHOS-D resources
    """
    OpenStack = get_driver(Provider.OPENSTACK)
    glbs = yaml.get("globals")
    os_cred = glbs.get("openstack-credentials")
    username = os_cred["username"]
    password = os_cred["password"]
    auth_url = os_cred["auth-url"]
    auth_version = os_cred["auth-version"]
    tenant_name = os_cred["tenant-name"]
    service_region = os_cred["service-region"]
    domain_name = os_cred["domain"]
    tenant_domain_id = os_cred["tenant-domain-id"]
    driver = OpenStack(
        username,
        password,
        ex_force_auth_url=auth_url,
        ex_force_auth_version=auth_version,
        ex_tenant_name=tenant_name,
        ex_force_service_region=service_region,
        ex_domain_name=domain_name,
        ex_tenant_domain_id=tenant_domain_id,
    )
    return driver


def volume_cleanup(volume, driver):
    """
    Detach and delete a particular volume
    """
    print("Removing volume", volume.name)
    errors = {}
    timeout = datetime.timedelta(seconds=200)
    starttime = datetime.datetime.now()
    while True:
        try:
            volobj = driver.ex_get_volume(volume.id)
            driver.detach_volume(volobj)
            driver.destroy_volume(volobj)
            break
        except BaseHTTPError as e:
            print(e)
            errors.update({volume.name: e.message})
            if errors:
                if datetime.datetime.now() - starttime > timeout:
                    for vol, err in errors.items():
                        print("Error destroying", vol, ":", err)
                    return 1


def cleanup_ceph_vols(osp_cred):
    """
    Cleanup stale volues with satus deleting, error
    """
    vol_states = ["deleting", "error"]
    driver = get_openstack_driver(osp_cred)
    with parallel() as p:
        for volume in driver.list_volumes():
            if volume.state in vol_states:
                p.spawn(volume_cleanup, volume, driver)
                sleep(1)
    sleep(10)


def list_available_volumes(osp_cred):
    """
    List volumes with state 'available'
    """
    driver = get_openstack_driver(osp_cred)
    avail_count = 0
    avail_size = 0
    for volume in driver.list_volumes():
        if volume.state in "available":
            avail_count = +1
            avail_size = +volume.size
            c_date = volume.extra["created_at"][:10]
            print(
                volume.name,
                "created on",
                c_date,
                "of size",
                volume.size,
                "GB is",
                volume.state,
            )
    if avail_count != "0":
        print(
            avail_count,
            "Volumes in available state using storage capacity",
            avail_size,
            "GB",
        )
    return 0


@retry(LibcloudError, tries=5, delay=15)
def run(args):
    osp_cred_file = args["--osp-cred"]
    if osp_cred_file:
        with open(osp_cred_file, "r") as osp_cred_stream:
            osp_cred = yaml.safe_load(osp_cred_stream)

    cleanup_ceph_vols(osp_cred)
    list_available_volumes(osp_cred)
    return 0


if __name__ == "__main__":
    args = docopt(doc)
    rc = run(args)
    sys.exit(rc)
