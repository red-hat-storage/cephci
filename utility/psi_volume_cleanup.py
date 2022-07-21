#!/usr/bin/env python3
"""Utility to remove orphaned volumes."""
from gevent import monkey, sleep

monkey.patch_all()
import sys
from datetime import datetime, timedelta

import yaml
from docopt import docopt
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

from ceph.parallel import parallel

doc = """
Utility to cleanup orphaned volumes.

    Usage:
        clean_ceph_vols.py [--osp-cred <file>]

    Options:
        --osp-cred <file>     Credential file [default: ~/osp-cred-ci-2.yaml]
"""


def get_openstack_driver(yaml, project):
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
    tenant_name = project
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
    """Detach and delete a particular volume."""
    print("Removing volume", volume.name)
    try:
        driver.detach_volume(volume)
        driver.destroy_volume(volume)
    except BaseException as e:
        print(e)


def cleanup_ceph_vols(osp_cred):
    """Cleanup stale volumes with satus deleting, error."""
    projects = ["ceph-jenkins", "ceph-core", "ceph-ci"]
    vol_states = ["deleting", "error", "available"]
    for each_project in projects:
        print("Checking in project : ", each_project)
        driver = get_openstack_driver(osp_cred, each_project)
        with parallel() as p:
            for volume in driver.list_volumes():
                if volume.state not in vol_states:
                    continue
                if volume.state == "available":
                    create_datetime = datetime.strptime(
                        volume.extra["created_at"], "%Y-%m-%dT%H:%M:%S.%f"
                    )
                    allowed_duration = create_datetime + timedelta(minutes=30)
                    if allowed_duration > datetime.utcnow():
                        continue
                p.spawn(volume_cleanup, volume, driver)
                sleep(1)
        sleep(10)


def run(args):
    osp_cred_file = args["--osp-cred"]
    if osp_cred_file:
        with open(osp_cred_file, "r") as osp_cred_stream:
            osp_cred = yaml.safe_load(osp_cred_stream)

    cleanup_ceph_vols(osp_cred)
    return 0


if __name__ == "__main__":
    args = docopt(doc)
    rc = run(args)
    sys.exit(rc)
