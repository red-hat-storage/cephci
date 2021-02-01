#!/usr/bin/env python
from gevent import monkey, sleep

monkey.patch_all()
import yaml
import sys
import os
import logging
from docopt import docopt
from libcloud.common.types import LibcloudError
from ceph.utils import get_openstack_driver, volume_cleanup
from ceph.parallel import parallel
from utility.retry import retry
from utility.utils import (
    timestamp,
    create_run_dir,
)


doc = """
A simple test suite wrapper that executes tests based on yaml test configuration

 Usage:
  run.py --cleanup=name [--osp-cred <file>]
        [--log-level <LEVEL>]
  run.py --clean-volumes=yes [--osp-cred <file>]
        [--log-level <LEVEL>]

Options:
  -h --help                         show this screen
  -v --version                      run version
  --osp-cred <file>                 openstack credentials as separate file
  --log-level <LEVEL>               Set logging level
  --log-dir <LEVEL>                 Set log directory [default: /tmp]
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


def cleanup_ceph_vols(osp_cred):
    """
    Cleanup stale volues with satus deleting, available, error, unkonwn, reserved
    """
    vol_states = ["deleting", "available", "error", "unknown", "reserved"]
    driver = get_openstack_driver(osp_cred)
    with parallel() as p:
        for volume in driver.list_volumes():
            if volume.state in vol_states:
                log.info(
                    "volume with id:%s, name:%s has status:%s",
                    volume.id,
                    volume.name,
                    volume.state,
                )
                p.spawn(volume_cleanup, volume, osp_cred)
                sleep(1)
    sleep(30)


@retry(LibcloudError, tries=5, delay=15)
def run(args):
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    osp_cred_file = args["--osp-cred"]
    cleanup_vols = args.get("--clean-volumes", None)
    console_log_level = args.get("--log-level")
    log_directory = args.get("--log-dir", "/tmp")

    # Set log directory and get absolute path
    run_id = timestamp()
    run_dir = create_run_dir(run_id, log_directory)
    startup_log = os.path.join(run_dir, "startup.log")
    print("Startup log location: {}".format(startup_log))
    handler = logging.FileHandler(startup_log)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    root.addHandler(handler)

    if console_log_level:
        ch.setLevel(logging.getLevelName(console_log_level.upper()))

    if osp_cred_file:
        with open(osp_cred_file, "r") as osp_cred_stream:
            osp_cred = yaml.safe_load(osp_cred_stream)

    if cleanup_vols is not None:
        cleanup_ceph_vols(osp_cred)
        return 0


if __name__ == "__main__":
    args = docopt(doc)
    rc = run(args)
    log.info("final rc of test run %d" % rc)
    sys.exit(rc)
