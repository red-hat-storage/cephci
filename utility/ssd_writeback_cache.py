import json
import logging
import os
import random
import subprocess
import sys
import time

from docopt import docopt

log = logging.getLogger(__name__)

logging.basicConfig(
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
)

doc = """

This script helps to validate the write back cache flushing of data

usage:
ssd_writeback_cache.py  --pool POOL --image IMAGE [--size=<size>] [--iteration=<iteration>]
Example:
    python3 ssd_writeback_cache.py --pool test --image test123 --size=10 --iteration=10
Options:
  -h --help                 Show this screen
  --pool <name>             Name of the pool
  --image <name>            Image name
  --size=<size>             Size of the image default is 1G     [default: 1]
  --iteration=<iteration>   No of iterations for Ios to run     [default: 10000]
"""


def ssd_write_cache():
    """

    This method to validate constant flushing and
    evicting of data for an extended period of time.
    This scenario in particular
    iterations of SIGKILLed "rbd bench" "fio" on the same image:
    recommend writing a script that would run through a few thousand

    """

    os.system(f"ceph osd pool create {pool_name} 128")
    os.system(f"ceph osd pool application enable {pool_name} rbd")
    os.system(f"rbd pool init -p {pool_name}")
    os.system(
        f"rbd create --size {image_size} {pool_name}/{image_name} --thick-provision"
    )

    for i in range(1, iteration):
        log.info(f"No of Iterations {i}")
        proc = subprocess.Popen(
            [
                f"fio --name=test-1 --ioengine=rbd --pool={pool_name} "
                f"--rbdname={image_name} --numjobs=2 --rw=write --bs=4k "
                f"--iodepth=8 --fsync=32 --runtime=80 --time_based "
                f"--group_reporting --ramp_time=120"
            ],
            shell=True,
        )
        timeout = random.randint(45, 100)
        log.info(f"sleeping for {timeout}")
        time.sleep(timeout)
        os.system(f"kill -9 {proc.pid}")
        output = subprocess.getoutput(
            f"rbd status {pool_name}/{image_name} --format json"
        )
        log.info("image status %s" % output)
        image_info = json.loads(output)
        if image_info.get("image_cache_state"):
            cache_state = json.loads(image_info["image_cache_state"])
            if cache_state.get("clean") == "false":
                raise AssertionError("image status is not in clean state")
    return 0


def del_pool(image_name, pool_name):
    os.system(f"rbd rm {image_name} -p {pool_name}")
    os.system(
        f"ceph osd pool delete {pool_name} {pool_name} --yes-i-really-really-mean-it "
    )


if __name__ == "__main__":
    args = docopt(doc)
    image_size = 1024 * int(args["--size"])
    iteration = int(args["--iteration"])
    pool_name = args["--pool"]
    image_name = args["--image"]
    ssd_write_cache()
    del_pool(image_name, pool_name)
