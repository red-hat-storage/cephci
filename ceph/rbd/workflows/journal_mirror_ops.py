import datetime
import time

from ceph.rbd.initial_config import random_string
from ceph.rbd.utils import get_md5sum_rbd_image
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.rbd_mirror import wait_for_replay_complete
from utility.log import Log

log = Log(__name__)


def config_mirroring_delay(**kw):
    """
    Config rbd mirroring delay as per inputs specified
    kw: {
        "delay": <value in seconds>,
        "delay_per_image": <true/false>,
        "rbd": <rbd object>,
        "client": <client node>,
        "pool": <pool>,
        "image": <image>,
        "operation": <set/get/remove>
    }
    """
    rbd = kw.get("rbd")
    client = kw.get("client")
    delay = kw.get("delay")
    pool = kw.get("pool")
    image = kw.get("image")
    operation = kw.get("operation")
    if kw.get("delay_per_image"):
        if operation == "set":
            _, err = rbd.config.image.set(
                pool=pool,
                image=image,
                key="rbd_mirroring_replay_delay",
                value=delay,
            )
        elif operation == "remove":
            out, err = rbd.config.image.remove(
                pool=pool, image=image, key="rbd_mirroring_replay_delay"
            )
            if not err:
                return out
        elif operation == "get":
            out, err = rbd.config.image.get(
                pool=pool, image=image, key="rbd_mirroring_replay_delay"
            )
            if not err:
                return out
        if err:
            log.error(
                f"Performing operation {operation} rbd_mirroring_replay_delay failed for {pool}/{image}"
            )
            return 1
    else:
        if operation == "set":
            _, err = client.exec_command(
                cmd=f"ceph config set client rbd_mirroring_replay_delay {delay}",
                sudo=True,
            )
        elif operation == "get":
            out, err = client.exec_command(
                cmd="ceph config get client rbd_mirroring_replay_delay", sudo=True
            )
            if not err:
                return out
        elif operation == "remove":
            out, err = client.exec_command(
                cmd="ceph config rm client rbd_mirroring_replay_delay", sudo=True
            )
            if not err:
                return out
        if err:
            log.error("Setting rbd_mirroring_replay_delay failed")
            return 1
    return 0


def run_io_wait_for_replay_complete(**kw):
    """
    Run IOs on the given image and wait for replay complete
    kw: {
        "rbd": <>,
        "sec_rbd": <>,
        "client": <>,
        "pool": <>,
        "image": <>,
        "mount_path": <>,
        "skip_mkfs": <>,
        "sec_cluster_name": <>,
        "image_config": {
            "size": <>,
            "io_size": <>,
        }
    }
    """
    rbd = kw.get("rbd")
    sec_rbd = kw.get("sec_rbd")
    client = kw.get("client")
    pool = kw.get("pool")
    image = kw.get("image")
    image_spec = f"{pool}/{image}"
    image_config = kw.get("image_config")
    sec_cluster_name = kw.get("sec_cluster_name")

    io_size = image_config.get("io_size", int(int(image_config["size"][:-1]) / 3))
    io_config = {
        "rbd_obj": rbd,
        "client": client,
        "size": image_config["size"],
        "do_not_create_image": True,
        "config": {
            "file_size": io_size,
            "file_path": [f"{kw['mount_path']}"],
            "get_time_taken": True,
            "image_spec": [image_spec],
            "operations": {
                "fs": "ext4",
                "io": True,
                "mount": True,
                "nounmap": False,
                "device_map": True,
            },
            "skip_mkfs": kw["skip_mkfs"],
        },
    }
    krbd_io_handler(**io_config)
    kw["io_config"] = io_config
    out = wait_for_replay_complete(sec_rbd, sec_cluster_name, image_spec)
    if int(out):
        log.error(f"Replay completion failed for image {pool}/{image}")
        if kw.get("raise_exception"):
            raise Exception(f"Replay completion failed for image {pool}/{image}")
        return 1
    return 0


def write_data_and_verify_no_mirror_till_delay(**kw):
    """
    Run IOs on the given image and verify that no
    mirroring happens until delay
    kw: {
        "rbd": <>,
        "client": <>,
        "sec_rbd": <>,
        "sec_client": <>,
        "initial_md5sums": <>,
        "pool": <>,
        "image": <>,
        "mount_path": <>,
        "skip_mkfs": <>,
        "cluster_name": <>,
        "image_config": {
            "size": <>,
            "io_size": <>,
        }
    }
    """
    rbd = kw.get("rbd")
    client = kw.get("client")
    sec_rbd = kw.get("sec_rbd")
    sec_client = kw.get("sec_client")
    pool = kw.get("pool")
    image = kw.get("image")
    image_spec = f"{pool}/{image}"
    image_config = kw.get("image_config")
    delay = kw.get("delay")

    exp_path = f"/tmp/{random_string(len=3)}"
    md5sum_before_delay = get_md5sum_rbd_image(
        image_spec=image_spec,
        file_path=exp_path,
        rbd=rbd,
        client=client,
    )

    starttime = datetime.datetime.now()
    wait_till = datetime.timedelta(seconds=(delay - 5))

    io_size = image_config.get("io_size", int(int(image_config["size"][:-1]) / 3))
    io_config = {
        "rbd_obj": rbd,
        "client": client,
        "size": image_config["size"],
        "do_not_create_image": True,
        "config": {
            "file_size": io_size,
            "file_path": [f"{kw['mount_path']}"],
            "get_time_taken": True,
            "image_spec": [image_spec],
            "operations": {
                "fs": "ext4",
                "io": True,
                "mount": True,
                "nounmap": False,
                "device_map": True,
            },
            "skip_mkfs": kw["skip_mkfs"],
        },
    }
    krbd_io_handler(**io_config)
    kw["io_config"] = io_config

    while datetime.datetime.now() - starttime <= wait_till:
        exp_path = f"/tmp/{random_string(len=3)}"
        md5_sum_secondary = get_md5sum_rbd_image(
            image_spec=image_spec, file_path=exp_path, rbd=sec_rbd, client=sec_client
        )
        if md5sum_before_delay != md5_sum_secondary:
            log.error("Mirroring activity seen on secondary without desired delay")
            return 1
        log.debug(
            "Verified that no data has been added to secondary image in last 50 seconds"
        )
        client.exec_command(cmd=f"rm -rf {exp_path}", sudo=True)
        time.sleep(50)

    log.info("No mirroring ios seen in secondary until the delay, Test case passed")
    return 0
