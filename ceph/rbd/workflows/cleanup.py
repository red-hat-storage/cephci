import json
from time import sleep

from ceph.rbd.utils import exec_cmd, getdict
from utility.log import Log

log = Log(__name__)


def cleanup(pool_types, multi_cluster_obj, **kw):
    """ """
    pools = list()
    for pool_type in pool_types:
        multi_pool_config = kw.get("config").get(pool_type)
        pools.extend(
            [key for key in getdict(multi_pool_config).keys() if key != "test_config"]
        )
        pools.extend(
            [
                val["data_pool"]
                for val in getdict(multi_pool_config).values()
                if val.get("data_pool")
            ]
        )
    multi_cluster_obj.pop("output", {})
    for cluster_config in multi_cluster_obj.values():
        pool_cleanup(
            cluster_config.get("client"),
            pools,
            ceph_version=int(kw["config"].get("rhbuild")[0]),
        )


def pool_cleanup(client, pools, **kw):
    """Remove all the specified pools from the cluster

    Args:
        client: client node for cluster from which pools to be removed
        pools: list of pools to be removed
        kw:
            dir_name: if any folders to be deleted.
    """
    if kw.get("dir_name"):
        exec_cmd(node=client, cmd="rm -rf {}".format(kw.get("dir_name")))

    ceph_version = kw.get("ceph_version")

    if ceph_version and ceph_version >= 5:
        exec_cmd(node=client, cmd="ceph config set mon mon_allow_pool_delete true")
        sleep(20)

    for pool in pools:
        exec_cmd(
            cmd=f"ceph osd pool delete {pool} {pool} " "--yes-i-really-really-mean-it",
            node=client,
        )


def unmount(client, mount_point):
    """ """
    flag = 0
    umount_cmd = f"umount -f {mount_point}"
    # if kw.get("read_only"):
    #     umount_cmd += " -o ro,noload"
    if exec_cmd(cmd=umount_cmd, sudo=True, node=client):
        log.error(f"Umount failed for {mount_point}")
        flag = 1

    if exec_cmd(cmd=f"rm -rf {mount_point}", sudo=True, node=client):
        log.error(f"Remove dir failed for {mount_point}")
        flag = 1

    return flag


def device_cleanup(rbd, client, **kw):
    """
    Unmout the given file_name, unmap the given device_name encrypted using encryption_config
    Args:
        rbd: rbd object
        kw:
            "file_name": <path>/<file> to be unmounted
            "passphrase_file": <path>/<passphrase_file> to be removed
            "image_spec": image which was encrypted
            "device_type": default "nbd"
            "device_name": device created for image map without encryption
            "all": if True will unmount and unmap all devices to type specified in device_type
                    (only nbd is supported as of now)
    """
    flag = 0

    if kw.get("all"):
        device_type = kw.get("device_type", "nbd")

        if device_type == "nbd":
            cmd = "lsblk --include 43 --json"
            out = exec_cmd(cmd=cmd, sudo=True, node=client, output=True)
            if out:
                nbd_devices = json.loads(out)

                for devices in nbd_devices.get("blockdevices"):
                    device_name = f"/dev/{devices.get('name')}"
                    mount_point = devices.get("mountpoints")
                    for mnt_pnt in mount_point:
                        if mnt_pnt is not None:
                            unmount(mount_point=mnt_pnt, client=client)
                    map_config = {
                        "image-snap-or-device-spec": device_name,
                        "device-type": kw.get("device_type", "nbd"),
                    }
                    _, err = rbd.device.unmap(**map_config)
                    if err:
                        log.error(f"Device unmap failed for {device_name} ")
                        flag = 1

    if kw.get("file_name"):
        file_name = kw.get("file_name")
        unmount(mount_point=file_name, client=client)

    if kw.get("passphrase_file") and exec_cmd(
        cmd=f"rm -rf {kw.get('passphrase_file')}", sudo=True, node=client
    ):
        log.error(f"Remove passphrase file failed for {kw.get('passphrase_file')}")
        flag = 1

    if kw.get("image_spec"):
        pool_name = kw["image_spec"].split("/")[0]
        image_name = kw["image_spec"].split("/")[1]
        map_config = {
            "pool": pool_name,
            "image": image_name,
            "device-type": kw.get("device_type", "nbd"),
        }

        _, err = rbd.device.unmap(**map_config)
        if err:
            log.error(f"Device unmap failed for {pool_name}/{image_name} ")
            flag = 1

    if kw.get("device_name"):
        if kw.get("device_type"):
            # RBD image map cleanup with device map
            map_config = {
                "image-snap-or-device-spec": kw["device_name"],
                "device-type": kw.get("device_type", "nbd"),
            }
            _, err = rbd.device.unmap(**map_config)

        else:
            # RBD image map cleanup without device map
            map_config = {"image-or-snap-or-device-spec": kw["device_name"]}
            _, err = rbd.unmap(**map_config)

        if err:
            log.error(f"Device unmap failed for {kw['device_name']} ")
            flag = 1

    return flag
