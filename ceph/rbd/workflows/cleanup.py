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
    """Unmount a filesystem and remove the mount directory.

    The mount directory is removed only when umount succeeds. Removing a
    still-mounted path (or unmapping underneath it) can leave a stale
    filesystem referencing a gone device.
    """
    umount_cmd = f"umount -f {mount_point}"
    if exec_cmd(cmd=umount_cmd, sudo=True, node=client):
        log.error(
            f"Unable to unmount {mount_point}. "
            "Skipping mount directory removal to avoid operating on a live mount."
        )
        return 1

    if exec_cmd(cmd=f"rm -rf {mount_point}", sudo=True, node=client):
        log.error(f"Remove dir failed for {mount_point}")
        return 1

    return 0


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

    Note:
        If unmount fails, device unmap is intentionally skipped so a live
        filesystem is never left pointing at an unmapped NBD/RBD device.
    """
    flag = 0
    unmount_failed = False

    if kw.get("all"):
        device_type = kw.get("device_type", "nbd")

        if device_type == "nbd":
            cmd = "lsblk --include 43 --json"
            out = exec_cmd(cmd=cmd, sudo=True, node=client, output=True)
            if out and out != 1:
                nbd_devices = json.loads(out)

                for devices in nbd_devices.get("blockdevices"):
                    device_name = f"/dev/{devices.get('name')}"
                    mount_points = devices.get("mountpoints") or []
                    device_unmount_failed = False
                    for mnt_pnt in mount_points:
                        if mnt_pnt is not None:
                            if unmount(mount_point=mnt_pnt, client=client):
                                device_unmount_failed = True
                                flag = 1
                    if device_unmount_failed:
                        log.error(
                            f"Unable to unmount filesystem(s) for {device_name}. "
                            f"Skipping unmap of {device_name} to avoid leaving a "
                            "stale filesystem mount."
                        )
                        continue
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
        if unmount(mount_point=file_name, client=client):
            unmount_failed = True
            flag = 1
            log.error(
                f"Unable to unmount {file_name}. "
                f"Skipping unmap of {kw.get('device_name') or kw.get('image_spec')} "
                "to avoid leaving a stale filesystem mount."
            )

    if kw.get("passphrase_file") and exec_cmd(
        cmd=f"rm -rf {kw.get('passphrase_file')}", sudo=True, node=client
    ):
        log.error(f"Remove passphrase file failed for {kw.get('passphrase_file')}")
        flag = 1

    if unmount_failed:
        return flag

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
        if kw.get("device-type"):
            # RBD image map cleanup with device map
            map_config = {
                "image-snap-or-device-spec": kw["device_name"],
                "device-type": kw.get("device_type", "nbd"),
            }
            if kw.get("options"):
                map_config.update({"options": kw.get("options")})
            _, err = rbd.device.unmap(**map_config)

        else:
            # RBD image map cleanup without device map
            map_config = {"image-or-snap-or-device-spec": kw["device_name"]}
            _, err = rbd.unmap(**map_config)

        if err:
            log.error(f"Device unmap failed for {kw['device_name']} ")
            flag = 1

    return flag
