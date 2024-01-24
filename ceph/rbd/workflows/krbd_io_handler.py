from ceph.rbd.utils import create_map_options, exec_cmd
from ceph.rbd.workflows.cleanup import device_cleanup
from cli.rbd.rbd import Rbd
from utility.log import Log
from utility.utils import run_fio, run_mkfs

log = Log(__name__)


def run(**kw):
    return krbd_io_handler(**kw)


def krbd_io_handler(**kw):
    """krbd io handler.
    Usage:
        config:
          operations:
            map: true
            mount: true
            fs: ext4/xfs
            io: true
            fsck: true
            nounmap: false
            device_map: true
          image_spec:
            - pool_name/image_name
          runtime: 30
          file_size: 100M
          device_names:
            - /dev/<>  # newly created devices will be returned in this parameter
          file_path:
            - /<path>/<name> # Pass the file path with file name where image needs to be mounted to run IOs
          encryption_config:
            - <encryption_type>: <passphrase> # List of encryptions and passphrases to be passed for device map
          skip_mkfs: true # Will skip creating new filesystem while mounting a device.
        rbd: The RBD object to be used for testing, if not passed a new RBD object will be created.
        do_not_create_image: Default False, Set True if image on which operations need to be executed already exists
        client: client node to be used
        read_only: True if map and mount needs to happen with read only configuration
        Note:
        1) if mount is set to true, a mount point will be created and fs will be created on the device by default
        2) Required pool needs to be created.
        3) Run time is per image
    """
    client = kw.get("client")
    if kw.get("rbd_obj"):
        rbd = kw.get("rbd_obj")
    else:
        rbd = Rbd(client)
    config = kw.get("config")
    log.debug(f"Config received for execution: {config}")

    operations = config["operations"]
    device_names = []
    mount_points = []
    return_flag = 0

    for image_spec in config["image_spec"]:
        pool_name = image_spec.split("/")[0]
        image_name = image_spec.split("/")[1]
        try:
            if not kw.get("do_not_create_image"):
                create_config = {
                    "pool": pool_name,
                    "image": image_name,
                    "size": config.get("size", "1G"),
                }
                if config.get("is_ec_pool"):
                    create_config.update({"data-pool": config.get("data_pool")})
                if config.get("thick_provision"):
                    create_config.update(
                        {"thick_provision": config.get("thick_provision")}
                    )
                if rbd.create(**create_config)[0]:
                    log.error(f"Image {image_name} creation failed")
                    return 1, "Image creation failed"

            if operations.get("map"):
                device_names.append(rbd.map(pool=pool_name, image=image_name)[:-1])

            if operations.get("device_map"):
                out = exec_cmd(
                    cmd="rpm -qa|grep rbd-nbd", node=client, sudo=True, output=True
                )
                if not out or out == 1 or "rbd-nbd" not in out:
                    exec_cmd(cmd="dnf install rbd-nbd -y", node=client, sudo=True)

                map_config = {
                    "pool": pool_name,
                    "image": image_name,
                    "device-type": config.get("device_type", "nbd"),
                }
                if config.get("encryption_config"):
                    options = create_map_options(config.get("encryption_config"))
                    map_config.update(
                        {
                            "options": options,
                        }
                    )
                if kw.get("read_only"):
                    map_config.update({"read-only": True})

                out, err = rbd.device.map(**map_config)
                if err:
                    log.error(
                        f"RBD device map failed for {image_spec} and "
                        f"encryption_config: {config.get('encryption_config')}"
                    )
                    return 1, err
                device_names.append(out.strip())

            image_index = config["image_spec"].index(image_spec)

            if operations.get("mount"):
                if not config.get("skip_mkfs"):
                    run_mkfs(
                        client_node=client,
                        device_name=device_names[-1][0].strip(),
                        type=operations.get("fs"),
                    )
                # Creating mount directory and mouting device
                mount_point = (
                    config.get("file_path")[image_index].rsplit("/", 1)[0]
                    if config.get("file_path")
                    else f"/tmp/mp_{device_names[-1].split('/')[-1]}"
                )
                mount_points.append(mount_point)

                mount_cmd = f"mkdir {mount_point}; mount {device_names[-1][0].strip()} {mount_point}"
                if kw.get("read_only"):
                    mount_cmd += " -o ro,noload"
                out, err = exec_cmd(cmd=mount_cmd, node=client, all=True)
                if err:
                    log.error(f"Mount failed for device: {device_names[-1]}")
                    return 1, err

            if operations.get("io"):
                if operations.get("mount"):
                    file_name = (
                        config.get("file_path")[image_index]
                        if config.get("file_path")
                        else f"{mount_point}/{image_name}"
                    )
                    config["time_taken"] = run_fio(
                        client_node=client,
                        filename=file_name,
                        run_time=config.get("run_time", 30),
                        size=config.get("file_size", "100M"),
                        get_time_taken=config.get("get_time_taken", False),
                        cmd_timeout=config.get("cmd_timeout"),
                        io_type=config.get("io_type", "write"),
                    )
                else:
                    fio_args = {
                        "client_node": rbd.ceph_client,
                        "device_name": device_names[-1][0].strip(),
                    }
                    if config.get("io_size"):
                        fio_args.update({"size": config.get("io_size")})
                    else:
                        fio_args.update({"run_time": config.get("run_time", 30)})
                    if config.get("get_time_taken"):
                        fio_args.update(
                            {"get_time_taken": config.get("get_time_taken")}
                        )
                    if config.get("cmd_timeout"):
                        fio_args.update({"cmd_timeout": config.get("cmd_timeout")})

                    config["time_taken"] = run_fio(**fio_args)

            if operations.get("fsck"):
                if exec_cmd(node=client, cmd=f"fsck -n {device_names[-1][0].strip()}"):
                    log.debug("fsck failed")
                    return_flag = 1

        finally:
            if not operations.get("nounmap"):
                for mount_point, device_name in zip(mount_points, device_names):
                    cleanup_config = {
                        "rbd": rbd,
                        "client": client,
                        "file_name": mount_point,
                        "device_name": device_name[0].strip(),
                    }
                    if operations.get("device_map"):
                        cleanup_config.update(
                            {"device_type": config.get("device_type", "nbd")}
                        )
                    return_flag = device_cleanup(**cleanup_config)
    kw["config"]["device_names"] = device_names
    return return_flag, ""
