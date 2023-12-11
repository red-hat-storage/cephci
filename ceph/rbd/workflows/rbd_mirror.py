import ast
import datetime
import json
import time

from ceph.parallel import parallel
from ceph.rbd.utils import copy_file, exec_cmd, getdict, value
from ceph.rbd.workflows.snap_scheduling import add_snapshot_scheduling
from utility.log import Log

log = Log(__name__)


# Get Position
def get_position(rbd, imagespec, pattern=None):
    status_config = {"image-spec": imagespec, "format": "json"}
    image_status, _ = rbd.mirror.image.status(**status_config)
    out = value(
        "description",
        json.loads(image_status),
    )
    if out == "local image is primary":
        raise Exception("Position cannot be determined ")
    if pattern is not None:
        primary_pos = out.find("primary_position")
        mirror_pos = out.find("mirror_position")
        entries_behind = out.find("entries")
        pos = [
            out[primary_pos : mirror_pos - 2],
            out[mirror_pos : entries_behind - 2],
            out[entries_behind:],
        ]
        if "primary" in pattern:
            return pos[0]
        elif "mirror" in pattern:
            return pos[1]
        else:
            return pos[2]
    else:
        return out


def wait_for_status(rbd, cluster_name, **kw):
    """Wait for required mirror status of pool or image.

    Args:
        kw:
        poolname: Name of the pool
        imagespec: Image specification of the image of which status needs to be checked
        state_pattern: Required mirror image state
        description_pattern: Required mirror image description
        retry_interval: sleep duration in between retries.
        ignore_command_failure: true if command failure is to be ignored.
    """
    starttime = datetime.datetime.now()
    if kw.get("tout"):
        tout = kw.get("tout")
    else:
        tout = datetime.timedelta(seconds=1200)
    time.sleep(kw.get("retry_interval", 20))
    while True:
        if kw.get("poolname", False):
            if kw.get("health_pattern"):
                out = value(
                    "health",
                    json.loads(
                        rbd.mirror.pool.status(pool=kw.get("poolname"), format="json")[
                            0
                        ]
                    ),
                )
                log.info(
                    "Health of {} pool in {} cluster: {}".format(
                        kw.get("poolname"), cluster_name, out
                    )
                )
                if kw.get("health_pattern") in out:
                    return 0
            if kw.get("images_pattern"):
                out = value(
                    "states",
                    json.loads(
                        rbd.mirror.pool.status(pool=kw.get("poolname"), format="json")[
                            0
                        ]
                    ),
                )
                out = ast.literal_eval(out)
                state_pattern = kw.get("state", "total")
                num_image = 0
                if "total" in state_pattern:
                    for k, v in out.items():
                        num_image = num_image + v
                else:
                    num_image = out[state_pattern]
                log.info(
                    "Images in {} pool in {} cluster {}: {}, expected is :{}".format(
                        kw.get("poolname"),
                        cluster_name,
                        state_pattern,
                        num_image,
                        kw.get("images_pattern"),
                    )
                )
                if kw.get("images_pattern") == num_image:
                    return 0
        else:
            try:
                if kw.get("state_pattern"):
                    status_config = {
                        "image-spec": kw.get("imagespec"),
                        "format": "json",
                    }
                    image_status = rbd.mirror.image.status(**status_config)
                    out = value(
                        "state",
                        json.loads(image_status[0]),
                    )
                    log.info(
                        f"State of image {kw['imagespec']} : {out}, \
                        waiting for {kw['state_pattern']}"
                    )
                    if kw["state_pattern"] in out:
                        return 0
                if kw.get("description_pattern"):
                    out = get_position(
                        rbd=rbd,
                        imagespec=kw.get("imagespec"),
                        pattern=kw.get("description_pattern"),
                    )
                    log.info(
                        "Description of {} image in {} cluster: {}".format(
                            kw.get("imagespec"), cluster_name, out
                        )
                    )
                    return out
                if kw.get("description"):
                    status_config = {
                        "image-spec": kw.get("imagespec"),
                        "format": "json",
                    }
                    image_status = rbd.mirror.image.status(**status_config)
                    image_description = value(
                        "description",
                        json.loads(image_status[0]),
                    )
                    log.debug(
                        f"Image description: {image_description}, expected {kw['description']}"
                    )
                    if kw["description"] == image_description:
                        return 0
            except Exception:
                if kw.get("state_pattern") == "down+unknown" or kw.get(
                    "ignore_command_failure"
                ):
                    continue
                else:
                    raise
        if datetime.datetime.now() - starttime <= tout:
            time.sleep(kw.get("retry_interval", 20))
        else:
            raise Exception("Required status can not be attained")


def wait_for_replay_complete(rbd, cluster_name, imagespec):
    """Waits till image replay to complete in journal based mirroring.

    Args:
        imagespec: image specification.
    """
    log.info(f"Waiting for {imagespec} to complete replay")
    while True:
        time.sleep(30)
        out = wait_for_status(
            rbd=rbd,
            imagespec=imagespec,
            cluster_name=cluster_name,
            description_pattern="entries",
        )
        out1 = out.split('entries_behind_primary":')
        out2 = out1[1].split(",")
        log.debug(f"entries_behind_primary : {out2[0]}")
        if int(out2[0]) == 0:
            time.sleep(30)
            return out2[0]


def bootstrap_and_add_peers(rbd_primary, rbd_secondary, **kw):
    """ """
    primary_client = kw.get("primary_client")
    secondary_client = kw.get("secondary_client")

    poolname = kw.get("pool_name")

    primary_cluster = kw.get("primary_cluster")
    primary_cluster_name = primary_cluster.name
    secondary_cluster = kw.get("secondary_cluster")
    secondary_cluster_name = secondary_cluster.name

    ceph_cluster_primary = kw.get("ceph_cluster_primary", "ceph")
    ceph_cluster_secondary = kw.get("ceph_cluster_secondary", "ceph")

    peer_mode = kw.get("peer_mode")
    rbd_client = kw.get("rbd_client")
    build = kw.get("build")
    ceph_version = kw.get("ceph_version")
    direction = "rx-only" if kw.get("way") == "one-way" else ""
    if ceph_version >= 4:
        if peer_mode == "bootstrap":
            file_name = "/root/bootstrap_token_primary"
            bootstrap_config = {
                "pool": poolname,
                "site-name": primary_cluster_name,
                "cluster": ceph_cluster_primary,
            }
            out = rbd_primary.mirror.pool.peer.bootstrap.create(**bootstrap_config)
            token = out[0].strip()
            cmd = f"echo {token} > {file_name}"
            rbd_primary.execute_as_sudo(cmd=cmd)

            copy_file(file_name, primary_client, secondary_client)

            import_config = {
                "pool": poolname,
                "site-name": secondary_cluster_name,
                "token-path": file_name,
                "cluster": ceph_cluster_secondary,
            }

            if direction:
                import_config.update({"direction": direction})
            rbd_secondary.mirror.pool.peer.bootstrap.import_(**import_config)

        else:
            primary_mon = ",".join(
                [
                    obj.node.ip_address
                    for obj in primary_cluster.get_ceph_objects(role="mon")
                ]
            )
            secondary_mon = ",".join(
                [
                    obj.node.ip_address
                    for obj in secondary_cluster.get_ceph_objects(role="mon")
                ]
            )
            primary_fsid = secondary_cluster.get_cluster_fsid(build)
            secondary_fsid = primary_cluster.get_cluster_fsid(build)
            secret = exec_cmd(
                node=primary_client,
                cmd=f"ceph auth get-or-create {rbd_client}",
                output=True,
            )
            secret = secret.split(" ")[-1].strip()
            key_file_path = "/etc/ceph/secret_key"
            exec_cmd(node=primary_client, cmd=f"echo {secret} > {key_file_path}")
            exec_cmd(node=secondary_client, cmd=f"echo {secret} > {key_file_path}")

            if "one-way" in kw.get("way", ""):
                peer_config = {
                    "pool": poolname,
                    "remote-cluster-spec": f"{rbd_client}@{primary_fsid}",
                    "remote-client-name": rbd_client,
                    "remote-cluster": primary_cluster_name,
                    "remote-mon-host": primary_mon,
                    "remote-key-file": key_file_path,
                    "direction": direction,
                }
                rbd_secondary.mirror.pool.peer.add_(**peer_config)
            else:
                peer_config = {
                    "pool": poolname,
                    "remote-cluster-spec": f"{rbd_client}@{secondary_fsid}",
                    "remote-client-name": rbd_client,
                    "remote-cluster": secondary_cluster_name,
                    "remote-mon-host": secondary_mon,
                    "remote-key-file": key_file_path,
                    "direction": direction,
                }
                rbd_primary.mirror.pool.peer.add_(**peer_config)
                peer_config = {
                    "pool": poolname,
                    "remote-cluster-spec": f"{rbd_client}@{primary_fsid}",
                    "remote-client-name": rbd_client,
                    "remote-cluster": primary_cluster_name,
                    "remote-mon-host": primary_mon,
                    "remote-key-file": key_file_path,
                    "direction": direction,
                }
                rbd_secondary.mirror.pool.peer.add_(**peer_config)
    else:
        if "one-way" in kw.get("way", ""):
            peer_config = {
                "pool": poolname,
                "remote-cluster-spec": f"{rbd_client}@{primary_cluster_name}",
            }
            rbd_secondary.mirror.pool.peer.add_(**peer_config)
        else:
            peer_config = {
                "pool": poolname,
                "remote-cluster-spec": f"{rbd_client}@{secondary_cluster_name}",
            }
            rbd_primary.mirror.pool.peer.add_(**peer_config)
            peer_config = {
                "pool": poolname,
                "remote-cluster-spec": f"{rbd_client}@{primary_cluster_name}",
            }

    primary_peer_info = value(
        key="peers",
        dictionary=json.loads(
            rbd_primary.mirror.pool.info(
                pool=poolname, format="json", cluster=ceph_cluster_primary
            )[0]
        ),
    )
    secondary_peer_info = value(
        key="peers",
        dictionary=json.loads(
            rbd_secondary.mirror.pool.info(
                pool=poolname, format="json", cluster=ceph_cluster_secondary
            )[0]
        ),
    )
    if primary_peer_info.lower() not in [
        "none",
        "",
        None,
    ] and secondary_peer_info.lower() not in ["none", "", None]:
        log.info("Peers were successfully added")

    else:
        log.error("Peers were not added")


def config_mirror(rbd_primary, rbd_secondary, **kw):
    """
    Configure mirroring on RBD clusters based on the parameters provided
    Args:
        peer_cluster: peer_cluster object for the secondary RBD cluster
        **kw:
            pool_name: poolname to be used for creating pool
            mode: mirroring mode, pool or image to be used
            way: one-way or two-way mirroring
    """
    poolname = kw.get("pool_name")

    primary_cluster = kw.get("primary_cluster")
    primary_cluster_name = primary_cluster.name
    secondary_cluster = kw.get("secondary_cluster")
    secondary_cluster_name = secondary_cluster.name
    mode = kw.get("mode")

    is_wait_for_status = kw.get("wait_for_status", True)

    ceph_cluster_primary = kw.get("ceph_cluster_primary", "ceph")
    ceph_cluster_secondary = kw.get("ceph_cluster_secondary", "ceph")

    enable_config = {
        "pool": poolname,
        "mode": mode,
        "cluster": ceph_cluster_primary,
    }
    out = rbd_primary.mirror.pool.enable(**enable_config)
    log.info(f"Output of RBD mirror pool enable: {out}")

    if "rbd: mirroring is already configured" not in out[0].strip():
        enable_config.update({"cluster": ceph_cluster_secondary})
        out = rbd_secondary.mirror.pool.enable(**enable_config)
        bootstrap_and_add_peers(rbd_primary, rbd_secondary, **kw)
    else:
        log.info(f"RBD Mirroring has already been configured for pool {poolname}")

    # Waiting for OK pool mirror status to be okay based on user input as in image based
    # mirorring status wouldn't reach OK without enabling mirroing on individual images
    if is_wait_for_status:
        wait_for_status(
            rbd=rbd_primary,
            cluster_name=primary_cluster_name,
            poolname=poolname,
            health_pattern="OK",
        )
        wait_for_status(
            rbd=rbd_secondary,
            cluster_name=secondary_cluster_name,
            poolname=poolname,
            health_pattern="OK",
        )


def enable_image_mirroring(primary_config, secondary_config, **kw):
    """ """
    rbd_primary = primary_config.get("rbd")

    rbd_secondary = secondary_config.get("rbd")

    primary_cluster = primary_config.get("cluster")
    secondary_cluster = secondary_config.get("cluster")
    pool = kw.get("pool")
    image = kw.get("image")
    mirrormode = kw.get("mirrormode")
    io_total = kw.get("io_total")

    out = rbd_primary.mirror.image.enable(pool=pool, image=image, mode=mirrormode)

    if "cannot enable mirroring: pool is not in image mirror mode" in out[1].strip():
        return out[1]

    wait_for_status(
        rbd=rbd_primary,
        cluster_name=primary_cluster.name,
        poolname=pool,
        health_pattern="OK",
    )
    wait_for_status(
        rbd=rbd_secondary,
        cluster_name=secondary_cluster.name,
        poolname=pool,
        health_pattern="OK",
    )

    # TBD: We need to override wait_for_status to match images in cluster1==cluster2
    # ITs failing here when the pool contains more than 1 image
    # Using same image pool for replicated and ec pool
    # mirror2.wait_for_status(poolname=poolname, images_pattern=1)

    if io_total:
        bench_config = {
            "io-type": "write",
            "io-threads": "16",
            "io-total": io_total,
            "pool_name": pool,
            "image_name": image,
        }
        rbd_primary.bench(**bench_config)
        time.sleep(60)
    with parallel() as p:
        p.spawn(
            wait_for_status,
            rbd=rbd_primary,
            cluster_name=primary_cluster.name,
            imagespec=f"{pool}/{image}",
            state_pattern="up+stopped",
        )
        p.spawn(
            wait_for_status,
            rbd=rbd_secondary,
            cluster_name=secondary_cluster.name,
            imagespec=f"{pool}/{image}",
            state_pattern="up+replaying",
        )


def config_mirror_multi_pool(
    primary_config, secondary_config, multi_pool_config, is_secondary=False, **kw
):
    """ """
    rbd_primary = primary_config.get("rbd")
    primary_client = primary_config.get("client")

    rbd_secondary = secondary_config.get("rbd")
    secondary_client = secondary_config.get("client")

    primary_cluster = primary_config.get("cluster")
    secondary_cluster = secondary_config.get("cluster")

    config = kw.get("config")
    pool_test_config = multi_pool_config.pop("test_config", None)

    output = ""

    for pool, pool_config in multi_pool_config.items():
        # If any pool level test config is present, pop it out
        # so that it does not get mistaken as another image configuration
        if pool_config.get("mode"):
            pool_config["peer_mode"] = pool_config.get("peer_mode", "bootstrap")
            pool_config["rbd_client"] = pool_config.get("rbd_client", "client.admin")
            if pool_config.get("mode") == "image":
                kw["wait_for_status"] = False
            config_mirror(
                rbd_primary,
                rbd_secondary,
                primary_client=primary_client,
                secondary_client=secondary_client,
                pool_name=pool,
                primary_cluster=primary_cluster,
                secondary_cluster=secondary_cluster,
                build=kw.get("build"),
                ceph_version=int(config.get("rhbuild")[0]),
                **pool_config,
            )

            # Enable image level mirroring only when mode is image type
            if (
                not kw.get("do_not_enable_mirror_on_image")
                and pool_config.get("mode") == "image"
            ) or (
                pool_config.get("mode") == "pool"
                and pool_config.get("mirrormode") == "snapshot"
            ):
                mirrormode = pool_config.get("mirrormode", "")
                multi_image_config = getdict(pool_config)
                image_config = {
                    k: v
                    for k, v in multi_image_config.items()
                    if v.get("is_secondary", False) == is_secondary
                }
                # for image, image_config in multi_image_config.items():
                for image, image_config_val in image_config.items():
                    image_enable_config = {
                        "pool": pool,
                        "image": image,
                        "mirrormode": mirrormode,
                        "io_total": image_config_val.get("io_total", None),
                    }
                    out = enable_image_mirroring(
                        primary_config, secondary_config, **image_enable_config
                    )

                    if (
                        pool_config.get("mode") == "pool"
                        and pool_config.get("mirrormode") == "snapshot"
                    ):
                        if (
                            out
                            and "cannot enable mirroring: pool is not in image mirror mode"
                            in out.strip()
                        ):
                            output = "Snapshot based mirroring cannot be enabled in pool mode"
                        elif not is_secondary:
                            output = (
                                "Snapshot based mirroring did not fail in pool mode"
                            )

                    if image_config_val.get(
                        "snap_schedule_levels"
                    ) and image_config_val.get("snap_schedule_intervals"):
                        for level, interval in zip(
                            image_config_val["snap_schedule_levels"],
                            image_config_val["snap_schedule_intervals"],
                        ):
                            snap_schedule_config = {
                                "pool": pool,
                                "image": image,
                                "level": level,
                                "interval": interval,
                            }
                            out, err = add_snapshot_scheduling(
                                rbd_primary, **snap_schedule_config
                            )
                            if out or err:
                                log.error(
                                    f"Adding snapshot scheduling failed for image {pool}/{image}"
                                )

    # Add back the popped pool test config once configuration is complete
    if pool_test_config:
        pool_config["test_config"] = pool_test_config

    return output
