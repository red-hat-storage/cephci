from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.rbd import config_rbd_multi_pool
from ceph.rbd.workflows.rbd_mirror import config_mirror_multi_pool
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def update_config(**kw):
    """
    Takes the configuration options given by user and updates it to the following format.
    If the configuration is given in the below format, it just returns the same,
    else it creates the formatting based on default values and/or the minimal config provided.
    Format:
        Note: pg_num and pgp_num are given as 64, 64 by default. Only if user specifies a value,
        it will be considered while creating below format, otherwise we let the method to create pool
        take care of default values
        config:
            do_not_create_image: True  # if not set then images will be created by default
            ec-pool-only: False
            rep-pool-only: False
            rep_pool_config:
                pool_1:
                    pg_num:
                    pgp_num:
                    image_1:
                        size:
                        io_total:
                        is_secondary: # if true the image in this cluster will
                            # become secondary, else by default it will be primary
                    image_2:
                        size:
                        io_total:
                pool_2:
                    pg_num:
                    pgp_num:
                    image_1:
                        size:
                        io_total:
                    image_2:
                        size:
                        io_total:
            ec_pool_config:
                pool_1:
                    data_pool:
                    ec-pool-k-m: 2,1
                    ec_profile:
                    pg_num:
                    pgp_num:
                    ec_pg_num:
                    ec_pgp_num:
                    image_1:
                        size:
                        io_total:
                        is_secondary: # if true the image in this cluster will
                            # become secondary, else by default it will be primary
                    image_2:
                        size:
                        io_total:
                pool_2:
                    data_pool:
                    ec-pool-k-m: 2,1
                    ec_profile:
                    pg_num:
                    pgp_num:
                    ec_pg_num:
                    ec_pgp_num:
                    image_1:
                        size:
                        io_total:
                    image_2:
                        size:
                        io_total:
    """
    # This method uses the mutable property of dictionaries to update kw with required values'
    pool_types = []
    config = kw.get("config", {})
    if not config or not config.get("ec-pool-only"):
        pool_types.append("rep_pool_config")
        if not config or not config.get("rep_pool_config"):
            log.info(
                "No configuration provided, so creating one replicated pool and one image with default parameters"
            )
            pool = "rep_pool_" + random_string(len=5)
            image = "rep_image_" + random_string(len=5)
            rep_pool_config = {pool: {image: {"size": "1G", "io_total": "1G"}}}
            kw["config"] = rep_pool_config
        elif config.get("rep_pool_config"):
            rep_pool_config = config.get("rep_pool_config")

            # If all values of rep_pool_config are dictionaries it means
            # it is already in required format, so return it as it is
            # Only iterate over required number of pools if config is
            # already not in required format and num_pools parameter is specified
            is_pool_config_updated = [
                k for k in getdict(rep_pool_config).keys() if k not in ["test_config"]
            ]

            if not is_pool_config_updated and rep_pool_config.get("num_pools"):
                for i in range(rep_pool_config.get("num_pools")):
                    pool = rep_pool_config.get(
                        "pool_prefix", "rep_pool_" + random_string(len=5)
                    ) + str(i)
                    if rep_pool_config.get("pg_num"):
                        kw["config"]["rep_pool_config"] = {
                            pool: {
                                "pg_num": rep_pool_config.get("pg_num"),
                                "pgp_num": rep_pool_config.get("pgp_num"),
                            }
                        }
                    image_config = {}
                    # if rep_pool_config.get("num_images") != 0:
                    num_images = rep_pool_config.get("num_images", 1)
                    num_secondary_images = rep_pool_config.get(
                        "num_secondary_images", 0
                    )
                    is_secondary = kw.get("is_secondary")
                    if is_secondary:
                        num_images_to_be_created = num_secondary_images
                    else:
                        num_images_to_be_created = num_images - num_secondary_images
                    # for j in range(rep_pool_config.get("num_images", 1)):
                    for j in range(num_images_to_be_created):
                        image = rep_pool_config.get(
                            "image_prefix", "rep_image_" + random_string(len=5)
                        ) + str(j)
                        image_config.update(
                            {
                                image: {
                                    "size": rep_pool_config.get("size", "1G"),
                                }
                            }
                        )
                        if not config.get("do_not_run_io"):
                            image_config[image].update(
                                {"io_total": rep_pool_config.get("io_total", "1G")}
                            )

                        if (
                            kw.get("is_mirror")
                            and rep_pool_config.get("mirrormode") != "snapshot"
                        ):
                            image_config[image].update(
                                {"image-feature": "exclusive-lock,journaling"}
                            )

                        if is_secondary:
                            image_config[image].update({"is_secondary": True})

                        if rep_pool_config.get("snap_schedule_levels"):
                            image_config[image].update(
                                {
                                    "snap_schedule_levels": rep_pool_config[
                                        "snap_schedule_levels"
                                    ]
                                }
                            )

                        if rep_pool_config.get("snap_schedule_intervals"):
                            image_config[image].update(
                                {
                                    "snap_schedule_intervals": rep_pool_config[
                                        "snap_schedule_intervals"
                                    ]
                                }
                            )
                        if rep_pool_config.get("io_size"):
                            image_config[image].update(
                                {"io_size": rep_pool_config["io_size"]}
                            )

                    if rep_pool_config.get(pool):
                        rep_pool_config[pool].update(image_config)
                    else:
                        rep_pool_config[pool] = image_config

                    if rep_pool_config.get("mode"):
                        rep_pool_config[pool].update({"mode": rep_pool_config["mode"]})

                    if rep_pool_config.get("mirrormode"):
                        rep_pool_config[pool].update(
                            {"mirrormode": rep_pool_config["mirrormode"]}
                        )

                    if rep_pool_config.get("way"):
                        rep_pool_config[pool].update({"way": rep_pool_config["way"]})

    if not kw.get("config").get("rep-pool-only"):
        pool_types.append("ec_pool_config")
        if not config.get("ec_pool_config"):
            log.info(
                "No configuration provided, so creating one EC pool and one image with default parameters"
            )
            pool = "ec_pool_" + random_string(len=5)
            image = "ec_image_" + random_string(len=5)
            ec_pool_config = {pool: {image: {"size": "1G", "io_total": "1G"}}}
            kw["config"] = ec_pool_config
        elif config.get("ec_pool_config"):
            ec_pool_config = config.get("ec_pool_config")

            # If all values of rep_pool_config are dictionaries it means
            # it is already in required format, so return it as it is
            # Only iterate over required number of pools if config is
            # already not in required format and num_pools parameter is specified
            is_pool_config_updated = [
                k for k in getdict(ec_pool_config).keys() if k not in ["test_config"]
            ]
            if not is_pool_config_updated and ec_pool_config.get("num_pools"):
                for i in range(ec_pool_config.get("num_pools")):
                    pool = ec_pool_config.get(
                        "pool_prefix", "ec_pool_" + random_string(len=5)
                    ) + str(i)
                    data_pool = ec_pool_config.get(
                        "data_pool_prefix", "data_pool_" + random_string(len=5)
                    ) + str(i)
                    if ec_pool_config.get("pg_num"):
                        kw["config"]["ec_pool_config"] = {
                            pool: {
                                "pg_num": ec_pool_config.get("pg_num"),
                                "pgp_num": ec_pool_config.get("pgp_num"),
                                "ec_pg_num": ec_pool_config.get("ec_pg_num"),
                                "ec_pgp_num": ec_pool_config.get("ec_pgp_num"),
                            }
                        }
                    image_config = {}
                    # if ec_pool_config.get("num_images") != 0:
                    num_images = ec_pool_config.get("num_images", 1)
                    num_secondary_images = ec_pool_config.get("num_secondary_images", 0)
                    is_secondary = kw.get("is_secondary")
                    if is_secondary:
                        num_images_to_be_created = num_secondary_images
                    else:
                        num_images_to_be_created = num_images - num_secondary_images
                    # for j in range(ec_pool_config.get("num_images", 1)):
                    for j in range(num_images_to_be_created):
                        image = ec_pool_config.get(
                            "image_prefix", "ec_image_" + random_string(len=5)
                        ) + str(j)
                        image_config.update(
                            {
                                image: {
                                    "size": ec_pool_config.get("size", "1G"),
                                }
                            }
                        )
                        if not config.get("do_not_run_io"):
                            image_config[image].update(
                                {"io_total": ec_pool_config.get("io_total", "1G")}
                            )

                        if (
                            kw.get("is_mirror")
                            and ec_pool_config.get("mirrormode") != "snapshot"
                        ):
                            image_config[image].update(
                                {"image-feature": "exclusive-lock,journaling"}
                            )

                        if is_secondary:
                            image_config[image].update({"is_secondary": True})

                        if ec_pool_config.get("snap_schedule_levels"):
                            image_config[image].update(
                                {
                                    "snap_schedule_levels": ec_pool_config[
                                        "snap_schedule_levels"
                                    ]
                                }
                            )

                        if ec_pool_config.get("snap_schedule_intervals"):
                            image_config[image].update(
                                {
                                    "snap_schedule_intervals": ec_pool_config[
                                        "snap_schedule_intervals"
                                    ]
                                }
                            )

                        if ec_pool_config.get("io_size"):
                            image_config[image].update(
                                {"io_size": ec_pool_config["io_size"]}
                            )

                    if ec_pool_config.get(pool):
                        ec_pool_config[pool].update(image_config)
                    else:
                        ec_pool_config[pool] = image_config
                    ec_pool_config[pool].update({"data_pool": data_pool})

                    if ec_pool_config.get("mode"):
                        ec_pool_config[pool].update({"mode": ec_pool_config["mode"]})

                    if ec_pool_config.get("mirrormode"):
                        ec_pool_config[pool].update(
                            {"mirrormode": ec_pool_config["mirrormode"]}
                        )
                    if ec_pool_config.get("ec-pool-k-m"):
                        ec_pool_config[pool].update(
                            {"ec-pool-k-m": ec_pool_config["ec-pool-k-m"]}
                        )
                    if ec_pool_config.get("ec_profile"):
                        ec_pool_config[pool].update(
                            {"ec_profile": ec_pool_config["ec_profile"]}
                        )
                    if ec_pool_config.get("way"):
                        ec_pool_config[pool].update({"way": ec_pool_config["way"]})
    return pool_types


def initial_rbd_config(ceph_cluster, **kw):  # ,
    """
    Create replicated pools, ecpools or both and corresponding images based on arguments specified
    Args:
        **kw:

    Examples: In default configuration, pool and image names will be taken as random values.
        Default configuration for ecpools only :
            config:
                ec-pool-only: True
        Default configuration for replicated pools only :
            config:
                rep-pool-only: True
        Advanced configuration <specifying only number of pools and images>:
            config:
                do_not_create_image: True     # if not set then images will be created by default
                ec-pool-only: False
                rep-pool-only: False
                rep_pool_config:
                    num_pools:
                    num_images:
                    num_secondary_images: # num of images out of num_images to be secondary
                    # in given cluster, (num_images - num_secondary_images) number of
                    # images will be created as primary in given cluster in same pool
                    pool_prefix:
                    image_prefix:
                    size:
                    io_total:
                    pg_num:
                    pgp_num:
                    ec_pg_num:
                    ec_pgp_num:
                ec_pool_config:
                    ec-pool-k-m: 2,1
                    num_pools:
                    num_images:
                    num_secondary_images: # num of images out of num_images to be secondary
                    # in given cluster, (num_images - num_secondary_images) number of
                    # images will be created as primary in given cluster in same pool
                    pool_prefix:
                    image_prefix:
                    data_pool_prefix:
                    size:
                    io_total:
                    pg_num:
                    pgp_num:
                    ec_pg_num:
                    ec_pgp_num:
        Advanced configuration <specifying only detailed pool and image configuration>:
            config:
                do_not_create_image: True  # if not set then images will be created by default
                ec-pool-only: False
                rep-pool-only: False
                rep_pool_config:
                    pool_1:
                        pg_num:
                        pgp_num:
                        image_1:
                            size:
                            io_total:
                            is_secondary: # if true the image in this cluster will
                            # become secondary, else by default it will be primary
                        image_2:
                            size:
                            io_total:
                    pool_2:
                        pg_num:
                        pgp_num:
                        image_1:
                            size:
                            io_total:
                        image_2:
                            size:
                            io_total:
                ec_pool_config:
                    pool_1:
                        data_pool:
                        ec-pool-k-m: 2,1
                        ec_profile:
                        pg_num:
                        pgp_num:
                        ec_pg_num:
                        ec_pgp_num:
                        image_1:
                            size:
                            io_total:
                            is_secondary: # if true the image in this cluster will
                            # become secondary, else by default it will be primary
                        image_2:
                            size:
                            io_total:
                    pool_2:
                        data_pool:
                        ec-pool-k-m: 2,1
                        ec_profile:
                        pg_num:
                        pgp_num:
                        ec_pg_num:
                        ec_pgp_num:
                        image_1:
                            size:
                            io_total:
                        image_2:
                            size:
                            io_total:
    """
    log.debug(
        f'config received for rbd_config: {kw.get("config", "Config not received, assuming default values")}'
    )
    pool_types = update_config(**kw)
    config = kw.get("config", {})
    ceph_version = int(config.get("rhbuild")[0])

    if kw.get("client_node"):
        client = get_node_by_id(ceph_cluster, kw.get("client_node"))
    else:
        client = ceph_cluster.get_nodes(role="client")[0]
    rbd = Rbd(client)

    for pool_type in pool_types:
        pool_config = getdict(config.get(pool_type))
        is_ec_pool = True if "ec" in pool_type else False
        if config_rbd_multi_pool(
            rbd=rbd,
            multi_pool_config=pool_config,
            is_ec_pool=is_ec_pool,
            ceph_version=ceph_version,
            config=config,
            client=client,
            is_secondary=kw.get("is_secondary", False),
        ):
            log.error(f"RBD configuration failed for {pool_type}")
            return None

    return {"rbd": rbd, "client": client, "pool_types": pool_types}


def initial_mirror_config(**kw):
    """
    Create replicated pools, ecpools or both and corresponding images based on arguments specified
    Configure mirroring and enable required type of mirroring on the given pools and images
    For all the images mentioned in the config, primary_cluster will be the primary cluster
    and secondary_cluster will be the secondary cluster.

    Note: If in any test case there is a requirement to create few images as primary from primary_cluster
    and few other images as primary from secondary_cluster, then please call initial_mirror_config twice
    by specifying only the required images to be primary on the given primary_cluster cluster
    Args:
        **kw:
            "clusters":
                "<cluster_1_name>":
                    "config": <config_as_shown_in_example_below>"
                "<cluster_2_name>":
                    "config": <config_as_shown_in_example_below>"
    Note: For configuration Advanced configuration <specifying only number of pools and images>,
    num_pools number of pools will be created as specified in both cluster configurations,
    num_images number of images will be created as primary in that pool, unless num_secondary_images
    is exclusively specified, if it is specified then the rest of the images will be primary.

    Examples: In default configuration, pool and image names will be taken as random values.
    Journal mirroring, at pool level will be applied to all pools by default
        Default configuration for ecpools only :
            config:
                ec-pool-only: True
        Default configuration for replicated pools only :
            config:
                rep-pool-only: True
        Advanced configuration <specifying only number of pools and images>:
            config:
                do_not_create_image: True  # if not set then images will be created by default
                do_not_run_io: True
                ec-pool-only: False
                rep-pool-only: False
                rep_pool_config:
                    num_pools:
                    num_images:
                    num_secondary_images: # num of images out of num_images to be secondary
                    # in given cluster, (num_images - num_secondary_images) number of
                    # images will be created as primary in given cluster in same pool
                    pool_prefix:
                    image_prefix:
                    size:
                    io_total:
                    pg_num:
                    pgp_num:
                    ec_pg_num:
                    ec_pgp_num:
                    mode: <pool/image>
                    mirrormode: <journal/snapshot>
                    way: "one-way"
                ec_pool_config:
                    ec-pool-k-m: 2,1
                    num_pools:
                    num_images:
                    num_secondary_images: # num of images out of num_images to be secondary
                    # in given cluster, (num_images - num_secondary_images) number of
                    # images will be created as primary in given cluster in same pool
                    pool_prefix:
                    image_prefix:
                    data_pool_prefix:
                    size:
                    io_total:
                    pg_num:
                    pgp_num:
                    ec_pg_num:
                    ec_pgp_num:
                    mode: <pool/image>
                    mirrormode: <journal/snapshot>
                    way: "one-way"
        Advanced configuration <specifying only detailed pool and image configuration>:
            config:
                do_not_create_image: True  # if not set then images will be created by default
                do_not_run_io: True # if not set then IO will be run on images by default
                ec-pool-only: False
                rep-pool-only: False
                rep_pool_config:
                    pool_1:
                        pg_num:
                        pgp_num:
                        mode: <pool/image>
                        mirrormode: <journal/snapshot>
                        peer_mode: <bootstrap>
                        rbd_client: <client.admin>
                        image_1:
                            size:
                            io_total:
                            is_secondary: # if true the image in this cluster will
                            # become secondary, else by default it will be primary
                        image_2:
                            size:
                            io_total:
                    pool_2:
                        pg_num:
                        pgp_num:
                        mode: <pool/image>
                        mirrormode: <journal/snapshot>
                        peer_mode: <bootstrap>
                        rbd_client: <client.admin>
                        image_1:
                            size:
                            io_total:
                        image_2:
                            size:
                            io_total:
                ec_pool_config:
                    pool_1:
                        data_pool:
                        ec-pool-k-m: 2,1
                        ec_profile:
                        pg_num:
                        pgp_num:
                        ec_pg_num:
                        ec_pgp_num:
                        mode: <pool/image>
                        mirrormode: <journal/snapshot>
                        peer_mode: <bootstrap>
                        rbd_client: <client.admin>
                        image_1:
                            size:
                            io_total:
                        image_2:
                            size:
                            io_total:
                    pool_2:
                        data_pool:
                        ec-pool-k-m: 2,1
                        ec_profile:
                        pg_num:
                        pgp_num:
                        ec_pg_num:
                        ec_pgp_num:
                        mode: <pool/image>
                        mirrormode: <journal/snapshot>
                        peer_mode: <bootstrap>
                        rbd_client: <client.admin>
                        image_1:
                            size:
                            io_total:
                        image_2:
                            size:
                            io_total:
    """
    log.info("Creating pools and images as specified in configuration")
    ret_config = dict()
    # fetch the current value of ceph_cluster key and store it elsewhere
    # update it with primary cluster since initial_rbd_config is for primary cluster
    # Once initial config is done, the change can be reverted
    ceph_cluster = kw["ceph_cluster"]
    primary_config = dict()
    secondary_config = dict()

    cluster_dict = {
        k: v for (k, v) in kw["ceph_cluster_dict"].items() if k == ceph_cluster.name
    }
    cluster_name = list(cluster_dict.keys())[0]
    kw["ceph_cluster"] = list(cluster_dict.values())[0]
    primary_config = ret_config[cluster_name] = initial_rbd_config(
        is_secondary=False, is_mirror=True, **kw
    )
    ret_config[cluster_name]["is_secondary"] = False
    primary_config["cluster"] = kw["ceph_cluster"]

    cluster_dict = {
        k: v for (k, v) in kw["ceph_cluster_dict"].items() if k != ceph_cluster.name
    }
    cluster_name = list(cluster_dict.keys())[0]
    kw["ceph_cluster"] = list(cluster_dict.values())[0]
    secondary_config = ret_config[cluster_name] = initial_rbd_config(
        is_secondary=True, is_mirror=True, **kw
    )
    ret_config[cluster_name]["is_secondary"] = True
    secondary_config["cluster"] = kw["ceph_cluster"]

    # Revert kw["ceph_cluster"] to its original value
    kw["ceph_cluster"] = ceph_cluster
    config = kw.get("config")

    ret_config["output"] = list()
    for pool_type in primary_config.get("pool_types"):
        pool_config = getdict(config.get(pool_type))
        kw["do_not_enable_mirror_on_image"] = config.get(pool_type).get(
            "do_not_enable_mirror_on_image"
        )

        out = config_mirror_multi_pool(
            primary_config, secondary_config, pool_config, **kw
        )

        if "Snapshot based mirroring cannot be enabled in pool mode" in out:
            ret_config["output"].append(f"{out} for {pool_type}")
            continue

        config_mirror_multi_pool(
            secondary_config, primary_config, pool_config, is_secondary=True, **kw
        )

    return ret_config
