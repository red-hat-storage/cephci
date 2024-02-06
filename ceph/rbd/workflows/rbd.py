import datetime
import json
from importlib import import_module
from time import sleep

from ceph.parallel import parallel
from ceph.rbd.utils import convert_size, getdict, random_string
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.pool import create_ecpool, create_pool
from utility.log import Log

log = Log(__name__)


def create_snap_and_clone(rbd, snap_spec, clone_spec, **kw):
    """_summary_

    Args:
        snap_spec (_type_): _description_
        clone_spec (_type_): _description_
    """
    snap_config = {"snap-spec": snap_spec}

    out, err = rbd.snap.create(**snap_config)
    if out or err and "100% complete" not in err:
        log.error(f"Snapshot creation failed for {snap_spec}")
        return 1

    out, err = rbd.snap.protect(**snap_config)
    if out or err:
        log.error(f"Snapshot protect failed for {snap_spec}")
        return 1

    clone_config = {"source-snap-spec": snap_spec, "dest-image-spec": clone_spec}
    if kw.get("clone_format"):
        clone_config.update({"rbd-default-clone-format": kw.get("clone_format")})

    out, err = rbd.clone(**clone_config)
    if out or err:
        log.error(f"Clone creation failed for {clone_spec}")
        return 1

    return 0


def create_single_image(
    config,
    cluster,
    rbd,
    pool,
    pool_config,
    image,
    image_config_val,
    is_ec_pool,
    raise_exception=False,
):
    """
    Method to create single image
    """
    if not config.get("do_not_create_image"):
        create_config = {"pool": pool, "image": image}
        if is_ec_pool:
            create_config.update({"data-pool": pool_config.get("data_pool")})
        # Update any other specific arguments that rbd create command takes if passed in config
        create_config.update(
            {
                k: v
                for k, v in image_config_val.items()
                if k
                not in [
                    "io_total",
                    "test_config",
                    "is_secondary",
                    "snap_schedule_levels",
                    "snap_schedule_intervals",
                    "io_size",
                ]
            }
        )
        create_config.update({"cluster": cluster})
        out, err = rbd.create(**create_config)
        if out or err:
            log.error(f"Image {image} creation failed with err {out} {err}")
            if raise_exception:
                raise Exception(f"Image {image} creation failed with err {out} {err}")
            return 1
    return 0


def create_images(
    config,
    cluster,
    rbd,
    pool,
    pool_config,
    image_config,
    is_ec_pool,
    create_image_parallely,
):
    """
    Method to create multiple images parallely/sequentially
    """
    if not create_image_parallely:
        for image, image_config_val in image_config.items():
            rc = create_single_image(
                config,
                cluster,
                rbd,
                pool,
                pool_config,
                image,
                image_config_val,
                is_ec_pool,
            )
            if rc:
                return rc
    else:
        try:
            with parallel() as p:
                for image, image_config_val in image_config.items():
                    p.spawn(
                        create_single_image,
                        config,
                        cluster,
                        rbd,
                        pool,
                        pool_config,
                        image,
                        image_config_val,
                        is_ec_pool,
                        raise_exception=True,
                    )
        except Exception:
            return 1
    return 0


def create_single_pool_and_images(
    config,
    pool,
    pool_config,
    client,
    cluster,
    rbd,
    ceph_version,
    is_ec_pool,
    is_secondary,
    create_image_parallely=False,
    do_not_create_image=False,
    raise_exception=False,
):
    """
    Method to create a single pool
    """
    if create_pool(
        pool=pool,
        pg_num=pool_config.get("pg_num", 64),
        pgp_num=pool_config.get("pgp_num", 64),
        client=client,
        cluster=cluster,
    ):
        log.error(f"Pool creation failed for pool {pool}")
        if raise_exception:
            raise Exception(f"Pool creation failed for pool {pool}")
        return 1

    if ceph_version >= 3:
        pool_init_conf = {"pool-name": pool, "cluster": cluster}
        rbd.pool.init(**pool_init_conf)

    if is_ec_pool and create_ecpool(
        pool=pool_config.get("data_pool"),
        k_m=pool_config.get("ec-pool-k-m"),
        profile=pool_config.get("ec_profile", "use_default"),
        pg_num=pool_config.get("ec_pg_num", 32),
        pgp_num=pool_config.get("ec_pgp_num", 32),
        failure_domain=pool_config.get("failure_domain", ""),
        client=client,
        cluster=cluster,
    ):
        log.error(f"EC Pool creation failed for {pool_config.get('data_pool')}")
        if raise_exception:
            raise Exception(
                f"EC Pool creation failed for {pool_config.get('data_pool')}"
            )
        return 1

    if is_ec_pool and ceph_version >= 3:
        pool_init_conf = {
            "pool-name": pool_config.get("data_pool"),
            "cluster": cluster,
        }
        rbd.pool.init(**pool_init_conf)

    if not do_not_create_image:
        multi_image_config = getdict(pool_config)
        image_config = {
            k: v
            for k, v in multi_image_config.items()
            if v.get("is_secondary", False) == is_secondary
        }
        rc = create_images(
            config,
            cluster,
            rbd,
            pool,
            pool_config,
            image_config,
            is_ec_pool,
            create_image_parallely,
        )
        if rc:
            log.error(f"Error while creating images for pool {pool}")
            if raise_exception:
                raise Exception(f"Error while creating images for pool {pool}")
            return 1
    return 0


def create_pools_and_images(
    rbd,
    multi_pool_config,
    is_ec_pool,
    ceph_version,
    config,
    client,
    is_secondary=False,
    cluster="ceph",
    create_pool_parallely=False,
    create_image_parallely=False,
):
    """
    Create number of pools as specified in parallel/sequential
    """

    # If any pool level test config is present, pop it out
    # so that it does not get mistaken as another image configuration
    pool_test_config = multi_pool_config.pop("test_config", None)

    if not create_pool_parallely:
        for pool, pool_config in multi_pool_config.items():
            rc = create_single_pool_and_images(
                config,
                pool,
                pool_config,
                client,
                cluster,
                rbd,
                ceph_version,
                is_ec_pool,
                is_secondary,
                create_image_parallely,
            )
            if rc:
                log.error("Multi pool and multi image config failed")
                return rc
    else:
        try:
            with parallel() as p:
                for pool, pool_config in multi_pool_config.items():
                    p.spawn(
                        create_single_pool_and_images,
                        config,
                        pool,
                        pool_config,
                        client,
                        cluster,
                        rbd,
                        ceph_version,
                        is_ec_pool,
                        is_secondary,
                        create_image_parallely,
                        raise_exception=True,
                    )
        except Exception:
            log.error("Multi pool and multi image config failed")
            return 1

    # Add back the popped pool test config once configuration is complete
    if pool_test_config:
        multi_pool_config["test_config"] = pool_test_config

    return 0


def list_images_and_verify_with_input(**kw):
    """
    List all images for in the given pool and verify that the images
    match the images in the input config

    Args:
        kw: {
            "rbd": <>,
            "pool": <>,
            "images": <>
        }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    log.info(f"Listing all pools in the cluster of pool type {pool}")
    out, err = rbd.ls(pool=pool, format="json")
    if err:
        log.error(f"Error while fetching images from pool {pool}: {err}")
        return 1
    images_in_pool = json.loads(out)
    images_in_conf = kw.get("images")
    log.info(f"Images given in input conf: {images_in_conf}")
    log.info(f"Images listed in pool {pool}: {images_in_pool}")
    if all(image in images_in_pool for image in images_in_conf):
        log.info(
            f"All images given in input conf are also present in images listed in pool {pool}"
        )
        return 0
    else:
        log.error(f"Images in pool do not match images in input for pool {pool}")
        return 1


def get_image_info_and_verify_for_single_image(**kw):
    """Get rbd info for the specified image and verify it with input conf

    Args:
        kw:{
            "rbd": <>,
            "pool": <>,
            "image": <>,
            "image_conf": <>,
            "test_ops_parallely": <>
        }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    image_conf = kw.pop("image_conf", {})
    test_ops_parallely = kw.get("test_ops_parallely", False)
    log.info(f"Fetching info and verifying for image {image}")
    out, err = rbd.info(pool=pool, image=image, format="json")
    if err:
        log.error(f"Error while fetching image info for image {pool}/{image}: {err}")
        return 1
    image_info = json.loads(out)
    log.info(f"Image info fetched for image {image_info}")
    log.info(f"Image config passed as input: {image_conf}")
    req_image_conf = {k: v for k, v in image_conf.items() if k not in ["io_total"]}
    if not all(keys in image_info.keys() for keys in req_image_conf.keys()):
        log.error(f"All parameters in {image_conf} are not present in {image_info}")
        if test_ops_parallely:
            raise Exception(
                f"All parameters in {image_conf} are not present in {image_info}"
            )
        return 1
    for k, v in req_image_conf.items():
        value = image_info[k]
        if "size" in k:
            value = convert_size(value)
        if value != v:
            log.error(
                f"Image info for parameter {k} in not same as input config value {v}"
            )
            if test_ops_parallely:
                raise Exception(
                    f"Image info for parameter {k} in not same as input config value {v}"
                )
            return 1
    log.info(f"Image info matches the conf provided in input for image {pool}/{image}")
    return 0


def resize_single_image_and_verify(**kw):
    """Resize images to a new given size and verify the operation

    Args:
        kw:{
            "rbd": <>,
            "pool": <>,
            "image": <>,
            "image_conf": {"resize_to": <>,...},
            "test_ops_parallely": <>
        }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    image_conf = kw.pop("image_conf", {})
    test_ops_parallely = kw.get("test_ops_parallely", False)
    resize_to = image_conf.get("resize_to")
    log.info(f"Resizing image {pool}/{image} to size {resize_to}")
    out, err = rbd.resize(pool=pool, image=image, size=resize_to)
    if out or err and "100% complete" not in out + err:
        log.error(
            f"Image resize failed for image {pool}/{image} with error {out} {err}"
        )
        if test_ops_parallely:
            raise Exception(
                f"Image resize failed for image {pool}/{image} with error {out} {err}"
            )
        return 1
    image_conf_new = {"size": resize_to}
    rc = get_image_info_and_verify_for_single_image(
        rbd=rbd,
        pool=pool,
        image=image,
        image_conf=image_conf_new,
        test_ops_parallely=test_ops_parallely,
    )
    if rc:
        log.error(
            f"Image size does not match the resized size for image {pool}/{image}"
        )
        if test_ops_parallely:
            raise Exception(
                f"Image size does not match the resized size for image {pool}/{image}"
            )
        return rc
    return 0


def rename_single_image_and_verify(**kw):
    """Rename images to a new given name and verify the operation

    Args:
        kw:{
            "rbd": <>,
            "pool": <>,
            "image": <>,
            "image_conf": {"rename_to": <>,...},
            "test_ops_parallely": <>
        }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    image_conf = kw.pop("image_conf", {})
    test_ops_parallely = kw.get("test_ops_parallely", False)
    rename_to = image_conf.get("rename_to")
    if not rename_to:
        rename_to = image + "_renamed"
    log.info(f"Renaming image {pool}/{image} to size {rename_to}")
    rename_config = {
        "source-image-spec": f"{pool}/{image}",
        "dest-image-spec": f"{pool}/{rename_to}",
    }
    out, err = rbd.rename(**rename_config)
    if out or err and "100% complete" not in out + err:
        log.error(
            f"Image rename failed for image {pool}/{image} with error {out} {err}"
        )
        if test_ops_parallely:
            raise Exception(
                f"Image rename failed for image {pool}/{image} with error {out} {err}"
            )
        return 1
    rc = list_images_and_verify_with_input(rbd=rbd, pool=pool, images=[rename_to])
    if rc:
        log.error(f"Image with new name {rename_to} is not present in pool {pool}")
        if test_ops_parallely:
            raise Exception(
                f"Image with new name {rename_to} is not present in pool {pool}"
            )
        return 1

    rc = list_images_and_verify_with_input(rbd=rbd, pool=pool, images=[image])
    if not rc:
        log.error(f"Image with old name {image} is present in pool {pool}")
        if test_ops_parallely:
            raise Exception(f"Image with old name {image} is present in pool {pool}")
        return 1
    return 0


def remove_single_image_and_verify(**kw):
    """Remove images in the given pol and verify the operation

    Args:
        kw:{
            "rbd": <>,
            "pool": <>,
            "image": <>,
            "test_ops_parallely": <>
        }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    test_ops_parallely = kw.get("test_ops_parallely", False)

    log.info(f"Removing image {image} from pool {pool}")
    out, err = rbd.rm(pool=pool, image=image)
    if out or err and "100% complete" not in out + err:
        log.error(
            f"Image remove failed for image {pool}/{image} with error {out} {err}"
        )
        if test_ops_parallely:
            raise Exception(
                f"Image remove failed for image {pool}/{image} with error {out} {err}"
            )
        return 1

    rc = list_images_and_verify_with_input(rbd=rbd, pool=pool, images=[image])
    if not rc:
        log.error(f"Image with name {image} is present in pool {pool} after removal")
        if test_ops_parallely:
            raise Exception(
                f"Image with name {image} is present in pool {pool} after removal"
            )
        return 1
    return 0


def copy_single_image_and_verify(**kw):
    """
    Copy images to a new given pool and verify the operation

    Args:
        kw:{
            "rbd": <>,
            "pool": <>,
            "image": <>,
            "new_pool": <>,
            "test_ops_parallely": <>
        }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    test_ops_parallely = kw.get("test_ops_parallely", False)
    new_pool = kw.get("new_pool")
    log.info(f"Copying image {pool}/{image} to pool {new_pool}")
    copy_config = {
        "source-image-or-snap-spec": f"{pool}/{image}",
        "dest-image-spec": f"{new_pool}/{image}",
    }
    out, err = rbd.copy(**copy_config)
    if out or err and "100% complete" not in out + err:
        log.error(f"Image copy failed for image {pool}/{image} with error {out} {err}")
        if test_ops_parallely:
            raise Exception(
                f"Image copy failed for image {pool}/{image} with error {out} {err}"
            )
        return 1
    rc = list_images_and_verify_with_input(rbd=rbd, pool=new_pool, images=[image])
    if rc:
        log.error(f"Image is not present in new pool {new_pool}")
        if test_ops_parallely:
            raise Exception(f"Image is not present in {new_pool}")
        return 1


def feature_ops_single_image(**kw):
    """Enable and disable image features and verify

    Args:
        kw:{
            "rbd": <>,
            "pool": <>,
            "image": <>,
            "test_ops_parallely": <>
        }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    test_ops_parallely = kw.get("test_ops_parallely")
    feature_list = ["object-map", "deep-flatten", "journaling", "exclusive-lock"]
    log.info(f"Fetching all features enabled for image {pool}/{image}")
    out, err = rbd.info(pool=pool, image=image, format="json")
    if err:
        log.error(f"Error while fetching image info for image {pool}/{image}: {err}")
        return 1
    image_info = json.loads(out)
    log.info(f"Image info fetched for image {pool}/{image}: {image_info}")
    image_features_enabled = image_info.get("features", [])
    features = ",".join(
        [feature for feature in feature_list if feature not in image_features_enabled]
    )
    log.info(f"Enable feature {features} for image {pool}/{image}")
    out, err = rbd.feature.enable(pool=pool, image=image, features=features)
    if out or err and "features are already enabled" not in out + err:
        log.error(f"Error while enabling feature {features}: {out} {err}")
        if test_ops_parallely:
            raise Exception(f"Error while enabling feature {features}: {out} {err}")
        return 1

    out, err = rbd.info(pool=pool, image=image, format="json")
    if err:
        log.error(f"Error while fetching image info for image {pool}/{image}: {err}")
        return 1
    image_info = json.loads(out)
    log.info(f"Image info fetched for image {pool}/{image}: {image_info}")
    image_features_enabled = image_info.get("features", [])

    if not all(feature in image_features_enabled for feature in features.split(",")):
        log.error(f"Not all features {features} are enabled for image {pool}/{image}")
        if test_ops_parallely:
            raise Exception(
                f"Not all features {features} are enabled for image {pool}/{image}"
            )
        return 1
    features = ",".join(
        [feature for feature in feature_list if feature in image_features_enabled]
    )

    log.info(f"Disable feature {features} for image {pool}/{image}")
    out, err = rbd.feature.disable(pool=pool, image=image, features=features)
    if out or err and "features are already disabled" not in out + err:
        log.error(f"Error while disabling feature {features}: {out} {err}")
        if test_ops_parallely:
            raise Exception(f"Error while disabling feature {features}: {out} {err}")
        return 1

    out, err = rbd.info(pool=pool, image=image, format="json")
    if err:
        log.error(f"Error while fetching image info for image {pool}/{image}: {err}")
        return 1
    image_info = json.loads(out)
    log.info(f"Image info fetched for image {pool}/{image}: {image_info}")
    image_features_enabled = image_info.get("features", [])

    if any(feature in image_features_enabled for feature in features.split(",")):
        log.error(f"Not all features {features} are disabled for image {pool}/{image}")
        if test_ops_parallely:
            raise Exception(
                f"Not all features {features} are disabled for image {pool}/{image}"
            )
        return 1


def check_rbd_status(rbd, pool, image, timeout):
    """
    Keep checking rbd status until watchers is None
    """
    timeout = datetime.timedelta(seconds=timeout)
    starttime = datetime.datetime.now()

    out = str()
    sleep(5)
    while datetime.datetime.now() - starttime <= timeout:
        out, err = rbd.status(pool=pool, image=image, format="json")
        if err:
            log.error(f"Error while fetching rbd status {pool}/{image}")
            raise Exception(f"Error while fetching rbd status {pool}/{image}")
        rbd_status = json.loads(out)
        log.info(f"RBD Status: {rbd_status}")
        if not rbd_status.get("watchers"):
            raise Exception(
                f"Watchers don't exist even while running IOs for {pool}/{image}"
            )
        sleep(5)
    return 0


def run_io_and_check_rbd_status(**kw):
    """Run IOs on the given image and verify output of rbd status

    Args:
        kw:{
            "rbd": <>,
            "pool": <>,
            "image": <>,
            "image_conf": <>,
            "client": <>,
            "test_ops_parallely": <>
        }
    """
    try:
        rbd = kw.get("rbd")
        pool = kw.get("pool")
        image = kw.get("image")
        image_spec = f"{pool}/{image}"
        image_conf = kw.pop("image_conf", {})
        client = kw.get("client")
        test_ops_parallely = kw.get("test_ops_parallely", False)
        if image_conf.get("io_size"):
            io_size = image_conf.get("io_size")
        else:
            io_size = (
                str(round(int(image_conf["size"][:-1]) / 3)) + image_conf["size"][-1]
            )
        io_config = {
            "rbd_obj": rbd,
            "client": client,
            "size": image_conf["size"],
            "do_not_create_image": True,
            "config": {
                "file_size": io_size,
                "run_time": 120,
                "file_path": [
                    f"/tmp/mnt_{random_string(len=5)}/file_{random_string(len=3)}"
                ],
                "get_time_taken": True,
                "image_spec": [image_spec],
                "operations": {
                    "fs": "ext4",
                    "io": True,
                    "mount": True,
                    "nounmap": False,
                    "device_map": True,
                },
                "skip_mkfs": kw.get("skip_mkfs", False),
            },
        }
        with parallel() as p:
            p.spawn(krbd_io_handler, **io_config)
            p.spawn(check_rbd_status, rbd=rbd, pool=pool, image=image, timeout=120)

        out, err = rbd.status(pool=pool, image=image, format="json")
        if err:
            log.error(f"Error while fetching rbd status {pool}/{image}")
            if test_ops_parallely:
                raise Exception(f"Error while fetching rbd status {pool}/{image}")
            return 1
        rbd_status = json.loads(out)
        log.info(f"RBD Status: {rbd_status}")
        if rbd_status.get("watchers"):
            log.error(
                f"Watchers exist even while IOs are not running for {pool}/{image}"
            )
            if test_ops_parallely:
                raise Exception(
                    f"Watchers exist even while IOs are not running for {pool}/{image}"
                )
            return 1
    except Exception as e:
        if test_ops_parallely:
            raise Exception(
                f"Run IOs and verify rbd status failed for {pool}/{image} with error {e}"
            )
        return 1

    return 0


def metadata_ops_single_image(**kw):
    """Set, list, get and remove image metadata for an image and verify

    kw: {
        "rbd": <>,
        "pool": <>,
        "image": <>,
        "test_ops_parallely": <>
    }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    test_ops_parallely = kw.get("test_ops_parallely", False)
    value = datetime.datetime.today().strftime("%Y-%m-%d")
    out, err = rbd.image_meta.set(
        pool=pool, image=image, key="last_update", value=value
    )

    if out or err:
        log.error(f"Setting image metadata failed for {pool}/{image}")
        if test_ops_parallely:
            raise Exception(f"Setting image metadata failed for {pool}/{image}")
        return 1

    out, err = rbd.image_meta.get(pool=pool, image=image, key="last_update")

    if err:
        log.error(f"Getting image meta value for last_update failed for {pool}/{image}")
        if test_ops_parallely:
            raise Exception(
                f"Getting image meta value for last_update failed for {pool}/{image}"
            )
        return 1

    if out.strip() != value:
        log.error(
            f"Image metadata value does not match the value set for last_update for {pool}/{image}"
        )
        if test_ops_parallely:
            raise Exception(
                f"Image metadata value does not match the value set for last_update for {pool}/{image}"
            )
        return 1
    else:
        log.info(
            f"Image metadata value set for last_update is the same as the value fetched using get method for {pool}/{image}"
        )

    out, err = rbd.image_meta.list(pool=pool, image=image, format="json")

    if err:
        log.error(f"Image metadata list failed for {pool}/{image}")
        if test_ops_parallely:
            raise Exception(f"Image metadata list failed for {pool}/{image}")
        return 1

    meta = json.loads(out)
    if meta.get("last_update") != value:
        log.error(
            f"Image metadata value does not match the value set for last_update for {pool}/{image}"
        )
        if test_ops_parallely:
            raise Exception(
                f"Image metadata value does not match the value set for last_update for {pool}/{image}"
            )
        return 1
    else:
        log.info(
            f"Image metadata value set for last_update is the same as the value fetched using list method for {pool}/{image}"
        )

    out, err = rbd.image_meta.rm(pool=pool, image=image, key="last_update")

    if out or err:
        log.error(f"Image metadata remove failed for {pool}/{image}")
        if test_ops_parallely:
            raise Exception(f"Image metadata remove failed for {pool}/{image}")
        return 1

    out, err = rbd.image_meta.get(pool=pool, image=image, key="last_update")

    if out or err and "failed to get metadata" not in out + err:
        log.error(f"Image metadata still present after deletion for {pool}/{image}")
        if test_ops_parallely:
            raise Exception(
                f"Image metadata still present after deletion for {pool}/{image}"
            )
        return 1
    elif "failed to get metadata" in out + err:
        log.info(
            f"Image metadata for last_update removed successfully for {pool}/{image}"
        )

    return 0


def wrapper_for_image_ops(**kw):
    """
    Perform specified image operation for images in pool,
    and verify either sequentially or parallely based on input

    Args:
        kw: {
            "rbd": <>,
            "pool": <>,
            "image_config": {
                <image_1>: <image1_conf>,
                <image_2>: <image2_conf>,....
            },
            "ops_module": <>,
            "ops_method": <>,
            "test_ops_parallely": <>
        }
    """
    pool = kw.get("pool")
    image_config = kw.pop("image_config", {})
    test_ops_parallely = kw.get("test_ops_parallely", False)
    ops_module = kw.pop("ops_module", {})
    mod_obj = import_module(ops_module)
    ops_method = kw.pop("ops_method", {})
    method_obj = getattr(mod_obj, ops_method)
    log.info(f"Test image operations {ops_method} for pool {pool}")

    if test_ops_parallely:
        with parallel() as p:
            for image, image_conf in image_config.items():
                p.spawn(method_obj, image=image, image_conf=image_conf, **kw)
    else:
        for image, image_conf in image_config.items():
            rc = method_obj(image=image, image_conf=image_conf, **kw)
            if rc:
                log.error(
                    f"Test image operation {ops_method} failed for {pool}/{image}"
                )
                return 1
    return 0
