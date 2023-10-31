from copy import deepcopy
from time import sleep

from ceph.rbd.initial_config import initial_mirror_config, random_string
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup, device_cleanup
from ceph.rbd.workflows.encryption import (
    create_and_encrypt_clone,
    execute_neg_encryption_scenarios,
    mount_image_and_mirror_and_check_data,
    test_encryption_between_image_and_clone,
)
from ceph.rbd.workflows.rbd_mirror import enable_image_mirroring
from utility.log import Log

log = Log(__name__)


def test_negative_scenarios(**kw):
    """_summary_
    kw: {
        "rbd": rbd,
        "client": client,
        "mirror_rbd": mirror_rbd,
        "mirror_client": mirror_client,
        "size": size,
        "parent_spec": image_spec,
        "clone_spec": clone_spec,
        "parent_encryption_config": parent_encryption_config,
        "clone_encryption_config": clone_encryption_config,
        "format": format,
    }
    """
    # 1) Execute negative scenarios on parent image
    scenarios = [4]
    encryption_config = [{"encryption-format": "NA", "encryption-passphrase-file": ""}]
    if kw.get("parent_encryption_config"):
        scenarios.extend([1, 2, 3])
        encryption_config = kw.get("parent_encryption_config")

    neg_config = {
        "scenarios": scenarios,
        "encryption_config": encryption_config,
        "rbd": kw.get("rbd"),
        "client": kw.get("client"),
        "size": kw.get("size"),
        "image_spec": kw.get("parent_spec"),
    }
    if execute_neg_encryption_scenarios(**neg_config):
        log.error(
            f"Testing of negative scenarios failed on parent image {kw.get('parent_spec')}"
        )
        return 1

    # 2) Execute negative scenarios on clone image (after flatten)
    scenarios = [4]
    if kw.get("clone_encryption_config") and 3 not in scenarios:
        scenarios.extend([1, 2, 3])

    neg_config = {
        "scenarios": scenarios,
        "encryption_config": [kw.get("clone_encryption_config")[0]],
        "rbd": kw.get("rbd"),
        "client": kw.get("client"),
        "size": kw.get("size"),
        "image_spec": kw.get("clone_spec"),
    }
    if execute_neg_encryption_scenarios(**neg_config):
        log.error(
            f"Testing of negative scenarios failed on clone image {kw.get('clone_spec')}"
        )
        return 1

    # 3) Execute negative scenarios on mirrored parent image
    scenarios = [4]
    if kw.get("parent_encryption_config") and 3 not in scenarios:
        scenarios.extend([1, 2, 3])
        encryption_config = kw.get("parent_encryption_config")

    scenarios.append(16)
    neg_config = {
        "scenarios": scenarios,
        "encryption_config": encryption_config,
        "rbd": kw.get("mirror_rbd"),
        "client": kw.get("mirror_client"),
        "size": kw.get("size"),
        "image_spec": kw.get("parent_spec"),
        "read_only": True,
    }
    if execute_neg_encryption_scenarios(**neg_config):
        log.error(
            f"Testing of negative scenarios failed on mirrored parent image {kw.get('parent_spec')}"
        )
        return 1

    # 4) Execute negative scenarios on mirrored clone image (after flatten)
    scenarios = [4]
    if kw.get("clone_encryption_config") and 3 not in scenarios:
        scenarios.extend([1, 2, 3])

    scenarios.append(16)
    neg_config = {
        "scenarios": scenarios,
        "encryption_config": [kw.get("clone_encryption_config")[0]],
        "rbd": kw.get("mirror_rbd"),
        "client": kw.get("mirror_client"),
        "size": kw.get("size"),
        "image_spec": kw.get("clone_spec"),
        "read_only": True,
    }
    if execute_neg_encryption_scenarios(**neg_config):
        log.error(
            f"Testing of negative scenarios failed on mirrored clone image {kw.get('clone_spec')}"
        )
        return 1

    # 5) Execute negative scenario on clone image (before flatten)
    [pool, image] = kw.get("parent_spec").split("/")
    snap = f"snap_{random_string(len=5)}"
    clone = f"clone_{random_string(len=5)}"
    clone_spec = f"{pool}/{clone}"
    snap_spec = f"{pool}/{image}@{snap}"
    passphrase = (
        ""
        if not kw.get("parent_encryption_config")
        else kw.get("parent_encryption_config")[0].get("encryption-passphrase-file")
    )
    clone_file_path = f"/tmp/{clone}_dir/{clone}_file"
    clone_passphrase = f"clone_{clone}_passphrase.bin"
    clone_config = {
        "rbd": kw.get("rbd"),
        "client": kw.get("client"),
        "format": kw.get("format"),
        "size": kw.get("size"),
        "image_spec": kw.get("parent_spec"),
        "clone_spec": clone_spec,
        "snap_spec": snap_spec,
        "parent_passphrase": passphrase,
        "clone_file_path": clone_file_path,
        "clone_passphrase": clone_passphrase,
        "do_not_map_and_mount": True,
    }

    clone_encryption_config = create_and_encrypt_clone(**clone_config)
    if not passphrase:
        clone_encryption_config.append(
            {"encryption-format": "NA", "encryption-passphrase-file": ""}
        )

    clone_not_encrypted = len(kw.get("parent_encryption_config", [])) == len(
        clone_encryption_config
    )
    scenarios = list(range(5, 14))

    # temp luks2,luks1
    # luks1,luks2
    # scenarios.remove(7)
    # scenarios.remove(13)

    if not kw.get("parent_encryption_config"):
        scenarios = [8, 11, 12, 14]

    if clone_not_encrypted:
        scenarios = [1, 2, 4, 15]

    neg_config = {
        "scenarios": scenarios,
        "encryption_config": clone_encryption_config,
        "rbd": kw.get("rbd"),
        "client": kw.get("client"),
        "size": kw.get("size"),
        "image_spec": clone_spec,
        "parent_spec": kw.get("parent_spec"),
    }
    if execute_neg_encryption_scenarios(**neg_config):
        log.error(
            f"Testing of negative scenarios before flatten failed on clone image {clone_spec}"
        )
        return 1

    return 0


def test_rbd_encryption(mirror_obj, pool, image, format, **kw):
    """ """
    size = kw.get("size")
    snap = f"snap_{random_string(len=5)}"
    clone = f"clone_{random_string(len=5)}"
    image_spec = f"{pool}/{image}"
    clone_spec = f"{pool}/{clone}"
    snap_spec = f"{pool}/{image}@{snap}"
    parent_encryption_config = list()
    clone_encryption_config = list()
    parent_file_path = f"/tmp/{image}_dir/{image}_file"
    clone_file_path = f"/tmp/{clone}_dir/{image}_file"
    mirrormode = kw.get("mirrormode", "")

    for val in mirror_obj.values():
        if val.get("is_secondary", False) == kw.get("is_secondary"):
            rbd = val.get("rbd")
            client = val.get("client")
            cluster = val.get("cluster")
        else:
            mirror_rbd = val.get("rbd")
            mirror_client = val.get("client")
            mirror_cluster = val.get("cluster")

    try:
        log.info(f"Testing encryption between {image=} and its {clone=}")
        test_config = {
            "rbd": rbd,
            "client": client,
            "image_spec": image_spec,
            "snap_spec": snap_spec,
            "clone_spec": clone_spec,
            "parent_file_path": parent_file_path,
            "clone_file_path": clone_file_path,
            "size": size,
            "format": format,
        }

        encryption_config = test_encryption_between_image_and_clone(**test_config)
        if not encryption_config:
            log.error(
                f"Testing encryption between {image=} and its {clone=} failed for encryption format: {format}"
            )
            return 1

        parent_encryption_config = encryption_config.get("parent_encryption_config", [])
        clone_encryption_config = encryption_config.get("clone_encryption_config", [])

        flatten_config = {
            "image-spec": clone_spec,
            "encryption_config": clone_encryption_config,
        }

        out, _ = rbd.flatten(**flatten_config)
        if out:
            log.error(
                f"Flatten clone with encryption failed for {clone_spec} for encryption format: {format}"
            )
            return None

        # Sleep after flatten clone for flattening to complete
        sleep(60)

        log.info(f"Applying mirroring on parent image: {image_spec}")
        primary_config = {
            "rbd": rbd,
            "cluster": cluster,
        }
        secondary_config = {"rbd": mirror_rbd, "cluster": mirror_cluster}
        mirror_enable_config = {"pool": pool, "image": image, "mirrormode": mirrormode}

        enable_image_mirroring(primary_config, secondary_config, **mirror_enable_config)

        log.info(f"Applying mirroring on cloned image: {clone_spec}")
        primary_config = {
            "rbd": rbd,
            "cluster": cluster,
        }
        secondary_config = {"rbd": mirror_rbd, "cluster": mirror_cluster}
        mirror_enable_config = {"pool": pool, "image": clone, "mirrormode": mirrormode}

        enable_image_mirroring(primary_config, secondary_config, **mirror_enable_config)

        # Sleep for image mirroring to copy all data to mirror cluster
        # Maybe check if there is a way to verify data is copied to mirror
        # instead of random wait
        sleep(60)

        log.info(f"Checking data integrity between {image} and its mirror")

        mirror_config = {
            "rbd": rbd,
            "client": client,
            "mirror_rbd": mirror_rbd,
            "mirror_client": mirror_client,
            "image_spec": image_spec,
            "size": size,
            "file_path": parent_file_path,
            "encryption_config": parent_encryption_config,
        }

        if mount_image_and_mirror_and_check_data(**mirror_config):
            log.error(
                f"Checking data integrity between {image} and its mirror failed for encryption format: {format}"
            )
            return 1

        log.info(f"Checking data integrity between {clone=} and its mirror")

        mirror_config.update(
            {
                "image_spec": clone_spec,
                "file_path": clone_file_path,
                "encryption_config": [clone_encryption_config[0]],
            }
        )

        if mount_image_and_mirror_and_check_data(**mirror_config):
            log.error(
                f"Checking data integrity between {clone=} and its mirror failed for encryption format: {format}"
            )
            return 1

        encryption_config = deepcopy(clone_encryption_config)

        negative_config = {
            "rbd": rbd,
            "client": client,
            "mirror_rbd": mirror_rbd,
            "mirror_client": mirror_client,
            "size": size,
            "parent_spec": image_spec,
            "clone_spec": clone_spec,
            "parent_encryption_config": parent_encryption_config,
            "clone_encryption_config": clone_encryption_config,
            "format": format,
        }
        if test_negative_scenarios(**negative_config):
            log.error(
                f"Testing negative scenarios failed for encryption format: {format}"
            )
            return 1

    except Exception as error:
        log.error(str(error))
        return 1

    finally:
        log.info("Executing cleanup")
        cleanup_config = {
            "rbd": rbd,
            "client": client,
            "all": True,
            "passphrase_file": "*.bin",
        }
        device_cleanup(**cleanup_config)
        cleanup_config.update(
            {
                "rbd": mirror_rbd,
                "client": mirror_client,
            }
        )
        device_cleanup(**cleanup_config)

    return 0


def test_encryption_on_mirrored_cluster(mirror_obj, pool_type, **kw):
    """ """
    config = deepcopy(kw.get("config").get(pool_type))
    test_config = config.pop("test_config", {})
    encryption_types = test_config.get("encryption_type")

    for pool, pool_config in getdict(config).items():
        image_index = 0
        multi_image_config = getdict(pool_config)
        multi_image_config.pop("test_config")
        for image, image_config in multi_image_config.items():
            format = encryption_types[image_index]
            image_index += 1
            log.info(
                f"Testing encryption for pool type : {pool_type} and encryption format : {format}"
            )
            if test_rbd_encryption(
                mirror_obj,
                pool,
                image,
                format,
                size=image_config.get("size"),
                is_secondary=image_config.get("is_secondary", False),
                mirrormode=config.get("mirrormode"),
            ):
                log.error(
                    f"RBD encryption test failed for image_spec: {pool}/{image} and encryption format: {format}"
                )
                return 1
    return 0


def run(**kw):
    """Verify the luks encryption functionality for a mirrored image
    Pre-requisites :
    We need atleast one client node with ceph-common, fio and rbd-nbd packages,
    conf and keyring files in both clusters with snapshot based RBD mirroring
    enabled between the clusters.
    Test cases covered -
    1) CEPH-83575408 - RBD mirroring operations on encrypted images
    kw:
        config:
            rep_pool_config:
              num_pools: 1
              num_images: 6 # one each of below encryptions will be applied on each image
              mode: image
              mirrormode: snapshot
              test_config:
                encryption_type: #parent,clone number of formats defined should correspond to num_images
                  - luks1,luks1
                  - luks1,NA
                  - luks2,luks1
                  - luks2,luks2
                  - luks2,NA
                  - NA,luks1
                  - NA,luks2
                  - luks1,luks2
            ec_pool_config:
              num_pools: 1
              num_images: 6 # one each of below encryptions will be applied on each image
              mode: image
              mirrormode: snapshot
              test_config:
                encryption_type: #parent,clone number of formats defined should correspond to num_images
                  - luks1,luks1
                  - luks1,NA
                  - luks2,luks1
                  - luks2,luks2
                  - luks2,NA
                  - NA,luks1
                  - NA,luks2
                  - luks1,luks2
    Test Case Flow
    1. Bootstrap two CEPH clusters and setup snapshot based mirroring in between these clusters
    2. Create a pool and given number of Images, enable snapshot based mirroring for all these images
    3. Apply Encryption to each of the images as per the formats mentioned in test_config above
    4. Create a clone for each image and apply encryption as per formats mentioned in test_config
    5. Verify the data integrity between encrypted image and its clone
    6. On the secondary site, map the encrypted image using its encryption passphrase as read-only
      and verify data integrity
    7. On the secondary site, map the encrypted image as read-write, it should error out appropriately
    8. On the secondary site, map the encrypted image using wrong encryption type and/or passphrase,
      it should error out appropriately
    9. Apply mirroring on the cloned images in Step 4 and perform steps 6 to 8 on these images
    10. Repeat the above steps for replicated and ec pool
    """
    pool_types = ["rep_pool_config", "ec_pool_config"]
    log.info(
        "Running luks encryption tests on images mirrored using snapshot based mirroring"
    )
    try:
        kw.get("config", {})["do_not_run_io"] = True
        mirror_obj = initial_mirror_config(**kw)
        log.info("Initial configuration complete")

        pool_types = list(mirror_obj.values())[0].get("pool_types")
        for pool_type in pool_types:
            ret_val = test_encryption_on_mirrored_cluster(
                mirror_obj=mirror_obj,
                pool_type=pool_type,
                **kw,
            )
            if ret_val:
                log.error(f"RBD encryption test failed for pool_type {pool_type}")
                break
    except Exception as e:
        log.error(f"Testing mirrored encryption failed with error {str(e)}")
        ret_val = 1
    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return ret_val
