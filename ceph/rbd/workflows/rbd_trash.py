import json

from ceph.rbd.workflows.rbd import list_images_and_verify_with_input
from utility.log import Log

log = Log(__name__)


def get_trash_list(**kw):
    """
    Get all images in trash for a given pool
    kw:{
        "rbd": <>,
        "pool": <>,
    }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    out, err = rbd.trash.ls(pool=pool, format="json")
    if err:
        log.error(f"Listing images in trash failed for pool {pool} with err {err}")
        return []

    trash_list = json.loads(out)
    return trash_list


def is_image_present_in_trash(**kw):
    """
    Check if given image is present in trash
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

    trash_list = get_trash_list(rbd=rbd, pool=pool)
    if not trash_list:
        log.info(f"No images present in trash/trash ls failed for pool {pool}")
        return False

    images_in_trash = [trash_image.get("name") for trash_image in trash_list]
    if image in images_in_trash:
        log.info(f"Image {pool}/{image} is moved to trash successfully")
        return True
    else:
        log.error(f"Image {pool}/{image} is not present in trash")
        if test_ops_parallely:
            raise Exception(f"Image {pool}/{image} is not present in trash")
        return False


def move_image_to_trash_and_verify(**kw):
    """
    Move the specified image to trash and verify that
    the image is present in trash
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
    test_ops_parallely = kw.get("test_ops_parallely", False)
    image_conf = kw.get("image_conf", {})
    expires_at = image_conf.get("expires-at", None)

    log.info(f"Moving image {pool}/{image} to trash")
    trash_conf = {"pool": pool, "image": image}
    if expires_at:
        trash_conf.update({"expires-at": f'"{expires_at}"'})
    out, err = rbd.trash.mv(**trash_conf)
    if (out or err) and f"image {image} will expire at" not in out + err:
        log.error(f"Moving image {pool}/{image} to trash failed with err {out} {err}")
        if test_ops_parallely:
            raise Exception(
                f"Moving image {pool}/{image} to trash failed with err {out} {err}"
            )
        return 1

    log.info(f"Verifying whether image {pool}/{image} is present in trash")

    if is_image_present_in_trash(rbd=rbd, pool=pool, image=image):
        log.info(f"Image {pool}/{image} is present in trash")
    else:
        log.error(f"Image {pool}/{image} not present in trash")
        if test_ops_parallely:
            raise Exception(f"Image {pool}/{image} not present in trash")
        return 1

    return 0


def restore_image_from_trash_and_verify(**kw):
    """
    Restore the specified image from trash and verify that
    the image is not present in trash instead present in rbd ls
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
    test_ops_parallely = kw.get("test_ops_parallely", False)

    log.info(f"Get image id for {pool}/{image} from trash list")

    out, err = rbd.trash.ls(pool=pool, format="json")
    if err:
        log.error(f"Listing images in trash failed for pool {pool} with err {err}")
        if test_ops_parallely:
            raise Exception(
                f"Listing images in trash failed for pool {pool} with err {err}"
            )
        return 1

    trash_list = json.loads(out)
    image_id = [
        trash_image.get("id")
        for trash_image in trash_list
        if trash_image.get("name") == image
    ][0]

    log.info(f"Restoring image {pool}/{image} with id {image_id} from trash")

    restore_conf = {"pool": pool, "image-id": image_id}
    out, err = rbd.trash.restore(**restore_conf)
    if out or err:
        log.error(f"Restore image {pool}/{image} failed with err {out} {err}")
        if test_ops_parallely:
            raise Exception(f"Restore image {pool}/{image} failed with err {out} {err}")
        return 1

    if not is_image_present_in_trash(rbd=rbd, pool=pool, image=image):
        log.info(f"Image {pool}/{image} is not present in trash after restore")
    else:
        log.error(f"Image {pool}/{image} is present in trash after restore")
        if test_ops_parallely:
            raise Exception(f"Image {pool}/{image} is present in trash after restore")
        return 1

    rc = list_images_and_verify_with_input(rbd=rbd, pool=pool, images=[image])
    if rc:
        log.error(f"Image {image} is not present in pool {pool}")
        if test_ops_parallely:
            raise Exception(f"Image {image} is not present in pool {pool}")
        return 1
    log.info(f"Image {image} is present in pool {pool} after restore")

    return 0


def purge_images_and_verify(**kw):
    """Purge all images in a pool and verify that all images without snaps and clones are purged
    Args: kw:{
        "rbd":<>,
        "pool":<>,
        "snap_count_per_image": <>,
        "test_ops_parallely":<>,
        "images_to_be_purged":<>
    }
    """
    purge_err_msg = "some expired images could not be removed\nEnsure that they are closed/unmapped, "
    purge_err_msg += (
        "do not have snapshots (including trashed snapshots with linked clones), are "
    )
    purge_err_msg += "not in a group and were moved to the trash successfully"
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    test_ops_parallely = kw.get("test_ops_parallely")
    images_to_be_purged = kw.get("images_to_be_purged", [])
    expired_before = kw.get("expired_before", None)

    purge_conf = {"pool": pool}
    if expired_before:
        purge_conf.update({"expired-before": f'"{expired_before}"'})
    out, err = rbd.trash.purge(**purge_conf)
    if (
        out
        or err
        and purge_err_msg not in out + err
        and "100% complete" not in out + err
    ):
        log.error(f"Trash purge failed for pool {pool}")
        if test_ops_parallely:
            raise Exception(f"Trash purge failed for pool {pool}")
        return 1

    for image in images_to_be_purged:
        if is_image_present_in_trash(rbd=rbd, pool=pool, image=image):
            log.error(f"Image {pool}/{image} is present in trash after purge")
            if test_ops_parallely:
                raise Exception(f"Image {pool}/{image} is present in trash after purge")
            return 1

    return 0
