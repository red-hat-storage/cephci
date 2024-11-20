# -*- code: utf-8 -*-

from copy import deepcopy
from logging import getLogger

from ceph.rbd.utils import random_string
from cli.rbd.rbd import Rbd

log = getLogger(__name__)


def create_group_and_verify(**kw):
    """
    Creates a group and verifies if group creation is successfull
    Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str): pool nane where group should be created
                namespace(str): pool|[namespace] where greoup should be created
                group(str): group name to be created
    """
    rbd = Rbd(kw["client"])
    group_kw = {}

    # verify if the pool exists, if pool is not passed then consider default pool name i.e rbd
    pool_name = kw.get("pool", "rbd")
    log.info(f"verifying if pool {pool_name} exists")
    pool_init_kw = {}
    pool_init_kw["pool-name"] = pool_name
    (_, pool_stats_err) = rbd.pool.stats(**pool_init_kw)

    # if pool does not exists, fail. Because pool and image creation is taken care by initial_rbd_config.py
    # but namespace creation is not taken care by initial_rbd_config.py
    if pool_stats_err:
        log.error("The pool where group is suppose to be created does not exist")
        return 1
    group_kw.update({"pool": pool_name})

    # verify namespace exists using namespace ls command
    namespace = kw.get("namespace", None)
    if namespace is not None:
        log.info(f"verifying if {namespace} in pool {pool_name} exists")
        ns_verify_kw = {}
        ns_verify_kw["pool-name"] = pool_name
        ns_ls_out = rbd.namespace.list(**ns_verify_kw)
        if namespace not in ns_ls_out[0]:
            log.info(f"namespace {namespace} in {pool_name} is getting created")
            ns_create_kw = {}
            ns_create_kw["namespace"] = namespace
            ns_create_kw["pool-name"] = pool_name
            (_, ns_create_err) = rbd.namespace.create(**ns_create_kw)
            if not ns_create_err:
                log.info(
                    "SUCCESS: Namespace {namespace_name} got created in pool {pool_name} "
                )
            else:
                log.error(
                    f"FAIL: Namespace {namespace} creation failed in pool {pool_name} with error {ns_create_err}"
                )
                return 1
            group_kw.update({"namespace": namespace})

    # create a group in the pool/[namespace]
    group = kw.get("group", None)
    if group is None:
        log.error("Group name is must to create a group")
        return 1
    group_kw.update({"group": group})
    (_, g_err) = rbd.group.create(**group_kw)
    if not g_err:
        log.info(f"SUCCESS: Group {group} got created in pool {pool_name}/{namespace} ")
    else:
        log.error(
            f"FAIL: group {group} creation failed in pool {pool_name}/{namespace} with error {g_err}"
        )
        return 1

    # verify group creation
    group_kw.pop("group")
    (gls_out, _) = rbd.group.list(**group_kw)
    if group in gls_out:
        log.info(f"Group creation {group} verification is successfull")
        return 0
    else:
        log.error(f"Group creation {group} verification failed")
        return 1


def add_image_to_group_and_verify(**kw):
    """
    Adding image to the group
    Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str): pool nane where group should be created
                namespace(str): pool|[namespace] where greoup should be created
                group(str): group name to be created
                image(str): image to be added to the group
    """
    rbd = Rbd(kw["client"])

    # add image to the [pool]|[namespace]|<group>
    pool = kw.get("pool", "rbd")
    namespace = kw.get("namespace", None)
    group = kw.get("group", None)
    image = kw.get("image", None)
    image_group_kw = {}
    if pool is not None:
        image_group_kw.update({"group-pool": pool})
        image_group_kw.update({"image-pool": pool})
    if namespace is not None:
        image_group_kw.update({"namespace": namespace})
    if group is not None:
        image_group_kw.update({"group": group})
    else:
        log.error("Group is the must param for adding image to the group")
        return 1
    if image is not None:
        image_group_kw.update({"image": image})
    else:
        log.error("Image is the must kw for adding image to the group")
        return 1
    (_, img_g_err) = rbd.group.image.add(**image_group_kw)
    if not img_g_err:
        log.info(f"{image} successfully added to the group {group}")
    else:
        log.error(f"{image} failed adding to the group {group} {img_g_err}")
        return 1

    # verify image creation to the group
    group_ls_kw = {}
    group_ls_kw.update({"pool": pool})
    group_ls_kw.update({"group": group})
    (g_ls_out, _) = rbd.group.image.list(**group_ls_kw)
    if f"{pool}/{image}" in g_ls_out:
        log.info(f"Image {image} to the group {group} successfully verified")
        return 0
    else:
        log.info(f"Image {image} to the group {group} verification failed")
        return 1


def create_snap_and_verify(**kw):
    """
    Create a snapshot for group of images
    Args:
        kw(dict): Key/value pairs that needs to be provided to this method
            Example::
            Supported keys:
                pool(str): pool nane where group should be created
                namespace(str): pool|[namespace] where greoup should be created
                group(str): group name to be created
                snap(str): snap to be created to the group
    """
    rbd = Rbd(kw["client"])
    pool = kw.get("pool", "rbd")
    namespace = kw.get("namespace", None)
    group = kw.get("group", None)
    snap = kw.get("snap", None)
    snap_create_kw = {}
    if pool is not None:
        snap_create_kw.update({"pool": pool})
    if namespace is not None:
        snap_create_kw.update({"namespace": namespace})
    if group is not None:
        snap_create_kw.update({"group": group})
    else:
        log.error(f"Group is the must param for snap create but given {group}")
        return 1
    if snap is None:
        snap = random_string(len=5)
    snap_create_kw.update({"snap": snap})

    # create group snapshot
    (_, snap_c_err) = rbd.group.snap.create(**snap_create_kw)
    if not snap_c_err:
        log.info(f"{snap} successfully created for the group {group}")
    else:
        log.error(f"{snap} creation failed for the group {group} {snap_c_err}")
        return 1

    # snap creation validation by rbd group snap list
    snap_list_kw = deepcopy(snap_create_kw)
    _ = snap_list_kw.pop("snap")
    (snap_l_out, _) = rbd.group.snap.list(**snap_list_kw)
    if snap in snap_l_out:
        log.info(
            f"{snap} creation successfully verified by snap list for the group {group}"
        )
        return 0
    else:
        log.error(
            f"{snap} creation validation by snap list failed for the group {group} {snap_c_err}"
        )
        return 1


def rollback_to_snap(**kw):
    """
    Rollbacks group of ima to the given snapshot
    Args:
        kw(dict): Key/value pairs that needs to be provided to this method
            Example::
            Supported keys:
                pool(str): pool nane where group should be created
                namespace(str): pool|[namespace] where greoup should be created
                group(str): group name to be created
                snap(str): snap to be rollbacked to the group
    """
    rbd = Rbd(kw["client"])
    pool = kw.get("pool", "rbd")
    namespace = kw.get("namespace", None)
    group = kw.get("group", None)
    snap = kw.get("snap", None)
    snap_rollback_kw = {}
    if pool is not None:
        snap_rollback_kw.update({"pool": pool})
    if namespace is not None:
        snap_rollback_kw.update({"namespace": namespace})
    if group is not None:
        snap_rollback_kw.update({"group": group})
    else:
        log.error(f"Group is the must param for snap create but given {group}")
        return 1
    if snap is not None:
        snap_rollback_kw.update({"snap": snap})
    else:
        log.error(f"Snap is the must param for snap rollback but given {snap}")
        return 1

    # rollback to given snap
    (snap_r_out, snap_r_err) = rbd.group.snap.rollback(**snap_rollback_kw)
    if snap_r_err and "100% complete" not in snap_r_out + snap_r_err:
        log.error(f"Group {group} rollbacked to {snap} failed {snap_r_err}")
        return 1
    else:
        log.info(f"SUCCESS: Group {group} rollbacked to {snap} successfully")
        return 0
