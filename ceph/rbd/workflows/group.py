from copy import deepcopy

from ceph.rbd.utils import random_string
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def create_group_and_verify(**kw):
    """
    Creates a group and verifies if group creation is successfull
    Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str): pool name where group should be created
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
                pool(str): pool name where group should be created
                namespace(str): pool|[namespace] where greoup should be created
                group(str): group name to be created
                image(str): image to be added to the group
                group-spec: {pool}/{namespace}/{group}
                image-spec: {pool}/{namespace}/{image}
    """
    rbd = Rbd(kw["client"])

    # add image to the [pool]|[namespace]|<group>
    pool = kw.get("pool", "rbd")
    namespace = kw.get("namespace", None)
    group = kw.get("group", None)
    image = kw.get("image", None)
    group_spec = kw.get("group-spec", None)
    image_spec = kw.get("image-spec", None)

    image_group_kw = {}

    if group_spec and image_spec:

        image_group_kw.update({"group-spec": group_spec})
        image_group_kw.update({"image-spec": image_spec})
        group_entities = group_spec.split("/")
        if len(group_entities) > 2:
            namespace = group_entities[1]
            group = group_entities[2]
        else:
            group = group_entities[1]
            pool = group_entities[0]
        image_entities = image_spec.split("/")
        if len(image_entities) > 2:
            image_pool = image_entities[0]
            namespace = image_entities[1]
            image = image_entities[2]
        else:
            image_pool = image_entities[0]
            image = image_entities[1]

    else:

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
    if group_spec and image_spec:
        group_ls_kw.update({"group-spec": group_spec})
    else:
        group_ls_kw.update({"pool": pool})
        group_ls_kw.update({"group": group})
        if namespace is not None:
            group_ls_kw.update({"namespace": namespace})
    (g_ls_out, _) = rbd.group.image.list(**group_ls_kw)

    if namespace and f"{image_pool}/{namespace}/{image}" in g_ls_out:
        log.info(f"Added Namespace image in the group {group}  successfully verified")
    elif f"{pool}/{image}" in g_ls_out:
        log.info(f"Image {image} to the group {group} successfully verified")
    elif f"{image_pool}/{image}" in g_ls_out:
        log.info(f"Image {image} to the group {group} successfully verified")
    else:
        log.info(f"Image {image} to the group {group} verification failed")
        return 1
    return 0


def create_snap_and_verify(**kw):
    """
    Create a snapshot for group of images
    Args:
        kw(dict): Key/value pairs that needs to be provided to this method
            Example::
            Supported keys:
                pool(str): pool name where group should be created
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
                pool(str): pool name where group should be created
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


def group_info(**kw):
    """
    Displays Pool Group info
    Args:
        kw(dict): Key/value pairs that needs to be provided to this method
            Example::
            Supported keys:
                pool(str): pool name where group exist
                group(str): group name for which information needs to be retrieved
    """
    rbd = Rbd(kw["client"])
    pool = kw.get("pool", "rbd")
    group = kw.get("group", None)
    group_info_kw = kw
    group_info_kw.pop("client")
    if pool is not None:
        group_info_kw.update({"pool": pool})
    if group is not None:
        group_info_kw.update({"group": group})
    else:
        log.error(
            f"Group is the must param for displaying group info for group: {group}"
        )
        return 1

    # Group info
    (group_i_out, group_i_err) = rbd.group.info(**group_info_kw)
    return (group_i_out, group_i_err)


def group_snap_info(**kw):
    """
    Displays info for group snapshot
    Args:
        kw(dict): Key/value pairs that needs to be provided to this method
            Example::
            Supported keys:
                pool(str): pool name where group is present
                group(str): group name for which information needs to be retrived
                snap(str): group snapshot name for which info is needed
    """
    rbd = Rbd(kw["client"])
    pool = kw.get("pool", "rbd")
    group = kw.get("group", None)
    snap = kw.get("snap", None)
    group_snap_info_kw = kw
    group_snap_info_kw.pop("client")
    if pool is not None:
        group_snap_info_kw.update({"pool": pool})
    if snap is not None:
        group_snap_info_kw.update({"snap": snap})
    if group is not None:
        group_snap_info_kw.update({"group": group})
    else:
        log.error(f"Group is the must param for group snapshot information: {group}")
        return 1

    # Group info
    (group_snap_out, group_snap_err) = rbd.group.snap.info(**group_snap_info_kw)
    return (group_snap_out, group_snap_err)
