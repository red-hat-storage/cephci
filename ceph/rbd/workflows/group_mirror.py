import json
import time

from ceph.rbd.workflows.rbd_mirror import wait_for_status
from utility.log import Log

log = Log(__name__)


def verify_group_mirroring_state(rbd, mirror_state, **group_kw):
    """
    Verifies whether group mirroring state matched with the expected state passed as argument
    Returns: True if state matches and False otherwise
    Args:
       rbd: Rbd object
       mirror_state: Mirroring state of the group against which verification has to be made
       **group_kw: Group spec <pool_name>/<group_name>
    """
    if mirror_state == "Disabled":
        (group_mirror_status, err) = rbd.mirror.group.status(**group_kw)
        if err and "mirroring not enabled on the group" in err:
            return True
        return False
    if mirror_state == "Enabled":
        (group_info, err) = rbd.group.info(**group_kw, format="json")
        if err:
            log.error(
                "Error in group info for group: " + group_kw["group"] + " err: " + err
            )
            return False
        if json.loads(group_info)["mirroring"]["state"] == "enabled":
            return True
        return False


def verify_group_image_list(rbd, **kw):
    """
    Verifies image list of the group contains particular image or not
    Returns: True if image is found in group image list and False otherwise
    Args:
        rbd: RBD object
        **kw: Image spec <pool_name>/<image_name>
    """
    (group_image_list, err) = rbd.group.image.list(**kw, format="json")
    if err:
        log.error(
            "Error in group image list for group: " + kw["group"] + " err: " + err
        )
        return False
    for spec in list(json.loads(group_image_list)):
        image_spec = spec["pool"] + "/" + spec["image"]
        if image_spec == kw["image-spec"]:
            return True
    return False


def enable_group_mirroring_and_verify_state(rbd, **group_kw):
    """
    Enable Group Mirroring and verify if the mirroring state shows enabled
    Args:
      rbd: RBD object
      **group_kw: Group spec <pool_name>/<group_name>
    """
    (mirror_group_enable_status, err) = rbd.mirror.group.enable(**group_kw)
    if err:
        raise Exception(
            "Error in group mirror enable for group: "
            + group_kw["group"]
            + " err: "
            + err
        )
    if (
        "Mirroring enabled" in mirror_group_enable_status
        and verify_group_mirroring_state(rbd, "Enabled", **group_kw)
    ):
        log.info("Mirroring is enabled on group")
    else:
        raise Exception("Enable group mirroring Failed")


def disable_group_mirroring_and_verify_state(rbd, **group_kw):
    """
    Disable Group Mirroring and verify if the mirroring state shows Disabled
    Args:
      rbd: RBD object
      **group_kw: Group spec <pool_name>/<group_name>
    """
    (group_mirroring_disable_status, err) = rbd.mirror.group.disable(**group_kw)
    if err:
        raise Exception(
            "Error in group mirror disable for group: "
            + group_kw["group"]
            + " err: "
            + err
        )
    if (
        "Mirroring disabled" in group_mirroring_disable_status
        and verify_group_mirroring_state(rbd, "Disabled", **group_kw)
    ):
        log.info("Mirroring is disabled on group")
    else:
        raise Exception("Disable group mirroring Failed")


def add_group_image_and_verify(rbd, **kw):
    """
    Add image to the group and verify if added
    Args:
      rbd: RBD object
      **kw: Group spec <pool_name>/<group_name>
    """
    (group_image_add_status, err) = rbd.group.image.add(**kw)
    if err:
        raise Exception(
            "Error in group image add for group: " + kw["group-spec"] + " err: " + err
        )
    if (
        "cannot add image to mirror enabled group" in group_image_add_status
        and not verify_group_image_list(rbd, **kw)
    ):
        raise Exception("cannot add image to mirror enabled group")
    elif (
        "cannot add mirror enabled image to group" in group_image_add_status
        and not verify_group_image_list(rbd, **kw)
    ):
        raise Exception("cannot add mirror enabled image to group")


def remove_group_image_and_verify(rbd, **kw):
    """
    Remove image from the group and verify if removed
    Args:
      rbd: RBD object
      **kw: Group spec <pool_name>/<group_name>
    """
    (group_image_remove_status, err) = rbd.group.image.rm(**kw)
    if err:
        raise Exception(
            "Error in group image remove for group: "
            + kw["group-spec"]
            + " err: "
            + err
        )
    if (
        "cannot remove image from mirror enabled group" in group_image_remove_status
        and not verify_group_image_list(rbd, **kw)
    ):
        raise Exception("cannot remove image from mirror enabled group")


def group_mirror_status_verify(
    primary_cluster,
    secondary_cluster,
    rbd_primary,
    rbd_secondary,
    primary_state,
    secondary_state,
    **group_kw
):
    """
    Verify Group mirror Status is matching the expected state passed as argument and Also
    Verifies global ids of both clusters matches
    Args:
        primary_cluster: Primary cluster object
        secondary_cluster: Secondary cluster object
        rbd_primary: rbd object of primary cluster
        rbd_secondary: rbd object of secondary cluster
        primary_state: mirroring state on primary group. for e.g 'up+stopped'
        secondary_state: mirroring state on secondary group for e.g 'up+replaying'
        **group_kw: Group spec <pool_name>/<group_name>
    """
    if "namespace" in group_kw.keys():
        groupspec = (
            group_kw["pool"] + "/" + group_kw["namespace"] + "/" + group_kw["group"]
        )
    else:
        groupspec = group_kw["pool"] + "/" + group_kw["group"]
    wait_for_status(
        rbd=rbd_primary,
        cluster_name=primary_cluster.name,
        groupspec=groupspec,
        state_pattern=primary_state,
    )
    wait_for_status(
        rbd=rbd_secondary,
        cluster_name=secondary_cluster.name,
        groupspec=groupspec,
        state_pattern=secondary_state,
    )
    (group_mirror_status, err) = rbd_primary.mirror.group.status(
        **group_kw, format="json"
    )
    if err:
        raise Exception(
            "Error in group mirror status for group: "
            + group_kw["group"]
            + " err: "
            + err
        )
    log.info("Primary cluster group mirorr status: " + str(group_mirror_status))
    primary_global_id = json.loads(group_mirror_status)["global_id"]

    (group_mirror_status, err) = rbd_secondary.mirror.group.status(
        **group_kw, format="json"
    )
    if err:
        raise Exception(
            "Error in group mirror status for group: "
            + group_kw["group"]
            + " err: "
            + err
        )
    log.info("Secondary cluster group mirorr status: " + str(group_mirror_status))
    secondary_global_id = json.loads(group_mirror_status)["global_id"]

    if primary_global_id == secondary_global_id:
        log.info("Global ids of both the clusters matched")
    else:
        raise Exception("Group Mirror status is not as expected")


def wait_for_idle(rbd, **group_kw):
    """
    Wait for 300 seconds for group mirroring replay state to be idle for all images in the group
    Args:
        rbd: Rbd object
        **group_kw: Group spec <pool_name>/<group_name>
    """
    retry = 0
    while retry < 60:
        time.sleep(10)
        (group_mirror_status, err) = rbd.mirror.group.status(**group_kw, format="json")
        group_mirror_status = str(group_mirror_status).strip("'<>() ").replace("'", '"')
        group_mirror_status = json.loads(group_mirror_status)
        cnt = 0
        if len(group_mirror_status["peer_sites"][0]["images"]) != 0:
            for image in group_mirror_status["peer_sites"][0]["images"]:
                if "PREPARE_REPLAY" in image["description"].split(", ")[-1]:
                    continue
                replay_state = json.loads(image["description"].split(", ")[-1])[
                    "replay_state"
                ]
                if replay_state == "idle":
                    cnt = cnt + 1
            if cnt == len(group_mirror_status["peer_sites"][0]["images"]):
                break
            else:
                time.sleep(5)
                retry = retry + 1
        else:
            time.sleep(5)
            retry = retry + 1
    if retry == 60:
        raise Exception(
            "Replay state is not idle for image " + image + " even after 300 seconds"
        )
