import json
import time

from ceph.rbd.workflows.rbd_mirror import wait_for_status
from utility.log import Log

log = Log(__name__)


def verify_group_mirroring_state(rbd, mirror_state, **group_kw):
    # Verifies whether group mirroring state matched with the expected state passed as argument
    if mirror_state == "Disabled":
        (group_mirror_status, err) = rbd.mirror.group.status(**group_kw)
        if err and "mirroring not enabled on the group" in err:
            return 1
        return 0
    if mirror_state == "Enabled":
        (group_info, err) = rbd.group.info(**group_kw)
        if "enabled" in group_info:
            return 1
        return 0


def verify_group_image_list(rbd, **kw):
    # Verifies image list of the group contains particular image or not
    (group_image_list, err) = rbd.group.image.list(**kw, format="json")
    for spec in list(json.loads(group_image_list)):
        image_spec = spec["pool"] + "/" + spec["image"]
        if image_spec == kw["image-spec"]:
            return 1
    return 0


def enable_group_mirroring_and_verify_state(rbd, **group_kw):
    # Enable Group Mirroring and verify if the mirroring state shows enabled
    (out, err) = rbd.mirror.group.enable(**group_kw)
    if "Mirroring enabled" in out and verify_group_mirroring_state(
        rbd, "Enabled", **group_kw
    ):
        return 0
    return 1


def disable_group_mirroring_and_verify_state(rbd, **group_kw):
    # Disable Group Mirroring and verify if the mirroring state shows Disabled
    (out, err) = rbd.mirror.group.disable(**group_kw)
    if "Mirroring disabled" in out and verify_group_mirroring_state(
        rbd, "Disabled", **group_kw
    ):
        return 0
    return 1


def add_group_image_and_verify(rbd, **kw):
    # Add image to the group and verify if added
    (out, err) = rbd.group.image.add(**kw)
    if "cannot add image to mirror enabled group" in out and not (
        verify_group_image_list(rbd, **kw)
    ):
        return 0
    return 1


def remove_group_image_and_verify(rbd, **kw):
    # Remove image from the group and verify if removed
    (out, err) = rbd.group.image.rm(**kw)
    if "cannot remove image from mirror enabled group" in out and not (
        verify_group_image_list(rbd, **kw)
    ):
        return 0
    return 1


def group_mirror_status_verify(
    primary_cluster,
    secondary_cluster,
    rbd_primary,
    rbd_secondary,
    primary_state,
    secondary_state,
    **group_kw
):
    # Verify Group mirror Status is matching the expected state passed as argument and Also
    # Verifies global ids of both clusters matches
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
    log.info("Primary cluster group mirorr status: " + str(group_mirror_status))
    primary_global_id = json.loads(group_mirror_status)["global_id"]

    (group_mirror_status, _) = rbd_secondary.mirror.group.status(
        **group_kw, format="json"
    )
    log.info("Secondary cluster group mirorr status: " + str(group_mirror_status))
    secondary_global_id = json.loads(group_mirror_status)["global_id"]

    if primary_global_id == secondary_global_id:
        log.info("Global ids of both the clusters matched")
        return 0
    else:
        return 1


def wait_for_idle(rbd, **group_kw):
    # Wait for 300 seconds for group mirroring replay state to be idle for all images in the group
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
        log.error(
            "Replay state is not idle for image " + image + " even after 300 seconds"
        )
        return 1

    return 0
