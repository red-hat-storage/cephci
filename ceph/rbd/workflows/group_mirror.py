import json
import time

from ceph.rbd.workflows.rbd_mirror import wait_for_status
from utility.log import Log

log = Log(__name__)


def verify_group_mirroring_state(rbd, mirror_state, **group_kw):
    # Verifies whether group mirroring state matched with the expected state passed as argument
    if mirror_state == "Disabled":
        (group_mirror_status, _) = rbd.mirror.group.status(**group_kw)
        if _:
            if "mirroring not enabled on the group" in _:
                return True
            else:
                return False
        else:
            return False
    if mirror_state == "Enabled":
        (group_info, _) = rbd.group.info(**group_kw)
        if "enabled" in group_info:
            return True
        else:
            return False


def enable_group_mirroring_and_verify_state(rbd, **group_kw):
    # Enable Group Mirroring and verify if the mirroring state shows enabled
    (out, _) = rbd.mirror.group.enable(**group_kw)
    if "Mirroring enabled" in out:
        out = verify_group_mirroring_state(rbd, "Enabled", **group_kw)
        if out:
            return 0
        else:
            return 1
    else:
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
    (group_mirror_status, _) = rbd_primary.mirror.group.status(
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
        return 0
    else:
        return 1


def wait_for_idle(rbd, **group_kw):
    # Wait for 300 seconds for group mirroring replay state to be idle for all images in the group
    retry = 0
    while retry < 60:
        (group_mirror_status, _) = rbd.mirror.group.status(**group_kw, format="json")
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
