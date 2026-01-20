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
        group_mirror_status, err = rbd.mirror.group.status(**group_kw)
        if err and "mirroring not enabled on the group" in err:
            return True
        return False
    if mirror_state == "Enabled":
        group_info, err = rbd.group.info(**group_kw, format="json")
        if err:
            log.error("Error in group info: " + err)
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
    group_image_list, err = rbd.group.image.list(**kw, format="json")
    if err:
        log.error("Error in group image list: " + err)
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
    mirror_group_enable_status, err = rbd.mirror.group.enable(**group_kw)
    if err:
        raise Exception("Error in group mirror enable: " + err)
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
    group_mirroring_disable_status, err = rbd.mirror.group.disable(**group_kw)
    if err:
        raise Exception("Error in group mirror disable: " + err)
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
    group_image_add_status, err = rbd.group.image.add(**kw)
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
    group_image_remove_status, err = rbd.group.image.rm(**kw)
    if err:
        raise Exception("Error in group image remove: " + err)
    if (
        "cannot remove image from mirror enabled group" in group_image_remove_status
        and not verify_group_image_list(rbd, **kw)
    ):
        raise Exception("cannot remove image from mirror enabled group")


def create_group_add_images(rbd, **kw):
    """
    Create groups, images and add images to group
    Args:
      rbd: RBD object
      **kw:
      no_of_group: int value, number of groups to be created
      no_of_images_in_each_group: int value, number of images to be created and added to each group
      size_of_image: Size of each image created
      pool_spec: pool spec (with or without namespace)
    """
    res = {}
    cnt = 0
    for i in range(0, kw["no_of_group"]):
        group_spec = f"{kw['pool_spec']}/group{i+1}"
        group_create, err = rbd.group.create(**{"group-spec": group_spec})
        if err:
            raise Exception("Error in group creation: " + err)

        # Create Image and add to the group
        group_image = []
        for i in range(0, kw["no_of_images_in_each_group"]):
            cnt = cnt + 1
            image_spec = f"{kw['pool_spec']}/image{cnt+1}"
            image_create, err = rbd.create(
                **{
                    "image-spec": image_spec,
                    "size": kw["size_of_image"],
                }
            )
            if err:
                raise Exception("Error in image creation: " + err)

            # Add Image to group
            add_group_image_and_verify(
                rbd, **{"group-spec": group_spec, "image-spec": image_spec}
            )
            log.info(
                "Successfully verified image "
                + image_spec
                + " is added to the group "
                + group_spec
            )
            group_image.append(image_spec)
        res[group_spec] = group_image
    return res


def group_mirror_status_verify(
    primary_cluster,
    secondary_cluster,
    rbd_primary,
    rbd_secondary,
    primary_state,
    secondary_state,
    *,
    global_id=False,
    **group_kw,
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
        global_id: True if global ids of both clusters need to be verified
        **group_kw: Group spec <pool_name>/<group_name>
    """
    if "group-spec" in group_kw.keys():
        groupspec = group_kw["group-spec"]
    elif "namespace" in group_kw.keys():
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
    if global_id is True:
        group_mirror_status, err = rbd_primary.mirror.group.status(
            **group_kw, format="json"
        )
        if err:
            raise Exception(
                "Error in group mirror status for group: " + groupspec + " err: " + err
            )
        log.info("Primary cluster group mirror status: " + str(group_mirror_status))
        primary_global_id = json.loads(group_mirror_status)["global_id"]

        group_mirror_status, err = rbd_secondary.mirror.group.status(
            **group_kw, format="json"
        )
        if err:
            raise Exception(
                "Error in group mirror status for group: " + groupspec + " err: " + err
            )
        log.info("Secondary cluster group mirror status: " + str(group_mirror_status))
        secondary_global_id = json.loads(group_mirror_status)["global_id"]
        if primary_global_id == secondary_global_id:
            log.info("Global ids of both the clusters matched")
        else:
            raise Exception("Global ids of both the clusters are not same")


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
        group_mirror_status, err = rbd.mirror.group.status(**group_kw, format="json")
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


def verify_group_snapshot_schedule(rbd, pool, group, interval="1m", **kw):
    """
    This will verify the group snapshot rolls over when the group
    snapshot based mirroring is enabled
    - verifies the 'schedule ls' has the expected interval listed
    - verifies the group snapshot gets created as per the schedule
    Args:
        rbd: rbd object
        pool: pool name
        group: group name
        interval : this is interval and specified in min
        kw: optional args
    Returns:
        0 if snapshot schedule is verified successfully
        1 if fails
    """
    try:
        namespace = kw.get("namespace", "")
        status_spec = {"pool": pool, "group": group, "format": "json"}
        group_spec = None
        if namespace:
            status_spec.update({"namespace": namespace})
            group_spec = pool + "/" + namespace + "/" + group
        else:
            group_spec = pool + "/" + group

        verify_group_snapshot_ls(rbd, group_spec, interval, **status_spec)
        output, err = rbd.mirror.group.status(**status_spec)
        if err:
            log.error(
                "Error while fetching mirror group status for group %s", group_spec
            )
            return 1
        json_dict = json.loads(output)
        log.info(f"Group status : \n {json_dict}")
        snapshot_names = [i["name"] for i in json_dict.get("snapshots")]
        log.info(f"Group snapshot_names Before : {snapshot_names}")
        interval_int = int(interval[:-1])
        wait_time = interval_int * 120
        log.info("Waiting for %s sec for snapshot to be rolled over", wait_time)
        time.sleep(wait_time)
        output, err = rbd.mirror.group.status(**status_spec)
        if err:
            log.error(
                "Error while fetching mirror group status for group %s", group_spec
            )
            return 1
        json_dict = json.loads(output)
        log.info(f"Group status : \n {json_dict}")
        snapshot_names_1 = [i["name"] for i in json_dict.get("snapshots")]
        log.info(f"Group snapshot_names After : {snapshot_names_1}")
        if snapshot_names != snapshot_names_1:
            log.info(
                "Snapshot schedule verification successful for group %s", group_spec
            )
            return 0
        log.error("Snapshot schedule verification failed for group %s", group_spec)
        return 1
    except Exception as e:
        log.error(
            "Snapshot verification failed for group %s with error %s", group_spec, e
        )
        return 1


def verify_group_snapshot_ls(rbd, group_spec, interval, **status_spec):
    """
    This will verify the group snapshot list in the scheduler when the group
    snapshot based mirroring is enabled
    Args:
        rbd: rbd object
        group_spec: group spec
        interval: this is interval and specified in min
        status_spec: pool, namespace and group details
    Returns:
        0 if snapshot schedule is verified successfully
        1 if fails
    """
    out, err = rbd.mirror.group.snapshot.schedule.ls(**status_spec)
    if err:
        log.error(
            "Error while fetching snapshot schedule list for group  %s, %s",
            group_spec,
            err,
        )
        return 1
    out = str(out).strip("'<>() ").replace("'", '"')
    schedule_list = json.loads(out)
    schedule_present = [
        schedule for schedule in schedule_list if schedule["interval"] == interval
    ]
    if not schedule_present:
        log.error(
            "Snapshot schedule not listed for group %s at interval %s",
            group_spec,
            interval,
        )
        return 1


def get_snap_state_by_snap_id(rbd, snapshot_id, **status_spec):
    """
    This function will return snapshot_state(creating/created) for a given snapshot id
    Args:
        rbd: rbd object
        snapshot_id: Snapshot job id
        status_spec: pool, namespace and group details
    Returns:
        Snapshot state (str), when successful
        raise Exception, if fails
    """
    out, err = rbd.group.snap.list(**status_spec)
    if err:
        raise Exception(
            "Error while fetching snapshot list for group  %s, %s",
            status_spec["group"],
            err,
        )
    snapshot_list = json.loads(out)
    for snap in snapshot_list:
        if snap["namespace"]["type"] == "mirror" and snap["id"] == snapshot_id:
            snapshot_state = snap["state"]
            break
    return snapshot_state


def get_mirror_group_snap_copied_status(rbd, snapshot_id, **status_spec):
    """
    This function will return snapshot_state('Complete':True or 'Complete':False) for a given snapshot id
    Args:
        rbd: rbd object
        snapshot_id: Snapshot job id
        status_spec: pool, namespace and group details
    Returns:
        Snapshot state (str), when successful
        raise Exception, if fails
    """
    out, err = rbd.group.snap.list(**status_spec)
    if err:
        raise Exception(
            "Error while fetching snapshot list for group  %s, %s",
            status_spec["group"],
            err,
        )
    snapshot_list = json.loads(out)
    log.info(f"Snapshot list: {snapshot_list}")
    for snap in snapshot_list:
        log.info(f"Checking snapshot: {snap}")
        if snap["namespace"]["type"] == "mirror" and snap["id"] == snapshot_id:
            snapshot_state = snap["namespace"]["complete"]
            break
    return snapshot_state


def get_mirror_group_snap_id(rbd, **status_spec):
    """
    This will get first mirror group snapshot id from group snap list CLI
    Args:
        rbd: rbd object
        status_spec: pool, namespace and group details
    Returns:
        snapshot id, if successfull
    """
    out, err = rbd.group.snap.list(**status_spec)
    if err:
        raise Exception(
            "Error while fetching snapshot list for group  %s, %s",
            status_spec["group"],
            err,
        )
    snapshot_list = json.loads(out)
    for snap in snapshot_list:
        if snap["namespace"]["type"] == "mirror":
            snapshot_id = snap["id"]
            break
    return snapshot_id


def wait_till_image_sync_percent(rbd, wait_sync_percent, **group_kw):
    """
    Wait till image reach sync_percent
    Args:
        rbd: Rbd object
        wait_sync_percent: Percentage of image sync till which this function has to wait
        **group_kw: Group spec <pool_name>/<group_name>
    """
    retry = 0
    while retry < 60:
        group_mirror_status, err = rbd.mirror.group.status(**group_kw, format="json")
        group_mirror_status = str(group_mirror_status).strip("'<>() ").replace("'", '"')
        group_mirror_status = json.loads(group_mirror_status)
        cnt = 0
        if len(group_mirror_status["peer_sites"][0]["images"]) != 0:
            sync_started = False
            for image in group_mirror_status["peer_sites"][0]["images"]:
                if "PREPARE_REPLAY" in image["description"].split(", ")[-1]:
                    continue
                replay_state = json.loads(image["description"].split(", ")[-1])[
                    "replay_state"
                ]
                if replay_state == "idle":
                    if sync_started is True:
                        cnt = cnt + 1
                    else:
                        continue
                if replay_state == "syncing":
                    sync_started = True
                    syncing_percent = json.loads(image["description"].split(", ")[-1])[
                        "syncing_percent"
                    ]
                    if int(syncing_percent) >= wait_sync_percent:
                        cnt = cnt + 1
                    else:
                        break

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
            "Sync percentage is not acheived to be "
            + wait_sync_percent
            + " even after 300 seconds"
        )
