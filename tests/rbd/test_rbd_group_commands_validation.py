"""Module to validate rbd group-related commands.

Test case covered -
CEPH-83594545 - RBD Group-related commands validation

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Deploy Ceph on version 8.0 or greater.
2. Create an RBD pool named pool1 using ceph osd pool create pool1
3. Create an RBD group named group1 in pool1 using rbd group create pool1/group1
4. Create an RBD image named image1 in pool1 using rbd create pool1/image1 --size 1024
5. Add the image image1 to the group group1 using rbd group image add pool1/group1 pool1/image1
6. List images in the group group1 using rbd group image list pool1/group1
7. Remove the image image1 from the group group1 using rbd group image remove pool1/group1 pool1/image1
8. List the RBD groups in pool1 using rbd group list pool1
9. Rename the group group1 to group2 within pool1 using rbd group rename pool1/group1 pool1/group2
10. Create a snapshot of the group group2 named snap1 using rbd group snap create pool1/group2 --snap snap1
11. List snapshots of the group group2 using rbd group snap list pool1/group2
12. Remove the snapshot snap1 from the group group2 using rbd group snap remove pool1/group2 --snap snap1
13. Rename the group's snapshot snap1 to snap2 using rbd group snap rename pool1/group2 --snap snap1 <new-snap-name>
14. Rollback the group group2 to the snapshot snap2 using rbd group snap rollback pool1/group2 --snap snap2
15. Delete the group group2 using rbd group remove pool1/group2
"""

from copy import deepcopy

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def validate_rbd_group_commands(rbd_obj, client, **kw):
    """
        validate rbd group-related commands.
    Args:
        rbd_obj: RBD object
        client : client node object
        **kw: any other arguments
    """

    kw["client"] = client
    rbd = rbd_obj.get("rbd")

    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = deepcopy(getdict(rbd_config))

        for pool, pool_config in multi_pool_config.items():
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")

            # Create an RBD group in pool
            group = "group_" + random_string(len=4)
            out, err = rbd.group.create(**{"group-spec": f"{pool}/{group}"})
            if err:
                log.error(f"Create group {group} failed with error {err}")
                return 1
            else:
                log.info(f"Successfully created group {group} in {pool}")

            # List group
            out, err = rbd.group.list(**{"pool": pool})
            if err:
                log.error(f"Group list failed with error {err}")
                return 1
            if group in out:
                log.info(f"Group {group} listed in pool {pool}")
            else:
                log.error(f"Group {group} not listed in pool {pool}")
                return 1

            # Create an RBD image in pool1
            image = "image_" + random_string(len=4)
            out, err = rbd.create(**{"image-spec": f"{pool}/{image}", "size": 1024})
            if err:
                log.error(f"Create image {pool}/{image} failed with error {err}")
                return 1
            else:
                log.info(f"Successfully created image {pool}/{image}")

            # Add image to the group
            out, err = rbd.group.image.add(
                **{"group-spec": f"{pool}/{group}", "image-spec": f"{pool}/{image}"}
            )
            if err:
                log.error(
                    f"Failed to add image {pool}/{image} to the group {pool}/{group} with error {err}"
                )
                return 1
            else:
                log.info(
                    f"Successfully added image {pool}/{image} to the group {pool}/{group}"
                )

            # List images in the group
            out, err = rbd.group.image.list(**{"group-spec": f"{pool}/{group}"})
            if err:
                log.error(f"List images in group failed with error {err}")
                return 1
            if f"{pool}/{image}" in out:
                log.info(f"{pool}/{image} listed in group {pool}/{group}")
            else:
                log.error(f"{pool}/{image} not listed in the group image list")
                return 1

            # Remove the image  from the group
            out, err = rbd.group.image.rm(
                **{"group-spec": f"{pool}/{group}", "image-spec": f"{pool}/{image}"}
            )
            if err:
                log.error(
                    f"Remove image {pool}/{image} from group {pool}/{group} failed with error {err}"
                )
                return 1
            else:
                log.info(
                    f"Successfully removed image {pool}/{image} from group {pool}/{group}"
                )

            # List images in the group
            out, err = rbd.group.image.list(**{"group-spec": f"{pool}/{group}"})

            if err:
                log.error(f"List images in group failed with error {err}")
                return 1
            if f"{pool}/{image}" not in out:
                log.info(
                    f"{pool}/{image} not listed in group after successful image deletion"
                )
            else:
                log.error(
                    f"{pool}/{image} listed in group after successful image deletion"
                )
                return 1

            # Add image back to the group
            out, err = rbd.group.image.add(
                **{"group-spec": f"{pool}/{group}", "image-spec": f"{pool}/{image}"}
            )
            if err:
                log.error(
                    f"Adding image {pool}/{image} to group failed with error {err}"
                )
                return 1
            else:
                log.info(
                    f"Successfully added image {pool}/{image} to group {pool}/{group}"
                )

            # Rename the group
            group_new = "group_new" + random_string(len=4)
            out, err = rbd.group.rename(
                **{
                    "source-group-spec": f"{pool}/{group}",
                    "dest-group-spec": f"{pool}/{group_new}",
                }
            )
            if err:
                log.error(f"Rename group {pool}/{group_new} failed with error {err}")
                return 1
            else:
                log.info(f"Successfully renamed group to {pool}/{group_new}")

            # Create group snap
            snap = "snap_" + random_string(len=4)
            out, err = rbd.group.snap.create(
                **{"group-spec": f"{pool}/{group_new}@{snap}"}
            )
            if err:
                log.error(
                    f"Create snap {pool}/{group_new}@{snap} failed with error {err}"
                )
                return 1
            else:
                log.info(f"Successfully created snap {pool}/{group_new}@{snap}")

            # list group snap
            out, err = rbd.group.snap.list(**{"group-spec": f"{pool}/{group_new}"})
            if err:
                log.error(f"Group snap list failed with error {err}")
                return 1
            if snap in out:
                log.info(f"Group snap {snap} listed in group")
            else:
                log.error(f"Group snap {snap} not listed in group")
                return 1

            # Delete the group snap
            out, err = rbd.group.snap.rm(**{"group-spec": f"{pool}/{group_new}@{snap}"})
            if err:
                log.error(
                    f"Delete group snap {pool}/{group_new}@{snap} failed with error {err}"
                )
                return 1
            else:
                log.info("Successfully deleted grop snap {pool}/{group_new}@{snap}")

            # list group snap
            out, err = rbd.group.snap.list(**{"group-spec": f"{pool}/{group_new}"})
            if err:
                log.error(f"Group snap list failed with error {err}")
                return 1
            if snap not in out:
                log.info(
                    f"Group snap {snap} not listed in group snap list after successful deletion"
                )
            else:
                log.error(
                    f"Group snap {snap} listed in group snap list after successful deletion"
                )
                return 1

            # Create group snap back
            out, err = rbd.group.snap.create(
                **{"group-spec": f"{pool}/{group_new}@{snap}"}
            )
            if err:
                log.error(
                    f"Create group snap {pool}/{group_new}@{snap} failed with error {err}"
                )
                return 1
            else:
                log.info(f"Successfully created group snap  {pool}/{group_new}@{snap}")

            # Rename group snap
            snap_new = "snap_" + random_string(len=4)
            out, err = rbd.group.snap.rename(
                **{
                    "group-snap-spec": f"{pool}/{group_new}@{snap}",
                    "dest-snap": snap_new,
                }
            )
            if err:
                log.error(
                    f"Group snap rename of {pool}/{group_new}@{snap_new} failed with error {err}"
                )
                return 1
            else:
                log.info(
                    f"Successfully renamed group snap to {pool}/{group_new}@{snap_new}"
                )

            # list group snap
            out, err = rbd.group.snap.list(**{"group-spec": f"{pool}/{group_new}"})
            if err:
                log.error(f"Group snap list failed with error {err}")
                return 1
            else:
                if snap_new in out and snap not in out:
                    log.info(f"Renamed Group snap {snap_new} listed in group snap list")
                else:
                    log.error(
                        f"Renamed Group snap {snap_new} not listed in group snap list"
                    )
                    return 1

            # Rollback group snap
            out, err = rbd.group.snap.rollback(
                **{"group-snap-spec": f"{pool}/{group_new}@{snap_new}"}
            )
            if "100% complete" in err:
                log.info(
                    f"Successfully rolled back group snap {pool}/{group_new}@{snap_new}"
                )
            else:
                log.error(
                    f"Group snap rollback of {pool}/{group_new}@{snap_new} failed with error {err}"
                )
                return 1

            # Remove Group
            out, err = rbd.group.remove(**{"group-spec": f"{pool}/{group_new}"})
            if err:
                log.error(f"Deleting group {pool}/{group_new} failed with error {err}")
                return 1
            else:
                log.info(f"Successfully deleted group {pool}/{group_new}")

            # List group
            out, err = rbd.group.list(**{"pool": pool})
            if err:
                log.error(f"Group list failed with error {err}")
                return 1
            else:
                if group not in out:
                    log.info(f"Group {group} not listed in pool {pool}")
                else:
                    log.error(f"Group {group} listed in pool {pool}")
                    return 1
    return 0


def run(**kw):
    """
    This test verifies rbd group commands
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    try:
        log.info("CEPH-83594545 - RBD group commands validation")
        pool_types = list()
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        if rbd_obj:
            log.info("Executing test on Replicated and EC pool")
            if validate_rbd_group_commands(rbd_obj, client, **kw):
                return 1
            log.info("Test RBD group commands validation is successful")

    except Exception as e:
        log.error(f"Test RBD group commands validation failed: {str(e)}")
        return 1

    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return 0
