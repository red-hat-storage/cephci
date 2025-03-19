"""
Module to verify :
  -  Live migration of image with external data source as RAW data format in single ceph cluster

Test case covered:
CEPH-83584085 -  Live migration of image with external data source as RAW data format in single ceph cluster

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1.Deploy single ceph cluster along with mon,mgr,osd’s
2.Create two pools with rbd application enabled for migration as source and destination pool
3.Store the external raw data in json spec
E.g: testspec.json
{
    "type": "raw",
    "stream": {
      "type": "file",
      "file_path": "/mnt/image-head.raw"
    },
    "snapshots": [
        {
            "type": "raw",
            "name": "snap1",
            "stream": {
              "type": "file",
       "file_path": "/mnt/image-snap1.raw"
            }
        },
    ] (optional oldest to newest ordering of snapshots)
}
Get the qcow2 data format with https or s3 streams.
'{"type":"raw","stream":{"type":"http","url":"http://download.ceph.com/qa/ubuntu-12.04.raw"}}'
4.Keep two files on image mounted path where one file with data filled
and checksum noted, other file keep IO in-progress
Note down md5sum checksum for rest data and IO should not interrupt during migration
5.Execute prepare migration with import-only option for RBD image from an external source of raw data
E.g:
echo '{"type":"raw","stream":{"type":"http","url":"http://download.ceph.com/qa/ubuntu-12.04.qcow2"}}' |
rbd migration prepare --import-only --source-spec-path - <pool_name>/<image_name> --cluster <cluster-source>
6. Initiate migration execute using migration execute command
E.g:
rbd migration execute TARGET_POOL_NAME/SOURCE_IMAGE_NAME
7. Check the progress of migration using rbd status
E.g:
rbd status TARGET_POOL_NAME/SOURCE_IMAGE_NAME	It should display output with migration key
[ceph: root@rbd-client /]# rbd status targetpool1/sourceimage1
Watchers: none
Migration:
source: sourcepool1/testimage1 (adb429cb769a)
destination: targetpool1/testimage1 (add299966c63)
state: executed
8.Commit the Migration using migration commit command
E.g:
rbd migration commit TARGET_POOL_NAME/SOURCE_IMAGE_NAME
9.Check the disk usage of the image for data transfer using rbd du command
10.Verify data integrity by calculating MD5 sums for both the source RBD image and the migrated RBD image.
11.Check migrated image metadata using rbd info command
12. initiate the IO on new image
"""

import tempfile
from copy import deepcopy

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import check_data_integrity, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.migration import run_prepare_execute_commit
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def migration_with_raw_data_format(rbd_obj, client, **kw):
    """
    Test Live migration of image with external data source as RAW data format in single ceph cluster
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
            kw["pool-name"] = pool
            kw.update({pool: {}})
            kw[pool].update({"pool_type": pool_type})
            # Create an RBD image in pool
            image = "image_" + random_string(len=3)
            out, err = rbd.create(**{"image-spec": pool + "/" + image, "size": 1024})
            if err:
                log.error(
                    "Create image " + pool + "/" + image + " failed with error " + err
                )
                return 1
            else:
                log.info("Successfully created image " + pool + "/" + image)

            # Map, mount and run IOs
            err = run_IO(rbd, pool, image, **kw)
            if err:
                return 1

            # Export rbd image to raw data file
            raw_file = tempfile.mktemp(prefix=image + "_", suffix=".raw")
            rbd.export(
                **{
                    "source-image-or-snap-spec": pool + "/" + image,
                    "path-name": raw_file,
                }
            )
            raw_spec = {
                "type": "raw",
                "stream": {"type": "file", "file_path": raw_file},
            }
            kw["cleanup_files"].append(raw_file)
            kw[pool].update({"spec": raw_spec})

            # Perform Prepare, execute, commit migration
            err = run_prepare_execute_commit(rbd, pool, image, **kw)
            if err:
                return 1

            # Check the disk usage of the image using rbd du
            log.info(
                "Verifying Image " + kw[pool]["target_image"] + " size with du command"
            )
            target_image_spec = kw[pool]["target_pool"] + "/" + kw[pool]["target_image"]
            image_config = {"image-spec": target_image_spec}
            out = rbd.image_usage(**image_config)
            log.info(out)
            image_data = out[0]
            target_image_size = image_data.split("\n")[1].split()[3].strip()
            log.info("Image size captured : " + target_image_size)
            out = rbd.get_raw_file_size(raw_file)
            file_data = out[0]
            exported_file_size = file_data.split(" ")[4].split(".")[0].strip()
            if target_image_size != exported_file_size:
                log.error(
                    "Image size Verification failed for " + kw[pool]["target_image"]
                )
                return 1

            # Compare md5sum Integrity
            err = migrate_check_consistency(rbd, pool, image, **kw)
            if err:
                return 1

            # Initiate IO/ Run IO on new image
            err = run_IO(rbd, kw[pool]["target_pool"], kw[pool]["target_image"], **kw)
            if err:
                return 1

    return 0


def migrate_check_consistency(rbd, pool, image, **kw):
    """
    Function to carry out the following:
      - Compare Md5sum of source and migrated images
    Args:
        kw: rbd object, pool, image, test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    data_integrity_spec = {
        "first": {
            "image_spec": pool + "/" + image,
            "rbd": rbd,
            "client": kw["client"],
            "file_path": "/tmp/" + random_string(len=3),
        },
        "second": {
            "image_spec": kw[pool]["target_pool"] + "/" + kw[pool]["target_image"],
            "rbd": rbd,
            "client": kw["client"],
            "file_path": "/tmp/" + random_string(len=3),
        },
    }
    rc = check_data_integrity(**data_integrity_spec)
    if rc:
        log.error(
            "Data consistency check failed for "
            + kw[pool]["target_pool"]
            + "/"
            + kw[pool]["target_image"]
        )
        return 1
    else:
        log.info("Data is consistent between the source and target images.")


def run_IO(rbd, pool, image, **kw):
    fio = kw.get("config", {}).get("fio", {})
    io_config = {
        "rbd_obj": rbd,
        "client": kw["client"],
        "size": fio["size"],
        "do_not_create_image": True,
        "config": {
            "file_size": fio["size"],
            "file_path": ["/mnt/mnt_" + random_string(len=5) + "/file"],
            "get_time_taken": True,
            "image_spec": [pool + "/" + image],
            "operations": {
                "fs": "ext4",
                "io": True,
                "mount": True,
                "device_map": True,
            },
            "cmd_timeout": 2400,
            "io_type": "write",
        },
    }
    out, err = krbd_io_handler(**io_config)
    if err:
        log.error("Map, mount and run IOs failed for " + pool + "/" + image)
        return 1
    else:
        log.info("Map, mount and IOs successful for " + pool + "/" + image)


def run(**kw):
    """
    This test verifies Live migration of image with external data source as RAW data format in single ceph cluster
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:

        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        kw.update({"cleanup_files": []})

        if rbd_obj:
            log.info("Executing test on Replicated and EC pool")
            if migration_with_raw_data_format(rbd_obj, client, **kw):
                return 1
            log.info(
                "Test live migration of image with external data source as RAW data format is successful"
            )

    except Exception as e:
        log.error(
            "Test live migration of image with external data source as RAW data format failed: "
            + str(e)
        )
        return 1

    finally:
        try:
            for file in kw["cleanup_files"]:
                out, err = client.exec_command(sudo=True, cmd="rm -f " + file)
                if err:
                    log.error("Failed to delete file " + file)
        except Exception as e:
            log.error("Failed to cleanup temp files with err " + e)
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return 0
