"""Module to execute Live migration as s3 image migration.

Pre-requisites :
- Cluster configured with atleast one client node with ceph-common package,
conf and keyring files
- Supportive rgw module to host image as s3 object in ceph-qe-scripts

Test cases covered :
CEPH-83574092 - verify snapshot downloaded creates a local RBD image

Test Case Flow :
1. Create images and take snapshots, export snapshots.
2. Backup exported snapshots as rbd images.
3. Mention s3 object as source for live migration preparation.
4. Execute and commit migration and verify.
"""
import json

from ceph.parallel import parallel
from tests.rbd.rbd_utils import Rbd, initial_rbd_config, verify_migration_commit
from utility.log import Log

log = Log(__name__)


def cleanup(rbd: Rbd, pool_config):
    for each_config in ["ec_pool_config", "rep_pool_config"]:
        rbd.clean_up(dir_name=f"image_export_{each_config[0:2]}.raw")
        rbd.clean_up(pools=[f"{pool_config[each_config]['pool']}"])

    rbd.clean_up(dir_name="ceph-qe-scripts/")


def run(**kw):
    log.info(
        "Executing CEPH-83574092 - \
        verify snapshot downloaded creates a local RBD image"
    )

    initial_rbd_config(**kw)
    rbd_obj = Rbd(**kw)

    imagespecs = []

    for each_config in ["ec_pool_config", "rep_pool_config"]:
        pool = f"{kw['config'][each_config]['pool']}"
        image = f"{kw['config'][each_config]['image']}"

        imagespecs.append(f"{pool}/{image}")
        rbd_obj.snap_create(pool, image, "snap")
        rbd_obj.export_image(
            f"{pool}/{image}@snap", f"image_export_{each_config[0:2]}.raw"
        )

    log.info("Initiating ceph-qe-script module to upload image exports")
    rgw_ceph_object = kw["ceph_cluster"].get_ceph_object("rgw")
    rgw_node = rgw_ceph_object.node
    append_param = " --rgw-node " + str(rgw_node.ip_address)

    repo_url = "https://github.com/red-hat-storage/ceph-qe-scripts.git"
    rbd_obj.exec_cmd(
        cmd="yum install python3 -y --nogpgcheck", check_ec=False, sudo=True
    )
    rbd_obj.exec_cmd(cmd="pip3 install --upgrade pip")
    rbd_obj.exec_cmd(cmd="pip3 install boto3")

    rbd_obj.exec_cmd(cmd=f"git clone {repo_url}")

    try:
        with parallel() as p:
            for each_config in ["ec_pool_config", "rep_pool_config"]:
                temp_append_param = (
                    append_param + f" --file-name image_export_{each_config[0:2]}.raw"
                )
                command = f"python3 ceph-qe-scripts/rbd/functional/test_rbd_s3_upload.py {temp_append_param}"
                p.spawn(rbd_obj.exec_cmd, cmd=command, long_running=True)

    except Exception:
        log.error("S3 upload of rbd image failed, can't proceed further")
        cleanup(rbd_obj, kw["config"])
        return 1

    try:
        for each_config in ["ec_pool_config", "rep_pool_config"]:
            migration_source_spec = {
                "type": "raw",
                "stream": {
                    "type": "s3",
                    "url": f"http://{str(rgw_node.ip_address)}:80/rbd/{each_config[0:2]}",
                    "access_key": "test_rbd",
                    "secret_key": "test_rbd",
                },
            }

            source_json = json.dumps(migration_source_spec)
            destination_spec = (
                f"{kw['config'][each_config]['pool']}/{each_config[0:2]}_import"
            )

            if rbd_obj.migration_prepare(f"'{source_json}'", destination_spec):
                log.error("Migration prepare failed while executing the testcase")
                return 1
            if rbd_obj.migration_action("execute", destination_spec):
                log.error("Migration preparation failed while executing testcase")
                return 1
            if rbd_obj.migration_action("commit", destination_spec):
                log.error("Migration commit failed while executing testcase")
                return 1
            verify_migration_commit(
                rbd_obj,
                f"{kw['config'][each_config]['pool']}",
                f"{kw['config'][each_config]['image']}",
            )
            log.info("Rbd live migration from s3 source successful")

        return 0

    except Exception:
        log.error("Image migration Failed, testcase failed")
        return 1
    finally:
        cleanup(rbd_obj, kw["config"])
