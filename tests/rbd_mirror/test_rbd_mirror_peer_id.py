"""Test case covered - CEPH-83590607

Test Case Flow:
1. Set up bidirectional mirroring on a test pool as usual
2. Verify that "rbd mirror pool status" reports "health: OK" on both clusters
3. Grab service_id and instance_id from "rbd mirror pool status --verbose" output on cluster B
4. Grab peer UUID from "rbd mirror pool info" output on cluster B
5. Set wrong client id to mirror peer client using
    "rbd mirror pool peer set <peer UUID from step 4> client client.invalid" command on cluster B
6. Wait 30-90 seconds and verify that "rbd mirror pool status" reports "health: ERROR" on cluster B
   and "health: WARNING" on cluster A
7. Reset correct client id to mirror peer client using
    "rbd mirror pool peer set <peer UUID from step 4> client client.rbd-mirror-peer" command on cluster B
8. Wait 30-90 seconds and verify that "rbd mirror pool status" reports "health: OK" on both clusters again
9. Grab service_id and instance_id from "rbd mirror pool status --verbose" output on cluster B again
10. Verify that service_id from step 3 is equal to the one from step 9
11. Verify that instance_id from step 3 is not same than the one from step 9
8. Perform test steps for both Replicated and EC pool
"""

import json
import time
from copy import deepcopy

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.rbd_mirror import compare_pool_mirror_status
from utility.log import Log

log = Log(__name__)


def test_peer_id(mirror1, mirror2, pool_type, **kw):
    """Method to validate Change and Reset peer ID
    and validate the mirror pool status reflects the same

    Args:
        mirror1: RBD Mirror primary cluster Object
        mirror2: RBD Mirror secondary cluster Object
        pool_type: Pool type (replicated or EC)
        **kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    try:
        log.info("Starting snapshot RBD mirroring test case")
        config = deepcopy(kw.get("config").get(pool_type))

        for pool_name, pool_config in getdict(config).items():
            multi_image_config = getdict(pool_config)

            for image, image_config in multi_image_config.items():
                # Verify that "rbd mirror pool status" reports "health: OK"on primary cluster
                if compare_pool_mirror_status(mirror1, pool_name, "OK"):
                    log.error("Primary site mirror pool health status is not OK")
                    return 1
                log.info("Primary site mirror pool health status is OK state")

                # Verify that "rbd mirror pool status" reports "health: OK"on secondary cluster
                if compare_pool_mirror_status(mirror2, pool_name, "OK"):
                    log.error("Secondary site mirror pool health status is not OK")
                    return 1
                log.info("Secondary site mirror pool health status is OK state")

                # Grab service_id and instance_id from secondary cluster
                pool_config = {"pool": pool_name, "verbose": True, "format": "json"}
                output, err = mirror2.mirror.pool.status(**pool_config)
                if err:
                    log.error(f"Error while fetching rbd mirror pool status as {err}")
                    return 1

                pool_status = json.loads(output)
                service_id = pool_status["daemons"][0]["service_id"]
                instance_id = pool_status["daemons"][0]["instance_id"]

                log.info(f"Service ID of secondary cluster: {service_id}")
                log.info(f"Instance ID of secondary cluster: {instance_id}")

                # Grab peer UUID from secondary
                output, err = mirror2.mirror.pool.info(pool=pool_name, format="json")
                output_json = json.loads(output)
                peer_uuid = output_json["peers"][0]["uuid"]
                client_name = output_json["peers"][0]["client_name"]
                log.info(f"Peer UUID of secondary cluster: {peer_uuid}")
                log.info(f"Peer client ID of secondary cluster: {client_name}")

                # Set invalid peer client name on secondary
                mirror2.mirror.pool.peer.set_(
                    pool=pool_name,
                    uuid=peer_uuid,
                    key="client",
                    value="client.invalid",
                )

                # Verify that "rbd mirror pool status" reports "health: ERROR" on secondary
                if compare_pool_mirror_status(mirror2, pool_name, "ERROR"):
                    log.error(
                        "Secondary site mirror pool health is not ERROR as expected"
                    )
                    return 1
                log.info("Secondary site mirror pool health is ERROR as expected")

                # Verify that "rbd mirror pool status" reports "health: WARNING" on primary
                if compare_pool_mirror_status(mirror1, pool_name, "WARNING"):
                    log.error(
                        "Primary site mirror pool health is not WARNING as expected"
                    )
                    return 1
                log.info("Primary site mirror pool health is WARNING as expected")

                # Reset valid peer client id on secondary i.e client.rbd-mirror-peer
                mirror2.mirror.pool.peer.set_(
                    pool=pool_name,
                    uuid=peer_uuid,
                    key="client",
                    value=client_name,
                )

                # wait for some seconds to reflect the pool status
                time.sleep(60)

                # Verify that "rbd mirror pool status" reports "health: OK" on secondary
                if compare_pool_mirror_status(mirror2, pool_name, "OK"):
                    log.error("Secondary site mirror pool health status is not OK")
                    return 1
                log.info("Secondary site mirror pool health status is OK state")

                # Verify that "rbd mirror pool status" reports "health: OK" on primary
                if compare_pool_mirror_status(mirror1, pool_name, "OK"):
                    log.error("Primary site mirror pool health status is not OK")
                    return 1
                log.info("Primary site mirror pool health status is OK state")

                # Grab service_id and instance_id from secondary cluster
                pool_config = {"pool": pool_name, "verbose": True, "format": "json"}
                output, err = mirror2.mirror.pool.status(**pool_config)
                pool_status = json.loads(output)
                service_id_new = pool_status["daemons"][0]["service_id"]
                instance_id_new = pool_status["daemons"][0]["instance_id"]

                log.info(f"New Service ID of secondary cluster: {service_id_new}")
                log.info(f"New Instance ID of secondary cluster: {instance_id_new}")

                # Verify service_id should be same as previous
                if int(service_id) == int(service_id_new):
                    log.info("Test passed: Service ID is equal")
                else:
                    log.error("Test failed: Service ID is not equal")
                    return 1

                # Verify instance_id should not be same as previous
                if int(instance_id) != int(instance_id_new):
                    log.info("Test passed: Instance ID is not same as previous")
                else:
                    log.error("Test failed: Instance ID is same as previous")
                    return 1

    except Exception as e:
        log.exception(e)
        return 1
    return 0


def run(**kw):
    """
    Test to verify changing and Reseting mirror pool peer ID.

    Args:
        **kw: test data
            config:
                rep_pool_config:
                    num_pools: 1
                    num_images: 1
                    mode: image
                    mirrormode: snapshot
                    imagesize: 2G
                    io_total: 100M
                ec_pool_config:
                    num_pools: 1
                    num_images: 1
                    mode: image
                    mirrormode: snapshot
                    imagesize: 2G
                    io_total: 100M
    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    pool_types = ["rep_pool_config", "ec_pool_config"]
    log.info(
        "Starting CEPH-83590607 Test to verify changing and Reseting mirror pool peer ID"
    )

    try:
        kw.get("config", {})["do_not_run_io"] = True
        mirror_obj = initial_mirror_config(**kw)
        mirror_obj.pop("output", [])

        for val in mirror_obj.values():
            if not val.get("is_secondary", False):
                rbd = val.get("rbd")
            else:
                sec_rbd = val.get("rbd")

        log.info("Initial configuration complete")

        pool_types = list(mirror_obj.values())[0].get("pool_types")
        for pool_type in pool_types:
            log.info(
                f"Running Test to verify changing and Reseting peer ID for {pool_type}"
            )
            rc = test_peer_id(rbd, sec_rbd, pool_type, **kw)
            if rc:
                log.error(
                    "Test failed to verify changing and Reseting peer ID for {pool_type}"
                )
                return 1
    except Exception as e:
        log.error(f"Test failed to verify changing and Reseting peer ID {str(e)}")
        return 1
    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return 0
