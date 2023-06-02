"""
test Module to :
1. Generate Crush.map.bin.
2. Add new Bucket entries.
3. Re-Weight Bucket items.
4. Move Bucket items.
5. Print the output of bin tests
"""
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.crushtool_workflows import CrushToolWorkflows
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to perform +ve and -ve workflows for the crushtool utility
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    crush_obj = CrushToolWorkflows(node=cephadm)
    rados_obj = RadosOrchestrator(node=cephadm)
    location = config.get("file_location", "/tmp/crushmaps")

    # Checking if location provided exists and proceeding to create folder
    if not rados_obj.check_file_exists_on_client(loc=location):
        log.debug(f"Creating folder {location} on the client")
        crush_obj.client.exec_command(cmd=f"mkdir -p {location}", sudo=True)

    # Generating a new bin file to location provided
    res, new_location = crush_obj.generate_crush_map_bin(loc=location)
    if not res:
        log.error(f"Failed to generate crush map at : {location}")
        raise Exception("Set-up Failed")

    # Adding new buckets into the bin
    buckets = config["add_buckets"]
    for bucket in buckets:
        res, new_location = crush_obj.add_new_bucket_into_bin(
            source_loc=new_location,
            target_loc=new_location,
            bucket_name=bucket,
            bucket_type=buckets[bucket],
        )
        if not res:
            log.error(f"Failed to add item : {bucket} of type : {buckets[bucket]}")
            raise Exception("Add Buckets Failed")
        log.debug(f"Added item : {bucket} of type : {buckets[bucket]}")
    log.debug("Completed addition of all the buckets, starting verification")

    # Verifying if new buckets have been added
    for bucket in buckets:
        res = crush_obj.verify_add_new_bucket_into_bin(
            loc=new_location,
            bucket_name=bucket,
            bucket_type=buckets[bucket],
        )
        if not res:
            log.error(f"Failed to add item : {bucket} of type : {buckets[bucket]}")
            raise Exception("Add Buckets Failed")
        log.debug(f"Verified Addition of item : {bucket} of type : {buckets[bucket]}")

    log.info("Successfully Verified the add scenarios on the Crush bin file")

    # -ve Workflow. Trying to set weight to -ve values. Should not be possible
    res, bin_loc = crush_obj.reweight_buckets_in_bin(
        source_loc=new_location,
        target_loc=location,
        bucket_name="osd.1",
        reweight_val="-1",
    )
    if res:
        log.error("It should not be possible to set weight to -ve values")
        raise Exception("Execution Error")

    log.debug("Completed -ve workflow. trying to set +ve allowed values")
    # +ve workflow. Trying to set weight to +ve values. Should be possible
    res, new_location = crush_obj.reweight_buckets_in_bin(
        source_loc=new_location,
        target_loc=new_location,
        bucket_name="osd.1",
        reweight_val="0.020",
    )
    if not (
        res
        and crush_obj.verify_reweight_buckets_in_bin(
            loc=new_location, bucket_name="osd.1", reweight_val="0.020"
        )
    ):
        log.error("Unable to set weight on OSDs")
        raise Exception("Execution Error")

    log.info("Successfully Verified the re-weight scenarios on the Crush bin file")

    # Moving a host to en existing bucket
    # +ve workflow. Moving correct host to correct Bucket
    res, new_location = crush_obj.move_existing_bucket_in_bin(
        source_loc=new_location,
        target_loc=new_location,
        source_bucket="test-host-1",
        target_bucket_name="DC1",
        target_bucket_type="datacenter",
    )

    if not (
        res
        and crush_obj.verify_move_bucket_in_bin(
            loc=new_location,
            source_bucket="test-host-1",
            target_bucket="DC1",
        )
    ):
        log.error("Failed to move hosts in the crush map")
        raise Exception("Execution Error")

    # -ve Workflow. Moving host to incorrect bucket
    res, new_location = crush_obj.move_existing_bucket_in_bin(
        source_loc=new_location,
        target_loc=new_location,
        source_bucket="test-host-1",
        target_bucket_name="DC2",
        target_bucket_type="zone",
    )
    if res:
        log.error(
            "Should not be possible to move hosts with wrong target bucket details"
        )
        raise Exception("Execution Error")

    res, new_location = crush_obj.move_existing_bucket_in_bin(
        source_loc=new_location,
        target_loc=new_location,
        source_bucket="test-host-1",
        target_bucket_name="DC2",
        target_bucket_type="test",
    )
    if res:
        log.error(
            "Should not be possible to move hosts with wrong target bucket details"
        )
        raise Exception("Execution Error")

    # Testing the modified crush map

    for test in config["bin_tests"]:
        res, out = crush_obj.test_crush_map_bin(loc=new_location, test=test)
        if not res:
            log.error(f"Failed to run test for : {test}")
            raise Exception("Execution Error")
        log.info(f"Output for {test} on bin file is : \n {out}")

    log.info("Completed all the CrushTool workflows. Pass")
    return 0
