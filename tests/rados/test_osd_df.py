import time

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-10787
    Capture and inspect ceph osd df stats at different stages -
    - After pool creation
    - After writing data to a particular object in the pool
    - After marking the acting pg set OSDs as 'out'
    1. Create an application pool
    2. Capture ceph osd df tree stats
    3. Write data to an object of the pool
    4. Fetch the acting pg set osds for the object
    5. Fetch hosts by daemon_type=osd and osd ids
    6. Capture ceph osd df tree stats after IOPS
    7. Mark acting pg set OSDs as out
    8. Fetch new acting pg set osds for the object
    9. Fetch new hosts by daemon_type=osd and osd ids
    10. Capture ceph osd df tree stats
    11. Verify the deviation at different stages for node/host,
        OSDs and Summary total stats
    12. Mark the previously 'out' OSDs as 'in'
    13. Delete the created app pool
    """
    log.info(run.__doc__)
    config = kw["config"]
    test_pass = 0
    run_iterations = config["run_iteration"]
    min_pass = 1

    log.info(f"Test is configured to run {run_iterations} iterations")
    log.info(
        f"Due to unpredictability of ceph osd df stats,"
        f" and varying changes observed in stats of other OSDs and nodes, "
        f" it is difficult to verify the changes with a fixed degree of allowed deviation, "
        f" therefore, the test is expected to pass at least 70% of time."
        f" \n Minimum number of pass runs: {min_pass}"
    )
    for i in range(run_iterations):
        log.info(f"--------- Test Iteration: {i+1} -----------")
        test_result = run_test(ceph_cluster, **kw)
        if not test_result:
            test_pass += 1
            break

    summary_text = (
        f"Test could only pass {test_pass} out of {run_iterations} of times,"
        f" expected min pass: {min_pass}"
    )
    if test_pass < min_pass:
        log.error(summary_text)
        return 1
    log.info(summary_text)
    return 0


def run_test(ceph_cluster, **kw):
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    iterations = config.get("write_iteration")
    acting_osd_hosts = []
    acting_osd_host_osds = []
    ignored_hosts = []
    new_acting_osd_hosts = []
    new_acting_osd_host_osds = []
    verification_dict = {"WI": iterations, "host_osd_map": {}}
    pool_name = config["pool_name"]
    object_name = config.get("object_name", "obj-osd-df")

    log.info("Running test case to verify ceph osd df stats")

    try:
        # create pool with given config
        if config["create_pool"]:
            rados_obj.create_pool(pool_name=pool_name)

        # retrieve ceph osd df stats before I/O operation
        pre_osd_df_stats = rados_obj.get_osd_df_stats(tree=True)
        log.debug(pre_osd_df_stats)
        verification_dict.update(
            {"pre_osd_df_stats": update_stats_dict(pre_osd_df_stats)}
        )

        # create objects and perform IOPs
        pool_obj.do_rados_put(
            client=client_node, pool=pool_name, obj_name=object_name, nobj=1
        )

        pool_obj.do_rados_append(
            client=client_node,
            pool=pool_name,
            obj_name=object_name,
            nobj=(iterations - 1),
        )

        osd_map = rados_obj.get_osd_map(pool=pool_name, obj=object_name)
        acting_pg_set = osd_map["acting"]
        if not acting_pg_set:
            log.error("Failed to retrieve acting pg set")
            return 1
        log.info(f"Acting set for {object_name} in {pool_name}: {acting_pg_set}")
        verification_dict.update({"acting_pg_set": acting_pg_set})

        time.sleep(5)

        # retrieve ceph osd df stats after I/O operation
        post_osd_df_stats = rados_obj.get_osd_df_stats(tree=True)
        log.debug(post_osd_df_stats)
        verification_dict.update(
            {"post_osd_df_stats": update_stats_dict(post_osd_df_stats)}
        )

        # Retrieve acting osd hosts and all the osds on these hosts
        try:
            for osd_id in acting_pg_set:
                osd_node = rados_obj.fetch_host_node(
                    daemon_type="osd", daemon_id=osd_id
                )
                acting_osd_hosts.append(osd_node)
                verification_dict["host_osd_map"].update(
                    {osd_node.hostname: {"iops": osd_id}}
                )
                osd_list = rados_obj.collect_osd_daemon_ids(osd_node=osd_node)
                acting_osd_host_osds.extend(osd_list)
        except Exception:
            log.error("Failed to fetch host details")
            return 1
        verification_dict.update({"acting_osd_hosts": acting_osd_hosts})
        verification_dict.update({"acting_osd_host_osds": acting_osd_host_osds})

        # mark osds in acting set as out
        for osd_id in acting_pg_set:
            if not utils.set_osd_out(ceph_cluster, osd_id):
                log.error(f"Failed to mark OSD.{osd_id} out")
                return 1

        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=1800)

        # Retrieve new acting pg set
        osd_map = rados_obj.get_osd_map(pool=pool_name, obj=object_name)
        new_acting_pg_set = osd_map["acting"]
        if not new_acting_pg_set:
            log.error("Failed to retrieve new acting pg set")
            return 1
        log.info(f"New acting set for {pool_name}: {new_acting_pg_set}")
        verification_dict.update({"new_acting_pg_set": new_acting_pg_set})

        # Retrieve new acting osd hosts and all the osds on these hosts
        try:
            for osd_id in new_acting_pg_set:
                osd_node = rados_obj.fetch_host_node(
                    daemon_type="osd", daemon_id=osd_id
                )
                new_acting_osd_hosts.append(osd_node)
                if osd_node not in acting_osd_hosts:
                    verification_dict["host_osd_map"].update(
                        {osd_node.hostname: {"out": osd_id}}
                    )
                verification_dict["host_osd_map"][osd_node.hostname].update(
                    {"out": osd_id}
                )
                osd_list = rados_obj.collect_osd_daemon_ids(osd_node=osd_node)
                new_acting_osd_host_osds.extend(osd_list)
        except Exception:
            log.error("Failed to fetch host details")
            return 1
        verification_dict.update({"new_acting_osd_hosts": new_acting_osd_hosts})
        verification_dict.update({"new_acting_osd_host_osds": new_acting_osd_host_osds})

        # Retrieve osd hosts which are not part of new acting osd hosts list
        for osd_id in acting_pg_set:
            if osd_id not in new_acting_osd_host_osds:
                ignored_hosts.append(
                    rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
                )
        verification_dict.update({"ignored_hosts": ignored_hosts})

        # retrieve ceph osd df stats after OSDs are out
        out_osd_df_stats = rados_obj.get_osd_df_stats(tree=True)
        log.debug(out_osd_df_stats)
        verification_dict.update(
            {"out_osd_df_stats": update_stats_dict(out_osd_df_stats)}
        )

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        # Mark the 'out' OSD as 'in'
        try:
            assert utils.set_osd_in(ceph_cluster, osd_ids=acting_pg_set)
        except AssertionError:
            log.error(f"Failed to mark OSDs {acting_pg_set} in")
            return 1

        # Delete the created osd pool
        if config.get("delete_pool"):
            rados_obj.delete_pool(pool=pool_name)

    log.info("--------------Verification dictionary --------------------")
    log.info(verification_dict)

    try:
        for host in acting_osd_hosts:
            assert verify_deviation(
                verification_dict,
                type="node",
                stage="iops",
                host_id=host.hostname,
            )

        for host in new_acting_osd_hosts:
            status = "old" if host in acting_osd_hosts else "new"
            assert verify_deviation(
                verification_dict,
                type="node",
                stage="out",
                host_id=host.hostname,
                status=status,
            )

        for host in ignored_hosts:
            assert verify_deviation(
                verification_dict,
                type="node",
                stage="out",
                host_id=host.hostname,
                status="ignored",
            )

        for o_id in acting_pg_set:
            for stage in ["iops", "out"]:
                assert verify_deviation(
                    verification_dict,
                    type="osd",
                    stage=stage,
                    osd_id=o_id,
                    status="old",
                )

        for o_id in new_acting_pg_set:
            assert verify_deviation(
                verification_dict, type="osd", stage="iops", osd_id=o_id, status="new"
            )

        for stage in ["iops", "out"]:
            assert verify_deviation(verification_dict, type="summary", stage=stage)
    except AssertionError as AE:
        log.error(f"Verification failed with exception: {AE.__doc__}")
        log.exception(AE)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # removal of rados pools
        rados_obj.rados_pool_cleanup()

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def update_stats_dict(stats_dict: dict) -> dict:
    """
    Modifies the osd df tree stats dictionary to include
    additional keys for better traversal
    Args:
        stats_dict: ceph osd df tree json output
    Returns: modified dictionary with additional keys
    """
    dict_copy = {}

    for node in stats_dict["nodes"]:
        if node["type"] == "osd":
            dict_copy.update({node["id"]: node})
        elif node["type"] == "host":
            dict_copy.update({node["name"]: node})

    dict_copy.update({"stray": stats_dict["stray"]})
    dict_copy.update({"summary": stats_dict["summary"]})

    return dict_copy


def verify_deviation(
    config: dict,
    type: str,
    stage: str,
    host_id: str = None,
    osd_id: int = None,
    status: str = None,
) -> bool:
    """
    Verifies the deviation in stats
    Args:
        config: dictionary having ceph osd df stats and other parameters
        from different stages
        type: node or osd or summary
        stage: Stage after which the deviation is to be calculated | iops or out
        host_id: hostname of the node whose deviation is to be calculated
        osd_id: osd_id of the osd whose deviation is to be calculated
        status: status of host/node or osd | new or old or ignored
    Returns: True -> pass | False - fail
    """
    acting_total_size = acting_total_raw_use = acting_total_data = (
        acting_total_avail
    ) = 0
    stored_data_kb = config["WI"] * 4 * 1024
    pre_osd_df_stats = config["pre_osd_df_stats"]
    post_osd_df_stats = config["post_osd_df_stats"]
    out_osd_df_stats = config["out_osd_df_stats"]
    acting_pg_set = config["acting_pg_set"]
    host_osd_map = config["host_osd_map"]
    deviation_multiplier = {
        "node": {"iops": 1.07, "out": 1.11},
        "osd": {"iops": 1.07, "out": 0},
        "summary": {"iops": 1.08, "out": 1.08},
    }

    if osd_id is None and type == "node" and stage == "out" and status != "new":
        osd_id = host_osd_map[host_id]["iops"]
    dev_factor = (
        1.1
        if type == "node" and status == "ignored"
        else deviation_multiplier[type][stage]
    )

    for x in acting_pg_set:
        acting_total_size += pre_osd_df_stats[x]["kb"]
        acting_total_raw_use += pre_osd_df_stats[x]["kb_used"]
        acting_total_data += pre_osd_df_stats[x]["kb_used_data"]
        acting_total_avail += pre_osd_df_stats[x]["kb_avail"]

    log.info(100 * "=")
    log.info(
        f"type: {type} | stage: {stage} | host: {host_id} | osd_id: {osd_id} | status: {status}"
    )
    try:
        if type == "node":
            if stage == "iops":
                log.info(f"Stats verification for node {host_id} post I/Os")
                log.info(
                    f"SIZE: {pre_osd_df_stats[host_id]['kb']} == {post_osd_df_stats[host_id]['kb']}"
                )
                assert (
                    pre_osd_df_stats[host_id]["kb"] == post_osd_df_stats[host_id]["kb"]
                )
                log.info(
                    f"RAW USE: {(pre_osd_df_stats[host_id]['kb_used'] + stored_data_kb) * dev_factor} "
                    f">= {post_osd_df_stats[host_id]['kb_used']}"
                )
                assert (
                    int(
                        (pre_osd_df_stats[host_id]["kb_used"] + stored_data_kb)
                        * dev_factor
                    )
                    >= post_osd_df_stats[host_id]["kb_used"]
                )
                log.info(
                    f"DATA: {(pre_osd_df_stats[host_id]['kb_used_data'] + stored_data_kb) * dev_factor}"
                    f" >= {post_osd_df_stats[host_id]['kb_used_data']}"
                )
                assert (
                    int(
                        (pre_osd_df_stats[host_id]["kb_used_data"] + stored_data_kb)
                        * dev_factor
                    )
                    >= post_osd_df_stats[host_id]["kb_used_data"]
                )
                log.info(
                    f"AVAIL: {pre_osd_df_stats[host_id]['kb_avail'] / 1048576}"
                    f" ~= {post_osd_df_stats[host_id]['kb_avail'] / 1048576}"
                )
                assert int(pre_osd_df_stats[host_id]["kb_avail"] / 1048576) == int(
                    post_osd_df_stats[host_id]["kb_avail"] / 1048576
                )
            elif stage == "out" and status == "old":
                log.info(f"Stats verification for node {host_id} after OSDs are out")
                log.info(
                    f"SIZE: {(post_osd_df_stats[host_id]['kb'] - pre_osd_df_stats[osd_id]['kb']) * dev_factor}"
                    f" >= {out_osd_df_stats[host_id]['kb']}"
                )
                assert (
                    int(
                        (
                            post_osd_df_stats[host_id]["kb"]
                            - pre_osd_df_stats[osd_id]["kb"]
                        )
                        * dev_factor
                    )
                    >= out_osd_df_stats[host_id]["kb"]
                )
                log.info(
                    f"RAW USE:"
                    f" {(post_osd_df_stats[host_id]['kb_used'] - pre_osd_df_stats[osd_id]['kb_used']) * dev_factor}"
                    f" >= {out_osd_df_stats[host_id]['kb_used']}"
                )
                assert (
                    int(
                        (
                            post_osd_df_stats[host_id]["kb_used"]
                            - pre_osd_df_stats[osd_id]["kb_used"]
                        )
                        * dev_factor
                    )
                    >= out_osd_df_stats[host_id]["kb_used"]
                )
                value = (
                    post_osd_df_stats[host_id]["kb_used_data"]
                    - pre_osd_df_stats[osd_id]["kb_used_data"]
                ) * dev_factor
                log.info(
                    f"DATA: {value}" f" >= {out_osd_df_stats[host_id]['kb_used_data']}"
                )
                assert (
                    int(
                        (
                            post_osd_df_stats[host_id]["kb_used_data"]
                            - pre_osd_df_stats[osd_id]["kb_used_data"]
                        )
                        * dev_factor
                    )
                    >= out_osd_df_stats[host_id]["kb_used_data"]
                )
                log.info(
                    f"AVAIL:"
                    f" {(post_osd_df_stats[host_id]['kb_avail'] - pre_osd_df_stats[osd_id]['kb_avail']) * dev_factor}"
                    f" >= {out_osd_df_stats[host_id]['kb_avail']}"
                )
                assert (
                    int(
                        (
                            post_osd_df_stats[host_id]["kb_avail"]
                            - pre_osd_df_stats[osd_id]["kb_avail"]
                        )
                        * dev_factor
                    )
                    >= out_osd_df_stats[host_id]["kb_avail"]
                )
            elif stage == "out" and status == "new":
                log.info(f"Stats verification for node {host_id} post OSDs are out")
                log.info(
                    f"SIZE: {post_osd_df_stats[host_id]['kb']} == {out_osd_df_stats[host_id]['kb']}"
                )
                assert (
                    post_osd_df_stats[host_id]["kb"] == out_osd_df_stats[host_id]["kb"]
                )
                log.info(
                    f"RAW USE: {(post_osd_df_stats[host_id]['kb_used'] + stored_data_kb) * dev_factor}"
                    f" >= {out_osd_df_stats[host_id]['kb_used']}"
                )
                assert (
                    int(
                        (post_osd_df_stats[host_id]["kb_used"] + stored_data_kb)
                        * dev_factor
                    )
                    >= out_osd_df_stats[host_id]["kb_used"]
                )
                log.info(
                    f"DATA: {(post_osd_df_stats[host_id]['kb_used_data'] + stored_data_kb) * dev_factor}"
                    f" >= {out_osd_df_stats[host_id]['kb_used_data']}"
                )
                assert (
                    int(
                        (post_osd_df_stats[host_id]["kb_used_data"] + stored_data_kb)
                        * dev_factor
                    )
                    >= out_osd_df_stats[host_id]["kb_used_data"]
                )
                log.info(
                    f"AVAIL: {post_osd_df_stats[host_id]['kb_avail'] / 1048576}"
                    f" ~= {out_osd_df_stats[host_id]['kb_avail'] / 1048576}"
                )
                assert int(post_osd_df_stats[host_id]["kb_avail"] / 1048576) == int(
                    out_osd_df_stats[host_id]["kb_avail"] / 1048576
                )
            elif stage == "out" and status == "ignored":
                log.info(f"Stats verification for node {host_id} post OSDs are out")
                log.info(
                    f"SIZE: {(post_osd_df_stats[host_id]['kb'] - post_osd_df_stats[osd_id]['kb']) * dev_factor}"
                    f" >= {out_osd_df_stats[host_id]['kb']}"
                )
                assert (
                    int(
                        (
                            post_osd_df_stats[host_id]["kb"]
                            - post_osd_df_stats[osd_id]["kb"]
                        )
                        * dev_factor
                    )
                    >= out_osd_df_stats[host_id]["kb"]
                )
                log.info(
                    f"RAW USE: "
                    f"{(post_osd_df_stats[host_id]['kb_used'] - post_osd_df_stats[osd_id]['kb_used'])* dev_factor}"
                    f" >= {out_osd_df_stats[host_id]['kb_used']}"
                )
                assert (
                    int(
                        (
                            post_osd_df_stats[host_id]["kb_used"]
                            - post_osd_df_stats[osd_id]["kb_used"]
                        )
                        * dev_factor
                    )
                    >= out_osd_df_stats[host_id]["kb_used"]
                )
                value = (
                    post_osd_df_stats[host_id]["kb_used_data"]
                    - post_osd_df_stats[osd_id]["kb_used_data"]
                ) * dev_factor
                log.info(
                    f"DATA: {value}" f" >= {out_osd_df_stats[host_id]['kb_used_data']}"
                )
                assert (
                    int(
                        (
                            post_osd_df_stats[host_id]["kb_used_data"]
                            - post_osd_df_stats[osd_id]["kb_used_data"]
                        )
                        * dev_factor
                    )
                    >= out_osd_df_stats[host_id]["kb_used_data"]
                )
                log.info(
                    f"AVAIL: {(post_osd_df_stats[host_id]['kb_avail'] - post_osd_df_stats[osd_id]['kb']) * dev_factor}"
                    f" >= {out_osd_df_stats[host_id]['kb_avail']}"
                )
                assert (
                    int(
                        (
                            post_osd_df_stats[host_id]["kb_avail"]
                            - post_osd_df_stats[osd_id]["kb"]
                        )
                        * dev_factor
                    )
                    >= out_osd_df_stats[host_id]["kb_avail"]
                )
            log.info(f"Stats verification completed for {type} {host_id}: PASS")
        elif type == "osd":
            if stage == "iops" and status == "old":
                log.info(f"Stats verification for OSD {osd_id} post I/Os")
                log.info(
                    f"SIZE: {pre_osd_df_stats[osd_id]['kb']} == {post_osd_df_stats[osd_id]['kb']}"
                )
                assert pre_osd_df_stats[osd_id]["kb"] == post_osd_df_stats[osd_id]["kb"]
                log.info(
                    f"RAW USE: {(pre_osd_df_stats[osd_id]['kb_used'] + stored_data_kb) * dev_factor}"
                    f" >= {post_osd_df_stats[osd_id]['kb_used']}"
                )
                assert (
                    int(
                        (pre_osd_df_stats[osd_id]["kb_used"] + stored_data_kb)
                        * dev_factor
                    )
                    >= post_osd_df_stats[osd_id]["kb_used"]
                )
                log.info(
                    f"DATA: {(pre_osd_df_stats[osd_id]['kb_used_data'] + stored_data_kb) * dev_factor}"
                    f" >= {post_osd_df_stats[osd_id]['kb_used_data']}"
                )
                assert (
                    int(
                        (pre_osd_df_stats[osd_id]["kb_used_data"] + stored_data_kb)
                        * dev_factor
                    )
                    >= post_osd_df_stats[osd_id]["kb_used_data"]
                )
                log.info(
                    f"AVAIL: {pre_osd_df_stats[osd_id]['kb_avail'] / 1048576}"
                    f" ~= {post_osd_df_stats[osd_id]['kb_avail'] / 1048576}"
                )
                assert int(pre_osd_df_stats[osd_id]["kb_avail"] / 1048576) == int(
                    post_osd_df_stats[osd_id]["kb_avail"] / 1048576
                )
            if stage == "iops" and status == "new":
                log.info(f"Stats verification for OSD {osd_id} post I/Os")
                log.info(
                    f"SIZE: {post_osd_df_stats[osd_id]['kb']} == {out_osd_df_stats[osd_id]['kb']}"
                )
                assert post_osd_df_stats[osd_id]["kb"] == out_osd_df_stats[osd_id]["kb"]
                log.info(
                    f"RAW USE: {(post_osd_df_stats[osd_id]['kb_used'] + stored_data_kb) * dev_factor}"
                    f" >= {out_osd_df_stats[osd_id]['kb_used']}"
                )
                assert (
                    int(
                        (post_osd_df_stats[osd_id]["kb_used"] + stored_data_kb)
                        * dev_factor
                    )
                    >= out_osd_df_stats[osd_id]["kb_used"]
                )
                log.info(
                    f"DATA: {(post_osd_df_stats[osd_id]['kb_used_data'] + stored_data_kb) * dev_factor}"
                    f" >= {out_osd_df_stats[osd_id]['kb_used_data']}"
                )
                assert (
                    int(
                        (post_osd_df_stats[osd_id]["kb_used_data"] + stored_data_kb)
                        * dev_factor
                    )
                    >= out_osd_df_stats[osd_id]["kb_used_data"]
                )
                log.info(
                    f"AVAIL: {post_osd_df_stats[osd_id]['kb_avail'] / 1048576}"
                    f" ~= {out_osd_df_stats[osd_id]['kb_avail'] / 1048576}"
                )
                assert int(post_osd_df_stats[osd_id]["kb_avail"] / 1048576) == int(
                    out_osd_df_stats[osd_id]["kb_avail"] / 1048576
                )
            if stage == "out":
                log.info(f"Stats verification for OSD {osd_id} post OSDs are OUT")
                log.info(
                    f"Reweight: {out_osd_df_stats[osd_id]['reweight']} | "
                    f"SIZE: {out_osd_df_stats[osd_id]['kb']} | "
                    f"RAW USE: {out_osd_df_stats[osd_id]['kb_used']} | "
                    f"DATA: {out_osd_df_stats[osd_id]['kb_used_data']} | "
                    f"AVAIL: {out_osd_df_stats[osd_id]['kb_avail']}"
                )
                assert (
                    out_osd_df_stats[osd_id]["reweight"]
                    == out_osd_df_stats[osd_id]["kb"]
                    == out_osd_df_stats[osd_id]["kb_used"]
                    == out_osd_df_stats[osd_id]["kb_used_data"]
                    == out_osd_df_stats[osd_id]["kb_avail"]
                    == 0
                )
                log.info(f"OUT OSD {osd_id} stats: PASS")
            log.info(f"Stats verification completed for {type} {osd_id}: PASS")
        elif type == "summary":
            if stage == "iops":
                log.info("Summary Stats verification post I/Os")
                log.info(
                    f"TOTAL SIZE: {pre_osd_df_stats['summary']['total_kb']}"
                    f" == {post_osd_df_stats['summary']['total_kb']}"
                )
                assert (
                    pre_osd_df_stats["summary"]["total_kb"]
                    == post_osd_df_stats["summary"]["total_kb"]
                )
                log.info(
                    f"TOTAL RAW USE: {(pre_osd_df_stats['summary']['total_kb_used'] + stored_data_kb * 3) * dev_factor}"
                    f" >= {post_osd_df_stats['summary']['total_kb_used']}"
                )
                assert (
                    pre_osd_df_stats["summary"]["total_kb_used"] + stored_data_kb * 3
                ) * dev_factor >= post_osd_df_stats["summary"]["total_kb_used"]
                log.info(
                    f"TOTAL DATA: "
                    f"{(pre_osd_df_stats['summary']['total_kb_used_data'] + stored_data_kb * 3) * dev_factor}"
                    f" >= {post_osd_df_stats['summary']['total_kb_used_data']}"
                )
                assert (
                    pre_osd_df_stats["summary"]["total_kb_used_data"]
                    + stored_data_kb * 3
                ) * dev_factor >= post_osd_df_stats["summary"]["total_kb_used_data"]
                log.info(
                    f"TOTAL AVAIL: {pre_osd_df_stats['summary']['total_kb_avail']}"
                    f" ~= {post_osd_df_stats['summary']['total_kb_avail']}"
                )
                assert int(
                    pre_osd_df_stats["summary"]["total_kb_avail"] / 1048576
                ) == int(post_osd_df_stats["summary"]["total_kb_avail"] / 1048576)
                log.info("Summary Stats verification post I/Os: PASSED")
            elif stage == "out":
                log.info("Summary Stats verification after OSDs are OUT")
                log.info(
                    f"TOTAL SIZE: {post_osd_df_stats['summary']['total_kb'] - acting_total_size}"
                    f" == {out_osd_df_stats['summary']['total_kb']}"
                )
                assert (
                    post_osd_df_stats["summary"]["total_kb"] - acting_total_size
                    == out_osd_df_stats["summary"]["total_kb"]
                )
                log.info(
                    f"TOTAL RAW USE: "
                    f"{(post_osd_df_stats['summary']['total_kb_used'] - acting_total_raw_use) * dev_factor}"
                    f" >= {out_osd_df_stats['summary']['total_kb_used']}"
                )
                assert (
                    post_osd_df_stats["summary"]["total_kb_used"] - acting_total_raw_use
                ) * dev_factor >= out_osd_df_stats["summary"]["total_kb_used"]
                log.info(
                    f"TOTAL DATA:"
                    f" {(post_osd_df_stats['summary']['total_kb_used_data'] - acting_total_data) * dev_factor}"
                    f" >= {out_osd_df_stats['summary']['total_kb_used_data']}"
                )
                assert (
                    post_osd_df_stats["summary"]["total_kb_used_data"]
                    - acting_total_data
                ) * dev_factor >= out_osd_df_stats["summary"]["total_kb_used_data"]
                log.info(
                    f"TOTAL AVAIL: {post_osd_df_stats['summary']['total_kb_avail'] - acting_total_avail}"
                    f" ~= {out_osd_df_stats['summary']['total_kb_avail']}"
                )
                assert int(
                    (
                        post_osd_df_stats["summary"]["total_kb_avail"]
                        - acting_total_avail
                    )
                    / 1048576
                ) == int(out_osd_df_stats["summary"]["total_kb_avail"] / 1048576)
                log.info("Summary Stats verification after OSD are OUT: PASSED")
    except Exception as E:
        log.info("^FAILED")
        log.error(f"Verification failed with exception: {E.__doc__}")
        log.exception(E)
        return False

    return True
