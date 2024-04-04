"""
Module to deploy 8+6 EC pool with custom CRUSH rules for testing

"""
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.crushtool_workflows import CrushToolWorkflows
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Module to deploy 8+6 EC pool with custom CRUSH rules for testing
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    crush_obj = CrushToolWorkflows(node=cephadm)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)

    crush_rule = config.get("crush_rule", "rule-86")
    crush_rules = {
        "rule-86": """id 86
type erasure
step set_chooseleaf_tries 5
step set_choose_tries 100
step take default
step choose indep 4 type host
step choose indep 4 type osd
step emit""",
        "rule-43": """id 43
type erasure
step set_chooseleaf_tries 5
step set_choose_tries 100
step take default
step choose indep 4 type host
step choose indep 2 type osd
step emit""",
        "rule-84": """id 84
type erasure
step set_chooseleaf_tries 5
step set_choose_tries 100
step take default
step choose indep 4 type host
step choose indep 3 type osd
step emit""",
    }
    try:
        # setting the crush rule on the cluster
        if config.get("create_rule"):
            log.debug("Creating new crush rules on the cluster")
            if not crush_obj.add_crush_rule(
                rule_name=crush_rule, rules=crush_rules[crush_rule]
            ):
                log.error(
                    f"Failed to add the crush rule : {crush_rule} into the cluster"
                )
                raise Exception(
                    f"Failed to add the crush rule : {crush_rule} into the cluster"
                )

        if config.get("create_pool"):
            log.debug("Creating new EC pool on the cluster")
            if not rados_obj.create_erasure_pool(name=crush_rule, **config):
                log.error("Failed to create the EC Pool")
                raise Exception("Failed to create the EC Pool")

        if config.get("change_subtree_limit"):
            bucket = config["change_subtree_limit"]
            log.info(f"Changing subtree limit to {bucket} and restarting all OSDs.")
            if not mon_obj.set_config(
                section="mon", name="mon_osd_down_out_subtree_limit", value=bucket
            ):
                log.error(f"Failed to set mon_osd_down_out_subtree_limit to {bucket} ")
                return 1

            time.sleep(5)
            out, _ = cephadm.shell(args=["ceph osd ls"])
            osd_list = out.strip().split("\n")
            for osd in osd_list:
                if not rados_obj.change_osd_state(action="restart", target=osd):
                    log.error(f" Failed to restart OSD : {osd}")
                    raise Exception(f" Failed to restart OSD : {osd}")

            log.info("Completed setting the subtree limit and restart of the OSDs")
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # removal of rados pool
        if config.get("delete_pool"):
            if not rados_obj.delete_pool(pool=config["pool_name"]):
                log.error("Failed to delete EC Pool")
                return 1

        mon_obj.remove_config(section="mon", name="mon_osd_down_out_subtree_limit")
        # log cluster health
        rados_obj.log_cluster_health()

    log.info(f"Created the EC pool : {config['pool_name']} with rule : {crush_rule}")
    return 0
