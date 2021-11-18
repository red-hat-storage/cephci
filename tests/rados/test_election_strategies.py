import logging
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonElectionStrategies

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Changes b/w various election strategies and observes mon quorum behaviour
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonElectionStrategies(rados_obj=rados_obj)
    cephadm_node_mon = ceph_cluster.get_nodes(role="installer")[0]

    # Collecting the number of mons in the quorum before the test
    mon_init_count = len(mon_obj.get_mon_quorum().keys())

    # By default, the election strategy is classic. Verifying that
    strategy = mon_obj.get_election_strategy()
    if strategy != 1:
        log.error(
            f"cluster created election strategy other than classic, i.e {strategy}"
        )
        return 1

    # Changing strategy to 2. i.e disallowed mode.
    if not mon_obj.set_election_strategy(mode="disallow"):
        log.error("could not set election strategy to disallow mode")
        return 1

    # sleeping for 2 seconds for new elections to be triggered and new leader to be elected
    time.sleep(2)

    log.info("Set election strategy to disallow mode. adding disallowed mons")
    # Checking if new leader will be chosen if leader is added to disallowed list
    old_leader = mon_obj.get_mon_quorum_leader()
    if not mon_obj.set_disallow_mon(mon=old_leader):
        log.error(f"could not add mon: {old_leader} to the disallowed list")
        return 1

    # sleeping for 2 seconds for new elections to be triggered and new leader to be elected
    time.sleep(2)

    current_leader = mon_obj.get_mon_quorum_leader()
    if re.search(current_leader, old_leader):
        log.error(f"The mon: {old_leader} added to disallow list is still leader")
        return 1

    # removing the mon from the disallowed list
    if not mon_obj.remove_disallow_mon(mon=old_leader):
        log.error(f"could not remove mon: {old_leader} from disallowed list")
        return 1

    # sleeping for 2 seconds for new elections to be triggered and new leader to be elected
    time.sleep(2)

    # Changing strategy to 3. i.e Connectivity mode.
    if not mon_obj.set_election_strategy(mode="connectivity"):
        log.error("could not set election strategy to connectivity mode")
        return 1

    # Checking connectivity scores of all the mons
    cmd = f"ceph daemon mon.{cephadm_node_mon.hostname} connection scores dump"
    rados_obj.run_ceph_command(cmd=cmd)

    # Changing strategy to default
    if not mon_obj.set_election_strategy(mode="classic"):
        log.error("could not set election strategy to classic mode")
        return 1

    # sleeping for 5 seconds for new elections to be triggered and new leader to be elected
    time.sleep(5)

    # Collecting the number of mons in the quorum after the test
    # todo: add other tests to ascertain the health of mon daemons in quorum
    mon_final_count = len(mon_obj.get_mon_quorum().keys())
    if mon_init_count < mon_final_count:
        log.error("There are less mons in the quorum at the end than there before")
        return 1

    log.info("Completed all mon election test cases")
    return 0
