"""
This test module is used to test mon replacement scenarios in the stretch environment
includes:
CEPH-83574971 - Adding and Removing Mons to the Cluster after stretch deployment
1. Data Site mon replacement
2. Arbiter Site  mon replacement

"""

from collections import namedtuple

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from tests.rados.test_stretch_site_down import (
    get_stretch_site_hosts,
    stretch_enabled_checks,
)
from utility.log import Log

log = Log(__name__)

Hosts = namedtuple("Hosts", ["dc_1_hosts", "dc_2_hosts", "tiebreaker_hosts"])


def run(ceph_cluster, **kw):
    """
    performs mon replacement scenarios in stretch mode
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """

    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_name = config.get("pool_name", "test_stretch_io")
    mon_obj = MonitorWorkflows(node=cephadm)
    replacement_site = config.get("replacement_site", "DC1")
    add_mon_without_location = config.get("add_mon_without_location", False)
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "arbiter")

    if not stretch_enabled_checks(rados_obj=rados_obj):
        log.error(
            "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
        )
        raise Exception("Test pre-execution checks failed")

    log.info(
        f"Starting test to perform mon replacement of {replacement_site}. Pre-checks Passed"
    )

    try:
        # Collecting site details
        osd_tree_cmd = "ceph osd tree"
        buckets = rados_obj.run_ceph_command(osd_tree_cmd)
        dc_buckets = [d for d in buckets["nodes"] if d.get("type") == "datacenter"]
        dc_1 = dc_buckets.pop()
        dc_1_name = dc_1["name"]
        dc_2 = dc_buckets.pop()
        dc_2_name = dc_2["name"]
        all_hosts = get_stretch_site_hosts(
            rados_obj=rados_obj, tiebreaker_mon_site_name=tiebreaker_mon_site_name
        )
        dc_1_hosts = all_hosts.dc_1_hosts
        dc_2_hosts = all_hosts.dc_2_hosts
        tiebreaker_hosts = all_hosts.tiebreaker_hosts

        log.debug(f"Hosts present in Datacenter : {dc_1_name} : {dc_1_hosts}")
        log.debug(f"Hosts present in Datacenter : {dc_2_name} : {dc_2_hosts}")
        log.debug(
            f"Hosts present in Datacenter : {tiebreaker_mon_site_name} : {tiebreaker_hosts}"
        )

        log.info("Beginning tests to replace mon daemons on RHCS cluster")

        mon_nodes = ceph_cluster.get_nodes(role="mon")
        cluster_nodes = ceph_cluster.get_nodes()

        # Checking if the site passed for mon replacement is present in the Cluster CRUSH
        if replacement_site in [dc_1_name, dc_2_name]:
            for item in mon_nodes:
                if item.hostname in dc_1_hosts:
                    mon_host = item
                    break
        elif replacement_site in [tiebreaker_mon_site_name]:
            for item in mon_nodes:
                if item.hostname in tiebreaker_hosts:
                    mon_host = item
                    break
        else:
            log.error(
                f"Passed site : {replacement_site} not part of crush locations on cluster.\n"
                f"locations present on cluster : {[tiebreaker_mon_site_name, dc_1_name, dc_2_name]}"
            )
            raise Exception("Test execution failed")

        log.info(
            f"Selected host : {mon_host.hostname} with IP {mon_host.ip_address} to test replacement"
        )

        # Checking if the selected mon is part of Mon Quorum before start of replacement tests
        quorum = mon_obj.get_mon_quorum_hosts()
        if mon_host.hostname not in quorum:
            log.error(
                f"selected host : {mon_host.hostname} does not have mon as part of Quorum"
            )
            raise Exception("Mon not in quorum error")

        # Setting the mon service as unmanaged
        if not mon_obj.set_mon_service_managed_type(unmanaged=True):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not unmanaged error")

        # Checking which DC mon to be replaced, It would be either data site or Arbiter site
        # Proceeding to replace mon in either one of the Data DCs if crush name sent is either DC1 or DC2.
        if replacement_site in [dc_1_name, dc_2_name]:
            log.debug(f"Proceeding to replace mon of the data site {dc_1_name}")

            # Removing mon service
            if not mon_obj.remove_mon_service(host=mon_host.hostname):
                log.error("Could not remove the Data site mon daemon")
                raise Exception("mon service not removed error")

            quorum = mon_obj.get_mon_quorum_hosts()
            if mon_host.hostname in quorum:
                log.error(
                    f"selected host : {mon_host.hostname} mon is present as part of Quorum post removal"
                )
                raise Exception("Mon in quorum error post removal")

            if add_mon_without_location:
                log.info(
                    "Performing -ve scenario where the mon daemon is added without location attributes"
                    "RFE Raised for new health warning to be generated in such cases"
                    "ID: https://bugzilla.redhat.com/show_bug.cgi?id=2280926"
                    "For now, just checking if the mon added without location attributes"
                    " is not included in mon quorum"
                )
                flag = False
                try:
                    # Adding the mon to the cluster without crush locations. -ve scenario
                    mon_obj.add_mon_service(
                        host=mon_host,
                        add_label=False,
                    )
                    flag = True
                except Exception as err:
                    log.info(
                        f"Could not add mon without location attributes."
                        f"Error message displayed : {err}"
                    )
                if flag:
                    log.error(
                        "Command execution passed for mon deployment in stretch mode without location "
                    )

                    # Checking if the new mon added is part of the quorum
                    quorum = mon_obj.get_mon_quorum_hosts()
                    if mon_host.hostname in quorum:
                        log.error(
                            f"selected host : {mon_host.hostname} mon is present as part of Quorum post addition,"
                            f"in -ve scenario"
                        )
                        raise Exception(
                            "Mon in quorum error post addition without location. -ve scenario"
                        )
                    # Removing mon service, which is not part of the quorum
                    if not mon_obj.remove_mon_service(host=mon_host.hostname):
                        log.error(
                            "Could not remove the Data site mon daemon, in -ve scenario"
                        )
                        raise Exception(
                            "mon service not removed error, in -ve scenario"
                        )
                log.info(
                    "-ve Test Pass: Add Mon daemon without location attributes into cluster. Mon could not be added"
                )

            # Adding the mon back to the cluster with crush locations
            if not mon_obj.add_mon_service(
                host=mon_host,
                location_type="datacenter",
                location_name=dc_1_name,
            ):
                log.error("Could not add data site mon service")
                raise Exception("mon service not added error")

            quorum = mon_obj.get_mon_quorum_hosts()
            if mon_host.hostname not in quorum:
                log.error(
                    f"selected host : {mon_host.hostname} mon is not present as part of Quorum post addition"
                )
                raise Exception("Mon not in quorum error post addition")

            log.info("A data site mon was removed and added back. Pass")

        else:
            log.info("replacing Arbiter Mon daemon")

            found = False
            # Arbiter mon cannot be removed directly. A new tiebreaker mon should be deployed temporarily
            for item in cluster_nodes:
                if item.hostname not in [mon.hostname for mon in mon_nodes]:
                    alt_mon_host = item
                    found = True
                    break
            if not found:
                log.error("No alternate mon host was found... fail")
                raise Exception("No temp mon host found error")

            log.debug(
                f"Selected temp mon host: {alt_mon_host.hostname} with "
                f"IP : {alt_mon_host.ip_address} to be deployed for replacement of Arbiter mon"
            )

            # making sure that no mon daemon is running on the alternate host selected.
            quorum = mon_obj.get_mon_quorum_hosts()
            if alt_mon_host.hostname in quorum:
                log.error(
                    f"selected alternate host : {alt_mon_host.hostname} "
                    f"has already a mon daemon present as part of Quorum"
                )
                raise Exception("Mon daemon already present error")

            log.debug(
                "No mon daemon present on selected host. Proceeding to deploy temp tiebreaker mon"
            )

            # Adding the temp mon into to the cluster
            if not mon_obj.add_mon_service(
                host=alt_mon_host,
                location_type="datacenter",
                location_name=tiebreaker_mon_site_name,
            ):
                log.error("Could not add mon service")
                raise Exception("mon service not added error")

            # Setting the new tiebreaker mon
            if not mon_obj.set_tiebreaker_mon(host=alt_mon_host.hostname):
                log.error("Could not set the temp mon as tiebreaker mon")
                raise Exception("mon service not set as tiebreaker error")

            log.debug(
                "New temp mon deployed and set as tiebreaker. Proceeding to remove Arbiter mon now"
            )

            # Removing mon service
            if not mon_obj.remove_mon_service(host=mon_host.hostname):
                log.error("Could not remove the Arbiter mon")
                raise Exception("mon service not removed error")

            quorum = mon_obj.get_mon_quorum_hosts()
            if mon_host.hostname in quorum:
                log.error(
                    f"selected host : {mon_host.hostname} is present as part of Quorum post removal"
                )
                raise Exception("Mon in quorum error post removal")

            log.debug(
                f"Mon {mon_host.hostname} removed from the cluster. Proceeding to add it back"
            )

            # Adding the mon back to the cluster
            if not mon_obj.add_mon_service(
                host=mon_host,
                location_type="datacenter",
                location_name=tiebreaker_mon_site_name,
            ):
                log.error("Could not add mon service")
                raise Exception("mon service not added error")

            # Setting the new tiebreaker mon
            if not mon_obj.set_tiebreaker_mon(host=mon_host.hostname):
                log.error("Could not set the mon as tiebreaker mon")
                raise Exception("mon service not set as tiebreaker error")

            # Removing temp mon service
            if not mon_obj.remove_mon_service(host=alt_mon_host.hostname):
                log.error("Could not remove the temp mon deployed")
                raise Exception("mon service not removed error")

            # Checking if the new mon is present and old is removed from the quorum
            quorum = mon_obj.get_mon_quorum_hosts()
            if mon_host.hostname not in quorum:
                log.error(
                    f"selected host : {mon_host.hostname} arbiter mon is not present as part of Quorum post addition"
                )
                raise Exception("Mon not in quorum error post addition")

            if alt_mon_host.hostname in quorum:
                log.error(
                    f"selected host : {alt_mon_host.hostname} temp mon is present as part of Quorum post removal"
                )
                raise Exception("Mon in quorum error post removal")

            log.info("Completed replacement of Arbiter mon daemon")

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )

        # Setting the mon service back as managed by cephadm
        if not mon_obj.set_mon_service_managed_type(unmanaged=False):
            log.error("Could not set the mon service back to managed")
            raise Exception("mon service not managed error")

        if not rados_obj.run_pool_sanity_check():
            log.error(f"Checks failed post mon replacement for {replacement_site} Site")
            raise Exception("Post execution checks failed on the Stretch cluster")

        if config.get("delete_pool"):
            rados_obj.delete_pool(pool=pool_name)

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("All the tests completed on the cluster, Pass!!!")
    return 0
