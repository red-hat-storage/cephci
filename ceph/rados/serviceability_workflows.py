"""
Module to perform Serviceability scenarios on Cluster Hosts / Nodes
"""

import datetime
import json
import time

from ceph import utils
from ceph.ceph_admin import CephAdmin
from ceph.rados import utils as osd_utils
from ceph.rados.core_workflows import RadosOrchestrator
from tests.ceph_installer import test_cephadm
from tests.cephadm.test_host import run as deploy_host
from tests.rados import rados_test_util as rados_utils
from utility.log import Log

log = Log(__name__)


class ServiceabilityMethods:
    """
    Contains various functions that help in addition and removal of hosts on the cluster.
    """

    def __init__(self, cluster, **config):
        """
        Initialize Cephadm with ceph_cluster object

        Args:
            cluster (Ceph.Ceph): Ceph cluster object
            config (Dict): test data configuration
        """
        self.cluster = cluster
        self.config = config
        self.cephadm = CephAdmin(cluster=self.cluster, **self.config)
        self.rados_obj = RadosOrchestrator(node=self.cephadm)
        self.client = self.rados_obj.client
        self.rhbuild = self.rados_obj.rhbuild

    def get_host_count(self):
        return len(self.rados_obj.run_ceph_command(cmd="ceph orch host ls"))

    def get_osd_count(self):
        return len(self.rados_obj.run_ceph_command(cmd="ceph osd ls"))

    def add_new_hosts(
        self,
        add_nodes: list = None,
        crush_bucket_name: str = None,
        crush_bucket_type: str = None,
        crush_bucket_val: str = None,
        deploy_osd: bool = True,
        osd_label: str = "osd-bak",
    ):
        """
        Module to add a new host to an existing deployment.
        Assumptions: If nodeId as per global conf is not provided,
        it is assumed that a 13-node cluster was deployed and method
        will try to add predefined spare nodes(node12 & node13) to
        the cluster
        Args:
            add_nodes(list): List input of nodeIDs to be added to the cluster
            crush_bucket_name: Name of the bucket to be moved
            crush_bucket_type(str): Name of the crush bucket to add during host addition.
                                    Eg: datacenter, zone, host etc...
            crush_bucket_val(str): Value of the crush bucket to add during host addition.
            deploy_osd(bool): Flag to control OSD deployment
            osd_label(str): Placement label for OSD deployment
        Returns:
            None | Raises exception in case of failure.

        NOTE :
        1. If OSDs are to be deployed on host that is added, make sure that the label on the host is "osd-bak"
        2. OSDs would be deployed on all available devices on the host/s added
        3. If no nodes are provided, the method tries the deployment on node12 & node13
        """
        try:
            if add_nodes is None:
                add_nodes = ["node12", "node13"]
            # Adding new hosts to the cluster
            add_args = {
                "command": "add_hosts",
                "service": "host",
                "args": {
                    "nodes": add_nodes,
                    "attach_address": True,
                    "labels": "apply-all-labels",
                },
            }
            add_args.update(self.config)

            ncount_pre = self.get_host_count()
            deploy_host(ceph_cluster=self.cluster, config=add_args)
            if crush_bucket_name:
                cmd = f"ceph osd crush move {crush_bucket_name} {crush_bucket_type}={crush_bucket_val}"
                self.rados_obj.run_ceph_command(cmd=cmd)
                time.sleep(5)

            if not ncount_pre < self.get_host_count():
                log.error("New hosts are not added into the cluster")
                raise Exception("Execution error")

            log.info("New hosts added to the cluster successfully.")

            if deploy_osd:
                log.info("Proceeding to deploy OSDs on the same.")
                # Deploying OSDs on the new nodes.
                osd_args = {
                    "steps": [
                        {
                            "config": {
                                "command": "apply_spec",
                                "service": "orch",
                                "validate-spec-services": True,
                                "specs": [
                                    {
                                        "service_type": "osd",
                                        "service_id": "new_osds",
                                        "encrypted": "true",
                                        "placement": {"label": osd_label},
                                        "spec": {"data_devices": {"all": "true"}},
                                    }
                                ],
                            }
                        }
                    ]
                }
                osd_args.update(self.config)
                osdcount_pre = self.get_osd_count()
                test_cephadm.run(ceph_cluster=self.cluster, config=osd_args)
                if not osdcount_pre < self.get_osd_count():
                    log.error("New OSDs were not added into the cluster")
                    raise Exception("Execution error")

                log.info("Deployed OSDs on new hosts")
            log.info("New hosts added to the cluster")
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            raise

    def remove_offline_host(self, host_node_name: str):
        """
        Method to remove a specific offline host from the cluster
        Args:
            host_node_name: node name of the host to be removed
            If hostname of machine to be removed is ceph-radosqe-l0wj0b-node7, then
            input should be 'node7'
        Returns:
            None | raises exception in case of failure
        """
        try:
            # Removing an offline host and checking status
            rm_host = utils.get_node_by_id(self.cluster, host_node_name)
            log.info(
                "Identified host : %s to be removed from the cluster" % rm_host.hostname
            )

            # cursory check to ensure input node is actually offline
            if not self.rados_obj.check_host_status(
                hostname=rm_host.hostname, status="offline"
            ):
                err_msg = (
                    f"Input host {rm_host.hostname} is not in Offline state. Exiting"
                )
                log.error(err_msg)
                raise Exception(err_msg)

            log.info("Removing offline host %s from the cluster" % rm_host.hostname)
            self.remove_custom_host(host_node_name=host_node_name, offline=True)
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            raise
        log.info(
            "Completed drain and removal operation for the offline host "
            + rm_host.hostname
        )

    def remove_custom_host(self, host_node_name: str, offline=False):
        """
        Method to remove a specific online host from the cluster
        Args:
            host_node_name: node name of the host to be removed
            If hostname of machine to be removed is ceph-radosqe-l0wj0b-node7, then
            input should be 'node7'
            offline: flag to reuse the method for an offline host
        Returns:
            None | raises exception in case of failure
        """
        try:
            # Removing an OSD host and checking status
            rm_host = utils.get_node_by_id(self.cluster, host_node_name)
            log.info(
                "Identified host : %s to be removed from the cluster" % rm_host.hostname
            )

            # get list of osd_id on the host to be removed
            rm_osd_list = self.rados_obj.collect_osd_daemon_ids(osd_node=rm_host)
            log.info(
                "The OSD list to be removed from the host %s is %s"
                % (rm_host.hostname, rm_osd_list)
            )

            if not offline:
                daemon_check = self.rados_obj.check_daemon_exists_on_host(
                    host=rm_host.hostname, daemon_type=None
                )
                host_labels = self.rados_obj.get_host_label(host_name=rm_host.hostname)
                if not daemon_check or "_no_schedule" in host_labels:
                    log.info("The node %s is already drained" % rm_host.hostname)
                else:
                    # Starting to drain an online host.
                    log.info("Starting to drain host " + rm_host.hostname)
                    if not self.drain_host(host_obj=rm_host):
                        log.error(
                            "Drain operation not completed on the cluster even after 3600 seconds"
                        )
                        raise Exception(
                            "Drain operation not completed on the cluster even after 3600 seconds"
                        )

                log.info(
                    "Completed drain operation on the host. %s\n Removing host from the cluster"
                    % rm_host.hostname
                )

            time.sleep(5)
            rm_cmd = f"ceph orch host rm {rm_host.hostname} --force"
            if offline:
                rm_cmd += " --offline"
            self.cephadm.shell([rm_cmd])

            # wait for 120 secs for host to be removed from the cluster
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=120)
            while end_time > datetime.datetime.now():
                # Checking if the host still exists on the cluster
                ls_cmd = f"ceph orch host ls --host_pattern {rm_host.hostname}"
                out = self.rados_obj.run_ceph_command(cmd=ls_cmd, client_exec=True)
                if not out:
                    log.info(
                        "Successfully removed host : %s from the cluster"
                        % rm_host.hostname
                    )
                    break
                log.info(out)
                log.error("Host : %s still present on the cluster" % rm_host.hostname)
                log.info("Sleeping for 30 secs and checking again")
                time.sleep(30)
            else:
                raise Exception(
                    "Could not remove host %s within timeout" % rm_host.hostname
                )
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            raise

    def remove_osds_from_host(self, host_obj):
        """
        Method to remove all existing OSDs from the given host
        Args:
            host_obj: node object of the OSD host
        Returns:
            None | raises exception in case of failure
        """
        try:
            # set existing services on the cluster to unmanaged
            osd_services = self.rados_obj.list_orch_services(service_type="osd")
            for service in osd_services:
                self.rados_obj.set_unmanaged_flag(
                    service_type="osd", service_name=service
                )

            # get list of osd_id on the host to be removed
            rm_osd_list = self.rados_obj.collect_osd_daemon_ids(osd_node=host_obj)
            dev_path_list = []
            for osd_id in rm_osd_list:
                dev_path_list.append(
                    rados_utils.get_device_path(host=host_obj, osd_id=osd_id)
                )
                osd_utils.set_osd_out(self.cluster, osd_id=osd_id)
                osd_utils.osd_remove(self.cluster, osd_id=osd_id, zap=True, force=True)

                # wait for 180 secs for OSD to be removed from the cluster
                end_time = datetime.datetime.now() + datetime.timedelta(seconds=180)
                while end_time > datetime.datetime.now():
                    if not self.rados_obj.get_daemon_status(
                        daemon_type="osd", daemon_id=osd_id
                    ):
                        break
                    log.info(
                        "OSD %s removal is in-progress. Sleeping for 180 secs" % osd_id
                    )
                    time.sleep(120)
                else:
                    err_msg = f"Could not remove OSD {osd_id} within 180 secs"
                    log.error(err_msg)
                    raise Exception(err_msg)

            for dev_path in dev_path_list:
                assert osd_utils.zap_device(
                    self.cluster, host=host_obj.hostname, device_path=dev_path
                )
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            raise
        finally:
            # set osd services to managed
            osd_services = self.rados_obj.list_orch_services(service_type="osd")
            for service in osd_services:
                self.rados_obj.set_managed_flag(
                    service_type="osd", service_name=service
                )

    def unmanaged_osd_service_exists(self) -> bool:
        """
        Method to check if the cluster has unmanaged service based OSDs
        Args:
        Returns:
            unmanaged service based OSDs exist -> True
            unmanaged service based OSDs do not exist -> False
        """
        out = self.rados_obj.list_orch_services(service_type="osd", export=True)
        for _service in out:
            if not (
                _service.get("placement", False) or _service.get("service_id", False)
            ):
                log.info("Service %s not managed", _service)
                return True
        log.info("All OSD daemons are managed by named service")
        return False

    def get_cephadm_managable_osd_specs(self) -> list:
        """
        Method to get the valid OSD specs that are present in the cluster.
        Args:
        returns:
            List of names of OSD specs that are valid
        """
        return [
            name["service_id"]
            for name in self.rados_obj.list_orch_services(
                service_type="osd", export=True
            )
            if ("placement" and "service_id") in name
        ]

    def get_osd_spec(self, osd_id) -> str:
        """
        method to get the name of spec, to which a particular OSD belongs
        Args:
            osd_id: OSD ID whose spec is needed
        Returns:
            name of the spec
        """
        osd_metadata = self.rados_obj.get_daemon_metadata(
            daemon_type="osd", daemon_id=osd_id
        )
        return osd_metadata.get("osdspec_affinity", None)

    def add_osds_to_managed_service(
        self,
        osds: list = None,
        spec: str = None,
        remove_empty_service_spec: bool = False,
    ) -> bool:
        """
        Method to identify non-managed OSDs and then add them back to managed state under existing OSD specs
        Args:
            osds: list of OSDs to be set to managed state
            spec: name of the spec that needs to be used for setting OSD to manage.
            remove_empty_service_spec: ( type: bool, default: false ) If set to True, any OSD service managing with 0
                daemons will be removed
        Returns:
            Pass -> True
            fail -> False

        Note : if osds list is provided, Only those osds will be added to managed and check if the cluster,
        is free of unmanaged OSDs.

        Note : If no OSDs are provided, we add all OSDs to managed host by host,
        until we're left with no unmanaged osds on cluster.

        Note: If Spec is not provided, 1st available - managed OSD spec name would be used
        """
        # 8.1 : https://bugzilla.redhat.com/show_bug.cgi?id=2304314
        # 7.1 : https://bugzilla.redhat.com/show_bug.cgi?id=2370168
        if not float(self.rhbuild.split("-")[0]) >= 7.1:
            log.info(
                "Feature not backport-ed on versions lower than 8.1. Skipping workflow"
                "Tests running on build : %s",
                self.rhbuild,
            )
            return True

        if not self.unmanaged_osd_service_exists():
            log.info(
                "No un-manage-able placement spec based OSDs exist on the cluster on the start of test"
            )

        if spec and spec not in self.get_cephadm_managable_osd_specs():
            log.error("Passed spec does not exist on the cluster")
            log.error(
                f"Spec name passed : {spec}, names on the Cluster : {self.get_cephadm_managable_osd_specs()}"
            )
            return False

        def set_osds_managed(osd_list):
            set_managed_raw = "ceph orch osd set-spec-affinity"
            osd_args = " ".join(str(osd) for osd in osd_list)
            placement = spec if spec else self.get_cephadm_managable_osd_specs()[0]
            log.debug("Placement spec chosen for movement : %s", placement)
            final_cmd = f"{set_managed_raw} osd.{placement} {osd_args}"
            self.rados_obj.client.exec_command(sudo=True, cmd=final_cmd)

        if osds:
            set_osds_managed(osds)
            time.sleep(10)
            for osd_id in osds:
                if not self.get_osd_spec(osd_id=osd_id):
                    log.error(
                        "un-manage-able placement spec based OSDs exist on cluster even after adding the provided osds."
                        "\nOffending OSD : %s",
                        osd_id,
                    )
                    return False
            log.info(
                "All OSDs part of list has been added to a managed OSD service. pass"
            )

        else:
            log.info(
                "No list passed. Setting the set-affinity command on all the OSDs that do not have a valid service name"
            )
            osd_daemons = self.rados_obj.get_osd_list(status="in")
            unmanaged_spec_osds = []
            for osd_id in osd_daemons:
                if not self.get_osd_spec(osd_id=osd_id):
                    log.debug("OSD ID %s in  un-manage-able placement spec", osd_id)
                    unmanaged_spec_osds.append(osd_id)
            if unmanaged_spec_osds:
                set_osds_managed(unmanaged_spec_osds)
                time.sleep(10)
            if self.unmanaged_osd_service_exists():
                log.info("All OSDs not set to manage-able placement spec")
                return False
            if remove_empty_service_spec:
                if (
                    self.rados_obj.remove_empty_service_spec(service_type="osd")
                    is False
                ):
                    log.info("Could not remove empty service spec")
                    return False
            log.info("Completed running set-managed for all OSDs without correct spec")

        if remove_empty_service_spec:
            if self.rados_obj.remove_empty_service_spec(service_type="osd") is False:
                log.info("Could not remove empty service spec")
                return False

        return True

    def drain_host(self, host_obj, zap=True, keep_conf=False):
        """
        Method to execution drain on a particular host
        Args:
            host_obj: node object of the cluster host
            zap: flag to control --zap-osd-devices
            keep_conf: flag to control --keep-conf-keyring
        Returns:
            True -> drain successful | Fail -> drain failed
        """
        # Starting to drain a host.
        log.info("Starting to drain host: " + host_obj.hostname)
        cmd = f"ceph orch host drain {host_obj.hostname} --force"
        if zap:
            if self.rhbuild.split(".")[0] > "6":
                cmd = cmd + " --zap-osd-devices"
            else:
                rm_osd_list = self.rados_obj.collect_osd_daemon_ids(osd_node=host_obj)
                dev_path_list = []
                for osd_id in rm_osd_list:
                    dev_path_list.append(
                        rados_utils.get_device_path(host=host_obj, osd_id=osd_id)
                    )
        if keep_conf:
            cmd = cmd + " --keep-conf-keyring"

        # enable faster recovery
        self.rados_obj.change_recovery_threads(config={}, action="set")

        self.cephadm.shell(args=[cmd], print_output=True, pretty_print=True)
        # Sleeping for 2 seconds for drain to have started
        time.sleep(2)
        log.debug("Started drain operation on node : " + host_obj.hostname)

        timeout_in_seconds = 3600
        end_time = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout_in_seconds
        )
        drain_success = False
        status_cmd = "ceph orch osd rm status"
        # TBD: Add checks for 8.1 BZs
        # https://bugzilla.redhat.com/show_bug.cgi?id=2375839
        while end_time > datetime.datetime.now():
            try:
                drain_ops = self.rados_obj.run_ceph_command(
                    cmd=status_cmd, client_exec=True, print_output=True
                )
                for entry in drain_ops:
                    if entry["drain_done_at"] is None or entry["draining"]:
                        log.info(
                            "Drain operations are going on host %s \nOperations: %s"
                            % (host_obj.hostname, entry)
                        )
                        log.debug(
                            "drain process for OSD %s is still going on"
                            % entry["osd_id"]
                        )
                log.info("Sleeping for 20 seconds and checking again....")
                time.sleep(20)
            except json.JSONDecodeError:
                log.info("Drain operations completed on host : " + host_obj.hostname)
                if self.rados_obj.check_daemon_exists_on_host(host_obj.hostname):
                    log.info(
                        "Drain operations not completed on host : " + host_obj.hostname
                    )
                    log.info("Sleeping for 20 seconds and checking again....")
                    time.sleep(20)
                else:
                    log.info(
                        "Drain operations completed on host : " + host_obj.hostname
                    )
                    drain_success = True
                    break

        if drain_success is False:
            log.info(
                "Drain operations not completed on host : "
                + host_obj.hostname
                + " with timeout of "
                + str(timeout_in_seconds)
                + " seconds"
            )

        if self.rhbuild.split(".")[0] < "7":
            # zap all devices on the removed host
            # because we are not zapping OSD devices
            # with host drain

            for dev_path in dev_path_list:
                assert osd_utils.zap_device(
                    self.cluster, host=host_obj.hostname, device_path=dev_path
                )

        # remove recovery thread settings
        self.rados_obj.change_recovery_threads(config={}, action="rm")

        return drain_success
