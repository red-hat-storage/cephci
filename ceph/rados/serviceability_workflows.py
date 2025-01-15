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
            deploy_osd(bool): Flag to control ODS deployment
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
                                        "placement": {"label": "osd-bak"},
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
        # get initial count of hosts in the cluster
        host_count_pre = self.get_host_count()
        # Removing an OSD host and checking status
        rm_host = utils.get_node_by_id(self.cluster, host_node_name)
        log.info(f"Identified host : {rm_host.hostname} to be removed from the cluster")

        # get list of osd_id on the host to be removed
        rm_osd_list = self.rados_obj.collect_osd_daemon_ids(osd_node=rm_host)
        log.info(f"list of osds on {rm_host.hostname}: {rm_osd_list}")

        # Starting to drain an offline host.
        log.info("Starting to drain an offline host")
        self.cephadm.shell([f"ceph orch host drain {rm_host.hostname} --force"])

        # Sleeping for 2 seconds for removal to have started
        time.sleep(2)
        log.debug(f"Started drain operation on node : {rm_host.hostname}")

        status_cmd = "ceph orch osd rm status -f json"
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=7200)
        flag = False
        while end_time > datetime.datetime.now():
            out, err = self.cephadm.shell([status_cmd])
            try:
                drain_ops = json.loads(out)
                for entry in drain_ops:
                    if entry["drain_done_at"] is None or entry["draining"]:
                        log.debug(
                            f"Drain operations are going on host {rm_host.hostname} \nOperations: {entry}"
                        )
                        raise Exception(
                            f"drain process for OSD {entry['osd_id']} is still going on"
                        )
                    log.info(f"Drain operation completed for OSD {entry['osd_id']}")
                log.info(f"Drain operations completed on host : {rm_host.hostname}")
                flag = True
                break
            except json.JSONDecodeError:
                log.info(f"Drain operations completed on host : {rm_host.hostname}")
                flag = True
                break
            except Exception as error:
                if end_time < datetime.datetime.now():
                    log.error(f"Hit issue during drain operations: {error}")
                    raise Exception(error)
                log.info("Sleeping for 120 seconds and checking again....")
                time.sleep(120)

        if not flag:
            log.error(
                "Drain operation not completed on the cluster even after 7200 seconds"
            )
            raise
        # remove the offline host so that drain operation gets completed
        log.info(f"Removing host {rm_host.hostname} from the cluster")
        self.cephadm.shell([f"ceph orch host rm {rm_host.hostname} --force --offline"])
        time.sleep(10)
        if not host_count_pre > self.get_host_count():
            log.error("Host count in the cluster has not reduced")
            out, err = self.cephadm.shell(
                [f"ceph orch host ls --host_pattern {rm_host.hostname}"]
            )
            if "0 hosts in cluster" not in out or rm_host.hostname in out:
                log.error(f"{rm_host.hostname} is still part of the cluster")
            raise Exception("Host count in the cluster has not reduced")
        log.info(
            f"Completed drain and removal operation on the host. {rm_host.hostname}"
        )

    def remove_custom_host(self, host_node_name: str):
        """
        Method to remove a specific online host from the cluster
        Args:
            host_node_name: node name of the host to be removed
            If hostname of machine to be removed is ceph-radosqe-l0wj0b-node7, then
            input should be 'node7'
        Returns:
            None | raises exception in case of failure
        """
        status_cmd = "ceph orch osd rm status -f json"
        try:

            def wait_osd_operation_status(status_cmd):
                status_flag = False
                txt_osd_removed_logic = """
                      The logic used to verify the OSD is removed or not is-
                      case1: If the ceph is still in process of removing the OSD the command generated
                      the proper json output.The json.loads method loads the output without any failure.
                      case2: If the OSDs are removed from the node then the command wont generate any output.
                      In this case the json.loads method throws the JSONDecodeError exception.This is the
                      confirmation that the OSDs removal are completed. """
                end_time = datetime.datetime.now() + datetime.timedelta(seconds=600)
                log.debug(f"{txt_osd_removed_logic}")
                while end_time > datetime.datetime.now():
                    out, err = self.cephadm.shell([status_cmd])
                    try:
                        drain_ops = json.loads(out)
                        for entry in drain_ops:
                            log.debug(
                                f"OSD remove operation is in progress {osd_id}\nOperations: {entry}"
                            )
                    except json.JSONDecodeError:
                        log.info(f"The OSD removal is completed on OSD : {osd_id}")
                        status_flag = True
                        break
                    except Exception as error:
                        log.error(f"Hit issue during drain operations: {error}")
                        raise Exception(error)
                    log.debug("Sleeping for 10 seconds and checking again....")
                    time.sleep(10)
                return status_flag

            daemon_check = self.rados_obj.check_daemon_exists_on_host(
                host=host_node_name, daemon_type=None
            )

            if not daemon_check:
                log.info(f" The node {host_node_name} is already drained.")
                return None
            # Removing an OSD host and checking status
            rm_host = utils.get_node_by_id(self.cluster, host_node_name)
            log.info(
                f"Identified host : {rm_host.hostname} to be removed from the cluster"
            )

            # Get list of osd_id on the host to be removed
            rm_osd_list = self.rados_obj.collect_osd_daemon_ids(osd_node=rm_host)
            log.info(
                f"The osd id  list to be removed from the {rm_host} is  {rm_osd_list}"
            )
            # Get the OSD out list and remove before drain the node
            osd_out_list = self.rados_obj.get_osd_list(status="out")
            log.info(
                f"The out osd id  list to be removed from the {rm_host} is  {osd_out_list}"
            )
            if osd_out_list:
                for osd_id in rm_osd_list:
                    if osd_id in osd_out_list:
                        osd_utils.osd_remove(self.cluster, osd_id=osd_id, zap=True)
                        time.sleep(10)
                        if wait_osd_operation_status(status_cmd):
                            log.info("The OSD successfully removed")
                        else:
                            log.error(
                                "OSD removal not completed on the cluster even after 600 seconds"
                            )
                            raise Exception("OSD not removed error")
                        rm_osd_list.remove(osd_id)
            dev_path_list = []
            if rm_osd_list:
                for osd_id in rm_osd_list:
                    dev_path_list.append(
                        rados_utils.get_device_path(host=rm_host, osd_id=osd_id)
                    )
                    osd_utils.set_osd_out(self.cluster, osd_id=osd_id)
                    time.sleep(30)
                    osd_utils.osd_remove(self.cluster, osd_id=osd_id)
                time.sleep(30)

            # Starting to drain the host
            drain_cmd = (
                f"ceph orch host drain {rm_host.hostname} --force --zap-osd-devices "
            )
            self.cephadm.shell([drain_cmd])

            # Sleeping for 2 seconds for removal to have started
            time.sleep(2)
            log.debug(f"Started drain operation on node : {rm_host.hostname}")
            if wait_osd_operation_status(status_cmd):
                log.info(
                    f"Completed drain operation on the host. {rm_host.hostname}\n Removing host from the cluster"
                )
            else:
                log.error(
                    "Drain operation not completed on the cluster even after 600 seconds"
                )
                raise Exception("Drain operation-OSD not removed error")

            if dev_path_list:
                for dev_path in dev_path_list:
                    assert osd_utils.zap_device(
                        self.cluster, host=rm_host.hostname, device_path=dev_path
                    )
            # Check that the OSD daemons are exists in the host
            daemon_check = self.rados_obj.check_daemon_exists_on_host(
                host=host_node_name, daemon_type=None
            )
            if not daemon_check:
                log.info(f" The node {host_node_name} is already drained.")
                return None

            time.sleep(5)
            rm_cmd = f"ceph orch host rm {rm_host.hostname} --force"
            self.cephadm.shell([rm_cmd])
            time.sleep(5)

            # Checking if the host still exists on the cluster
            ls_cmd = "ceph orch host ls"
            hosts = self.rados_obj.run_ceph_command(cmd=ls_cmd)
            for host in hosts:
                if host["hostname"] == rm_host.hostname:
                    log.error(f"Host : {rm_host.hostname} still present on the cluster")
                    raise Exception("Host not removed error")
            log.info(
                f"Successfully removed host : {rm_host.hostname} from the cluster. Checking status after removal"
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
            # get list of osd_id on the host to be removed
            rm_osd_list = self.rados_obj.collect_osd_daemon_ids(osd_node=host_obj)
            dev_path_list = []
            for osd_id in rm_osd_list:
                dev_path_list.append(
                    rados_utils.get_device_path(host=host_obj, osd_id=osd_id)
                )
                osd_utils.set_osd_out(self.cluster, osd_id=osd_id)
                osd_utils.osd_remove(self.cluster, osd_id=osd_id)
            time.sleep(30)
            for dev_path in dev_path_list:
                assert osd_utils.zap_device(
                    self.cluster, host=host_obj.hostname, device_path=dev_path
                )
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            raise
