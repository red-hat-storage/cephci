"""Test module to validate the deployment of Ceph iSCSI Gateways and Run E2E test."""

from ceph.ceph import Ceph
from ceph.parallel import parallel
from tests.iscsi.workflows.iscsi import ISCSI
from tests.iscsi.workflows.iscsi_utils import (
    delete_iscsi_service,
    deploy_iscsi_service,
    map_iqn_to_config,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def teardown(ceph_cluster, iscsi, rbd_obj, config):
    """Cleanup ISCSI.

    Args:
        ceph_cluster: Ceph Cluster
        rbd_obj: RBD object
        config: test config
    """
    # Disconnect targets
    if "initiators" in config["cleanup"]:
        for initiator in iscsi.initiators:
            initiator.cleanup()

    # Delete the Individual iSCSI Targets
    if "targets" in config["cleanup"]:
        gwnode = iscsi.gateways[0]
        for target in config["targets"]:
            gwnode.gwcli.target.delete(target["iqn"])

    # Delete the gateway
    if "gateway" in config["cleanup"]:
        delete_iscsi_service(ceph_cluster, config)

    # Delete the pool
    if "pool" in config["cleanup"]:
        rbd_obj.clean_up(pools=[config["rbd_pool"]])


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph iSCSI test execution.

    - Configure Gateways
    - Configures Initiators and Run FIO on iSCSI targets.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:

        # Execute the iSCSI GW test
            - test:
                name: Ceph iSCSI deployment
                desc: Configure iSCSI gateways and initiators
                config:
                    gw_nodes:
                        - node6
                        - node7
                    rbd_pool: rbd
                    do_not_create_image: true
                    rep-pool-only: true
                    rep_pool_config:
                        pool: rbd
                    install: true
                    targets:
                      - iqn: iqn.2003-01.com.redhat.iscsi-gw:ceph-igw
                        gateways:
                          - node6
                          - node7
                        hosts:
                          - client_iqn: iqn.2003-01.com.redhat.iscsi-gw:rh-client
                            disks:
                            - count: 5
                              size: 2G
                    initiators:
                      - iqn: iqn.2003-01.com.redhat.iscsi-gw:rh-client
    """
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    iscsi = None
    try:
        if config.get("install"):
            LOG.info("Starting Ceph iSCSI deployment.")
            deploy_iscsi_service(ceph_cluster, config)

        config = map_iqn_to_config(config)

        # Generate IQNs and map to config
        iscsi = ISCSI(ceph_cluster, config["gw_nodes"], config)

        # Configure targets
        if config.get("targets"):
            with parallel() as p:
                for target_args in config["targets"]:
                    target_args["ceph_cluster"] = ceph_cluster
                    p.spawn(iscsi.configure_targets, rbd_pool, target_args)

        # Configure Initiators and run IO
        if config.get("initiators"):
            iscsi.prepare_initiators()
            with parallel() as p:
                for initiator in iscsi.initiators:
                    p.spawn(initiator.start_fio)
        LOG.info("Basic Sanity test completed.....")
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, iscsi, rbd_obj, config)

    return 1
