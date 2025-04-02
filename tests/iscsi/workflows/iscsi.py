from ceph.ceph_admin.orch import Orch
from ceph.iscsi.gateway import Iscsi_Gateway
from ceph.utils import get_node_by_id
from tests.iscsi.workflows.initiator import Initiator
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


class ISCSI:

    def __init__(self, ceph_cluster, gateways, config):
        self.cluster = ceph_cluster
        self.config = config
        self.gateways = []
        self.orch = Orch(cluster=self.cluster, **{})

        for gw in gateways:
            gw_node = get_node_by_id(self.cluster, gw)
            self.gateways.append(Iscsi_Gateway(gw_node))

        self.initiators = []

    def configure_targets(self, pool, config):
        """Configure Ceph-iscsi targets."""
        LOG.info(f"Configure iSCSI targets and luns as per config: {config}")
        gw_node = self.gateways[0]

        # Add iSCSI target
        iqn = config["iqn"]
        LOG.info(f"Configure IQN target - {iqn}")
        ceph_cluster = config["ceph_cluster"]
        gw_node.gwcli.target.create(iqn)

        # Add gateways
        for gw in config.get("gateways"):
            _gw = get_node_by_id(ceph_cluster, gw)
            gw_cfg = {
                "gateway_name": _gw.hostname,
                "ip_addresses": _gw.ip_address,
            }
            gw_node.gwcli.target.gateways.create(iqn, **gw_cfg)

        # Add Hosts and its lun(s)
        if config.get("hosts"):
            for host in config["hosts"]:
                client_iqn = host["client_iqn"]
                host_args = {"client_iqn": client_iqn}
                gw_node.gwcli.target.hosts.create(iqn, **host_args)

                if host.get("disks"):
                    bdev_configs = host["disks"]
                    if isinstance(bdev_configs, dict):
                        bdev_configs = [bdev_configs]

                    for bdev_cfg in bdev_configs:
                        name = generate_unique_id(length=4)
                        size = bdev_cfg["size"]

                        for num in range(bdev_cfg["count"]):
                            args = {
                                "pool": pool,
                                "image": f"{name}-{num}",
                                "size": size,
                            }
                            # Create disk
                            gw_node.gwcli.disks.create(**args)

                            # Add Disk to target
                            args = {"disk": f"{pool}/{name}-{num}"}
                            gw_node.gwcli.target.disks.create(iqn, **args)

                            # Attach Disk to client
                            args = {
                                "disk": f"{pool}/{name}-{num}",
                                "size": size,
                                "action": "add",
                            }
                            gw_node.gwcli.target.hosts.client.disk(
                                iqn, client_iqn, **args
                            )
        out, err = gw_node.gwcli.list()
        LOG.error(f"err-{err}")
        LOG.debug(f"Output - {out}")

    def prepare_initiators(self):
        for initiator in self.config.get("initiators"):
            _node = get_node_by_id(self.cluster, initiator["node"])
            _client = Initiator(_node, initiator["iqn"], self.gateways[0])
            _client.connect_targets()
            self.initiators.append(_client)
