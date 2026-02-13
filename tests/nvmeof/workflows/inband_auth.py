from ceph.nvmeof.initiators.linux import Initiator
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.initiator import get_or_create_initiator
from utility.log import Log

LOG = Log(__name__)


def create_dhchap_key(config, ceph_cluster, update_host_key=False):
    """Generate DHCHAP key for subsystem (once) and unique keys for each initiator host."""
    subnqn = config["subnqn"]

    initiators = []
    for host_config in config["hosts"]:
        node_id = host_config["node"]
        initiator = get_or_create_initiator(node_id, subnqn, ceph_cluster)

        # Case 1: Subsystem key (generate once and reuse for all hosts)
        if not update_host_key:
            if "subsys_key" not in config or config.get(
                "update_dhchap_key", False
            ):  # only generate once
                key, _ = initiator.gen_dhchap_key(n=subnqn)
                config["subsys_key"] = key.strip()
                LOG.info(f"Generated subsystem key {config['subsys_key']} for {subnqn}")
            initiator.subsys_key = config["subsys_key"]
            config["dhchap-key"] = config["subsys_key"]  # backward compatibility

        # Case 2: Host key (unique per initiator)
        else:
            key, _ = initiator.gen_dhchap_key(n=initiator.initiator_nqn())
            host_config["dhchap-key"] = key.strip()
            initiator.host_key = key.strip()
            LOG.info(
                f"Generated host key {host_config['dhchap-key']} for host {node_id}"
            )

        initiator.nqn = subnqn
        initiator.auth_mode = config.get("auth_mode")
        initiators.append(initiator)
    return initiators


def change_subsystem_key(nvmegwcli, subsys_config, ceph_cluster):
    """Change DHCHAP key for Subsystem and its hosts."""
    # Change key for subsystem
    sub_args = {"subsystem": subsys_config["subnqn"]}
    subsys_update_key = subsys_config.get("update_dhchap_key", False)
    if subsys_update_key:
        initiators = create_dhchap_key(subsys_config, ceph_cluster)
        sub_args["dhchap-key"] = subsys_config["dhchap-key"]
        LOG.info(f"Updating DHCHAP key for Subsystem {subsys_config['subnqn']}")
        nvmegwcli.subsystem.change_key(
            **{
                "args": {
                    **sub_args,
                }
            }
        )
    return initiators


def change_host_key(ceph_cluster, nvmegwcli, subsys_config):
    # Change key for hosts
    sub_args = {"subsystem": subsys_config["subnqn"]}
    initiators = []
    for host in subsys_config["hosts"]:
        host_update_key = host.get("update_dhchap_key", False)
        initiator_node = get_node_by_id(ceph_cluster, host.get("node"))
        initiator = Initiator(initiator_node)
        if host_update_key:
            initiators.extend(
                create_dhchap_key(subsys_config, ceph_cluster, update_host_key=True)
            )
            sub_args.pop("dhchap-key", None)
            sub_args["dhchap-key"] = host["dhchap-key"]
            LOG.info(f"Updating DHCHAP key for Host {initiator.initiator_nqn()}")
            nvmegwcli.host.change_key(
                **{"args": {**sub_args, **{"host": initiator.initiator_nqn()}}}
            )
    return initiators
