from ceph.nvmeof.initiators.linux import Initiator
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.initiator import get_or_create_initiator
from utility.log import Log

LOG = Log(__name__)


def create_dhchap_key(config, ceph_cluster, update_host_key=False, initiators=None):
    """Generate DHCHAP key for subsystem (once) and unique keys for each initiator host."""
    subnqn = config["subnqn"]

    ret_initiators = []
    for host_config in config["hosts"]:
        node_id = host_config["node"]
        # Check if initiator already exists (from configure_subsystems for bidirectional auth)
        # Only check initiators from current group's list, not global dictionary
        initiator = None
        if initiators:
            # Look for existing initiator with matching node and nqn
            # This ensures we only reuse initiators from the current gateway group
            for existing_init in initiators:
                if existing_init.node.id == node_id and existing_init.nqn == subnqn:
                    initiator = existing_init
                    LOG.info(
                        f"create_dhchap_key: Reusing existing initiator \
                            from current group for node={node_id}, nqn={subnqn}"
                    )
                    break

        # If not found in current group's initiators, get or create a new one
        # Note: get_or_create_initiator uses global dictionary, but we ensure we only
        # reuse initiators from the current group's list above
        if not initiator:
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
            # Only generate host key if it doesn't already exist
            if not initiator.host_key:
                key, _ = initiator.gen_dhchap_key(n=initiator.initiator_nqn())
                host_config["dhchap-key"] = key.strip()
                initiator.host_key = key.strip()
                LOG.info(
                    f"Generated host key {host_config['dhchap-key']} for host {node_id}"
                )
            else:
                # Reuse existing host key
                host_config["dhchap-key"] = initiator.host_key
                LOG.info(f"Reusing existing host key for host {node_id}")

        initiator.nqn = subnqn
        initiator.auth_mode = config.get("auth_mode")
        ret_initiators.append(initiator)
    return ret_initiators


def change_subsystem_key(nvmegwcli, subsys_config, ceph_cluster, initiators=None):
    """Change DHCHAP key for Subsystem and its hosts."""
    # Change key for subsystem
    sub_args = {"subsystem": subsys_config["subnqn"]}
    subsys_update_key = subsys_config.get("update_dhchap_key", False)
    # Initialize initiators list if not provided
    if initiators is None:
        initiators = []
    if subsys_update_key:
        new_initiators = create_dhchap_key(
            subsys_config, ceph_cluster, initiators=initiators
        )
        # Add new initiators to the list (avoid duplicates)
        for new_init in new_initiators:
            if new_init not in initiators:
                initiators.append(new_init)
        sub_args["dhchap-key"] = subsys_config["dhchap-key"]
        LOG.info(f"Updating DHCHAP key for Subsystem {subsys_config['subnqn']}")
        nvmegwcli.subsystem.change_key(
            **{
                "args": {
                    **sub_args,
                }
            }
        )


def change_host_key(ceph_cluster, nvmegwcli, subsys_config, initiators=None):
    # Change key for hosts
    sub_args = {"subsystem": subsys_config["subnqn"]}
    # Initialize initiators list if not provided
    if initiators is None:
        initiators = []
    for host in subsys_config["hosts"]:
        host_update_key = host.get("update_dhchap_key", False)
        initiator_node = get_node_by_id(ceph_cluster, host.get("node"))
        initiator = Initiator(initiator_node)
        if host_update_key:
            new_initiators = create_dhchap_key(
                subsys_config,
                ceph_cluster,
                update_host_key=True,
                initiators=initiators,
            )
            # Add new initiators to the list (avoid duplicates)
            for new_init in new_initiators:
                if new_init not in initiators:
                    initiators.append(new_init)
            sub_args.pop("dhchap-key", None)
            sub_args["dhchap-key"] = host["dhchap-key"]
            LOG.info(f"Updating DHCHAP key for Host {initiator.initiator_nqn()}")
            nvmegwcli.host.change_key(
                **{"args": {**sub_args, **{"host": initiator.initiator_nqn()}}}
            )
