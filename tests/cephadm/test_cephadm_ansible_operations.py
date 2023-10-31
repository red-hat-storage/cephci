import time

from cli.exceptions import ConfigError
from cli.ops.cephadm_ansible import (
    exec_ceph_config,
    exec_ceph_orch_apply,
    exec_ceph_orch_daemon,
    exec_ceph_orch_host,
)
from cli.utilities.operations import wait_for_osd_daemon_state


def run(ceph_cluster, **kwargs):
    """Verify cephadm ansible modules"""
    # Get cluster config
    installer, nodes = (
        ceph_cluster.get_nodes(role="installer")[0],
        ceph_cluster.get_nodes(),
    )

    # Get config specs
    config = kwargs.get("config")

    for module in config.keys():
        # Check for module
        if module not in [
            "ceph_orch_host",
            "ceph_orch_apply",
            "ceph_config",
            "ceph_orch_daemon",
        ]:
            continue

        # Get module configs
        module_config = config.get(module)

        # Get bootstrap module configs
        playbook = module_config.get("playbook")
        if not playbook:
            raise ConfigError("Mandatory parameter 'playbook' not provided")

        module_args = module_config.get("module_args", {})

        if module == "ceph_orch_host":
            # Eexecute `ceph_orch_host` module playbook
            exec_ceph_orch_host(installer, nodes, playbook, **module_args)

        elif module == "ceph_orch_apply":
            # Eexecute `ceph_orch_apply` module playbook
            exec_ceph_orch_apply(installer, playbook, **module_args)

        elif module == "ceph_config":
            # Eexecute `ceph_config` module playbook
            exec_ceph_config(installer, playbook, **module_args)

        elif module == "ceph_orch_daemon":
            # Wait for OSD to be expected state
            if module_config.get("wait_for_state") and module_args.get(
                "daemon_type"
            ) in ["osd"]:
                wait_for_osd_daemon_state(
                    installer,
                    module_args.get("daemon_id"),
                    module_config.get("wait_for_state"),
                )

            # Explicitly wait for 60 sec
            time.sleep(60)

            # Eexecute `ceph_orch_daemon` module playbook
            exec_ceph_orch_daemon(installer, playbook, **module_args)

    return 0
