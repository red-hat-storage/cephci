import ast
import json

from ceph.ceph_admin.orch import Orch
from ceph.utils import get_node_by_id, get_nodes_by_ids
from tests.cephadm import test_nvmeof
from utility.log import Log

LOG = Log(__name__)


class NVMeDeployArgumentError(Exception):
    pass


class NVMeDeployConfigParamRequired(Exception):
    pass


class OMAPValidationFailure(Exception):
    pass


def get_nvme_service_name(pool, group=None):
    svc_name = f"nvmeof.{pool}"
    if group:
        svc_name = f"{svc_name}.{group}"
    return svc_name


def apply_nvme_sdk_cli_support(ceph_cluster, config):
    """Configure NVMe deployment CLI w.r.t release support.

    This definition helps to select deployment CLI as supported
     from a downstream release perspective.

    Currently,
     7.x - Only RBD pool name has to be provided as positional arg
     8.0 - Along RBD pool name, the Gateway group name has to be provided.

    And in future any change in deployment could be handled here.

    Args:
      ceph_cluster: Ceph cluster object
      config: test case configuration parameters

    ::Example:
        config:
            rbd_pool: rbd               # rbd pool name
            gw_group: gateway_group1    # NVMe Gateway group name
    """

    release = ceph_cluster.rhcs_version
    rbd_pool = config.get("rbd_pool") or config.get("pool")
    if not rbd_pool:
        raise NVMeDeployConfigParamRequired(
            "Please provide RBD pool name nodes via rbd_pool or pool"
        )

    gw_nodes = config.get("gw_nodes", None) or config.get("gw_node", None)

    if not gw_nodes:
        raise NVMeDeployConfigParamRequired(
            "Please provide gateway nodes via gw_nodes or gw_node"
        )

    if not isinstance(gw_nodes, list):
        gw_nodes = [gw_nodes]

    gw_nodes = get_nodes_by_ids(ceph_cluster, gw_nodes)
    is_spec_or_mtls = config.get("mtls", False) or config.get("spec_deployment", False)
    gw_group = config.get("gw_group")

    cfg = {
        "no_cluster_state": False,
        "config": {
            "command": "apply",
            "service": "nvmeof",
            "args": {"placement": {"nodes": [i.hostname for i in gw_nodes]}},
            "pos_args": [rbd_pool],
        },
    }
    if is_spec_or_mtls:
        cfg = {
            "no_cluster_state": False,
            "config": {
                "command": "apply_spec",
                "service": "nvmeof",
                "validate-spec-services": True,
                "specs": [
                    {
                        "service_type": "nvmeof",
                        "service_id": rbd_pool,
                        "mtls": config.get("mtls", False),
                        "placement": {"nodes": [i.hostname for i in gw_nodes]},
                        "spec": {
                            "pool": rbd_pool,
                            "enable_auth": config.get("mtls", False),
                        },
                    }
                ],
            },
        }

    if release <= ("7.1"):
        return cfg
    elif release == "8.0":
        if not gw_group:
            raise NVMeDeployArgumentError("Gateway group not provided..")

        if is_spec_or_mtls:
            cfg["config"]["specs"][0]["service_id"] = f"{rbd_pool}.{gw_group}"
            cfg["config"]["specs"][0]["spec"]["group"] = gw_group
        else:
            cfg["config"]["pos_args"].append(gw_group)
        return cfg


def deploy_nvme_service(ceph_cluster, config):
    """Deploy NVMe Service with apply or with spec

    Args:
        ceph_cluster: Ceph cluster object
        config: Test case config

    Test case config should have below important params,
    - rbd_pool
    - gw_nodes
    - gw_group      # optional, as per release
    - mtls          # optional
    """
    _cfg = apply_nvme_sdk_cli_support(ceph_cluster, config)
    test_nvmeof.run(ceph_cluster, **_cfg)


def delete_nvme_service(ceph_cluster, config):
    """Delete the NVMe gateway service.

    Args:
        ceph_cluster: Ceph cluster object
        config: Test case config

    Test case config should have below important params,
    - rbd_pool
    - gw_nodes
    - gw_group      # optional, as per release
    - mtls          # optional
    """
    gw_groups = config.get("gw_groups", [{"gw_group": config.get("gw_group", "")}])

    for gwgroup_config in gw_groups:
        gw_group = gwgroup_config["gw_group"]
        service_name = f"nvmeof.{config['rbd_pool']}"
        service_name = f"{service_name}.{gw_group}" if gw_group else service_name
        cfg = {
            "no_cluster_state": False,
            "config": {
                "command": "remove",
                "service": "nvmeof",
                "args": {
                    "service_name": service_name,
                    "verify": True,
                },
            },
        }
        test_nvmeof.run(ceph_cluster, **cfg)


def fetch_nvme_entity_in_omap(cluster, entity, pool, group=""):
    """NVMe Entity OMAP Validation."""
    err = None
    try:
        orch = Orch(cluster, **{})
        out, err = orch.shell(
            args=[
                f"rados -p {pool} getomapval nvmeof{f'.{group}' or str()}.state {entity} /tmp/out"
            ],
            base_cmd_args={"mount": "/tmp:/tmp"},
        )

        out, err = orch.installer.exec_command(cmd="cat /tmp/out")
        if out:
            LOG.info(f"{out}")
            return json.loads(out.strip())
        else:
            raise OMAPValidationFailure
    except Exception as e:
        LOG.error(f"Error : {e}\n{err}")
    return False


def validate_nvme_metadata(cluster, config, pool, group=""):
    """Validate configured NVMe entity against OMAP."""
    nvme_entt = config["service"]
    action = config["command"]
    deleted_entity = action == "delete"
    entity = f"{nvme_entt}_{config['args']['subsystem']}"

    if nvme_entt == "subsystem" and not deleted_entity:
        if not config.get("args", {}).get("no-group-append") and group not in entity:
            entity += f".{group}"

    elif nvme_entt == "host":
        host = config["args"]["host"]
        try:
            host = ast.literal_eval(host)
        except (ValueError, SyntaxError):
            pass
        entity += f"_{host}"

    elif nvme_entt == "listener":
        listener = get_node_by_id(cluster, config["args"]["host-name"])
        entity += f"_{listener.hostname}_TCP_{listener.ip_address}_{config['args']['trsvcid']}"

    elif nvme_entt == "namespace":
        if action == "set_qos":
            entity = f"qos_{config['args']['subsystem']}_{config['args']['nsid']}"
        else:
            entity += f"_{config['args']['nsid']}"

    out = fetch_nvme_entity_in_omap(cluster, entity, pool, group)

    # deleted_entity represents delete, If deleted_entity, output should be False
    if deleted_entity:
        if out:
            raise OMAPValidationFailure(
                f"{entity} is still exist in OMAP metadata even after delete."
            )
        LOG.info(
            f"[ OMAP VALIDATION SUCCESSFULL ] - {entity} deleted successfully from NVMeoF OMAP state file."
        )
        return True

    if not out:
        raise OMAPValidationFailure(
            f"{entity} Not Found in nvmeof state OMAP file.\n{out}."
        )

    LOG.info(
        f"[ OMAP VALIDATION SUCCESSFULL ] - {entity} Found in nvmeof state OMAP file.\n{out}."
    )
