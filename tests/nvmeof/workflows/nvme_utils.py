import ast
import json
import re
import time
from typing import Type, Union

from packaging.version import Version

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from ceph.nvmeof.cli.v1 import NVMeGWCLI
from ceph.nvmeof.cli.v2 import NVMeGWCLIV2
from ceph.utils import get_node_by_id, get_nodes_by_ids
from tests.cephadm import test_nvmeof
from utility.log import Log
from utility.systemctl import SystemCtl

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


def setup_firewalld(nodes) -> None:
    """Setup firewalld service.

    Important:
        Currently NVMe GW nodes 4420, 8009, 5500, 9100 TCP ports would be
        opened on Gateway deployment, So any other listener ports like 5001
        will be blocked (Meaning node listens on that port,
          but firewall doesn't allow port).
        Hence this method would opening up the ports from 5000-6000
        for testing purpose.

        Basically this is not limitation from product side, but ensuring
        test cases run smoothly.

        In case expanding the port range, please update this defintion and
        port range accordingly.

        If firewalld is not active, do nothing in order to honor the
        objective of the use-case.

    Args:
        nodes: List of GW nodes
    """
    port_range = "5000-6000"
    firewalld = "firewalld"
    firewalld_cmds = [
        f"firewall-cmd --permanent --add-port={port_range}/tcp",
        "firewall-cmd --reload",
    ]

    for node in nodes:
        if not SystemCtl(node).is_active(firewalld):
            LOG.info("Firewalld is disabled or not Active.")
            continue

        for cmd in firewalld_cmds:
            node.exec_command(cmd=cmd, sudo=True)
        LOG.info("Configured firewalld to allow port range: %s", port_range)


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

    # Open up firewall ports if running.
    setup_firewalld(gw_nodes)

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
    elif release >= "8":
        if not gw_group:
            raise NVMeDeployArgumentError("Gateway group not provided..")

        if is_spec_or_mtls:
            cfg["config"]["specs"][0]["service_id"] = f"{rbd_pool}.{gw_group}"
            cfg["config"]["specs"][0]["spec"]["group"] = gw_group
        else:
            cfg["config"]["pos_args"].append(gw_group)

        if config.get("rebalance_period", False):
            cfg["config"]["specs"][0]["spec"]["rebalance_period_sec"] = config.get(
                "rebalance_period_sec"
            )
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
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")
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


def validate_qos(client, device, **kw):

    bandwidth = {"mb_read/s": [], "mb_write/s": [], "mb_r/s": [], "mb_w/s": []}
    try:
        client.exec_command(cmd="dnf install -y sysstat", sudo=True, long_running=True)
        for _ in range(3):
            out, _, _, _ = client.exec_command(
                cmd="iostat -m -dx 5 1", sudo=True, verbose=True
            )

            lines = out.strip().split("\n")
            found_header = False

            for line in lines:

                # Identify the headers row
                if "Device" in line and "rMB/s" in line and "wMB/s" in line:
                    found_header = True
                    continue

                if found_header:
                    parts = line.split()
                    if len(parts) >= 6 and parts[0] == device:
                        mb_read = float(parts[2])  # MB_read/s
                        mb_write = float(parts[8])  # MB_wrtn/s
                        mb_write_iops = float(parts[7])  # MB_w/s
                        mb_read_iops = float(parts[1])  # MB_rs

                        bandwidth["mb_read/s"].append(mb_read)
                        bandwidth["mb_write/s"].append(mb_write)
                        bandwidth["mb_r/s"].append(mb_write_iops)
                        bandwidth["mb_w/s"].append(mb_read_iops)
                        break

            time.sleep(5)

        if "r-megabytes-per-second" in kw:
            limit = float(kw["r-megabytes-per-second"])
            if all(r < limit for r in bandwidth["mb_read/s"]):
                print(
                    f"QoS validated for {device}: Read values {bandwidth['mb_read/s']} "
                    f"are below {kw['r-megabytes-per-second']} MB/s."
                )
            else:
                raise Exception(
                    f"QoS validation failed for {device}: Read values {bandwidth['mb_read/s']} "
                    f"exceed {kw['r-megabytes-per-second']} MB/s at least once."
                )

        if "w-megabytes-per-second" in kw:
            limit = float(kw["w-megabytes-per-second"])

            if all(w < limit for w in bandwidth["mb_write/s"]):
                print(
                    f"QoS validated for {device}: Write values {bandwidth['mb_write/s']} "
                    f"are below {limit} MB/s."
                )
            else:
                raise Exception(
                    f"QoS validation failed for {device}: Write values {bandwidth['mb_write/s']} "
                    f"exceed {limit} MB/s at least once."
                )

        if "rw-megabytes-per-second" in kw:
            max_rw_mb = kw["rw-megabytes-per-second"]
            read_bw = bandwidth["mb_read/s"]
            write_bw = bandwidth["mb_write/s"]

            # Check if both read and write bandwidths are below the specified limit
            if all(r < max_rw_mb for r in read_bw) and all(
                w < max_rw_mb for w in write_bw
            ):
                print(
                    f"QoS validated for {device}: Read values {read_bw} and Write values {write_bw} "
                    f"are below {max_rw_mb} MB/s."
                )
            else:
                raise Exception(
                    f"QoS validation failed for {device}: At least one of the Read or Write values "
                    f"exceeds {max_rw_mb} MB/s. Read values: {read_bw}, Write values: {write_bw}."
                )

        if "rw-ios-per-second" in kw:
            max_rw_mb = kw["rw-ios-per-second"]
            total_bw = [r + w for r, w in zip(bandwidth["mb_r/s"], bandwidth["mb_w/s"])]
            if all(rw < max_rw_mb for rw in total_bw):
                print(
                    f"QoS validated for {device}: Read+Write values {total_bw} "
                    f"are below {kw['rw-ios-per-second']} MB/s."
                )
            else:
                raise Exception(
                    f"QoS validation failed for {device}: Read+Write values {total_bw} "
                    f"exceed {kw['rw-ios-per-second']} MB/s at least once."
                )

    except Exception as e:
        print(f"Error: {e}")
        raise e


def verify_qos(expected_config, nvmegwcli):
    subnqn = expected_config.pop("subsystem")
    nsid = expected_config.pop("nsid")
    _config = {
        "base_cmd_args": {"format": "json"},
        "args": {"subsystem": subnqn, "nsid": nsid},
    }
    namespace, _ = nvmegwcli.namespace.list(**_config)
    namespace_data = json.loads(namespace)["namespaces"][0]

    def transform_rw_ios(value):
        quotient = value // 1000
        if value % 1000 == 0:
            return value
        transformed_quotient = quotient + 1
        return transformed_quotient * 1000

    for key, expected_value in expected_config.items():
        actual_value = namespace_data.get(
            key.replace("-", "_").replace("megabytes", "mbytes"), ""
        )
        if key == "rw-ios-per-second":
            expected_value = transform_rw_ios(expected_value)
        if int(actual_value) != int(expected_value):
            raise Exception(
                f"QoS verification failed for {key}: Expected {expected_value}, got {actual_value}"
            )

    LOG.info("Verification of QoS values is successful")


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


def nvme_gw_cli_version_adapter(
    ceph: Ceph,
) -> Union[Type[NVMeGWCLI], Type[NVMeGWCLIV2]]:
    """Select the appropriate NVMe Gateway CLI obj based on the Ceph version.

    This function determines which NVMe Gateway CLI implementation to use
    depending on the Ceph version number(upstream, basically which starts from 20.x.x).
    It ensures that commands are executed with the correct CLI for compatibility
    with the target gateway.

    Args:
        ceph (Ceph): CephCI Ceph object

    Returns:
        type[NVMeGWCLI] | type[NVMeGWCLIV2]: CLI class (not an instance).
    """
    out, _ = Orch(ceph, **{}).shell(args=["ceph", "--format", "json", "version"])

    match = re.search(r"[0-9]+(\.[0-9]+)*", out)
    if not match:
        raise RuntimeError("Ceph version not found.")

    version = Version(match.group())
    return NVMeGWCLIV2 if version.major >= 20 else NVMeGWCLI


def check_and_set_nvme_cli_image(
    ceph: Ceph, image: str = "", config: list = []
) -> None:
    """Set CLI image on NVMeGWCLI Version1."""
    version = nvme_gw_cli_version_adapter(ceph)
    if version is NVMeGWCLIV2:
        return

    if not (image or config):
        raise RuntimeError(
            "NVMe CLI image not provided. user --custom-config to provide CLI image"
        )
    if image:
        NVMeGWCLI.NVMEOF_CLI_IMAGE = image
    elif config:
        for key, value in dict(item.split("=") for item in config).items():
            if key == "nvmeof_cli_image":
                NVMeGWCLI.NVMEOF_CLI_IMAGE = value
                break
