import ast
import ipaddress
import json
import re
import time
from typing import Type, Union

from looseversion import LooseVersion
from packaging.version import Version

from ceph.ceph import Ceph, CommandFailed
from ceph.ceph_admin.orch import Orch
from ceph.nvmeof.cli.v1 import NVMeGWCLI
from ceph.nvmeof.cli.v2 import NVMeGWCLIV2
from ceph.parallel import parallel
from ceph.utils import get_node_by_id, get_nodes_by_ids
from tests.cephadm import test_nvmeof
from utility.log import Log
from utility.retry import retry
from utility.systemctl import SystemCtl
from utility.utils import log_json_dump

LOG = Log(__name__)


class NVMeDeployArgumentError(Exception):
    pass


class NVMeDeployConfigParamRequired(Exception):
    pass


class OMAPValidationFailure(Exception):
    pass


def get_nvme_service_id(pool, group=None):
    """Build orch ``service_id`` for an nvmeof apply_spec.

    Default metadata pool ``.nvmeof`` must keep its leading dot. Ceph names
    the service ``nvmeof.<service_id>``, so pool ``.nvmeof`` + group
    ``group1`` yields service_id ``.nvmeof.group1`` and full name
    ``nvmeof..nvmeof.group1`` (current product naming after the cephadm
    revert). Do not strip the leading dot.

    For redeploy/delete/lookups, prefer resolving the live service_id from
    ``ceph orch ls nvmeof --export`` instead of reconstructing from pool.
    """
    if group:
        return f"{pool}.{group}"
    return str(pool)


def get_nvme_service_name(pool, group=None):
    return f"nvmeof.{get_nvme_service_id(pool, group)}"


def fetch_nvme_service_from_orch(ceph_cluster, group=None):
    """Return ``(service_name, service_id)`` from ``ceph orch ls nvmeof --export``.

    Prefer this over constructing names from pool/group. When *group* is set,
    match the export entry whose ``service_id`` or ``spec.group`` contains it.
    """
    orch = Orch(ceph_cluster, **{})
    out, _ = orch.shell(
        args=["ceph orch ls nvmeof --export --format json"]
    )
    if not out or "No services reported" in out:
        raise RuntimeError("No nvmeof services reported by ceph orch ls --export")

    services = json.loads(out)
    if isinstance(services, dict):
        services = [services]

    matched = None
    for service in services:
        if service.get("service_type") != "nvmeof":
            continue
        service_id = service.get("service_id", "")
        spec_group = (service.get("spec") or {}).get("group")
        if group:
            if group == spec_group or (
                isinstance(service_id, str) and group in service_id
            ):
                matched = service
                break
        else:
            matched = service
            break

    if not matched:
        raise RuntimeError(
            "No nvmeof orch export entry found"
            + (f" for group '{group}'" if group else "")
        )

    service_id = matched.get("service_id")
    if not service_id:
        raise RuntimeError("nvmeof orch export entry missing service_id")

    # orch export carries service_id; full name is always service_type.service_id
    service_name = matched.get("service_name") or f"nvmeof.{service_id}"
    LOG.info(
        "Resolved nvmeof service from orch export: "
        f"service_name={service_name}, service_id={service_id}"
    )
    return service_name, service_id


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


def check_and_enable_nvmeof_module(**kwargs):
    """Check and enable NVMeoF module if not enabled."""
    ceph_cluster = kwargs.get("ceph_cluster")
    orch = Orch(ceph_cluster, **{})
    ceph_version = kwargs.get("ceph_version")
    if LooseVersion(ceph_version) >= LooseVersion("20.2.1"):
        LOG.info(
            f"Checking and enabling NVMeoF module for ceph version: {ceph_version}"
        )
        out, _ = orch.shell(args=["ceph", "mgr", "module", "ls", "--format", "json"])
        modules = json.loads(out)
        if "nvmeof" not in modules["enabled_modules"]:
            LOG.info(f"Enabling NVMeoF module for ceph version: {ceph_version}")
            out, _ = orch.shell(args=["ceph", "mgr", "module", "enable", "nvmeof"])
            out, _ = orch.shell(
                args=["ceph", "mgr", "module", "ls", "--format", "json"]
            )
            modules = json.loads(out)
            if "nvmeof" not in modules["enabled_modules"]:
                raise Exception(
                    f"Failed to enable NVMeoF module for ceph version: {ceph_version}"
                )
            LOG.info(
                f"NVMeoF module enabled successfully for ceph version: {ceph_version}"
            )
        else:
            LOG.info(f"NVMeoF module already enabled for ceph version: {ceph_version}")


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
                        "service_id": get_nvme_service_id(rbd_pool),
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
            cfg["config"]["specs"][0]["service_id"] = get_nvme_service_id(
                rbd_pool, gw_group
            )
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
        service_name = get_nvme_service_name(config["rbd_pool"], gw_group or None)
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


def string_to_dict(string):
    """Parse ANA states from the string."""
    states = string.replace(" ", "").split(",")
    dict = {}
    for state in states:
        if not state:
            continue
        _id, _state = state.split(":")
        dict[int(_id)] = _state
    return dict


def catogorize(nvme_service, gws):
    """Categorize to-be failed and running GWs.

    Args:
        all_gws: all gateways
        gws: gateways to be failed/stopped/scaled-down

    Returns:
        list of,
            - to-be failed gateways
            - rest of the gateways
    """
    fail_gws = []
    running_gws = []

    # collect impending Gateways to be failed.
    if isinstance(gws, str):
        gws = [gws]
    for gw_id in gws:
        fail_gws.append(check_gateway(nvme_service.gateways, gw_id))

    # Collect rest of the Gateways
    for gw in nvme_service.gateways:
        if gw.node.id not in gws:
            running_gws.append(gw)

    return fail_gws, running_gws


def ana_states(nvme_service, orch, gw_group=""):
    """Fetch ANA states and convert into python dict."""

    # For 7.1 ceph version group name is not required
    group_name = repr(nvme_service.group)
    if nvme_service.ceph_cluster.rhcs_version == "7.1":
        group_name = repr("")

    out, _ = orch.shell(
        args=[
            "ceph",
            "nvme-gw",
            "show",
            nvme_service.nvme_metadata_pool,
            group_name,
        ]
    )
    states = {}
    if nvme_service.ceph_cluster.rhcs_version >= "8":
        out = json.loads(out)
        for gateway in out.get("Created Gateways:"):
            gw = gateway["gw-id"]
            states[gw] = gateway
            states[gw].update(string_to_dict(gateway["ana states"]))
    else:
        for data in out.split("}"):
            data = data.strip()
            if not data:
                continue
            data = json.loads(f"{data}}}")
            if data.get("ana states"):
                gw = data["gw-id"]
                states[gw] = data
                states[gw].update(string_to_dict(data["ana states"]))

    return states


def check_gateway_availability(
    nvme_service, ana_id, orch, state="AVAILABLE", anastates=None
):
    """Check for failed ANA GW become unavailable.

    Args:
        ana_id: Gateway ANA group id.
        state: Gateway availability state
        ana_states: Overall ana state. (output from self.ana_states)
    Return:
        True if Gateway availability is in expected state, else False
    """
    # get ANA states
    if not anastates:
        anastates = ana_states(nvme_service, orch)

    # Check Availability of ANA Group Gateway
    for _, _state in anastates.items():
        if _state["anagrp-id"] == ana_id:
            if _state["Availability"] == state:
                return True
            return False
    return False


def check_gateway(gateways, node_id):
    """Check node is NVMeoF Gateway node.

    Args:
        node_id: Ceph node Id (ex., node6)
    """
    for gw in gateways:
        if gw.node.id == node_id:
            LOG.info(f"[{node_id}] {gw.node.hostname} is NVMeoF Gateway node.")
            return gw
    raise Exception(f"{node_id} doesn't match to any gateways provided...")


def get_optimized_state(nvme_service, orch, failed_ana_id):
    """Fetch the Optimized ANA states for failed gateway.

    Args:
        gateway: The gateway which is operational.
        failed_ana_id: failed gateway ANA Group Id.

    Returns:
        gateways which shows ACTIVE state for failed ANA Group Id
    """
    # get ANA states
    anastates = ana_states(nvme_service, orch)

    # Fetch failed ANA Group Id in ACTIVE state
    found = []

    for ana_gw_id, state in anastates.items():
        if (
            state["Availability"] == "AVAILABLE"
            and state.get(failed_ana_id) == "ACTIVE"
        ):
            found.append({ana_gw_id: state})

    return found


@retry((IOError, TimeoutError, CommandFailed), tries=7, delay=2)
def validate_io(orch, namespaces, negative=False):
    """Validate Continuous IO on namespaces.

    - Collect rbd disk usage info for each rbd image.
    - Validate written bytes value is incremental.

    Args:
        namespaces: list of namespaces
    """

    def io_value(ns):
        sub_ns, pool, image = ns.rsplit("|", 2)
        # Handle both {pool}/{image} and {pool}/{namespace}/{image} formats
        rbd_path = f"{pool}/{image}"
        count = 3
        samples = []
        for _ in range(count):
            out, _ = orch.shell(args=[f"rbd --format json du {rbd_path}"], timeout=600)
            out = json.loads(out)["images"][0]
            samples.append(out)
            time.sleep(6)
        return sub_ns, rbd_path, samples

    def validate_incremetal_io(write_samples):
        for i in range(len(write_samples) - 1):
            if write_samples[i] >= write_samples[i + 1]:
                return False
        return True

    with parallel() as p:
        for namespace in namespaces:
            p.spawn(io_value, namespace)

        for result in p:
            subsys, pool_img, samples = result
            res = [i["used_size"] for i in samples]

            LOG.info(
                f"[ {subsys}|{pool_img} ] RBD DU Detailed - {log_json_dump(samples)}"
            )
            LOG.info(f"[ {subsys}|{pool_img} ] RBD DU samples - {res}")
            if not validate_incremetal_io(res):
                if negative:
                    LOG.info(
                        f"[ {subsys}|{pool_img} ] IO is not progressing as expected - {res}"
                    )
                    continue
                raise IOError(f"[ {subsys}|{pool_img} ] IO is not progressing - {res}")
            if negative:
                LOG.error(
                    f"[ {subsys}|{pool_img} ] IO is progressing as expected - {res}"
                )
                raise IOError(
                    f"[ {subsys}|{pool_img} ] IO is progressing as expected - {res}"
                )
            LOG.info(f"IO validation for {subsys}|{pool_img} is successful.")

    LOG.info("IO Validation is Successfull on all RBD images..")


def fetch_lb_groups(gateways, nodes):
    """Fetch Load balancing group ids for given nodes."""
    lb_group_ids = {}
    for node in nodes:
        nvmegwcli = check_gateway(gateways, node)
        hostname = nvmegwcli.fetch_gateway_hostname()
        lb_group_ids.update({hostname: nvmegwcli.ana_group_id})
    return lb_group_ids


def get_minimal_network_mask(ips):
    """
    Derive the tightest IPv4 network mask that covers all given IPs.

    Uses XOR of min/max addresses to find the minimal prefix length.
    """
    if not ips:
        return None

    addrs = [ipaddress.ip_address(ip) for ip in ips]
    ip_ints = [int(ip) for ip in addrs]
    min_ip = min(ip_ints)
    max_ip = max(ip_ints)
    diff = min_ip ^ max_ip  # XOR min & max → differing bits → prefix length
    prefix_len = 32 - diff.bit_length()

    network = ipaddress.ip_network(f"{addrs[0]}/{prefix_len}", strict=False)
    return str(network)


def get_network_mask(gateways):
    """Derive minimal network-mask from gateway primary IPs."""
    ips = []
    for gateway in gateways:
        gw_ip = getattr(gateway.node, "ip_address", None)
        if gw_ip:
            ips.append(gw_ip)
    return get_minimal_network_mask(ips)


def list_listeners(gateway, nqn):
    """Return listener dicts for a subsystem."""
    args = {"base_cmd_args": {"format": "json"}, "args": {"subsystem": nqn}}
    out, _ = gateway.listener.list(**args)
    if not out:
        return []
    data = json.loads(out)
    return data.get("listeners", [])


def get_listener_traddrs(gateway, nqn):
    """Return sorted listener traddrs for a subsystem."""
    return sorted(
        {
            listener.get("traddr")
            for listener in list_listeners(gateway, nqn)
            if listener.get("traddr")
        }
    )


def get_subsystem_network_masks(gateway, nqn):
    """Return network_mask list configured on a subsystem."""
    args = {"base_cmd_args": {"format": "json"}, "args": {"subsystem": nqn}}
    out, _ = gateway.subsystem.list(**args)
    if not out:
        return []
    data = json.loads(out)
    for subsystem in data.get("subsystems", []):
        if subsystem.get("nqn") == nqn:
            masks = subsystem.get("network_mask") or []
            if isinstance(masks, str):
                return [masks] if masks else []
            return list(masks)
    raise ValueError(f"Subsystem {nqn} not found while fetching network masks")


def get_ipv4_on_interface(node, iface):
    """Return the first IPv4 address configured on iface, or None."""
    out, _ = node.exec_command(
        cmd=f"ip -o -4 addr show dev {iface} | awk '{{print $4}}'",
        sudo=True,
        check_ec=False,
    )
    line = (out or "").strip().splitlines()
    if not line:
        return None
    return line[0].split("/")[0]


def list_iface_ipv4_cidrs(node, iface):
    """Return IPv4 CIDRs currently configured on ``iface`` (e.g. ['10.64.94.1/27'])."""
    out, _ = node.exec_command(
        cmd=f"ip -o -4 addr show dev {iface} | awk '{{print $4}}'",
        sudo=True,
        check_ec=False,
    )
    return [line.strip() for line in (out or "").strip().splitlines() if line.strip()]


def _nmcli_available(node):
    out, _ = node.exec_command(cmd="command -v nmcli", check_ec=False)
    return bool((out or "").strip())


def interface_exists(node, iface):
    """Return True if network device ``iface`` is present (even if DOWN)."""
    out, err = node.exec_command(
        cmd=f"ip link show dev {iface}",
        sudo=True,
        check_ec=False,
    )
    text = f"{out or ''}{err or ''}".lower()
    if "does not exist" in text or "cannot find device" in text:
        return False
    # ip link show prints the iface name on success
    return iface in (out or "")


def _ensure_interface_present(node, iface):
    """
    Ensure ``iface`` exists. Never use ``nmcli device disconnect`` — that can
    delete VLAN/virtual devices (e.g. virtual@eno12399np0).

    If the device is missing, try bringing an NM connection profile back up.
    """
    if interface_exists(node, iface):
        return

    LOG.warning(
        f"[{node.hostname}] iface {iface} is missing; attempting NM connection restore"
    )
    if _nmcli_available(node):
        # Prefer connection profile named like the iface, then any profile bound to it
        for cmd in (
            f"nmcli connection up id {iface}",
            f"nmcli connection up {iface}",
            f"nmcli -g NAME,DEVICE connection show | awk -F: -v d={iface} '$2==d{{print $1; exit}}' "
            f"| xargs -r nmcli connection up id",
        ):
            node.exec_command(cmd=cmd, sudo=True, check_ec=False)
            if interface_exists(node, iface):
                LOG.info(f"[{node.hostname}] restored iface {iface} via nmcli")
                return

    raise RuntimeError(
        f"[{node.hostname}] secondary iface {iface} no longer exists. "
        f"Do not use nmcli device disconnect / ip link delete on VLAN/virtual "
        f"NICs — recreate the iface (e.g. nmcli connection up <profile> or "
        f"re-add the VLAN on the parent), then re-run the test."
    )


def take_interface_ipv4s_down(node, iface, saved_cidrs=None):
    """
    Remove IPv4 addresses from ``iface`` only — keep the device itself.

    Intentionally avoids ``nmcli device disconnect`` and ``ip link set down``,
    which can destroy VLAN/virtual interfaces (lab: virtual@parent disappears).
    """
    if not interface_exists(node, iface):
        raise RuntimeError(
            f"[{node.hostname}] cannot remove IPv4s: iface {iface} does not exist"
        )

    cidrs = (
        list(saved_cidrs)
        if saved_cidrs is not None
        else list_iface_ipv4_cidrs(node, iface)
    )
    LOG.info(
        f"[{node.hostname}] removing IPv4s from {iface} (keep device; cidrs={cidrs})"
    )
    for cidr in cidrs:
        node.exec_command(
            cmd=f"ip addr del {cidr} dev {iface}",
            sudo=True,
            check_ec=False,
        )
    # Safety: clear any remaining IPv4s without deleting the link
    node.exec_command(cmd=f"ip -4 addr flush dev {iface}", sudo=True, check_ec=False)

    if not interface_exists(node, iface):
        raise RuntimeError(
            f"[{node.hostname}] iface {iface} disappeared after IPv4 removal — "
            f"unexpected; check lab networking"
        )

    remaining = list_iface_ipv4_cidrs(node, iface)
    if remaining:
        raise RuntimeError(
            f"[{node.hostname}] expected no IPv4 on {iface} after addr del; still {remaining}"
        )
    return cidrs


def bring_interface_ipv4s_up(node, iface, cidrs, timeout=60):
    """
    Re-apply saved IPv4 CIDRs on ``iface`` (device must already exist or be
    restorable via NM connection profile).
    """
    if not cidrs:
        raise ValueError(f"[{node.hostname}] no CIDRs to restore on {iface}")

    LOG.info(f"[{node.hostname}] restoring IPv4s on {iface} (cidrs={cidrs})")
    _ensure_interface_present(node, iface)
    node.exec_command(cmd=f"ip link set {iface} up", sudo=True, check_ec=False)

    present = set(list_iface_ipv4_cidrs(node, iface))
    for cidr in cidrs:
        if cidr not in present:
            node.exec_command(
                cmd=f"ip addr add {cidr} dev {iface}",
                sudo=True,
                check_ec=False,
            )

    deadline = time.time() + timeout
    while time.time() < deadline:
        present_ips = {c.split("/")[0] for c in list_iface_ipv4_cidrs(node, iface)}
        expected_ips = {c.split("/")[0] for c in cidrs}
        if expected_ips.issubset(present_ips):
            LOG.info(f"[{node.hostname}] {iface} IPv4s restored: {sorted(present_ips)}")
            return
        time.sleep(2)

    raise RuntimeError(
        f"[{node.hostname}] timed out waiting for {iface} IPv4s {cidrs}; "
        f"have {list_iface_ipv4_cidrs(node, iface)}; "
        f"iface_exists={interface_exists(node, iface)}"
    )


def secondary_iface_state_on_gateways(gateways, networks, state):
    """
    Apply secondary IPv4 remove/restore across all gateways.

    ``networks`` is the dict from ``discover_gateway_network_roles``.
    For ``state='down'``, stores saved CIDRs on each per_gateway entry under
    ``secondary_cidrs``. For ``state='up'``, restores from that.
    """
    if state not in ("up", "down"):
        raise ValueError(f"state must be 'up' or 'down', got {state}")

    for gw in gateways:
        host = gw.node.hostname
        info = networks["per_gateway"][host]
        secondary = info.get("secondary") or {}
        iface = secondary.get("iface")
        if not iface:
            raise RuntimeError(f"[{host}] no secondary iface in discovered roles")

        if state == "down":
            cidrs = take_interface_ipv4s_down(
                gw.node, iface, saved_cidrs=info.get("secondary_cidrs")
            )
            info["secondary_cidrs"] = cidrs
        else:
            cidrs = info.get("secondary_cidrs")
            if not cidrs:
                # Fall back to discovered secondary CIDR
                cidr = secondary.get("cidr")
                cidrs = [cidr] if cidr else []
            bring_interface_ipv4s_up(gw.node, iface, cidrs)


def list_node_ipv4_addrs(node):
    """
    Discover non-loopback IPv4 addresses on a node via ``ip -o -4 addr``.

    Returns:
        list[dict]: [{"iface": "eno8303", "ip": "10.64.0.192", "prefix": "23"}, ...]
    """
    out, _ = node.exec_command(
        cmd="ip -o -4 addr show | awk '{print $2,$4}'",
        sudo=True,
    )
    addrs = []
    seen = set()
    for line in (out or "").strip().splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        iface, cidr = parts[0], parts[1]
        iface = iface.split("@")[0]
        if iface == "lo" or iface.startswith("lo:"):
            continue
        ip = cidr.split("/")[0]
        prefix = cidr.split("/")[1] if "/" in cidr else None
        key = (iface, ip)
        if key in seen:
            continue
        seen.add(key)
        addrs.append({"iface": iface, "ip": ip, "prefix": prefix, "cidr": cidr})
    return addrs


def classify_gateway_ipv4s(node, primary_ip=None):
    """
    Split node IPv4s into GW-primary vs secondary.

    Primary = interface hosting ``primary_ip`` (defaults to node.ip_address).
    Secondary = another IPv4-bearing interface (minimum requirement: 2 IPv4 ifaces).

    Returns:
        dict with keys primary, secondaries (list), all
    """
    primary_ip = primary_ip or getattr(node, "ip_address", None)
    addrs = list_node_ipv4_addrs(node)
    ifaces_with_ip = {}
    for entry in addrs:
        ifaces_with_ip.setdefault(entry["iface"], []).append(entry)

    if len(ifaces_with_ip) < 2:
        raise RuntimeError(
            f"{node.hostname} needs at least 2 IPv4 interfaces for refresh-network "
            f"E2E; found {sorted(ifaces_with_ip.keys()) or 'none'}"
        )

    primary = None
    for entry in addrs:
        if primary_ip and entry["ip"] == primary_ip:
            primary = entry
            break
    if not primary:
        # Fall back to first iface if primary_ip not found on any iface
        first_iface = sorted(ifaces_with_ip.keys())[0]
        primary = ifaces_with_ip[first_iface][0]
        LOG.warning(
            f"[{node.hostname}] primary IP {primary_ip} not found in ip a; "
            f"using {primary['iface']}/{primary['ip']}"
        )

    secondaries = [
        entry
        for entry in addrs
        if entry["iface"] != primary["iface"] and entry["ip"] != primary["ip"]
    ]
    if not secondaries:
        raise RuntimeError(
            f"{node.hostname}: no secondary IPv4 found besides primary "
            f"{primary['iface']}/{primary['ip']}"
        )

    return {"primary": primary, "secondaries": secondaries, "all": addrs}


def discover_gateway_network_roles(gateways, secondary_iface=None):
    """
    Discover primary/secondary IPv4 roles across gateway nodes.

    - Primary IPs: each GW's hosted address (node.ip_address / matching iface)
    - Secondary IPs: IPv4s on a non-primary iface
      Prefer a common secondary iface name present on all GWs; otherwise use
      the first secondary IPv4 on each GW. Optional ``secondary_iface`` forces
      the iface name.

    Returns:
        dict:
          primary_ips, secondary_ips, primary_mask, secondary_mask,
          per_gateway (hostname -> {primary, secondary})
    """
    per_gateway = {}
    secondary_iface_candidates = None

    for gw in gateways:
        classified = classify_gateway_ipv4s(gw.node)
        per_gateway[gw.node.hostname] = {
            "primary": classified["primary"],
            "secondaries": classified["secondaries"],
        }
        names = {s["iface"] for s in classified["secondaries"]}
        secondary_iface_candidates = (
            names
            if secondary_iface_candidates is None
            else secondary_iface_candidates & names
        )
        LOG.info(
            f"[{gw.node.hostname}] ip a IPv4 roles: primary="
            f"{classified['primary']['iface']}/{classified['primary']['ip']}, "
            f"secondaries="
            f"{[(s['iface'], s['ip']) for s in classified['secondaries']]}"
        )

    if secondary_iface:
        chosen_secondary_iface = secondary_iface
    elif secondary_iface_candidates:
        chosen_secondary_iface = sorted(secondary_iface_candidates)[0]
    else:
        chosen_secondary_iface = None

    primary_ips = []
    secondary_ips = []
    secondary_map = {}

    for gw in gateways:
        host = gw.node.hostname
        info = per_gateway[host]
        primary_ips.append(info["primary"]["ip"])

        secondary = None
        if chosen_secondary_iface:
            for entry in info["secondaries"]:
                if entry["iface"] == chosen_secondary_iface:
                    secondary = entry
                    break
            if not secondary:
                raise RuntimeError(
                    f"{host}: configured/chosen secondary iface "
                    f"'{chosen_secondary_iface}' has no IPv4"
                )
        else:
            secondary = info["secondaries"][0]

        secondary_map[host] = secondary
        secondary_ips.append(secondary["ip"])
        per_gateway[host]["secondary"] = secondary

    primary_mask = get_minimal_network_mask(primary_ips)
    secondary_mask = get_minimal_network_mask(secondary_ips)

    LOG.info(
        f"Discovered GW networks: primary_mask={primary_mask} "
        f"from {sorted(set(primary_ips))}; secondary_mask={secondary_mask} "
        f"from {sorted(set(secondary_ips))} "
        f"(secondary_iface={chosen_secondary_iface or 'per-node first secondary'})"
    )

    return {
        "primary_ips": sorted(set(primary_ips)),
        "secondary_ips": sorted(set(secondary_ips)),
        "primary_mask": primary_mask,
        "secondary_mask": secondary_mask,
        "secondary_iface": chosen_secondary_iface,
        "per_gateway": per_gateway,
    }


def refresh_gateway_network(gateway, nqn):
    """
    Run gateway refresh_network for a subsystem and return parsed JSON status.

    Command: ceph nvmeof gateway refresh_network --subsystem <nqn>
    """
    args = {
        "base_cmd_args": {"format": "json"},
        "args": {"subsystem": nqn},
    }
    out, err = gateway.gateway.refresh_network(**args)
    LOG.info(
        f"[{gateway.node.hostname}] refresh_network for {nqn}: out={out}, err={err}"
    )
    if not out:
        return {"status": 0, "added": [], "removed": [], "raw": out}
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        # Plain-text success path
        return {
            "status": 0 if "Successful" in (out or "") else 1,
            "added": [],
            "removed": [],
            "raw": out,
        }
