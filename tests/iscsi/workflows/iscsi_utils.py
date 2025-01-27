import json
import re
import time
from datetime import datetime

from ceph.ceph_admin.orch import Orch
from ceph.utils import get_node_by_id
from tests.cephadm import test_iscsi
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


class GWCLIConfigError(Exception):
    pass


def deploy_iscsi_service(ceph_cluster, config):
    """Deploy iSCSI Service with apply or with spec

    Args:
        ceph_cluster: Ceph cluster object
        config: Test case config
    """
    trusted_ip_list = config.get("trusted_ip_list", [])
    orch = Orch(ceph_cluster, config={})
    if not trusted_ip_list:
        orch = Orch(ceph_cluster, config={})
        ps_args = {
            "base_cmd_args": {"format": "json"},
            "args": {
                "daemon_type": "mgr",
                "refresh": True,
            },
        }
        out, _ = orch.ps(ps_args)
        out = json.loads(out)
        for mgr in out:
            trusted_ip_list.append(
                get_node_by_id(ceph_cluster, mgr["hostname"]).ip_address
            )

    _cfg = {
        "command": "apply_spec",
        "validate-spec-services": True,
        "specs": [
            {
                "service_type": "iscsi",
                "service_id": config["rbd_pool"],
                "placement": {
                    "nodes": [
                        get_node_by_id(ceph_cluster, i).hostname
                        for i in config["gw_nodes"]
                    ]
                },
                "spec": {
                    "pool": config["rbd_pool"],
                    "trusted_ip_list": ",".join(trusted_ip_list),
                },
            }
        ],
    }
    orch.apply_spec(_cfg)


def delete_iscsi_service(ceph_cluster, config):
    """Delete the iSCSI gateway service.

    Args:
        ceph_cluster: Ceph cluster object
        config: Test case config
    """
    service_name = f"iscsi.{config['rbd_pool']}"
    cfg = {
        "no_cluster_state": False,
        "config": {
            "command": "remove",
            "service": "iscsi",
            "args": {
                "service_name": service_name,
                "verify": True,
            },
        },
    }
    test_iscsi.run(ceph_cluster, **cfg)


def generate_iqn(domain=None, target=None):
    """Create IQN.

    Args:
        domain: IQN domain eg., "com.rh-ibm.iscsi-gw"
        target: IQN target name
    """
    if not target:
        target = f"target-{generate_unique_id(length=3)}"
    if not domain:
        domain = "com.rh-ibm.iscsi-gw"
    current_time = datetime.now().strftime(f"%Y-%m.{int(time.time())}").lower()
    return f"iqn.{current_time}.{domain}:{target}"


def map_iqn_to_config(config):
    """Map iscsi target IQNs on the config."""

    for idx, target in enumerate(config["targets"]):
        target["iqn"] = generate_iqn(
            domain=target.get("domain", None),
            target=target.get("iqn", None),
        )
        config["targets"][idx] = target
    return config


def fetch_tcmu_devices(initiator):
    """Fetch TCMU device disks.

    Note: This definition works only when iscsi targets
          are already connected.

    Args:
        initiator: Initiator Node
    """
    cmd = "lsblk -po name,wwn,serial,size,model --json"
    devices, _ = initiator.exec_command(cmd=cmd, sudo=True)
    devices = json.loads(devices)

    _devices = {}
    for dev in devices["blockdevices"]:
        _model = dev.get("model")
        if not _model or "tcmu" not in _model.lower():
            continue

        uuid = dev["serial"]
        if uuid not in _devices:
            _devices[uuid] = {
                "wwn": dev["wwn"],
                "size": dev["size"],
                "model": dev["model"],
                "mapper_path": dev["children"][0]["name"],
                "paths": [dev["name"]],
            }
        else:
            _devices[uuid]["paths"].append(dev["name"])

    return _devices


def fetch_multipath_devices(mpath_obj):
    """Fetch Mutlipath devices.

    This works well with multipath list with verbose level '2'.

    Args:
        mpath_obj: mpath object from node
    """
    mappers = mpath_obj.list_by_level()
    multipath_dict = []

    # Regex patterns to match the lines for multipath device, paths, etc.
    multipath_pattern = re.compile(r"^(\S+) \((.*?)\) dm-(\d+) +(.+)")
    size_pattern = re.compile(r"size=(\S+)")
    mapper_pattern = re.compile(r"mapper: (/dev/mapper/\S+)")

    current_device = None

    # Split the output into lines and process each line
    for line in mappers.splitlines():
        # Match multipath device details
        multipath_match = multipath_pattern.match(line)
        if multipath_match:
            device_name = multipath_match.group(1)
            wwn = multipath_match.group(2)
            dm_device = multipath_match.group(3)
            model = multipath_match.group(4)

            # Initialize device dictionary
            current_device = {
                "device_name": device_name,
                "wwn": wwn,
                "dm_device": dm_device,
                "model": model,
                "size": None,  # Size will be populated later
                "mapper_path": None,  # Mapper path will be populated later
                "paths": [],
            }
            multipath_dict.append(current_device)
            continue

        # Match the device size
        size_match = size_pattern.match(line)
        if size_match and current_device:
            current_device["size"] = size_match.group(1)

        # Match the full mapper path
        mapper_match = mapper_pattern.match(line)
        if mapper_match and current_device:
            current_device["mapper_path"] = mapper_match.group(1)
            continue

    if not multipath_dict:
        raise Exception("No device mappers found..")

    return multipath_dict


def format_iscsiadm_output(output):
    """Format Output to dict based on the output mode response.

    Args:
        mode: mode of operation
        output: output data
    """
    targets = dict()
    for line in output.splitlines():
        if line.strip():
            # Extract portal, connect-id, and target
            temp, target = line.split()

            # Split the portal into IP address and port
            temp, connect_id = temp.split(",")
            ip_address, port = temp.split(":")

            # Create a dictionary for each target and append it to the list
            target_info = {
                "portal": ip_address,
                "port": port,
                "session-id": connect_id.strip(","),
                "target": target,
            }
            if target in targets:
                targets[target].append(target_info)
            else:
                targets[target] = list()
                targets[target].append(target_info)

    return targets


def validate_gwcli_configuration(ceph_cluster, gwcli_node, config):
    """Validate all entities existence using config export option."""
    export_cfg, _ = gwcli_node.gwcli.export()
    LOG.info(export_cfg)
    export_cfg = json.loads(export_cfg)

    # validate the iSCSI Targets
    iqn = config["iqn"]
    if iqn not in export_cfg["targets"]:
        raise GWCLIConfigError(f"{iqn} Target Not found")
    target = export_cfg["targets"][iqn]
    LOG.info(f"[{iqn}] Target has created succesfully... {target}")

    # Validate the iSCSI gateway portals
    if config.get("gateways"):
        for gw in config["gateways"]:
            node = get_node_by_id(ceph_cluster, gw)

            if node.hostname not in target["portals"]:
                raise GWCLIConfigError(
                    f"[{iqn}]: {node.hostname} Host not found in target"
                )
            LOG.info(
                f"[{iqn}] Gateway {gw} created successfully.. {target['portals'][node.hostname]}"
            )

    # Validate the iSCSI target hosts
    for host in config["hosts"]:
        client_iqn = host["client_iqn"]
        if client_iqn not in target["clients"]:
            raise GWCLIConfigError(f"[{iqn}]: {client_iqn} Host not found in target")
        LOG.info(
            f"[{iqn}] - {client_iqn} Host created sucessfully - {target['clients'][client_iqn]}"
        )

        if host.get("disks"):
            disks = config.get("disks")
            _disks = sorted(disks[client_iqn])
            host_luns = target["clients"][client_iqn]["luns"]

            if _disks != sorted(host_luns):
                raise GWCLIConfigError(
                    f"[{iqn}]: Disks didn't match {host_luns} - {disks[client_iqn]}"
                )
            LOG.info(f"[{iqn}] - Luns created sucessfully - {host_luns}")
