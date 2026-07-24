"""
Utility to cleanup stale virtual machines from IBM environment
"""

import csv
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Dict, List, Optional, Set, Union
from urllib.parse import parse_qs, urlparse

import yaml
from docopt import docopt

from ceph.parallel import parallel
from compute.ibm_vpc import CephVMNodeIBM

# ---------- Logging ----------
logger = logging.getLogger("ibm_vm_cleanup_report")
if not logger.handlers:
    _stream = logging.StreamHandler()
    _formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    _stream.setFormatter(_formatter)
    logger.addHandler(_stream)
logger.setLevel(logging.INFO)
# -----------------------------

doc = """
Utility to generate a CSV report of vm records from IBM cloud.
    Usage:
        ibm_vm_cleanup.py --creds <cred-file> [--output <csv-file>] [--delete] [--dry-run]
        ibm_vm_cleanup.py (-h | --help)
    Options:
        -h --help                Shows the command usage
        --creds <file>           API Credential file to access ibm cloud.
        --output <file>          Output CSV file path (default: vm_report_<timestamp>.csv)
        --delete                 Delete listed VMs after generating report
        --dry-run                Preview deletion targets without executing actual delete operations
"""

# Constants
API_RETRY_DELAY_SECONDS = 3
VM_AGE_THRESHOLD_HOURS = 48
RESOURCE_GROUP_NAME = "qe"
DNS_ZONE_NAME = "qe.ceph.au.lab"
DEFAULT_IBM_VPC_ENDPOINT = "https://eu-de.iaas.cloud.ibm.com/v1"

_DEFAULT_EXCLUDE_VM_NAMES_YAML = (
    Path(__file__).resolve().parent / "config" / "ibm_vm_cleanup_exclude_names.yaml"
)
_DEFAULT_IBMC_CONFIG_DIR = Path(__file__).resolve().parent.parent / "conf" / "ibmc"


def _load_exclude_vm_names(
    yaml_path: Optional[Union[str, Path]] = None,
) -> Set[str]:
    """
    Load VM ``name`` values to omit from reports/deletes (exact match).
    Expects ``exclude_vm_names:`` as a list of strings in the YAML file.
    """
    path = Path(yaml_path) if yaml_path is not None else _DEFAULT_EXCLUDE_VM_NAMES_YAML

    if not path.is_file():
        logger.warning("Exclude VM names YAML not found: %s", path)
        return set()

    try:
        with path.open("r", encoding="utf-8") as stream:
            data = yaml.safe_load(stream) or {}
    except (OSError, yaml.YAMLError) as exc:
        logger.warning("Could not read exclude VM names YAML %s: %s", path, exc)
        return set()

    raw = data.get("exclude_vm_names")
    if not isinstance(raw, list):
        logger.warning("%s: exclude_vm_names must be a list", path)
        return set()

    # Use set comprehension for efficiency and simplicity
    names = {item.strip() for item in raw if isinstance(item, str) and item.strip()}

    # Log invalid entries
    invalid = [item for item in raw if not isinstance(item, str) or not item.strip()]
    if invalid:
        logger.warning("Ignoring %d invalid exclude_vm_names entries", len(invalid))

    return names


# VM ``name`` values to omit from the generated CSV (exact match, all modes).
EXCLUDE_VM_NAMES = _load_exclude_vm_names()


def get_service_urls_from_ibmc_configs(
    config_dir: Optional[Union[str, Path]] = None,
) -> Set[str]:
    """
    Return the set of distinct ``service_url`` strings from ``*.yaml`` / ``*.yml``
    files under the IBM VPC config directory (default: ``conf/ibmc`` next to repo root).
    Files without a non-empty string ``service_url`` are skipped with a warning.
    """
    root = Path(config_dir) if config_dir is not None else _DEFAULT_IBMC_CONFIG_DIR
    if not root.is_dir():
        logger.warning(
            "IBM VPC config directory does not exist or is not a directory: %s", root
        )
        return set()

    urls: Set[str] = set()
    for pattern in ("*.yaml", "*.yml"):
        for path in sorted(root.glob(pattern)):
            try:
                with path.open("r", encoding="utf-8") as stream:
                    data = yaml.safe_load(stream) or {}
            except (OSError, yaml.YAMLError) as exc:
                logger.warning("Could not read IBM VPC config %s: %s", path, exc)
                continue

            raw = data.get("service_url")
            if not isinstance(raw, str) or not raw.strip():
                logger.warning("Skipping %s: missing or invalid service_url", path)
                continue
            urls.add(raw.strip())

    return urls


def exclude_vms_by_name(instances: List[Dict]) -> List[Dict]:
    """Return instances whose ``name`` is not in ``EXCLUDE_VM_NAMES``."""
    if not EXCLUDE_VM_NAMES:
        return instances
    return [inst for inst in instances if inst.get("name") not in EXCLUDE_VM_NAMES]


def get_instance_resource_group_name(instance: Dict) -> str:
    """IBM VPC ``instances`` record: return ``resource_group.name``, or empty string."""
    rg = instance.get("resource_group")
    if not isinstance(rg, dict):
        return ""
    name = rg.get("name")
    if not isinstance(name, str):
        return ""
    return name.strip()


def filter_instances_by_resource_group(
    instances: List[Dict], resource_group_name: str
) -> List[Dict]:
    """
    Return instances whose IBM resource group name equals ``resource_group_name``
    (exact match after stripping whitespace on the instance side).
    """
    if not resource_group_name:
        return list(instances)
    return [
        inst
        for inst in instances
        if get_instance_resource_group_name(inst) == resource_group_name
    ]


def get_volume_names(instance: Dict) -> List[str]:
    """Extract volume names from instance's volume attachments."""
    return [
        attachment.get("volume", {}).get("name")
        for attachment in instance.get("volume_attachments", [])
        if attachment.get("volume", {}).get("name")
    ]


def get_instance_ip(instance: Dict) -> str:
    """Get the primary IP address of an instance."""
    primary_ip = (
        instance.get("primary_network_interface", {})
        .get("primary_ip", {})
        .get("address")
    )
    if primary_ip:
        return primary_ip

    for interface in instance.get("network_interfaces", []):
        ip_address = interface.get("primary_ip", {}).get("address")
        if ip_address:
            return ip_address

    return ""


def parse_created_at(created_at: str) -> Optional[datetime]:
    """Return creation time in UTC, or None if unparseable or empty."""
    if not created_at:
        return None
    try:
        created_time = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    if created_time.tzinfo is None:
        created_time = created_time.replace(tzinfo=timezone.utc)
    return created_time


def get_instance_age_seconds(created_at: str) -> Optional[float]:
    """Seconds since instance creation, or None if created_at is missing/invalid."""
    created_time = parse_created_at(created_at)
    if created_time is None:
        return None
    age_delta = datetime.now(timezone.utc) - created_time
    return max(age_delta.total_seconds(), 0.0)


def get_vms_running_over_threshold_hours(instances: List[Dict]) -> List[Dict]:
    """
    Return instances whose age from ``created_at`` is strictly greater than threshold hours.
    Instances without a valid ``created_at`` are excluded.
    """
    threshold_sec = VM_AGE_THRESHOLD_HOURS * 3600.0
    return [
        inst
        for inst in instances
        if get_instance_age_seconds(inst.get("created_at") or "") > threshold_sec
    ]


def get_vm_age(created_at: Optional[str]) -> str:
    """Calculate and format VM age from creation timestamp."""
    if not created_at:
        return ""
    created_time = parse_created_at(created_at)
    if created_time is None:
        return ""

    age_delta = datetime.now(timezone.utc) - created_time
    total_seconds = max(int(age_delta.total_seconds()), 0)
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, _ = divmod(remainder, 60)
    return "{days}d {hours}hrs {minutes}mins".format(
        days=days,
        hours=hours,
        minutes=minutes,
    )


def extract_start_token(next_href: str) -> str:
    """Extract pagination start token from next_href URL."""
    if not next_href:
        return ""

    query = parse_qs(urlparse(next_href).query)
    start_values = query.get("start", [])
    return start_values[0] if start_values else ""


def write_instances_to_csv(instances: List[Dict], output_file: str) -> None:
    """Write VM instance information to a CSV file."""
    fieldnames = [
        "name",
        "id",
        "ip_address",
        "status",
        "created_at",
        "age",
        "resource_group",
        "vpc",
        "zone",
        "volumes",
        "service_url",
    ]
    try:
        with open(output_file, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for instance in instances:
                writer.writerow(
                    {
                        "name": instance.get("name"),
                        "id": instance.get("id"),
                        "ip_address": get_instance_ip(instance),
                        "status": instance.get("status"),
                        "created_at": instance.get("created_at"),
                        "age": get_vm_age(instance.get("created_at")),
                        "resource_group": instance.get("resource_group", {}).get(
                            "name"
                        ),
                        "vpc": instance.get("vpc", {}).get("name"),
                        "zone": instance.get("zone", {}).get("name"),
                        "volumes": ",".join(get_volume_names(instance)),
                        "service_url": instance.get("_ibm_service_url"),
                    }
                )
    except (OSError, IOError) as e:
        logger.error("Failed to write CSV file %s: %s", output_file, e)
        raise Exception("Failed to write CSV file")


def get_all_vms(vm_node: CephVMNodeIBM, vpc_name: str) -> List[Dict]:
    """Retrieve all VM instances from a specific VPC using pagination."""
    instances = []
    start = None
    total_count = None

    while True:
        kwargs = {"vpc_name": vpc_name}
        if start:
            kwargs["start"] = start

        list_inst = vm_node.service.list_instances(**kwargs)
        if list_inst.get_status_code() != 200:
            raise Exception("Failed to retrieve instances")

        list_result = list_inst.get_result()
        if total_count is None:
            total_count = list_result.get("total_count")

        page_instances = list_result.get("instances", [])
        if page_instances:
            instances.extend(page_instances)

        next_href = list_result.get("next", {}).get("href")
        if not next_href:
            break

        start = extract_start_token(next_href)
        if not start:
            raise Exception("Could not parse pagination token for instances")

    if total_count is not None and len(instances) != total_count:
        raise Exception(
            "Failed to list all instances. Total: {total}, Listed: {listed}".format(
                total=total_count, listed=len(instances)
            )
        )
    logger.info("Found %s VM instances in %s", len(instances), vpc_name)
    return instances


def get_all_vpcs(vm_node: CephVMNodeIBM) -> List[Dict]:
    """List all VPCs from IBM Cloud (paginated)."""
    vpcs = []
    start = None

    while True:
        kwargs = {}
        if start:
            kwargs["start"] = start

        vpc_list_response = vm_node.service.list_vpcs(**kwargs)
        if vpc_list_response.get_status_code() != 200:
            raise Exception("Failed to retrieve VPCs")

        result = vpc_list_response.get_result()
        vpcs.extend(result.get("vpcs", []))

        next_href = result.get("next", {}).get("href")
        if not next_href:
            break

        start = extract_start_token(next_href)
        if not start:
            raise Exception("Could not parse pagination token for VPCs")

    logger.info("Found %s VPCs", len(vpcs))

    return vpcs


def delete_listed_ibmc_vms(
    instances: List[Dict],
    os_cred_ibm: Dict[str, str],
    dns_zone_name: str,
    dns_svc_id: str,
) -> None:
    """
    Delete the given IBM VPC instance dicts (API ``instances`` records).
    ``os_cred_ibm`` must include ``accesskey`` and a fallback ``service_url``.
    Each instance may include ``_ibm_service_url`` (set when listing across
    multiple regional endpoints) so deletes use the correct VPC API host.
    Uses the same pattern as ``cleanup_ibmc_ceph_nodes`` in ceph/utils.py:
    ``CephVMNodeIBM.delete`` per VM with staggered starts to reduce DNS API
    throttling.
    """
    if not instances:
        logger.info("No VM instances to delete.")
        return

    names = [inst.get("name") or inst.get("id", "") for inst in instances]
    logger.warning(
        "Deleting %s IBM VM instance(s): %s",
        len(instances),
        ", ".join(names),
    )

    counter = 0
    with parallel() as p:
        for instance in instances:
            sleep(counter * API_RETRY_DELAY_SECONDS)
            regional_cred = {
                **os_cred_ibm,
                "service_url": instance.get("_ibm_service_url")
                or os_cred_ibm.get("service_url", DEFAULT_IBM_VPC_ENDPOINT),
            }
            vsi = CephVMNodeIBM(os_cred_ibm=regional_cred, node=instance)
            p.spawn(vsi.delete, dns_zone_name, dns_svc_id)
            counter += 1

    logger.info("Completed IBM VM delete workflow for %s instance(s).", len(instances))


def _service_urls_for_scan(ibm_cred: Dict) -> List[str]:
    """
    Region endpoints to scan: distinct ``service_url`` values from ``conf/ibmc``
    (see ``get_service_urls_from_ibmc_configs``), plus the credential file's
    ``service-url`` when present. If no config URLs exist, use cred default or
    IBM EU-DE VPC endpoint.
    """
    default_url = ibm_cred.get("service-url") or DEFAULT_IBM_VPC_ENDPOINT
    if isinstance(default_url, str):
        default_url = default_url.strip()
    else:
        default_url = DEFAULT_IBM_VPC_ENDPOINT

    from_configs = get_service_urls_from_ibmc_configs()
    if not from_configs:
        logger.info(
            "No service_url in conf/ibmc; using single endpoint: %s", default_url
        )
        return [default_url]

    merged: Set[str] = set(from_configs)
    cred_url = ibm_cred.get("service-url")
    if isinstance(cred_url, str) and cred_url.strip():
        merged.add(cred_url.strip())
    ordered = sorted(merged)
    logger.info("Scanning %s IBM VPC endpoint(s): %s", len(ordered), ordered)
    return ordered


def run(args: Dict) -> int:
    """Main execution function for IBM VM cleanup utility."""
    cred_file = args["--creds"]
    output_file = args.get("--output")
    delete_enabled = bool(args.get("--delete"))
    dry_run = bool(args.get("--dry-run"))

    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = "vm_report_{ts}.csv".format(ts=timestamp)

    try:
        with open(cred_file, "r") as cred_stream:
            yh = yaml.safe_load(cred_stream) or {}
            ibm_cred = yh.get("globals", {}).get("ibm-credentials", {})
            if not ibm_cred:
                raise KeyError("globals.ibm-credentials")

            access_key = ibm_cred.get("access-key")
            if not access_key:
                raise KeyError("globals.ibm-credentials.access-key")

            service_urls = _service_urls_for_scan(ibm_cred)
            base_os_cred_ibm = {"accesskey": access_key, "service_url": service_urls[0]}
            logger.info(
                "IBM access key loaded; base service_url: %s",
                base_os_cred_ibm["service_url"],
            )

            all_instances: List[Dict] = []
            failed_vpcs: List[str] = []
            seen_instance_ids: Set[str] = set()

            for service_url in service_urls:
                os_cred_ibm = {"accesskey": access_key, "service_url": service_url}
                vm_node = CephVMNodeIBM(os_cred_ibm=os_cred_ibm)

                vpcs = get_all_vpcs(vm_node)
                for vpc in vpcs:
                    vpc_name = vpc.get("name")
                    if not vpc_name:
                        logger.warning("Skipping VPC record without name: %s", vpc)
                        continue
                    logger.info(
                        "Retrieving VM instances from VPC %s (endpoint %s)...",
                        vpc_name,
                        service_url,
                    )
                    try:
                        vpc_instances = get_all_vms(vm_node, vpc_name)
                        logger.info(
                            "Found %s VM instances in %s @ %s",
                            len(vpc_instances),
                            vpc_name,
                            service_url,
                        )
                        for inst in vpc_instances:
                            iid = inst.get("id")
                            if isinstance(iid, str) and iid in seen_instance_ids:
                                continue
                            if isinstance(iid, str):
                                seen_instance_ids.add(iid)
                            tagged = {**inst, "_ibm_service_url": service_url}
                            all_instances.append(tagged)
                    except Exception as e:
                        logger.warning(
                            "Could not retrieve instances from VPC %s @ %s: %s",
                            vpc_name,
                            service_url,
                            e,
                        )
                        failed_vpcs.append("{0} @ {1}".format(vpc_name, service_url))
                        continue

            logger.info(
                "Total VM instances across all VPCs and endpoints: %s",
                len(all_instances),
            )
            all_instances = filter_instances_by_resource_group(
                all_instances, RESOURCE_GROUP_NAME
            )

            total_before = len(all_instances)
            all_instances = get_vms_running_over_threshold_hours(all_instances)
            logger.info(
                "Long-running (>threshold hours): %s of %s VM(s) in report",
                len(all_instances),
                total_before,
            )
            if EXCLUDE_VM_NAMES:
                before_ex = len(all_instances)
                all_instances = exclude_vms_by_name(all_instances)
                excluded = before_ex - len(all_instances)
                if excluded:
                    logger.info(
                        "Excluded %s VM(s) matching EXCLUDE_VM_NAMES",
                        excluded,
                    )

            write_instances_to_csv(all_instances, output_file)
            logger.info("VM report written to %s", output_file)

            if dry_run:
                logger.info(
                    "Listed %s VM(s) to be deleted: %s",
                    len(all_instances),
                    ", ".join(
                        inst.get("name") or inst.get("id", "") for inst in all_instances
                    ),
                )

            if delete_enabled:
                if dry_run:
                    logger.info(
                        "--dry-run set: skipping delete for %s VM(s)",
                        len(all_instances),
                    )
                else:
                    dns_svc_id = ibm_cred.get("dns_svc_id")
                    if not dns_svc_id:
                        logger.error("Cannot delete VMs: dns_svc_id must be defined")
                        return 1
                    logger.info(
                        "Deleting VMs: dns_zone_name: %s, dns_svc_id: %s",
                        DNS_ZONE_NAME,
                        dns_svc_id,
                    )
                    try:
                        delete_listed_ibmc_vms(
                            all_instances,
                            base_os_cred_ibm,
                            DNS_ZONE_NAME,
                            dns_svc_id,
                        )
                    except Exception:
                        logger.exception("IBM VM deletion failed")
                        return 1

            if failed_vpcs:
                logger.warning(
                    "Failed to retrieve instances from %s VPC/endpoint pair(s): %s",
                    len(failed_vpcs),
                    ", ".join(sorted(failed_vpcs)),
                )
                return 1

            return 0

    except FileNotFoundError:
        logger.error("Error: Credential file not found: %s", cred_file)
        return 1
    except KeyError as e:
        logger.error("Error: Missing required configuration key: %s", e)
        return 1
    except Exception:
        logger.exception("Error generating vm report or deleting VMs")
        return 1


if __name__ == "__main__":
    try:
        arguments = docopt(doc)
        rc = run(arguments)
    except Exception:
        logger.exception("Failed to cleanup stale virtual machines")
        rc = 1
    sys.exit(rc)
